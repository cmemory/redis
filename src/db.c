/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "cluster.h"
#include "atomicvar.h"

#include <signal.h>
#include <ctype.h>

/* Database backup. */
struct dbBackup {
    redisDb *dbarray;
    rax *slots_to_keys;
    uint64_t slots_keys_count[CLUSTER_SLOTS];
};

/*-----------------------------------------------------------------------------
 * C-level DB API
 *----------------------------------------------------------------------------*/

int keyIsExpired(redisDb *db, robj *key);

/* Update LFU when an object is accessed.
 * Firstly, decrement the counter if the decrement time is reached.
 * Then logarithmically increment the counter, and update the access time. */
// 当obj被访问时，更新对象的LFU。
// 我们先基于当前时间与上一次访问时间的差值，来计算并减小相应的频次，得到当前查询key的lfu频次。
// 然后针对查出的lfu频次，对数形式增加一次访问计数，最后将新的频次与当前访问时间结合形成该对象新的lfu值。
void updateLFU(robj *val) {
    // 根据当前时间，提取出key剩余的lfu频次。
    unsigned long counter = LFUDecrAndReturn(val);
    // 当前访问了key，对数形式增加lfu频次。
    counter = LFULogIncr(counter);
    // lfu频次结合当前访问时间，形成对象新的lfu值。
    val->lru = (LFUGetTimeInMinutes()<<8) | counter;
}

/* Low level key lookup API, not actually called directly from commands
 * implementations that should instead rely on lookupKeyRead(),
 * lookupKeyWrite() and lookupKeyReadWithFlags(). */
// 低级别的查询API。不应该直接调用该函数来实现命令，而应该通过lookupKey[Read|Write|ReadWithFlags]()等方法来处理。
robj *lookupKey(redisDb *db, robj *key, int flags) {
    // 根据key从dict中查询entry。
    dictEntry *de = dictFind(db->dict,key->ptr);
    if (de) {
        robj *val = dictGetVal(de);

        /* Update the access time for the ageing algorithm.
         * Don't do it if we have a saving child, as this will trigger
         * a copy on write madness. */
        // 当前访问该key，需要更新lru空闲时间 或者 lfu访问频次。
        // 注意当有后台持久化处理的子进程时，我们不更新lru/lfu，否则会导致很多的copy-on-write页面。
        if (!hasActiveChildProcess() && !(flags & LOOKUP_NOTOUCH)){
            // 如果是LFU策略，则更新对象lfu值，否则更新lru时钟。
            if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
                updateLFU(val);
            } else {
                val->lru = LRU_CLOCK();
            }
        }
        // 查到了则返回val，没查到返回NULL。
        return val;
    } else {
        return NULL;
    }
}

/* Lookup a key for read operations, or return NULL if the key is not found
 * in the specified DB.
 *
 * As a side effect of calling this function:
 * 1. A key gets expired if it reached it's TTL.
 * 2. The key last access time is updated.
 * 3. The global keys hits/misses stats are updated (reported in INFO).
 * 4. If keyspace notifications are enabled, a "keymiss" notification is fired.
 *
 * This API should not be used when we write to the key after obtaining
 * the object linked to the key, but only for read only operations.
 *
 * Flags change the behavior of this command:
 *
 *  LOOKUP_NONE (or zero): no special flags are passed.
 *  LOOKUP_NOTOUCH: don't alter the last access time of the key.
 *
 * Note: this function also returns NULL if the key is logically expired
 * but still existing, in case this is a slave, since this API is called only
 * for read operations. Even if the key expiry is master-driven, we can
 * correctly report a key is expired on slaves even if the master is lagging
 * expiring our key via DELs in the replication link. */
// 查询一个key用于读操作，当指定的db中不存在该key时，返回NULL。这个函数有如下的副作用：
//  1、如果检查到key达到了过期时间，则会处理该key过期。
//  2、会更新该key的最近一次访问时间（lru/lfu信息）。
//  3、根据查询结果，更新全局的keys hits/misses状态（用于INFO命令展示）。
//  4、如果flags允许keyspace变动消息通知，则在keymiss时触发通知。
// 这个函数应该只拥于读操作查询，如果想要查询数据来进行写操作不应该使用这个函数（而应该使用lookupKeyWriteWithFlags）。
// flags标识会改变命令的行为：
//  LOOKUP_NONE（或者0），表示没有特殊标识。
//  LOOKUP_NOTOUCH，表示不更新key的最近访问时间（lru/lfu信息）。
//  LOOKUP_NONOTIFY，不触发keyspace keymiss通知。
// 注意如果key逻辑上过期了，但还是存在时，我们仍然会返回NULL，因为这是读取操作，有可能在slave上执行，所以可能会出现存在过期的key（主从大的lag）。
robj *lookupKeyReadWithFlags(redisDb *db, robj *key, int flags) {
    robj *val;

    // 处理key过期操作，如果key过期了会返回1。
    if (expireIfNeeded(db,key) == 1) {
        /* If we are in the context of a master, expireIfNeeded() returns 1
         * when the key is no longer valid, so we can return NULL ASAP. */
        // 如果当前是master，expireIfNeeded返回1表示我们过期删除了该key，所以key无效了，直接跳到keymiss尽早处理返回。
        if (server.masterhost == NULL)
            goto keymiss;

        /* However if we are in the context of a slave, expireIfNeeded() will
         * not really try to expire the key, it only returns information
         * about the "logical" status of the key: key expiring is up to the
         * master in order to have a consistent view of master's data set.
         *
         * However, if the command caller is not the master, and as additional
         * safety measure, the command invoked is a read-only command, we can
         * safely return NULL here, and provide a more consistent behavior
         * to clients accessing expired values in a read-only fashion, that
         * will say the key as non existing.
         *
         * Notably this covers GETs when slaves are used to scale reads. */
        // 如果当前是slave，expireIfNeeded虽然返回1，但并没有真的过期删除key，只是逻辑上与master一致。
        // 为了保证数据的一致，key的真正过期需要master过期处理并同步DEL过来，然后slave才能真正删除。
        // 如果命令调用方不是master，因为命令处理时做了额外的安全措施，命令是只读的，所以可以安全的返回NULL，为访问过期key提供一致性结果。
        if (server.current_client &&
            server.current_client != server.master &&
            server.current_client->cmd &&
            server.current_client->cmd->flags & CMD_READONLY)
        {
            // client不是master，并没命令是只读的，keymiss处理。
            goto keymiss;
        }
    }
    // expires字典中没有找到该key的过期时间，可能key不存在，或者key没有设置过期时间。那么这里从数据db查询该key的值。
    // 查到了全面统计命中次数+1，并返回查到的value。如果没查到，即val为NULL，则处理keymiss。
    val = lookupKey(db,key,flags);
    if (val == NULL)
        goto keymiss;
    server.stat_keyspace_hits++;
    return val;

keymiss:
    // keymiss，统计全面miss次数+1，并返回NULL。另外如果需要keyspace通知，则触发keymiss事件通知。
    if (!(flags & LOOKUP_NONOTIFY)) {
        notifyKeyspaceEvent(NOTIFY_KEY_MISS, "keymiss", key, db->id);
    }
    server.stat_keyspace_misses++;
    return NULL;
}

/* Like lookupKeyReadWithFlags(), but does not use any flag, which is the
 * common case. */
robj *lookupKeyRead(redisDb *db, robj *key) {
    return lookupKeyReadWithFlags(db,key,LOOKUP_NONE);
}

/* Lookup a key for write operations, and as a side effect, if needed, expires
 * the key if its TTL is reached.
 *
 * Returns the linked value object if the key exists or NULL if the key
 * does not exist in the specified DB. */
// 查找key用于写操作。有副作用，会检测key是否过期，过期则会移除key。
// 如果指定DB中，查询的key存在，则返回对应的value，否则会返回NULL。
robj *lookupKeyWriteWithFlags(redisDb *db, robj *key, int flags) {
    // 检查key过期处理
    expireIfNeeded(db,key);
    // 查询key，返回value
    return lookupKey(db,key,flags);
}

robj *lookupKeyWrite(redisDb *db, robj *key) {
    return lookupKeyWriteWithFlags(db, key, LOOKUP_NONE);
}
static void SentReplyOnKeyMiss(client *c, robj *reply){
    serverAssert(sdsEncodedObject(reply));
    sds rep = reply->ptr;
    // 根据传入的reply的首字符判断，'-'表示err，其他的都是正常回复。
    if (sdslen(rep) > 1 && rep[0] == '-'){
        addReplyErrorObject(c, reply);
    } else {
        addReply(c,reply);
    }
}
robj *lookupKeyReadOrReply(client *c, robj *key, robj *reply) {
    // 查询key for read
    robj *o = lookupKeyRead(c->db, key);
    // key miss消息回复给client
    if (!o) SentReplyOnKeyMiss(c, reply);
    return o;
}

robj *lookupKeyWriteOrReply(client *c, robj *key, robj *reply) {
    // db查询key
    robj *o = lookupKeyWrite(c->db, key);
    // key不存在则将传入的信息回复给client。
    if (!o) SentReplyOnKeyMiss(c, reply);
    return o;
}

/* Add the key to the DB. It's up to the caller to increment the reference
 * counter of the value if needed.
 *
 * The program is aborted if the key already exists. */
// 将key加入到db中。这里不会管value的refcount，需要处理的化，应该由调用方来增加value的引用计数。
// 另外调用方应该检查好db中是否有key，这里添加如果发现key存在会直接abort。
void dbAdd(redisDb *db, robj *key, robj *val) {
    // 加入key-value到db中。key是这里新创建的sds，不会共享原key对象。value可能共享，需要调用方来处理引用计数。
    sds copy = sdsdup(key->ptr);
    int retval = dictAdd(db->dict, copy, val);

    serverAssertWithInfo(NULL,key,retval == DICT_OK);
    // key发生变化，通知阻塞在该key上的clients，对应的clients会加入到ready列表中，等待进行处理。
    signalKeyAsReady(db, key, val->type);
    // 如果是集群模式，我们需要将key加入到slot->keys关联的keys列表中。
    if (server.cluster_enabled) slotToKeyAdd(key->ptr);
}

/* This is a special version of dbAdd() that is used only when loading
 * keys from the RDB file: the key is passed as an SDS string that is
 * retained by the function (and not freed by the caller).
 *
 * Moreover this function will not abort if the key is already busy, to
 * give more control to the caller, nor will signal the key as ready
 * since it is not useful in this context.
 *
 * The function returns 1 if the key was added to the database, taking
 * ownership of the SDS string, otherwise 0 is returned, and is up to the
 * caller to free the SDS string. */
int dbAddRDBLoad(redisDb *db, sds key, robj *val) {
    int retval = dictAdd(db->dict, key, val);
    if (retval != DICT_OK) return 0;
    if (server.cluster_enabled) slotToKeyAdd(key);
    return 1;
}

/* Overwrite an existing key with a new value. Incrementing the reference
 * count of the new value is up to the caller.
 * This function does not modify the expire time of the existing key.
 *
 * The program is aborted if the key was not already present. */
// 使用一个新的value对象来重写已经存在的key。新value对象的引用计数+1需要该函数的调用者来处理。
// 这个函数不会改变key原有的过期时间。如果key不存在，则abort，所以需要调用方保证key存在才调用这个方法。
void dbOverwrite(redisDb *db, robj *key, robj *val) {
    dictEntry *de = dictFind(db->dict,key->ptr);

    // 确保key存在与db中
    serverAssertWithInfo(NULL,key,de != NULL);
    dictEntry auxentry = *de;
    // 获取老的value对象
    robj *old = dictGetVal(de);
    // 如果是lru，对象创建的时候会写入当前时间，而访问对象时这里要设置也是更新lur为当前时间，所以不需要处理。
    // 如果是lfu，则我们需要使用原来value的数据（因为跟访问频率有关，查询时会实时计算）。
    if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
        val->lru = old->lru;
    }
    /* Although the key is not really deleted from the database, we regard 
    overwrite as two steps of unlink+add, so we still need to call the unlink 
    callback of the module. */
    // 尽管key没有被真正删除，但是我们认为overwrite处理分为unlink+add两步，所以我们这里还是需要调用module的unlink回调函数。
    moduleNotifyKeyUnlink(key,old);
    // 设置key的新val
    dictSetVal(db->dict, de, val);

    if (server.lazyfree_lazy_server_del) {
        // 如果配置可以异步删除，这里加入异步队列进行处理，另外设置value值为NULL，从而后面dict free不用处理value释放。
        freeObjAsync(key,old);
        dictSetVal(db->dict, &auxentry, NULL);
    }

    // 释放value，free(NULL)不会有问题。
    dictFreeVal(db->dict, &auxentry);
}

/* High level Set operation. This function can be used in order to set
 * a key, whatever it was existing or not, to a new object.
 *
 * 1) The ref count of the value object is incremented.
 * 2) clients WATCHing for the destination key notified.
 * 3) The expire time of the key is reset (the key is made persistent),
 *    unless 'keepttl' is true.
 *
 * All the new keys in the database should be created via this interface.
 * The client 'c' argument may be set to NULL if the operation is performed
 * in a context where there is no clear client performing the operation. */
// 高级别的Set操作，不管key存不存在，都会set一个新的value对象（即使值与原value一致）。主要操作如下：
//  1、查询db，如果不存在key直接dbAdd加入；如果存在则创建新的value对象重写db中存的数据。
//  2、增加新的value对象的引用计数。
//  3、如果需要，则通知key变更处理（事务watch处理，client侧缓存追踪表中对应key的无效处理）。
//  4、重置移除key的过期时间，即设置key为不过期。当然如有设置keepttl，则不移除过期时间。
// 所有在db中写入新key的操作都应该通过这个方法处理。如果执行这个函数的context并不是一个client上下午，则c可以传入NULL。
void genericSetKey(client *c, redisDb *db, robj *key, robj *val, int keepttl, int signal) {
    if (lookupKeyWrite(db,key) == NULL) {
        // 不存在，直接add
        dbAdd(db,key,val);
    } else {
        // 存在，则重写
        dbOverwrite(db,key,val);
    }
    incrRefCount(val);
    // 处理过期时间，没指定keepttl则直接移除。
    if (!keepttl) removeExpire(db,key);
    // 如果需要则通知key变更。
    if (signal) signalModifiedKey(c,db,key);
}

/* Common case for genericSetKey() where the TTL is not retained. */
// genericSetKey()的封装，一般的SET命令操作，不保留TTL。
void setKey(client *c, redisDb *db, robj *key, robj *val) {
    genericSetKey(c,db,key,val,0,1);
}

/* Return a random key, in form of a Redis object.
 * If there are no keys, NULL is returned.
 *
 * The function makes sure to return keys not already expired. */
robj *dbRandomKey(redisDb *db) {
    dictEntry *de;
    int maxtries = 100;
    int allvolatile = dictSize(db->dict) == dictSize(db->expires);

    while(1) {
        sds key;
        robj *keyobj;

        de = dictGetFairRandomKey(db->dict);
        if (de == NULL) return NULL;

        key = dictGetKey(de);
        keyobj = createStringObject(key,sdslen(key));
        if (dictFind(db->expires,key)) {
            if (allvolatile && server.masterhost && --maxtries == 0) {
                /* If the DB is composed only of keys with an expire set,
                 * it could happen that all the keys are already logically
                 * expired in the slave, so the function cannot stop because
                 * expireIfNeeded() is false, nor it can stop because
                 * dictGetRandomKey() returns NULL (there are keys to return).
                 * To prevent the infinite loop we do some tries, but if there
                 * are the conditions for an infinite loop, eventually we
                 * return a key name that may be already expired. */
                return keyobj;
            }
            if (expireIfNeeded(db,keyobj)) {
                decrRefCount(keyobj);
                continue; /* search for another key. This expired. */
            }
        }
        return keyobj;
    }
}

/* Delete a key, value, and associated expiration entry if any, from the DB */
// 从DB中删除key、value，以及相关联的expiration元素。
int dbSyncDelete(redisDb *db, robj *key) {
    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary. */
    // 从expires删除元素并不会释放key，因为它是与主要数据dict共享的。
    if (dictSize(db->expires) > 0) dictDelete(db->expires,key->ptr);
    // Unlink移除db关联，取到该元素，后面自己处理释放。
    dictEntry *de = dictUnlink(db->dict,key->ptr);
    if (de) {
        robj *val = dictGetVal(de);
        /* Tells the module that the key has been unlinked from the database. */
        // key移除db事件通知module
        moduleNotifyKeyUnlink(key,val);
        // 释放取到的key
        dictFreeUnlinkedEntry(db->dict,de);
        // 如果是集群模式，还要从slot->keys映射中，移除该key。
        if (server.cluster_enabled) slotToKeyDel(key->ptr);
        return 1;
    } else {
        return 0;
    }
}

/* This is a wrapper whose behavior depends on the Redis lazy free
 * configuration. Deletes the key synchronously or asynchronously. */
int dbDelete(redisDb *db, robj *key) {
    return server.lazyfree_lazy_server_del ? dbAsyncDelete(db,key) :
                                             dbSyncDelete(db,key);
}

/* Prepare the string object stored at 'key' to be modified destructively
 * to implement commands like SETBIT or APPEND.
 *
 * An object is usually ready to be modified unless one of the two conditions
 * are true:
 *
 * 1) The object 'o' is shared (refcount > 1), we don't want to affect
 *    other users.
 * 2) The object encoding is not "RAW".
 *
 * If the object is found in one of the above conditions (or both) by the
 * function, an unshared / not-encoded copy of the string object is stored
 * at 'key' in the specified 'db'. Otherwise the object 'o' itself is
 * returned.
 *
 * USAGE:
 *
 * The object 'o' is what the caller already obtained by looking up 'key'
 * in 'db', the usage pattern looks like this:
 *
 * o = lookupKeyWrite(db,key);
 * if (checkType(c,o,OBJ_STRING)) return;
 * o = dbUnshareStringValue(db,key,o);
 *
 * At this point the caller is ready to modify the object, for example
 * using an sdscat() call to append some data, or anything else.
 */
// 处理key对应的value对象，从而我们后面能对它进行破坏性的更新操作，主要用于实现SETBIT、APPEND这类命令。
// 下面情况任意一种，我们都会基于原value复制并创建新的raw编码的字符串，用于更新操作：
//  1、value字符串对象是共享的。我们需要破坏性更新，所以复制创建自己的值对象来处理。
//  2、value字符串编码不是raw类型。因为这类更新操作，需要是字符串类型编码处理，很可能需要扩容。所以直接raw编码最好。
// 使用方法见上面注释。
robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o) {
    serverAssert(o->type == OBJ_STRING);
    if (o->refcount != 1 || o->encoding != OBJ_ENCODING_RAW) {
        // 共享对象 或者 编码不是raw 时，我们需要基于原字符串数据重新创建新的value对象。
        robj *decoded = getDecodedObject(o);
        o = createRawStringObject(decoded->ptr, sdslen(decoded->ptr));
        decrRefCount(decoded);
        // 新的值更新到db中
        dbOverwrite(db,key,o);
    }
    return o;
}

/* Remove all keys from the database(s) structure. The dbarray argument
 * may not be the server main DBs (could be a backup).
 *
 * The dbnum can be -1 if all the DBs should be emptied, or the specified
 * DB index if we want to empty only a single database.
 * The function returns the number of keys removed from the database(s). */
// 从database结构中移除所有的keys。dbarray参数可以是服务主要的数据DB，也可以是一个backup DB。
// dbnum=-1表示所有的DBs都需要被清空。也可以指定DB索引，从而只清空单个DB。
// 函数返回从所有DB中移除的keys的数量。
long long emptyDbStructure(redisDb *dbarray, int dbnum, int async,
                           void(callback)(void*))
{
    long long removed = 0;
    int startdb, enddb;

    // 处理start和end db
    if (dbnum == -1) {
        startdb = 0;
        enddb = server.dbnum-1;
    } else {
        startdb = enddb = dbnum;
    }

    for (int j = startdb; j <= enddb; j++) {
        // 统计总共删除的keys数量
        removed += dictSize(dbarray[j].dict);
        // 同步删除的话直接调用dictEmpty处理dict和expire。异步的话构建清空DB的job加入任务队列，等待异步线程后台处理。
        if (async) {
            emptyDbAsync(&dbarray[j]);
        } else {
            dictEmpty(dbarray[j].dict,callback);
            dictEmpty(dbarray[j].expires,callback);
        }
        /* Because all keys of database are removed, reset average ttl. */
        // 该DB所有的key被清理了，需要重置相关属性。
        dbarray[j].avg_ttl = 0;
        dbarray[j].expires_cursor = 0;
    }

    return removed;
}

/* Remove all keys from all the databases in a Redis server.
 * If callback is given the function is called from time to time to
 * signal that work is in progress.
 *
 * The dbnum can be -1 if all the DBs should be flushed, or the specified
 * DB number if we want to flush only a single Redis database number.
 *
 * Flags are be EMPTYDB_NO_FLAGS if no special flags are specified or
 * EMPTYDB_ASYNC if we want the memory to be freed in a different thread
 * and the function to return ASAP.
 *
 * On success the function returns the number of keys removed from the
 * database(s). Otherwise -1 is returned in the specific case the
 * DB number is out of range, and errno is set to EINVAL. */
// 移除redis服务数据库中的所有keys。如果传入了callback，则每处理一部分数据会调用该回调函数。
// dbnum可以传-1，表示所有的DBs都要被清空。也可以指定DB number，从而只清空指定的DB。
// flags参数，EMPTYDB_NO_FLAGS表示没有特殊标识指定，EMPTYDB_ASYNC表示异步线程清理数据。
// 函数成功会返回删除的keys数量。失败会返回-1，表示dbnum指定不对，同时errno会设置为EINVAL。
long long emptyDb(int dbnum, int flags, void(callback)(void*)) {
    // flags标识是否是执行异步清理
    int async = (flags & EMPTYDB_ASYNC);
    RedisModuleFlushInfoV1 fi = {REDISMODULE_FLUSHINFO_VERSION,!async,dbnum};
    long long removed = 0;

    if (dbnum < -1 || dbnum >= server.dbnum) {
        errno = EINVAL;
        return -1;
    }

    /* Fire the flushdb modules event. */
    // 清空DB事件通知modules
    moduleFireServerEvent(REDISMODULE_EVENT_FLUSHDB,
                          REDISMODULE_SUBEVENT_FLUSHDB_START,
                          &fi);

    /* Make sure the WATCHed keys are affected by the FLUSH* commands.
     * Note that we need to call the function while the keys are still
     * there. */
    // 确保被WATCH的keys都受到FLUSH*命令的影响。注意需要在所有keys都在的情况下调用这个函数。
    signalFlushedDb(dbnum, async);

    /* Empty redis database structure. */
    // 清空redis database结构
    removed = emptyDbStructure(server.db, dbnum, async, callback);

    /* Flush slots to keys map if enable cluster, we can flush entire
     * slots to keys map whatever dbnum because only support one DB
     * in cluster mode. */
    // 如果是集群模式，需要清空slots->keys的映射表。我们可以不用管dbnum而清空所有，因为集群模式下只支持一个DB。
    if (server.cluster_enabled) slotToKeyFlush(async);

    // 当dbnum=-1时，清除slave中自己创建的带有过期时间的keys。避免master传递相同name/db的key过来从而被误expire掉。
    if (dbnum == -1) flushSlaveKeysWithExpireList();

    /* Also fire the end event. Note that this event will fire almost
     * immediately after the start event if the flush is asynchronous. */
    // 触发flushdb end事件通知module。注意，即使是异步清空数据，这事件也几乎在开始事件之后立即触发。
    moduleFireServerEvent(REDISMODULE_EVENT_FLUSHDB,
                          REDISMODULE_SUBEVENT_FLUSHDB_END,
                          &fi);

    return removed;
}

/* Store a backup of the database for later use, and put an empty one
 * instead of it. */
// 备份当前的database供后面使用，并创建一个新的数据库来替代。
dbBackup *backupDb(void) {
    // 分配空间
    dbBackup *backup = zmalloc(sizeof(dbBackup));

    /* Backup main DBs. */
    // 备份主要是处理dict，expires。因为我们DB层面的持久化或替换什么的只对数据及过期时间处理。
    // 当我们要替换数据库时，显然其他keys监听相关的我们会在备份之前处理掉，所以这里不用管这些。
    backup->dbarray = zmalloc(sizeof(redisDb)*server.dbnum);
    for (int i=0; i<server.dbnum; i++) {
        // 备份，也就是调整指针而已
        backup->dbarray[i] = server.db[i];
        // 用新的空dict来替换数据及过期时间字典。
        server.db[i].dict = dictCreate(&dbDictType,NULL);
        server.db[i].expires = dictCreate(&dbExpiresDictType,NULL);
    }

    /* Backup cluster slots to keys map if enable cluster. */
    // 如果是集群，我们还要备份slots_to_keys关联表。
    if (server.cluster_enabled) {
        backup->slots_to_keys = server.cluster->slots_to_keys;
        memcpy(backup->slots_keys_count, server.cluster->slots_keys_count,
            sizeof(server.cluster->slots_keys_count));
        // 备份后为当前数据库创建新的映射表
        server.cluster->slots_to_keys = raxNew();
        memset(server.cluster->slots_keys_count, 0,
            sizeof(server.cluster->slots_keys_count));
    }

    // 通知module复制备份事件。
    moduleFireServerEvent(REDISMODULE_EVENT_REPL_BACKUP,
                          REDISMODULE_SUBEVENT_REPL_BACKUP_CREATE,
                          NULL);

    return backup;
}

/* Discard a previously created backup, this can be slow (similar to FLUSHALL)
 * Arguments are similar to the ones of emptyDb, see EMPTYDB_ flags. */
void discardDbBackup(dbBackup *buckup, int flags, void(callback)(void*)) {
    int async = (flags & EMPTYDB_ASYNC);

    /* Release main DBs backup . */
    emptyDbStructure(buckup->dbarray, -1, async, callback);
    for (int i=0; i<server.dbnum; i++) {
        dictRelease(buckup->dbarray[i].dict);
        dictRelease(buckup->dbarray[i].expires);
    }

    /* Release slots to keys map backup if enable cluster. */
    if (server.cluster_enabled) freeSlotsToKeysMap(buckup->slots_to_keys, async);

    /* Release buckup. */
    zfree(buckup->dbarray);
    zfree(buckup);

    moduleFireServerEvent(REDISMODULE_EVENT_REPL_BACKUP,
                          REDISMODULE_SUBEVENT_REPL_BACKUP_DISCARD,
                          NULL);
}

/* Restore the previously created backup (discarding what currently resides
 * in the db).
 * This function should be called after the current contents of the database
 * was emptied with a previous call to emptyDb (possibly using the async mode). */
void restoreDbBackup(dbBackup *buckup) {
    /* Restore main DBs. */
    for (int i=0; i<server.dbnum; i++) {
        serverAssert(dictSize(server.db[i].dict) == 0);
        serverAssert(dictSize(server.db[i].expires) == 0);
        dictRelease(server.db[i].dict);
        dictRelease(server.db[i].expires);
        server.db[i] = buckup->dbarray[i];
    }

    /* Restore slots to keys map backup if enable cluster. */
    if (server.cluster_enabled) {
        serverAssert(server.cluster->slots_to_keys->numele == 0);
        raxFree(server.cluster->slots_to_keys);
        server.cluster->slots_to_keys = buckup->slots_to_keys;
        memcpy(server.cluster->slots_keys_count, buckup->slots_keys_count,
                sizeof(server.cluster->slots_keys_count));
    }

    /* Release buckup. */
    zfree(buckup->dbarray);
    zfree(buckup);

    moduleFireServerEvent(REDISMODULE_EVENT_REPL_BACKUP,
                          REDISMODULE_SUBEVENT_REPL_BACKUP_RESTORE,
                          NULL);
}

int selectDb(client *c, int id) {
    if (id < 0 || id >= server.dbnum)
        return C_ERR;
    c->db = &server.db[id];
    return C_OK;
}

// 计算所有db的总的key数量。
long long dbTotalServerKeyCount() {
    long long total = 0;
    int j;
    // 遍历db，计数
    for (j = 0; j < server.dbnum; j++) {
        total += dictSize(server.db[j].dict);
    }
    return total;
}

/*-----------------------------------------------------------------------------
 * Hooks for key space changes.
 *
 * Every time a key in the database is modified the function
 * signalModifiedKey() is called.
 *
 * Every time a DB is flushed the function signalFlushDb() is called.
 *----------------------------------------------------------------------------*/
// key空间发生改变时，做一些hook操作。每次key发生变化时会调用signalModifiedKey()。每次flush DB时会调用signalFlushDb()。

/* Note that the 'c' argument may be NULL if the key was modified out of
 * a context of a client. */
// 注意这里如果key变更不是在一个client上下文的时候，参数c可能为为NULL。
void signalModifiedKey(client *c, redisDb *db, robj *key) {
    // watch该key的client事务标识置为dirty状态。
    touchWatchedKey(db,key);
    // 追踪的key需要删除，并通知client缓存key无效了。
    trackingInvalidateKey(c,key);
}

void signalFlushedDb(int dbid, int async) {
    // 针对dbid为-1或其他值时统一处理
    int startdb, enddb;
    if (dbid == -1) {
        startdb = 0;
        enddb = server.dbnum-1;
    } else {
        startdb = enddb = dbid;
    }

    for (int j = startdb; j <= enddb; j++) {
        // 处理每个db中的watch keys。
        touchAllWatchedKeysInDb(&server.db[j], NULL);
    }

    // 处理client track相关数据
    trackingInvalidateKeysOnFlush(async);
}

/*-----------------------------------------------------------------------------
 * Type agnostic commands operating on the key space
 *----------------------------------------------------------------------------*/

/* Return the set of flags to use for the emptyDb() call for FLUSHALL
 * and FLUSHDB commands.
 *
 * sync: flushes the database in an sync manner.
 * async: flushes the database in an async manner.
 * no option: determine sync or async according to the value of lazyfree-lazy-user-flush.
 *
 * On success C_OK is returned and the flags are stored in *flags, otherwise
 * C_ERR is returned and the function sends an error to the client. */
int getFlushCommandFlags(client *c, int *flags) {
    /* Parse the optional ASYNC option. */
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"sync")) {
        *flags = EMPTYDB_NO_FLAGS;
    } else if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"async")) {
        *flags = EMPTYDB_ASYNC;
    } else if (c->argc == 1) {
        *flags = server.lazyfree_lazy_user_flush ? EMPTYDB_ASYNC : EMPTYDB_NO_FLAGS;
    } else {
        addReplyErrorObject(c,shared.syntaxerr);
        return C_ERR;
    }
    return C_OK;
}

/* Flushes the whole server data set. */
void flushAllDataAndResetRDB(int flags) {
    server.dirty += emptyDb(-1,flags,NULL);
    if (server.child_type == CHILD_TYPE_RDB) killRDBChild();
    if (server.saveparamslen > 0) {
        /* Normally rdbSave() will reset dirty, but we don't want this here
         * as otherwise FLUSHALL will not be replicated nor put into the AOF. */
        int saved_dirty = server.dirty;
        rdbSaveInfo rsi, *rsiptr;
        rsiptr = rdbPopulateSaveInfo(&rsi);
        rdbSave(server.rdb_filename,rsiptr);
        server.dirty = saved_dirty;
    }

    /* Without that extra dirty++, when db was already empty, FLUSHALL will
     * not be replicated nor put into the AOF. */
    server.dirty++;
#if defined(USE_JEMALLOC)
    /* jemalloc 5 doesn't release pages back to the OS when there's no traffic.
     * for large databases, flushdb blocks for long anyway, so a bit more won't
     * harm and this way the flush and purge will be synchroneus. */
    if (!(flags & EMPTYDB_ASYNC))
        jemalloc_purge();
#endif
}

/* FLUSHDB [ASYNC]
 *
 * Flushes the currently SELECTed Redis DB. */
void flushdbCommand(client *c) {
    int flags;

    if (getFlushCommandFlags(c,&flags) == C_ERR) return;
    server.dirty += emptyDb(c->db->id,flags,NULL);
    addReply(c,shared.ok);
#if defined(USE_JEMALLOC)
    /* jemalloc 5 doesn't release pages back to the OS when there's no traffic.
     * for large databases, flushdb blocks for long anyway, so a bit more won't
     * harm and this way the flush and purge will be synchroneus. */
    if (!(flags & EMPTYDB_ASYNC))
        jemalloc_purge();
#endif
}

/* FLUSHALL [ASYNC]
 *
 * Flushes the whole server data set. */
void flushallCommand(client *c) {
    int flags;
    if (getFlushCommandFlags(c,&flags) == C_ERR) return;
    flushAllDataAndResetRDB(flags);
    addReply(c,shared.ok);
}

/* This command implements DEL and LAZYDEL. */
void delGenericCommand(client *c, int lazy) {
    int numdel = 0, j;

    for (j = 1; j < c->argc; j++) {
        expireIfNeeded(c->db,c->argv[j]);
        int deleted  = lazy ? dbAsyncDelete(c->db,c->argv[j]) :
                              dbSyncDelete(c->db,c->argv[j]);
        if (deleted) {
            signalModifiedKey(c,c->db,c->argv[j]);
            notifyKeyspaceEvent(NOTIFY_GENERIC,
                "del",c->argv[j],c->db->id);
            server.dirty++;
            numdel++;
        }
    }
    addReplyLongLong(c,numdel);
}

void delCommand(client *c) {
    delGenericCommand(c,server.lazyfree_lazy_user_del);
}

void unlinkCommand(client *c) {
    delGenericCommand(c,1);
}

/* EXISTS key1 key2 ... key_N.
 * Return value is the number of keys existing. */
void existsCommand(client *c) {
    long long count = 0;
    int j;

    for (j = 1; j < c->argc; j++) {
        if (lookupKeyReadWithFlags(c->db,c->argv[j],LOOKUP_NOTOUCH)) count++;
    }
    addReplyLongLong(c,count);
}

void selectCommand(client *c) {
    int id;

    if (getIntFromObjectOrReply(c, c->argv[1], &id, NULL) != C_OK)
        return;

    if (server.cluster_enabled && id != 0) {
        addReplyError(c,"SELECT is not allowed in cluster mode");
        return;
    }
    if (selectDb(c,id) == C_ERR) {
        addReplyError(c,"DB index is out of range");
    } else {
        addReply(c,shared.ok);
    }
}

void randomkeyCommand(client *c) {
    robj *key;

    if ((key = dbRandomKey(c->db)) == NULL) {
        addReplyNull(c);
        return;
    }

    addReplyBulk(c,key);
    decrRefCount(key);
}

void keysCommand(client *c) {
    dictIterator *di;
    dictEntry *de;
    sds pattern = c->argv[1]->ptr;
    int plen = sdslen(pattern), allkeys;
    unsigned long numkeys = 0;
    void *replylen = addReplyDeferredLen(c);

    di = dictGetSafeIterator(c->db->dict);
    allkeys = (pattern[0] == '*' && plen == 1);
    while((de = dictNext(di)) != NULL) {
        sds key = dictGetKey(de);
        robj *keyobj;

        if (allkeys || stringmatchlen(pattern,plen,key,sdslen(key),0)) {
            keyobj = createStringObject(key,sdslen(key));
            if (!keyIsExpired(c->db,keyobj)) {
                addReplyBulk(c,keyobj);
                numkeys++;
            }
            decrRefCount(keyobj);
        }
    }
    dictReleaseIterator(di);
    setDeferredArrayLen(c,replylen,numkeys);
}

/* This callback is used by scanGenericCommand in order to collect elements
 * returned by the dictionary iterator into a list. */
// 这个回调函数用于scanGenericCommand中，当对hash表进行dictScan时，会调用这个回调函数来收集当前遍历到的元素，用于返回给client。
void scanCallback(void *privdata, const dictEntry *de) {
    void **pd = (void**) privdata;
    // 存储迭代元素的列表
    list *keys = pd[0];
    // 当前迭代对象，o为NULL表示我们在当前db dict上迭代所有keys。
    robj *o = pd[1];
    robj *key, *val = NULL;

    // 根据o的不同类型，来获取迭代到的元素。注意hash和zset对象，还需要获取value值。
    if (o == NULL) {
        sds sdskey = dictGetKey(de);
        key = createStringObject(sdskey, sdslen(sdskey));
    } else if (o->type == OBJ_SET) {
        sds keysds = dictGetKey(de);
        key = createStringObject(keysds,sdslen(keysds));
    } else if (o->type == OBJ_HASH) {
        sds sdskey = dictGetKey(de);
        sds sdsval = dictGetVal(de);
        key = createStringObject(sdskey,sdslen(sdskey));
        val = createStringObject(sdsval,sdslen(sdsval));
    } else if (o->type == OBJ_ZSET) {
        sds sdskey = dictGetKey(de);
        key = createStringObject(sdskey,sdslen(sdskey));
        val = createStringObjectFromLongDouble(*(double*)dictGetVal(de),0);
    } else {
        serverPanic("Type not handled in SCAN callback.");
    }

    // 将key和value对象加入到结果集keys列表中。
    listAddNodeTail(keys, key);
    if (val) listAddNodeTail(keys, val);
}

/* Try to parse a SCAN cursor stored at object 'o':
 * if the cursor is valid, store it as unsigned integer into *cursor and
 * returns C_OK. Otherwise return C_ERR and send an error to the
 * client. */
int parseScanCursorOrReply(client *c, robj *o, unsigned long *cursor) {
    char *eptr;

    /* Use strtoul() because we need an *unsigned* long, so
     * getLongLongFromObject() does not cover the whole cursor space. */
    errno = 0;
    *cursor = strtoul(o->ptr, &eptr, 10);
    if (isspace(((char*)o->ptr)[0]) || eptr[0] != '\0' || errno == ERANGE)
    {
        addReplyError(c, "invalid cursor");
        return C_ERR;
    }
    return C_OK;
}

/* This command implements SCAN, HSCAN and SSCAN commands.
 * If object 'o' is passed, then it must be a Hash, Set or Zset object, otherwise
 * if 'o' is NULL the command will operate on the dictionary associated with
 * the current database.
 *
 * When 'o' is not NULL the function assumes that the first argument in
 * the client arguments vector is a key so it skips it before iterating
 * in order to parse options.
 *
 * In the case of a Hash object the function returns both the field and value
 * of every element on the Hash. */
// scan、hscan、sscan、zscan命令的统一实现。如果传入了o参数，则它必定是hash、set或zset，否则o传入NULL表示scan整个数据库db。
// 如果传入的o非NULL，则我们假定client参数向量的第一个参数为key，所以迭代前会跳过它解析后面的选项参数。
// 如果是hash对象，函数会返回hash对象的每一个元素的key和value。
void scanGenericCommand(client *c, robj *o, unsigned long cursor) {
    int i, j;
    list *keys = listCreate();
    listNode *node, *nextnode;
    long count = 10;
    sds pat = NULL;
    sds typename = NULL;
    int patlen = 0, use_pattern = 0;
    dict *ht;

    /* Object must be NULL (to iterate keys names), or the type of the object
     * must be Set, Sorted Set, or Hash. */
    // o要么为空，要么该对象的type是hash、set或zset。
    serverAssert(o == NULL || o->type == OBJ_SET || o->type == OBJ_HASH ||
                o->type == OBJ_ZSET);

    /* Set i to the first option argument. The previous one is the cursor. */
    // i是第一个选项参数的index。0是命令，1是key，2是cursor参数，后面都是选项参数。
    // 当o为NULL时，命令没有key，所以i=2；当o非NULL时，我们要跳过key，所i=3。
    i = (o == NULL) ? 2 : 3; /* Skip the key argument if needed. */

    /* Step 1: Parse options. */
    // 第一步：解析选项参数
    while (i < c->argc) {
        j = c->argc - i;
        if (!strcasecmp(c->argv[i]->ptr, "count") && j >= 2) {
            // 解析count参数
            if (getLongFromObjectOrReply(c, c->argv[i+1], &count, NULL)
                != C_OK)
            {
                goto cleanup;
            }

            if (count < 1) {
                addReplyErrorObject(c,shared.syntaxerr);
                goto cleanup;
            }

            i += 2;
        } else if (!strcasecmp(c->argv[i]->ptr, "match") && j >= 2) {
            // 解析match的pattern参数
            pat = c->argv[i+1]->ptr;
            patlen = sdslen(pat);

            /* The pattern always matches if it is exactly "*", so it is
             * equivalent to disabling it. */
            // 如果参数是'*'，则该模式总是匹配，则相当于不使用match。
            use_pattern = !(pat[0] == '*' && patlen == 1);

            i += 2;
        } else if (!strcasecmp(c->argv[i]->ptr, "type") && o == NULL && j >= 2) {
            // 解析type参数，扫描指定的类型。该参数只用于SCAN命令，扫描数据库db。
            /* SCAN for a particular type only applies to the db dict */
            typename = c->argv[i+1]->ptr;
            i+= 2;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            goto cleanup;
        }
    }

    /* Step 2: Iterate the collection.
     *
     * Note that if the object is encoded with a ziplist, intset, or any other
     * representation that is not a hash table, we are sure that it is also
     * composed of a small number of elements. So to avoid taking state we
     * just return everything inside the object in a single call, setting the
     * cursor to zero to signal the end of the iteration. */

    /* Handle the case of a hash table. */
    // 第二步：迭代集合。
    // 如果对象是ziplist、intset或者其他不是hash表的编码表现形式，我们确信它包含的元素很少，
    // 所以避免保存底层结构的迭代状态，我们单次调用返回该对象里的所有元素，并设置cursor为0表示迭代结束。
    ht = NULL;
    // 先根据对象o的类型及编码，来确定我们迭代的底层hash表。对于hash和zset类型对象，我们迭代返回k-v对，其他类型我们只返回key。
    if (o == NULL) {
        ht = c->db->dict;
    } else if (o->type == OBJ_SET && o->encoding == OBJ_ENCODING_HT) {
        ht = o->ptr;
    } else if (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT) {
        ht = o->ptr;
        count *= 2; /* We return key / value for this type. */
    } else if (o->type == OBJ_ZSET && o->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = o->ptr;
        ht = zs->dict;
        count *= 2; /* We return key / value for this type. */
    }

    // ht非空则表示底层是hash表存储。而ht为空表示底层intset或ziplist结构。
    if (ht) {
        void *privdata[2];
        /* We set the max number of iterations to ten times the specified
         * COUNT, so if the hash table is in a pathological state (very
         * sparsely populated) we avoid to block too much time at the cost
         * of returning no or very few elements. */
        // 这里将最大迭代次数设置为10倍的count，从而当hash表很稀疏时，避免阻塞太长时间但最终却只返回很少的数据。
        // 我们每次遍历一个bulket，当数据稀疏时大多bulket为NULL，可能需要遍历很多bulket才能得到count个数据，所以这里设置单次查询bulket上限。
        long maxiterations = count*10;

        /* We pass two pointers to the callback: the list to which it will
         * add new elements, and the object containing the dictionary so that
         * it is possible to fetch more data in a type-dependent way. */
        // 我们传递了2个指针给callback函数：一个是keys列表，用于存储我们当前迭代到的元素；一个当前scan的对象，因为需要类型信息来确定处理方式。
        privdata[0] = keys;
        privdata[1] = o;
        do {
            // 调用dictScan遍历。ht为遍历的hash表；cursor为当前遍历的游标；
            // scanCallback为回调函数，这里注意处理暂存迭代到的元素数据；privdata为传入回调函数的参数。
            cursor = dictScan(ht, cursor, scanCallback, NULL, privdata);
            // 当3个条件任意一个满足时退出循环：
            //  1、hash表scan完成（cursor为0）；2、达到最大迭代次数maxiterations；3、获取到需要的count个keys。
        } while (cursor &&
              maxiterations-- &&
              listLength(keys) < (unsigned long)count);
    } else if (o->type == OBJ_SET) {
        int pos = 0;
        int64_t ll;

        // 如果是set对象，执行到这里说明不是hash表存储，那目前只有intset了。遍历intset中所有的元素，加入到keys列表中。
        while(intsetGet(o->ptr,pos++,&ll))
            listAddNodeTail(keys,createStringObjectFromLongLong(ll));
        cursor = 0;
    } else if (o->type == OBJ_HASH || o->type == OBJ_ZSET) {
        // 如果是hash或zset对象，执行到这里说明不是hash表存储，那只能是ziplist。遍历ziplist中所有entry，将k、v分别加入结果列表。
        // 获取ziplist数据部分的首地址，即第一个entry地址，从这往后遍历。
        unsigned char *p = ziplistIndex(o->ptr,0);
        unsigned char *vstr;
        unsigned int vlen;
        long long vll;

        // 要么本来ziplist就是空的，我们不进入循环；要么当循环中ziplistNext返回NULL时表示ziplist遍历结束了，退出循环。
        while(p) {
            // 获取p指向的entry数据
            ziplistGet(p,&vstr,&vlen,&vll);
            // 根据vstr是否为NULL来判断返回的数据是字符串还是数字，从而不同方式创建对象加入到返回结果列表。
            listAddNodeTail(keys,
                (vstr != NULL) ? createStringObject((char*)vstr,vlen) :
                                 createStringObjectFromLongLong(vll));
            p = ziplistNext(o->ptr,p);
        }
        cursor = 0;
    } else {
        serverPanic("Not handled encoding in SCAN.");
    }

    /* Step 3: Filter elements. */
    // 第三步：过滤元素，遍历keys列表处理。
    node = listFirst(keys);
    while (node) {
        robj *kobj = listNodeValue(node);
        nextnode = listNextNode(node);
        int filter = 0;

        /* Filter element if it does not match the pattern. */
        // 如果使用pattern，则需要过滤掉不匹配pattern的key（模式匹配的是key，不是value）。
        if (use_pattern) {
            if (sdsEncodedObject(kobj)) {
                // 字符串编码，直接match处理，不匹配则需要过滤
                if (!stringmatchlen(pat, patlen, kobj->ptr, sdslen(kobj->ptr), 0))
                    filter = 1;
            } else {
                char buf[LONG_STR_SIZE];
                int len;

                // 数字编码，需要先转成字符串形式，然后在match处理，不匹配模式的需要过滤。
                serverAssert(kobj->encoding == OBJ_ENCODING_INT);
                len = ll2string(buf,sizeof(buf),(long)kobj->ptr);
                if (!stringmatchlen(pat, patlen, buf, len, 0)) filter = 1;
            }
        }

        /* Filter an element if it isn't the type we want. */
        // 过滤我们不想要的类型对象。type参数只用于在db dict进行SCAN。
        if (!filter && o == NULL && typename){
            // 根据key获取value对象，查看value对象是否是我们指定的类型，不是则需要过滤
            robj* typecheck = lookupKeyReadWithFlags(c->db, kobj, LOOKUP_NOTOUCH);
            char* type = getObjectTypeName(typecheck);
            if (strcasecmp((char*) typename, type)) filter = 1;
        }

        /* Filter element if it is an expired key. */
        // 过滤掉已过期的key。在对db dict进行SCAN时，这里会有处理过期。如果key过期了，则需要过滤掉。
        if (!filter && o == NULL && expireIfNeeded(c->db, kobj)) filter = 1;

        /* Remove the element and its associated value if needed. */
        // 如果key需要过滤，则我们从keys中移除对应的节点。
        if (filter) {
            decrRefCount(kobj);
            listDelNode(keys, node);
        }

        /* If this is a hash or a sorted set, we have a flat list of
         * key-value elements, so if this element was filtered, remove the
         * value, or skip it if it was not filtered: we only match keys. */
        // 注意对于hash或zset对象，我们keys列表中既有key也有value。
        // 所以如果key被过滤了，这里我们也要从列表移除value；而如果key没被过滤，我们需要跳过value，因为我们只处理key的匹配过滤。
        if (o && (o->type == OBJ_ZSET || o->type == OBJ_HASH)) {
            node = nextnode;
            serverAssert(node); /* assertion for valgrind (avoid NPD) */
            // 获取下一个key的node
            nextnode = listNextNode(node);
            // 如果需要，移除当前value node。
            if (filter) {
                kobj = listNodeValue(node);
                decrRefCount(kobj);
                listDelNode(keys, node);
            }
        }
        node = nextnode;
    }

    /* Step 4: Reply to the client. */
    // 第四步：回复数据给client。到这里，keys列表中数据都是需要回复给client的，前面该过滤的已经过滤了。
    // 回复是2个元素的数组，第一个元素是下一个scan的cursor，第二个元素是一个数组（keys所有元素）。
    addReplyArrayLen(c, 2);
    addReplyBulkLongLong(c,cursor);

    // 第二个元素，数组长度为keys列表的长度，数组元素为我们遍历keys列表依次回复给client。
    addReplyArrayLen(c, listLength(keys));
    while ((node = listFirst(keys)) != NULL) {
        robj *kobj = listNodeValue(node);
        addReplyBulk(c, kobj);
        decrRefCount(kobj);
        // 从keys列表删除对应的key节点
        listDelNode(keys, node);
    }

cleanup:
    // 释放keys列表
    listSetFreeMethod(keys,decrRefCountVoid);
    listRelease(keys);
}

/* The SCAN command completely relies on scanGenericCommand. */
void scanCommand(client *c) {
    unsigned long cursor;
    if (parseScanCursorOrReply(c,c->argv[1],&cursor) == C_ERR) return;
    scanGenericCommand(c,NULL,cursor);
}

void dbsizeCommand(client *c) {
    addReplyLongLong(c,dictSize(c->db->dict));
}

void lastsaveCommand(client *c) {
    addReplyLongLong(c,server.lastsave);
}

char* getObjectTypeName(robj *o) {
    char* type;
    if (o == NULL) {
        type = "none";
    } else {
        switch(o->type) {
        case OBJ_STRING: type = "string"; break;
        case OBJ_LIST: type = "list"; break;
        case OBJ_SET: type = "set"; break;
        case OBJ_ZSET: type = "zset"; break;
        case OBJ_HASH: type = "hash"; break;
        case OBJ_STREAM: type = "stream"; break;
        case OBJ_MODULE: {
            moduleValue *mv = o->ptr;
            type = mv->type->name;
        }; break;
        default: type = "unknown"; break;
        }
    }
    return type;
}

void typeCommand(client *c) {
    robj *o;
    o = lookupKeyReadWithFlags(c->db,c->argv[1],LOOKUP_NOTOUCH);
    addReplyStatus(c, getObjectTypeName(o));
}

void shutdownCommand(client *c) {
    int flags = 0;

    if (c->argc > 2) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    } else if (c->argc == 2) {
        if (!strcasecmp(c->argv[1]->ptr,"nosave")) {
            flags |= SHUTDOWN_NOSAVE;
        } else if (!strcasecmp(c->argv[1]->ptr,"save")) {
            flags |= SHUTDOWN_SAVE;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }
    if (prepareForShutdown(flags) == C_OK) exit(0);
    addReplyError(c,"Errors trying to SHUTDOWN. Check logs.");
}

void renameGenericCommand(client *c, int nx) {
    robj *o;
    long long expire;
    int samekey = 0;

    /* When source and dest key is the same, no operation is performed,
     * if the key exists, however we still return an error on unexisting key. */
    if (sdscmp(c->argv[1]->ptr,c->argv[2]->ptr) == 0) samekey = 1;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr)) == NULL)
        return;

    if (samekey) {
        addReply(c,nx ? shared.czero : shared.ok);
        return;
    }

    incrRefCount(o);
    expire = getExpire(c->db,c->argv[1]);
    if (lookupKeyWrite(c->db,c->argv[2]) != NULL) {
        if (nx) {
            decrRefCount(o);
            addReply(c,shared.czero);
            return;
        }
        /* Overwrite: delete the old key before creating the new one
         * with the same name. */
        dbDelete(c->db,c->argv[2]);
    }
    dbAdd(c->db,c->argv[2],o);
    if (expire != -1) setExpire(c,c->db,c->argv[2],expire);
    dbDelete(c->db,c->argv[1]);
    signalModifiedKey(c,c->db,c->argv[1]);
    signalModifiedKey(c,c->db,c->argv[2]);
    notifyKeyspaceEvent(NOTIFY_GENERIC,"rename_from",
        c->argv[1],c->db->id);
    notifyKeyspaceEvent(NOTIFY_GENERIC,"rename_to",
        c->argv[2],c->db->id);
    server.dirty++;
    addReply(c,nx ? shared.cone : shared.ok);
}

void renameCommand(client *c) {
    renameGenericCommand(c,0);
}

void renamenxCommand(client *c) {
    renameGenericCommand(c,1);
}

void moveCommand(client *c) {
    robj *o;
    redisDb *src, *dst;
    int srcid, dbid;
    long long expire;

    if (server.cluster_enabled) {
        addReplyError(c,"MOVE is not allowed in cluster mode");
        return;
    }

    /* Obtain source and target DB pointers */
    src = c->db;
    srcid = c->db->id;

    if (getIntFromObjectOrReply(c, c->argv[2], &dbid, NULL) != C_OK)
        return;

    if (selectDb(c,dbid) == C_ERR) {
        addReplyError(c,"DB index is out of range");
        return;
    }
    dst = c->db;
    selectDb(c,srcid); /* Back to the source DB */

    /* If the user is moving using as target the same
     * DB as the source DB it is probably an error. */
    if (src == dst) {
        addReplyErrorObject(c,shared.sameobjecterr);
        return;
    }

    /* Check if the element exists and get a reference */
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (!o) {
        addReply(c,shared.czero);
        return;
    }
    expire = getExpire(c->db,c->argv[1]);

    /* Return zero if the key already exists in the target DB */
    if (lookupKeyWrite(dst,c->argv[1]) != NULL) {
        addReply(c,shared.czero);
        return;
    }
    dbAdd(dst,c->argv[1],o);
    if (expire != -1) setExpire(c,dst,c->argv[1],expire);
    incrRefCount(o);

    /* OK! key moved, free the entry in the source DB */
    dbDelete(src,c->argv[1]);
    signalModifiedKey(c,src,c->argv[1]);
    signalModifiedKey(c,dst,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_GENERIC,
                "move_from",c->argv[1],src->id);
    notifyKeyspaceEvent(NOTIFY_GENERIC,
                "move_to",c->argv[1],dst->id);

    server.dirty++;
    addReply(c,shared.cone);
}

void copyCommand(client *c) {
    robj *o;
    redisDb *src, *dst;
    int srcid, dbid;
    long long expire;
    int j, replace = 0, delete = 0;

    /* Obtain source and target DB pointers 
     * Default target DB is the same as the source DB 
     * Parse the REPLACE option and targetDB option. */
    src = c->db;
    dst = c->db;
    srcid = c->db->id;
    dbid = c->db->id;
    for (j = 3; j < c->argc; j++) {
        int additional = c->argc - j - 1;
        if (!strcasecmp(c->argv[j]->ptr,"replace")) {
            replace = 1;
        } else if (!strcasecmp(c->argv[j]->ptr, "db") && additional >= 1) {
            if (getIntFromObjectOrReply(c, c->argv[j+1], &dbid, NULL) != C_OK)
                return;

            if (selectDb(c, dbid) == C_ERR) {
                addReplyError(c,"DB index is out of range");
                return;
            }
            dst = c->db;
            selectDb(c,srcid); /* Back to the source DB */
            j++; /* Consume additional arg. */
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    if ((server.cluster_enabled == 1) && (srcid != 0 || dbid != 0)) {
        addReplyError(c,"Copying to another database is not allowed in cluster mode");
        return;
    }

    /* If the user select the same DB as
     * the source DB and using newkey as the same key
     * it is probably an error. */
    robj *key = c->argv[1];
    robj *newkey = c->argv[2];
    if (src == dst && (sdscmp(key->ptr, newkey->ptr) == 0)) {
        addReplyErrorObject(c,shared.sameobjecterr);
        return;
    }

    /* Check if the element exists and get a reference */
    o = lookupKeyWrite(c->db, key);
    if (!o) {
        addReply(c,shared.czero);
        return;
    }
    expire = getExpire(c->db,key);

    /* Return zero if the key already exists in the target DB. 
     * If REPLACE option is selected, delete newkey from targetDB. */
    if (lookupKeyWrite(dst,newkey) != NULL) {
        if (replace) {
            delete = 1;
        } else {
            addReply(c,shared.czero);
            return;
        }
    }

    /* Duplicate object according to object's type. */
    robj *newobj;
    switch(o->type) {
        case OBJ_STRING: newobj = dupStringObject(o); break;
        case OBJ_LIST: newobj = listTypeDup(o); break;
        case OBJ_SET: newobj = setTypeDup(o); break;
        case OBJ_ZSET: newobj = zsetDup(o); break;
        case OBJ_HASH: newobj = hashTypeDup(o); break;
        case OBJ_STREAM: newobj = streamDup(o); break;
        case OBJ_MODULE:
            newobj = moduleTypeDupOrReply(c, key, newkey, o);
            if (!newobj) return;
            break;
        default:
            addReplyError(c, "unknown type object");
            return;
    }

    if (delete) {
        dbDelete(dst,newkey);
    }

    dbAdd(dst,newkey,newobj);
    if (expire != -1) setExpire(c, dst, newkey, expire);

    /* OK! key copied */
    signalModifiedKey(c,dst,c->argv[2]);
    notifyKeyspaceEvent(NOTIFY_GENERIC,"copy_to",c->argv[2],dst->id);

    server.dirty++;
    addReply(c,shared.cone);
}

/* Helper function for dbSwapDatabases(): scans the list of keys that have
 * one or more blocked clients for B[LR]POP or other blocking commands
 * and signal the keys as ready if they are of the right type. See the comment
 * where the function is used for more info. */
void scanDatabaseForReadyLists(redisDb *db) {
    dictEntry *de;
    dictIterator *di = dictGetSafeIterator(db->blocking_keys);
    while((de = dictNext(di)) != NULL) {
        robj *key = dictGetKey(de);
        robj *value = lookupKey(db,key,LOOKUP_NOTOUCH);
        if (value) signalKeyAsReady(db, key, value->type);
    }
    dictReleaseIterator(di);
}

/* Swap two databases at runtime so that all clients will magically see
 * the new database even if already connected. Note that the client
 * structure c->db points to a given DB, so we need to be smarter and
 * swap the underlying referenced structures, otherwise we would need
 * to fix all the references to the Redis DB structure.
 *
 * Returns C_ERR if at least one of the DB ids are out of range, otherwise
 * C_OK is returned. */
int dbSwapDatabases(int id1, int id2) {
    if (id1 < 0 || id1 >= server.dbnum ||
        id2 < 0 || id2 >= server.dbnum) return C_ERR;
    if (id1 == id2) return C_OK;
    redisDb aux = server.db[id1];
    redisDb *db1 = &server.db[id1], *db2 = &server.db[id2];

    /* Swap hash tables. Note that we don't swap blocking_keys,
     * ready_keys and watched_keys, since we want clients to
     * remain in the same DB they were. */
    db1->dict = db2->dict;
    db1->expires = db2->expires;
    db1->avg_ttl = db2->avg_ttl;
    db1->expires_cursor = db2->expires_cursor;

    db2->dict = aux.dict;
    db2->expires = aux.expires;
    db2->avg_ttl = aux.avg_ttl;
    db2->expires_cursor = aux.expires_cursor;

    /* Now we need to handle clients blocked on lists: as an effect
     * of swapping the two DBs, a client that was waiting for list
     * X in a given DB, may now actually be unblocked if X happens
     * to exist in the new version of the DB, after the swap.
     *
     * However normally we only do this check for efficiency reasons
     * in dbAdd() when a list is created. So here we need to rescan
     * the list of clients blocked on lists and signal lists as ready
     * if needed.
     *
     * Also the swapdb should make transaction fail if there is any
     * client watching keys */
    scanDatabaseForReadyLists(db1);
    touchAllWatchedKeysInDb(db1, db2);
    scanDatabaseForReadyLists(db2);
    touchAllWatchedKeysInDb(db2, db1);
    return C_OK;
}

/* SWAPDB db1 db2 */
void swapdbCommand(client *c) {
    int id1, id2;

    /* Not allowed in cluster mode: we have just DB 0 there. */
    if (server.cluster_enabled) {
        addReplyError(c,"SWAPDB is not allowed in cluster mode");
        return;
    }

    /* Get the two DBs indexes. */
    if (getIntFromObjectOrReply(c, c->argv[1], &id1,
        "invalid first DB index") != C_OK)
        return;

    if (getIntFromObjectOrReply(c, c->argv[2], &id2,
        "invalid second DB index") != C_OK)
        return;

    /* Swap... */
    if (dbSwapDatabases(id1,id2) == C_ERR) {
        addReplyError(c,"DB index is out of range");
        return;
    } else {
        RedisModuleSwapDbInfo si = {REDISMODULE_SWAPDBINFO_VERSION,id1,id2};
        moduleFireServerEvent(REDISMODULE_EVENT_SWAPDB,0,&si);
        server.dirty++;
        addReply(c,shared.ok);
    }
}

/*-----------------------------------------------------------------------------
 * Expires API
 *----------------------------------------------------------------------------*/

int removeExpire(redisDb *db, robj *key) {
    /* An expire may only be removed if there is a corresponding entry in the
     * main dict. Otherwise, the key will never be freed. */
    // 只有在数据dict中有一致的key时，我们才移除该key的过期时间，否则将永远不会移除。
    serverAssertWithInfo(NULL,key,dictFind(db->dict,key->ptr) != NULL);
    // expires dict中删除key对应项。
    return dictDelete(db->expires,key->ptr) == DICT_OK;
}

/* Set an expire to the specified key. If the expire is set in the context
 * of an user calling a command 'c' is the client, otherwise 'c' is set
 * to NULL. The 'when' parameter is the absolute unix time in milliseconds
 * after which the key will no longer be considered valid. */
// 为指定的key设置过期时间。如果是在client上下文设置key的过期时间，则c传入该client，否则c传NULL。
// when参数是绝对的unix毫秒时间，在这个时间点之后key将不在有效。
void setExpire(client *c, redisDb *db, robj *key, long long when) {
    dictEntry *kde, *de;

    /* Reuse the sds from the main dict in the expire dict */
    // 我们这里key使用的是数据dict中的key。所以需要保持这两个db key一致，否则会导致expire中数据永远不会删除。
    kde = dictFind(db->dict,key->ptr);
    serverAssertWithInfo(NULL,key,kde != NULL);
    // 查询或添加entry，如果是add则只有key数据，需要后面填充。
    de = dictAddOrFind(db->expires,dictGetKey(kde));
    // 填充或更新value
    dictSetSignedIntegerVal(de,when);

    // 是slave且不是read only，表示可写的slave。
    int writable_slave = server.masterhost && server.repl_slave_ro == 0;
    // 如果是可写的slave，且当前设置过期命令不是master传过来的，则我们需要在slaveKeysWithExpire中记录相关信息。
    if (c && writable_slave && !(c->flags & CLIENT_MASTER))
        rememberSlaveKeyWithExpire(db,key);
}

/* Return the expire time of the specified key, or -1 if no expire
 * is associated with this key (i.e. the key is non volatile) */
// 返回指定key的过期时间。如果key没有设置过期时间，则返回-1
long long getExpire(redisDb *db, robj *key) {
    dictEntry *de;

    /* No expire? return ASAP */
    // 没有过期时间，尽早返回。
    if (dictSize(db->expires) == 0 ||
       (de = dictFind(db->expires,key->ptr)) == NULL) return -1;

    /* The entry was found in the expire dict, this means it should also
     * be present in the main dict (safety check). */
    // 安全检查。key如果在expire字典中，那么它一定在主数据字典中。
    serverAssertWithInfo(NULL,key,dictFind(db->dict,key->ptr) != NULL);
    // 返回过期时间数字
    return dictGetSignedIntegerVal(de);
}

/* Propagate expires into slaves and the AOF file.
 * When a key expires in the master, a DEL operation for this key is sent
 * to all the slaves and the AOF file if enabled.
 *
 * This way the key expiry is centralized in one place, and since both
 * AOF and the master->slave link guarantee operation ordering, everything
 * will be consistent even if we allow write operations against expiring
 * keys. */
// 往salves和AOF传播key过期操作。
// 当master上一个key过期时，对于该key的一个DEL操作将发送到所有slaves和AOF（如果AOF启用）。
// 这样操作使得key的过期集中化的管理，并且AOF和slave的传播也保证了数据操作的顺序，一致性。
// 即使我们允许在过期key上进行写操作，也没有影响。
void propagateExpire(redisDb *db, robj *key, int lazy) {
    robj *argv[2];

    // 处理传播参数。key以及是否lazy删除
    argv[0] = lazy ? shared.unlink : shared.del;
    argv[1] = key;
    incrRefCount(argv[0]);
    incrRefCount(argv[1]);

    /* If the master decided to expire a key we must propagate it to replicas no matter what..
     * Even if module executed a command without asking for propagation. */
    // 如果master决定过期一个key，无论如何我们都必须要将操作传播到slave中。
    // 即使是在module中执行的命令且没有要求传播，我们也要对过期key进行传播。
    // 强制传播，先保存server当前值，然后强制replication_allowed置为1，传播完再还原。
    int prev_replication_allowed = server.replication_allowed;
    server.replication_allowed = 1;
    // 调用propagate传播函数进行处理
    propagate(server.delCommand,db->id,argv,2,PROPAGATE_AOF|PROPAGATE_REPL);
    server.replication_allowed = prev_replication_allowed;

    decrRefCount(argv[0]);
    decrRefCount(argv[1]);
}

/* Check if the key is expired. */
// 判断是否key已过期
int keyIsExpired(redisDb *db, robj *key) {
    // 获取db中该key的过期时间。
    mstime_t when = getExpire(db,key);
    mstime_t now;

    // when为-1，表示没有查到key对应的过期时间。可能key不存在，也可能key没设置过期时间。
    if (when < 0) return 0; /* No expire for this key */

    /* Don't expire anything while loading. It will be done later. */
    // 如果正在loading数据，不做任何过期处理。
    if (server.loading) return 0;

    /* If we are in the context of a Lua script, we pretend that time is
     * blocked to when the Lua script started. This way a key can expire
     * only the first time it is accessed and not in the middle of the
     * script execution, making propagation to slaves / AOF consistent.
     * See issue #1525 on Github for more information. */
    // 如果我们当前在执行Lua脚本，我们假装时间在Lua脚本开始执行时阻塞了（即使用脚本执行时的时间作为当前时间处理）。
    // 这样key只能在首次访问的时候处理过期，而不能在脚本执行期间过期，从而保持传播到slaves/AOF是一致。
    if (server.lua_caller) {
        now = server.lua_time_snapshot;
    }
    /* If we are in the middle of a command execution, we still want to use
     * a reference time that does not change: in that case we just use the
     * cached time, that we update before each call in the call() function.
     * This way we avoid that commands such as RPOPLPUSH or similar, that
     * may re-open the same key multiple times, can invalidate an already
     * open object in a next call, if the next call will see the key expired,
     * while the first did not. */
    // 如果我们在执行命令期间处理过期，我们希望整个命令执行期间时间都是不变的（执行call前更新时间，并缓存到server.mstime中）。
    // 因为对于RPOPLPUSH这类命令，会多次访问相同的key，可能第一次访问没过期，而第二次访问过期了，这样命令执行会有问题。
    // 所以我们在一个命令执行期间，使用相同的缓存时间处理。
    else if (server.fixed_time_expire > 0) {
        now = server.mstime;
    }
    /* For the other cases, we want to use the most fresh time we have. */
    // 其他情况，我们使用最新的时间处理。
    else {
        now = mstime();
    }

    /* The key expired if the current (virtual or real) time is greater
     * than the expire time of the key. */
    // 如果当前时间>key的过期时间，则表示key过期了。
    return now > when;
}

/* This function is called when we are going to perform some operation
 * in a given key, but such key may be already logically expired even if
 * it still exists in the database. The main way this function is called
 * is via lookupKey*() family of functions.
 *
 * The behavior of the function depends on the replication role of the
 * instance, because slave instances do not expire keys, they wait
 * for DELs from the master for consistency matters. However even
 * slaves will try to have a coherent return value for the function,
 * so that read commands executed in the slave side will be able to
 * behave like if the key is expired even if still present (because the
 * master has yet to propagate the DEL).
 *
 * In masters as a side effect of finding a key which is expired, such
 * key will be evicted from the database. Also this may trigger the
 * propagation of a DEL/UNLINK command in AOF / replication stream.
 *
 * The return value of the function is 0 if the key is still valid,
 * otherwise the function returns 1 if the key is expired. */
// 当对一个给定key执行某些操作时，会调用这个函数检查key是否过期（过期策略有一定的随机性，有可能key过期了但还在db中），过期则进行移除。
// 这个函数的行为还取决于实例的复制角色，slaves不会主动处理过期的key，而是通过master传播DELs命令来移除，从而与master保持一致性。
// 虽然slaves不会主动过期keys，但是如果查到key过期了，返回值还是与master一致，这样保证slave读取数据与master一致。
// 这个函数会有副作用，即查到key过期了会进行删除操作，进一步还会传播DEL/UNLINK命令到AOF/复制流。
// 函数返回0表示key没过期，仍然有效。否则返回1，表示key已过期，master会进行删除处理。
int expireIfNeeded(redisDb *db, robj *key) {
    // key没有过期，直接返回0
    if (!keyIsExpired(db,key)) return 0;

    /* If we are running in the context of a slave, instead of
     * evicting the expired key from the database, we return ASAP:
     * the slave key expiration is controlled by the master that will
     * send us synthesized DEL operations for expired keys.
     *
     * Still we try to return the right information to the caller,
     * that is, 0 if we think the key should be still valid, 1 if
     * we think the key is expired at this time. */
    // 如果当前节点是slave，不主动过期key，尽早返回，等待master传播DEL命令来进行删除操作。
    // 但是这里我们需要保证数据查询与master一致，所以我们返回1表示key过期了。
    if (server.masterhost != NULL) return 1;

    /* If clients are paused, we keep the current dataset constant,
     * but return to the client what we believe is the right state. Typically,
     * at the end of the pause we will properly expire the key OR we will
     * have failed over and the new primary will send us the expire. */
    // 如果client是暂停状态，我们需要保证当前db数据不变。所以也是直接返回1。
    // 等解除暂停时，要么我们是master后面会主动过期该key，要么我们故障转移被接管，新的master会通过DEL传播key过期移除处理。
    if (checkClientPauseTimeoutAndReturnIfPaused()) return 1;

    /* Delete the key */
    // 过期了，需要删除该key。如果配置了lazy_expire，则异步删除。
    if (server.lazyfree_lazy_expire) {
        dbAsyncDelete(db,key);
    } else {
        dbSyncDelete(db,key);
    }
    server.stat_expiredkeys++;
    // 针对该key的过期处理，传播unlink或del命令到aof/复制流。
    propagateExpire(db,key,server.lazyfree_lazy_expire);
    // 通知key变更处理
    notifyKeyspaceEvent(NOTIFY_EXPIRED,
        "expired",key,db->id);
    signalModifiedKey(NULL,db,key);
    return 1;
}

/* -----------------------------------------------------------------------------
 * API to get key arguments from commands
 * ---------------------------------------------------------------------------*/

/* Prepare the getKeysResult struct to hold numkeys, either by using the
 * pre-allocated keysbuf or by allocating a new array on the heap.
 *
 * This function must be called at least once before starting to populate
 * the result, and can be called repeatedly to enlarge the result array.
 */
int *getKeysPrepareResult(getKeysResult *result, int numkeys) {
    /* GETKEYS_RESULT_INIT initializes keys to NULL, point it to the pre-allocated stack
     * buffer here. */
    if (!result->keys) {
        serverAssert(!result->numkeys);
        result->keys = result->keysbuf;
    }

    /* Resize if necessary */
    if (numkeys > result->size) {
        if (result->keys != result->keysbuf) {
            /* We're not using a static buffer, just (re)alloc */
            result->keys = zrealloc(result->keys, numkeys * sizeof(int));
        } else {
            /* We are using a static buffer, copy its contents */
            result->keys = zmalloc(numkeys * sizeof(int));
            if (result->numkeys)
                memcpy(result->keys, result->keysbuf, result->numkeys * sizeof(int));
        }
        result->size = numkeys;
    }

    return result->keys;
}

/* The base case is to use the keys position as given in the command table
 * (firstkey, lastkey, step). */
int getKeysUsingCommandTable(struct redisCommand *cmd,robj **argv, int argc, getKeysResult *result) {
    int j, i = 0, last, *keys;
    UNUSED(argv);

    if (cmd->firstkey == 0) {
        result->numkeys = 0;
        return 0;
    }

    last = cmd->lastkey;
    if (last < 0) last = argc+last;

    int count = ((last - cmd->firstkey)+1);
    keys = getKeysPrepareResult(result, count);

    for (j = cmd->firstkey; j <= last; j += cmd->keystep) {
        if (j >= argc) {
            /* Modules commands, and standard commands with a not fixed number
             * of arguments (negative arity parameter) do not have dispatch
             * time arity checks, so we need to handle the case where the user
             * passed an invalid number of arguments here. In this case we
             * return no keys and expect the command implementation to report
             * an arity or syntax error. */
            if (cmd->flags & CMD_MODULE || cmd->arity < 0) {
                getKeysFreeResult(result);
                result->numkeys = 0;
                return 0;
            } else {
                serverPanic("Redis built-in command declared keys positions not matching the arity requirements.");
            }
        }
        keys[i++] = j;
    }
    result->numkeys = i;
    return i;
}

/* Return all the arguments that are keys in the command passed via argc / argv.
 *
 * The command returns the positions of all the key arguments inside the array,
 * so the actual return value is a heap allocated array of integers. The
 * length of the array is returned by reference into *numkeys.
 *
 * 'cmd' must be point to the corresponding entry into the redisCommand
 * table, according to the command name in argv[0].
 *
 * This function uses the command table if a command-specific helper function
 * is not required, otherwise it calls the command-specific function. */
// 返回command通过argc/argv传递的所有keys。返回数据存储在result中。
// cmd必须指向argv[0]表示的name所对应的命令表中的命令入口。
// 获取keys的处理，优先使用命令指定的getkeys函数，其次才是通过命令表处理。
int getKeysFromCommand(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    if (cmd->flags & CMD_MODULE_GETKEYS) {
        // 如果有CMD_MODULE_GETKEYS标识，标识使用module的getkeys接口获取所有key。
        return moduleGetCommandKeysViaAPI(cmd,argv,argc,result);
    } else if (!(cmd->flags & CMD_MODULE) && cmd->getkeys_proc) {
        // 如果不是模块导出命令，且命令自带getkeys方法，使用该方法获取所有key。
        return cmd->getkeys_proc(cmd,argv,argc,result);
    } else {
        // 如果命令没有带getkeys方法，则使用cmd中firstkey、lastkey、keystep来确定所有keys。
        return getKeysUsingCommandTable(cmd,argv,argc,result);
    }
}

/* Free the result of getKeysFromCommand. */
// 释放getKeysFromCommand的result结构。
// 根据result->keys != result->keysbuf 可判断是在堆上分配的内存，需要手动释放掉。
void getKeysFreeResult(getKeysResult *result) {
    if (result && result->keys != result->keysbuf)
        zfree(result->keys);
}

/* Helper function to extract keys from following commands:
 * COMMAND [destkey] <num-keys> <key> [...] <key> [...] ... <options>
 *
 * eg:
 * ZUNION <num-keys> <key> <key> ... <key> <options>
 * ZUNIONSTORE <destkey> <num-keys> <key> <key> ... <key> <options>
 *
 * 'storeKeyOfs': destkey index, 0 means destkey not exists.
 * 'keyCountOfs': num-keys index.
 * 'firstKeyOfs': firstkey index.
 * 'keyStep': the interval of each key, usually this value is 1.
 * */
int genericGetKeys(int storeKeyOfs, int keyCountOfs, int firstKeyOfs, int keyStep,
                    robj **argv, int argc, getKeysResult *result) {
    int i, num, *keys;

    num = atoi(argv[keyCountOfs]->ptr);
    /* Sanity check. Don't return any key if the command is going to
     * reply with syntax error. (no input keys). */
    if (num < 1 || num > (argc - firstKeyOfs)/keyStep) {
        result->numkeys = 0;
        return 0;
    }

    int numkeys = storeKeyOfs ? num + 1 : num;
    keys = getKeysPrepareResult(result, numkeys);
    result->numkeys = numkeys;

    /* Add all key positions for argv[firstKeyOfs...n] to keys[] */
    for (i = 0; i < num; i++) keys[i] = firstKeyOfs+(i*keyStep);

    if (storeKeyOfs) keys[num] = storeKeyOfs;
    return result->numkeys;
}

int zunionInterDiffStoreGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(1, 2, 3, 1, argv, argc, result);
}

int zunionInterDiffGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(0, 1, 2, 1, argv, argc, result);
}

int evalGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(0, 2, 3, 1, argv, argc, result);
}

/* Helper function to extract keys from the SORT command.
 *
 * SORT <sort-key> ... STORE <store-key> ...
 *
 * The first argument of SORT is always a key, however a list of options
 * follow in SQL-alike style. Here we parse just the minimum in order to
 * correctly identify keys in the "STORE" option. */
int sortGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int i, j, num, *keys, found_store = 0;
    UNUSED(cmd);

    num = 0;
    keys = getKeysPrepareResult(result, 2); /* Alloc 2 places for the worst case. */
    keys[num++] = 1; /* <sort-key> is always present. */

    /* Search for STORE option. By default we consider options to don't
     * have arguments, so if we find an unknown option name we scan the
     * next. However there are options with 1 or 2 arguments, so we
     * provide a list here in order to skip the right number of args. */
    struct {
        char *name;
        int skip;
    } skiplist[] = {
        {"limit", 2},
        {"get", 1},
        {"by", 1},
        {NULL, 0} /* End of elements. */
    };

    for (i = 2; i < argc; i++) {
        for (j = 0; skiplist[j].name != NULL; j++) {
            if (!strcasecmp(argv[i]->ptr,skiplist[j].name)) {
                i += skiplist[j].skip;
                break;
            } else if (!strcasecmp(argv[i]->ptr,"store") && i+1 < argc) {
                /* Note: we don't increment "num" here and continue the loop
                 * to be sure to process the *last* "STORE" option if multiple
                 * ones are provided. This is same behavior as SORT. */
                found_store = 1;
                keys[num] = i+1; /* <store-key> */
                break;
            }
        }
    }
    result->numkeys = num + found_store;
    return result->numkeys;
}

int migrateGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int i, num, first, *keys;
    UNUSED(cmd);

    /* Assume the obvious form. */
    first = 3;
    num = 1;

    /* But check for the extended one with the KEYS option. */
    if (argc > 6) {
        for (i = 6; i < argc; i++) {
            if (!strcasecmp(argv[i]->ptr,"keys") &&
                sdslen(argv[3]->ptr) == 0)
            {
                first = i+1;
                num = argc-first;
                break;
            }
        }
    }

    keys = getKeysPrepareResult(result, num);
    for (i = 0; i < num; i++) keys[i] = first+i;
    result->numkeys = num;
    return num;
}

/* Helper function to extract keys from following commands:
 * GEORADIUS key x y radius unit [WITHDIST] [WITHHASH] [WITHCOORD] [ASC|DESC]
 *                             [COUNT count] [STORE key] [STOREDIST key]
 * GEORADIUSBYMEMBER key member radius unit ... options ... */
int georadiusGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int i, num, *keys;
    UNUSED(cmd);

    /* Check for the presence of the stored key in the command */
    int stored_key = -1;
    for (i = 5; i < argc; i++) {
        char *arg = argv[i]->ptr;
        /* For the case when user specifies both "store" and "storedist" options, the
         * second key specified would override the first key. This behavior is kept
         * the same as in georadiusCommand method.
         */
        if ((!strcasecmp(arg, "store") || !strcasecmp(arg, "storedist")) && ((i+1) < argc)) {
            stored_key = i+1;
            i++;
        }
    }
    num = 1 + (stored_key == -1 ? 0 : 1);

    /* Keys in the command come from two places:
     * argv[1] = key,
     * argv[5...n] = stored key if present
     */
    keys = getKeysPrepareResult(result, num);

    /* Add all key positions to keys[] */
    keys[0] = 1;
    if(num > 1) {
         keys[1] = stored_key;
    }
    result->numkeys = num;
    return num;
}

/* LCS ... [KEYS <key1> <key2>] ... */
int lcsGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int i;
    int *keys = getKeysPrepareResult(result, 2);
    UNUSED(cmd);

    /* We need to parse the options of the command in order to check for the
     * "KEYS" argument before the "STRINGS" argument. */
    for (i = 1; i < argc; i++) {
        char *arg = argv[i]->ptr;
        int moreargs = (argc-1) - i;

        if (!strcasecmp(arg, "strings")) {
            break;
        } else if (!strcasecmp(arg, "keys") && moreargs >= 2) {
            keys[0] = i+1;
            keys[1] = i+2;
            result->numkeys = 2;
            return result->numkeys;
        }
    }
    result->numkeys = 0;
    return result->numkeys;
}

/* Helper function to extract keys from memory command.
 * MEMORY USAGE <key> */
int memoryGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);

    getKeysPrepareResult(result, 1);
    if (argc >= 3 && !strcasecmp(argv[1]->ptr,"usage")) {
        result->keys[0] = 2;
        result->numkeys = 1;
        return result->numkeys;
    }
    result->numkeys = 0;
    return 0;
}

/* XREAD [BLOCK <milliseconds>] [COUNT <count>] [GROUP <groupname> <ttl>]
 *       STREAMS key_1 key_2 ... key_N ID_1 ID_2 ... ID_N */
int xreadGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int i, num = 0, *keys;
    UNUSED(cmd);

    /* We need to parse the options of the command in order to seek the first
     * "STREAMS" string which is actually the option. This is needed because
     * "STREAMS" could also be the name of the consumer group and even the
     * name of the stream key. */
    int streams_pos = -1;
    for (i = 1; i < argc; i++) {
        char *arg = argv[i]->ptr;
        if (!strcasecmp(arg, "block")) {
            i++; /* Skip option argument. */
        } else if (!strcasecmp(arg, "count")) {
            i++; /* Skip option argument. */
        } else if (!strcasecmp(arg, "group")) {
            i += 2; /* Skip option argument. */
        } else if (!strcasecmp(arg, "noack")) {
            /* Nothing to do. */
        } else if (!strcasecmp(arg, "streams")) {
            streams_pos = i;
            break;
        } else {
            break; /* Syntax error. */
        }
    }
    if (streams_pos != -1) num = argc - streams_pos - 1;

    /* Syntax error. */
    if (streams_pos == -1 || num == 0 || num % 2 != 0) {
        result->numkeys = 0;
        return 0;
    }
    num /= 2; /* We have half the keys as there are arguments because
                 there are also the IDs, one per key. */

    keys = getKeysPrepareResult(result, num);
    for (i = streams_pos+1; i < argc-num; i++) keys[i-streams_pos-1] = i;
    result->numkeys = num;
    return num;
}

/* Slot to Key API. This is used by Redis Cluster in order to obtain in
 * a fast way a key that belongs to a specified hash slot. This is useful
 * while rehashing the cluster and in other conditions when we need to
 * understand if we have keys for a given hash slot. */
// slot->keys映射处理的API。从该映射中添加或删除key。
// 这个映射主要用于集群快速获取某个slot关联的keys。这在集群rehash时，或者需要快速判断指定slot中有没有给定的keys时很有用。
void slotToKeyUpdateKey(sds key, int add) {
    size_t keylen = sdslen(key);
    // key通过hash规则，算出对应的slot
    unsigned int hashslot = keyHashSlot(key,keylen);
    unsigned char buf[64];
    unsigned char *indexed = buf;

    // 根据是add还是del，来更新该slot对应的keys的数量。
    server.cluster->slots_keys_count[hashslot] += add ? 1 : -1;
    // 如果key长度超过了62，这里堆上分配空间，而不使用栈空间。
    // 因为我们需要额外使用前两个字节来保存slot信息。key数据追加在后面。这样好使用基数树进行处理。
    if (keylen+2 > 64) indexed = zmalloc(keylen+2);
    indexed[0] = (hashslot >> 8) & 0xff;
    indexed[1] = hashslot & 0xff;
    memcpy(indexed+2,key,keylen);
    if (add) {
        // 如果是添加数据，插入基数树
        raxInsert(server.cluster->slots_to_keys,indexed,keylen+2,NULL,NULL);
    } else {
        // 如果是删除，从基数树移除。
        raxRemove(server.cluster->slots_to_keys,indexed,keylen+2,NULL);
    }
    // 最后如果前面有再堆上分配空间，需要释放。
    if (indexed != buf) zfree(indexed);
}

void slotToKeyAdd(sds key) {
    slotToKeyUpdateKey(key,1);
}

void slotToKeyDel(sds key) {
    slotToKeyUpdateKey(key,0);
}

/* Release the radix tree mapping Redis Cluster keys to slots. If 'async'
 * is true, we release it asynchronously. */
void freeSlotsToKeysMap(rax *rt, int async) {
    if (async) {
        freeSlotsToKeysMapAsync(rt);
    } else {
        raxFree(rt);
    }
}

/* Empty the slots-keys map of Redis CLuster by creating a new empty one and
 * freeing the old one. */
// 清空redis集群中的slots-keys map。创建一个新的空表替换，删除释放老的映射表。
void slotToKeyFlush(int async) {
    rax *old = server.cluster->slots_to_keys;

    server.cluster->slots_to_keys = raxNew();
    memset(server.cluster->slots_keys_count,0,
           sizeof(server.cluster->slots_keys_count));
    freeSlotsToKeysMap(old, async);
}

/* Populate the specified array of objects with keys in the specified slot.
 * New objects are returned to represent keys, it's up to the caller to
 * decrement the reference count to release the keys names. */
unsigned int getKeysInSlot(unsigned int hashslot, robj **keys, unsigned int count) {
    raxIterator iter;
    int j = 0;
    unsigned char indexed[2];

    indexed[0] = (hashslot >> 8) & 0xff;
    indexed[1] = hashslot & 0xff;
    raxStart(&iter,server.cluster->slots_to_keys);
    raxSeek(&iter,">=",indexed,2);
    while(count-- && raxNext(&iter)) {
        if (iter.key[0] != indexed[0] || iter.key[1] != indexed[1]) break;
        keys[j++] = createStringObject((char*)iter.key+2,iter.key_len-2);
    }
    raxStop(&iter);
    return j;
}

/* Remove all the keys in the specified hash slot.
 * The number of removed items is returned. */
unsigned int delKeysInSlot(unsigned int hashslot) {
    raxIterator iter;
    int j = 0;
    unsigned char indexed[2];

    indexed[0] = (hashslot >> 8) & 0xff;
    indexed[1] = hashslot & 0xff;
    raxStart(&iter,server.cluster->slots_to_keys);
    while(server.cluster->slots_keys_count[hashslot]) {
        raxSeek(&iter,">=",indexed,2);
        raxNext(&iter);

        robj *key = createStringObject((char*)iter.key+2,iter.key_len-2);
        dbDelete(&server.db[0],key);
        decrRefCount(key);
        j++;
    }
    raxStop(&iter);
    return j;
}

// 查询集群中对应slot的key数量
unsigned int countKeysInSlot(unsigned int hashslot) {
    return server.cluster->slots_keys_count[hashslot];
}
