#include "server.h"
#include "bio.h"
#include "atomicvar.h"
#include "cluster.h"

static redisAtomic size_t lazyfree_objects = 0;
static redisAtomic size_t lazyfreed_objects = 0;

/* Release objects from the lazyfree thread. It's just decrRefCount()
 * updating the count of objects to release. */
void lazyfreeFreeObject(void *args[]) {
    robj *o = (robj *) args[0];
    decrRefCount(o);
    atomicDecr(lazyfree_objects,1);
    atomicIncr(lazyfreed_objects,1);
}

/* Release a database from the lazyfree thread. The 'db' pointer is the
 * database which was substituted with a fresh one in the main thread
 * when the database was logically deleted. */
void lazyfreeFreeDatabase(void *args[]) {
    dict *ht1 = (dict *) args[0];
    dict *ht2 = (dict *) args[1];

    size_t numkeys = dictSize(ht1);
    dictRelease(ht1);
    dictRelease(ht2);
    atomicDecr(lazyfree_objects,numkeys);
    atomicIncr(lazyfreed_objects,numkeys);
}

/* Release the skiplist mapping Redis Cluster keys to slots in the
 * lazyfree thread. */
void lazyfreeFreeSlotsMap(void *args[]) {
    rax *rt = args[0];
    size_t len = rt->numele;
    raxFree(rt);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* Release the key tracking table. */
void lazyFreeTrackingTable(void *args[]) {
    rax *rt = args[0];
    size_t len = rt->numele;
    freeTrackingRadixTree(rt);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

void lazyFreeLuaScripts(void *args[]) {
    dict *lua_scripts = args[0];
    long long len = dictSize(lua_scripts);
    dictRelease(lua_scripts);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* Return the number of currently pending objects to free. */
// 返回当前需要lazy free的objects数。
size_t lazyfreeGetPendingObjectsCount(void) {
    size_t aux;
    atomicGet(lazyfree_objects,aux);
    return aux;
}

/* Return the number of objects that have been freed. */
size_t lazyfreeGetFreedObjectsCount(void) {
    size_t aux;
    atomicGet(lazyfreed_objects,aux);
    return aux;
}

/* Return the amount of work needed in order to free an object.
 * The return value is not always the actual number of allocations the
 * object is composed of, but a number proportional to it.
 *
 * For strings the function always returns 1.
 *
 * For aggregated objects represented by hash tables or other data structures
 * the function just returns the number of elements the object is composed of.
 *
 * Objects composed of single allocations are always reported as having a
 * single item even if they are actually logical composed of multiple
 * elements.
 *
 * For lists the function returns the number of elements in the quicklist
 * representing the list. */
// 返回需要free的元素数量。并不总是真实的分配的数量，而是正比于它。
// string类型，总返回1；对于聚合对象，返回内部元素。对于list，返回quicklist节点数。
// 当一次性分配一块内存，但里面存储很多元素，此时也认为是单个item。如单个ziplist算一个。
size_t lazyfreeGetFreeEffort(robj *key, robj *obj) {
    if (obj->type == OBJ_LIST) {
        // list对象，返回list的节点数
        quicklist *ql = obj->ptr;
        return ql->len;
    } else if (obj->type == OBJ_SET && obj->encoding == OBJ_ENCODING_HT) {
        dict *ht = obj->ptr;
        // set对象，hash表存储，返回hash表中元素个数
        return dictSize(ht);
    } else if (obj->type == OBJ_ZSET && obj->encoding == OBJ_ENCODING_SKIPLIST){
        // zset对象，跳表存储，返回skiplist元素个数
        zset *zs = obj->ptr;
        return zs->zsl->length;
    } else if (obj->type == OBJ_HASH && obj->encoding == OBJ_ENCODING_HT) {
        // hash对象，hash表存储，返回hash表中元素个数
        dict *ht = obj->ptr;
        return dictSize(ht);
    } else if (obj->type == OBJ_STREAM) {
        // stream对象，rax存储的，返回rax节点个数。（具体rax结构，TODO）
        size_t effort = 0;
        stream *s = obj->ptr;

        /* Make a best effort estimate to maintain constant runtime. Every macro
         * node in the Stream is one allocation. */
        effort += s->rax->numnodes;

        /* Every consumer group is an allocation and so are the entries in its
         * PEL. We use size of the first group's PEL as an estimate for all
         * others. */
        if (s->cgroups && raxSize(s->cgroups)) {
            raxIterator ri;
            streamCG *cg;
            raxStart(&ri,s->cgroups);
            raxSeek(&ri,"^",NULL,0);
            /* There must be at least one group so the following should always
             * work. */
            serverAssert(raxNext(&ri));
            cg = ri.data;
            effort += raxSize(s->cgroups)*(1+raxSize(cg->pel));
            raxStop(&ri);
        }
        return effort;
    } else if (obj->type == OBJ_MODULE) {
        // module对象，如果对应的module type有free_effort函数，则调函数获取。否则返回1。
        moduleValue *mv = obj->ptr;
        moduleType *mt = mv->type;
        if (mt->free_effort != NULL) {
            // 如果有free_effort，调用该函数获取。
            // 如果获取到effort>0，则直接返回effort。否则会返回最大Long值，表示需要进行异步删除（调用者判断>64，则异步释放）。
            size_t effort  = mt->free_effort(key,mv->value);
            /* If the module's free_effort returns 0, it will use asynchronous free
             memory by default */
            return effort == 0 ? ULONG_MAX : effort;
        } else {
            return 1;
        }
    } else {
        // 其他类型，都是单次分配存储，返回1
        return 1; /* Everything else is a single allocation. */
    }
}

/* Delete a key, value, and associated expiration entry if any, from the DB.
 * If there are enough allocations to free the value object may be put into
 * a lazy free list instead of being freed synchronously. The lazy free list
 * will be reclaimed in a different bio.c thread. */
#define LAZYFREE_THRESHOLD 64
// 超过64个元素的集合删除，需要lazy free
// 异步删除处理，加入lazy free队列。只有满足要求的key才会进行异步删除。
int dbAsyncDelete(redisDb *db, robj *key) {
    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary. */
    // 先删除expires中数据。此时并不会释放key的sds，因为还有引用。
    // 所以直接使用dictDelete，不涉及数据清理，很快，不需要使用unlink。
    if (dictSize(db->expires) > 0) dictDelete(db->expires,key->ptr);

    /* If the value is composed of a few allocations, to free in a lazy way
     * is actually just slower... So under a certain limit we just free
     * the object synchronously. */
    // 使用unlink处理，先将要删除的key与db解除关系。我们拿到了entry，后面再处理释放value的操作。
    // 如果value值几乎没怎么分配空间，那么以lazy方式释放实际上可能得不偿失，所以在一些条件下我们会同步来处理对象释放。
    dictEntry *de = dictUnlink(db->dict,key->ptr);
    if (de) {
        robj *val = dictGetVal(de);

        /* Tells the module that the key has been unlinked from the database. */
        // 通知module，我们从db中移除了key。
        moduleNotifyKeyUnlink(key,val);

        // 检查我们释放该value需要做多少工作。
        size_t free_effort = lazyfreeGetFreeEffort(key,val);

        /* If releasing the object is too much work, do it in the background
         * by adding the object to the lazy free list.
         * Note that if the object is shared, to reclaim it now it is not
         * possible. This rarely happens, however sometimes the implementation
         * of parts of the Redis core may call incrRefCount() to protect
         * objects, and then call dbDelete(). In this case we'll fall
         * through and reach the dictFreeUnlinkedEntry() call, that will be
         * equivalent to just calling decrRefCount(). */
        // 如果释放对象要做很多工作，则加入lazy free列表，等待后台线程来处理。只有清理工作大于64时，才会走异步处理，加入队列。
        // 注意如果对象是共享的，现在是不能释放的。这种情况很少，不过在redis核心实现上还是有些会调用incrRefCount()来保护对象的。
        // 所以我们在refcount!=1时，会走到下面dictFreeUnlinkedEntry，从而最终调用decrRefCount()来进行处理。
        if (free_effort > LAZYFREE_THRESHOLD && val->refcount == 1) {
            atomicIncr(lazyfree_objects,1);
            bioCreateLazyFreeJob(lazyfreeFreeObject,1, val);
            dictSetVal(db->dict,de,NULL);
        }
    }

    /* Release the key-val pair, or just the key if we set the val
     * field to NULL in order to lazy free it later. */
    // 前面是通过dictUnlink来从dict中移除元素的，但并没有释放，所以这里需要调用dictFreeUnlinkedEntry对entry进行释放。
    // 注意对于大val我们是进行异步删除的，然后通过dictSetVal将entry的value置为了null，所以这里同步释放这个entry是很快的。
    if (de) {
        dictFreeUnlinkedEntry(db->dict,de);
        // 集群这里更新下key对应的slot。
        if (server.cluster_enabled) slotToKeyDel(key->ptr);
        return 1;
    } else {
        return 0;
    }
}

/* Free an object, if the object is huge enough, free it in async way. */
// 释放对象，如果对象足够大，则异步释放。
void freeObjAsync(robj *key, robj *obj) {
    // 获取释放对象需要做多少工作，即处理多少次释放操作。
    size_t free_effort = lazyfreeGetFreeEffort(key,obj);
    if (free_effort > LAZYFREE_THRESHOLD && obj->refcount == 1) {
        // 如果effort>64，且refcount=1表示需要释放，则添加到队列中等待异步进行处理。
        atomicIncr(lazyfree_objects,1);
        bioCreateLazyFreeJob(lazyfreeFreeObject,1,obj);
    } else {
        decrRefCount(obj);
    }
}

/* Empty a Redis DB asynchronously. What the function does actually is to
 * create a new empty set of hash tables and scheduling the old ones for
 * lazy freeing. */
// 异步清空一个db
void emptyDbAsync(redisDb *db) {
    dict *oldht1 = db->dict, *oldht2 = db->expires;
    // 先创建一个新的空db，释放旧db是创建一个job加入异步队列
    db->dict = dictCreate(&dbDictType,NULL);
    db->expires = dictCreate(&dbExpiresDictType,NULL);
    atomicIncr(lazyfree_objects,dictSize(oldht1));
    bioCreateLazyFreeJob(lazyfreeFreeDatabase,2,oldht1,oldht2);
}

/* Release the radix tree mapping Redis Cluster keys to slots asynchronously. */
// 异步清空slots-keys映射，slots_to_keys基数树置空，slots_keys_count每个slot上key的数量置空。
void freeSlotsToKeysMapAsync(rax *rt) {
    atomicIncr(lazyfree_objects,rt->numele);
    bioCreateLazyFreeJob(lazyfreeFreeSlotsMap,1,rt);
}

/* Free an object, if the object is huge enough, free it in async way. */
// 异步清理大对象
void freeTrackingRadixTreeAsync(rax *tracking) {
    atomicIncr(lazyfree_objects,tracking->numele);
    bioCreateLazyFreeJob(lazyFreeTrackingTable,1,tracking);
}

/* Free lua_scripts dict, if the dict is huge enough, free it in async way. */
// 清理释放lua_scripts dict
void freeLuaScriptsAsync(dict *lua_scripts) {
    if (dictSize(lua_scripts) > LAZYFREE_THRESHOLD) {
        atomicIncr(lazyfree_objects,dictSize(lua_scripts));
        bioCreateLazyFreeJob(lazyFreeLuaScripts,1,lua_scripts);
    } else {
        dictRelease(lua_scripts);
    }
}
