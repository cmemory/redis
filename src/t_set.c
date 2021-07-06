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

/*-----------------------------------------------------------------------------
 * Set Commands
 *----------------------------------------------------------------------------*/

void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum,
                              robj *dstkey, int op);

/* Factory method to return a set that *can* hold "value". When the object has
 * an integer-encodable value, an intset will be returned. Otherwise a regular
 * hash table. */
// 工程方法，返回一个能存储该value的set，当value可以编码为数字时，底层使用intset存储，否则使用hash表存储。
robj *setTypeCreate(sds value) {
    if (isSdsRepresentableAsLongLong(value,NULL) == C_OK)
        return createIntsetObject();
    return createSetObject();
}

/* Add the specified value into a set.
 *
 * If the value was already member of the set, nothing is done and 0 is
 * returned, otherwise the new element is added and 1 is returned. */
// 添加指定的value到set中。如果value已经在set中了，则什么都不做，返回0；否则加入该元素，返回1。
int setTypeAdd(robj *subject, sds value) {
    long long llval;
    if (subject->encoding == OBJ_ENCODING_HT) {
        // 底层hash表存储。
        dict *ht = subject->ptr;
        // dictAddRaw，查询到value则返回NULL；没查到则插入元素，并返回新写入的entry。
        dictEntry *de = dictAddRaw(ht,value,NULL);
        if (de) {
            // 新写入的entry，这里使用dup复制value后填入。
            // 注意前面dictAddRaw中也会填入value，是复用的原client参数对象里字符串，client后面会释放该参数对象，所以这里要复制后写入。
            dictSetKey(ht,de,sdsdup(value));
            dictSetVal(ht,de,NULL);
            return 1;
        }
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        // 底层intset存储，先检查当前要写入的value能否转换为数字，如果还是可以编码为数字，则仍然使用intset存储，否则我们要转为hash表存储了。
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            uint8_t success = 0;
            // 能转成数字，则加入intset，注意可能会导致升级intset的编码。
            subject->ptr = intsetAdd(subject->ptr,llval,&success);
            if (success) {
                /* Convert to regular set when the intset contains
                 * too many entries. */
                // 每次intset成功加入一个元素，我们都要判断intset总元素是否达到限制（默认512），达到了则要转为hash表存储。
                if (intsetLen(subject->ptr) > server.set_max_intset_entries)
                    setTypeConvert(subject,OBJ_ENCODING_HT);
                return 1;
            }
        } else {
            /* Failed to get integer from object, convert to regular set. */
            // 当前写入的元素不能转为数字，则我们需要将原intset变更为hash表存储后，再写入该元素。
            setTypeConvert(subject,OBJ_ENCODING_HT);

            /* The set *was* an intset and this value is not integer
             * encodable, so dictAdd should always work. */
            // intset转为hash表存储，说明新加入的元素肯定是在原set中不存在的（字符串和数字类型都不一样），一定能add成功。
            serverAssert(dictAdd(subject->ptr,sdsdup(value),NULL) == DICT_OK);
            return 1;
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

// 从set中移除元素
int setTypeRemove(robj *setobj, sds value) {
    long long llval;
    if (setobj->encoding == OBJ_ENCODING_HT) {
        // hash表存储，直接使用dictDelete来删除。
        if (dictDelete(setobj->ptr,value) == DICT_OK) {
            // 这里set每次删除一个元素后，我们检查是否需要缩容，并进行操作。
            // 注意对于redis db，我们删除是并没有检查需要resize，因为我们有dbcron定时会检查。
            // 而db中的hash、set、zset这些对象，我们无法在定时任务处理，只有每次删除元素时检查。
            if (htNeedsResize(setobj->ptr)) dictResize(setobj->ptr);
            return 1;
        }
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
        // intset编码，检查传入的valu能否编码为数字，不能则value肯定不在set中不需要处理。
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            int success;
            // 能编码为数字，调用intsetRemove来移除该数字元素。
            setobj->ptr = intsetRemove(setobj->ptr,llval,&success);
            if (success) return 1;
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

// 检查value是否在set中。
int setTypeIsMember(robj *subject, sds value) {
    long long llval;
    if (subject->encoding == OBJ_ENCODING_HT) {
        // hash表编码，dictFind查询该该元素。
        return dictFind((dict*)subject->ptr,value) != NULL;
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        // intset编码，先检查value是否可转为数字，不能则不在set中。
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            // 能编码为数字，调用intsetFind查询该数字。
            return intsetFind((intset*)subject->ptr,llval);
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

// set迭代器初始化
setTypeIterator *setTypeInitIterator(robj *subject) {
    setTypeIterator *si = zmalloc(sizeof(setTypeIterator));
    si->subject = subject;
    si->encoding = subject->encoding;
    // 根据不同的编码，分别初始化底层dict和intset迭代器。
    if (si->encoding == OBJ_ENCODING_HT) {
        si->di = dictGetIterator(subject->ptr);
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        si->ii = 0;
    } else {
        serverPanic("Unknown set encoding");
    }
    return si;
}

// 释放迭代器
void setTypeReleaseIterator(setTypeIterator *si) {
    if (si->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(si->di);
    zfree(si);
}

/* Move to the next entry in the set. Returns the object at the current
 * position.
 *
 * Since set elements can be internally be stored as SDS strings or
 * simple arrays of integers, setTypeNext returns the encoding of the
 * set object you are iterating, and will populate the appropriate pointer
 * (sdsele) or (llele) accordingly.
 *
 * Note that both the sdsele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused.
 *
 * When there are no longer elements -1 is returned. */
// 迭代器指向下一个元素，然后返回该元素数据以及编码方式。如果迭代完成，没有下一个元素时，函数返回-1。
// 因为set元素底层可能是使用hash表存储的字符串，或者是intset存储的数字，所以这里分别使用sdsele、llele来存储取到的字符串或数字。
// 注意这里sdsele和llele应该传入非NULL指针，因为即使某个字段类型不匹配，该函数也会尝试防御性的填充字段值，这样在误用时很容易捕获。
int setTypeNext(setTypeIterator *si, sds *sdsele, int64_t *llele) {
    if (si->encoding == OBJ_ENCODING_HT) {
        // hash表存储，dictNext获取下一个entry。
        dictEntry *de = dictNext(si->di);
        if (de == NULL) return -1;
        // 下一个元素entry存储，则dictGetKey获取字符串值返回。
        *sdsele = dictGetKey(de);
        *llele = -123456789; /* Not needed. Defensive. */
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        // intset存储，intsetGet获取si->ii索引处的数据。然后迭代器索引+1，供下次获取元素使用。
        if (!intsetGet(si->subject->ptr,si->ii++,llele))
            return -1;
        *sdsele = NULL; /* Not needed. Defensive. */
    } else {
        serverPanic("Wrong set encoding in setTypeNext");
    }
    return si->encoding;
}

/* The not copy on write friendly version but easy to use version
 * of setTypeNext() is setTypeNextObject(), returning new SDS
 * strings. So if you don't retain a pointer to this object you should call
 * sdsfree() against it.
 *
 * This function is the way to go for write operations where COW is not
 * an issue. */
// setTypeNextObject 是 setTypeNext 的另一个迭代获取next元素的版本。
// 这个版本函数对copy on write不友好，但是更易于使用。如果没有COW问题时，一般使用这个方法来迭代返回数据，然后处理写操作。
// 该函数会返回新的sds字符串，所以如果调用方不想保留该sds对象的指针，应该使用sdsfree()来释放它。
sds setTypeNextObject(setTypeIterator *si) {
    int64_t intele;
    sds sdsele;
    int encoding;

    // 调用setTypeNext获取迭代到的元素。
    encoding = setTypeNext(si,&sdsele,&intele);
    // 根据编码方式，新创建sds字符串返回。
    switch(encoding) {
        case -1:    return NULL;
        case OBJ_ENCODING_INTSET:
            return sdsfromlonglong(intele);
        case OBJ_ENCODING_HT:
            return sdsdup(sdsele);
        default:
            serverPanic("Unsupported encoding");
    }
    return NULL; /* just to suppress warnings */
}

/* Return random element from a non empty set.
 * The returned element can be an int64_t value if the set is encoded
 * as an "intset" blob of integers, or an SDS string if the set
 * is a regular set.
 *
 * The caller provides both pointers to be populated with the right
 * object. The return value of the function is the object->encoding
 * field of the object and is used by the caller to check if the
 * int64_t pointer or the redis object pointer was populated.
 *
 * Note that both the sdsele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused. */
// 从非空set中随机返回元素。如果set时intset编码，则返回数字；如果时hash表存储，则返回sds字符串。
// 函数提供两个参数sdsele、llele用于填充返回的字符串或数字数据。另外函数return的值为数据编码方式，用于调用者确定返回数据是字符串还是数字。
// 注意sdsele和llele应该传入非NULL指针，因为即使编码不匹配，这里也会尝试防御性的填充字段值，这样在误用时很容易捕获。
int setTypeRandomElement(robj *setobj, sds *sdsele, int64_t *llele) {
    if (setobj->encoding == OBJ_ENCODING_HT) {
        // hash表存储，使用dictGetFairRandomKey来随机获取一个entry，返回entry中的元素。
        dictEntry *de = dictGetFairRandomKey(setobj->ptr);
        *sdsele = dictGetKey(de);
        *llele = -123456789; /* Not needed. Defensive. */
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
        // intset编码，使用intsetRandom随机获取一个索引处的数字返回。
        *llele = intsetRandom(setobj->ptr);
        *sdsele = NULL; /* Not needed. Defensive. */
    } else {
        serverPanic("Unknown set encoding");
    }
    return setobj->encoding;
}

// 返回set中元素的总个数。hash表存储使用dictSize获取，intset存储使用intsetLen来获取。
unsigned long setTypeSize(const robj *subject) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        return dictSize((const dict*)subject->ptr);
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        return intsetLen((const intset*)subject->ptr);
    } else {
        serverPanic("Unknown set encoding");
    }
}

/* Convert the set to specified encoding. The resulting dict (when converting
 * to a hash table) is presized to hold the number of elements in the original
 * set. */
// 转换set为指定的编码方式（从intset转为hash表）。转换后，对象底层dict会包含原intset中的所有元素。
void setTypeConvert(robj *setobj, int enc) {
    setTypeIterator *si;
    serverAssertWithInfo(NULL,setobj,setobj->type == OBJ_SET &&
                             setobj->encoding == OBJ_ENCODING_INTSET);

    if (enc == OBJ_ENCODING_HT) {
        int64_t intele;
        // 先创建一个空的dict
        dict *d = dictCreate(&setDictType,NULL);
        sds element;

        /* Presize the dict to avoid rehashing */
        // 预先设置好dict的大小，避免转换期间过多rehash。
        dictExpand(d,intsetLen(setobj->ptr));

        /* To add the elements we extract integers and create redis objects */
        // 迭代intset元素，取出对应的数字，转换为sds字符串，加入到新的dict中。
        si = setTypeInitIterator(setobj);
        while (setTypeNext(si,&element,&intele) != -1) {
            element = sdsfromlonglong(intele);
            serverAssert(dictAdd(d,element,NULL) == DICT_OK);
        }
        setTypeReleaseIterator(si);

        // 更新对象的底层数据结构为hash表。
        setobj->encoding = OBJ_ENCODING_HT;
        zfree(setobj->ptr);
        setobj->ptr = d;
    } else {
        serverPanic("Unsupported set conversion");
    }
}

/* This is a helper function for the COPY command.
 * Duplicate a set object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * The resulting object always has refcount set to 1 */
// COPY命令的辅助函数。复制一个set对象，保证返回对象与原对象编码方式以及数据都完全一致。返回对象的引用计数总为1。
robj *setTypeDup(robj *o) {
    robj *set;
    setTypeIterator *si;
    sds elesds;
    int64_t intobj;

    serverAssert(o->type == OBJ_SET);

    /* Create a new set object that have the same encoding as the original object's encoding */
    // 创建一个set对象，并复制所有的数据到新对象中，注意底层编码方式一致。
    if (o->encoding == OBJ_ENCODING_INTSET) {
        intset *is = o->ptr;
        size_t size = intsetBlobLen(is);
        // intset编码，直接分配一段连续的空间，并使用memcpy将原intset的数据完全复制过期。
        intset *newis = zmalloc(size);
        memcpy(newis,is,size);
        // 基于新的intset数据创建新的set对象。
        set = createObject(OBJ_SET, newis);
        set->encoding = OBJ_ENCODING_INTSET;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        // hash表存储，这里先创建一个空基于hash表的新set对象。
        set = createSetObject();
        dict *d = o->ptr;
        dictExpand(set->ptr, dictSize(d));
        // 迭代原set数据，依次将取到的数据加入到新的set中。
        si = setTypeInitIterator(o);
        while (setTypeNext(si, &elesds, &intobj) != -1) {
            setTypeAdd(set, elesds);
        }
        setTypeReleaseIterator(si);
    } else {
        serverPanic("Unknown set encoding");
    }
    return set;
}

// sadd obj v1 [v2]，将元素加入到集合中
void saddCommand(client *c) {
    robj *set;
    int j, added = 0;

    // 查询set对象for write，并确保对象的类型为set
    set = lookupKeyWrite(c->db,c->argv[1]);
    if (checkType(c,set,OBJ_SET)) return;

    // 如果db中该set不存在，则创建一个新的set并加入到db中。
    // 注意这里创建新的set时，会根据参数中 第一个待写入set中的数据 来决定set的编码是intset还是hash表，当然后面再插入元素可能会再转换。
    if (set == NULL) {
        set = setTypeCreate(c->argv[2]->ptr);
        dbAdd(c->db,c->argv[1],set);
    }

    for (j = 2; j < c->argc; j++) {
        // 依次将指定的多个数据，加入到set中
        if (setTypeAdd(set,c->argv[j]->ptr)) added++;
    }
    // 如果有元素加入到集合中，说明key变更了，这里处理key变更通知。
    if (added) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[1],c->db->id);
    }
    // 更新db中数据修改变动数，用于RDB自动存储（当一定时间内达到一定的变更量，会进快照备份）。
    server.dirty += added;
    // 返回加入set中的元素个数
    addReplyLongLong(c,added);
}

// srem obj v1 [v2]，从集合中移除元素。
void sremCommand(client *c) {
    robj *set;
    int j, deleted = 0, keyremoved = 0;

    // 从db中查询set对象，并确保对象的编码是set类型。
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,OBJ_SET)) return;

    for (j = 2; j < c->argc; j++) {
        // 对于指定的每个元素，依次调用setTypeRemove来处理移除。
        if (setTypeRemove(set,c->argv[j]->ptr)) {
            // 如果元素存在且移除成功，则更新移除元素的个数，用于后面统计db变更数以及返回删除元素的个数。
            deleted++;
            // 另外每次成功移除一个元素，我们需要检查set是否为空，如果为空则从db中删除该set对象。
            if (setTypeSize(set) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }
    // 有移除元素（或者set对象删除），处理key变更通知。
    if (deleted) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        server.dirty += deleted;
    }
    // 回复从set移除的元素数
    addReplyLongLong(c,deleted);
}

// smove src dst val
void smoveCommand(client *c) {
    robj *srcset, *dstset, *ele;
    // 获取指定的src、dst集合set对象
    srcset = lookupKeyWrite(c->db,c->argv[1]);
    dstset = lookupKeyWrite(c->db,c->argv[2]);
    ele = c->argv[3];

    /* If the source key does not exist return 0 */
    // srcset为NULL表示源set没有元素，直接返回0。
    if (srcset == NULL) {
        addReply(c,shared.czero);
        return;
    }

    /* If the source key has the wrong type, or the destination key
     * is set and has the wrong type, return with an error. */
    // 如果取到的源对象或目标对象的的类型不是set，则返回err。
    if (checkType(c,srcset,OBJ_SET) ||
        checkType(c,dstset,OBJ_SET)) return;

    /* If srcset and dstset are equal, SMOVE is a no-op */
    // 如果源set和目标set是同一个，则SMOVE不需要操作。这里根据指定move的元素是否在当前set中，来决定返回1还是0。
    if (srcset == dstset) {
        addReply(c,setTypeIsMember(srcset,ele->ptr) ?
            shared.cone : shared.czero);
        return;
    }

    /* If the element cannot be removed from the src set, return 0. */
    // smove操作分为两步，先移除源set中指定元素，再将该元素加入到目标set中。
    // 从src中移除元素指定元素，如果元素不存在无法移除，则返回0。
    if (!setTypeRemove(srcset,ele->ptr)) {
        addReply(c,shared.czero);
        return;
    }
    // 移除set中元素成功，处理key变更通知
    notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);

    /* Remove the src set from the database when empty */
    // 当src移除元素后为空时，从db中移除该set。
    if (setTypeSize(srcset) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* Create the destination set when it doesn't exist */
    // 如果目标set不存在，则这里创建一个新的set，用于加入元素。
    if (!dstset) {
        dstset = setTypeCreate(ele->ptr);
        dbAdd(c->db,c->argv[2],dstset);
    }

    // 处理key变更通知，dstset的变更通知不是应该放到真正add数据成功后？
    signalModifiedKey(c,c->db,c->argv[1]);
    signalModifiedKey(c,c->db,c->argv[2]);
    server.dirty++;

    /* An extra key has changed when ele was successfully added to dstset */
    // 将前面移除的元素加入到dstset中，并通知key变更。
    if (setTypeAdd(dstset,ele->ptr)) {
        server.dirty++;
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[2],c->db->id);
    }
    // 返回1，表示处理成功1个元素
    addReply(c,shared.cone);
}

// sismember obj val，判断元素是否在集合set中
void sismemberCommand(client *c) {
    robj *set;

    // 获取对象，并确保对象的类型是set。
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,OBJ_SET)) return;

    // 使用setTypeIsMember来判断元素是否在集合中。在则返回1，不在返回0。
    if (setTypeIsMember(set,c->argv[2]->ptr))
        addReply(c,shared.cone);
    else
        addReply(c,shared.czero);
}

// smismember obj val1 [val2]，一次性判断多个元素是否在集合中。
void smismemberCommand(client *c) {
    robj *set;
    int j;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * sets, where SMISMEMBER should respond with a series of zeros. */
    // 获取对象，并确保对象的类型是set。
    // 注意这里如果对象在db中不存在，我们不直接返回空，因为当前命令会返回一个数组，对应每个元素是否在集合中。
    set = lookupKeyRead(c->db,c->argv[1]);
    if (set && checkType(c,set,OBJ_SET)) return;

    // 返回数据是一个数组，这里先写入数组长度
    addReplyArrayLen(c,c->argc - 2);

    for (j = 2; j < c->argc; j++) {
        // 遍历每个指定的元素，判断元素在集合中则回复1，否则回复0。
        if (set && setTypeIsMember(set,c->argv[j]->ptr))
            addReply(c,shared.cone);
        else
            addReply(c,shared.czero);
    }
}

// scard obj，获取集合的元素个数
void scardCommand(client *c) {
    robj *o;

    // 获取对象，并确保对象是set类型。
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_SET)) return;

    // 使用setTypeSize来获取集合元素总数，并返回给client。
    addReplyLongLong(c,setTypeSize(o));
}

/* Handle the "SPOP key <count>" variant. The normal version of the
 * command is handled by the spopCommand() function itself. */

/* How many times bigger should be the set compared to the remaining size
 * for us to use the "create new set" strategy? Read later in the
 * implementation for more info. */
// SPOP key的变体，多了count参数。正常该命令pop一个元素，由spopCommand()函数自己处理。
// 如果pop后，集合剩余元素个数的SPOP_MOVE_STRATEGY_MUL倍还是小于等于count时，说明剩余元素很少，我们使用"移除元素"的策略。
#define SPOP_MOVE_STRATEGY_MUL 5

void spopWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    robj *set;

    /* Get the count argument */
    // 获取count参数。
    if (getPositiveLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) return;
    count = (unsigned long) l;

    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set. Otherwise, return nil */
    // 获取对象，并确保对象类型是set
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.emptyset[c->resp]))
        == NULL || checkType(c,set,OBJ_SET)) return;

    /* If count is zero, serve an empty set ASAP to avoid special
     * cases later. */
    // 如果count为0，则表示我们不需要pop元素，直接返回。
    if (count == 0) {
        addReply(c,shared.emptyset[c->resp]);
        return;
    }

    size = setTypeSize(set);

    /* Generate an SPOP keyspace notification */
    // key变更通知
    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);
    server.dirty += (count >= size) ? size : count;

    /* CASE 1:
     * The number of requested elements is greater than or equal to
     * the number of elements inside the set: simply return the whole set. */
    // 情形1：要pop的数量 >= 集合中元素总数，则直接返回整个set，并db中删除该对象。
    if (count >= size) {
        /* We just return the entire set */
        // 返回整个set，这里调用集合求并交集的方法处理（只有一个集合，所以相当于返回该集合所有数据）。
        sunionDiffGenericCommand(c,c->argv+1,1,NULL,SET_OP_UNION);

        /* Delete the set as it is now empty */
        // 因为set的所有元素都pop返回了，该set为空了，所有这里删除该对象。
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);

        /* Propagate this command as a DEL operation */
        // 将SPOP命令改写为DEL命令来进行传播。
        rewriteClientCommandVector(c,2,shared.del,c->argv[1]);
        signalModifiedKey(c,c->db,c->argv[1]);
        return;
    }

    /* Case 2 and 3 require to replicate SPOP as a set of SREM commands.
     * Prepare our replication argument vector. Also send the array length
     * which is common to both the code paths. */
    // 后面情形2、3，pop元素，我们需要重写为srem命令来进行传播。
    robj *propargv[3];
    propargv[0] = shared.srem;
    propargv[1] = c->argv[1];
    // SPOP命令回复的是一个集合，这里写入回复数据长度（对于RESP2用数组返回的，而RESP3有集合类型）。
    addReplySetLen(c,count);

    /* Common iteration vars. */
    sds sdsele;
    robj *objele;
    int encoding;
    int64_t llele;
    // pop后剩余的元素数量
    unsigned long remaining = size-count; /* Elements left after SPOP. */

    /* If we are here, the number of requested elements is less than the
     * number of elements inside the set. Also we are sure that count < size.
     * Use two different strategies.
     *
     * CASE 2: The number of elements to return is small compared to the
     * set size. We can just extract random elements and return them to
     * the set. */
    // 执行到这里，请求pop元素数量count肯定是小于set总元素数的，我们使用两种策略来处理。
    // 当剩余元素个数很小，不多余 count/SPOP_MOVE_STRATEGY_MUL 个时，也即我们的case3，通过"移除元素"策略来处理。
    // 而当剩余元素个数还比较大，大于count/SPOP_MOVE_STRATEGY_MUL时，即这里的case2，我们从set中随机获取元素来返回。
    if (remaining*SPOP_MOVE_STRATEGY_MUL > count) {
        // 遍历count次，来获取count个随机元素返回。
        while(count--) {
            /* Emit and remove. */
            // 随机获取一个元素。
            encoding = setTypeRandomElement(set,&sdsele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                // intset编码，则取到的是数字。处理数字回复client，并使用intsetRemove移除元素。
                addReplyBulkLongLong(c,llele);
                objele = createStringObjectFromLongLong(llele);
                set->ptr = intsetRemove(set->ptr,llele,NULL);
            } else {
                // hash表存储，则取到的是字符串，直接回复client，并调用setTypeRemove处理元素移除。
                addReplyBulkCBuffer(c,sdsele,sdslen(sdsele));
                objele = createStringObject(sdsele,sdslen(sdsele));
                setTypeRemove(set,sdsele);
            }

            /* Replicate/AOF this command as an SREM operation */
            // 每pop移除一个元素，这里传播SRAM命令给slave/aof。
            propargv[2] = objele;
            alsoPropagate(server.sremCommand,c->db->id,propargv,3,
                PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(objele);
        }
    } else {
    /* CASE 3: The number of elements to return is very big, approaching
     * the size of the set itself. After some time extracting random elements
     * from such a set becomes computationally expensive, so we use
     * a different strategy, we extract random elements that we don't
     * want to return (the elements that will remain part of the set),
     * creating a new set as we do this (that will be stored as the original
     * set). Then we return the elements left in the original set and
     * release it. */
    // 情形3：要pop返回的元素数量很多，接近整个set的总数量了。所以我们不再是每次随机pop元素返回，而是随机pop元素留下。
        robj *newset = NULL;

        /* Create a new set with just the remaining elements. */
        // 从原set中随机选取remaining个元素，放入newset中作为留下的元素集合。
        while(remaining--) {
            // 随机取出一个元素，转成sds字符串。
            encoding = setTypeRandomElement(set,&sdsele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                sdsele = sdsfromlonglong(llele);
            } else {
                sdsele = sdsdup(sdsele);
            }
            // 将元素加入到newset中，并从原set中移除。
            if (!newset) newset = setTypeCreate(sdsele);
            setTypeAdd(newset,sdsele);
            setTypeRemove(set,sdsele);
            sdsfree(sdsele);
        }

        /* Transfer the old set to the client. */
        // 原set剩余的元素即为我们需要返回给client的回复。这里迭代原set，将遍历的每个元素加入回复中。
        setTypeIterator *si;
        si = setTypeInitIterator(set);
        while((encoding = setTypeNext(si,&sdsele,&llele)) != -1) {
            // 遍历到了元素，根据字符串或数字类型处理回复。
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
                objele = createStringObjectFromLongLong(llele);
            } else {
                addReplyBulkCBuffer(c,sdsele,sdslen(sdsele));
                objele = createStringObject(sdsele,sdslen(sdsele));
            }

            /* Replicate/AOF this command as an SREM operation */
            // 每pop一个数据，传播SREM命令给slaves/aof。
            propargv[2] = objele;
            alsoPropagate(server.sremCommand,c->db->id,propargv,3,
                PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(objele);
        }
        setTypeReleaseIterator(si);

        /* Assign the new set as the key value. */
        // 最后需要用newset来替代db中原set。
        dbOverwrite(c->db,c->argv[1],newset);
    }

    /* Don't propagate the command itself even if we incremented the
     * dirty counter. We don't want to propagate an SPOP command since
     * we propagated the command as a set of SREMs operations using
     * the alsoPropagate() API. */
    // 前面对于处理的3种情形，我们都已经处理传播了DEL或SREM命令，所以这里我们禁止当前SPOP命令的传播。
    preventCommandPropagation(c);
    signalModifiedKey(c,c->db,c->argv[1]);
}

// spop obj [count]，随机从set中pop count个元素。
// 不带count参数，默认为只pop一个元素。如果有count参数，则会调用spopWithCountCommand来处理。
void spopCommand(client *c) {
    robj *set, *ele;
    sds sdsele;
    int64_t llele;
    int encoding;

    if (c->argc == 3) {
        spopWithCountCommand(c);
        return;
    } else if (c->argc > 3) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set */
    // 获取指定对象，并确保对象是set类型。
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.null[c->resp]))
         == NULL || checkType(c,set,OBJ_SET)) return;

    /* Get a random element from the set */
    // 随机从set中获取一个元素。
    encoding = setTypeRandomElement(set,&sdsele,&llele);

    /* Remove the element from the set */
    // 从set中移除该元素
    if (encoding == OBJ_ENCODING_INTSET) {
        ele = createStringObjectFromLongLong(llele);
        set->ptr = intsetRemove(set->ptr,llele,NULL);
    } else {
        ele = createStringObject(sdsele,sdslen(sdsele));
        setTypeRemove(set,ele->ptr);
    }

    // 处理key变更通知
    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);

    /* Replicate/AOF this command as an SREM operation */
    // 重写命令为srem来进行slaves/aof传播。
    rewriteClientCommandVector(c,3,shared.srem,c->argv[1],ele);

    /* Add the element to the reply */
    // 回复pop到的数据给client。
    addReplyBulk(c,ele);
    decrRefCount(ele);

    /* Delete the set if it's empty */
    // 如果set移除元素后为空，则我们要从db中移除该set。
    if (setTypeSize(set) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* Set has been modified */
    // 处理key变更通知。
    signalModifiedKey(c,c->db,c->argv[1]);
    server.dirty++;
}

/* handle the "SRANDMEMBER key <count>" variant. The normal version of the
 * command is handled by the srandmemberCommand() function itself. */

/* How many times bigger should be the set compared to the requested size
 * for us to don't use the "remove elements" strategy? Read later in the
 * implementation for more info. */
// 处理SRANDMEMBER key <count>带count参数的命令，count为负数表示元素可以重复。正常不带count的命令由srandmemberCommand函数处理。
// 当请求的count超过set元素总数的1/3时，我们使用"移除元素"的策略处理。
#define SRANDMEMBER_SUB_STRATEGY_MUL 3

void srandmemberWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    int uniq = 1;
    robj *set;
    sds ele;
    int64_t llele;
    int encoding;

    dict *d;

    // 获取count参数。
    if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) return;
    if (l >= 0) {
        count = (unsigned long) l;
    } else {
        /* A negative count means: return the same elements multiple times
         * (i.e. don't remove the extracted element after every extraction). */
        // 负的count表示，可以返回相同的元素。即我们取到该元素后并不会移除该元素，后面还可以取到。
        count = -l;
        uniq = 0;
    }

    // 从db获取对象，确保对象是set类型
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptyarray))
        == NULL || checkType(c,set,OBJ_SET)) return;
    size = setTypeSize(set);

    /* If count is zero, serve it ASAP to avoid special cases later. */
    // count为0，不需要取元素，尽早返回。
    if (count == 0) {
        addReply(c,shared.emptyarray);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. This case is the only one that also needs to return the
     * elements in random order. */
    // 情形1：count为负，可重复，则我们重复count次随机从对象中取出元素添加到回复中就可以了。
    if (!uniq || count == 1) {
        // 先写入回复的总元素数。
        addReplyArrayLen(c,count);
        while(count--) {
            // 循环count次，随机从set中获取元素，加入到回复buf中。
            encoding = setTypeRandomElement(set,&ele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
            } else {
                addReplyBulkCBuffer(c,ele,sdslen(ele));
            }
        }
        return;
    }

    /* CASE 2:
     * The number of requested elements is greater than the number of
     * elements inside the set: simply return the whole set. */
    // 执行到这里，我们知道返回的元素肯定是不可重复的，所以最终返回的元素数可能小于count。
    // 情形2：请求的count元素数量 >= 集合中总元素数，则只需要返回整个set的元素即可。
    if (count >= size) {
        setTypeIterator *si;
        // 写入回复的总元素数为size。
        addReplyArrayLen(c,size);
        si = setTypeInitIterator(set);
        // 迭代集合中所有的元素，加入到回复列表中。
        while ((encoding = setTypeNext(si,&ele,&llele)) != -1) {
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
            } else {
                addReplyBulkCBuffer(c,ele,sdslen(ele));
            }
            size--;
        }
        setTypeReleaseIterator(si);
        // 确保我们返回的总数与集合元素总数是一致的。
        serverAssert(size==0);
        return;
    }

    /* For CASE 3 and CASE 4 we need an auxiliary dictionary. */
    // 对于情形3和4我们都需要一个辅助dict，最终该dict中的所有元素就是我们需要返回数据。
    d = dictCreate(&sdsReplyDictType,NULL);

    /* CASE 3:
     * The number of elements inside the set is not greater than
     * SRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a set from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requested elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 4 is highly inefficient. */
    // 情形3：set中的总元素数不大于请求count的 SRANDMEMBER_SUB_STRATEGY_MUL 倍。
    // 我们复制该set对象，然后从新set中随机去除部分元素，当新set剩余元素个数与count一致时，该set中的元素即我们需要回复给client的数据。
    // 这样做是因为，当用户请求count很小时，情形4中使用的自然方式效率非常低。
    if (count*SRANDMEMBER_SUB_STRATEGY_MUL > size) {
        setTypeIterator *si;

        /* Add all the elements into the temporary dictionary. */
        // 迭代set中的元素，将所有元素依次加入到临时dict中。
        si = setTypeInitIterator(set);
        dictExpand(d, size);
        while ((encoding = setTypeNext(si,&ele,&llele)) != -1) {
            int retval = DICT_ERR;

            if (encoding == OBJ_ENCODING_INTSET) {
                retval = dictAdd(d,sdsfromlonglong(llele),NULL);
            } else {
                retval = dictAdd(d,sdsdup(ele),NULL);
            }
            serverAssert(retval == DICT_OK);
        }
        setTypeReleaseIterator(si);
        serverAssert(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        // 随机从临时dict移除元素，直到临时dict元素数量达到count。
        while (size > count) {
            dictEntry *de;
            de = dictGetRandomKey(d);
            dictUnlink(d,dictGetKey(de));
            sdsfree(dictGetKey(de));
            dictFreeUnlinkedEntry(d,de);
            size--;
        }
    }

    /* CASE 4: We have a big set compared to the requested number of elements.
     * In this case we can simply get random elements from the set and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. */
    // 情形4：set中总元素数大于3倍count，即相对于请求count来说我们又一个较大的set。
    // 此时我们可以简单的随机获取元素，添加到临时dict中，临时dict主要用于去重处理，最终我们会返回该dict中的所有元素。
    else {
        unsigned long added = 0;
        sds sdsele;

        // 扩容临时dict，避免过多的rehash。
        dictExpand(d, count);
        while (added < count) {
            // 只要没有取够数据，就一直循环的随机从set中取数据加入到临时dict中。
            encoding = setTypeRandomElement(set,&ele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                sdsele = sdsfromlonglong(llele);
            } else {
                sdsele = sdsdup(ele);
            }
            /* Try to add the object to the dictionary. If it already exists
             * free it, otherwise increment the number of objects we have
             * in the result dictionary. */
            // 尝试将取到的元素加入到临时dict中。如果已经存在了，则不满足需求，再继续取数据加入。
            if (dictAdd(d,sdsele,NULL) == DICT_OK)
                added++;
            else
                sdsfree(sdsele);
        }
    }

    /* CASE 3 & 4: send the result to the user. */
    // 情形3和4：遍历临时dict中的元素，依次加入到回复buf中。
    {
        dictIterator *di;
        dictEntry *de;

        addReplyArrayLen(c,count);
        di = dictGetIterator(d);
        while((de = dictNext(di)) != NULL)
            addReplyBulkSds(c,dictGetKey(de));
        dictReleaseIterator(di);
        dictRelease(d);
    }
}

/* SRANDMEMBER obj [<count>] */
// 随机从set中获取count个元素，如果没有count参数，则默认随机取一个元素。如果有count，则调用srandmemberWithCountCommand来处理。
void srandmemberCommand(client *c) {
    robj *set;
    sds ele;
    int64_t llele;
    int encoding;

    if (c->argc == 3) {
        srandmemberWithCountCommand(c);
        return;
    } else if (c->argc > 3) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* Handle variant without <count> argument. Reply with simple bulk string */
    // 获取指定对象，并确保对象是set类型。
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]))
        == NULL || checkType(c,set,OBJ_SET)) return;

    // 随机获取一个元素，然后回复给client。
    encoding = setTypeRandomElement(set,&ele,&llele);
    if (encoding == OBJ_ENCODING_INTSET) {
        addReplyBulkLongLong(c,llele);
    } else {
        addReplyBulkCBuffer(c,ele,sdslen(ele));
    }
}

// 该函数用于SINTER中，将多个集合按元素个数从少到多排序。函数传入的必须是set对象，且不能为NULL。
int qsortCompareSetsByCardinality(const void *s1, const void *s2) {
    if (setTypeSize(*(robj**)s1) > setTypeSize(*(robj**)s2)) return 1;
    if (setTypeSize(*(robj**)s1) < setTypeSize(*(robj**)s2)) return -1;
    return 0;
}

/* This is used by SDIFF and in this case we can receive NULL that should
 * be handled as empty sets. */
// 该函数用于SDIFF中，将多个集合按元素个数从多到少排序。此函数可以接收NULL参数，会当￿做空集合处理。
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2) {
    robj *o1 = *(robj**)s1, *o2 = *(robj**)s2;
    unsigned long first = o1 ? setTypeSize(o1) : 0;
    unsigned long second = o2 ? setTypeSize(o2) : 0;

    if (first < second) return 1;
    if (first > second) return -1;
    return 0;
}

// 求交集的统一处理函数。如果传入的dstkey参数为NULL，则结果会返回给client；否则非空，则会将交集写入dstkey集合中，并返回写入该集合的元素数量。
void sinterGenericCommand(client *c, robj **setkeys,
                          unsigned long setnum, robj *dstkey) {
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    sds elesds;
    int64_t intobj;
    void *replylen = NULL;
    unsigned long j, cardinality = 0;
    int encoding;

    // 遍历获取求交集的所有sets。
    for (j = 0; j < setnum; j++) {
        // 如果没有dst，则获取set对象for read；如果有dst，则获取set对象for write。why？
        robj *setobj = dstkey ?
            lookupKeyWrite(c->db,setkeys[j]) :
            lookupKeyRead(c->db,setkeys[j]);
        if (!setobj) {
            // 如果有一个源set不存在，则所有源sets的交集是空，可以直接返回。
            // 另外如果需要将交集写入dst集合中，则dst则为空的set，而空set是不写入db中的，所以我们一般只用返回0就可以了。
            // 注意对于特殊情况，如果dstkey已经在db中了，则需要删除该key，即设置它为空集合。
            zfree(sets);
            if (dstkey) {
                // 需要设置dstkey为空集合，这里尝试删除dstkey。（默认就是覆盖更新dstkey，所以它原来是不是set都没关系）
                if (dbDelete(c->db,dstkey)) {
                    signalModifiedKey(c,c->db,dstkey);
                    notifyKeyspaceEvent(NOTIFY_GENERIC,"del",dstkey,c->db->id);
                    server.dirty++;
                }
                // 回复加入dstkey集合的元素数量0。
                addReply(c,shared.czero);
            } else {
                // 回复空的集合给client。
                addReply(c,shared.emptyset[c->resp]);
            }
            return;
        }
        // 确保要求交集的对象类型为set。
        if (checkType(c,setobj,OBJ_SET)) {
            zfree(sets);
            return;
        }
        // sets存储要求交集的所有集合。
        sets[j] = setobj;
    }
    /* Sort sets from the smallest to largest, this will improve our
     * algorithm's performance */
    // 多个set集合，按照其中元素个数从小到大进行排序，这样有利于提升后面求交集算法的性能。
    qsort(sets,setnum,sizeof(robj*),qsortCompareSetsByCardinality);

    /* The first thing we should output is the total number of elements...
     * since this is a multi-bulk write, but at this stage we don't know
     * the intersection set size, so we use a trick, append an empty object
     * to the output list and save the pointer to later modify it with the
     * right length */
    if (!dstkey) {
        // 如果交集是要回复给client，则首先写入回复中的是交集元素的总个数。
        // 但是现在我们并不知道总回复元素个数，所以使用一个小技巧，在回复list尾部加一个NULL的占位节点，后面再向这个地方填充长度信息。
        replylen = addReplyDeferredLen(c);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with an empty set inside */
        // 如果交集是写入新的集合中，这里先创建一个空的set，后面往里面加入交集元素。
        dstset = createIntsetObject();
    }

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    // 迭代sets中第一个（元素最少）集合的所有元素，检查元素是否都在sets所有其他集合中，如果有一个集合不包含该元素，则就不用加入到dstset中。
    si = setTypeInitIterator(sets[0]);
    while((encoding = setTypeNext(si,&elesds,&intobj)) != -1) {
        // 迭代取到第一个集合的元素，for遍历第二个到后面所有的集合，判断是否包含该元素。
        for (j = 1; j < setnum; j++) {
            // 如果集合与第一个是同一个集合，显然包含，continue跳过。
            if (sets[j] == sets[0]) continue;
            if (encoding == OBJ_ENCODING_INTSET) {
                // 如果取到的数据编码方式（即第一个集合的编码方式）是intset，
                // 则我们可以统一将数据转为sds，然后调用setTypeIsMember判断是否在集合中。
                // 但是这里如果后面集合也是intset编码方式，则可以直接使用intsetFind来判断，这样更简单快速。
                /* intset with intset is simple... and fast */
                if (sets[j]->encoding == OBJ_ENCODING_INTSET &&
                    !intsetFind((intset*)sets[j]->ptr,intobj))
                {
                    break;
                /* in order to compare an integer with an object we
                 * have to use the generic function, creating an object
                 * for this */
                } else if (sets[j]->encoding == OBJ_ENCODING_HT) {
                    // 如果后面集合是hash表存储，则为了比较，我们需要将数字转为sds字符串，然后再调用setTypeIsMember来判断。
                    elesds = sdsfromlonglong(intobj);
                    if (!setTypeIsMember(sets[j],elesds)) {
                        sdsfree(elesds);
                        break;
                    }
                    sdsfree(elesds);
                }
            } else if (encoding == OBJ_ENCODING_HT) {
                // 如果取到的数据编码方式（即第一个集合的编码方式）是hash表，则直接调用setTypeIsMember来判断是否在后面的集合中。
                if (!setTypeIsMember(sets[j],elesds)) {
                    break;
                }
            }
        }

        /* Only take action when all sets contain the member */
        // 只有当j==setnum时，表示遍历完了后面所有集合，且都包含该元素。否则如果有集合不包含，则会break提前跳出循环，从而有j<setnum。
        if (j == setnum) {
            // 该元素为所有集合的交集，如果有dstkey则需要将该元素加入到dstset中，否则我们将元素写入到回复buf中。
            if (!dstkey) {
                if (encoding == OBJ_ENCODING_HT)
                    addReplyBulkCBuffer(c,elesds,sdslen(elesds));
                else
                    addReplyBulkLongLong(c,intobj);
                cardinality++;
            } else {
                if (encoding == OBJ_ENCODING_INTSET) {
                    // 如果遍历到的数据编码方式是INTSET，那么第一个集合的编码方式就是INTSET，那么我们知道的交集的编码方式肯定是INTSET。
                    // 所以这里不用将数字转为sds，再调用setTypeAdd处理。可以直接调用intsetAdd(dstset->ptr, intobj, NULL)处理。
                    dstset->ptr = intsetAdd(dstset->ptr, intobj, NULL);
                    // elesds = sdsfromlonglong(intobj);
                    // setTypeAdd(dstset,elesds);
                    // sdsfree(elesds);
                } else {
                    setTypeAdd(dstset,elesds);
                }
            }
        }
    }
    setTypeReleaseIterator(si);

    if (dstkey) {
        /* Store the resulting set into the target, if the intersection
         * is not an empty set. */
        if (setTypeSize(dstset) > 0) {
            // 如果有dstkey，且求的交集非空，则我们将dstset设置为dstkey的value，并返回交集元素的个数。
            setKey(c,c->db,dstkey,dstset);
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(NOTIFY_SET,"sinterstore",
                dstkey,c->db->id);
            server.dirty++;
        } else {
            // 如果有dstkey，且求的交集为空，则我们要从db中移除dstkey，也就是将它更新为空集合。回复交集元素个数0给client。
            addReply(c,shared.czero);
            if (dbDelete(c->db,dstkey)) {
                server.dirty++;
                signalModifiedKey(c,c->db,dstkey);
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",dstkey,c->db->id);
            }
        }
        decrRefCount(dstset);
    } else {
        // 如果没有dstkey，表示要将交集数据回复给client。
        // 我们前面已经将元素都加入到回复huf中了，这里需要在之前NULL占位节点位置写入最终回复client的总元素个数。
        setDeferredSetLen(c,replylen,cardinality);
    }
    zfree(sets);
}

// SINTER key1 [key2]，返回所有集合的交集。
void sinterCommand(client *c) {
    sinterGenericCommand(c,c->argv+1,c->argc-1,NULL);
}

// SINTERSTORE dst key1 [key2]，求所有集合的交集，并将结果存储在dst集合中。
void sinterstoreCommand(client *c) {
    sinterGenericCommand(c,c->argv+2,c->argc-2,c->argv[1]);
}

#define SET_OP_UNION 0
#define SET_OP_DIFF 1
#define SET_OP_INTER 2

// 求并集、差集的统一处理方法。SPOP所有元素时也使用这个方法处理的。
void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum,
                              robj *dstkey, int op) {
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    sds ele;
    int j, cardinality = 0;
    int diff_algo = 1;

    // 遍历获取所有等待进行求并集和差集的集合对象。
    for (j = 0; j < setnum; j++) {
        // 根据key获取对象
        robj *setobj = dstkey ?
            lookupKeyWrite(c->db,setkeys[j]) :
            lookupKeyRead(c->db,setkeys[j]);
        if (!setobj) {
            // db中没有该对象，我们设为NULL，后面会当作空集合来处理。
            sets[j] = NULL;
            continue;
        }
        // 确保获取到对象的类型是set
        if (checkType(c,setobj,OBJ_SET)) {
            zfree(sets);
            return;
        }
        // 集合对象加入到sets中
        sets[j] = setobj;
    }

    /* Select what DIFF algorithm to use.
     *
     * Algorithm 1 is O(N*M) where N is the size of the element first set
     * and M the total number of sets.
     *
     * Algorithm 2 is O(N) where N is the total number of elements in all
     * the sets.
     *
     * We compute what is the best bet with the current input here. */
    // 选择哪个DIFF差集算法使用：
    //  算法1复杂度是O(N*M)，N是第一个集合的元素个数，M是DIFF处理的后面所有集合数量。
    //  算法2复杂度是O(N)，N是所有集合总共的元素数量。
    // 我们这里根据输入数据，来判断使用哪个算法最好。其实最终就是对比 M*N/2 与 所有集合总元素 哪个大来选择的。
    if (op == SET_OP_DIFF && sets[0]) {
        long long algo_one_work = 0, algo_two_work = 0;

        // 计算两种算法总的对比元素的次数。
        for (j = 0; j < setnum; j++) {
            if (sets[j] == NULL) continue;

            algo_one_work += setTypeSize(sets[0]);
            algo_two_work += setTypeSize(sets[j]);
        }

        /* Algorithm 1 has better constant times and performs less operations
         * if there are elements in common. Give it some advantage. */
        // 因为算法一有更优的常数项，执行的操作更少，所以我们algo_one_work除以2后再进行比较。
        algo_one_work /= 2;
        diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2;

        if (diff_algo == 1 && setnum > 1) {
            /* With algorithm 1 it is better to order the sets to subtract
             * by decreasing size, so that we are more likely to find
             * duplicated elements ASAP. */
            // 当使用算法1时，我们把后面的sets按元素由多到少进行排序，这样可能可以更快的找到重复元素来移除掉。
            qsort(sets+1,setnum-1,sizeof(robj*),
                qsortCompareSetsByRevCardinality);
        }
    }

    /* We need a temp set object to store our union. If the dstkey
     * is not NULL (that is, we are inside an SUNIONSTORE operation) then
     * this set object will be the resulting object to set into the target key*/
    // 我们需要一个临时的set对象来存储结果数据。如果dstkey非空，则会将这个set更新为该dstkey的新值。
    dstset = createIntsetObject();

    if (op == SET_OP_UNION) {
        /* Union is trivial, just add every element of every set to the
         * temporary set. */
        // union处理很简单，只需要遍历将每个集合的每个元素加入到临时set中即可。
        for (j = 0; j < setnum; j++) {
            // 如果当前集合为空，直接跳过
            if (!sets[j]) continue; /* non existing keys are like empty sets */

            // 遍历当前集合的每个元素，并使用setTypeAdd将元素加入到集合中。
            si = setTypeInitIterator(sets[j]);
            while((ele = setTypeNextObject(si)) != NULL) {
                if (setTypeAdd(dstset,ele)) cardinality++;
                sdsfree(ele);
            }
            setTypeReleaseIterator(si);
        }
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 1) {
        /* DIFF Algorithm 1:
         *
         * We perform the diff by iterating all the elements of the first set,
         * and only adding it to the target set if the element does not exist
         * into all the other sets.
         *
         * This way we perform at max N*M operations, where N is the size of
         * the first set, and M the number of sets. */
        // DIFF算法1：迭代第一个集合中的所有元素，当该元素不在后面所有的集合中时，我们才将它加入到dstset中。O(N*M)
        // we set skip to 1 when there is a same set to the first which
        // means the result of diff is empty set and no need to check，
        // and since the sets are sorted, we can just remain the first
        // one of the continuously same sets。
        int skip = 0;
        robj *last = NULL;
        for (j = 1; j < setnum && !skip; j++) {
            if (sets[j] == sets[0]) skip =1;
            else if (last == sets[j]) sets[j] = NULL;
            else last = sets[j];
        }

        si = setTypeInitIterator(sets[0]);
        // 迭代第一个集合中的元素处理
        while(!skip && (ele = setTypeNextObject(si)) != NULL) {
            // 遍历后面所有的集合处理：
            // 1、如果集合为空则跳过。
            // 2、如果集合与第一个集合是同一个，则显然元素在集合中，不能加入到dstset中跳过后面集合检查。
            // 3、setTypeIsMember检查元素是否在集合中，在则不能加入到dstset中跳过后面集合检查。
            for (j = 1; j < setnum; j++) {
                if (!sets[j]) continue; /* no key is an empty set. */
                // if (sets[j] == sets[0]) break; /* same set! */
                if (setTypeIsMember(sets[j],ele)) break;
            }
            if (j == setnum) {
                /* There is no other set with this element. Add it. */
                // 到这里说明后面所有集合中都不包含该元素，则将该元素加入到dstset中。
                setTypeAdd(dstset,ele);
                cardinality++;
            }
            sdsfree(ele);
        }
        setTypeReleaseIterator(si);
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 2) {
        /* DIFF Algorithm 2:
         *
         * Add all the elements of the first set to the auxiliary set.
         * Then remove all the elements of all the next sets from it.
         *
         * This is O(N) where N is the sum of all the elements in every
         * set. */
        // DIFF算法2：将第一个set的所有元素先加入到辅助set中，然后遍历后面所有sets的元素，从辅助set中移除。O(N)
        for (j = 0; j < setnum; j++) {
            // 空集合跳过
            if (!sets[j]) continue; /* non existing keys are like empty sets */

            si = setTypeInitIterator(sets[j]);
            // 迭代所有的集合进行处理。如果是第一个集合则将元素加入到dstset，如果是其他集合则从dstset中移除该元素。
            while((ele = setTypeNextObject(si)) != NULL) {
                if (j == 0) {
                    if (setTypeAdd(dstset,ele)) cardinality++;
                } else {
                    if (setTypeRemove(dstset,ele)) cardinality--;
                }
                sdsfree(ele);
            }
            setTypeReleaseIterator(si);

            /* Exit if result set is empty as any additional removal
             * of elements will have no effect. */
            // 当我们处理完一个集合后，发现dstset为空了，那么可以不再处理后面的集合了，因为diff结果为空集，后面的集合对结果不再有影响。
            if (cardinality == 0) break;
        }
    }

    /* Output the content of the resulting set, if not in STORE mode */
    if (!dstkey) {
        // 如果没有dstkey，则集合结果全部回复给client。
        // 先写入结果集合总长度，然后迭代结果集合，依次将元素加入到回复buf中。
        addReplySetLen(c,cardinality);
        si = setTypeInitIterator(dstset);
        while((ele = setTypeNextObject(si)) != NULL) {
            addReplyBulkCBuffer(c,ele,sdslen(ele));
            sdsfree(ele);
        }
        setTypeReleaseIterator(si);
        // 释放dstset
        server.lazyfree_lazy_server_del ? freeObjAsync(NULL, dstset) :
                                          decrRefCount(dstset);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with the result set inside */
        if (setTypeSize(dstset) > 0) {
            // 如果有dstkey，且交集非空，则设置db中dstkey的值为dstset，并回复client交集总元素数。
            setKey(c,c->db,dstkey,dstset);
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(NOTIFY_SET,
                op == SET_OP_UNION ? "sunionstore" : "sdiffstore",
                dstkey,c->db->id);
            server.dirty++;
        } else {
            // 如果有dstkey，但交集为空，则需要从db中移除dstkey，并回复client交集总元素数0。
            addReply(c,shared.czero);
            if (dbDelete(c->db,dstkey)) {
                server.dirty++;
                signalModifiedKey(c,c->db,dstkey);
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",dstkey,c->db->id);
            }
        }
        decrRefCount(dstset);
    }
    zfree(sets);
}

// SUNION key1 [key2]，返回给定集合的并集。
void sunionCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,SET_OP_UNION);
}

// SUNIONSTORE dst key1 [key2]，求指定集合的并集，并存储在dst集合中。
void sunionstoreCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],SET_OP_UNION);
}

// SDIFF key1 [key2]，返回第一个集合与其他集合的差集。
void sdiffCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,SET_OP_DIFF);
}

// SDIFFSTORE dst key1 [key2]，求第一个集合与其他集合的差集，并存储到dst集合中。
void sdiffstoreCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],SET_OP_DIFF);
}

// SSCAN key cursor [MATCH pattern] [COUNT count]，扫描set对象中的所有元素。
void sscanCommand(client *c) {
    robj *set;
    unsigned long cursor;

    // 解析cursor参数
    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    // 获取对象，确保对象类型是set。
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,set,OBJ_SET)) return;
    // 调用scanGenericCommand来处理该set的扫描。
    scanGenericCommand(c,set,cursor);
}
