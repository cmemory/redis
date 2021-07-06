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
 * List API
 *----------------------------------------------------------------------------*/

/* The function pushes an element to the specified list object 'subject',
 * at head or tail position as specified by 'where'.
 *
 * There is no need for the caller to increment the refcount of 'value' as
 * the function takes care of it if needed. */
// 该函数将元素push到指定的列表对象subject中，where决定从head或tail加入。
// 调用方不需要增加value的引用计数，因为这个函数会在需要的时候自己处理。
void listTypePush(robj *subject, robj *value, int where) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        // 确定是在head/tail push
        int pos = (where == LIST_HEAD) ? QUICKLIST_HEAD : QUICKLIST_TAIL;
        if (value->encoding == OBJ_ENCODING_INT) {
            char buf[32];
            // 如果value的编码是INT，则需要转成string再存入。
            ll2string(buf, 32, (long)value->ptr);
            quicklistPush(subject->ptr, buf, strlen(buf), pos);
        } else {
            quicklistPush(subject->ptr, value->ptr, sdslen(value->ptr), pos);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
}

void *listPopSaver(unsigned char *data, unsigned int sz) {
    return createStringObject((char*)data,sz);
}

robj *listTypePop(robj *subject, int where) {
    long long vlong;
    robj *value = NULL;

    // 根据where参数决定是在head还是tail操作。
    int ql_where = where == LIST_HEAD ? QUICKLIST_HEAD : QUICKLIST_TAIL;
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        // 目前list只有QUICKLIST编码，即以ziplist为节点的双向链表结构。
        if (quicklistPopCustom(subject->ptr, ql_where, (unsigned char **)&value,
                               NULL, &vlong, listPopSaver)) {
            if (!value)
                // 如果不是字符串类型，我们需要自己对数字类型来创建对象。（这个其实也可以放到listPopSaver中一起处理的）
                value = createStringObjectFromLongLong(vlong);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
    return value;
}

// 获取list总元素数
unsigned long listTypeLength(const robj *subject) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        // quicklist中count字段保存了总元素数，直接获取返回就可以。
        return quicklistCount(subject->ptr);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Initialize an iterator at the specified index. */
// 初始化list类型的迭代器
listTypeIterator *listTypeInitIterator(robj *subject, long index,
                                       unsigned char direction) {
    listTypeIterator *li = zmalloc(sizeof(listTypeIterator));
    // list类型迭代器包含对应的list、list编码类型、迭代方向以及目前唯一使用的quicklist迭代器。
    li->subject = subject;
    li->encoding = subject->encoding;
    li->direction = direction;
    li->iter = NULL;
    /* LIST_HEAD means start at TAIL and move *towards* head.
     * LIST_TAIL means start at HEAD and move *towards tail. */
    // LIST_HEAD对于正常list操作元素表示在头部或前面位置，对于遍历这里表示从后向前。LIST_TAIL对于遍历则表示从前往后。
    int iter_direction =
        direction == LIST_HEAD ? AL_START_TAIL : AL_START_HEAD;
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {
        // 目前list编码方式只有quicklist，这里初始化quicklis迭代器。注意quicklis节点底层编码方式目前是ziplist。
        li->iter = quicklistGetIteratorAtIdx(li->subject->ptr,
                                             iter_direction, index);
    } else {
        serverPanic("Unknown list encoding");
    }
    return li;
}

/* Clean up the iterator. */
// 释放迭代器
void listTypeReleaseIterator(listTypeIterator *li) {
    zfree(li->iter);
    zfree(li);
}

/* Advances the position of the iterator and then stores the pointer to
 * current entry in the provided entry structure. Returns 1 when the current
 * entry is in fact an entry, 0 otherwise. */
// 迭代器前进一步，并获取新位置上指向元素的指针存储到传入entry结构中。
int listTypeNext(listTypeIterator *li, listTypeEntry *entry) {
    /* Protect from converting when iterating */
    // 防止迭代时编码变化。
    serverAssert(li->subject->encoding == li->encoding);

    entry->li = li;
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {
        // quicklist编码，调用底层函数来获取next entry
        return quicklistNext(li->iter, &entry->entry);
    } else {
        serverPanic("Unknown list encoding");
    }
    return 0;
}

/* Return entry or NULL at the current position of the iterator. */
// 获取当前迭代到的entry中的元素数据
robj *listTypeGet(listTypeEntry *entry) {
    robj *value = NULL;
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        if (entry->entry.value) {
            // 如果value不为空，则为字符串值，直接创建string对象。
            value = createStringObject((char *)entry->entry.value,
                                       entry->entry.sz);
        } else {
            // 数字类型，不同的处理方式
            value = createStringObjectFromLongLong(entry->entry.longval);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
    return value;
}

// 在迭代到的元素前/后插入新元素。注意插入元素后，迭代器会失效，这里没处理需要调用方来处理（释放或重建迭代器）。
void listTypeInsert(listTypeEntry *entry, robj *value, int where) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        value = getDecodedObject(value);
        sds str = value->ptr;
        size_t len = sdslen(str);
        // 根据传入位置（前/后）调用不同的插入方法处理
        if (where == LIST_TAIL) {
            quicklistInsertAfter((quicklist *)entry->entry.quicklist,
                                 &entry->entry, str, len);
        } else if (where == LIST_HEAD) {
            quicklistInsertBefore((quicklist *)entry->entry.quicklist,
                                  &entry->entry, str, len);
        }
        decrRefCount(value);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Compare the given object with the entry at the current position. */
// 对比给定的对象o与entry指向的数据是否一致。
int listTypeEqual(listTypeEntry *entry, robj *o) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        serverAssertWithInfo(NULL,o,sdsEncodedObject(o));
        // 使用quicklistCompare，进一步调用ziplistCompare来进行对比。
        return quicklistCompare(entry->entry.zi,o->ptr,sdslen(o->ptr));
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Delete the element pointed to. */
// 迭代删除entry所指向的元素。参数iter和entry应该配套，否则可能有意外结果。
void listTypeDelete(listTypeIterator *iter, listTypeEntry *entry) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        // 调用quicklistDelEntry进行迭代删除。删除了当前迭代的元素，会更新迭代器iter指向信息。
        quicklistDelEntry(iter->iter, &entry->entry);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Create a quicklist from a single ziplist */
// 从单个ziplist来创建quicklist结构。会将ziplist中的数据遍历一个个加入到quicklist中。
// 目前list只有quicklist编码，已经没有ziplist编码了。这个函数主要在RDB load时使用，用于兼容老的RDB文件。
void listTypeConvert(robj *subject, int enc) {
    serverAssertWithInfo(NULL,subject,subject->type==OBJ_LIST);
    serverAssertWithInfo(NULL,subject,subject->encoding==OBJ_ENCODING_ZIPLIST);

    if (enc == OBJ_ENCODING_QUICKLIST) {
        size_t zlen = server.list_max_ziplist_size;
        int depth = server.list_compress_depth;
        // 创建新的quicklist，将原ziplist中元素一次加入quicklist中
        subject->ptr = quicklistCreateFromZiplist(zlen, depth, subject->ptr);
        subject->encoding = OBJ_ENCODING_QUICKLIST;
    } else {
        serverPanic("Unsupported list conversion");
    }
}

/* This is a helper function for the COPY command.
 * Duplicate a list object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * The resulting object always has refcount set to 1 */
// COPY命令的帮助函数。copy一个list对象返回，保证返回的对象与原list对象有相同的编码方式。
// 注意返回对象的refcount总会是1。后面不主动删除就一直不会释放？
robj *listTypeDup(robj *o) {
    robj *lobj;

    serverAssert(o->type == OBJ_LIST);

    switch (o->encoding) {
        case OBJ_ENCODING_QUICKLIST:
            // 复制list，创建obj对象，注意obj默认编码方式是row，需要要手动设置为quicklist。
            lobj = createObject(OBJ_LIST, quicklistDup(o->ptr));
            lobj->encoding = OBJ_ENCODING_QUICKLIST;
            break;
        default:
            serverPanic("Unknown list encoding");
            break;
    }
    return lobj;
}

/*-----------------------------------------------------------------------------
 * List Commands
 *----------------------------------------------------------------------------*/

/* Implements LPUSH/RPUSH/LPUSHX/RPUSHX. 
 * 'xx': push if key exists. */
// LPUSH/RPUSH/LPUSHX/RPUSHX 命令的通用实现。
// where 表示是在list头部还是尾部进行push。
// xx 表示存key在才push，不存在时不会创建list，而是直接回复client 0。主要用于实现LPUSHX/RPUSHX命令。
void pushGenericCommand(client *c, int where, int xx) {
    int j;

    // 从db中查询key对应的value用于写操作。
    robj *lobj = lookupKeyWrite(c->db, c->argv[1]);
    // 检查type如果不是OBJ_LIST，则会回复client type err。这里直接return。
    if (checkType(c,lobj,OBJ_LIST)) return;
    if (!lobj) {
        // 返回是NULL，key不存在。如果有xx标识，则我们直接回复client 0；否则我们会新创建list，后面写入元素。
        if (xx) {
            addReply(c, shared.czero);
            return;
        }

        // 创建新的list作为key的value，加入到db中。
        lobj = createQuicklistObject();
        quicklistSetOptions(lobj->ptr, server.list_max_ziplist_size,
                            server.list_compress_depth);
        dbAdd(c->db,c->argv[1],lobj);
    }

    // 将我们要push的元素添加到list中。
    for (j = 2; j < c->argc; j++) {
        listTypePush(lobj,c->argv[j],where);
        server.dirty++;
    }

    // 回复client list中元素总个数。
    addReplyLongLong(c, listTypeLength(lobj));

    // key变更通知处理。
    char *event = (where == LIST_HEAD) ? "lpush" : "rpush";
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_LIST,event,c->argv[1],c->db->id);
}

/* LPUSH <key> <element> [<element> ...] */
// 从list左边push，即在head插入元素。list不存在则自动创建。
void lpushCommand(client *c) {
    pushGenericCommand(c,LIST_HEAD,0);
}

/* RPUSH <key> <element> [<element> ...] */
// 从list右边push，即在tail插入元素。list不存在则自动创建。
void rpushCommand(client *c) {
    pushGenericCommand(c,LIST_TAIL,0);
}

/* LPUSHX <key> <element> [<element> ...] */
// lpush if exist
void lpushxCommand(client *c) {
    pushGenericCommand(c,LIST_HEAD,1);
}

/* RPUSH <key> <element> [<element> ...] */
// rpush if exist
void rpushxCommand(client *c) {
    pushGenericCommand(c,LIST_TAIL,1);
}

/* LINSERT <key> (BEFORE|AFTER) <pivot> <element> */
// 在list中从左向右查询指定的pivot元素，然后在该元素前/后插入element元素。
void linsertCommand(client *c) {
    int where;
    robj *subject;
    listTypeIterator *iter;
    listTypeEntry entry;
    int inserted = 0;

    // 根据参数确定是在指定元素的前面还是后面插入
    if (strcasecmp(c->argv[2]->ptr,"after") == 0) {
        where = LIST_TAIL;
    } else if (strcasecmp(c->argv[2]->ptr,"before") == 0) {
        where = LIST_HEAD;
    } else {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    // db中查询key对应的list，不存在则返回0给client。如果存在key，但不是list则返回err给client。
    if ((subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,subject,OBJ_LIST)) return;

    /* Seek pivot from head to tail */
    // 从左向右查询pivot元素的位置。基本上遍历链表处理，所以这个命令应该少用。
    iter = listTypeInitIterator(subject,0,LIST_TAIL);
    // 初始化迭代器后，挨个遍历获取entry数据。
    while (listTypeNext(iter,&entry)) {
        if (listTypeEqual(&entry,c->argv[3])) {
            // 对比entry数据与pivot是否一致，如果一致则在该元素前/后插入元素。
            listTypeInsert(&entry,c->argv[4],where);
            inserted = 1;
            break;
        }
    }
    listTypeReleaseIterator(iter);

    if (inserted) {
        // 如果有插入元素，则key变更通知处理。
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST,"linsert",
                            c->argv[1],c->db->id);
        server.dirty++;
    } else {
        /* Notify client of a failed insert */
        // 回复client插入失败。
        addReplyLongLong(c,-1);
        return;
    }

    // 如果成功插入，则返回lis中总的元素数。
    addReplyLongLong(c,listTypeLength(subject));
}

/* LLEN <key> */
// 获取list中总元素数。
void llenCommand(client *c) {
    // 如果key对应list不存在 或 key的类型不是list，则直接回复对应的信息或err给client。
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.czero);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;
    // 存在对应list，返回总元素数。
    addReplyLongLong(c,listTypeLength(o));
}

/* LINDEX <key> <index> */
// 获取指定index处的元素
void lindexCommand(client *c) {
    // db中根据key获取list。
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;
    long index;

    // 解析index参数
    if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != C_OK))
        return;

    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistEntry entry;
        // quicklist获取指定index处的entry
        if (quicklistIndex(o->ptr, index, &entry)) {
            // 根据entry值是字符串还是数字调用不同函数处理回复client
            if (entry.value) {
                addReplyBulkCBuffer(c, entry.value, entry.sz);
            } else {
                addReplyBulkLongLong(c, entry.longval);
            }
        } else {
            addReplyNull(c);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* LSET <key> <index> <element> */
// 使用指定的element来替换list中index处的元素。如果index超出范围，则返回outofrangeerr。
void lsetCommand(client *c) {
    // 获取list
    robj *o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr);
    if (o == NULL || checkType(c,o,OBJ_LIST)) return;
    long index;
    robj *value = c->argv[3];

    // 解析index参数
    if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != C_OK))
        return;

    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklist *ql = o->ptr;
        // 替换元素值
        int replaced = quicklistReplaceAtIndex(ql, index,
                                               value->ptr, sdslen(value->ptr));
        if (!replaced) {
            // index超出范围，替换失败
            addReplyErrorObject(c,shared.outofrangeerr);
        } else {
            // 替换成功，我们修改了key，通知key变更处理。
            addReply(c,shared.ok);
            signalModifiedKey(c,c->db,c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_LIST,"lset",c->argv[1],c->db->id);
            server.dirty++;
        }
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* A helper for replying with a list's range between the inclusive start and end
 * indexes as multi-bulk, with support for negative indexes. Note that start
 * must be less than end or an empty array is returned. When the reverse
 * argument is set to a non-zero value, the reply is reversed so that elements
 * are returned from end to start. */
// 帮助函数，用于将[start, end]范围的list range数据处理为multi-bulk回复，支持负数索引。
// 注意start必须要小于end，否则将返回一个空的数组。当reverse为非零时，元素将从end到start的顺序返回。
void addListRangeReply(client *c, robj *o, long start, long end, int reverse) {
    long rangelen, llen = listTypeLength(o);

    /* Convert negative indexes. */
    // 将负的index转为正的。
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    // 不变式：start>=0，所以end<0时 start>end 总为true。
    // 这里判断查询范围的合法性，当start>end 或 start>=length时，区间不合法，返回空的数组回复。
    if (start > end || start >= llen) {
        addReply(c,shared.emptyarray);
        return;
    }
    // 区间修复end index。
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    // 构造multi-bulk回复。先是'*'开头的数字表示回复总长度。
    addReplyArrayLen(c,rangelen);
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        int from = reverse ? end : start;
        int direction = reverse ? LIST_HEAD : LIST_TAIL;
        // 根据遍历方向，初始化迭代器。
        listTypeIterator *iter = listTypeInitIterator(o,from,direction);

        // 需要查询rangelen个数据，所以迭代这么多次
        while(rangelen--) {
            listTypeEntry entry;
            // 迭代获取entry
            listTypeNext(iter, &entry);
            quicklistEntry *qe = &entry.entry;
            // 对于遍历到的数据，构造bulk回复
            if (qe->value) {
                addReplyBulkCBuffer(c,qe->value,qe->sz);
            } else {
                addReplyBulkLongLong(c,qe->longval);
            }
        }
        listTypeReleaseIterator(iter);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* A housekeeping helper for list elements popping tasks. */
// pop的一个辅助函数
void listElementsRemoved(client *c, robj *key, int where, robj *o, long count) {
    char *event = (where == LIST_HEAD) ? "lpop" : "rpop";

    // 发送key变更通知
    notifyKeyspaceEvent(NOTIFY_LIST, event, key, c->db->id);
    if (listTypeLength(o) == 0) {
        // 如果list pop后没有元素了，我们需要整个删除该list。
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, c->db->id);
        dbDelete(c->db, key);
    }
    // key变更，与key相关client事务和client缓存清理
    signalModifiedKey(c, c->db, key);
    server.dirty += count;
}

/* Implements the generic list pop operation for LPOP/RPOP.
 * The where argument specifies which end of the list is operated on. An
 * optional count may be provided as the third argument of the client's
 * command. */
// 实现LPOP/RPOP操作的通用函数。where指定list的哪一侧进行pop。可选的第三个参数用于指定pop的元素数量(redis 6)。
void popGenericCommand(client *c, int where) {
    long count = 0;
    robj *value;

    if (c->argc > 3) {
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",
                            c->cmd->name);
        return;
    } else if (c->argc == 3) {
        /* Parse the optional count argument. */
        // 解析第三个count参数，必须要>0，才会pop数据。
        if (getPositiveLongFromObjectOrReply(c,c->argv[2],&count,NULL) != C_OK) 
            return;
        if (count == 0) {
            /* Fast exit path. */
            addReplyNullArray(c);
            return;
        }
    }

    // 获取list
    robj *o = lookupKeyWriteOrReply(c, c->argv[1], shared.null[c->resp]);
    if (o == NULL || checkType(c, o, OBJ_LIST))
        return;

    if (!count) {
        /* Pop a single element. This is POP's original behavior that replies
         * with a bulk string. */
        // 没有count参数，兼容老的pop指令形式，返回bulk字符串。
        value = listTypePop(o,where);
        serverAssert(value != NULL);
        addReplyBulk(c,value);
        decrRefCount(value);
        // pop执行完，调用listElementsRemoved进行后续处理。
        listElementsRemoved(c,c->argv[1],where,o,1);
    } else {
        /* Pop a range of elements. An addition to the original POP command,
         *  which replies with a multi-bulk. */
        // pop多个元素。
        long llen = listTypeLength(o);
        long rangelen = (count > llen) ? llen : count;
        // 根据pop的count，构造range
        long rangestart = (where == LIST_HEAD) ? 0 : -rangelen;
        long rangeend = (where == LIST_HEAD) ? rangelen - 1 : -1;
        int reverse = (where == LIST_HEAD) ? 0 : 1;

        // range查询，并回复multi-bulk数据给client
        addListRangeReply(c,o,rangestart,rangeend,reverse);
        // 范围删除元素
        quicklistDelRange(o->ptr,rangestart,rangelen);
        // pop执行完，调用listElementsRemoved进行后续处理。
        listElementsRemoved(c,c->argv[1],where,o,rangelen);
    }
}

/* LPOP <key> [count] */
// 从左边pop数据，可选参数count可指定pop元素个数
void lpopCommand(client *c) {
    popGenericCommand(c,LIST_HEAD);
}

/* RPOP <key> [count] */
void rpopCommand(client *c) {
    popGenericCommand(c,LIST_TAIL);
}

/* LRANGE <key> <start> <stop> */
// range查询数据
void lrangeCommand(client *c) {
    robj *o;
    long start, end;

    // 解析范围索引参数
    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != C_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != C_OK)) return;

    // 获取list
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyarray)) == NULL
         || checkType(c,o,OBJ_LIST)) return;

    // range查询，并回复multi-bulk数据给client
    addListRangeReply(c,o,start,end,0);
}

/* LTRIM <key> <start> <stop> */
void ltrimCommand(client *c) {
    robj *o;
    long start, end, llen, ltrim, rtrim;

    // 解析trim的范围索引参数
    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != C_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != C_OK)) return;

    // 取到list
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.ok)) == NULL ||
        checkType(c,o,OBJ_LIST)) return;
    llen = listTypeLength(o);

    /* convert negative indexes */
    // 负的index转化为正索引。
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    // 索引区间修复。
    if (start > end || start >= llen) {
        /* Out of range start or start > end result in empty list */
        // 给定的区间不存在，或者超出了list范围，那么trim将删除整个list元素，最终只剩下空的list。
        ltrim = llen;
        rtrim = 0;
    } else {
        // 修复end索引
        if (end >= llen) end = llen-1;
        ltrim = start;
        rtrim = llen-end-1;
    }

    /* Remove list elements to perform the trim */
    // 通过移除list中的元素来实现trim。
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        // 先删除trim指定区间左边的所有元素，在删除指定区间右边的所有元素。quicklistDelRange第三个参数是删除的数量。
        // 如果是需要删除整个list元素，第一次调用就全删除掉了。第二次调用，在使用start索引查询entry时，发现不合法，会返回0，所以没问题。
        quicklistDelRange(o->ptr,0,ltrim);
        quicklistDelRange(o->ptr,-rtrim,rtrim);
    } else {
        serverPanic("Unknown list encoding");
    }

    // keyspace变化通知订阅者。
    notifyKeyspaceEvent(NOTIFY_LIST,"ltrim",c->argv[1],c->db->id);
    if (listTypeLength(o) == 0) {
        // 如果list元素全被移除了，这里从db删除该key
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }
    // 关于key的client事务和client缓存追踪的清理
    signalModifiedKey(c,c->db,c->argv[1]);
    server.dirty += (ltrim + rtrim);
    addReply(c,shared.ok);
}

/* LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
 *
 * The "rank" is the position of the match, so if it is 1, the first match
 * is returned, if it is 2 the second match is returned and so forth.
 * It is 1 by default. If negative has the same meaning but the search is
 * performed starting from the end of the list.
 *
 * If COUNT is given, instead of returning the single element, a list of
 * all the matching elements up to "num-matches" are returned. COUNT can
 * be combiled with RANK in order to returning only the element starting
 * from the Nth. If COUNT is zero, all the matching elements are returned.
 *
 * MAXLEN tells the command to scan a max of len elements. If zero (the
 * default), all the elements in the list are scanned if needed.
 *
 * The returned elements indexes are always referring to what LINDEX
 * would return. So first element from head is 0, and so forth. */
// LPOS从头到尾扫描列表，返回list中指定元素的的索引，默认返回第一个匹配的索引。
// 可使用rank参数来指定所匹配的第几个返回。为1表示返回第一个匹配的索引，为2表示返回第二个匹配的索引。
// 如果rank指定为负数，则表示list从后往前进行匹配，返回第abs(rank)个匹配的索引。
// COUNT参数表示返回多少个匹配的索引，如果为零表示返回所有。可结合rank参数一起使用，表示从第rank个匹配开始返回count个匹配的索引。
// MAXLEN参数表示最多扫描list的多少个元素来进行对比匹配，如果为0表示最多会扫描所有元素。
// 返回的元素索引都是从0开始计算的。
void lposCommand(client *c) {
    robj *o, *ele;
    ele = c->argv[2];
    // 默认从前往后扫描
    int direction = LIST_TAIL;
    // rank=1表示默认返回1个索引，count=-1表示默认没有该选项参数，maxlen=0表示默认最多会扫描所有元素进行对比。
    long rank = 1, count = -1, maxlen = 0; /* Count -1: option not given. */

    /* Parse the optional arguments. */
    for (int j = 3; j < c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc-1)-j;

        if (!strcasecmp(opt,"RANK") && moreargs) {
            j++;
            // 解析rank参数
            if (getLongFromObjectOrReply(c, c->argv[j], &rank, NULL) != C_OK)
                return;
            if (rank == 0) {
                addReplyError(c,"RANK can't be zero: use 1 to start from "
                                "the first match, 2 from the second, ...");
                return;
            }
        } else if (!strcasecmp(opt,"COUNT") && moreargs) {
            j++;
            // 解析count参数
            if (getPositiveLongFromObjectOrReply(c, c->argv[j], &count,
              "COUNT can't be negative") != C_OK)
                return;
        } else if (!strcasecmp(opt,"MAXLEN") && moreargs) {
            j++;
            // 解析最大扫描数量参数
            if (getPositiveLongFromObjectOrReply(c, c->argv[j], &maxlen, 
              "MAXLEN can't be negative") != C_OK)
                return;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    /* A negative rank means start from the tail. */
    // 如果rank传的负数，表示从后往前扫描
    if (rank < 0) {
        rank = -rank;
        direction = LIST_HEAD;
    }

    /* We return NULL or an empty array if there is no such key (or
     * if we find no matches, depending on the presence of the COUNT option. */
    // 如果db中没有该key对应的list，则处理返回。基于count参数会有不同的返回类型，这里应该是兼容老版本的命令。
    if ((o = lookupKeyRead(c->db,c->argv[1])) == NULL) {
        if (count != -1)
            addReply(c,shared.emptyarray);
        else
            addReply(c,shared.null[c->resp]);
        return;
    }
    if (checkType(c,o,OBJ_LIST)) return;

    /* If we got the COUNT option, prepare to emit an array. */
    // 有count参数，我们返回数据是一个数组，但是我们最开始并不知道返回数组的长度。
    // 所以这里先分配一个NULL node作为占位符，用于后面填充长度信息。
    void *arraylenptr = NULL;
    if (count != -1) arraylenptr = addReplyDeferredLen(c);

    /* Seek the element. */
    listTypeIterator *li;
    li = listTypeInitIterator(o,direction == LIST_HEAD ? -1 : 0,direction);
    listTypeEntry entry;
    long llen = listTypeLength(o);
    long index = 0, matches = 0, matchindex = -1, arraylen = 0;
    // 遍历元素进行对比。循环结束条件：整个list遍历完 或者 遍历达到了设置的最大遍历元素量。
    while (listTypeNext(li,&entry) && (maxlen == 0 || index < maxlen)) {
        if (listTypeEqual(&entry,ele)) {
            // 如果遍历到的元素数据与命令指定的元素值一致，则匹配，记录当前是第几次匹配。
            matches++;
            // 根据参数指定的rank，判断是否将当前匹配的index加入到回复中。
            if (matches >= rank) {
                // 获取当前匹配的元素的index。（这个可以移到if里面，这样后面就不用再设置matchindex=-1）
                matchindex = (direction == LIST_TAIL) ? index : llen - index - 1;
                if (arraylenptr) {
                    // arraylenptr非空，说明有指定count参数，要返回数组。
                    // 这里将匹配元素的index加入到回复中，并将返回数组len+1。
                    arraylen++;
                    addReplyLongLong(c,matchindex);
                    // 如果指定的count为0表示返回所有rank后的匹配。如果count非0，则需要判断是否获取到足够数据，达到则跳出循环。
                    if (count && matches-rank+1 >= count) break;
                } else {
                    // 到这里说明arraylenptr为空，表示没有指定count参数，那么我们找到一个匹配的就跳出循环。
                    break;
                }
            }
        }
        // 当前遍历的元素总数+1
        index++;
    }
    listTypeReleaseIterator(li);

    /* Reply to the client. Note that arraylenptr is not NULL only if
     * the COUNT option was selected. */
    // 回复给client。注意只有在指定了COUNT选项时，arraylenptr才是非空的。
    if (arraylenptr != NULL) {
        // 处理数组返回，前面我们写入了数据到reply节点中了，这里我们要更新返回数组的长度。
        setDeferredArrayLen(c,arraylenptr,arraylen);
    } else {
        // 没指定COUNT，处理单个匹配返回。
        // 如果matchindex=-1，表示没有指定匹配的的元素，返回null。否则返回匹配元素的索引（单个值）。
        if (matchindex != -1)
            addReplyLongLong(c,matchindex);
        else
            addReply(c,shared.null[c->resp]);
    }
}

/* LREM <key> <count> <element> */
// 根据count参数移除list中与element相等的元素。
// count=0，表示移除所有相等元素，否则最多移除abs(count)个元素。注意count>0表示从头向尾查询，<0表示从尾向头查询。
void lremCommand(client *c) {
    robj *subject, *obj;
    obj = c->argv[3];
    long toremove;
    long removed = 0;

    // 获取count参数
    if ((getLongFromObjectOrReply(c, c->argv[2], &toremove, NULL) != C_OK))
        return;

    // 获取list
    subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero);
    if (subject == NULL || checkType(c,subject,OBJ_LIST)) return;

    listTypeIterator *li;
    // 初始化遍历的迭代器。
    if (toremove < 0) {
        toremove = -toremove;
        li = listTypeInitIterator(subject,-1,LIST_HEAD);
    } else {
        li = listTypeInitIterator(subject,0,LIST_TAIL);
    }

    listTypeEntry entry;
    // 遍历list，对于每个遍历到的entry，对比是否与指定的元素相等。
    while (listTypeNext(li,&entry)) {
        if (listTypeEqual(&entry,obj)) {
            // 与指定的元素向头，处理删除。
            listTypeDelete(li, &entry);
            server.dirty++;
            removed++;
            // 如果指定了最多删除元素数量，这里检查，达到了就结束遍历跳出循环。
            if (toremove && removed == toremove) break;
        }
    }
    listTypeReleaseIterator(li);

    // 如果有删除元素，则处理key变更相关操作。
    if (removed) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST,"lrem",c->argv[1],c->db->id);
    }

    // 如果删除后，list中没有元素了，则从db中删除该key。
    if (listTypeLength(subject) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    // 回复client删除了多少条数据。
    addReplyLongLong(c,removed);
}

void lmoveHandlePush(client *c, robj *dstkey, robj *dstobj, robj *value,
                     int where) {
    /* Create the list if the key does not exist */
    // 如果目标list不存在，则创建并加入到db中。
    if (!dstobj) {
        dstobj = createQuicklistObject();
        quicklistSetOptions(dstobj->ptr, server.list_max_ziplist_size,
                            server.list_compress_depth);
        dbAdd(c->db,dstkey,dstobj);
    }
    // 将value push到目标list中，并通知key变更事件。
    signalModifiedKey(c,c->db,dstkey);
    listTypePush(dstobj,value,where);
    notifyKeyspaceEvent(NOTIFY_LIST,
                        where == LIST_HEAD ? "lpush" : "rpush",
                        dstkey,
                        c->db->id);
    /* Always send the pushed value to the client. */
    // 总是将push进去的元素返回给client
    addReplyBulk(c,value);
}

// 获取命令中的right/left参数，设置对应的tail/head操作位置。
int getListPositionFromObjectOrReply(client *c, robj *arg, int *position) {
    if (strcasecmp(arg->ptr,"right") == 0) {
        *position = LIST_TAIL;
    } else if (strcasecmp(arg->ptr,"left") == 0) {
        *position = LIST_HEAD;
    } else {
        addReplyErrorObject(c,shared.syntaxerr);
        return C_ERR;
    }
    return C_OK;
}

robj *getStringObjectFromListPosition(int position) {
    if (position == LIST_HEAD) {
        return shared.left;
    } else {
        // LIST_TAIL
        return shared.right;
    }
}

// move相关命令的统一处理方法
void lmoveGenericCommand(client *c, int wherefrom, int whereto) {
    robj *sobj, *value;
    // 获取源list
    if ((sobj = lookupKeyWriteOrReply(c,c->argv[1],shared.null[c->resp]))
        == NULL || checkType(c,sobj,OBJ_LIST)) return;

    if (listTypeLength(sobj) == 0) {
        /* This may only happen after loading very old RDB files. Recent
         * versions of Redis delete keys of empty lists. */
        // 用于兼容老的RDB版本，新版的list如果没有元素时，list key会从redis db中移除。
        addReplyNull(c);
    } else {
        // 获取目标list
        robj *dobj = lookupKeyWrite(c->db,c->argv[2]);
        robj *touchedkey = c->argv[1];

        if (checkType(c,dobj,OBJ_LIST)) return;
        // 从源list指定侧（head/tail由left/right指定）pop出元素。
        value = listTypePop(sobj,wherefrom);
        // 避免NPD，null pointer dereference，空指针解引用
        serverAssert(value); /* assertion for valgrind (avoid NPD) */
        // 将从源list pop出的元素，push到目标list中。
        lmoveHandlePush(c,c->argv[2],dobj,value,whereto);

        /* listTypePop returns an object with its refcount incremented */
        // listTypePop返回的obj会增加引用计数，这里引用计数-1，如果需要处理空间释放。
        decrRefCount(value);

        /* Delete the source list when it is empty */
        notifyKeyspaceEvent(NOTIFY_LIST,
                            wherefrom == LIST_HEAD ? "lpop" : "rpop",
                            touchedkey,
                            c->db->id);
        // 如果源list pop后没有元素了，这里需要从db中移除该key。
        if (listTypeLength(sobj) == 0) {
            dbDelete(c->db,touchedkey);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                                touchedkey,c->db->id);
        }
        // key变更处理
        signalModifiedKey(c,c->db,touchedkey);
        server.dirty++;
        // 我们不会传播阻塞命令，所以对于blocked命令这里重写，用于传播到AOF/slaves。
        if (c->cmd->proc == blmoveCommand) {
            rewriteClientCommandVector(c,5,shared.lmove,
                                       c->argv[1],c->argv[2],c->argv[3],c->argv[4]);
        } else if (c->cmd->proc == brpoplpushCommand) {
            rewriteClientCommandVector(c,3,shared.rpoplpush,
                                       c->argv[1],c->argv[2]);
        }
    }
}

/* LMOVE <source> <destination> (LEFT|RIGHT) (LEFT|RIGHT) */
// 从source列表 LEFT|RIGHT 侧pop出元素，然后将元素从 LEFT|RIGHT 侧push到destination列表中。
// LMOVE命令是RPOPLPUSH的扩展替代，Redis 6.2.0起废弃了RPOPLPUSH命令。
void lmoveCommand(client *c) {
    int wherefrom, whereto;
    if (getListPositionFromObjectOrReply(c,c->argv[3],&wherefrom)
        != C_OK) return;
    if (getListPositionFromObjectOrReply(c,c->argv[4],&whereto)
        != C_OK) return;
    lmoveGenericCommand(c, wherefrom, whereto);
}

/* This is the semantic of this command:
 *  RPOPLPUSH srclist dstlist:
 *    IF LLEN(srclist) > 0
 *      element = RPOP srclist
 *      LPUSH dstlist element
 *      RETURN element
 *    ELSE
 *      RETURN nil
 *    END
 *  END
 *
 * The idea is to be able to get an element from a list in a reliable way
 * since the element is not just returned but pushed against another list
 * as well. This command was originally proposed by Ezra Zygmuntowicz.
 */
// RPOPLPUSH srclist dstlist。
// 当srclist非空时，RPOP元素，然后LPUSH到dstlist中，返回对应的元素。当srclist为空时，直接返回nil。
// 这个命令用于可靠的从list中获取元素，因为命令不仅返回了元素，还将元素放到另一个队列（正在处理数据的队列），从而可以作为ACK检查。
// 该命令可以实现安全的消息队列（能ack，另外可以开启一个client监控新队列中的数据，时间过长则重新加入原队列）。
// 如果srclist和dstlist相同，还可以作为一个循环列表，比如监控器循环check指定节点。支持多个客户端并行处理，支持动态的添加删除数据。
void rpoplpushCommand(client *c) {
    lmoveGenericCommand(c, LIST_TAIL, LIST_HEAD);
}

/*-----------------------------------------------------------------------------
 * Blocking POP operations
 *----------------------------------------------------------------------------*/

/* This is a helper function for handleClientsBlockedOnKeys(). Its work
 * is to serve a specific client (receiver) that is blocked on 'key'
 * in the context of the specified 'db', doing the following:
 *
 * 1) Provide the client with the 'value' element.
 * 2) If the dstkey is not NULL (we are serving a BLMOVE) also push the
 *    'value' element on the destination list (the "push" side of the command).
 * 3) Propagate the resulting BRPOP, BLPOP and additional xPUSH if any into
 *    the AOF and replication channel.
 *
 * The argument 'wherefrom' is LIST_TAIL or LIST_HEAD, and indicates if the
 * 'value' element was popped from the head (BLPOP) or tail (BRPOP) so that
 * we can propagate the command properly.
 *
 * The argument 'whereto' is LIST_TAIL or LIST_HEAD, and indicates if the
 * 'value' element is to be pushed to the head or tail so that we can
 * propagate the command properly.
 *
 * The function returns C_OK if we are able to serve the client, otherwise
 * C_ERR is returned to signal the caller that the list POP operation
 * should be undone as the client was not served: This only happens for
 * BLMOVE that fails to push the value to the destination key as it is
 * of the wrong type. */
// 这是handleClientsBlockedOnKeys()的帮助函数，主要用于执行client关于指定db中指定key的阻塞操作。会处理如下工作：
//  1、提供'value'元素给client。
//  2、如果dstkey非空，则我们在处理BLMOVE命令，需要push value到dstkey的list中。
//  3、将产生的BRPOP、BLPOP以及附加的xPUSH传播到AOF/slave去。
// wherefrom、whereto参数值为LIST_TAIL或LIST_HEAD，表示我们pop/push操作的位置，进一步我们确定生成传播到AOF/slave的命令。
// 如果函数成功执行client的阻塞操作，则返回ok。否则返回err（只可能发生在BLMOVE中，push数据类型错误），调用方需要回滚之前的pop操作。
int serveClientBlockedOnList(client *receiver, robj *key, robj *dstkey, redisDb *db, robj *value, int wherefrom, int whereto)
{
    robj *argv[5];

    if (dstkey == NULL) {
        /* Propagate the [LR]POP operation. */
        // dstkey为空，表示阻塞操作是B[LR]POP。我们需要构造[LR]POP命令传播。
        argv[0] = (wherefrom == LIST_HEAD) ? shared.lpop :
                                             shared.rpop;
        argv[1] = key;
        propagate((wherefrom == LIST_HEAD) ?
            server.lpopCommand : server.rpopCommand,
            db->id,argv,2,PROPAGATE_AOF|PROPAGATE_REPL);

        /* BRPOP/BLPOP */
        // 将BRPOP/BLPOP取到的数据，回复给client。
        addReplyArrayLen(receiver,2);
        addReplyBulk(receiver,key);
        addReplyBulk(receiver,value);

        /* Notify event. */
        // 通知key变更事件
        char *event = (wherefrom == LIST_HEAD) ? "lpop" : "rpop";
        notifyKeyspaceEvent(NOTIFY_LIST,event,key,receiver->db->id);
    } else {
        /* BLMOVE */
        // dstkey非空，表示是BLMOVE操作。
        // 先从db中根据dstkey拿到对应对象，然后检查目标对象是否是list，因为只有list才能push处理。
        robj *dstobj =
            lookupKeyWrite(receiver->db,dstkey);
        if (!(dstobj &&
             checkType(receiver,dstobj,OBJ_LIST)))
        {
            // 处理将value加入到目标list的操作
            lmoveHandlePush(receiver,dstkey,dstobj,value,whereto);
            /* Propagate the LMOVE/RPOPLPUSH operation. */
            // 构造LMOVE/RPOPLPUSH命令传播
            int isbrpoplpush = (receiver->lastcmd->proc == brpoplpushCommand);
            argv[0] = isbrpoplpush ? shared.rpoplpush : shared.lmove;
            argv[1] = key;
            argv[2] = dstkey;
            argv[3] = getStringObjectFromListPosition(wherefrom);
            argv[4] = getStringObjectFromListPosition(whereto);
            propagate(isbrpoplpush ? server.rpoplpushCommand : server.lmoveCommand,
                db->id,argv,(isbrpoplpush ? 3 : 5),
                PROPAGATE_AOF|
                PROPAGATE_REPL);

            /* Notify event ("lpush" or "rpush" was notified by lmoveHandlePush). */
            // 通知key变更事件
            notifyKeyspaceEvent(NOTIFY_LIST,wherefrom == LIST_TAIL ? "rpop" : "lpop",
                                key,receiver->db->id);
        } else {
            /* BLMOVE failed because of wrong
             * destination type. */
            // 因为错误的dst类型，BLMOVE执行失败。
            return C_ERR;
        }
    }
    return C_OK;
}

/* Blocking RPOP/LPOP */
// 阻塞的RPOP/LPOP命令统一处理函数。
void blockingPopGenericCommand(client *c, int where) {
    robj *o;
    mstime_t timeout;
    int j;

    // 获取timeout参数
    if (getTimeoutFromObjectOrReply(c,c->argv[c->argc-1],&timeout,UNIT_SECONDS)
        != C_OK) return;

    for (j = 1; j < c->argc-1; j++) {
        // 遍历参数中的keys，根据key中db中获取对象。
        o = lookupKeyWrite(c->db,c->argv[j]);
        if (o != NULL) {
            // 判断如果遍历到的key不是list类型直接返回。
            if (checkType(c,o,OBJ_LIST)) {
                return;
            } else {
                if (listTypeLength(o) != 0) {
                    /* Non empty list, this is like a normal [LR]POP. */
                    // 如果当前遍历到的list有元素，则我们只需要pop出来返回就可以了，不需要阻塞。
                    robj *value = listTypePop(o,where);
                    serverAssert(value != NULL);

                    // 添加回复给client
                    addReplyArrayLen(c,2);
                    addReplyBulk(c,c->argv[j]);
                    addReplyBulk(c,value);
                    decrRefCount(value);
                    // 从当前list移除该pop的元素
                    listElementsRemoved(c,c->argv[j],where,o,1);

                    /* Replicate it as an [LR]POP instead of B[LR]POP. */
                    // 阻塞命令用非阻塞的[LR]POP来替代传播。
                    rewriteClientCommandVector(c,2,
                        (where == LIST_HEAD) ? shared.lpop : shared.rpop,
                        c->argv[j]);
                    return;
                }
            }
        }
    }

    /* If we are not allowed to block the client, the only thing
     * we can do is treating it as a timeout (even with timeout 0). */
    // 执行到这里说明，我们前面遍历玩所有key，所有的list都没有元素，那我们只能进入阻塞状态了。
    // 如果我们当前client不允许阻塞，那么只能直接返回空数组了。即使timeout设置为0表示一直阻塞，我们也无法进入阻塞状态。
    if (c->flags & CLIENT_DENY_BLOCKING) {
        addReplyNullArray(c);
        return;
    }

    /* If the keys do not exist we must block */
    // 这里将client加入阻塞状态，阻塞于所有的keys上
    struct listPos pos = {where};
    blockForKeys(c,BLOCKED_LIST,c->argv + 1,c->argc - 2,timeout,NULL,&pos,NULL);
}

/* BLPOP <key> [<key> ...] <timeout> */
void blpopCommand(client *c) {
    blockingPopGenericCommand(c,LIST_HEAD);
}

/* BRPOP <key> [<key> ...] <timeout> */
void brpopCommand(client *c) {
    blockingPopGenericCommand(c,LIST_TAIL);
}

// BLMOVE 和 BRPOPLPUSH的统一处理函数
void blmoveGenericCommand(client *c, int wherefrom, int whereto, mstime_t timeout) {
    // 从db中取出list
    robj *key = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c,key,OBJ_LIST)) return;

    if (key == NULL) {
        // 如果不存在key，说明需要进入阻塞处理。
        if (c->flags & CLIENT_DENY_BLOCKING) {
            /* Blocking against an empty list when blocking is not allowed
             * returns immediately. */
            // 如果client不允许阻塞，则直接返回null。
            addReplyNull(c);
        } else {
            /* The list is empty and the client blocks. */
            // 调用blockForKeys将当前client阻塞到src列表的key上。
            struct listPos pos = {wherefrom, whereto};
            blockForKeys(c,BLOCKED_LIST,c->argv + 1,1,timeout,c->argv[2],&pos,NULL);
        }
    } else {
        /* The list exists and has elements, so
         * the regular lmoveCommand is executed. */
        // 如果列表存在，则说明有元素，所以我们正常走lmove流程返回，不会进入阻塞状态。
        serverAssertWithInfo(c,key,listTypeLength(key) > 0);
        lmoveGenericCommand(c,wherefrom,whereto);
    }
}

/* BLMOVE <source> <destination> (LEFT|RIGHT) (LEFT|RIGHT) <timeout> */
// BLMOVE命令，解析参数，然后调用blmoveGenericCommand处理。
void blmoveCommand(client *c) {
    mstime_t timeout;
    int wherefrom, whereto;
    if (getListPositionFromObjectOrReply(c,c->argv[3],&wherefrom)
        != C_OK) return;
    if (getListPositionFromObjectOrReply(c,c->argv[4],&whereto)
        != C_OK) return;
    if (getTimeoutFromObjectOrReply(c,c->argv[5],&timeout,UNIT_SECONDS)
        != C_OK) return;
    blmoveGenericCommand(c,wherefrom,whereto,timeout);
}

/* BRPOPLPUSH <source> <destination> <timeout> */
// BRPOPLPUSH命令，解析参数，然后调用blmoveGenericCommand处理。建议用BLMOVE替代。
void brpoplpushCommand(client *c) {
    mstime_t timeout;
    if (getTimeoutFromObjectOrReply(c,c->argv[3],&timeout,UNIT_SECONDS)
        != C_OK) return;
    blmoveGenericCommand(c, LIST_TAIL, LIST_HEAD, timeout);
}
