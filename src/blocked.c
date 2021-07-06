/* blocked.c - generic support for blocking operations like BLPOP & WAIT.
 *
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
 *
 * ---------------------------------------------------------------------------
 *
 * API:
 *
 * blockClient() set the CLIENT_BLOCKED flag in the client, and set the
 * specified block type 'btype' filed to one of BLOCKED_* macros.
 *
 * unblockClient() unblocks the client doing the following:
 * 1) It calls the btype-specific function to cleanup the state.
 * 2) It unblocks the client by unsetting the CLIENT_BLOCKED flag.
 * 3) It puts the client into a list of just unblocked clients that are
 *    processed ASAP in the beforeSleep() event loop callback, so that
 *    if there is some query buffer to process, we do it. This is also
 *    required because otherwise there is no 'readable' event fired, we
 *    already read the pending commands. We also set the CLIENT_UNBLOCKED
 *    flag to remember the client is in the unblocked_clients list.
 *
 * processUnblockedClients() is called inside the beforeSleep() function
 * to process the query buffer from unblocked clients and remove the clients
 * from the blocked_clients queue.
 *
 * replyToBlockedClientTimedOut() is called by the cron function when
 * a client blocked reaches the specified timeout (if the timeout is set
 * to 0, no timeout is processed).
 * It usually just needs to send a reply to the client.
 *
 * When implementing a new type of blocking operation, the implementation
 * should modify unblockClient() and replyToBlockedClientTimedOut() in order
 * to handle the btype-specific behavior of this two functions.
 * If the blocking operation waits for certain keys to change state, the
 * clusterRedirectBlockedClientIfNeeded() function should also be updated.
 */

#include "server.h"
#include "slowlog.h"
#include "latency.h"
#include "monotonic.h"

int serveClientBlockedOnList(client *receiver, robj *key, robj *dstkey, redisDb *db, robj *value, int wherefrom, int whereto);
int getListPositionFromObjectOrReply(client *c, robj *arg, int *position);

/* This structure represents the blocked key information that we store
 * in the client structure. Each client blocked on keys, has a
 * client->bpop.keys hash table. The keys of the hash table are Redis
 * keys pointers to 'robj' structures. The value is this structure.
 * The structure has two goals: firstly we store the list node that this
 * client uses to be listed in the database "blocked clients for this key"
 * list, so we can later unblock in O(1) without a list scan.
 * Secondly for certain blocking types, we have additional info. Right now
 * the only use for additional info we have is when clients are blocked
 * on streams, as we have to remember the ID it blocked for. */
// 这个结构代表存储于client中的阻塞的key的信息。每一个阻塞到keys上的client都有一个client->bpop.keys的hash表。
// 该hash映射表的key是指向阻塞key对象的指针，value就是这个bkinfo结构。
// 这个结构有两个目的，首先因为我们client被阻塞会加入到db中对应key的blocked clients列表中，这里listnode指向该列表中的对应节点，
// 从而解除阻塞后不用遍历，可以快速定位该节点进行删除。
// 其次对于指定的阻塞类型，我们会有附加信息，目前仅对于阻塞在stream类型上数据有附加的stream_id信息。
typedef struct bkinfo {
    listNode *listnode;     /* List node for db->blocking_keys[key] list. */
    streamID stream_id;     /* Stream ID if we blocked in a stream. */
} bkinfo;

/* Block a client for the specific operation type. Once the CLIENT_BLOCKED
 * flag is set client query buffer is not longer processed, but accumulated,
 * and will be processed when the client is unblocked. */
// 阻塞客户端，指定阻塞类型。 一旦设置了CLIENT_BLOCKED标识，client的查询缓冲区就不会再被处理，一直累积直到client解除阻塞才会再进行处理。
void blockClient(client *c, int btype) {
    // 设置相关标识、计数
    c->flags |= CLIENT_BLOCKED;
    c->btype = btype;
    server.blocked_clients++;
    server.blocked_clients_by_type[btype]++;
    // 如果阻塞有设置超时，我们将该超时加入基数树实现的超时表中
    addClientToTimeoutTable(c);
    if (btype == BLOCKED_PAUSE) {
        // 如果是client暂停导致的阻塞，这里client还会加入paused_clients队列。并有一个字段指向队列中自己，用于后面快速定位节点进行删除。
        listAddNodeTail(server.paused_clients, c);
        c->paused_list_node = listLast(server.paused_clients);
        /* Mark this client to execute its command */
        // 标记client有完整的命令待执行
        c->flags |= CLIENT_PENDING_COMMAND;
    }
}

/* This function is called after a client has finished a blocking operation
 * in order to update the total command duration, log the command into
 * the Slow log if needed, and log the reply duration event if needed. */
// 当client执行完阻塞操作后，会调用这个函数来更新总的命令执行时长，可能会写入慢处理日志，加入抽样延迟监控。
void updateStatsOnUnblock(client *c, long blocked_us, long reply_us){
    // 计算命令执行时长，加入到该命令执行的总时长统计中
    const ustime_t total_cmd_duration = c->duration + blocked_us + reply_us;
    c->lastcmd->microseconds += total_cmd_duration;

    /* Log the command into the Slow log if needed. */
    // 如果需要，添加该命令到慢日志中
    slowlogPushCurrentCommand(c, c->lastcmd, total_cmd_duration);
    /* Log the reply duration event. */
    // 将当前unblock操作的时长，加入到延时监控统计中。
    latencyAddSampleIfNeeded("command-unblocking",reply_us/1000);
}

/* This function is called in the beforeSleep() function of the event loop
 * in order to process the pending input buffer of clients that were
 * unblocked after a blocking operation. */
// 在事件循环的beforeSleep()中调用这个函数。用于处理unblocked client中等待的请求。
void processUnblockedClients(void) {
    listNode *ln;
    client *c;

    while (listLength(server.unblocked_clients)) {
        // 遍历处理节点
        ln = listFirst(server.unblocked_clients);
        serverAssert(ln != NULL);
        c = ln->value;
        listDelNode(server.unblocked_clients,ln);
        c->flags &= ~CLIENT_UNBLOCKED;

        /* Process remaining data in the input buffer, unless the client
         * is blocked again. Actually processInputBuffer() checks that the
         * client is not blocked before to proceed, but things may change and
         * the code is conceptually more correct this way. */
        // 除非client又一次进入阻塞状态，否则这里处理input buffer中的数据。
        // 事实上processInputBuffer()在处理前也是会检查client是否阻塞状态的，但是这里判断总没有错，万一processInputBuffer会改呢。
        if (!(c->flags & CLIENT_BLOCKED)) {
            /* If we have a queued command, execute it now. */
            // 如果有一个可以执行的命令了，这里立即执行。
            if (processPendingCommandsAndResetClient(c) == C_ERR) {
                continue;
            }
            /* Then process client if it has more data in it's buffer. */
            // 如果请求buffer中还有待处理的数据，这里进行读取解析处理。
            if (c->querybuf && sdslen(c->querybuf) > 0) {
                processInputBuffer(c);
            }
        }
    }
}

/* This function will schedule the client for reprocessing at a safe time.
 *
 * This is useful when a client was blocked for some reason (blocking operation,
 * CLIENT PAUSE, or whatever), because it may end with some accumulated query
 * buffer that needs to be processed ASAP:
 *
 * 1. When a client is blocked, its readable handler is still active.
 * 2. However in this case it only gets data into the query buffer, but the
 *    query is not parsed or executed once there is enough to proceed as
 *    usually (because the client is blocked... so we can't execute commands).
 * 3. When the client is unblocked, without this function, the client would
 *    have to write some query in order for the readable handler to finally
 *    call processQueryBuffer*() on it.
 * 4. With this function instead we can put the client in a queue that will
 *    process it for queries ready to be executed at a safe time.
 */
void queueClientForReprocessing(client *c) {
    /* The client may already be into the unblocked list because of a previous
     * blocking operation, don't add back it into the list multiple times. */
    if (!(c->flags & CLIENT_UNBLOCKED)) {
        c->flags |= CLIENT_UNBLOCKED;
        listAddNodeTail(server.unblocked_clients,c);
    }
}

/* Unblock a client calling the right function depending on the kind
 * of operation the client is blocking for. */
// 对于client不同的block类型，调用不同的方法来解除阻塞。
void unblockClient(client *c) {
    if (c->btype == BLOCKED_LIST ||
        c->btype == BLOCKED_ZSET ||
        c->btype == BLOCKED_STREAM) {
        // 一些block命令会导致client阻塞，这里解除这类等待数据的阻塞
        unblockClientWaitingData(c);
    } else if (c->btype == BLOCKED_WAIT) {
        // 等待同步复制，进入的阻塞
        unblockClientWaitingReplicas(c);
    } else if (c->btype == BLOCKED_MODULE) {
        // 加载数据或加载模块，进入的阻塞
        if (moduleClientIsBlockedOnKeys(c)) unblockClientWaitingData(c);
        unblockClientFromModule(c);
    } else if (c->btype == BLOCKED_PAUSE) {
        // client暂停进入的阻塞
        listDelNode(server.paused_clients,c->paused_list_node);
        c->paused_list_node = NULL;
    } else {
        serverPanic("Unknown btype in unblockClient().");
    }

    /* Reset the client for a new query since, for blocking commands
     * we do not do it immediately after the command returns (when the
     * client got blocked) in order to be still able to access the argument
     * vector from module callbacks and updateStatsOnUnblock. */
    // 重置client 命令参数、各种flags，从而可以开始下一轮的请求处理。
    // 对于阻塞的命令，不能在命令返回时立即做这些操作，因为命令没执行完，还需要访问一些信息。
    // BLOCKED_PAUSE类型还没执行命令，只是解除暂停，所以不做reset操作。
    if (c->btype != BLOCKED_PAUSE) {
        freeClientOriginalArgv(c);
        resetClient(c);
    }

    /* Clear the flags, and put the client in the unblocked list so that
     * we'll process new commands in its query buffer ASAP. */
    // 清除阻塞clients的计数，清除当前client的block状态并加入unblocked_clients队列，使得能尽快处理。
    server.blocked_clients--;
    server.blocked_clients_by_type[c->btype]--;
    c->flags &= ~CLIENT_BLOCKED;
    c->btype = BLOCKED_NONE;
    // 将该client移除timeout列表，该列表是用于超时判断的。这里说明阻塞时间已经到了，且没有超时。
    removeClientFromTimeoutTable(c);
    // 将client加入到unblocked_clients队列，在beforesleep中会处理该队列中的client
    queueClientForReprocessing(c);
}

/* This function gets called when a blocked client timed out in order to
 * send it a reply of some kind. After this function is called,
 * unblockClient() will be called with the same client as argument. */
// 当阻塞的client超时时，调用该函数向其发送回复。注意调用此函数后，还应该调用unblockClient()解除阻塞。
void replyToBlockedClientTimedOut(client *c) {
    if (c->btype == BLOCKED_LIST ||
        c->btype == BLOCKED_ZSET ||
        c->btype == BLOCKED_STREAM) {
        // 阻塞到某key上面，这里返回空的数组
        addReplyNullArray(c);
    } else if (c->btype == BLOCKED_WAIT) {
        // 阻塞在WAIT命令，返回计算到同步完成的slaves数量
        addReplyLongLong(c,replicationCountAcksByOffset(c->bpop.reploffset));
    } else if (c->btype == BLOCKED_MODULE) {
        // module加载阻塞client超时处理
        moduleBlockedClientTimedOut(c);
    } else {
        serverPanic("Unknown btype in replyToBlockedClientTimedOut().");
    }
}

/* Mass-unblock clients because something changed in the instance that makes
 * blocking no longer safe. For example clients blocked in list operations
 * in an instance which turns from master to slave is unsafe, so this function
 * is called when a master turns into a slave.
 *
 * The semantics is to send an -UNBLOCKED error to the client, disconnecting
 * it at the same time. */
// 因为当前节点发生了一些变化，阻塞操作不再安全，所以这里批量unblock阻塞的客户端，
// 例如，阻塞发生在master节点，但现在该节点将要变成其他节点的slave，显然阻塞的client都不再有效，所以需要调用这个函数来进行unblock。
// 主要操作是向客户端发送-UNBLOCKED错误，同时断开连接。
void disconnectAllBlockedClients(void) {
    listNode *ln;
    listIter li;

    // 遍历所有的clients，如果是blocked状态则进行unblock。
    listRewind(server.clients,&li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);

        if (c->flags & CLIENT_BLOCKED) {
            /* PAUSED clients are an exception, when they'll be unblocked, the
             * command processing will start from scratch, and the command will
             * be either executed or rejected. (unlike LIST blocked clients for
             * which the command is already in progress in a way. */
            // 如果是client暂停引起的阻塞，这里不断开连接。
            // 因为暂停结束时，这类client会从头开始解析，然后执行或拒绝该命令，不会有安全问题。
            // 而其他blocked的client是在执行命令中阻塞，可能会有一直阻塞的风险。
            if (c->btype == BLOCKED_PAUSE)
                continue;

            addReplyError(c,
                "-UNBLOCKED force unblock from blocking operation, "
                "instance state changed (master -> replica?)");
            // 取消client的block，设置CLOSE_AFTER_REPLY状态，从而发送reply后关闭client。
            unblockClient(c);
            c->flags |= CLIENT_CLOSE_AFTER_REPLY;
        }
    }
}

/* Helper function for handleClientsBlockedOnKeys(). This function is called
 * when there may be clients blocked on a list key, and there may be new
 * data to fetch (the key is ready). */
// handleClientsBlockedOnKeys()的帮助函数，用于处理阻塞于ready key上的clients。
void serveClientsBlockedOnListKey(robj *o, readyList *rl) {
    /* We serve clients in the same order they blocked for
     * this key, from the first blocked to the last. */
    // blocking_keys字典存储了[key->阻塞的clients列表]的映射。根据ready key查找对应的entry。
    dictEntry *de = dictFind(rl->db->blocking_keys,rl->key);
    if (de) {
        // 取出阻塞在该key上的clients列表
        list *clients = dictGetVal(de);
        int numclients = listLength(clients);

        // 按blocked的先后顺序来处理clients，总共迭代numclients次。
        while(numclients--) {
            // 每次从队首拿client处理。
            listNode *clientnode = listFirst(clients);
            client *receiver = clientnode->value;

            // 如果client阻塞类型不是LIST，我们将该client移到队尾，进入下一轮迭代。
            if (receiver->btype != BLOCKED_LIST) {
                /* Put at the tail, so that at the next call
                 * we'll not run into it again. */
                listRotateHeadToTail(clients);
                continue;
            }

            // 从阻塞状态中获取目标list的key，以及pop和push元素的位置。
            robj *dstkey = receiver->bpop.target;
            int wherefrom = receiver->bpop.listpos.wherefrom;
            int whereto = receiver->bpop.listpos.whereto;
            // 从源list的wherefrom处pop元素。
            robj *value = listTypePop(o, wherefrom);

            if (value) {
                /* Protect receiver->bpop.target, that will be
                 * freed by the next unblockClient()
                 * call. */
                // 保护dstkey对象，防止后面unblockClient()释放bpop.target（即dstkey）。
                // 老代码unblockClient()紧跟这个之后，在serveClientBlockedOnList使用的前面，所以需要保护。
                // 新代码unblockClient移到了后面，可以删除这个操作了。
                if (dstkey) incrRefCount(dstkey);

                monotime replyTimer;
                elapsedStart(&replyTimer);
                // 处理阻塞的client的操作
                if (serveClientBlockedOnList(receiver,
                    rl->key,dstkey,rl->db,value,
                    wherefrom, whereto) == C_ERR)
                {
                    /* If we failed serving the client we need
                     * to also undo the POP operation. */
                    // 如果我们处理阻塞操作失败了，那么我们要还原之前的pop操作，即再把value push回去
                    listTypePush(o,value,wherefrom);
                }
                // 处理Unblock相关的统计
                updateStatsOnUnblock(receiver, 0, elapsedUs(replyTimer));
                // 解除client的阻塞
                unblockClient(receiver);

                if (dstkey) decrRefCount(dstkey);
                decrRefCount(value);
            } else {
                break;
            }
        }
    }

    // 如果处理后，list里面没有元素了，则从db中移除key。
    if (listTypeLength(o) == 0) {
        dbDelete(rl->db,rl->key);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",rl->key,rl->db->id);
    }
    /* We don't call signalModifiedKey() as it was already called
     * when an element was pushed on the list. */
    // 我们这里不再调用signalModifiedKey()，因为在前面PUSH元素到list的时候已经调用该函数处理过了。
}

/* Helper function for handleClientsBlockedOnKeys(). This function is called
 * when there may be clients blocked on a sorted set key, and there may be new
 * data to fetch (the key is ready). */
void serveClientsBlockedOnSortedSetKey(robj *o, readyList *rl) {
    /* We serve clients in the same order they blocked for
     * this key, from the first blocked to the last. */
    dictEntry *de = dictFind(rl->db->blocking_keys,rl->key);
    if (de) {
        list *clients = dictGetVal(de);
        int numclients = listLength(clients);
        unsigned long zcard = zsetLength(o);

        while(numclients-- && zcard) {
            listNode *clientnode = listFirst(clients);
            client *receiver = clientnode->value;

            if (receiver->btype != BLOCKED_ZSET) {
                /* Put at the tail, so that at the next call
                 * we'll not run into it again. */
                listRotateHeadToTail(clients);
                continue;
            }

            int where = (receiver->lastcmd &&
                         receiver->lastcmd->proc == bzpopminCommand)
                         ? ZSET_MIN : ZSET_MAX;
            monotime replyTimer;
            elapsedStart(&replyTimer);
            genericZpopCommand(receiver,&rl->key,1,where,1,NULL);
            updateStatsOnUnblock(receiver, 0, elapsedUs(replyTimer));
            unblockClient(receiver);
            zcard--;

            /* Replicate the command. */
            robj *argv[2];
            struct redisCommand *cmd = where == ZSET_MIN ?
                                       server.zpopminCommand :
                                       server.zpopmaxCommand;
            argv[0] = createStringObject(cmd->name,strlen(cmd->name));
            argv[1] = rl->key;
            incrRefCount(rl->key);
            propagate(cmd,receiver->db->id,
                      argv,2,PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(argv[0]);
            decrRefCount(argv[1]);
        }
    }
}

/* Helper function for handleClientsBlockedOnKeys(). This function is called
 * when there may be clients blocked on a stream key, and there may be new
 * data to fetch (the key is ready). */
void serveClientsBlockedOnStreamKey(robj *o, readyList *rl) {
    dictEntry *de = dictFind(rl->db->blocking_keys,rl->key);
    stream *s = o->ptr;

    /* We need to provide the new data arrived on the stream
     * to all the clients that are waiting for an offset smaller
     * than the current top item. */
    if (de) {
        list *clients = dictGetVal(de);
        listNode *ln;
        listIter li;
        listRewind(clients,&li);

        while((ln = listNext(&li))) {
            client *receiver = listNodeValue(ln);
            if (receiver->btype != BLOCKED_STREAM) continue;
            bkinfo *bki = dictFetchValue(receiver->bpop.keys,rl->key);
            streamID *gt = &bki->stream_id;

            /* If we blocked in the context of a consumer
             * group, we need to resolve the group and update the
             * last ID the client is blocked for: this is needed
             * because serving other clients in the same consumer
             * group will alter the "last ID" of the consumer
             * group, and clients blocked in a consumer group are
             * always blocked for the ">" ID: we need to deliver
             * only new messages and avoid unblocking the client
             * otherwise. */
            streamCG *group = NULL;
            if (receiver->bpop.xread_group) {
                group = streamLookupCG(s,
                        receiver->bpop.xread_group->ptr);
                /* If the group was not found, send an error
                 * to the consumer. */
                if (!group) {
                    addReplyError(receiver,
                        "-NOGROUP the consumer group this client "
                        "was blocked on no longer exists");
                    unblockClient(receiver);
                    continue;
                } else {
                    *gt = group->last_id;
                }
            }

            if (streamCompareID(&s->last_id, gt) > 0) {
                streamID start = *gt;
                streamIncrID(&start);

                /* Lookup the consumer for the group, if any. */
                streamConsumer *consumer = NULL;
                int noack = 0;

                if (group) {
                    int created = 0;
                    consumer =
                        streamLookupConsumer(group,
                                             receiver->bpop.xread_consumer->ptr,
                                             SLC_NONE,
                                             &created);
                    noack = receiver->bpop.xread_group_noack;
                    if (created && noack) {
                        streamPropagateConsumerCreation(receiver,rl->key,
                                                        receiver->bpop.xread_group,
                                                        consumer->name);
                    }
                }

                monotime replyTimer;
                elapsedStart(&replyTimer);
                /* Emit the two elements sub-array consisting of
                 * the name of the stream and the data we
                 * extracted from it. Wrapped in a single-item
                 * array, since we have just one key. */
                if (receiver->resp == 2) {
                    addReplyArrayLen(receiver,1);
                    addReplyArrayLen(receiver,2);
                } else {
                    addReplyMapLen(receiver,1);
                }
                addReplyBulk(receiver,rl->key);

                streamPropInfo pi = {
                    rl->key,
                    receiver->bpop.xread_group
                };
                streamReplyWithRange(receiver,s,&start,NULL,
                                     receiver->bpop.xread_count,
                                     0, group, consumer, noack, &pi);
                updateStatsOnUnblock(receiver, 0, elapsedUs(replyTimer));

                /* Note that after we unblock the client, 'gt'
                 * and other receiver->bpop stuff are no longer
                 * valid, so we must do the setup above before
                 * this call. */
                unblockClient(receiver);
            }
        }
    }
}

/* Helper function for handleClientsBlockedOnKeys(). This function is called
 * in order to check if we can serve clients blocked by modules using
 * RM_BlockClientOnKeys(), when the corresponding key was signaled as ready:
 * our goal here is to call the RedisModuleBlockedClient reply() callback to
 * see if the key is really able to serve the client, and in that case,
 * unblock it. */
void serveClientsBlockedOnKeyByModule(readyList *rl) {
    dictEntry *de;

    /* Optimization: If no clients are in type BLOCKED_MODULE,
     * we can skip this loop. */
    if (!server.blocked_clients_by_type[BLOCKED_MODULE]) return;

    /* We serve clients in the same order they blocked for
     * this key, from the first blocked to the last. */
    de = dictFind(rl->db->blocking_keys,rl->key);
    if (de) {
        list *clients = dictGetVal(de);
        int numclients = listLength(clients);

        while(numclients--) {
            listNode *clientnode = listFirst(clients);
            client *receiver = clientnode->value;

            /* Put at the tail, so that at the next call
             * we'll not run into it again: clients here may not be
             * ready to be served, so they'll remain in the list
             * sometimes. We want also be able to skip clients that are
             * not blocked for the MODULE type safely. */
            listRotateHeadToTail(clients);

            if (receiver->btype != BLOCKED_MODULE) continue;

            /* Note that if *this* client cannot be served by this key,
             * it does not mean that another client that is next into the
             * list cannot be served as well: they may be blocked by
             * different modules with different triggers to consider if a key
             * is ready or not. This means we can't exit the loop but need
             * to continue after the first failure. */
            monotime replyTimer;
            elapsedStart(&replyTimer);
            if (!moduleTryServeClientBlockedOnKey(receiver, rl->key)) continue;
            updateStatsOnUnblock(receiver, 0, elapsedUs(replyTimer));

            moduleUnblockClient(receiver);
        }
    }
}

/* This function should be called by Redis every time a single command,
 * a MULTI/EXEC block, or a Lua script, terminated its execution after
 * being called by a client. It handles serving clients blocked in
 * lists, streams, and sorted sets, via a blocking commands.
 *
 * All the keys with at least one client blocked that received at least
 * one new element via some write operation are accumulated into
 * the server.ready_keys list. This function will run the list and will
 * serve clients accordingly. Note that the function will iterate again and
 * again as a result of serving BLMOVE we can have new blocking clients
 * to serve because of the PUSH side of BLMOVE.
 *
 * This function is normally "fair", that is, it will server clients
 * using a FIFO behavior. However this fairness is violated in certain
 * edge cases, that is, when we have clients blocked at the same time
 * in a sorted set and in a list, for the same key (a very odd thing to
 * do client side, indeed!). Because mismatching clients (blocking for
 * a different type compared to the current key type) are moved in the
 * other side of the linked list. However as long as the key starts to
 * be used only for a single type, like virtually any Redis application will
 * do, the function is already fair. */
// 每次单个命令、MULTI/EXEC块、Lua脚本执行完成时，都应该调用这个函数来处理阻塞在lists、streams、sorted sets上的clients。
// 所有的阻塞clients的keys中，当某个key通过写操作加入了新元素时，相应的会被加入到server.ready_keys列表中，从而这个函数来进行处理。
// 注意：这个函数处理BLMOVE时可能反复迭代，因为可能有client阻塞在BLMOVE的PUSH端。对于这样的情况，我们会一直处理直到ready_keys为空。

// 这个函数通常来说是公平的，以FIFO方式来处理clients。
// 但是在某些边界条件下会违反这种公平性，如有两个clients同时阻塞在sorted set和list上，但阻塞的key是相同的（client端很奇怪的操作，要避免）。
// 因为不匹配（相同的key，但阻塞类型不匹配），client会被移动到链表另一端。不过只要单个key只用于一种类型，那么这个方法就是公平的。
void handleClientsBlockedOnKeys(void) {
    // 只要ready_keys列表不为空，则一直遍历处理。
    while(listLength(server.ready_keys) != 0) {
        list *l;

        /* Point server.ready_keys to a fresh list and save the current one
         * locally. This way as we run the old list we are free to call
         * signalKeyAsReady() that may push new elements in server.ready_keys
         * when handling clients blocked into BLMOVE. */
        // 将server.ready_keys指向新的链表，并保存当前ready keys作为遍历处理。
        // 这样我们处理老的ready keys列表时，如果需要可以自由调用signalKeyAsReady()往server.ready_keys加入新元素。
        // 如处理BLMOVE阻塞时可能唤醒PUSH端的阻塞，从而再次加入到ready_keys中。
        l = server.ready_keys;
        server.ready_keys = listCreate();

        // 遍历老的ready keys列表
        while(listLength(l) != 0) {
            // 取第一个元素
            listNode *ln = listFirst(l);
            readyList *rl = ln->value;

            /* First of all remove this key from db->ready_keys so that
             * we can safely call signalKeyAsReady() against this key. */
            // 首先从db->ready_keys字典中移除这个key，这样可以保证后面安全的调用signalKeyAsReady()来加入该key。
            dictDelete(rl->db->ready_keys,rl->key);

            /* Even if we are not inside call(), increment the call depth
             * in order to make sure that keys are expired against a fixed
             * reference time, and not against the wallclock time. This
             * way we can lookup an object multiple times (BLMOVE does
             * that) without the risk of it being freed in the second
             * lookup, invalidating the first one.
             * See https://github.com/redis/redis/pull/6554. */
            // 即使不在call()里面，我们这里也增加call深度，从而确保执行ready key的client在操作期间，处理key过期使用固定的缓存时间。
            // 这样我们就可以多次访问同一个key，不用担心后面访问时会过期key，而使前面拿到的key失效。
            server.fixed_time_expire++;
            updateCachedTime(0);

            /* Serve clients blocked on the key. */
            // 根据ready key从db中获取value。
            robj *o = lookupKeyWrite(rl->db,rl->key);

            if (o != NULL) {
                // 根据value对象的类型，具体使用对应的方法来处理阻塞的client。
                if (o->type == OBJ_LIST)
                    serveClientsBlockedOnListKey(o,rl);
                else if (o->type == OBJ_ZSET)
                    serveClientsBlockedOnSortedSetKey(o,rl);
                else if (o->type == OBJ_STREAM)
                    serveClientsBlockedOnStreamKey(o,rl);
                /* We want to serve clients blocked on module keys
                 * regardless of the object type: we don't know what the
                 * module is trying to accomplish right now. */
                // 无论对象是什么类型，我们都希望处理blocked在module keys上的client，因为我们现在还不知道module在尝试处理什么。
                serveClientsBlockedOnKeyByModule(rl);
            }
            // 一个key处理完了，call深度还原。
            server.fixed_time_expire--;

            /* Free this item. */
            // 释放该ready key，并从列表中移除。
            decrRefCount(rl->key);
            zfree(rl);
            listDelNode(l,ln);
        }
        // 如果老的ready keys列表处理完了，我们需要释放整个列表。
        listRelease(l); /* We have the new list on place at this point. */
    }
}

/* This is how the current blocking lists/sorted sets/streams work, we use
 * BLPOP as example, but the concept is the same for other list ops, sorted
 * sets and XREAD.
 * - If the user calls BLPOP and the key exists and contains a non empty list
 *   then LPOP is called instead. So BLPOP is semantically the same as LPOP
 *   if blocking is not required.
 * - If instead BLPOP is called and the key does not exists or the list is
 *   empty we need to block. In order to do so we remove the notification for
 *   new data to read in the client socket (so that we'll not serve new
 *   requests if the blocking request is not served). Also we put the client
 *   in a dictionary (db->blocking_keys) mapping keys to a list of clients
 *   blocking for this keys.
 * - If a PUSH operation against a key with blocked clients waiting is
 *   performed, we mark this key as "ready", and after the current command,
 *   MULTI/EXEC block, or script, is executed, we serve all the clients waiting
 *   for this list, from the one that blocked first, to the last, accordingly
 *   to the number of elements we have in the ready list.
 */

/* Set a client in blocking mode for the specified key (list, zset or stream),
 * with the specified timeout. The 'type' argument is BLOCKED_LIST,
 * BLOCKED_ZSET or BLOCKED_STREAM depending on the kind of operation we are
 * waiting for an empty key in order to awake the client. The client is blocked
 * for all the 'numkeys' keys as in the 'keys' argument. When we block for
 * stream keys, we also provide an array of streamID structures: clients will
 * be unblocked only when items with an ID greater or equal to the specified
 * one is appended to the stream. */
void blockForKeys(client *c, int btype, robj **keys, int numkeys, mstime_t timeout, robj *target, struct listPos *listpos, streamID *ids) {
    dictEntry *de;
    list *l;
    int j;

    // 设置client的blocked相关状态属性
    c->bpop.timeout = timeout;
    c->bpop.target = target;

    if (listpos != NULL) c->bpop.listpos = *listpos;

    if (target != NULL) incrRefCount(target);

    // 对于传入的所有keys，我们client都需要加入对应key的阻塞列表，这里遍历keys处理。
    for (j = 0; j < numkeys; j++) {
        /* Allocate our bkinfo structure, associated to each key the client
         * is blocked for. */
        // 对于该key构建bkinfo结构
        bkinfo *bki = zmalloc(sizeof(*bki));
        if (btype == BLOCKED_STREAM)
            bki->stream_id = ids[j];

        /* If the key already exists in the dictionary ignore it. */
        // 将当前阻塞的key-bki，加入到c->bpop.keys hash表中，如果阻塞的keys表中已经存在该key了，则跳过。
        if (dictAdd(c->bpop.keys,keys[j],bki) != DICT_OK) {
            zfree(bki);
            continue;
        }
        incrRefCount(keys[j]);

        /* And in the other "side", to map keys -> clients */
        // 前面将key加入到cleint->keys中。这里我们还需要将client加入到key->clients表中。
        de = dictFind(c->db->blocking_keys,keys[j]);
        if (de == NULL) {
            int retval;

            /* For every key we take a list of clients blocked for it */
            // 对于每个key，因为可能阻塞多个client，所以这里创建新的list来存储cliets。
            l = listCreate();
            // 将新 key->新list 加入到c->db->blocking_keys中。
            retval = dictAdd(c->db->blocking_keys,keys[j],l);
            incrRefCount(keys[j]);
            serverAssertWithInfo(c,keys[j],retval == DICT_OK);
        } else {
            // 如果该key已经在blocking_keys中存在了，我们只需要取出来用就可以了
            l = dictGetVal(de);
        }
        // 将当前client加入到阻塞于key的clients列表中，并将bki中的指针指向列表中该client节点。
        listAddNodeTail(l,c);
        bki->listnode = listLast(l);
    }
    // 将当前client设置为阻塞状态。
    blockClient(c,btype);
}

/* Unblock a client that's waiting in a blocking operation such as BLPOP.
 * You should never call this function directly, but unblockClient() instead. */
void unblockClientWaitingData(client *c) {
    dictEntry *de;
    dictIterator *di;
    list *l;

    serverAssertWithInfo(c,NULL,dictSize(c->bpop.keys) != 0);
    di = dictGetIterator(c->bpop.keys);
    /* The client may wait for multiple keys, so unblock it for every key. */
    while((de = dictNext(di)) != NULL) {
        robj *key = dictGetKey(de);
        bkinfo *bki = dictGetVal(de);

        /* Remove this client from the list of clients waiting for this key. */
        l = dictFetchValue(c->db->blocking_keys,key);
        serverAssertWithInfo(c,key,l != NULL);
        listDelNode(l,bki->listnode);
        /* If the list is empty we need to remove it to avoid wasting memory */
        if (listLength(l) == 0)
            dictDelete(c->db->blocking_keys,key);
    }
    dictReleaseIterator(di);

    /* Cleanup the client structure */
    dictEmpty(c->bpop.keys,NULL);
    if (c->bpop.target) {
        decrRefCount(c->bpop.target);
        c->bpop.target = NULL;
    }
    if (c->bpop.xread_group) {
        decrRefCount(c->bpop.xread_group);
        decrRefCount(c->bpop.xread_consumer);
        c->bpop.xread_group = NULL;
        c->bpop.xread_consumer = NULL;
    }
}

static int getBlockedTypeByType(int type) {
    switch (type) {
        case OBJ_LIST: return BLOCKED_LIST;
        case OBJ_ZSET: return BLOCKED_ZSET;
        case OBJ_MODULE: return BLOCKED_MODULE;
        case OBJ_STREAM: return BLOCKED_STREAM;
        default: return BLOCKED_NONE;
    }
}

/* If the specified key has clients blocked waiting for list pushes, this
 * function will put the key reference into the server.ready_keys list.
 * Note that db->ready_keys is a hash table that allows us to avoid putting
 * the same key again and again in the list in case of multiple pushes
 * made by a script or in the context of MULTI/EXEC.
 *
 * The list will be finally processed by handleClientsBlockedOnKeys() */
// 如果对于指定的key，有client正阻塞等待key的数据，这个函数将会把这个key放入到server.ready_keys列表中。
// 注意db->ready_keys是一个hash表，所以如果我们重复加入key是没有影响的。
void signalKeyAsReady(redisDb *db, robj *key, int type) {
    readyList *rl;

    /* Quick returns. */
    // 检查blocked type，如果该类型不能被blocked，则不需要后面的处理，直接return。
    int btype = getBlockedTypeByType(type);
    if (btype == BLOCKED_NONE) {
        /* The type can never block. */
        return;
    }
    if (!server.blocked_clients_by_type[btype] &&
        !server.blocked_clients_by_type[BLOCKED_MODULE]) {
        /* No clients block on this type. Note: Blocked modules are represented
         * by BLOCKED_MODULE, even if the intention is to wake up by normal
         * types (list, zset, stream), so we need to check that there are no
         * blocked modules before we do a quick return here. */
        // 如果没有client阻塞在该类型上，我们可以尽快返回。
        // 注意：即使我们打算使用正常阻塞类型（list、zset、stream）来唤醒，阻塞的modules还是使用BLOCKED_MODULE来表示的。
        // 所以我们这里想要尽快返回，不仅要检查该类型本身没有阻塞的clients，还要检查MODULE类型也没有阻塞的clients。
        return;
    }

    /* No clients blocking for this key? No need to queue it. */
    // 在db->blocking_keys字典中查找该key，如果没有说明没有client阻塞在该key上，不需要处理，直接返回。
    if (dictFind(db->blocking_keys,key) == NULL) return;

    /* Key was already signaled? No need to queue it again. */
    // 如果该key已经在db->ready_keys中了，说明之前加入过，不需要再重复加入了，直接返回。
    if (dictFind(db->ready_keys,key) != NULL) return;

    /* Ok, we need to queue this key into server.ready_keys. */
    // 将key加入到server.ready_keys列表中。
    rl = zmalloc(sizeof(*rl));
    rl->key = key;
    rl->db = db;
    incrRefCount(key);
    listAddNodeTail(server.ready_keys,rl);

    /* We also add the key in the db->ready_keys dictionary in order
     * to avoid adding it multiple times into a list with a simple O(1)
     * check. */
    // 同时也将key加入到db->ready_keys字典中，O(1)方式检查key是否已经加入ready列表，避免重复加入。
    incrRefCount(key);
    serverAssert(dictAdd(db->ready_keys,key,NULL) == DICT_OK);
}

