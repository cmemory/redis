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
#include "atomicvar.h"
#include "cluster.h"
#include <sys/socket.h>
#include <sys/uio.h>
#include <math.h>
#include <ctype.h>

static void setProtocolError(const char *errstr, client *c);
int postponeClientRead(client *c);
int ProcessingEventsWhileBlocked = 0; /* See processEventsWhileBlocked(). */

/* Return the size consumed from the allocator, for the specified SDS string,
 * including internal fragmentation. This function is used in order to compute
 * the client output buffer size. */
size_t sdsZmallocSize(sds s) {
    void *sh = sdsAllocPtr(s);
    return zmalloc_size(sh);
}

/* Return the amount of memory used by the sds string at object->ptr
 * for a string object. This includes internal fragmentation. */
size_t getStringObjectSdsUsedMemory(robj *o) {
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
    switch(o->encoding) {
    case OBJ_ENCODING_RAW: return sdsZmallocSize(o->ptr);
    case OBJ_ENCODING_EMBSTR: return zmalloc_size(o)-sizeof(robj);
    default: return 0; /* Just integer encoding for now. */
    }
}

/* Return the length of a string object.
 * This does NOT includes internal fragmentation or sds unused space. */
// 返回string对象的长度。这里计算时没有包含内部碎片以及sds没使用的空间。
size_t getStringObjectLen(robj *o) {
    // 确保对象类型是string
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
    switch(o->encoding) {
    // raw和enbstr，实体sdslen求长度
    case OBJ_ENCODING_RAW: return sdslen(o->ptr);
    case OBJ_ENCODING_EMBSTR: return sdslen(o->ptr);
    // int的话，直接使用的指针域，不算空间
    default: return 0; /* Just integer encoding for now. */
    }
}

/* Client.reply list dup and free methods. */
void *dupClientReplyValue(void *o) {
    clientReplyBlock *old = o;
    clientReplyBlock *buf = zmalloc(sizeof(clientReplyBlock) + old->size);
    memcpy(buf, o, sizeof(clientReplyBlock) + old->size);
    return buf;
}

void freeClientReplyValue(void *o) {
    zfree(o);
}

int listMatchObjects(void *a, void *b) {
    return equalStringObjects(a,b);
}

/* This function links the client to the global linked list of clients.
 * unlinkClient() does the opposite, among other things. */
// 这个函数将client加入全局的clients列表。unlinkClient()做相反操作，移除client。
void linkClient(client *c) {
    // client加入服务全局clients列表
    listAddNodeTail(server.clients,c);
    /* Note that we remember the linked list node where the client is stored,
     * this way removing the client in unlinkClient() will not require
     * a linear scan, but just a constant time operation. */
    // client中的属性client_list_node，指向server全局clients列表中的当前client所在的节点。
    // 主要用于在unlinkClient()中从全局clients列表中移除这个client。避免全链表扫描处理，直接从这个属性就可以找到。
    c->client_list_node = listLast(server.clients);
    uint64_t id = htonu64(c->id);
    // 写入clients字典，ID -> client的映射。
    raxInsert(server.clients_index,(unsigned char*)&id,sizeof(id),c,NULL);
}

/* Initialize client authentication state.
 */
static void clientSetDefaultAuth(client *c) {
    /* If the default user does not require authentication, the user is
     * directly authenticated. */
    c->user = DefaultUser;
    c->authenticated = (c->user->flags & USER_FLAG_NOPASS) &&
                       !(c->user->flags & USER_FLAG_DISABLED);
}

client *createClient(connection *conn) {
    client *c = zmalloc(sizeof(client));

    /* passing NULL as conn it is possible to create a non connected client.
     * This is useful since all the commands needs to be executed
     * in the context of a client. When commands are executed in other
     * contexts (for instance a Lua script) we need a non connected client. */
    // conn代表一个连接，可以有无连接的client。在执行Lue脚本时就是使用无连接client
    if (conn) {
        // 设置非阻塞
        connNonBlock(conn);
        // 禁用Nagle。立即传输，允许小包。
        connEnableTcpNoDelay(conn);
        // 设置keepalive
        if (server.tcpkeepalive)
            connKeepAlive(conn,server.tcpkeepalive);
        // 设置读处理函数
        connSetReadHandler(conn, readQueryFromClient);
        // 设置conn私有数据为当前创建的client
        connSetPrivateData(conn, c);
    }

    // client选择db 0
    selectDb(c,0);
    uint64_t client_id;
    atomicGetIncr(server.next_client_id, client_id, 1);
    // 设置全局client_id
    c->id = client_id;
    // RESP协议
    c->resp = 2;
    c->conn = conn;
    c->name = NULL;
    c->bufpos = 0;
    c->qb_pos = 0;
    c->querybuf = sdsempty();
    c->pending_querybuf = sdsempty();
    c->querybuf_peak = 0;
    c->reqtype = 0;
    c->argc = 0;
    c->argv = NULL;
    c->argv_len_sum = 0;
    c->original_argc = 0;
    c->original_argv = NULL;
    c->cmd = c->lastcmd = NULL;
    c->multibulklen = 0;
    c->bulklen = -1;
    c->sentlen = 0;
    c->flags = 0;
    c->ctime = c->lastinteraction = server.unixtime;
    clientSetDefaultAuth(c);
    c->replstate = REPL_STATE_NONE;
    c->repl_put_online_on_ack = 0;
    c->reploff = 0;
    c->read_reploff = 0;
    c->repl_ack_off = 0;
    c->repl_ack_time = 0;
    c->repl_last_partial_write = 0;
    c->slave_listening_port = 0;
    c->slave_addr = NULL;
    c->slave_capa = SLAVE_CAPA_NONE;
    c->reply = listCreate();
    c->reply_bytes = 0;
    c->obuf_soft_limit_reached_time = 0;
    listSetFreeMethod(c->reply,freeClientReplyValue);
    listSetDupMethod(c->reply,dupClientReplyValue);
    c->btype = BLOCKED_NONE;
    c->bpop.timeout = 0;
    c->bpop.keys = dictCreate(&objectKeyHeapPointerValueDictType,NULL);
    c->bpop.target = NULL;
    c->bpop.xread_group = NULL;
    c->bpop.xread_consumer = NULL;
    c->bpop.xread_group_noack = 0;
    c->bpop.numreplicas = 0;
    c->bpop.reploffset = 0;
    c->woff = 0;
    c->watched_keys = listCreate();
    c->pubsub_channels = dictCreate(&objectKeyPointerValueDictType,NULL);
    c->pubsub_patterns = listCreate();
    c->peerid = NULL;
    c->sockname = NULL;
    c->client_list_node = NULL;
    c->paused_list_node = NULL;
    c->client_tracking_redirection = 0;
    c->client_tracking_prefixes = NULL;
    c->client_cron_last_memory_usage = 0;
    c->client_cron_last_memory_type = CLIENT_TYPE_NORMAL;
    c->auth_callback = NULL;
    c->auth_callback_privdata = NULL;
    c->auth_module = NULL;
    listSetFreeMethod(c->pubsub_patterns,decrRefCountVoid);
    listSetMatchMethod(c->pubsub_patterns,listMatchObjects);
    if (conn) linkClient(c);
    initClientMultiState(c);
    return c;
}

/* This function puts the client in the queue of clients that should write
 * their output buffers to the socket. Note that it does not *yet* install
 * the write handler, to start clients are put in a queue of clients that need
 * to write, so we try to do that before returning in the event loop (see the
 * handleClientsWithPendingWrites() function).
 * If we fail and there is more data to write, compared to what the socket
 * buffers can hold, then we'll really install the handler. */
// 注意这里并没有安装写事件handler并加入监听，只是将client加入可写队列，该队列都是应该将写缓冲数据写入socket的client。
// 队列中的client等待beforsleep中的handleClientsWithPendingWrites（或使用io线程）进行处理。
void clientInstallWriteHandler(client *c) {
    /* Schedule the client to write the output buffers to the socket only
     * if not already done and, for slaves, if the slave can actually receive
     * writes at this stage. */
    // 只有在client不是pending write，并且要么没有活跃replication，要么replication当前可以接受数据写入。
    // 这样的情况下，才处理write。
    // CLIENT_PENDING_WRITE状态可能是之前已经尝试写了的，没写完，等待可写事件触发，所以这里就不重复加入了。
    if (!(c->flags & CLIENT_PENDING_WRITE) &&
        (c->replstate == REPL_STATE_NONE ||
         (c->replstate == SLAVE_STATE_ONLINE && !c->repl_put_online_on_ack)))
    {
        /* Here instead of installing the write handler, we just flag the
         * client and put it into a list of clients that have something
         * to write to the socket. This way before re-entering the event
         * loop, we can try to directly write to the client sockets avoiding
         * a system call. We'll only really install the write handler if
         * we'll not be able to write the whole reply at once. */
        // 这里我们并不会为该socket创建可写事件监听，只是将flags标识为待写，加入到待写队列中。
        // 这样我们接下来可以直接将回复写到sockets中，避免一次事件监听的系统调用。
        // 我们只在不能一次性写完整个回复时候，写一部分数据，并创建写可事件监听，等待写入剩余数据。
        // 显然如果能直接处理就不用强行使用事件触发逻辑了，尽可能减少系统调用，只有不得已才使用事件监听。
        c->flags |= CLIENT_PENDING_WRITE;
        listAddNodeHead(server.clients_pending_write,c);
    }
}

/* This function is called every time we are going to transmit new data
 * to the client. The behavior is the following:
 *
 * If the client should receive new data (normal clients will) the function
 * returns C_OK, and make sure to install the write handler in our event
 * loop so that when the socket is writable new data gets written.
 *
 * If the client should not receive new data, because it is a fake client
 * (used to load AOF in memory), a master or because the setup of the write
 * handler failed, the function returns C_ERR.
 *
 * The function may return C_OK without actually installing the write
 * event handler in the following cases:
 *
 * 1) The event handler should already be installed since the output buffer
 *    already contains something.
 * 2) The client is a slave but not yet online, so we want to just accumulate
 *    writes in the buffer but not actually sending them yet.
 *
 * Typically gets called every time a reply is built, before adding more
 * data to the clients output buffers. If the function returns C_ERR no
 * data should be appended to the output buffers. */
// 每次我们想向client传输新的数据时，都会调用这个方法来准备好可写的client。主要处理如下：
// 1、如果是正常client，可以接收新数据，函数返回ok，并确保安装好写handler事件，加入监听，在socket可写时，在事件循环中处理写数据回复。
// 2、如果是fake client（用于加载AOF数据到内存），或者是master client，又或者是安装写handler失败的client，都不能接收新数据，返回err。
// 3、下面两种情况，函数可能在没有实际安装写handler时，返回ok：
//      1) 事件处理handler应该已经安装了，因为output buffer中已经有数据了。
//      2) client是slave，但是目前不是online状态，我们希望累积写到buffer中，但现在并不立即发送。
// 通常在每次构建回复时先调用这个方法，然后向client输出缓冲区添加更多数据。如果返回C_ERR，则不应将数据附加到输出缓冲区。
int prepareClientToWrite(client *c) {
    /* If it's the Lua client we always return ok without installing any
     * handler since there is no socket at all. */
    // 如果是Lua或module client，总是返回ok。我不需要安装任何写处理handler，因为根本没有socket。
    if (c->flags & (CLIENT_LUA|CLIENT_MODULE)) return C_OK;

    /* If CLIENT_CLOSE_ASAP flag is set, we need not write anything. */
    // 如果client有尽早close标识，我们不需要写任何数据。
    if (c->flags & CLIENT_CLOSE_ASAP) return C_ERR;

    /* CLIENT REPLY OFF / SKIP handling: don't send replies. */
    // 如果client有关闭或跳过reply标识，我们不需要发送回复。
    if (c->flags & (CLIENT_REPLY_OFF|CLIENT_REPLY_SKIP)) return C_ERR;

    /* Masters don't receive replies, unless CLIENT_MASTER_FORCE_REPLY flag
     * is set. */
    // master连接永远不需要接收回复，除非有force reply标识
    if ((c->flags & CLIENT_MASTER) &&
        !(c->flags & CLIENT_MASTER_FORCE_REPLY)) return C_ERR;

    // 如果没有conn，fake client不需要回复。
    if (!c->conn) return C_ERR; /* Fake client for AOF loading. */

    /* Schedule the client to write the output buffers to the socket, unless
     * it should already be setup to do so (it has already pending data).
     *
     * If CLIENT_PENDING_READ is set, we're in an IO thread and should
     * not install a write handler. Instead, it will be done by
     * handleClientsWithPendingReadsUsingThreads() upon return.
     */
    // 1、如果client当前有待回复的数据，我们之前已经安装过了写处理handler，不需要再次安装了。
    // 2、如果client当前有PENDING_READ标识，说明我们开启了io线程，这里也不应安装写handler，
    //      我们会在handleClientsWithPendingReadsUsingThreads()处理完读后再安装写处理handler的。
    if (!clientHasPendingReplies(c) && !(c->flags & CLIENT_PENDING_READ))
            clientInstallWriteHandler(c);

    /* Authorize the caller to queue in the output buffer of this client. */
    // 返回ok，通知调用者可以将待回复数据加入输出缓冲区了。
    return C_OK;
}

/* -----------------------------------------------------------------------------
 * Low level functions to add more data to output buffers.
 * -------------------------------------------------------------------------- */

/* Attempts to add the reply to the static buffer in the client struct.
 * Returns C_ERR if the buffer is full, or the reply list is not empty,
 * in which case the reply must be added to the reply list. */
// 尝试将回复数据加入到client结构的静态缓存区。
// 当reply列表不为NULL，或者静态buffer已满时，返回err表示我们需要将回复加入到reply列表。
// 注意当reply列表有数据时，因为要保证回复的顺序，所以即使buffer是空的，我们也只能写数据到reply列表。
int _addReplyToBuffer(client *c, const char *s, size_t len) {
    // 计算静态buf可写入数据量
    size_t available = sizeof(c->buf)-c->bufpos;

    // 如果client有CLIENT_CLOSE_AFTER_REPLY标识，我们不写入新回复了，直接返回ok认为成功。
    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return C_OK;

    /* If there already are entries in the reply list, we cannot
     * add anything more to the static buffer. */
    // 如果reply列表有数据，我们不能往静态buf中写数据了。
    if (listLength(c->reply) > 0) return C_ERR;

    /* Check that the buffer has enough space available for this string. */
    // 检查buffer是否有足够的空间写当前回复。有才全部写入，空间不足我们就只能加入reply列表了。
    if (len > available) return C_ERR;

    // 处理写入静态buffer的情况。
    memcpy(c->buf+c->bufpos,s,len);
    c->bufpos+=len;
    return C_OK;
}

/* Adds the reply to the reply linked list.
 * Note: some edits to this function need to be relayed to AddReplyFromClient. */
// 将回复加入到reply列表中。注意对此函数的一些修改，可能也需要在AddReplyFromClient中做同样处理。
void _addReplyProtoToList(client *c, const char *s, size_t len) {
    // CLIENT_CLOSE_AFTER_REPLY，不需要写新数据了，直接返回。
    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    // 找到reply的最后一个节点。
    listNode *ln = listLast(c->reply);
    clientReplyBlock *tail = ln? listNodeValue(ln): NULL;

    /* Note that 'tail' may be NULL even if we have a tail node, because when
     * addReplyDeferredLen() is used, it sets a dummy node to NULL just
     * fo fill it later, when the size of the bulk length is set. */
    // 注意：即使我们有tail节点，tail也可能为NULL。
    // 因为当我们写入数据时，如果不知道bulk长度，则会调用addReplyDeferredLen()来创建NULL节点来占位，后续再填充长度信息。

    /* Append to tail string when possible. */
    // 如果tail节点非NULL，我们尝试追加数据到该节点。
    if (tail) {
        /* Copy the part we can fit into the tail, and leave the rest for a
         * new node */
        // 计算tail剩余空间，尽可能填充满数据。还剩余的数据要新建block来写入。
        size_t avail = tail->size - tail->used;
        size_t copy = avail >= len? len: avail;
        memcpy(tail->buf + tail->used, s, copy);
        tail->used += copy;
        s += copy;
        len -= copy;
    }
    if (len) {
        /* Create a new node, make sure it is allocated to at
         * least PROTO_REPLY_CHUNK_BYTES */
        // 还有数据待写入，我们需要创建一个新的节点处理，这里保证新节点至少PROTO_REPLY_CHUNK_BYTES大小。
        size_t size = len < PROTO_REPLY_CHUNK_BYTES? PROTO_REPLY_CHUNK_BYTES: len;
        // 分配空间，计算总size以及使用大小。
        tail = zmalloc(size + sizeof(clientReplyBlock));
        /* take over the allocation's internal fragmentation */
        tail->size = zmalloc_usable_size(tail) - sizeof(clientReplyBlock);
        tail->used = len;
        // 写入数据，将节点加入reply列表末尾，统计总的reply大小。
        memcpy(tail->buf, s, len);
        listAddNodeTail(c->reply, tail);
        c->reply_bytes += tail->size;

        // 检查client的输出buffer大小大小，如果达到了软性或硬性限制，则异步处理client连接的关闭。
        closeClientOnOutputBufferLimitReached(c, 1);
    }
}

/* -----------------------------------------------------------------------------
 * Higher level functions to queue data on the client output buffer.
 * The following functions are the ones that commands implementations will call.
 * -------------------------------------------------------------------------- */

/* Add the object 'obj' string representation to the client output buffer. */
void addReply(client *c, robj *obj) {
    if (prepareClientToWrite(c) != C_OK) return;

    if (sdsEncodedObject(obj)) {
        if (_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != C_OK)
            _addReplyProtoToList(c,obj->ptr,sdslen(obj->ptr));
    } else if (obj->encoding == OBJ_ENCODING_INT) {
        /* For integer encoded strings we just convert it into a string
         * using our optimized function, and attach the resulting string
         * to the output buffer. */
        char buf[32];
        size_t len = ll2string(buf,sizeof(buf),(long)obj->ptr);
        if (_addReplyToBuffer(c,buf,len) != C_OK)
            _addReplyProtoToList(c,buf,len);
    } else {
        serverPanic("Wrong obj->encoding in addReply()");
    }
}

/* Add the SDS 's' string to the client output buffer, as a side effect
 * the SDS string is freed. */
void addReplySds(client *c, sds s) {
    if (prepareClientToWrite(c) != C_OK) {
        /* The caller expects the sds to be free'd. */
        sdsfree(s);
        return;
    }
    if (_addReplyToBuffer(c,s,sdslen(s)) != C_OK)
        _addReplyProtoToList(c,s,sdslen(s));
    sdsfree(s);
}

/* This low level function just adds whatever protocol you send it to the
 * client buffer, trying the static buffer initially, and using the string
 * of objects if not possible.
 *
 * It is efficient because does not create an SDS object nor an Redis object
 * if not needed. The object will only be created by calling
 * _addReplyProtoToList() if we fail to extend the existing tail object
 * in the list of objects. */
// 这个低级别的函数用于添加任何你发送的数据到回复buffer，先尝试加到静态缓存区，如果不行则使用string加入reply列表。
// 这是高效的，因为如果不需要的话，我们不会创建SDS对象或redis对象。
// 只有当在_addReplyProtoToList()中当前block写满了，我们才会新创建新block对象，写入数据并加入到reply列表中
void addReplyProto(client *c, const char *s, size_t len) {
    // 准备好可写的client，检查某些标识，安装写事件处理器等。
    if (prepareClientToWrite(c) != C_OK) return;
    // 先尝试数据写入静态buffer中，如果不能写静态buf，则再写入reply列表。
    if (_addReplyToBuffer(c,s,len) != C_OK)
        _addReplyProtoToList(c,s,len);
}

/* Low level function called by the addReplyError...() functions.
 * It emits the protocol for a Redis error, in the form:
 *
 * -ERRORCODE Error Message<CR><LF>
 *
 * If the error code is already passed in the string 's', the error
 * code provided is used, otherwise the string "-ERR " for the generic
 * error code is automatically added.
 * Note that 's' must NOT end with \r\n. */
void addReplyErrorLength(client *c, const char *s, size_t len) {
    /* If the string already starts with "-..." then the error code
     * is provided by the caller. Otherwise we use "-ERR". */
    if (!len || s[0] != '-') addReplyProto(c,"-ERR ",5);
    addReplyProto(c,s,len);
    addReplyProto(c,"\r\n",2);
}

/* Do some actions after an error reply was sent (Log if needed, updates stats, etc.) */
void afterErrorReply(client *c, const char *s, size_t len) {
    /* Increment the global error counter */
    server.stat_total_error_replies++;
    /* Increment the error stats
     * If the string already starts with "-..." then the error prefix
     * is provided by the caller ( we limit the search to 32 chars). Otherwise we use "-ERR". */
    if (s[0] != '-') {
        incrementErrorCount("ERR", 3);
    } else {
        char *spaceloc = memchr(s, ' ', len < 32 ? len : 32);
        if (spaceloc) {
            const size_t errEndPos = (size_t)(spaceloc - s);
            incrementErrorCount(s+1, errEndPos-1);
        } else {
            /* Fallback to ERR if we can't retrieve the error prefix */
            incrementErrorCount("ERR", 3);
        }
    }

    /* Sometimes it could be normal that a slave replies to a master with
     * an error and this function gets called. Actually the error will never
     * be sent because addReply*() against master clients has no effect...
     * A notable example is:
     *
     *    EVAL 'redis.call("incr",KEYS[1]); redis.call("nonexisting")' 1 x
     *
     * Where the master must propagate the first change even if the second
     * will produce an error. However it is useful to log such events since
     * they are rare and may hint at errors in a script or a bug in Redis. */
    int ctype = getClientType(c);
    if (ctype == CLIENT_TYPE_MASTER || ctype == CLIENT_TYPE_SLAVE || c->id == CLIENT_ID_AOF) {
        char *to, *from;

        if (c->id == CLIENT_ID_AOF) {
            to = "AOF-loading-client";
            from = "server";
        } else if (ctype == CLIENT_TYPE_MASTER) {
            to = "master";
            from = "replica";
        } else {
            to = "replica";
            from = "master";
        }

        if (len > 4096) len = 4096;
        char *cmdname = c->lastcmd ? c->lastcmd->name : "<unknown>";
        serverLog(LL_WARNING,"== CRITICAL == This %s is sending an error "
                             "to its %s: '%.*s' after processing the command "
                             "'%s'", from, to, (int)len, s, cmdname);
        if (ctype == CLIENT_TYPE_MASTER && server.repl_backlog &&
            server.repl_backlog_histlen > 0)
        {
            showLatestBacklog();
        }
        server.stat_unexpected_error_replies++;
    }
}

/* The 'err' object is expected to start with -ERRORCODE and end with \r\n.
 * Unlike addReplyErrorSds and others alike which rely on addReplyErrorLength. */
void addReplyErrorObject(client *c, robj *err) {
    addReply(c, err);
    afterErrorReply(c, err->ptr, sdslen(err->ptr)-2); /* Ignore trailing \r\n */
}

/* See addReplyErrorLength for expectations from the input string. */
void addReplyError(client *c, const char *err) {
    addReplyErrorLength(c,err,strlen(err));
    afterErrorReply(c,err,strlen(err));
}

/* See addReplyErrorLength for expectations from the input string. */
/* As a side effect the SDS string is freed. */
void addReplyErrorSds(client *c, sds err) {
    addReplyErrorLength(c,err,sdslen(err));
    afterErrorReply(c,err,sdslen(err));
    sdsfree(err);
}

/* See addReplyErrorLength for expectations from the formatted string.
 * The formatted string is safe to contain \r and \n anywhere. */
void addReplyErrorFormat(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    /* Trim any newlines at the end (ones will be added by addReplyErrorLength) */
    s = sdstrim(s, "\r\n");
    /* Make sure there are no newlines in the middle of the string, otherwise
     * invalid protocol is emitted. */
    s = sdsmapchars(s, "\r\n", "  ",  2);
    addReplyErrorLength(c,s,sdslen(s));
    afterErrorReply(c,s,sdslen(s));
    sdsfree(s);
}

void addReplyStatusLength(client *c, const char *s, size_t len) {
    addReplyProto(c,"+",1);
    addReplyProto(c,s,len);
    addReplyProto(c,"\r\n",2);
}

void addReplyStatus(client *c, const char *status) {
    addReplyStatusLength(c,status,strlen(status));
}

void addReplyStatusFormat(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    addReplyStatusLength(c,s,sdslen(s));
    sdsfree(s);
}

/* Sometimes we are forced to create a new reply node, and we can't append to
 * the previous one, when that happens, we wanna try to trim the unused space
 * at the end of the last reply node which we won't use anymore. */
// 有时候我们写入回复时不能追加到最后一个节点中，要创建一个新的reply节点处理。
// 此时我们需要尝试trim释放掉当前最后一个节点中不再使用的空间。
void trimReplyUnusedTailSpace(client *c) {
    // 取到最后一个节点
    listNode *ln = listLast(c->reply);
    clientReplyBlock *tail = ln? listNodeValue(ln): NULL;

    /* Note that 'tail' may be NULL even if we have a tail node, because when
     * addReplyDeferredLen() is used */
    // 如果最后一个节点已经是NULL了，可能之前已经调用addReplyDeferredLen()处理过了，直接返回。
    if (!tail) return;

    /* We only try to trim the space is relatively high (more than a 1/4 of the
     * allocation), otherwise there's a high chance realloc will NOP.
     * Also, to avoid large memmove which happens as part of realloc, we only do
     * that if the used part is small.  */
    // 我们只尝试trim处理有较多未使用空间（多余tail->size/4 ）的节点，否则如果trim后释放空间很少，可能得不偿失。
    // 另外为了避免很多数据的memmove，我们只trim处理used部分小于PROTO_REPLY_CHUNK_BYTES的节点。
    if (tail->size - tail->used > tail->size / 4 &&
        tail->used < PROTO_REPLY_CHUNK_BYTES)
    {
        size_t old_size = tail->size;
        // 重新分配空间
        tail = zrealloc(tail, tail->used + sizeof(clientReplyBlock));
        /* take over the allocation's internal fragmentation (at least for
         * memory usage tracking) */
        // 计算实际分配的size大小，分配时可能会有内存对齐或避免碎片多分配？
        tail->size = zmalloc_usable_size(tail) - sizeof(clientReplyBlock);
        // 更新reply列表所有obj占内存大小。
        c->reply_bytes = c->reply_bytes + tail->size - old_size;
        listNodeValue(ln) = tail;
    }
}

/* Adds an empty object to the reply list that will contain the multi bulk
 * length, which is not known when this function is called. */
// 将一个空对象添加到回复列表中，因为待写入的回复长度还未知，我们没办法提前分配长度，所以写一个NULL作为占位符。
void *addReplyDeferredLen(client *c) {
    /* Note that we install the write event here even if the object is not
     * ready to be sent, since we are sure that before returning to the
     * event loop setDeferredAggregateLen() will be called. */
    // 注意，即使对象尚未准备好发送，我们也会在此安装写处理handler。
    // 因为我们确定在返回到事件循环之前setDeferredAggregateLen()将被调用。
    if (prepareClientToWrite(c) != C_OK) return NULL;
    // 移除当前最后一个节点未使用的空间
    trimReplyUnusedTailSpace(c);
    // 使用NULL占位符创建新的节点加入到reply列表中。
    listAddNodeTail(c->reply,NULL); /* NULL is our placeholder. */
    return listLast(c->reply);
}

void setDeferredReply(client *c, void *node, const char *s, size_t length) {
    listNode *ln = (listNode*)node;
    clientReplyBlock *next, *prev;

    /* Abort when *node is NULL: when the client should not accept writes
     * we return NULL in addReplyDeferredLen() */
    // 如果node为NULL，返回。addReplyDeferredLen()中当client不能写时，是会返回NULL的。
    if (node == NULL) return;
    serverAssert(!listNodeValue(ln));

    /* Normally we fill this dummy NULL node, added by addReplyDeferredLen(),
     * with a new buffer structure containing the protocol needed to specify
     * the length of the array following. However sometimes there might be room
     * in the previous/next node so we can instead remove this NULL node, and
     * suffix/prefix our data in the node immediately before/after it, in order
     * to save a write(2) syscall later. Conditions needed to do it:
     *
     * - The prev node is non-NULL and has space in it or
     * - The next node is non-NULL,
     * - It has enough room already allocated
     * - And not too large (avoid large memmove) */
    // 通常我们会构造一个新的buffer数据存储长度信息，来填充这个虚拟的NULL节点。
    // 但是有时候这个NULL节点前后的节点有可能有空间能容纳下这个长度信息，所以我们可以将长度数据追加到prev的末尾，或者是next的开头。
    // prev节点有空间的话可以直接追加；next有空间我们只在数据量不大的情况下才加到开头，避免过多的内存移动。
    if (ln->prev != NULL && (prev = listNodeValue(ln->prev)) &&
        prev->size - prev->used > 0)
    {
        // 如果prev节点有空间，则计算可用空间大小。
        size_t len_to_copy = prev->size - prev->used;
        if (len_to_copy > length)
            len_to_copy = length;
        // 尽可能存入部分数据
        memcpy(prev->buf + prev->used, s, len_to_copy);
        prev->used += len_to_copy;
        length -= len_to_copy;
        // 如果所有数据都能写入prev节点，那么就可以直接删除占位的NULL节点返回了。否则我们还需要找后面的节点来获取空间写入。
        if (length == 0) {
            listDelNode(c->reply, ln);
            return;
        }
        // s指向剩余待写入的位置
        s += len_to_copy;
    }

    if (ln->next != NULL && (next = listNodeValue(ln->next)) &&
        next->size - next->used >= length &&
        next->used < PROTO_REPLY_CHUNK_BYTES * 4)
    {
        // 如果next我们写入回复数据的节点有空间可用，并且next节点数据长度 < 64k 的话，后移腾出空间来用于写入剩余数据。
        memmove(next->buf + length, next->buf, next->used);
        memcpy(next->buf, s, length);
        next->used += length;
        // 全部写完了，删除占位的NULL节点。
        listDelNode(c->reply,ln);
    } else {
        /* Create a new node */
        // 如果不能写入next节点，可能next为NULL，可能next节点空间不足，也可能next节点使用的长度超过64k（避免过多内存数据移动）。
        // 创建一个新的block来写入数据。
        clientReplyBlock *buf = zmalloc(length + sizeof(clientReplyBlock));
        /* Take over the allocation's internal fragmentation */
        buf->size = zmalloc_usable_size(buf) - sizeof(clientReplyBlock);
        buf->used = length;
        memcpy(buf->buf, s, length);
        listNodeValue(ln) = buf;
        // 注意只有每次新增reply节点的时候，我们将整个size大小加入到reply_bytes中。
        // 这样总共分配的空间即为我们reply所使用的大小，将内存碎片也考虑进去了。
        c->reply_bytes += buf->size;

        // 检查如果超出了output buf限制则异步释放client。
        closeClientOnOutputBufferLimitReached(c, 1);
    }
}

/* Populate the length object and try gluing it to the next chunk. */
// 填充node中数据的length信息。
void setDeferredAggregateLen(client *c, void *node, long length, char prefix) {
    serverAssert(length >= 0);

    /* Abort when *node is NULL: when the client should not accept writes
     * we return NULL in addReplyDeferredLen() */
    // 这里node为NULL时，直接返回。addReplyDeferredLen()中当client不应该接收写操作时是会返回NULL的。
    if (node == NULL) return;

    char lenstr[128];
    // 格式化长度数据
    size_t lenstr_len = sprintf(lenstr, "%c%ld\r\n", prefix, length);
    // 长度信息加入到回复中
    setDeferredReply(c, node, lenstr, lenstr_len);
}

void setDeferredArrayLen(client *c, void *node, long length) {
    setDeferredAggregateLen(c,node,length,'*');
}

void setDeferredMapLen(client *c, void *node, long length) {
    int prefix = c->resp == 2 ? '*' : '%';
    if (c->resp == 2) length *= 2;
    setDeferredAggregateLen(c,node,length,prefix);
}

void setDeferredSetLen(client *c, void *node, long length) {
    int prefix = c->resp == 2 ? '*' : '~';
    setDeferredAggregateLen(c,node,length,prefix);
}

void setDeferredAttributeLen(client *c, void *node, long length) {
    int prefix = c->resp == 2 ? '*' : '|';
    if (c->resp == 2) length *= 2;
    setDeferredAggregateLen(c,node,length,prefix);
}

void setDeferredPushLen(client *c, void *node, long length) {
    int prefix = c->resp == 2 ? '*' : '>';
    setDeferredAggregateLen(c,node,length,prefix);
}

/* Add a double as a bulk reply */
void addReplyDouble(client *c, double d) {
    if (isinf(d)) {
        /* Libc in odd systems (Hi Solaris!) will format infinite in a
         * different way, so better to handle it in an explicit way. */
        if (c->resp == 2) {
            addReplyBulkCString(c, d > 0 ? "inf" : "-inf");
        } else {
            addReplyProto(c, d > 0 ? ",inf\r\n" : ",-inf\r\n",
                              d > 0 ? 6 : 7);
        }
    } else {
        char dbuf[MAX_LONG_DOUBLE_CHARS+3],
             sbuf[MAX_LONG_DOUBLE_CHARS+32];
        int dlen, slen;
        if (c->resp == 2) {
            dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
            slen = snprintf(sbuf,sizeof(sbuf),"$%d\r\n%s\r\n",dlen,dbuf);
            addReplyProto(c,sbuf,slen);
        } else {
            dlen = snprintf(dbuf,sizeof(dbuf),",%.17g\r\n",d);
            addReplyProto(c,dbuf,dlen);
        }
    }
}

/* Add a long double as a bulk reply, but uses a human readable formatting
 * of the double instead of exposing the crude behavior of doubles to the
 * dear user. */
void addReplyHumanLongDouble(client *c, long double d) {
    if (c->resp == 2) {
        robj *o = createStringObjectFromLongDouble(d,1);
        addReplyBulk(c,o);
        decrRefCount(o);
    } else {
        char buf[MAX_LONG_DOUBLE_CHARS];
        int len = ld2string(buf,sizeof(buf),d,LD_STR_HUMAN);
        addReplyProto(c,",",1);
        addReplyProto(c,buf,len);
        addReplyProto(c,"\r\n",2);
    }
}

/* Add a long long as integer reply or bulk len / multi bulk count.
 * Basically this is used to output <prefix><long long><crlf>. */
void addReplyLongLongWithPrefix(client *c, long long ll, char prefix) {
    char buf[128];
    int len;

    /* Things like $3\r\n or *2\r\n are emitted very often by the protocol
     * so we have a few shared objects to use if the integer is small
     * like it is most of the times. */
    if (prefix == '*' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        addReply(c,shared.mbulkhdr[ll]);
        return;
    } else if (prefix == '$' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        addReply(c,shared.bulkhdr[ll]);
        return;
    }

    buf[0] = prefix;
    len = ll2string(buf+1,sizeof(buf)-1,ll);
    buf[len+1] = '\r';
    buf[len+2] = '\n';
    addReplyProto(c,buf,len+3);
}

void addReplyLongLong(client *c, long long ll) {
    if (ll == 0)
        addReply(c,shared.czero);
    else if (ll == 1)
        addReply(c,shared.cone);
    else
        addReplyLongLongWithPrefix(c,ll,':');
}

void addReplyAggregateLen(client *c, long length, int prefix) {
    serverAssert(length >= 0);
    addReplyLongLongWithPrefix(c,length,prefix);
}

void addReplyArrayLen(client *c, long length) {
    addReplyAggregateLen(c,length,'*');
}

void addReplyMapLen(client *c, long length) {
    int prefix = c->resp == 2 ? '*' : '%';
    if (c->resp == 2) length *= 2;
    addReplyAggregateLen(c,length,prefix);
}

void addReplySetLen(client *c, long length) {
    int prefix = c->resp == 2 ? '*' : '~';
    addReplyAggregateLen(c,length,prefix);
}

void addReplyAttributeLen(client *c, long length) {
    int prefix = c->resp == 2 ? '*' : '|';
    if (c->resp == 2) length *= 2;
    addReplyAggregateLen(c,length,prefix);
}

void addReplyPushLen(client *c, long length) {
    int prefix = c->resp == 2 ? '*' : '>';
    addReplyAggregateLen(c,length,prefix);
}

void addReplyNull(client *c) {
    if (c->resp == 2) {
        addReplyProto(c,"$-1\r\n",5);
    } else {
        addReplyProto(c,"_\r\n",3);
    }
}

void addReplyBool(client *c, int b) {
    if (c->resp == 2) {
        addReply(c, b ? shared.cone : shared.czero);
    } else {
        addReplyProto(c, b ? "#t\r\n" : "#f\r\n",4);
    }
}

/* A null array is a concept that no longer exists in RESP3. However
 * RESP2 had it, so API-wise we have this call, that will emit the correct
 * RESP2 protocol, however for RESP3 the reply will always be just the
 * Null type "_\r\n". */
// RESP3中已没有了空数组的概念，但RESP2中还有，所以还有这个API调用。
// 这个函数将正确de发送RESP2协议数据，对于RESP3会发送空类型"_\r\n"。
void addReplyNullArray(client *c) {
    if (c->resp == 2) {
        addReplyProto(c,"*-1\r\n",5);
    } else {
        addReplyProto(c,"_\r\n",3);
    }
}

/* Create the length prefix of a bulk reply, example: $2234 */
void addReplyBulkLen(client *c, robj *obj) {
    size_t len = stringObjectLen(obj);

    addReplyLongLongWithPrefix(c,len,'$');
}

/* Add a Redis Object as a bulk reply */
void addReplyBulk(client *c, robj *obj) {
    addReplyBulkLen(c,obj);
    addReply(c,obj);
    addReply(c,shared.crlf);
}

/* Add a C buffer as bulk reply */
void addReplyBulkCBuffer(client *c, const void *p, size_t len) {
    addReplyLongLongWithPrefix(c,len,'$');
    addReplyProto(c,p,len);
    addReply(c,shared.crlf);
}

/* Add sds to reply (takes ownership of sds and frees it) */
void addReplyBulkSds(client *c, sds s)  {
    addReplyLongLongWithPrefix(c,sdslen(s),'$');
    addReplySds(c,s);
    addReply(c,shared.crlf);
}

/* Set sds to a deferred reply (for symmetry with addReplyBulkSds it also frees the sds) */
void setDeferredReplyBulkSds(client *c, void *node, sds s) {
    sds reply = sdscatprintf(sdsempty(), "$%d\r\n%s\r\n", (unsigned)sdslen(s), s);
    setDeferredReply(c, node, reply, sdslen(reply));
    sdsfree(reply);
    sdsfree(s);
}

/* Add a C null term string as bulk reply */
void addReplyBulkCString(client *c, const char *s) {
    if (s == NULL) {
        addReplyNull(c);
    } else {
        addReplyBulkCBuffer(c,s,strlen(s));
    }
}

/* Add a long long as a bulk reply */
void addReplyBulkLongLong(client *c, long long ll) {
    char buf[64];
    int len;

    len = ll2string(buf,64,ll);
    addReplyBulkCBuffer(c,buf,len);
}

/* Reply with a verbatim type having the specified extension.
 *
 * The 'ext' is the "extension" of the file, actually just a three
 * character type that describes the format of the verbatim string.
 * For instance "txt" means it should be interpreted as a text only
 * file by the receiver, "md " as markdown, and so forth. Only the
 * three first characters of the extension are used, and if the
 * provided one is shorter than that, the remaining is filled with
 * spaces. */
void addReplyVerbatim(client *c, const char *s, size_t len, const char *ext) {
    if (c->resp == 2) {
        addReplyBulkCBuffer(c,s,len);
    } else {
        char buf[32];
        size_t preflen = snprintf(buf,sizeof(buf),"=%zu\r\nxxx:",len+4);
        char *p = buf+preflen-4;
        for (int i = 0; i < 3; i++) {
            if (*ext == '\0') {
                p[i] = ' ';
            } else {
                p[i] = *ext++;
            }
        }
        addReplyProto(c,buf,preflen);
        addReplyProto(c,s,len);
        addReplyProto(c,"\r\n",2);
    }
}

/* Add an array of C strings as status replies with a heading.
 * This function is typically invoked by from commands that support
 * subcommands in response to the 'help' subcommand. The help array
 * is terminated by NULL sentinel. */
void addReplyHelp(client *c, const char **help) {
    sds cmd = sdsnew((char*) c->argv[0]->ptr);
    void *blenp = addReplyDeferredLen(c);
    int blen = 0;

    sdstoupper(cmd);
    addReplyStatusFormat(c,
        "%s <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",cmd);
    sdsfree(cmd);

    while (help[blen]) addReplyStatus(c,help[blen++]);

    addReplyStatus(c,"HELP");
    addReplyStatus(c,"    Prints this help.");

    blen += 1;  /* Account for the header. */
    blen += 2;  /* Account for the footer. */
    setDeferredArrayLen(c,blenp,blen);
}

/* Add a suggestive error reply.
 * This function is typically invoked by from commands that support
 * subcommands in response to an unknown subcommand or argument error. */
void addReplySubcommandSyntaxError(client *c) {
    sds cmd = sdsnew((char*) c->argv[0]->ptr);
    sdstoupper(cmd);
    addReplyErrorFormat(c,
        "Unknown subcommand or wrong number of arguments for '%s'. Try %s HELP.",
        (char*)c->argv[1]->ptr,cmd);
    sdsfree(cmd);
}

/* Append 'src' client output buffers into 'dst' client output buffers.
 * This function clears the output buffers of 'src' */
void AddReplyFromClient(client *dst, client *src) {
    /* If the source client contains a partial response due to client output
     * buffer limits, propagate that to the dest rather than copy a partial
     * reply. We don't wanna run the risk of copying partial response in case
     * for some reason the output limits don't reach the same decision (maybe
     * they changed) */
    if (src->flags & CLIENT_CLOSE_ASAP) {
        sds client = catClientInfoString(sdsempty(),dst);
        freeClientAsync(dst);
        serverLog(LL_WARNING,"Client %s scheduled to be closed ASAP for overcoming of output buffer limits.", client);
        sdsfree(client);
        return;
    }

    /* First add the static buffer (either into the static buffer or reply list) */
    addReplyProto(dst,src->buf, src->bufpos);

    /* We need to check with prepareClientToWrite again (after addReplyProto)
     * since addReplyProto may have changed something (like CLIENT_CLOSE_ASAP) */
    if (prepareClientToWrite(dst) != C_OK)
        return;

    /* We're bypassing _addReplyProtoToList, so we need to add the pre/post
     * checks in it. */
    if (dst->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    /* Concatenate the reply list into the dest */
    if (listLength(src->reply))
        listJoin(dst->reply,src->reply);
    dst->reply_bytes += src->reply_bytes;
    src->reply_bytes = 0;
    src->bufpos = 0;

    /* Check output buffer limits */
    closeClientOnOutputBufferLimitReached(dst, 1);
}

/* Copy 'src' client output buffers into 'dst' client output buffers.
 * The function takes care of freeing the old output buffers of the
 * destination client. */
// 复制'src' client的回复缓冲区数据给'dst'。这里我们会释放'dst'原来的输出缓冲区数据。
void copyClientOutputBuffer(client *dst, client *src) {
    listRelease(dst->reply);
    dst->sentlen = 0;
    dst->reply = listDup(src->reply);
    memcpy(dst->buf,src->buf,src->bufpos);
    dst->bufpos = src->bufpos;
    dst->reply_bytes = src->reply_bytes;
}

/* Return true if the specified client has pending reply buffers to write to
 * the socket. */
// 如果output buf有数据，或者reply列表中有数据，则说明当前有数据需要回复给这个client。
int clientHasPendingReplies(client *c) {
    return c->bufpos || listLength(c->reply);
}

void clientAcceptHandler(connection *conn) {
    client *c = connGetPrivateData(conn);

    // 调用这个函数之前，已经设置了conn状态为CONN_STATE_CONNECTED
    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_WARNING,
                "Error accepting a client connection: %s",
                connGetLastError(conn));
        freeClientAsync(c);
        return;
    }

    /* If the server is running in protected mode (the default) and there
     * is no password set, nor a specific interface is bound, we don't accept
     * requests from non loopback interfaces. Instead we try to explain the
     * user what to do to fix it if needed. */
    // 如果服务是在protected（默认）模式运行，没设置密码，也没绑定特定地址接口，我们将不接受来自非回环地址的请求。
    if (server.protected_mode &&
        server.bindaddr_count == 0 &&
        DefaultUser->flags & USER_FLAG_NOPASS &&
        !(c->flags & CLIENT_UNIX_SOCKET))
    {
        char cip[NET_IP_STR_LEN+1] = { 0 };
        // 获取对端，即client的ip
        connPeerToString(conn, cip, sizeof(cip)-1, NULL);

        // 如果client ip不是"127.0.0.1"，也不是"::1"，则返回错误信息给client。
        // 注意这里strcmp比较，相等返回0，非0即是不相等，非0表示true。
        if (strcmp(cip,"127.0.0.1") && strcmp(cip,"::1")) {
            char *err =
                "-DENIED Redis is running in protected mode because protected "
                "mode is enabled, no bind address was specified, no "
                "authentication password is requested to clients. In this mode "
                "connections are only accepted from the loopback interface. "
                "If you want to connect from external computers to Redis you "
                "may adopt one of the following solutions: "
                "1) Just disable protected mode sending the command "
                "'CONFIG SET protected-mode no' from the loopback interface "
                "by connecting to Redis from the same host the server is "
                "running, however MAKE SURE Redis is not publicly accessible "
                "from internet if you do so. Use CONFIG REWRITE to make this "
                "change permanent. "
                "2) Alternatively you can just disable the protected mode by "
                "editing the Redis configuration file, and setting the protected "
                "mode option to 'no', and then restarting the server. "
                "3) If you started the server manually just for testing, restart "
                "it with the '--protected-mode no' option. "
                "4) Setup a bind address or an authentication password. "
                "NOTE: You only need to do one of the above things in order for "
                "the server to start accepting connections from the outside.\r\n";
            if (connWrite(c->conn,err,strlen(err)) == -1) {
                /* Nothing to do, Just to avoid the warning... */
            }
            server.stat_rejected_conn++;
            freeClientAsync(c);
            return;
        }
    }

    server.stat_numconnections++;
    moduleFireServerEvent(REDISMODULE_EVENT_CLIENT_CHANGE,
                          REDISMODULE_SUBEVENT_CLIENT_CHANGE_CONNECTED,
                          c);
}

#define MAX_ACCEPTS_PER_CALL 1000
static void acceptCommonHandler(connection *conn, int flags, char *ip) {
    client *c;
    char conninfo[100];
    UNUSED(ip);

    // 如果当前conn状态不是CONN_STATE_ACCEPTING，不会走到这里。记录log，关闭conn，返回。
    if (connGetState(conn) != CONN_STATE_ACCEPTING) {
        serverLog(LL_VERBOSE,
            "Accepted client connection in error state: %s (conn: %s)",
            connGetLastError(conn),
            connGetInfo(conn, conninfo, sizeof(conninfo)));
        connClose(conn);
        return;
    }

    /* Limit the number of connections we take at the same time.
     *
     * Admission control will happen before a client is created and connAccept()
     * called, because we don't want to even start transport-level negotiation
     * if rejected. */
    // 检查服务器最大连接数是否超了。在创建cleint和调用connAccept()之前尽早处理拒绝，避免进行一些无效的处理。
    if (listLength(server.clients) + getClusterConnectionsCount()
        >= server.maxclients)
    {
        char *err;
        if (server.cluster_enabled)
            err = "-ERR max number of clients + cluster "
                  "connections reached\r\n";
        else
            err = "-ERR max number of clients reached\r\n";

        /* That's a best effort error message, don't check write errors.
         * Note that for TLS connections, no handshake was done yet so nothing
         * is written and the connection will just drop. */
        // 发送错误消息到conn中，尽最大努力，不做错误处理。
        if (connWrite(conn,err,strlen(err)) == -1) {
            /* Nothing to do, Just to avoid the warning... */
        }
        server.stat_rejected_conn++;
        connClose(conn);
        return;
    }

    /* Create connection and client */
    // 基于conn创建client，处理传输相关协商（主要是client相关field初始化）。
    if ((c = createClient(conn)) == NULL) {
        serverLog(LL_WARNING,
            "Error registering fd event for the new client: %s (conn: %s)",
            connGetLastError(conn),
            connGetInfo(conn, conninfo, sizeof(conninfo)));
        connClose(conn); /* May be already closed, just ignore errors */
        return;
    }

    /* Last chance to keep flags */
    c->flags |= flags;

    /* Initiate accept.
     *
     * Note that connAccept() is free to do two things here:
     * 1. Call clientAcceptHandler() immediately;
     * 2. Schedule a future call to clientAcceptHandler().
     *
     * Because of that, we must do nothing else afterwards.
     */
    // 如果是socket conn，会调用 connSocketAccept 设置conn状态为 CONN_STATE_CONNECTED。
    // 然后调用clientAcceptHandler做一些服务层连接处理。完成之后一个连接即建立起来了，可以处理请求了。
    if (connAccept(conn, clientAcceptHandler) == C_ERR) {
        char conninfo[100];
        if (connGetState(conn) == CONN_STATE_ERROR)
            serverLog(LL_WARNING,
                    "Error accepting a client connection: %s (conn: %s)",
                    connGetLastError(conn), connGetInfo(conn, conninfo, sizeof(conninfo)));
        freeClient(connGetPrivateData(conn));
        return;
    }
}

void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd, max = MAX_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    // 一次调用可以处理1000个请求
    while(max--) {
        // 调用anet里面的TCP accept，并获取对端ip和port。
        // 如果所有连接都处理完了，因为是非阻塞的，没有连接会返回EWOULDBLOCK错误，从而直接返回，等待新连接进来再次触发事件即可。
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                    "Accepting client connection: %s", server.neterr);
            return;
        }
        anetCloexec(cfd);
        serverLog(LL_VERBOSE,"Accepted %s:%d", cip, cport);
        // 通过accept返回的fd创建socket conn，并基于conn做相关操作如创建client等。
        acceptCommonHandler(connCreateAcceptedSocket(cfd),0,cip);
    }
}

void acceptTLSHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd, max = MAX_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while(max--) {
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                    "Accepting client connection: %s", server.neterr);
            return;
        }
        anetCloexec(cfd);
        serverLog(LL_VERBOSE,"Accepted %s:%d", cip, cport);
        acceptCommonHandler(connCreateAcceptedTLS(cfd, server.tls_auth_clients),0,cip);
    }
}

void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cfd, max = MAX_ACCEPTS_PER_CALL;
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while(max--) {
        cfd = anetUnixAccept(server.neterr, fd);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                    "Accepting client connection: %s", server.neterr);
            return;
        }
        anetCloexec(cfd);
        serverLog(LL_VERBOSE,"Accepted connection to %s", server.unixsocket);
        acceptCommonHandler(connCreateAcceptedSocket(cfd),CLIENT_UNIX_SOCKET,NULL);
    }
}

// 释放client原来的参数
void freeClientOriginalArgv(client *c) {
    /* We didn't rewrite this client */
    // 如果没有重写过这个client命令参数，也就没有原来的参数，不需要处理直接返回
    if (!c->original_argv) return;

    // 释放original_argv，先遍历处理每个参数，再释放整个结构
    for (int j = 0; j < c->original_argc; j++)
        decrRefCount(c->original_argv[j]);
    zfree(c->original_argv);
    c->original_argv = NULL;
    c->original_argc = 0;
}

static void freeClientArgv(client *c) {
    int j;
    for (j = 0; j < c->argc; j++)
        // 释放所有参数obj
        decrRefCount(c->argv[j]);
    // 其他参数清空
    c->argc = 0;
    c->cmd = NULL;
    c->argv_len_sum = 0;
}

/* Close all the slaves connections. This is useful in chained replication
 * when we resync with our own master and want to force all our slaves to
 * resync with us as well. */
// 关闭所有的slaves连接。当我们与master重新同步，并希望强制所有salves也与我们重新同步时，在这种的多级链式处理中，这样是很有用的。
void disconnectSlaves(void) {
    listIter li;
    listNode *ln;
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        // 遍历server.slaves，释放连接
        freeClient((client*)ln->value);
    }
}

/* Check if there is any other slave waiting dumping RDB finished expect me.
 * This function is useful to judge current dumping RDB can be used for full
 * synchronization or not. */
// 遍历server.slaves，检查是否有其他的slaves正在等待RDB dump结束。
// 这个函数主要用于判断当前正dump的RDB是否能用于全量数据同步。
int anyOtherSlaveWaitRdb(client *except_me) {
    listIter li;
    listNode *ln;

    listRewind(server.slaves, &li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;
        // 遍历节点，排除自己。找到一个在等待RDB dump结束的slave，则返回1。
        if (slave != except_me &&
            slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END)
        {
            return 1;
        }
    }
    return 0;
}

/* Remove the specified client from global lists where the client could
 * be referenced, not including the Pub/Sub channels.
 * This is used by freeClient() and replicationCacheMaster(). */
// 从server的各种全局列表中移除当前client的引用。（不包含Pub/Sub channels订阅处理）
// 这个函数用于freeClient() 和 replicationCacheMaster()
void unlinkClient(client *c) {
    listNode *ln;

    /* If this is marked as current client unset it. */
    // 如果要释放的client被标记为服务的current_client，则置空
    if (server.current_client == c) server.current_client = NULL;

    /* Certain operations must be done only if the client has an active connection.
     * If the client was already unlinked or if it's a "fake client" the
     * conn is already set to NULL. */
    // 只有在client有活跃的conn时，才执行关闭conn操作。
    // 如果client已经unlink了或者client是构造的"fake client"，conn已经置为NULL了，不需要再关闭conn处理。
    if (c->conn) {
        /* Remove from the list of active clients. */
        // 断开conn操作：
        // 1、如果当前活跃的clients中有当前client，则移除（字典和列表都移除）
        if (c->client_list_node) {
            uint64_t id = htonu64(c->id);
            raxRemove(server.clients_index,(unsigned char*)&id,sizeof(id),NULL);
            listDelNode(server.clients,c->client_list_node);
            c->client_list_node = NULL;
        }

        /* Check if this is a replica waiting for diskless replication (rdb pipe),
         * in which case it needs to be cleaned from that list */
        // 2、检查这个client是否是slave，且正等待rdb pipe。如果是要从rdb_pipe_conns队列移除。
        if (c->flags & CLIENT_SLAVE &&
            c->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
            server.rdb_pipe_conns)
        {
            int i;
            for (i=0; i < server.rdb_pipe_numconns; i++) {
                if (server.rdb_pipe_conns[i] == c->conn) {
                    // 从rdb_pipe_conns移除当前conn
                    rdbPipeWriteHandlerConnRemoved(c->conn);
                    server.rdb_pipe_conns[i] = NULL;
                    break;
                }
            }
        }
        // 3、close 连接
        connClose(c->conn);
        c->conn = NULL;
    }

    /* Remove from the list of pending writes if needed. */
    // 如果当前client待写，在pending write队列，这里清除掉。
    if (c->flags & CLIENT_PENDING_WRITE) {
        ln = listSearchKey(server.clients_pending_write,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_pending_write,ln);
        c->flags &= ~CLIENT_PENDING_WRITE;
    }

    /* Remove from the list of pending reads if needed. */
    // 如果当前client待读，在pending read队列，这里清除掉。
    if (c->flags & CLIENT_PENDING_READ) {
        ln = listSearchKey(server.clients_pending_read,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_pending_read,ln);
        c->flags &= ~CLIENT_PENDING_READ;
    }

    /* When client was just unblocked because of a blocking operation,
     * remove it from the list of unblocked clients. */
    // 如果这个client被unblocked了，在unblocked队列中，清理掉。
    if (c->flags & CLIENT_UNBLOCKED) {
        ln = listSearchKey(server.unblocked_clients,c);
        serverAssert(ln != NULL);
        listDelNode(server.unblocked_clients,ln);
        c->flags &= ~CLIENT_UNBLOCKED;
    }

    /* Clear the tracking status. */
    // 清理对这个client tracking的数据。
    if (c->flags & CLIENT_TRACKING) disableTracking(c);
}

// 释放client的具体逻辑
void freeClient(client *c) {
    listNode *ln;

    /* If a client is protected, yet we need to free it right now, make sure
     * to at least use asynchronous freeing. */
    // 如果client是保护状态，暂时不处理释放。加入clients_to_close队列后续处理。
    if (c->flags & CLIENT_PROTECTED) {
        freeClientAsync(c);
        return;
    }

    /* For connected clients, call the disconnection event of modules hooks. */
    // 如果client处于连接状态，因为我们要关闭连接，所以需要通知disconnection给一些modules。
    if (c->conn) {
        moduleFireServerEvent(REDISMODULE_EVENT_CLIENT_CHANGE,
                              REDISMODULE_SUBEVENT_CLIENT_CHANGE_DISCONNECTED,
                              c);
    }

    /* Notify module system that this client auth status changed. */
    // 通知module系统这个client的auth状态变更。
    moduleNotifyUserChanged(c);

    /* If this client was scheduled for async freeing we need to remove it
     * from the queue. Note that we need to do this here, because later
     * we may call replicationCacheMaster() and the client should already
     * be removed from the list of clients to free. */
    // 注意client有可能先放入clients_to_close列表，后面又直接调用freeClient处理释放。
    // 所以这里检查client是否有走异步释放的流程，有的话从列表中移除。
    if (c->flags & CLIENT_CLOSE_ASAP) {
        ln = listSearchKey(server.clients_to_close,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_to_close,ln);
    }

    /* If it is our master that's being disconnected we should make sure
     * to cache the state to try a partial resynchronization later.
     *
     * Note that before doing this we make sure that the client is not in
     * some unexpected state, by checking its flags. */
    // 如果要释放的client是我们的master，需要确保缓存状态，用于下次尝试部分重新同步（需要同步ID和offset）。
    // 注意在做这之前，我们要先检查下flags，确保client不是处于我们不期望的状态。
    if (server.master && c->flags & CLIENT_MASTER) {
        serverLog(LL_WARNING,"Connection with master lost.");
        if (!(c->flags & (CLIENT_PROTOCOL_ERROR|CLIENT_BLOCKED))) {
            c->flags &= ~(CLIENT_CLOSE_ASAP|CLIENT_CLOSE_AFTER_REPLY);
            // 处理master信息的缓存
            replicationCacheMaster(c);
            return;
        }
    }

    /* Log link disconnection with slave */
    // 如果释放的client是slave，这里记录日志
    if (getClientType(c) == CLIENT_TYPE_SLAVE) {
        serverLog(LL_WARNING,"Connection with replica %s lost.",
            replicationGetSlaveName(c));
    }

    /* Free the query buffer */
    // 释放client的查询缓存
    sdsfree(c->querybuf);
    sdsfree(c->pending_querybuf);
    c->querybuf = NULL;

    /* Deallocate structures used to block on blocking ops. */
    // 如果client处于阻塞状态，这里解除阻塞，即移除阻塞相关结构，并加入待处理结构中等待后面unlinkClient()从队列中完全移除。
    if (c->flags & CLIENT_BLOCKED) unblockClient(c);
    // 释放client中引起阻塞的keys。
    dictRelease(c->bpop.keys);

    /* UNWATCH all the keys */
    // client中有watch的keys全部移除，并释放watched_keys结构
    unwatchAllKeys(c);
    listRelease(c->watched_keys);

    /* Unsubscribe from all the pubsub channels */
    // 取消所有的pubsub channels订阅，并移除client中相关结构
    pubsubUnsubscribeAllChannels(c,0);
    pubsubUnsubscribeAllPatterns(c,0);
    dictRelease(c->pubsub_channels);
    listRelease(c->pubsub_patterns);

    /* Free data structures. */
    // 释放client的reply列表，释放client请求命令参数。
    listRelease(c->reply);
    freeClientArgv(c);
    freeClientOriginalArgv(c);

    /* Unlink the client: this will close the socket, remove the I/O
     * handlers, and remove references of the client from different
     * places where active clients may be referenced. */
    // unlinkClient，关闭socket，移除io事件监听，移除所有对该client的引用
    unlinkClient(c);

    /* Master/slave cleanup Case 1:
     * we lost the connection with a slave. */
    // Master/slave相关清理，情形1：我们与slave连接断开了。
    if (c->flags & CLIENT_SLAVE) {
        /* If there is no any other slave waiting dumping RDB finished, the
         * current child process need not continue to dump RDB, then we kill it.
         * So child process won't use more memory, and we also can fork a new
         * child process asap to dump rdb for next full synchronization or bgsave.
         * But we also need to check if users enable 'save' RDB, if enable, we
         * should not remove directly since that means RDB is important for users
         * to keep data safe and we may delay configured 'save' for full sync. */
        // 如果当前没有其他的slave在等待RDB完成导出，则当前在导出RDB的子进程就不需要继续处理了，我们kill掉。
        // 这样子进程就不会再使用更多内存，当下一次需要全量同步或bgsave时，我们可以再fork一个新的子进程尽快处理RDB导出。
        // 但是我们仍然需要检查用户是否被允许保存RDB，如果允许，就意味着RDB对用户保持数据安全很重要，我们不应该直接删除它，并且我们会延迟去处理'save'。
        if (server.saveparamslen == 0 &&
            c->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
            server.child_type == CHILD_TYPE_RDB &&
            server.rdb_child_type == RDB_CHILD_TYPE_DISK &&
            anyOtherSlaveWaitRdb(c) == 0)
        {
            // kill掉RDB dump的子进程
            killRDBChild();
        }
        if (c->replstate == SLAVE_STATE_SEND_BULK) {
            // 关闭复制文件描述符，释放传输的前文信息
            if (c->repldbfd != -1) close(c->repldbfd);
            if (c->replpreamble) sdsfree(c->replpreamble);
        }
        // 从master的slaves/monitors列表中找到该slave节点删除。
        list *l = (c->flags & CLIENT_MONITOR) ? server.monitors : server.slaves;
        ln = listSearchKey(l,c);
        serverAssert(ln != NULL);
        listDelNode(l,ln);
        /* We need to remember the time when we started to have zero
         * attached slaves, as after some time we'll free the replication
         * backlog. */
        // 当我们的slaves连接全断掉的时候，我们需要记录这个时间点。后面好释放复制backlog
        if (getClientType(c) == CLIENT_TYPE_SLAVE && listLength(server.slaves) == 0)
            server.repl_no_slaves_since = server.unixtime;
        // 更新good slaves数量
        refreshGoodSlavesCount();
        /* Fire the replica change modules event. */
        // 节点的slave有变化，需要通知module相关事件处理
        if (c->replstate == SLAVE_STATE_ONLINE)
            moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                                  REDISMODULE_SUBEVENT_REPLICA_CHANGE_OFFLINE,
                                  NULL);
    }

    /* Master/slave cleanup Case 2:
     * we lost the connection with the master. */
    // Master/slave清理，情形2：我们与master断开了，要处理master断开的逻辑。
    if (c->flags & CLIENT_MASTER) replicationHandleMasterDisconnection();

   /* Remove the contribution that this client gave to our
     * incrementally computed memory usage. */
   // 删除这个client在对应类型的client使用内存统计中的数据。
    server.stat_clients_type_memory[c->client_cron_last_memory_type] -=
        c->client_cron_last_memory_usage;

    /* Release other dynamically allocated client structure fields,
     * and finally release the client structure itself. */
    // 释放动态其他分配的的client结构，并最后释放client本身。
    if (c->name) decrRefCount(c->name);
    zfree(c->argv);
    c->argv_len_sum = 0;
    freeClientMultiState(c);
    sdsfree(c->peerid);
    sdsfree(c->sockname);
    sdsfree(c->slave_addr);
    zfree(c);
}

/* Schedule a client to free it at a safe time in the serverCron() function.
 * This function is useful when we need to terminate a client but we are in
 * a context where calling freeClient() is not possible, because the client
 * should be valid for the continuation of the flow of the program. */
// 调度一个client异步安全释放，真正处理释放在beforesleep()中处理。
// 这个函数通常用于我们想要终止一个client，但我们所在的上下文不能直接调用freeClient()处理时。
// 因为程序上下文client需要有效，从而支持程序接着执行下去。
// 所以我们只是将client加入到clients_to_close队列，等安全的时候处理释放。
void freeClientAsync(client *c) {
    /* We need to handle concurrent access to the server.clients_to_close list
     * only in the freeClientAsync() function, since it's the only function that
     * may access the list while Redis uses I/O threads. All the other accesses
     * are in the context of the main thread while the other threads are
     * idle. */
    // 我们只需要在freeClientAsync()中处理server.clients_to_close列表的并发访问。
    // 因为它是redis使用io线程时，能访问clients_to_close这个列表的唯一函数。
    // 所有其他的访问都在主线程上下文环境，这时候io或其他线程都是空闲状态不会有影响。

    // 如果client以及是待close了，或者client是执行LUA脚本的假客户端。直接返回
    if (c->flags & CLIENT_CLOSE_ASAP || c->flags & CLIENT_LUA) return;
    // 将当前client标记为待close，并加入clients_to_close列表
    c->flags |= CLIENT_CLOSE_ASAP;
    if (server.io_threads_num == 1) {
        /* no need to bother with locking if there's just one thread (the main thread) */
        // 如果当前只使用了一个线程，即至于主线程，直接可以操作，不用加锁处理
        listAddNodeTail(server.clients_to_close,c);
        return;
    }
    static pthread_mutex_t async_free_queue_mutex = PTHREAD_MUTEX_INITIALIZER;
    // 加锁互斥操作clients_to_close列表（加入client）
    pthread_mutex_lock(&async_free_queue_mutex);
    listAddNodeTail(server.clients_to_close,c);
    pthread_mutex_unlock(&async_free_queue_mutex);
}

/* Free the clients marked as CLOSE_ASAP, return the number of clients
 * freed. */
// 处理clients_to_close列表里的client释放
int freeClientsInAsyncFreeQueue(void) {
    int freed = 0;
    listIter li;
    listNode *ln;

    // 遍历clients_to_close列表处理
    listRewind(server.clients_to_close,&li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);

        // 如果client处于保护状态，跳过
        if (c->flags & CLIENT_PROTECTED) continue;

        // 释放client，删除节点
        c->flags &= ~CLIENT_CLOSE_ASAP;
        freeClient(c);
        listDelNode(server.clients_to_close,ln);
        freed++;
    }
    return freed;
}

/* Return a client by ID, or NULL if the client ID is not in the set
 * of registered clients. Note that "fake clients", created with -1 as FD,
 * are not registered clients. */
// 通过ID找对应的client。从clients_index中获取，如果不存在返回NULL
// 注意：构造的"fake clients"，创建时FD为-1，不会注册到这个活跃clients列表。
client *lookupClientByID(uint64_t id) {
    id = htonu64(id);
    client *c = raxFind(server.clients_index,(unsigned char*)&id,sizeof(id));
    return (c == raxNotFound) ? NULL : c;
}

/* Write data in output buffers to client. Return C_OK if the client
 * is still valid after the call, C_ERR if it was freed because of some
 * error.  If handler_installed is set, it will attempt to clear the
 * write event.
 *
 * This function is called by threads, but always with handler_installed
 * set to 0. So when handler_installed is set to 0 the function must be
 * thread safe. */
// 将数据写入output buffers给client。当处理完client仍然有效则返回ok，因其他原因释放client则繁华err。
// 参数如果设置了handler_installed，将尝试清除掉该client的可写监听事件。
// io线程调用时handler_installed总是设置为0，为0时该函数必须是线程安全的。
int writeToClient(client *c, int handler_installed) {
    /* Update total number of writes on server */
    // 更新服务总的处理写的计数
    atomicIncr(server.stat_total_writes_processed, 1);

    ssize_t nwritten = 0, totwritten = 0;
    size_t objlen;
    clientReplyBlock *o;

    // 当client有待回复的数据时，则写入Response buffer
    while(clientHasPendingReplies(c)) {
        if (c->bufpos > 0) {
            // 此时还有未写入conn的数据，先写完。也有可能一次write只写了部分数据，下次循环接着写。
            nwritten = connWrite(c->conn,c->buf+c->sentlen,c->bufpos-c->sentlen);
            // 报错就break
            if (nwritten <= 0) break;
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If the buffer was sent, set bufpos to zero to continue with
             * the remainder of the reply. */
            // 如果buffer数据发送完，则bufpos置空，继续处理剩下的reply
            if ((int)c->sentlen == c->bufpos) {
                c->bufpos = 0;
                c->sentlen = 0;
            }
        } else {
            // buf中没数据，则c->reply中肯定有数据，否则进不了循环。
            o = listNodeValue(listFirst(c->reply));
            objlen = o->used;

            // 如果当前处理的reply块没数据，移除该obj，更新剩余reply_bytes。
            if (objlen == 0) {
                c->reply_bytes -= o->size;
                listDelNode(c->reply,listFirst(c->reply));
                continue;
            }

            // 写数据到conn，当前buf已经处理了c->sentlen字节，即buf中已经写了o->buf ～ o->buf + c->sentlen区间的数据。
            // 这次写从o->buf + c->sentlen处开始，写总共要写objlen - c->sentlen字节。
            nwritten = connWrite(c->conn, o->buf + c->sentlen, objlen - c->sentlen);
            if (nwritten <= 0) break;
            // 更新当前buf已经处理的字节数，和总共写的字节数（可能有多个reply obj，这些总和）。
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If we fully sent the object on head go to the next one */
            // 如果当前obj完全发送完了，移除该obj节点，置sentlen为0，更新剩余reply_bytes。
            if (c->sentlen == objlen) {
                c->reply_bytes -= o->size;
                listDelNode(c->reply,listFirst(c->reply));
                c->sentlen = 0;
                /* If there are no longer objects in the list, we expect
                 * the count of reply bytes to be exactly zero. */
                // 如果reply列表没有obj了，这里断言下c->reply_bytes应该为0。
                if (listLength(c->reply) == 0)
                    serverAssert(c->reply_bytes == 0);
            }
        }
        /* Note that we avoid to send more than NET_MAX_WRITES_PER_EVENT
         * bytes, in a single threaded server it's a good idea to serve
         * other clients as well, even if a very large request comes from
         * super fast link that is always able to accept data (in real world
         * scenario think about 'KEYS *' against the loopback interface).
         *
         * However if we are over the maxmemory limit we ignore that and
         * just deliver as much data as it is possible to deliver.
         *
         * Moreover, we also send as much as possible if the client is
         * a slave or a monitor (otherwise, on high-speed traffic, the
         * replication/output buffer will grow indefinitely) */
        // 注意到，我们避免send大于NET_MAX_WRITES_PER_EVENT字节数据。
        // 在单线程服务中这样能控制数据发送时间，从而更好的服务其他的client，避免阻塞太久。
        // 这个send数据限制，只有在使用内存没达到上限，且client不是slave时才起作用。
        // 也就是说当使用内存达到上限，或者client是slave时，将允许一次性发送更多的数据，可以超限。
        if (totwritten > NET_MAX_WRITES_PER_EVENT &&
            (server.maxmemory == 0 ||
             zmalloc_used_memory() < server.maxmemory) &&
            !(c->flags & CLIENT_SLAVE)) break;
    }
    // 原子增加服务output的字节数。
    atomicIncr(server.stat_net_output_bytes, totwritten);
    if (nwritten == -1) {
        // nwritten为-1表示写出现err，这里检查conn状态还是不是connected，不是则异步释放到client。
        if (connGetState(c->conn) != CONN_STATE_CONNECTED) {
            serverLog(LL_VERBOSE,
                "Error writing to client: %s", connGetLastError(c->conn));
            freeClientAsync(c);
            return C_ERR;
        }
    }
    if (totwritten > 0) {
        /* For clients representing masters we don't count sending data
         * as an interaction, since we always send REPLCONF ACK commands
         * that take some time to just fill the socket output buffer.
         * We just rely on data / pings received for timeout detection. */
        // 当有成功写入数据，如果client不是master，更新client的最近交互时间，用于timeout判断。
        // 对于masters作为client，不需要将发送数据作为记录交互的时间点。
        // 因为我们总是会发送REPLCONF ACK命令，从而master会写数据到output buffer。
        // 我们仅仅依赖收到的data/pings消息来作为超时探测。
        if (!(c->flags & CLIENT_MASTER)) c->lastinteraction = server.unixtime;
    }
    // 如果待回复的数据都处理完了，处理一些操作。
    // 如根据handler_installed标识，移除socket的write handler。根据CLIENT_CLOSE_AFTER_REPLY，free client。
    if (!clientHasPendingReplies(c)) {
        // client的发送len置为0
        c->sentlen = 0;
        /* Note that writeToClient() is called in a threaded way, but
         * adDeleteFileEvent() is not thread safe: however writeToClient()
         * is always called with handler_installed set to 0 from threads
         * so we are fine. */
        // writeToClient是多线程调用，adDeleteFileEvent不是线程安全的。
        // io线程中调用 writeToClient 时 handler_installed 总是为0，所以这里主线程为1执行是没问题的。
        if (handler_installed) connSetWriteHandler(c->conn, NULL);

        /* Close connection after entire reply has been sent. */
        // CLIENT_CLOSE_AFTER_REPLY 标识 回复后释放client
        if (c->flags & CLIENT_CLOSE_AFTER_REPLY) {
            freeClientAsync(c);
            return C_ERR;
        }
    }
    return C_OK;
}

/* Write event handler. Just send data to the client. */
// 处理client回复写事件的handler，调用writeToClient来发送数据。
void sendReplyToClient(connection *conn) {
    client *c = connGetPrivateData(conn);
    writeToClient(c,1);
}

/* This function is called just before entering the event loop, in the hope
 * we can just write the replies to the client output buffer without any
 * need to use a syscall in order to install the writable event handler,
 * get it called, and so forth. */
// 该函数在进入event loop前（beforeSleep）执行。
// 尽可能先写到socket缓存，写不下再创建fd的写事件，放入事件监听中，等待触发。
int handleClientsWithPendingWrites(void) {
    listIter li;
    listNode *ln;
    // 获取需要处理的pending write clients，用于最后返回。
    int processed = listLength(server.clients_pending_write);

    // 遍历pending write列表中的client进行处理
    listRewind(server.clients_pending_write,&li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        // 移除client的pending write标识，并从pending write队列中删除该client。
        c->flags &= ~CLIENT_PENDING_WRITE;
        listDelNode(server.clients_pending_write,ln);

        /* If a client is protected, don't do anything,
         * that may trigger write error or recreate handler. */
        // 因为protected状态的client在调用protectClient()函数被保护时，是移除了读写handler及事件监听的。
        // 这里如果处理的话，可能触发写error导致conn断开 或者 重新加入handler监听。
        // 所以这里对protected状态client不做任何处理，跳过。
        // 该client只需要等待被调用unprotectClient()即可，移除保护时会加入读事件监听，如果有待回复数据还会再加入pending write队列。
        if (c->flags & CLIENT_PROTECTED) continue;

        /* Don't write to clients that are going to be closed anyway. */
        // 如果client正等待被close，这里也跳过不处理。
        if (c->flags & CLIENT_CLOSE_ASAP) continue;

        /* Try to write buffers to the client socket. */
        // 尝试将client output buf数据写入到socket，发送出去。如果写失败了，跳过。
        if (writeToClient(c,0) == C_ERR) continue;

        /* If after the synchronous writes above we still have data to
         * output to the client, we need to install the writable handler. */
        // 如果前面同步写了一部分数据成功后，仍然有数据要回复。即一次性socket写不完回复，需要等待处理后再写。
        // 所以这里重新安装writable handler，加入写事件监听。
        if (clientHasPendingReplies(c)) {
            int ae_barrier = 0;
            /* For the fsync=always policy, we want that a given FD is never
             * served for reading and writing in the same event loop iteration,
             * so that in the middle of receiving the query, and serving it
             * to the client, we'll call beforeSleep() that will do the
             * actual fsync of AOF to disk. the write barrier ensures that. */
            // 对于配置fsync=always策略，每次命令执行后都要进行刷盘处理。
            // 这种情况下，我们希望对于一个给定的FD，在同一个事件循环中不同时处理读和写操作。
            // 因此在接收到query，并处理回复client的过程中，我们调用beforeSleep()来实际处理AOF的刷盘。
            // 写屏障来保证这一点，处理事件时先处理写，后处理读。这样先刷盘再回复client，确保数据真正罗盘后才回复。
            if (server.aof_state == AOF_ON &&
                server.aof_fsync == AOF_FSYNC_ALWAYS)
            {
                ae_barrier = 1;
            }
            // 重新安装写事件处理handler加入事件监听，根据需要设置conn的barrier属性。
            // 如果设置失败，处理client异步释放。
            if (connSetWriteHandlerWithBarrier(c->conn, sendReplyToClient, ae_barrier) == C_ERR) {
                freeClientAsync(c);
            }
        }
    }
    return processed;
}

/* resetClient prepare the client to process the next command */
// 重置client，以全新状态来处理下一个命令。
void resetClient(client *c) {
    // 更新prevcmd为刚执行的命令处理函数。
    redisCommandProc *prevcmd = c->cmd ? c->cmd->proc : NULL;

    // 清理client处理请求的相关参数。
    freeClientArgv(c);
    c->reqtype = 0;
    c->multibulklen = 0;
    c->bulklen = -1;

    /* We clear the ASKING flag as well if we are not inside a MULTI, and
     * if what we just executed is not the ASKING command itself. */
    // 如果我们不处于MULTI状态，且执行的命令不是ASKING命令，则需要清除ASKING标识。
    // 因为ASKING标识是对下一个命令生效的，所以对于ASKING命令我们不能清理该标识。
    if (!(c->flags & CLIENT_MULTI) && prevcmd != askingCommand)
        c->flags &= ~CLIENT_ASKING;

    /* We do the same for the CACHING command as well. It also affects
     * the next command or transaction executed, in a way very similar
     * to ASKING. */
    // 对于CACHING命令处理与ASKING命令一样，因为它同样是影响下一个命令或事务执行，所以不能移除。
    if (!(c->flags & CLIENT_MULTI) && prevcmd != clientCommand)
        c->flags &= ~CLIENT_TRACKING_CACHING;

    /* Remove the CLIENT_REPLY_SKIP flag if any so that the reply
     * to the next command will be sent, but set the flag if the command
     * we just processed was "CLIENT REPLY SKIP". */
    // 清除CLIENT_REPLY_SKIP标志（如果有），以便我们发送下一个命令的回复（不跳过）。
    // 但如果我们刚刚处理的命令就是“CLIENT REPLY_SKIP”，表示下一条命令不回复，则需要设置该标志。
    c->flags &= ~CLIENT_REPLY_SKIP;
    if (c->flags & CLIENT_REPLY_SKIP_NEXT) {
        c->flags |= CLIENT_REPLY_SKIP;
        c->flags &= ~CLIENT_REPLY_SKIP_NEXT;
    }
}

/* This function is used when we want to re-enter the event loop but there
 * is the risk that the client we are dealing with will be freed in some
 * way. This happens for instance in:
 *
 * * DEBUG RELOAD and similar.
 * * When a Lua script is in -BUSY state.
 *
 * So the function will protect the client by doing two things:
 *
 * 1) It removes the file events. This way it is not possible that an
 *    error is signaled on the socket, freeing the client.
 * 2) Moreover it makes sure that if the client is freed in a different code
 *    path, it is not really released, but only marked for later release. */
// 当我们想重新进入event loop，但又不想当前处理的client被释放（以便后面回来继续执行）时，可以执行这个函数。
// 这个函数会执行两个操作：
// 1、设置client为CLIENT_PROTECTED保护状态，不允许被释放。
// 2、移除conn的读写handler，这样socket上就不可能有error出现导致断开连接。
void protectClient(client *c) {
    c->flags |= CLIENT_PROTECTED;
    if (c->conn) {
        connSetReadHandler(c->conn,NULL);
        connSetWriteHandler(c->conn,NULL);
    }
}

/* This will undo the client protection done by protectClient() */
// 移除client的保护。
// 1、去除CLIENT_PROTECTED标识。
// 2、设置client conn的读handler，并重新创建读事件加入事件监听列表。
// 3、如果client有需要处理的回复，加入pending_write列表，等待beforesleep中io线程处理write。一次处理不完才加入事件监听。
void unprotectClient(client *c) {
    if (c->flags & CLIENT_PROTECTED) {
        c->flags &= ~CLIENT_PROTECTED;
        if (c->conn) {
            connSetReadHandler(c->conn,readQueryFromClient);
            if (clientHasPendingReplies(c)) clientInstallWriteHandler(c);
        }
    }
}

/* Like processMultibulkBuffer(), but for the inline protocol instead of RESP,
 * this function consumes the client query buffer and creates a command ready
 * to be executed inside the client structure. Returns C_OK if the command
 * is ready to be executed, or C_ERR if there is still protocol to read to
 * have a well formed command. The function also returns C_ERR when there is
 * a protocol error: in such a case the client structure is setup to reply
 * with the error and close the connection. */
// 读取query buffer中的数据，inline协议解析指令数据放到client中。
// 如果命令解析完成，可以被执行，则返回ok；如果命令参数不全，还需要部分数据，则返回err。
// 另外解析时，如果出现协议错误，返回err，且client的flag中会标记
// CLIENT_CLOSE_AFTER_REPLY 和 CLIENT_PROTOCOL_ERROR。发送完err回复后异步关闭conn。
int processInlineBuffer(client *c) {
    char *newline;
    int argc, j, linefeed_chars = 1;
    sds *argv, aux;
    size_t querylen;

    /* Search for end of line */
    // 找出换行符
    newline = strchr(c->querybuf+c->qb_pos,'\n');

    /* Nothing to do without a \r\n */
    // 没有换行符，什么都不做，返回err
    if (newline == NULL) {
        // 如果处理的inline数据超出了上限，回复err，后续close conn
        if (sdslen(c->querybuf)-c->qb_pos > PROTO_INLINE_MAX_SIZE) {
            addReplyError(c,"Protocol error: too big inline request");
            setProtocolError("too big inline request",c);
        }
        return C_ERR;
    }

    /* Handle the \r\n case. */
    // 换行有\n和\r\n，这里判断前一个字符是否是\r，来获取字符串边界。
    if (newline != c->querybuf+c->qb_pos && *(newline-1) == '\r')
        newline--, linefeed_chars++;

    /* Split the input buffer up to the \r\n */
    // 计算字符串长度，基于获取的buffer字符串内容，复制构建新的sds字符串用于解析参数。
    querylen = newline-(c->querybuf+c->qb_pos);
    aux = sdsnewlen(c->querybuf+c->qb_pos,querylen);
    // 解析参数，返回argv参数列表 和 argc参数数量
    argv = sdssplitargs(aux,&argc);
    // 释放复制构造的sds
    sdsfree(aux);
    if (argv == NULL) {
        // 解析参数处理报错，argv为NULL。这里返回err，标记协议错误，reply后close conn
        addReplyError(c,"Protocol error: unbalanced quotes in request");
        setProtocolError("unbalanced quotes in inline request",c);
        return C_ERR;
    }

    /* Newline from slaves can be used to refresh the last ACK time.
     * This is useful for a slave to ping back while loading a big
     * RDB file. */
    // 如果query是一个空字符串，且client是slave，则更新client中的repl ack时间。
    // 这用于slave在loading大的RDB文件时，回ping master使用。
    if (querylen == 0 && getClientType(c) == CLIENT_TYPE_SLAVE)
        c->repl_ack_time = server.unixtime;

    /* Masters should never send us inline protocol to run actual
     * commands. If this happens, it is likely due to a bug in Redis where
     * we got some desynchronization in the protocol, for example
     * beause of a PSYNC gone bad.
     *
     * However the is an exception: masters may send us just a newline
     * to keep the connection active. */
    // master 绝不会使用inline协议来执行实际命令。
    // 如果出现了，说明redis中出现了bug，可能是部分代码逻辑协议不同步。如PSYNC协议处理问题等。
    // 不过又一个例外，master可能发送一个空字符串来保持conn活跃。
    if (querylen != 0 && c->flags & CLIENT_MASTER) {
        // 传输的不是空字符串，且client是master。打印日志，回复协议错误err，并异步关闭client
        sdsfreesplitres(argv,argc);
        serverLog(LL_WARNING,"WARNING: Receiving inline protocol from master, master stream corruption? Closing the master connection and discarding the cached master.");
        setProtocolError("Master using the inline protocol. Desync?",c);
        return C_ERR;
    }

    /* Move querybuffer position to the next query in the buffer. */
    // 将qb_pos移到buffer中下一个请求处。querylen为请求字符串长度，linefeed_chars为结束符长度。
    c->qb_pos += querylen+linefeed_chars;

    /* Setup argv array on client structure */
    // 重置client的argv
    if (argc) {
        if (c->argv) zfree(c->argv);
        c->argv = zmalloc(sizeof(robj*)*argc);
        c->argv_len_sum = 0;
    }

    /* Create redis objects for all arguments. */
    // 用解析的参数填充client的argv
    for (c->argc = 0, j = 0; j < argc; j++) {
        // createObject构建每个参数对象
        c->argv[c->argc] = createObject(OBJ_STRING,argv[j]);
        c->argc++;
        c->argv_len_sum += sdslen(argv[j]);
    }
    zfree(argv);
    return C_OK;
}

/* Helper function. Record protocol erro details in server log,
 * and set the client as CLIENT_CLOSE_AFTER_REPLY and
 * CLIENT_PROTOCOL_ERROR. */
#define PROTO_DUMP_LEN 128
static void setProtocolError(const char *errstr, client *c) {
    if (server.verbosity <= LL_VERBOSE || c->flags & CLIENT_MASTER) {
        sds client = catClientInfoString(sdsempty(),c);

        /* Sample some protocol to given an idea about what was inside. */
        char buf[256];
        if (sdslen(c->querybuf)-c->qb_pos < PROTO_DUMP_LEN) {
            snprintf(buf,sizeof(buf),"Query buffer during protocol error: '%s'", c->querybuf+c->qb_pos);
        } else {
            snprintf(buf,sizeof(buf),"Query buffer during protocol error: '%.*s' (... more %zu bytes ...) '%.*s'", PROTO_DUMP_LEN/2, c->querybuf+c->qb_pos, sdslen(c->querybuf)-c->qb_pos-PROTO_DUMP_LEN, PROTO_DUMP_LEN/2, c->querybuf+sdslen(c->querybuf)-PROTO_DUMP_LEN/2);
        }

        /* Remove non printable chars. */
        char *p = buf;
        while (*p != '\0') {
            if (!isprint(*p)) *p = '.';
            p++;
        }

        /* Log all the client and protocol info. */
        int loglevel = (c->flags & CLIENT_MASTER) ? LL_WARNING :
                                                    LL_VERBOSE;
        serverLog(loglevel,
            "Protocol error (%s) from client: %s. %s", errstr, client, buf);
        sdsfree(client);
    }
    c->flags |= (CLIENT_CLOSE_AFTER_REPLY|CLIENT_PROTOCOL_ERROR);
}

/* Process the query buffer for client 'c', setting up the client argument
 * vector for command execution. Returns C_OK if after running the function
 * the client has a well-formed ready to be processed command, otherwise
 * C_ERR if there is still to read more buffer to get the full command.
 * The function also returns C_ERR when there is a protocol error: in such a
 * case the client structure is setup to reply with the error and close
 * the connection.
 *
 * This function is called if processInputBuffer() detects that the next
 * command is in RESP format, so the first byte in the command is found
 * to be '*'. Otherwise for inline commands processInlineBuffer() is called. */
// 解析RESP协议的数据，解析出指令参数放到argv中。
// 如果解析完成获得了完整可执行的命令，则返回ok，如果命令不完整，还需要再读conn数据解析，则返回err。
// 另外如果协议解析时发现错误协议，也返回err，同时回复协议错误并close conn。
// 在processInputBuffer中，如果认为下一个命令是RESP协议格式（"*"开头），就会调用这个方法处理。
// 如果认为是inline协议格式，会调用processInlineBuffer处理。
int processMultibulkBuffer(client *c) {
    char *newline = NULL;
    int ok;
    long long ll;

    // multibulklen为0，说明刚开始RESP协议解析，这里要先解析处理，获取到当前命令所有的参数数量。
    if (c->multibulklen == 0) {
        // 如果待处理的multi bulk长度为0，client可能被reset了，这里验证下。
        /* The client should have been reset */
        serverAssertWithInfo(c,NULL,c->argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        // 每部分数据都是"\r\n"分割的，所以读取每部分都是找字符'\r'这样处理。
        newline = strchr(c->querybuf+c->qb_pos,'\r');
        if (newline == NULL) {
            // 如果一行数据太多，超过了PROTO_INLINE_MAX_SIZE，不符合协议，回复err，关闭conn。
            // 每超过限制，但是不足以取一个参数数据，直接返回，并跳出processInputBuffer，等待读取更多的数据到buffer中后再解析。
            // 有可能buffer过小，在readQueryFromClient中会对buffer扩容处理，使其能装下一个参数。
            if (sdslen(c->querybuf)-c->qb_pos > PROTO_INLINE_MAX_SIZE) {
                addReplyError(c,"Protocol error: too big mbulk count string");
                setProtocolError("too big mbulk count string",c);
            }
            return C_ERR;
        }

        /* Buffer should also contain \n */
        // 字符串长度 = newline-(c->querybuf+c->qb_pos)
        // sdslen(c->querybuf)-c->qb_pos-2 表示buffer中剩余待处理数据长度，即移除了"\r\n"和已处理数据。
        // 显然前者是小于等于后者的。相等的情况是buffer剩余数据刚好为读的字符串+"\r\n"。
        if (newline-(c->querybuf+c->qb_pos) > (ssize_t)(sdslen(c->querybuf)-c->qb_pos-2))
            return C_ERR;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        // 协议第一个字符是"*"
        serverAssertWithInfo(c,NULL,c->querybuf[c->qb_pos] == '*');
        // 解析协议命令指定的参数个数。即"*"后紧接着的数字。
        ok = string2ll(c->querybuf+1+c->qb_pos,newline-(c->querybuf+1+c->qb_pos),&ll);
        // 如果数字解析失败，或数字超过最大限制，说明协议解析出错，回复err并close连接。
        if (!ok || ll > 1024*1024) {
            addReplyError(c,"Protocol error: invalid multibulk length");
            setProtocolError("invalid mbulk count",c);
            return C_ERR;
        }

        // 更新qb_pos指针指向下一个处理参数位置
        c->qb_pos = (newline-c->querybuf)+2;

        // ll<=0，没有参数，说明没有命令执行，返回。
        if (ll <= 0) return C_OK;

        // 更新一个RESP协议指令后面包含的参数数量。
        c->multibulklen = ll;

        /* Setup argv array on client structure */
        // 知道了后面参数数量，这里构建client argv结构。
        if (c->argv) zfree(c->argv);
        c->argv = zmalloc(sizeof(robj*)*c->multibulklen);
        c->argv_len_sum = 0;
    }

    // c->multibulklen <= 0 前面就返回了，不会走到这里。
    serverAssertWithInfo(c,NULL,c->multibulklen > 0);
    // 根据前面获取的参数数量，进行参数填充。c->multibulklen实际上表示剩余需要填充的参数数。
    while(c->multibulklen) {
        /* Read bulk length if unknown */
        // 如果当前解析的参数长度未知，显然是刚开始处理这个参数，需要先解析参数的长度。
        if (c->bulklen == -1) {
            // 同前面解析参数个数一样处理，先找'\r'
            newline = strchr(c->querybuf+c->qb_pos,'\r');
            if (newline == NULL) {
                // 整行没有\r，数据不够解析，返回。
                // 如果长度超限，回复协议错误err并关闭conn。
                if (sdslen(c->querybuf)-c->qb_pos > PROTO_INLINE_MAX_SIZE) {
                    addReplyError(c,
                        "Protocol error: too big bulk count string");
                    setProtocolError("too big bulk count string",c);
                    return C_ERR;
                }
                break;
            }

            /* Buffer should also contain \n */
            // 最后一个字符是\r，每找到\n，数据不够解析，返回。
            if (newline-(c->querybuf+c->qb_pos) > (ssize_t)(sdslen(c->querybuf)-c->qb_pos-2))
                break;

            // 解析参数长度，第一个字符必须是'$'，否则协议解析出错，回复err并关闭conn。
            if (c->querybuf[c->qb_pos] != '$') {
                addReplyErrorFormat(c,
                    "Protocol error: expected '$', got '%c'",
                    c->querybuf[c->qb_pos]);
                setProtocolError("expected $ but got something else",c);
                return C_ERR;
            }

            // 解析参数长度，字符串转数字。校验长度，不合法则返回err并关闭conn。
            ok = string2ll(c->querybuf+c->qb_pos+1,newline-(c->querybuf+c->qb_pos+1),&ll);
            if (!ok || ll < 0 ||
                (!(c->flags & CLIENT_MASTER) && ll > server.proto_max_bulk_len)) {
                addReplyError(c,"Protocol error: invalid bulk length");
                setProtocolError("invalid bulk length",c);
                return C_ERR;
            }

            // 更新qb_pos指向下一个处理参数位置
            c->qb_pos = newline-c->querybuf+2;
            if (ll >= PROTO_MBULK_BIG_ARG) {
                /* If we are going to read a large object from network
                 * try to make it likely that it will start at c->querybuf
                 * boundary so that we can optimize object creation
                 * avoiding a large copy of data.
                 *
                 * But only when the data we have not parsed is less than
                 * or equal to ll+2. If the data length is greater than
                 * ll+2, trimming querybuf is just a waste of time, because
                 * at this time the querybuf contains not only our bulk. */
                // 处理参数构建对象时都是从buffer中copy一份数据来构建sds的。
                // 如果待处理参数很大，达到上限PROTO_MBULK_BIG_ARG。这里进行优化处理，使得构建时不进行copy。
                // 优化的方式是，将querybuf进行trim处理，使得其内容刚好是参数内容。当然，只有在未处理的数据<=ll+2时，才能trim。
                // 因为当数据>ll+2时，querybuf中包含的不仅仅只有当前要处理参数，还有后面的参数数据。trim后也没法直接用，还浪费时间。
                if (sdslen(c->querybuf)-c->qb_pos <= (size_t)ll+2) {
                    // trim操作，去掉前面已处理的数据，qb_pos置为0
                    sdsrange(c->querybuf,c->qb_pos,-1);
                    c->qb_pos = 0;
                    /* Hint the sds library about the amount of bytes this string is
                     * going to contain. */
                    // 扩展足够的buf空间，使得能够装下这个参数的内容。
                    c->querybuf = sdsMakeRoomFor(c->querybuf,ll+2-sdslen(c->querybuf));
                }
            }
            // 之前没有设置bulklen才会走到这个if中。这里解析出来后，设置bulklen值。
            // 该值用于在一次buf获取不全参数时，可以多次去conn read后处理。通过bulklen确定读取的数据长度。
            c->bulklen = ll;
        }

        /* Read bulk argument */
        if (sdslen(c->querybuf)-c->qb_pos < (size_t)(c->bulklen+2)) {
            /* Not enough data (+2 == trailing \r\n) */
            // 没有足够的数据来构建当前处理的参数，返回。
            break;
        } else {
            /* Optimization: if the buffer contains JUST our bulk element
             * instead of creating a new object by *copying* the sds we
             * just use the current sds string. */
            // 如果当前参数长度达到一定上限，且buffer中仅包含当前参数的数据（前面处理过），
            // 则直接使用buffer对应的sds构建参数对象，不进行数据复制处理。
            if (c->qb_pos == 0 &&
                c->bulklen >= PROTO_MBULK_BIG_ARG &&
                sdslen(c->querybuf) == (size_t)(c->bulklen+2))
            {
                // 直接使用querybuf对应的sds构建对象。后面需要对这个buf重新分配空间。
                c->argv[c->argc++] = createObject(OBJ_STRING,c->querybuf);
                c->argv_len_sum += c->bulklen;
                // 移除"\r\n"
                sdsIncrLen(c->querybuf,-2); /* remove CRLF */
                /* Assume that if we saw a fat argument we'll see another one
                 * likely... */
                // 如果我们看到一个很大的参数，这里假定后面的参数也很大。
                // 所以这里对querybuf重新分配空间时使用c->bulklen+2。
                c->querybuf = sdsnewlen(SDS_NOINIT,c->bulklen+2);
                sdsclear(c->querybuf);
            } else {
                // 将buf中的数据copy，来构建参数对象。
                c->argv[c->argc++] =
                    createStringObject(c->querybuf+c->qb_pos,c->bulklen);
                c->argv_len_sum += c->bulklen;
                // 更新qb_pos指向下一个待处理位置。
                c->qb_pos += c->bulklen+2;
            }
            // 成功构建了当前参数对象，这里需要将bulklen还原，下一次还是先解析参数长度，再解析参数本身。
            // 另外成功处理完一个参数，需要将multi bulk query总的等待解析的参数数量-1。
            c->bulklen = -1;
            c->multibulklen--;
        }
    }

    /* We're done when c->multibulk == 0 */
    // 如果c->multibulklen减为0了，说明参数处理完了，返回ok
    if (c->multibulklen == 0) return C_OK;

    /* Still not ready to process the command */
    // 说明解析到某个参数时，数据不足（找不到"\r\n"了），需要从conn里read足够的数据才能继续处理。
    return C_ERR;
}

/* Perform necessary tasks after a command was executed:
 *
 * 1. The client is reset unless there are reasons to avoid doing it.
 * 2. In the case of master clients, the replication offset is updated.
 * 3. Propagate commands we got from our master to replicas down the line. */
void commandProcessed(client *c) {
    long long prev_offset = c->reploff;
    if (c->flags & CLIENT_MASTER && !(c->flags & CLIENT_MULTI)) {
        /* Update the applied replication offset of our master. */
        c->reploff = c->read_reploff - sdslen(c->querybuf) + c->qb_pos;
    }

    /* Don't reset the client structure for blocked clients, so that the reply
     * callback will still be able to access the client argv and argc fields.
     * The client will be reset in unblockClient(). */
    if (!(c->flags & CLIENT_BLOCKED)) {
        resetClient(c);
    }

    /* If the client is a master we need to compute the difference
     * between the applied offset before and after processing the buffer,
     * to understand how much of the replication stream was actually
     * applied to the master state: this quantity, and its corresponding
     * part of the replication stream, will be propagated to the
     * sub-replicas and to the replication backlog. */
    if (c->flags & CLIENT_MASTER) {
        long long applied = c->reploff - prev_offset;
        if (applied) {
            replicationFeedSlavesFromMasterStream(server.slaves,
                    c->pending_querybuf, applied);
            sdsrange(c->pending_querybuf,applied,-1);
        }
    }
}

/* This function calls processCommand(), but also performs a few sub tasks
 * for the client that are useful in that context:
 *
 * 1. It sets the current client to the client 'c'.
 * 2. calls commandProcessed() if the command was handled.
 *
 * The function returns C_ERR in case the client was freed as a side effect
 * of processing the command, otherwise C_OK is returned. */
// 调用processCommand来执行命令，成功后调用commandProcessed处理后续工作。
// 如果执行后client被释放（调用副作用）则返回err，否则返回ok。
int processCommandAndResetClient(client *c) {
    int deadclient = 0;
    // 执行前先保存老的current_client。并将执行命令的clinet赋值给server.current_client。
    client *old_client = server.current_client;
    server.current_client = c;
    // 执行命令
    if (processCommand(c) == C_OK) {
        // 成功后处理一些操作。
        commandProcessed(c);
    }
    // 判断执行命令的client是否被释放。后面根据释放与否返回err或ok。
    // deadclient为1表示client释放。
    if (server.current_client == NULL) deadclient = 1;
    /*
     * Restore the old client, this is needed because when a script
     * times out, we will get into this code from processEventsWhileBlocked.
     * Which will cause to set the server.current_client. If not restored
     * we will return 1 to our caller which will falsely indicate the client
     * is dead and will stop reading from its buffer.
     */
    // 最后需要还原之前的current_client。即还原上下文中client。
    // 当前context的client先保存起来，执行命令时使用对应的client，执行完后还原。
    // 这是很有必要的，因为当一个脚本执行超时。我们可能从processEventsWhileBlocked中处理事件，执行命令进入到这里。
    // 如果不先保存current_client，处理完命令再还原的话，可能错误将执行脚本的context中current_client置为NULL。
    // 即完全使用当前执行命令的上下文client替换了外面执行脚本上下文中的client，这显然是有问题的。
    // 可能导致外层client被认为释放掉了，从而停止读取数据。
    server.current_client = old_client;
    /* performEvictions may flush slave output buffers. This may
     * result in a slave, that may be the active client, to be
     * freed. */
    return deadclient ? C_ERR : C_OK;
}


/* This function will execute any fully parsed commands pending on
 * the client. Returns C_ERR if the client is no longer valid after executing
 * the command, and C_OK for all other cases. */
// 如果该client有了完整可以执行的命令，则执行该命令。
// 如果执行完命令，client就无效了，返回C_ERR。正常情况都返回C_OK
int processPendingCommandsAndResetClient(client *c) {
    if (c->flags & CLIENT_PENDING_COMMAND) {
        c->flags &= ~CLIENT_PENDING_COMMAND;
        // 处理命令执行
        if (processCommandAndResetClient(c) == C_ERR) {
            return C_ERR;
        }
    }
    return C_OK;
}

/* This function is called every time, in the client structure 'c', there is
 * more query buffer to process, because we read more data from the socket
 * or because a client was blocked and later reactivated, so there could be
 * pending query buffer, already representing a full command, to process. */
// 每次我们从conn中读出数据后总会执行这个函数，或者因为其他原因导致query buffer中多了一些数据时要调用这个函数处理。
void processInputBuffer(client *c) {
    /* Keep processing while there is something in the input buffer */
    // 如果querybuf中还有数据未处理，循环处理。
    // qb_pos指向我们已经处理的字符位置，即已处理字符数
    while(c->qb_pos < sdslen(c->querybuf)) {
        /* Immediately abort if the client is in the middle of something. */
        // 如果client处于blocked状态，立即返回。
        if (c->flags & CLIENT_BLOCKED) break;

        /* Don't process more buffers from clients that have already pending
         * commands to execute in c->argv. */
        // 如果client已经有完整的待处理命令，暂时不再处理请求数据。多个请求按顺序执行。
        if (c->flags & CLIENT_PENDING_COMMAND) break;

        /* Don't process input from the master while there is a busy script
         * condition on the slave. We want just to accumulate the replication
         * stream (instead of replying -BUSY like we do with other clients) and
         * later resume the processing. */
        // 如果slave服务执行脚本超时，且client是master，暂时不处理复制数据。
        // 此时并不返回-BUSY给master，复制数据继续累加到buffer中等待后面处理。
        if (server.lua_timedout && c->flags & CLIENT_MASTER) break;

        /* CLIENT_CLOSE_AFTER_REPLY closes the connection once the reply is
         * written to the client. Make sure to not let the reply grow after
         * this flag has been set (i.e. don't process more commands).
         *
         * The same applies for clients we want to terminate ASAP. */
        // 如果有CLIENT_CLOSE_AFTER_REPLY 或 CLIENT_CLOSE_ASAP标识，不再处理更多的命令。
        // reply 回复后就立即close conn，不再处理新的query。
        if (c->flags & (CLIENT_CLOSE_AFTER_REPLY|CLIENT_CLOSE_ASAP)) break;

        /* Determine request type when unknown. */
        // 根据qb_pos处的数据决定请求类型
        if (!c->reqtype) {
            if (c->querybuf[c->qb_pos] == '*') {
                // 命令以*开头，是PROTO_REQ_MULTIBULK类型，RESP协议命令
                // *3\r\n$3\r\nset\r\n$4\r\nname\r\n$9\r\nchenqiang\r\n
                // *开头，3表示3个指令元素，后面每个元素都是 "$len \r\n string" 格式。
                c->reqtype = PROTO_REQ_MULTIBULK;
            } else {
                // inline 协议命令
                c->reqtype = PROTO_REQ_INLINE;
            }
        }

        if (c->reqtype == PROTO_REQ_INLINE) {
            // 处理inline协议命令解析
            if (processInlineBuffer(c) != C_OK) break;
            /* If the Gopher mode and we got zero or one argument, process
             * the request in Gopher mode. To avoid data race, Redis won't
             * support Gopher if enable io threads to read queries. */
            // Gopher模式开启时，如果query参数是0个或1个，按gopher模式处理请求。
            // 为了避免数据争用，服务器开启io线程读数据时，不支持gopher模式协议处理。
            if (server.gopher_enabled && !server.io_threads_do_reads &&
                ((c->argc == 1 && ((char*)(c->argv[0]->ptr))[0] == '/') ||
                  c->argc == 0))
            {
                // gopher模式处理请求。处理完后重置client，并将client标记为回复后close。
                processGopherRequest(c);
                resetClient(c);
                c->flags |= CLIENT_CLOSE_AFTER_REPLY;
                break;
            }
        } else if (c->reqtype == PROTO_REQ_MULTIBULK) {
            // 处理RESP协议命令解析
            if (processMultibulkBuffer(c) != C_OK) break;
        } else {
            serverPanic("Unknown request type");
        }

        /* Multibulk processing could see a <= 0 length. */
        if (c->argc == 0) {
            // 如果解析后没有命令参数，重置client等待处理下一个query。
            resetClient(c);
        } else {
            /* If we are in the context of an I/O thread, we can't really
             * execute the command here. All we can do is to flag the client
             * as one that needs to process the command. */
            // CLIENT_PENDING_READ，多io时，首次进入readQueryFromClient，会加入io队列等待query解析，并打上该标识。
            // 此处是io线程解析阶段，显然io线程不能处理命令，否则redis命令执行就不是单线程了，有并发问题。
            // 所以这里将该client标识设置为CLIENT_PENDING_COMMAND，io线程处理完成，返回。等待主线程执行命令。
            if (c->flags & CLIENT_PENDING_READ) {
                c->flags |= CLIENT_PENDING_COMMAND;
                break;
            }

            /* We are finally ready to execute the command. */
            // 到这里时，client不是CLIENT_PENDING_READ，就可以直接执行命令。
            if (processCommandAndResetClient(c) == C_ERR) {
                /* If the client is no longer valid, we avoid exiting this
                 * loop and trimming the client buffer later. So we return
                 * ASAP in that case. */
                // 如果client不再有效，直接尽快返回。不再跳出循环去做trim操作。
                return;
            }
        }
    }

    /* Trim to pos */
    // buffer 中 c->qb_pos之前的数据都处理完了，trim处理只剩下未处理的数据。
    if (c->qb_pos) {
        sdsrange(c->querybuf,c->qb_pos,-1);
        c->qb_pos = 0;
    }
}

// 从client conn中读取query放到buffer中，并解析命令
// 目前处理一个请求期间，会有三种情况进入这个方法：
// 1、conn上有新的请求，fd可读时，在主线程event循环中，执行fd的read handler（readQueryFromClient）。
// 此时，postponeClientRead判断，如果不加入io线程处理，则主线程直接处理完命令解析执行整个流程，不会有后面再进入这个方法的机会了。
// 如果postponeClientRead判断使用io线程，则加入io队列，该conn对应的client会被标记为CLIENT_PENDING_READ，等待io读的状态。
// 而当前主线程执行的read handler直接返回。接着事件循环处理下一个fd。
// 2、主线程当前事件循环处理完了，下一个循环的beforesleep中handleClientsWithPendingReadsUsingThreads会处理待读的client。
// 此时主线程将这些待读的client分配到各个io线程队列，启用io线程来并行处理读数据。
// io线程会再调用readQueryFromClient来真正的处理query buffer的解析。
// 因为此时是pending read状态，readQueryFromClient的processInputBuffer根据标识不会直接执行命令，
// 而是打上pending command标识，等待主线程接管时，单线程执行。避免数据并发。
// 3、io线程处理完后，移除client的pending read标识，主线程接管挨个处理client。
// 如果client标识是pending command，则直接调用processPendingCommandsAndResetClient执行命令。
// 不是待执行状态，则说明命令没准备好，则调用processInputBuffer继续解析命令执行。
// 执行完成后，如果有数据回复，则设置CLIENT_PENDING_WRITE状态，加入待写队列，等待io线程处理写。
void readQueryFromClient(connection *conn) {
    // 从conn中获取client
    client *c = connGetPrivateData(conn);
    int nread, readlen;
    size_t qblen;

    /* Check if we want to read from the client later when exiting from
     * the event loop. This is the case if threaded I/O is enabled. */
    // 如果可以用I/O线程延后处理，直接返回。否则需要在主线程直接处理。
    // 以下3种不会加入io队列：
    // 1、对于blocked期间处理，需要后面立即返回loading err。
    // 2、对于client是master或slave不使用io线程处理。
    // 3、对于已经有CLIENT_PENDING_READ标识了，说明是已经加入io队列，现在需要真正执行操作了。
    if (postponeClientRead(c)) return;

    /* Update total number of reads on server */
    // 更新服务总的read处理数
    atomicIncr(server.stat_total_reads_processed, 1);

    // 设置最大可read量，即buf的大小。
    readlen = PROTO_IOBUF_LEN;
    /* If this is a multi bulk request, and we are processing a bulk reply
     * that is large enough, try to maximize the probability that the query
     * buffer contains exactly the SDS string representing the object, even
     * at the risk of requiring more read(2) calls. This way the function
     * processMultiBulkBuffer() can avoid copying buffers to create the
     * Redis Object representing the argument. */
    // 如果是multi bulk请求，而我们正在处理的bulk请求数据太大，可能会尝试增加query buffer的容量，即使这样可能会导致更多次read调用。
    // 这样，在processMultiBulkBuffer中处理时，能直接由buffer中数据构建参数obj，
    // 而不用再从buffer中copy出来，等待完全获取到参数信息后构建对象。
    if (c->reqtype == PROTO_REQ_MULTIBULK && c->multibulklen && c->bulklen != -1
        && c->bulklen >= PROTO_MBULK_BIG_ARG)
    {
        // c->bulklen multi bulk请求中bulk请求参数长度。
        // 目前处于bulk请求处理中，需要多次从conn读取数据到buf。
        // buf过小，无法完全容纳请求数据，这里需要计算剩下的数据大小，从而进行buf扩容
        ssize_t remaining = (size_t)(c->bulklen+2)-sdslen(c->querybuf);

        /* Note that the 'remaining' variable may be zero in some edge case,
         * for example once we resume a blocked client after CLIENT PAUSE. */
        // 注意到这里remaining在某些边界条件时可能为0，如从CLIENT PAUSE状态恢复的client。
        if (remaining > 0 && remaining < readlen) readlen = remaining;
    }

    qblen = sdslen(c->querybuf);
    // 更新querybuf长度峰值
    if (c->querybuf_peak < qblen) c->querybuf_peak = qblen;
    // 扩容querybuf
    c->querybuf = sdsMakeRoomFor(c->querybuf, readlen);
    // 读取readlen长度数据
    nread = connRead(c->conn, c->querybuf+qblen, readlen);
    if (nread == -1) {
        // 报错了，检查conn状态，如果不是connected就异步释放client，否则直接返回等待下一次读取。
        if (connGetState(conn) == CONN_STATE_CONNECTED) {
            return;
        } else {
            serverLog(LL_VERBOSE, "Reading from client: %s",connGetLastError(c->conn));
            freeClientAsync(c);
            return;
        }
    } else if (nread == 0) {
        // 读到数据为0，表示conn已关闭，异步释放client
        serverLog(LL_VERBOSE, "Client closed connection");
        freeClientAsync(c);
        return;
    } else if (c->flags & CLIENT_MASTER) {
        /* Append the query buffer to the pending (not applied) buffer
         * of the master. We'll use this buffer later in order to have a
         * copy of the string applied by the last command executed. */
        // 读到了数据，此时如果client是master。将读到的数据追加到pending_querybuf。
        // pending_querybuf中的数据表示master同步过来的replication stream数据，等待slave处理。
        c->pending_querybuf = sdscatlen(c->pending_querybuf,
                                        c->querybuf+qblen,nread);
    }

    // 更新querybuf中数据长度
    sdsIncrLen(c->querybuf,nread);
    // 设置最近一次交互时间
    c->lastinteraction = server.unixtime;
    // 如果client是master，更新读取repl的offset
    if (c->flags & CLIENT_MASTER) c->read_reploff += nread;
    // 原子更新网络传入的字节数用于统计
    atomicIncr(server.stat_net_input_bytes, nread);
    // 如果client的querybuf中数据长度超出了client最大的长度限制，则打印错误并异步释放client
    if (sdslen(c->querybuf) > server.client_max_querybuf_len) {
        sds ci = catClientInfoString(sdsempty(),c), bytes = sdsempty();

        bytes = sdscatrepr(bytes,c->querybuf,64);
        serverLog(LL_WARNING,"Closing client that reached max query buffer length: %s (qbuf initial bytes: %s)", ci, bytes);
        sdsfree(ci);
        sdsfree(bytes);
        freeClientAsync(c);
        return;
    }

    /* There is more data in the client input buffer, continue parsing it
     * in case to check if there is a full command to execute. */
    // conn有新的数据写入buffer了，解析client的input buffer中的数据，获取一个完整的可执行命令来执行。
     processInputBuffer(c);
}

// 获取所有clients中使用的最大buffer，没使用？
void getClientsMaxBuffers(unsigned long *longest_output_list,
                          unsigned long *biggest_input_buffer) {
    client *c;
    listNode *ln;
    listIter li;
    // 这俩变量分别表示最大output和input buffer
    unsigned long lol = 0, bib = 0;

    // 遍历server.clients
    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);

        // 比较更新最大buffer值
        if (listLength(c->reply) > lol) lol = listLength(c->reply);
        if (sdslen(c->querybuf) > bib) bib = sdslen(c->querybuf);
    }
    // 赋值给返回变量
    *longest_output_list = lol;
    *biggest_input_buffer = bib;
}

/* A Redis "Address String" is a colon separated ip:port pair.
 * For IPv4 it's in the form x.y.z.k:port, example: "127.0.0.1:1234".
 * For IPv6 addresses we use [] around the IP part, like in "[::1]:1234".
 * For Unix sockets we use path:0, like in "/tmp/redis:0".
 *
 * An Address String always fits inside a buffer of NET_ADDR_STR_LEN bytes,
 * including the null term.
 *
 * On failure the function still populates 'addr' with the "?:0" string in case
 * you want to relax error checking or need to display something anyway (see
 * anetFdToString implementation for more info). */
// 返回client addr信息。根据fd_to_str_type决定查询本地还是对端。
// redis addr信息根据协议不同有不同格式。IPv4、Ipv6、Unix socket。
// 地址字符串长度总是小于NET_ADDR_STR_LEN的。
// 当查询报错时，仍然生成"?:0"格式字符串，常用于放松错误检查或某些地方展示使用。具体见anetFdToString实现。
void genClientAddrString(client *client, char *addr,
                         size_t addr_len, int fd_to_str_type) {
    if (client->flags & CLIENT_UNIX_SOCKET) {
        /* Unix socket client. */
        // Unix socket信息
        snprintf(addr,addr_len,"%s:0",server.unixsocket);
    } else {
        /* TCP client. */
        // TCP conn获取使用底层conn中方法
        connFormatFdAddr(client->conn,addr,addr_len,fd_to_str_type);
    }
}

/* This function returns the client peer id, by creating and caching it
 * if client->peerid is NULL, otherwise returning the cached value.
 * The Peer ID never changes during the life of the client, however it
 * is expensive to compute. */
// 获取client对端的id，该peer id整个client生存期间不变，但计算需要花费时间。
char *getClientPeerId(client *c) {
    char peerid[NET_ADDR_STR_LEN];

    if (c->peerid == NULL) {
        // 没有缓存，genClientAddrString调用底层conn接口获取，然后填入c->peerid
        // FD_TO_PEER_NAME参数标识获取peer对端信息
        genClientAddrString(c,peerid,sizeof(peerid),FD_TO_PEER_NAME);
        c->peerid = sdsnew(peerid);
    }
    return c->peerid;
}

/* This function returns the client bound socket name, by creating and caching
 * it if client->sockname is NULL, otherwise returning the cached value.
 * The Socket Name never changes during the life of the client, however it
 * is expensive to compute. */
// 获取client底层socket name。该name整个client生存期间不变，但计算需要花费时间。
// client缓存有值就直接返回，没有就调用底层conn接口计算。
char *getClientSockname(client *c) {
    char sockname[NET_ADDR_STR_LEN];

    if (c->sockname == NULL) {
        // 没有缓存，genClientAddrString会调用底层conn接口获取，然后填入c->sockname
        // FD_TO_SOCK_NAME标识获取本地socket信息
        genClientAddrString(c,sockname,sizeof(sockname),FD_TO_SOCK_NAME);
        c->sockname = sdsnew(sockname);
    }
    return c->sockname;
}

/* Concatenate a string representing the state of a client in a human
 * readable format, into the sds string 's'. */
// 以人看得懂的格式处理client的状态拼接字符串返回。
sds catClientInfoString(sds s, client *client) {
    char flags[16], events[3], conninfo[CONN_INFO_LEN], *p;

    p = flags;
    if (client->flags & CLIENT_SLAVE) {
        if (client->flags & CLIENT_MONITOR)
            *p++ = 'O';
        else
            *p++ = 'S';
    }
    if (client->flags & CLIENT_MASTER) *p++ = 'M';
    if (client->flags & CLIENT_PUBSUB) *p++ = 'P';
    if (client->flags & CLIENT_MULTI) *p++ = 'x';
    if (client->flags & CLIENT_BLOCKED) *p++ = 'b';
    if (client->flags & CLIENT_TRACKING) *p++ = 't';
    if (client->flags & CLIENT_TRACKING_BROKEN_REDIR) *p++ = 'R';
    if (client->flags & CLIENT_TRACKING_BCAST) *p++ = 'B';
    if (client->flags & CLIENT_DIRTY_CAS) *p++ = 'd';
    if (client->flags & CLIENT_CLOSE_AFTER_REPLY) *p++ = 'c';
    if (client->flags & CLIENT_UNBLOCKED) *p++ = 'u';
    if (client->flags & CLIENT_CLOSE_ASAP) *p++ = 'A';
    if (client->flags & CLIENT_UNIX_SOCKET) *p++ = 'U';
    if (client->flags & CLIENT_READONLY) *p++ = 'r';
    if (p == flags) *p++ = 'N';
    *p++ = '\0';

    p = events;
    if (client->conn) {
        if (connHasReadHandler(client->conn)) *p++ = 'r';
        if (connHasWriteHandler(client->conn)) *p++ = 'w';
    }
    *p = '\0';

    /* Compute the total memory consumed by this client. */
    size_t obufmem = getClientOutputBufferMemoryUsage(client);
    size_t total_mem = obufmem;
    total_mem += zmalloc_size(client); /* includes client->buf */
    total_mem += sdsZmallocSize(client->querybuf);
    /* For efficiency (less work keeping track of the argv memory), it doesn't include the used memory
     * i.e. unused sds space and internal fragmentation, just the string length. but this is enough to
     * spot problematic clients. */
    total_mem += client->argv_len_sum;
    if (client->argv)
        total_mem += zmalloc_size(client->argv);

    return sdscatfmt(s,
        "id=%U addr=%s laddr=%s %s name=%s age=%I idle=%I flags=%s db=%i sub=%i psub=%i multi=%i qbuf=%U qbuf-free=%U argv-mem=%U obl=%U oll=%U omem=%U tot-mem=%U events=%s cmd=%s user=%s redir=%I",
        (unsigned long long) client->id,
        getClientPeerId(client),
        getClientSockname(client),
        connGetInfo(client->conn, conninfo, sizeof(conninfo)),
        client->name ? (char*)client->name->ptr : "",
        (long long)(server.unixtime - client->ctime),
        (long long)(server.unixtime - client->lastinteraction),
        flags,
        client->db->id,
        (int) dictSize(client->pubsub_channels),
        (int) listLength(client->pubsub_patterns),
        (client->flags & CLIENT_MULTI) ? client->mstate.count : -1,
        (unsigned long long) sdslen(client->querybuf),
        (unsigned long long) sdsavail(client->querybuf),
        (unsigned long long) client->argv_len_sum,
        (unsigned long long) client->bufpos,
        (unsigned long long) listLength(client->reply),
        (unsigned long long) obufmem, /* should not include client->buf since we want to see 0 for static clients. */
        (unsigned long long) total_mem,
        events,
        client->lastcmd ? client->lastcmd->name : "NULL",
        client->user ? client->user->name : "(superuser)",
        (client->flags & CLIENT_TRACKING) ? (long long) client->client_tracking_redirection : -1);
}

// 获取某类client的信息。
sds getAllClientsInfoString(int type) {
    listNode *ln;
    listIter li;
    client *client;
    sds o = sdsnewlen(SDS_NOINIT,200*listLength(server.clients));
    sdsclear(o);
    // 遍历server.clients
    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        client = listNodeValue(ln);
        // 不是所要的type跳过
        if (type != -1 && getClientType(client) != type) continue;
        // fmtclient信息追加到o中
        o = catClientInfoString(o,client);
        o = sdscatlen(o,"\n",1);
    }
    return o;
}

/* This function implements CLIENT SETNAME, including replying to the
 * user with an error if the charset is wrong (in that case C_ERR is
 * returned). If the function succeeeded C_OK is returned, and it's up
 * to the caller to send a reply if needed.
 *
 * Setting an empty string as name has the effect of unsetting the
 * currently set name: the client will remain unnamed.
 *
 * This function is also used to implement the HELLO SETNAME option. */
// 用于CLIENT/HELLO SETNAME，设置name。
// 报错将返回err并发送回复给client，成功返回ok，调用者决定是否发送回复。
int clientSetNameOrReply(client *c, robj *name) {
    int len = sdslen(name->ptr);
    char *p = name->ptr;

    /* Setting the client name to an empty string actually removes
     * the current name. */
    // 设置孔字符串，将清除现有的name，client变为unnamed状态。
    if (len == 0) {
        if (c->name) decrRefCount(c->name);
        c->name = NULL;
        return C_OK;
    }

    /* Otherwise check if the charset is ok. We need to do this otherwise
     * CLIENT LIST format will break. You should always be able to
     * split by space to get the different fields. */
    for (int j = 0; j < len; j++) {
        // 空白符、换行符以及某些特殊字符不允许设置。
        // 空白换行总是需要用于分割字符串的，同时字符也要是可format打印的。
        if (p[j] < '!' || p[j] > '~') { /* ASCII is assumed. */
            addReplyError(c,
                "Client names cannot contain spaces, "
                "newlines or special characters.");
            return C_ERR;
        }
    }
    // 名字替换，obj引用更新
    if (c->name) decrRefCount(c->name);
    c->name = name;
    incrRefCount(name);
    return C_OK;
}

/* Reset the client state to resemble a newly connected client.
 */
// reset命令，重置client状态，还原到刚连接时一样。
void resetCommand(client *c) {
    listNode *ln;

    /* MONITOR clients are also marked with CLIENT_SLAVE, we need to
     * distinguish between the two.
     */
    // MONITOR client也被标识为CLIENT_SLAVE，这里需要区分一下。
    if (c->flags & CLIENT_MONITOR) {
        ln = listSearchKey(server.monitors,c);
        serverAssert(ln != NULL);
        listDelNode(server.monitors,ln);

        // CLIENT_MONITOR也是CLIENT_SLAVE，所以两个都要移除
        c->flags &= ~(CLIENT_MONITOR|CLIENT_SLAVE);
    }

    // 只能reset mormal类型的client，MONITOR也是normal类型，前面处理了。
    if (c->flags & (CLIENT_SLAVE|CLIENT_MASTER|CLIENT_MODULE)) {
        addReplyError(c,"can only reset normal client connections");
        return;
    }

    // 如果启用了CLIENT_TRACKING，来进行client侧的缓存，这里需要禁用掉。
    if (c->flags & CLIENT_TRACKING) disableTracking(c);
    // 恢复默认db
    selectDb(c,0);
    // 恢复默认RESP协议版本。
    c->resp = 2;

    // 恢复初始认证状态
    clientSetDefaultAuth(c);
    // 用户改变需要通知追踪一些信息的模块清理信息
    moduleNotifyUserChanged(c);
    // 清除事务相关状态
    discardTransaction(c);

    // 解除所有channel的订阅
    pubsubUnsubscribeAllChannels(c,0);
    // 解除所有模式的订阅
    pubsubUnsubscribeAllPatterns(c,0);

    // 如果设置了client的name，清除
    if (c->name) {
        decrRefCount(c->name);
        c->name = NULL;
    }

    /* Selectively clear state flags not covered above */
    // 清除其他状态，不包含前面已处理过的
    c->flags &= ~(CLIENT_ASKING|CLIENT_READONLY|CLIENT_PUBSUB|
            CLIENT_REPLY_OFF|CLIENT_REPLY_SKIP_NEXT);

    // 添加返回信息
    addReplyStatus(c,"RESET");
}

// client 命令处理，查询client信息
void clientCommand(client *c) {
    listNode *ln;
    listIter li;

    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"CACHING (YES|NO)",
"    Enable/disable tracking of the keys for next command in OPTIN/OPTOUT modes.",
"GETREDIR",
"    Return the client ID we are redirecting to when tracking is enabled.",
"GETNAME",
"    Return the name of the current connection.",
"ID",
"    Return the ID of the current connection.",
"INFO",
"    Return information about the current client connection.",
"KILL <ip:port>",
"    Kill connection made from <ip:port>.",
"KILL <option> <value> [<option> <value> [...]]",
"    Kill connections. Options are:",
"    * ADDR (<ip:port>|<unixsocket>:0)",
"      Kill connections made from the specified address",
"    * LADDR (<ip:port>|<unixsocket>:0)",
"      Kill connections made to specified local address",
"    * TYPE (normal|master|replica|pubsub)",
"      Kill connections by type.",
"    * USER <username>",
"      Kill connections authenticated by <username>.",
"    * SKIPME (YES|NO)",
"      Skip killing current connection (default: yes).",
"LIST [options ...]",
"    Return information about client connections. Options:",
"    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
"      Return clients of specified type.",
"UNPAUSE",
"    Stop the current client pause, resuming traffic.",
"PAUSE <timeout> [WRITE|ALL]",
"    Suspend all, or just write, clients for <timout> milliseconds.",
"REPLY (ON|OFF|SKIP)",
"    Control the replies sent to the current connection.",
"SETNAME <name>",
"    Assign the name <name> to the current connection.",
"UNBLOCK <clientid> [TIMEOUT|ERROR]",
"    Unblock the specified blocked client.",
"TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]]",
"         [OPTIN] [OPTOUT]",
"    Control server assisted client side caching.",
"TRACKINGINFO",
"    Report tracking status for the current connection.",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"id") && c->argc == 2) {
        /* CLIENT ID */
        addReplyLongLong(c,c->id);
    } else if (!strcasecmp(c->argv[1]->ptr,"info") && c->argc == 2) {
        /* CLIENT INFO */
        sds o = catClientInfoString(sdsempty(), c);
        o = sdscatlen(o,"\n",1);
        addReplyVerbatim(c,o,sdslen(o),"txt");
        sdsfree(o);
    } else if (!strcasecmp(c->argv[1]->ptr,"list")) {
        /* CLIENT LIST */
        int type = -1;
        sds o = NULL;
        if (c->argc == 4 && !strcasecmp(c->argv[2]->ptr,"type")) {
            type = getClientTypeByName(c->argv[3]->ptr);
            if (type == -1) {
                addReplyErrorFormat(c,"Unknown client type '%s'",
                    (char*) c->argv[3]->ptr);
                return;
            }
        } else if (c->argc > 3 && !strcasecmp(c->argv[2]->ptr,"id")) {
            int j;
            o = sdsempty();
            for (j = 3; j < c->argc; j++) {
                long long cid;
                if (getLongLongFromObjectOrReply(c, c->argv[j], &cid,
                            "Invalid client ID")) {
                    sdsfree(o);
                    return;
                }
                client *cl = lookupClientByID(cid);
                if (cl) {
                    o = catClientInfoString(o, cl);
                    o = sdscatlen(o, "\n", 1);
                }
            }
        } else if (c->argc != 2) {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }

        if (!o)
            o = getAllClientsInfoString(type);
        addReplyVerbatim(c,o,sdslen(o),"txt");
        sdsfree(o);
    } else if (!strcasecmp(c->argv[1]->ptr,"reply") && c->argc == 3) {
        /* CLIENT REPLY ON|OFF|SKIP */
        if (!strcasecmp(c->argv[2]->ptr,"on")) {
            c->flags &= ~(CLIENT_REPLY_SKIP|CLIENT_REPLY_OFF);
            addReply(c,shared.ok);
        } else if (!strcasecmp(c->argv[2]->ptr,"off")) {
            c->flags |= CLIENT_REPLY_OFF;
        } else if (!strcasecmp(c->argv[2]->ptr,"skip")) {
            if (!(c->flags & CLIENT_REPLY_OFF))
                c->flags |= CLIENT_REPLY_SKIP_NEXT;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"kill")) {
        /* CLIENT KILL <ip:port>
         * CLIENT KILL <option> [value] ... <option> [value] */
        char *addr = NULL;
        char *laddr = NULL;
        user *user = NULL;
        int type = -1;
        uint64_t id = 0;
        int skipme = 1;
        int killed = 0, close_this_client = 0;

        if (c->argc == 3) {
            /* Old style syntax: CLIENT KILL <addr> */
            addr = c->argv[2]->ptr;
            skipme = 0; /* With the old form, you can kill yourself. */
        } else if (c->argc > 3) {
            int i = 2; /* Next option index. */

            /* New style syntax: parse options. */
            while(i < c->argc) {
                int moreargs = c->argc > i+1;

                if (!strcasecmp(c->argv[i]->ptr,"id") && moreargs) {
                    long long tmp;

                    if (getLongLongFromObjectOrReply(c,c->argv[i+1],&tmp,NULL)
                        != C_OK) return;
                    id = tmp;
                } else if (!strcasecmp(c->argv[i]->ptr,"type") && moreargs) {
                    type = getClientTypeByName(c->argv[i+1]->ptr);
                    if (type == -1) {
                        addReplyErrorFormat(c,"Unknown client type '%s'",
                            (char*) c->argv[i+1]->ptr);
                        return;
                    }
                } else if (!strcasecmp(c->argv[i]->ptr,"addr") && moreargs) {
                    addr = c->argv[i+1]->ptr;
                } else if (!strcasecmp(c->argv[i]->ptr,"laddr") && moreargs) {
                    laddr = c->argv[i+1]->ptr;
                } else if (!strcasecmp(c->argv[i]->ptr,"user") && moreargs) {
                    user = ACLGetUserByName(c->argv[i+1]->ptr,
                                            sdslen(c->argv[i+1]->ptr));
                    if (user == NULL) {
                        addReplyErrorFormat(c,"No such user '%s'",
                            (char*) c->argv[i+1]->ptr);
                        return;
                    }
                } else if (!strcasecmp(c->argv[i]->ptr,"skipme") && moreargs) {
                    if (!strcasecmp(c->argv[i+1]->ptr,"yes")) {
                        skipme = 1;
                    } else if (!strcasecmp(c->argv[i+1]->ptr,"no")) {
                        skipme = 0;
                    } else {
                        addReplyErrorObject(c,shared.syntaxerr);
                        return;
                    }
                } else {
                    addReplyErrorObject(c,shared.syntaxerr);
                    return;
                }
                i += 2;
            }
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }

        /* Iterate clients killing all the matching clients. */
        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            client *client = listNodeValue(ln);
            if (addr && strcmp(getClientPeerId(client),addr) != 0) continue;
            if (laddr && strcmp(getClientSockname(client),laddr) != 0) continue;
            if (type != -1 && getClientType(client) != type) continue;
            if (id != 0 && client->id != id) continue;
            if (user && client->user != user) continue;
            if (c == client && skipme) continue;

            /* Kill it. */
            if (c == client) {
                close_this_client = 1;
            } else {
                freeClient(client);
            }
            killed++;
        }

        /* Reply according to old/new format. */
        if (c->argc == 3) {
            if (killed == 0)
                addReplyError(c,"No such client");
            else
                addReply(c,shared.ok);
        } else {
            addReplyLongLong(c,killed);
        }

        /* If this client has to be closed, flag it as CLOSE_AFTER_REPLY
         * only after we queued the reply to its output buffers. */
        if (close_this_client) c->flags |= CLIENT_CLOSE_AFTER_REPLY;
    } else if (!strcasecmp(c->argv[1]->ptr,"unblock") && (c->argc == 3 ||
                                                          c->argc == 4))
    {
        /* CLIENT UNBLOCK <id> [timeout|error] */
        long long id;
        int unblock_error = 0;

        if (c->argc == 4) {
            if (!strcasecmp(c->argv[3]->ptr,"timeout")) {
                unblock_error = 0;
            } else if (!strcasecmp(c->argv[3]->ptr,"error")) {
                unblock_error = 1;
            } else {
                addReplyError(c,
                    "CLIENT UNBLOCK reason should be TIMEOUT or ERROR");
                return;
            }
        }
        if (getLongLongFromObjectOrReply(c,c->argv[2],&id,NULL)
            != C_OK) return;
        struct client *target = lookupClientByID(id);
        if (target && target->flags & CLIENT_BLOCKED) {
            if (unblock_error)
                addReplyError(target,
                    "-UNBLOCKED client unblocked via CLIENT UNBLOCK");
            else
                replyToBlockedClientTimedOut(target);
            unblockClient(target);
            addReply(c,shared.cone);
        } else {
            addReply(c,shared.czero);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"setname") && c->argc == 3) {
        /* CLIENT SETNAME */
        if (clientSetNameOrReply(c,c->argv[2]) == C_OK)
            addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"getname") && c->argc == 2) {
        /* CLIENT GETNAME */
        if (c->name)
            addReplyBulk(c,c->name);
        else
            addReplyNull(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"unpause") && c->argc == 2) {
        /* CLIENT UNPAUSE */
        unpauseClients();
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"pause") && (c->argc == 3 ||
                                                        c->argc == 4))
    {
        /* CLIENT PAUSE TIMEOUT [WRITE|ALL] */
        mstime_t end;
        int type = CLIENT_PAUSE_ALL;
        if (c->argc == 4) {
            if (!strcasecmp(c->argv[3]->ptr,"write")) {
                type = CLIENT_PAUSE_WRITE;
            } else if (!strcasecmp(c->argv[3]->ptr,"all")) {
                type = CLIENT_PAUSE_ALL;
            } else {
                addReplyError(c,
                    "CLIENT PAUSE mode must be WRITE or ALL");  
                return;       
            }
        }

        if (getTimeoutFromObjectOrReply(c,c->argv[2],&end,
            UNIT_MILLISECONDS) != C_OK) return;
        pauseClients(end, type);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"tracking") && c->argc >= 3) {
        /* CLIENT TRACKING (on|off) [REDIRECT <id>] [BCAST] [PREFIX first]
         *                          [PREFIX second] [OPTIN] [OPTOUT] ... */
        long long redir = 0;
        uint64_t options = 0;
        robj **prefix = NULL;
        size_t numprefix = 0;

        /* Parse the options. */
        for (int j = 3; j < c->argc; j++) {
            int moreargs = (c->argc-1) - j;

            if (!strcasecmp(c->argv[j]->ptr,"redirect") && moreargs) {
                j++;
                if (redir != 0) {
                    addReplyError(c,"A client can only redirect to a single "
                                    "other client");
                    zfree(prefix);
                    return;
                }

                if (getLongLongFromObjectOrReply(c,c->argv[j],&redir,NULL) !=
                    C_OK)
                {
                    zfree(prefix);
                    return;
                }
                /* We will require the client with the specified ID to exist
                 * right now, even if it is possible that it gets disconnected
                 * later. Still a valid sanity check. */
                if (lookupClientByID(redir) == NULL) {
                    addReplyError(c,"The client ID you want redirect to "
                                    "does not exist");
                    zfree(prefix);
                    return;
                }
            } else if (!strcasecmp(c->argv[j]->ptr,"bcast")) {
                options |= CLIENT_TRACKING_BCAST;
            } else if (!strcasecmp(c->argv[j]->ptr,"optin")) {
                options |= CLIENT_TRACKING_OPTIN;
            } else if (!strcasecmp(c->argv[j]->ptr,"optout")) {
                options |= CLIENT_TRACKING_OPTOUT;
            } else if (!strcasecmp(c->argv[j]->ptr,"noloop")) {
                options |= CLIENT_TRACKING_NOLOOP;
            } else if (!strcasecmp(c->argv[j]->ptr,"prefix") && moreargs) {
                j++;
                prefix = zrealloc(prefix,sizeof(robj*)*(numprefix+1));
                prefix[numprefix++] = c->argv[j];
            } else {
                zfree(prefix);
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
        }

        /* Options are ok: enable or disable the tracking for this client. */
        if (!strcasecmp(c->argv[2]->ptr,"on")) {
            /* Before enabling tracking, make sure options are compatible
             * among each other and with the current state of the client. */
            if (!(options & CLIENT_TRACKING_BCAST) && numprefix) {
                addReplyError(c,
                    "PREFIX option requires BCAST mode to be enabled");
                zfree(prefix);
                return;
            }

            if (c->flags & CLIENT_TRACKING) {
                int oldbcast = !!(c->flags & CLIENT_TRACKING_BCAST);
                int newbcast = !!(options & CLIENT_TRACKING_BCAST);
                if (oldbcast != newbcast) {
                    addReplyError(c,
                    "You can't switch BCAST mode on/off before disabling "
                    "tracking for this client, and then re-enabling it with "
                    "a different mode.");
                    zfree(prefix);
                    return;
                }
            }

            if (options & CLIENT_TRACKING_BCAST &&
                options & (CLIENT_TRACKING_OPTIN|CLIENT_TRACKING_OPTOUT))
            {
                addReplyError(c,
                "OPTIN and OPTOUT are not compatible with BCAST");
                zfree(prefix);
                return;
            }

            if (options & CLIENT_TRACKING_OPTIN && options & CLIENT_TRACKING_OPTOUT)
            {
                addReplyError(c,
                "You can't specify both OPTIN mode and OPTOUT mode");
                zfree(prefix);
                return;
            }

            if ((options & CLIENT_TRACKING_OPTIN && c->flags & CLIENT_TRACKING_OPTOUT) ||
                (options & CLIENT_TRACKING_OPTOUT && c->flags & CLIENT_TRACKING_OPTIN))
            {
                addReplyError(c,
                "You can't switch OPTIN/OPTOUT mode before disabling "
                "tracking for this client, and then re-enabling it with "
                "a different mode.");
                zfree(prefix);
                return;
            }

            if (options & CLIENT_TRACKING_BCAST) {
                if (!checkPrefixCollisionsOrReply(c,prefix,numprefix)) {
                    zfree(prefix);
                    return;
                }
            }

            enableTracking(c,redir,options,prefix,numprefix);
        } else if (!strcasecmp(c->argv[2]->ptr,"off")) {
            disableTracking(c);
        } else {
            zfree(prefix);
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
        zfree(prefix);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"caching") && c->argc >= 3) {
        if (!(c->flags & CLIENT_TRACKING)) {
            addReplyError(c,"CLIENT CACHING can be called only when the "
                            "client is in tracking mode with OPTIN or "
                            "OPTOUT mode enabled");
            return;
        }

        char *opt = c->argv[2]->ptr;
        if (!strcasecmp(opt,"yes")) {
            if (c->flags & CLIENT_TRACKING_OPTIN) {
                c->flags |= CLIENT_TRACKING_CACHING;
            } else {
                addReplyError(c,"CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.");
                return;
            }
        } else if (!strcasecmp(opt,"no")) {
            if (c->flags & CLIENT_TRACKING_OPTOUT) {
                c->flags |= CLIENT_TRACKING_CACHING;
            } else {
                addReplyError(c,"CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.");
                return;
            }
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }

        /* Common reply for when we succeeded. */
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"getredir") && c->argc == 2) {
        /* CLIENT GETREDIR */
        if (c->flags & CLIENT_TRACKING) {
            addReplyLongLong(c,c->client_tracking_redirection);
        } else {
            addReplyLongLong(c,-1);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"trackinginfo") && c->argc == 2) {
        addReplyMapLen(c,3);

        /* Flags */
        addReplyBulkCString(c,"flags");
        void *arraylen_ptr = addReplyDeferredLen(c);
        int numflags = 0;
        addReplyBulkCString(c,c->flags & CLIENT_TRACKING ? "on" : "off");
        numflags++;
        if (c->flags & CLIENT_TRACKING_BCAST) {
            addReplyBulkCString(c,"bcast");
            numflags++;
        }
        if (c->flags & CLIENT_TRACKING_OPTIN) {
            addReplyBulkCString(c,"optin");
            numflags++;
            if (c->flags & CLIENT_TRACKING_CACHING) {
                addReplyBulkCString(c,"caching-yes");
                numflags++;        
            }
        }
        if (c->flags & CLIENT_TRACKING_OPTOUT) {
            addReplyBulkCString(c,"optout");
            numflags++;
            if (c->flags & CLIENT_TRACKING_CACHING) {
                addReplyBulkCString(c,"caching-no");
                numflags++;        
            }
        }
        if (c->flags & CLIENT_TRACKING_NOLOOP) {
            addReplyBulkCString(c,"noloop");
            numflags++;
        }
        if (c->flags & CLIENT_TRACKING_BROKEN_REDIR) {
            addReplyBulkCString(c,"broken_redirect");
            numflags++;
        }
        setDeferredSetLen(c,arraylen_ptr,numflags);

        /* Redirect */
        addReplyBulkCString(c,"redirect");
        if (c->flags & CLIENT_TRACKING) {
            addReplyLongLong(c,c->client_tracking_redirection);
        } else {
            addReplyLongLong(c,-1);
        }

        /* Prefixes */
        addReplyBulkCString(c,"prefixes");
        if (c->client_tracking_prefixes) {
            addReplyArrayLen(c,raxSize(c->client_tracking_prefixes));
            raxIterator ri;
            raxStart(&ri,c->client_tracking_prefixes);
            raxSeek(&ri,"^",NULL,0);
            while(raxNext(&ri)) {
                addReplyBulkCBuffer(c,ri.key,ri.key_len);
            }
            raxStop(&ri);
        } else {
            addReplyArrayLen(c,0);
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* HELLO [<protocol-version> [AUTH <user> <password>] [SETNAME <name>] ] */
// hello 命令处理
void helloCommand(client *c) {
    long long ver = 0;
    int next_arg = 1;

    if (c->argc >= 2) {
        if (getLongLongFromObjectOrReply(c, c->argv[next_arg++], &ver,
            "Protocol version is not an integer or out of range") != C_OK) {
            return;
        }

        if (ver < 2 || ver > 3) {
            addReplyError(c,"-NOPROTO unsupported protocol version");
            return;
        }
    }

    for (int j = next_arg; j < c->argc; j++) {
        int moreargs = (c->argc-1) - j;
        const char *opt = c->argv[j]->ptr;
        if (!strcasecmp(opt,"AUTH") && moreargs >= 2) {
            redactClientCommandArgument(c, j+1);
            redactClientCommandArgument(c, j+2);
            if (ACLAuthenticateUser(c, c->argv[j+1], c->argv[j+2]) == C_ERR) {
                addReplyError(c,"-WRONGPASS invalid username-password pair or user is disabled.");
                return;
            }
            j += 2;
        } else if (!strcasecmp(opt,"SETNAME") && moreargs) {
            if (clientSetNameOrReply(c, c->argv[j+1]) == C_ERR) return;
            j++;
        } else {
            addReplyErrorFormat(c,"Syntax error in HELLO option '%s'",opt);
            return;
        }
    }

    /* At this point we need to be authenticated to continue. */
    if (!c->authenticated) {
        addReplyError(c,"-NOAUTH HELLO must be called with the client already "
                        "authenticated, otherwise the HELLO AUTH <user> <pass> "
                        "option can be used to authenticate the client and "
                        "select the RESP protocol version at the same time");
        return;
    }

    /* Let's switch to the specified RESP mode. */
    if (ver) c->resp = ver;
    addReplyMapLen(c,6 + !server.sentinel_mode);

    addReplyBulkCString(c,"server");
    addReplyBulkCString(c,"redis");

    addReplyBulkCString(c,"version");
    addReplyBulkCString(c,REDIS_VERSION);

    addReplyBulkCString(c,"proto");
    addReplyLongLong(c,c->resp);

    addReplyBulkCString(c,"id");
    addReplyLongLong(c,c->id);

    addReplyBulkCString(c,"mode");
    if (server.sentinel_mode) addReplyBulkCString(c,"sentinel");
    else if (server.cluster_enabled) addReplyBulkCString(c,"cluster");
    else addReplyBulkCString(c,"standalone");

    if (!server.sentinel_mode) {
        addReplyBulkCString(c,"role");
        addReplyBulkCString(c,server.masterhost ? "replica" : "master");
    }

    addReplyBulkCString(c,"modules");
    addReplyLoadedModules(c);
}

/* This callback is bound to POST and "Host:" command names. Those are not
 * really commands, but are used in security attacks in order to talk to
 * Redis instances via HTTP, with a technique called "cross protocol scripting"
 * which exploits the fact that services like Redis will discard invalid
 * HTTP headers and will process what follows.
 *
 * As a protection against this attack, Redis will terminate the connection
 * when a POST or "Host:" header is seen, and will log the event from
 * time to time (to avoid creating a DOS as a result of too many logs). */
// redis遇到 POST 或 "Host:" 命令名时的处理函数。
// 这种不是真实的命令，而可能是通过http来进行跨协议的安全攻击，主要利用类似于redis服务器这种丢弃无效的http头，并接着处理的方式来攻击。
// 遇到这样的命令，redis这里打印日志后关闭该client连接，60s内只打印一条，防止过多这种请求形成dos攻击。
void securityWarningCommand(client *c) {
    static time_t logged_time;
    time_t now = time(NULL);

    // 60s内只打印一条日志
    if (llabs(now-logged_time) > 60) {
        serverLog(LL_WARNING,"Possible SECURITY ATTACK detected. It looks like somebody is sending POST or Host: commands to Redis. This is likely due to an attacker attempting to use Cross Protocol Scripting to compromise your Redis instance. Connection aborted.");
        logged_time = now;
    }
    // 异步关闭连接
    freeClientAsync(c);
}

/* Keep track of the original command arguments so that we can generate
 * an accurate slowlog entry after the command has been executed. */
// 这里保持原来的命令参数向量，这样命令执行后我们可以生成准确的慢日志入口。
static void retainOriginalCommandVector(client *c) {
    /* We already rewrote this command, so don't rewrite it again */
    // 如果以及rewrite过了，直接返回
    if (c->original_argv) return;
    // 分配空间，挨个拷贝赋值。原有参数对象的引用计数+1
    c->original_argc = c->argc;
    c->original_argv = zmalloc(sizeof(robj*)*(c->argc));
    for (int j = 0; j < c->argc; j++) {
        c->original_argv[j] = c->argv[j];
        incrRefCount(c->argv[j]);
    }
}

/* Redact a given argument to prevent it from being shown
 * in the slowlog. This information is stored in the
 * original_argv array. */
// 编辑给定的参数以防止它出现在慢日志中。此信息存储在original_argv数组中。
void redactClientCommandArgument(client *c, int argc) {
    retainOriginalCommandVector(c);
    decrRefCount(c->argv[argc]);
    c->original_argv[argc] = shared.redacted;
}

/* Rewrite the command vector of the client. All the new objects ref count
 * is incremented. The old command vector is freed, and the old objects
 * ref count is decremented. */
// 重写命令参数向量，使用传入的多个obj组成一个新的向量，替换原来的参数向量
void rewriteClientCommandVector(client *c, int argc, ...) {
    va_list ap;
    int j;
    robj **argv; /* The new argument vector */

    argv = zmalloc(sizeof(robj*)*argc);
    // 可变参数循环处理并赋值给新参数向量，增加新参数对象的引用。
    va_start(ap,argc);
    for (j = 0; j < argc; j++) {
        robj *a;

        a = va_arg(ap, robj*);
        argv[j] = a;
        // 增加引用
        incrRefCount(a);
    }
    // 参数向量替换
    replaceClientCommandVector(c, argc, argv);
    va_end(ap);
}

/* Completely replace the client command vector with the provided one. */
// 完全替换client命令参数向量。
void replaceClientCommandVector(client *c, int argc, robj **argv) {
    int j;
    // 保持原有参数向量
    retainOriginalCommandVector(c);
    // 释放原来所有的参数
    freeClientArgv(c);
    zfree(c->argv);
    // 赋值新的参数
    c->argv = argv;
    c->argc = argc;
    c->argv_len_sum = 0;
    // 更新新的argv_len_sum值
    for (j = 0; j < c->argc; j++)
        if (c->argv[j])
            c->argv_len_sum += getStringObjectLen(c->argv[j]);
    // 从命令列表里找替换后的cmd，并进行断言
    c->cmd = lookupCommandOrOriginal(c->argv[0]->ptr);
    serverAssertWithInfo(c,NULL,c->cmd != NULL);
}

/* Rewrite a single item in the command vector.
 * The new val ref count is incremented, and the old decremented.
 *
 * It is possible to specify an argument over the current size of the
 * argument vector: in this case the array of objects gets reallocated
 * and c->argc set to the max value. However it's up to the caller to
 *
 * 1. Make sure there are no "holes" and all the arguments are set.
 * 2. If the original argument vector was longer than the one we
 *    want to end with, it's up to the caller to set c->argc and
 *    free the no longer used objects on c->argv. */
// 重写命令向量中的单个item。新值的引用计数+1，旧值的引用计数-1。
// 这里可以指定超过当前参数向量size处的item，这种情况参数向量会reallocated，并更新c->argc为最大值。
// 当然这样操作调用者要确保原参数向量没有孔，即每个参数值都有set。
// 另外如果原始的参数向量长度比我们想要的多，调用者可以手动设置c->argc，并释放掉c->argv中不再使用的对象。
void rewriteClientCommandArgument(client *c, int i, robj *newval) {
    robj *oldval;
    // 保持原命令参数向量，用于慢日志等追踪操作。
    retainOriginalCommandVector(c);
    // 超出了向量长度，reallocation
    if (i >= c->argc) {
        c->argv = zrealloc(c->argv,sizeof(robj*)*(i+1));
        c->argc = i+1;
        c->argv[i] = NULL;
    }
    // 更新c->argv_len_sum
    oldval = c->argv[i];
    if (oldval) c->argv_len_sum -= getStringObjectLen(oldval);
    if (newval) c->argv_len_sum += getStringObjectLen(newval);
    // 更新i位置的新值
    c->argv[i] = newval;
    // 增加新obj的引用计数，减少旧obj的引用计数。减为0时对象会释放。
    incrRefCount(newval);
    if (oldval) decrRefCount(oldval);

    /* If this is the command name make sure to fix c->cmd. */
    // 如果rewrite的是命令的name，需要修复新设置的命令
    if (i == 0) {
        // 从命令列表里找这里修改后的cmd，并进行断言
        c->cmd = lookupCommandOrOriginal(c->argv[0]->ptr);
        serverAssertWithInfo(c,NULL,c->cmd != NULL);
    }
}

/* This function returns the number of bytes that Redis is
 * using to store the reply still not read by the client.
 *
 * Note: this function is very fast so can be called as many time as
 * the caller wishes. The main usage of this function currently is
 * enforcing the client output length limits. */
// 计算client的output buffer所占用的内存大小。
// 这个函数执行非常快，所以可以经常使用。目前主要在判断output buffer大小是否超限时使用。
unsigned long getClientOutputBufferMemoryUsage(client *c) {
    unsigned long list_item_size = sizeof(listNode) + sizeof(clientReplyBlock);
    return c->reply_bytes + (list_item_size*listLength(c->reply));
}

/* Get the class of a client, used in order to enforce limits to different
 * classes of clients.
 *
 * The function will return one of the following:
 * CLIENT_TYPE_NORMAL -> Normal client
 * CLIENT_TYPE_SLAVE  -> Slave
 * CLIENT_TYPE_PUBSUB -> Client subscribed to Pub/Sub channels
 * CLIENT_TYPE_MASTER -> The client representing our replication master.
 */
// 获取client的type，一般在需要根据不同类别进行限制的时候调用这个函数获取type。
int getClientType(client *c) {
    if (c->flags & CLIENT_MASTER) return CLIENT_TYPE_MASTER;
    /* Even though MONITOR clients are marked as replicas, we
     * want the expose them as normal clients. */
    // 即使MONITOR client被标记为slave，这里也将它当作正常client看待，而不作为slave。
    if ((c->flags & CLIENT_SLAVE) && !(c->flags & CLIENT_MONITOR))
        return CLIENT_TYPE_SLAVE;
    if (c->flags & CLIENT_PUBSUB) return CLIENT_TYPE_PUBSUB;
    return CLIENT_TYPE_NORMAL;
}

// 根据类型name获取client type
int getClientTypeByName(char *name) {
    if (!strcasecmp(name,"normal")) return CLIENT_TYPE_NORMAL;
    else if (!strcasecmp(name,"slave")) return CLIENT_TYPE_SLAVE;
    else if (!strcasecmp(name,"replica")) return CLIENT_TYPE_SLAVE;
    else if (!strcasecmp(name,"pubsub")) return CLIENT_TYPE_PUBSUB;
    else if (!strcasecmp(name,"master")) return CLIENT_TYPE_MASTER;
    else return -1;
}

// 获取client的类型name
char *getClientTypeName(int class) {
    switch(class) {
    case CLIENT_TYPE_NORMAL: return "normal";
    case CLIENT_TYPE_SLAVE:  return "slave";
    case CLIENT_TYPE_PUBSUB: return "pubsub";
    case CLIENT_TYPE_MASTER: return "master";
    default:                       return NULL;
    }
}

/* The function checks if the client reached output buffer soft or hard
 * limit, and also update the state needed to check the soft limit as
 * a side effect.
 *
 * Return value: non-zero if the client reached the soft or the hard limit.
 *               Otherwise zero is returned. */
// check是否output buffer达到软硬性要求。同时会更新一些状态信息，如达到软性限制的时间。
// 达到限制返回非0，否则返回0
int checkClientOutputBufferLimits(client *c) {
    int soft = 0, hard = 0, class;
    // 获取该client output buffer占用的内存，包括总的数据reply_bytes，以及reply链表节点所占用空间。
    unsigned long used_mem = getClientOutputBufferMemoryUsage(c);

    // 获取client类型，master、slave、pubsub、normal
    class = getClientType(c);
    /* For the purpose of output buffer limiting, masters are handled
     * like normal clients. */
    // check output buffer限制时，master当作普通client看待
    if (class == CLIENT_TYPE_MASTER) class = CLIENT_TYPE_NORMAL;

    // 根据配置中的限制大小进行检查。normal类型限制值为0，表示不做限制
    if (server.client_obuf_limits[class].hard_limit_bytes &&
        used_mem >= server.client_obuf_limits[class].hard_limit_bytes)
        hard = 1;
    if (server.client_obuf_limits[class].soft_limit_bytes &&
        used_mem >= server.client_obuf_limits[class].soft_limit_bytes)
        soft = 1;

    /* We need to check if the soft limit is reached continuously for the
     * specified amount of seconds. */
    // 对于达到soft limit的情况，需要判断达到这个上限持续的时长，只有持续超过一定时间才会真的软限制。
    if (soft) {
        if (c->obuf_soft_limit_reached_time == 0) {
            // 首次达到软性限制，设置时间为当前时间。暂时不需要软限制
            c->obuf_soft_limit_reached_time = server.unixtime;
            soft = 0; /* First time we see the soft limit reached */
        } else {
            // 不是首次达到，计算持续时长。如果持续时长不超过配置的上限，则也不真的进行软限制
            time_t elapsed = server.unixtime - c->obuf_soft_limit_reached_time;

            if (elapsed <=
                server.client_obuf_limits[class].soft_limit_seconds) {
                soft = 0; /* The client still did not reached the max number of
                             seconds for the soft limit to be considered
                             reached. */
            }
        }
    } else {
        // 没达到软性限制，时间设置为0
        c->obuf_soft_limit_reached_time = 0;
    }
    // 软性、硬性限制只要有一种真的需要限制，则进行限制
    return soft || hard;
}

/* Asynchronously close a client if soft or hard limit is reached on the
 * output buffer size. The caller can check if the client will be closed
 * checking if the client CLIENT_CLOSE_ASAP flag is set.
 *
 * Note: we need to close the client asynchronously because this function is
 * called from contexts where the client can't be freed safely, i.e. from the
 * lower level functions pushing data inside the client output buffers.
 * When `async` is set to 0, we close the client immediately, this is
 * useful when called from cron.
 *
 * Returns 1 if client was (flagged) closed. */
// 当output buffer大小限制达到时，异步关闭client。
// 调用者可以通过检查client是否有CLIENT_CLOSE_ASAP标识判断该client是否将被关闭。
// 注意我们需要异步来close，因为这个函数调用的上下文中client可能不能被安全的free掉。
// 例如：在低级别的方法中将数据写入client的output buffer中时，需要掉这个方法check处理。
// 另外当async设置为0时表示我们想同步直接关闭client，我们在cron任务中处理这类client是很有用。
int closeClientOnOutputBufferLimitReached(client *c, int async) {
    // 如果没有conn，是一个fake的client，不free。
    if (!c->conn) return 0; /* It is unsafe to free fake clients. */
    // 断言下total reply字节数。
    serverAssert(c->reply_bytes < SIZE_MAX-(1024*64));
    // reply_bytes为0 或 已经设置了CLIENT_CLOSE_ASAP标识，不需要check，直接返回。
    if (c->reply_bytes == 0 || c->flags & CLIENT_CLOSE_ASAP) return 0;
    // 检查output buffer是否达到了软性或硬性限制
    // 达到了则需要异步close，加入等待异步释放的队列。
    if (checkClientOutputBufferLimits(c)) {
        // sds存储client信息，用于打印
        sds client = catClientInfoString(sdsempty(),c);

        // 根据async标识来判断同步/异步处理client free
        if (async) {
            freeClientAsync(c);
            serverLog(LL_WARNING,
                      "Client %s scheduled to be closed ASAP for overcoming of output buffer limits.",
                      client);
        } else {
            freeClient(c);
            serverLog(LL_WARNING,
                      "Client %s closed for overcoming of output buffer limits.",
                      client);
        }
        sdsfree(client);
        return  1;
    }
    return 0;
}

/* Helper function used by performEvictions() in order to flush slaves
 * output buffers without returning control to the event loop.
 * This is also called by SHUTDOWN for a best-effort attempt to send
 * slaves the latest writes. */
// performEvictions（驱逐数据）使用的帮助函数，用于刷新主从同步的output buffers，而不返回控制给event loop
// SHUTDOWN时也会调用这个函数，从而尽力尝试发送最近写入的数据给slaves。
void flushSlavesOutputBuffers(void) {
    listIter li;
    listNode *ln;

    // 遍历当前节点的所有从节点进行处理。
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = listNodeValue(ln);
        // 判断slave作为client目前是否能接受数据，有conn write handler或output中有数据可写
        int can_receive_writes = connHasWriteHandler(slave->conn) ||
                                 (slave->flags & CLIENT_PENDING_WRITE);

        /* We don't want to send the pending data to the replica in a few
         * cases:
         *
         * 1. For some reason there is neither the write handler installed
         *    nor the client is flagged as to have pending writes: for some
         *    reason this replica may not be set to receive data. This is
         *    just for the sake of defensive programming.
         *
         * 2. The put_online_on_ack flag is true. To know why we don't want
         *    to send data to the replica in this case, please grep for the
         *    flag for this flag.
         *
         * 3. Obviously if the slave is not ONLINE.
         */
        // 一些情况下我们不想发送数据到从节点：
        // 1、conn write handler没设置 且 client output buf中没数据。
        // 这种情况没数据要写，就不需要处理了。一般这样是因为某些原因slave设置为不需要接受数据，仅用于防护程序。
        // 2、put_online_on_ack 为 true时。
        // 此时不发送是因为RDB传输完成，设置了ONLINE状态，但没有设置write handler。
        // RDB传输结束，转为增量复制时，有几步需要操作。详情见设置repl_put_online_on_ack=1的地方。
        // 3、slave不是ONLINE状态。只有增量update才通过缓冲区处理。
        if (slave->replstate == SLAVE_STATE_ONLINE &&
            can_receive_writes &&
            !slave->repl_put_online_on_ack &&
            clientHasPendingReplies(slave))
        {
            // 满足条件，才向slave发送同步数据
            writeToClient(slave,0);
        }
    }
}

/* Pause clients up to the specified unixtime (in ms) for a given type of
 * commands.
 *
 * A main use case of this function is to allow pausing replication traffic
 * so that a failover without data loss to occur. Replicas will continue to receive
 * traffic to faciliate this functionality.
 * 
 * This function is also internally used by Redis Cluster for the manual
 * failover procedure implemented by CLUSTER FAILOVER.
 *
 * The function always succeed, even if there is already a pause in progress.
 * In such a case, the duration is set to the maximum and new end time and the
 * type is set to the more restrictive type of pause. */
// 以指定的类型，暂停clients到指定的时间。
// 主要的用法是暂停replication流量，使得在故障转移时不出现数据丢失。这样Replicas可以继续接受数据不会受到影响。
// 这个方法也用于集群内部CLUSTER FAILOVER实现的手动故障转移程序。
// 这个方法总是成功，即使已经暂停。这时暂停时间会设置为较大的那个，暂停类型会设置为更严格的那个。
void pauseClients(mstime_t end, pause_type type) {
    // 使用更严格的暂停策略
    if (type > server.client_pause_type) {
        server.client_pause_type = type;
    }

    // 暂停时间设置更大的那个
    if (end > server.client_pause_end_time) {
        server.client_pause_end_time = end;
    }

    /* We allow write commands that were queued
     * up before and after to execute. We need
     * to track this state so that we don't assert
     * in propagate(). */
    // 如果当前server在执行一个事务，则标记在事务期间暂停。
    // 在事务执行期间暂停时，我们允许已入队和未入队的写命令执行。这里做一个记录，这样在propagate中就不再断言了。
    if (server.in_exec) {
        server.client_pause_in_transaction = 1;
    }
}

/* Unpause clients and queue them for reprocessing. */
// 解除paused_clients列表中client的暂停，并将之加入unblocked_clients队列，使之尽快处理。
void unpauseClients(void) {
    listNode *ln;
    listIter li;
    client *c;

    // 暂停状态解除
    server.client_pause_type = CLIENT_PAUSE_OFF;

    /* Unblock all of the clients so they are reprocessed. */
    // 遍历paused_clients列表，解除每个client的阻塞状态。
    // 该列表中client的block type应该都是BLOCKED_PAUSE。
    listRewind(server.paused_clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);
        // 解除client阻塞
        unblockClient(c);
    }
}

/* Returns true if clients are paused and false otherwise. */
// 判断是否是clients暂停状态
int areClientsPaused(void) {
    return server.client_pause_type != CLIENT_PAUSE_OFF;
}

/* Checks if the current client pause has elapsed and unpause clients
 * if it has. Also returns true if clients are now paused and false 
 * otherwise. */
// 检查是否是client暂停状态，不是则返回false。
int checkClientPauseTimeoutAndReturnIfPaused(void) {
    // 不是暂停状态，立即返回
    if (!areClientsPaused())
        return 0;
    // 是暂停状态，判断是否已到了时间，到了则解除暂停
    if (server.client_pause_end_time < server.mstime) {
        unpauseClients();
    }
    // 再次判断是否暂停状态
    return areClientsPaused();
}

/* This function is called by Redis in order to process a few events from
 * time to time while blocked into some not interruptible operation.
 * This allows to reply to clients with the -LOADING error while loading the
 * data set at startup or after a full resynchronization with the master
 * and so forth.
 *
 * It calls the event loop in order to process a few events. Specifically we
 * try to call the event loop 4 times as long as we receive acknowledge that
 * some event was processed, in order to go forward with the accept, read,
 * write, close sequence needed to serve a client.
 *
 * The function returns the total number of events processed. */

// ProcessingEventsWhileBlocked，阻塞操作期间会每隔一段时间检查处理events，避免client长时间等待。
// 如RDB、AOF load数据或执行Lua脚本批量处理数据时，有连接等待，及时处理返回-LOADING err。
void processEventsWhileBlocked(void) {
    // 迭代4次
    int iterations = 4; /* See the function top-comment. */

    /* Update our cached time since it is used to create and update the last
     * interaction time with clients and for other important things. */
    // 更新缓存的时间
    updateCachedTime(0);

    /* Note: when we are processing events while blocked (for instance during
     * busy Lua scripts), we set a global flag. When such flag is set, we
     * avoid handling the read part of clients using threaded I/O.
     * See https://github.com/redis/redis/issues/6988 for more info. */
    // 处理阻塞的events时，设置全局标识。当有该标识时，避免使用io线程处理client读。
    ProcessingEventsWhileBlocked = 1;
    while (iterations--) {
        long long startval = server.events_processed_while_blocked;
        // 直接调用aeProcessEvents处理事件
        long long ae_events = aeProcessEvents(server.el,
            AE_FILE_EVENTS|AE_DONT_WAIT|
            AE_CALL_BEFORE_SLEEP|AE_CALL_AFTER_SLEEP);
        /* Note that server.events_processed_while_blocked will also get
         * incremeted by callbacks called by the event loop handlers. */
        // 记录blocked时处理的client事件数量
        server.events_processed_while_blocked += ae_events;
        long long events = server.events_processed_while_blocked - startval;
        // 如果迭代时一直有事件处理，说明很忙，就一直迭代，不过最多也只迭代4次。
        // 如果没有事件处理，立即跳出循环。
        if (!events) break;
    }

    // blocked期间处理定时任务
    whileBlockedCron();

    ProcessingEventsWhileBlocked = 0;
}

/* ==========================================================================
 * Threaded I/O
 * ========================================================================== */

#define IO_THREADS_MAX_NUM 128  // 最大io线程数
#define IO_THREADS_OP_READ 0
#define IO_THREADS_OP_WRITE 1

// io线程列表
pthread_t io_threads[IO_THREADS_MAX_NUM];
// io线程的锁，主线程通过这个锁来控制io线程的start和stop。
pthread_mutex_t io_threads_mutex[IO_THREADS_MAX_NUM];
// io线程各自队列中待执行的client数量，该值的读写都是原子操作。
redisAtomic unsigned long io_threads_pending[IO_THREADS_MAX_NUM];
// io线程当前操作类型，读或写
int io_threads_op;      /* IO_THREADS_OP_WRITE or IO_THREADS_OP_READ. */

/* This is the list of clients each thread will serve when threaded I/O is
 * used. We spawn io_threads_num-1 threads, since one is the main thread
 * itself. */
// 每个线程各自的等待队列，包括主线程一共io_threads_num个线程队列。io_threads_list[0]为主线程队列。
list *io_threads_list[IO_THREADS_MAX_NUM];

// 获取对应io线程等待执行的client数量。
static inline unsigned long getIOPendingCount(int i) {
    unsigned long count = 0;
    atomicGetWithSync(io_threads_pending[i], count);
    return count;
}

// 设置对应io线程等待执行的client数量。
static inline void setIOPendingCount(int i, unsigned long count) {
    atomicSetWithSync(io_threads_pending[i], count);
}

void *IOThreadMain(void *myid) {
    /* The ID is the thread number (from 0 to server.iothreads_num-1), and is
     * used by the thread to just manipulate a single sub-array of clients. */
    long id = (unsigned long)myid;
    char thdname[16];

    snprintf(thdname, sizeof(thdname), "io_thd_%ld", id);
    // 设置线程name、cpu亲和性、线程可kill
    redis_set_thread_title(thdname);
    redisSetCpuAffinity(server.server_cpulist);
    makeThreadKillable();

    while(1) {
        /* Wait for start */
        // 大部分时间自旋等待
        for (int j = 0; j < 1000000; j++) {
            if (getIOPendingCount(id) != 0) break;
        }
        // io_threads_pending 是一个原子变量，起到同步作用。
        // 负载较大时，因为主线程只处理事件和内存操作，运行较快，可认为IO线程每次读取到pending不为0，也就不需要去lock。
        // 即使主线程阻塞，导致了IO线程去做了lock，但也是锁了就释放，不会有阻塞现象。
        // 因而 从这个意义上可以讲是无锁的。
        //
        // 主线程在填充队列时，此时pending为0，可以放心的填充，不用担心IO线程的数据竞争。
        // 填充后，设置pending为非0，此时IO线程开始去读取任务队列。完成端口读取后，再设置pending为0.
        // 主线程设置pending后，则会等待pending再次变为0，此时一轮的同步结束，放心操作队列中数据。
        // 在主线程检测任务不繁忙时候，可以做lock操作，这时IO线程就阻塞在锁上，释放CPU资源。

        /* Give the main thread a chance to stop this thread. */
        // 如果pending一直是0，没有可处理的io，主线程会关闭io线程，即将io线程的锁持有住，让io线程都阻塞，释放cpu。
        if (getIOPendingCount(id) == 0) {
            pthread_mutex_lock(&io_threads_mutex[id]);
            pthread_mutex_unlock(&io_threads_mutex[id]);
            continue;
        }

        serverAssert(getIOPendingCount(id) != 0);

        /* Process: note that the main thread will never touch our list
         * before we drop the pending count to 0. */
        // io线程是启动的，那么就遍历线程任务队列，挨个处理读或写处理事件。
        // io_threads_op是由主线程先设置，然后才active启动的。每一轮io线程处理只会有一种操作。
        listIter li;
        listNode *ln;
        listRewind(io_threads_list[id],&li);
        while((ln = listNext(&li))) {
            client *c = listNodeValue(ln);
            if (io_threads_op == IO_THREADS_OP_WRITE) {
                // 写回复client
                writeToClient(c,0);
            } else if (io_threads_op == IO_THREADS_OP_READ) {
                // 读client数据，解析命令
                readQueryFromClient(c->conn);
            } else {
                serverPanic("io_threads_op value is unknown");
            }
        }
        // 置空线程任务队列，等待处理数设置为0
        listEmpty(io_threads_list[id]);
        setIOPendingCount(id, 0);
    }
}

/* Initialize the data structures needed for threaded I/O. */
// io线程初始化
void initThreadedIO(void) {
    server.io_threads_active = 0; /* We start with threads not active. */

    /* Don't spawn any thread if the user selected a single thread:
     * we'll handle I/O directly from the main thread. */
    // 当io_threads_num == 1时，使用主线程，不再有额外io线程
    if (server.io_threads_num == 1) return;

    // 最大io线程设置为128
    if (server.io_threads_num > IO_THREADS_MAX_NUM) {
        serverLog(LL_WARNING,"Fatal: too many I/O threads configured. "
                             "The maximum number is %d.", IO_THREADS_MAX_NUM);
        exit(1);
    }

    /* Spawn and initialize the I/O threads. */
    for (int i = 0; i < server.io_threads_num; i++) {
        /* Things we do for all the threads including the main thread. */
        // 每个线程都会有一堆client等待处理，等待client的链表。
        io_threads_list[i] = listCreate();
        // Thread 0是主线程，不需要再进行创建初始化
        if (i == 0) continue; /* Thread 0 is the main thread. */

        /* Things we do only for the additional threads. */
        pthread_t tid;
        // 初始化io线程锁
        pthread_mutex_init(&io_threads_mutex[i],NULL);
        // 设置待执行count的数量
        setIOPendingCount(i, 0);
        // 本函数initThreadedIO是主函数执行的，这里先获取io线程的锁，从而使新创建的io线程进入阻塞状态。
        // io线程处理stop状态，等待主线程调度激活io线程。
        pthread_mutex_lock(&io_threads_mutex[i]); /* Thread will be stopped. */
        // 创建io线程，IOThreadMain为线程真正执行内容
        if (pthread_create(&tid,NULL,IOThreadMain,(void*)(long)i) != 0) {
            serverLog(LL_WARNING,"Fatal: Can't initialize IO thread.");
            exit(1);
        }
        io_threads[i] = tid;
    }
}

// 主线程kill调用io线程
void killIOThreads(void) {
    int err, j;
    // 遍历处理
    for (j = 0; j < server.io_threads_num; j++) {
        // 如果kill的线程就是当前线程本身，即主线程，则跳过
        if (io_threads[j] == pthread_self()) continue;
        // pthread_cancel 结束线程io_threads[j]
        if (io_threads[j] && pthread_cancel(io_threads[j]) == 0) {
            // pthread_join 当前线程等待线程io_threads[j]结束。
            if ((err = pthread_join(io_threads[j],NULL)) != 0) {
                serverLog(LL_WARNING,
                    "IO thread(tid:%lu) can not be joined: %s",
                        (unsigned long)io_threads[j], strerror(err));
            } else {
                serverLog(LL_WARNING,
                    "IO thread(tid:%lu) terminated",(unsigned long)io_threads[j]);
            }
        }
    }
}

void startThreadedIO(void) {
    serverAssert(server.io_threads_active == 0);
    // 主线程为每个线程锁解锁，这样阻塞在锁上的io线程就可以获取到锁，从而活跃起来。
    for (int j = 1; j < server.io_threads_num; j++)
        pthread_mutex_unlock(&io_threads_mutex[j]);
    // io线程置为active
    server.io_threads_active = 1;
}

void stopThreadedIO(void) {
    /* We may have still clients with pending reads when this function
     * is called: handle them before stopping the threads. */
    // 停止io线程前，先处理pending read clients。
    handleClientsWithPendingReadsUsingThreads();
    serverAssert(server.io_threads_active == 1);
    // 主线程为每个线程锁加锁，这样io线程就在获取锁时阻塞了，释放cpu资源。从而相当于stop了io线程
    for (int j = 1; j < server.io_threads_num; j++)
        pthread_mutex_lock(&io_threads_mutex[j]);
    // io线程置为非active
    server.io_threads_active = 0;
}

/* This function checks if there are not enough pending clients to justify
 * taking the I/O threads active: in that case I/O threads are stopped if
 * currently active. We track the pending writes as a measure of clients
 * we need to handle in parallel, however the I/O threading is disabled
 * globally for reads as well if we have too little pending clients.
 *
 * The function returns 0 if the I/O threading should be used because there
 * are enough active threads, otherwise 1 is returned and the I/O threads
 * could be possibly stopped (if already active) as a side effect. */
// 这个函数检查是否有足够都的待处理clients，来使得我们需要开启I/O线程。
// 如果没有足够clients，我们实际上是不需要IO线程在那空转的，所以这里在主线程里对每个IO线程的互斥量加锁，从而让IO线程阻塞让出cpu进入等待。
// 这里以待写的clients作为需要并行处理评估量，不过在真正要暂停IO线程前，stopThreadedIO中会先使用IO线程处理完待读clients，所以不会有影响。
// 如果IO线程不应该stop，则返回0；否则返回1，且当前IO线程活跃时，副作用会stop掉。
int stopThreadedIOIfNeeded(void) {
    int pending = listLength(server.clients_pending_write);

    /* Return ASAP if IO threads are disabled (single threaded mode). */
    // 如果是只有一个线程，尽早返回
    if (server.io_threads_num == 1) return 1;

    // 等待写的client数小于2倍的io线程，停止使用io线程。
    if (pending < (server.io_threads_num*2)) {
        if (server.io_threads_active) stopThreadedIO();
        return 1;
    } else {
        return 0;
    }
}

// 使用多io处理写
int handleClientsWithPendingWritesUsingThreads(void) {
    // 待写client数为0，直接返回
    int processed = listLength(server.clients_pending_write);
    if (processed == 0) return 0; /* Return ASAP if there are no clients. */

    /* If I/O threads are disabled or we have few clients to serve, don't
     * use I/O threads, but the boring synchronous code. */
    // 如果io线程不可用，或者只有很少的client，那么不使用io线程，只用同步处理就好。
    if (server.io_threads_num == 1 || stopThreadedIOIfNeeded()) {
        return handleClientsWithPendingWrites();
    }

    /* Start threads if needed. */
    // 需要启动io线程处理
    if (!server.io_threads_active) startThreadedIO();

    /* Distribute the clients across N different lists. */
    // 待写的client取模分配到各io线程的工作队列
    listIter li;
    listNode *ln;
    listRewind(server.clients_pending_write,&li);
    int item_id = 0;
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        c->flags &= ~CLIENT_PENDING_WRITE;

        /* Remove clients from the list of pending writes since
         * they are going to be closed ASAP. */
        // 如果client的标识是CLIENT_CLOSE_ASAP，直接删除节点，尽早close
        if (c->flags & CLIENT_CLOSE_ASAP) {
            listDelNode(server.clients_pending_write, ln);
            continue;
        }

        // 加入到对应io线程队列
        int target_id = item_id % server.io_threads_num;
        listAddNodeTail(io_threads_list[target_id],c);
        item_id++;
    }

    /* Give the start condition to the waiting threads, by setting the
     * start condition atomic var. */
    // 设置io操作为写，原子写入每个io线程需要处理的client数量
    io_threads_op = IO_THREADS_OP_WRITE;
    for (int j = 1; j < server.io_threads_num; j++) {
        int count = listLength(io_threads_list[j]);
        setIOPendingCount(j, count);
    }

    /* Also use the main thread to process a slice of clients. */
    // 使用当前主线程处理一个队列
    listRewind(io_threads_list[0],&li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        // 真正处理写入数据
        writeToClient(c,0);
    }
    listEmpty(io_threads_list[0]);

    /* Wait for all the other threads to end their work. */
    // 等待其他线程处理完写操作
    while(1) {
        unsigned long pending = 0;
        for (int j = 1; j < server.io_threads_num; j++)
            pending += getIOPendingCount(j);
        if (pending == 0) break;
    }

    /* Run the list of clients again to install the write handler where
     * needed. */
    // 遍历clients_pending_write，看buffer中是否还有数据要回复。
    // 一次写不完的数据，需要等待fd再次可写，才能继续处理。
    listRewind(server.clients_pending_write,&li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);

        /* Install the write handler if there are pending writes in some
         * of the clients. */
        // 如果还有待回复的数据，即一次写不下，那么需要等待fd可写再处理，所以创建可写事件监听。
        //
        if (clientHasPendingReplies(c) &&
                connSetWriteHandler(c->conn, sendReplyToClient) == AE_ERR)
        {
            // 创建监听事件失败，则异步释放client
            freeClientAsync(c);
        }
    }
    // 置空clients_pending_write链表
    listEmpty(server.clients_pending_write);

    /* Update processed count on server */
    // 记录处理写的次数统计
    server.stat_io_writes_processed += processed;

    return processed;
}

/* Return 1 if we want to handle the client read later using threaded I/O.
 * This is called by the readable handler of the event loop.
 * As a side effect of calling this function the client is put in the
 * pending read clients and flagged as such. */
// 如果有io线程，则将client加入到clients_pending_read队列中，并设置该client标识为CLIENT_PENDING_READ。
// 会判断传入的client标识是否是CLIENT_PENDING_READ，避免重复加入。
int postponeClientRead(client *c) {
    // 程序中io线程是否已启动 && io线程配置为true && 非ProcessingEventsWhileBlocked状态
    if (server.io_threads_active &&
        server.io_threads_do_reads &&
        !ProcessingEventsWhileBlocked &&
        !(c->flags & (CLIENT_MASTER|CLIENT_SLAVE|CLIENT_PENDING_READ)))
    {
        // io线程实际上也是调用readQueryFromClient来处理io的。
        // 所以这里要设置CLIENT_PENDING_READ，再次从readQueryFromClient进来的时候，就走到else里了。
        c->flags |= CLIENT_PENDING_READ;
        // 将client加入pending read队列
        listAddNodeHead(server.clients_pending_read,c);
        return 1;
    } else {
        return 0;
    }
}

/* When threaded I/O is also enabled for the reading + parsing side, the
 * readable handler will just put normal clients into a queue of clients to
 * process (instead of serving them synchronously). This function runs
 * the queue using the I/O threads, and process them in order to accumulate
 * the reads in the buffers, and also parse the first command available
 * rendering it in the client structures. */
// 当开启io线程处理reading+parsing时，readable handler会将可读的client加到待处理队列中，而不是同步处理。
// 这个函数使用io线程来处理队列，并将数据读出来放入buffers中，同时解析第一个有效命令放入client结构中。
int handleClientsWithPendingReadsUsingThreads(void) {
    if (!server.io_threads_active || !server.io_threads_do_reads) return 0;
    // 如果没有等待read的client，直接返回
    int processed = listLength(server.clients_pending_read);
    if (processed == 0) return 0;

    /* Distribute the clients across N different lists. */
    // 遍历等待read的list，轮训分配给对应的io线程。
    listIter li;
    listNode *ln;
    listRewind(server.clients_pending_read,&li);
    int item_id = 0;
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        // 取模，加入对应线程待处理列表
        int target_id = item_id % server.io_threads_num;
        listAddNodeTail(io_threads_list[target_id],c);
        item_id++;
    }

    /* Give the start condition to the waiting threads, by setting the
     * start condition atomic var. */
    // 设置线程操作为read
    io_threads_op = IO_THREADS_OP_READ;
    // 原子方式更新每个线程待处理读io的数量
    for (int j = 1; j < server.io_threads_num; j++) {
        int count = listLength(io_threads_list[j]);
        setIOPendingCount(j, count);
    }

    /* Also use the main thread to process a slice of clients. */
    // 这里主线程也参与执行一份io操作。循环遍历列表处理每个client的读io。
    listRewind(io_threads_list[0],&li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        // 处理读io
        readQueryFromClient(c->conn);
    }
    // 处理完了置空链表。
    listEmpty(io_threads_list[0]);

    /* Wait for all the other threads to end their work. */
    // 主线程等待所有其他线程处理完他们的read工作。
    // 所有线程都执行完了，主线程才继续前进。
    while(1) {
        unsigned long pending = 0;
        for (int j = 1; j < server.io_threads_num; j++)
            pending += getIOPendingCount(j);
        if (pending == 0) break;
    }

    /* Run the list of clients again to process the new buffers. */
    // 前面io线程处理读完数据到buffer并解析，这里主线程挨个处理列表中的client
    while(listLength(server.clients_pending_read)) {
        // 拿到一个client
        ln = listFirst(server.clients_pending_read);
        client *c = listNodeValue(ln);
        c->flags &= ~CLIENT_PENDING_READ;
        listDelNode(server.clients_pending_read,ln);

        // 如果该client命令解析完了，处于可执行状态，则直接执行。
        // 如果client执行完命令就无效了，那么直接continue处理下一个client。
        if (processPendingCommandsAndResetClient(c) == C_ERR) {
            /* If the client is no longer valid, we avoid
             * processing the client later. So we just go
             * to the next. */
            continue;
        }

        // client的命令还没准备好，需要继续解析buffer中的数据，获得完整命令再执行
        processInputBuffer(c);

        /* We may have pending replies if a thread readQueryFromClient() produced
         * replies and did not install a write handler (it can't).
         */
        // 如果client处理完命令后需要回复，并且该client没有加入pending write列表，则设置CLIENT_PENDING_WRITE并加入写等待队列。
        if (!(c->flags & CLIENT_PENDING_WRITE) && clientHasPendingReplies(c))
            // 加入队列
            clientInstallWriteHandler(c);
    }

    /* Update processed count on server */
    // 更新io线程读处理数量
    server.stat_io_reads_processed += processed;

    return processed;
}
