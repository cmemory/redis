/* Copyright (c) 2009-2020, Salvatore Sanfilippo <antirez at gmail dot com>
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

/* ========================== Clients timeouts ============================= */

/* Check if this blocked client timedout (does nothing if the client is
 * not blocked right now). If so send a reply, unblock it, and return 1.
 * Otherwise 0 is returned and no operation is performed. */
// 检查这个被阻塞的client是否超时（如果client不处于阻塞状态，什么都不做）。
// 如果超时，则发送回复，解除阻塞，并返回1。否则返回0，不执行任何操作。
int checkBlockedClientTimeout(client *c, mstime_t now) {
    if (c->flags & CLIENT_BLOCKED &&
        c->bpop.timeout != 0
        && c->bpop.timeout < now)
    {
        /* Handle blocking operation specific timeout. */
        // timeout了，发送回复给阻塞的client，然后解除阻塞。
        replyToBlockedClientTimedOut(c);
        unblockClient(c);
        return 1;
    } else {
        return 0;
    }
}

/* Check for timeouts. Returns non-zero if the client was terminated.
 * The function gets the current time in milliseconds as argument since
 * it gets called multiple times in a loop, so calling gettimeofday() for
 * each iteration would be costly without any actual gain. */
// 检查client各种超时。如果client被释放则返回非0。
// 因为本函数会迭代执行多次，每次调用gettimeofday()获取当前时间得不偿失，所以使用now_ms传入本轮执行clientsCron的时间，
int clientsCronHandleTimeout(client *c, mstime_t now_ms) {
    time_t now = now_ms/1000;

    // 如果有设置最大空闲时间，且已超时，则释放该空闲client连接。
    // 注意这只真的普通clients连接，对于slaves、monitors、masters、BLOCKED以及Pub/Sub clients，我们不检测空闲时间超时。
    if (server.maxidletime &&
        /* This handles the idle clients connection timeout if set. */
        !(c->flags & CLIENT_SLAVE) &&   /* No timeout for slaves and monitors */
        !(c->flags & CLIENT_MASTER) &&  /* No timeout for masters */
        !(c->flags & CLIENT_BLOCKED) && /* No timeout for BLPOP */
        !(c->flags & CLIENT_PUBSUB) &&  /* No timeout for Pub/Sub clients */
        (now - c->lastinteraction > server.maxidletime))
    {
        serverLog(LL_VERBOSE,"Closing idle client");
        // 释放client，返回1
        freeClient(c);
        return 1;
    } else if (c->flags & CLIENT_BLOCKED) {
        /* Cluster: handle unblock & redirect of clients blocked
         * into keys no longer served by this server. */
        // 对于集群模式的CLIENT_BLOCKED，这里调用clusterRedirectBlockedClientIfNeeded，检测阻塞的key是否被迁移，从而决定是否要回复重定向。
        // 之前这里有处理正常CLIENT_BLOCKED的clients超时的，后面移到beforeSleep中handleBlockedClientsTimeout()处理了。
        if (server.cluster_enabled) {
            if (clusterRedirectBlockedClientIfNeeded(c))
                // 如果需要重定向，解除block，发送重定向回复。
                unblockClient(c);
        }
    }
    return 0;
}

/* For blocked clients timeouts we populate a radix tree of 128 bit keys
 * composed as such:
 *
 *  [8 byte big endian expire time]+[8 byte client ID]
 *
 * We don't do any cleanup in the Radix tree: when we run the clients that
 * reached the timeout already, if they are no longer existing or no longer
 * blocked with such timeout, we just go forward.
 *
 * Every time a client blocks with a timeout, we add the client in
 * the tree. In beforeSleep() we call handleBlockedClientsTimeout() to run
 * the tree and unblock the clients. */

#define CLIENT_ST_KEYLEN 16    /* 8 bytes mstime + 8 bytes client ID. */

/* Given client ID and timeout, write the resulting radix tree key in buf. */
void encodeTimeoutKey(unsigned char *buf, uint64_t timeout, client *c) {
    timeout = htonu64(timeout);
    memcpy(buf,&timeout,sizeof(timeout));
    memcpy(buf+8,&c,sizeof(c));
    if (sizeof(c) == 4) memset(buf+12,0,4); /* Zero padding for 32bit target. */
}

/* Given a key encoded with encodeTimeoutKey(), resolve the fields and write
 * the timeout into *toptr and the client pointer into *cptr. */
void decodeTimeoutKey(unsigned char *buf, uint64_t *toptr, client **cptr) {
    memcpy(toptr,buf,sizeof(*toptr));
    *toptr = ntohu64(*toptr);
    memcpy(cptr,buf+8,sizeof(*cptr));
}

/* Add the specified client id / timeout as a key in the radix tree we use
 * to handle blocked clients timeouts. The client is not added to the list
 * if its timeout is zero (block forever). */
void addClientToTimeoutTable(client *c) {
    if (c->bpop.timeout == 0) return;
    uint64_t timeout = c->bpop.timeout;
    unsigned char buf[CLIENT_ST_KEYLEN];
    encodeTimeoutKey(buf,timeout,c);
    if (raxTryInsert(server.clients_timeout_table,buf,sizeof(buf),NULL,NULL))
        c->flags |= CLIENT_IN_TO_TABLE;
}

/* Remove the client from the table when it is unblocked for reasons
 * different than timing out. */
void removeClientFromTimeoutTable(client *c) {
    if (!(c->flags & CLIENT_IN_TO_TABLE)) return;
    c->flags &= ~CLIENT_IN_TO_TABLE;
    uint64_t timeout = c->bpop.timeout;
    unsigned char buf[CLIENT_ST_KEYLEN];
    encodeTimeoutKey(buf,timeout,c);
    raxRemove(server.clients_timeout_table,buf,sizeof(buf),NULL);
}

/* This function is called in beforeSleep() in order to unblock clients
 * that are waiting in blocking operations with a timeout set. */
// 这个函数在beforeSleep()中调用，用于对超时的阻塞client解除阻塞。
void handleBlockedClientsTimeout(void) {
    // 超时表没有数据，直接返回
    if (raxSize(server.clients_timeout_table) == 0) return;
    uint64_t now = mstime();
    raxIterator ri;
    // 初始化rax迭代器
    raxStart(&ri,server.clients_timeout_table);
    // 定位到最小key
    raxSeek(&ri,"^",NULL,0);

    // 迭代，取出迭代器指定的元素处理
    while(raxNext(&ri)) {
        uint64_t timeout;
        client *c;
        // 对处理到的key解析出timeout时间
        decodeTimeoutKey(ri.key,&timeout,&c);
        // 如果timeout>=now，说明表中最小的过期时间都大于当前时间，即所有的timeouts都在未来，所以我们什么都不需要处理，跳出返回。
        if (timeout >= now) break; /* All the timeouts are in the future. */
        // 如果超时了，我们需要检查超时，发送回复，并从表中移除该元素。
        c->flags &= ~CLIENT_IN_TO_TABLE;
        checkBlockedClientTimeout(c,now);
        raxRemove(server.clients_timeout_table,ri.key,ri.key_len,NULL);
        // 移除最小超时时间的元素后，我们还要定位到表中还剩的最小的超时时间元素，用于下一轮循环处理。
        raxSeek(&ri,"^",NULL,0);
    }
    // 如果没有元素超时了，释放迭代器退出。
    raxStop(&ri);
}

/* Get a timeout value from an object and store it into 'timeout'.
 * The final timeout is always stored as milliseconds as a time where the
 * timeout will expire, however the parsing is performed according to
 * the 'unit' that can be seconds or milliseconds.
 *
 * Note that if the timeout is zero (usually from the point of view of
 * commands API this means no timeout) the value stored into 'timeout'
 * is zero. */
int getTimeoutFromObjectOrReply(client *c, robj *object, mstime_t *timeout, int unit) {
    long long tval;
    long double ftval;

    if (unit == UNIT_SECONDS) {
        if (getLongDoubleFromObjectOrReply(c,object,&ftval,
            "timeout is not a float or out of range") != C_OK)
            return C_ERR;
        tval = (long long) (ftval * 1000.0);
    } else {
        if (getLongLongFromObjectOrReply(c,object,&tval,
            "timeout is not an integer or out of range") != C_OK)
            return C_ERR;
    }

    if (tval < 0) {
        addReplyError(c,"timeout is negative");
        return C_ERR;
    }

    if (tval > 0) {
        tval += mstime();
    }
    *timeout = tval;

    return C_OK;
}
