/* Asynchronous replication implementation.
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
 */


#include "server.h"
#include "cluster.h"
#include "bio.h"

#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>

void replicationDiscardCachedMaster(void);
void replicationResurrectCachedMaster(connection *conn);
void replicationSendAck(void);
void putSlaveOnline(client *slave);
int cancelReplicationHandshake(int reconnect);

/* We take a global flag to remember if this instance generated an RDB
 * because of replication, so that we can remove the RDB file in case
 * the instance is configured to have no persistence. */
// 使用一个全局变量来记录当前生成的RDB是否因为复制产生的，这样如果我们实例配置不持久化的情况下后面可以移除这个RDB文件。
int RDBGeneratedByReplication = 0;

/* --------------------------- Utility functions ---------------------------- */

/* Return the pointer to a string representing the slave ip:listening_port
 * pair. Mostly useful for logging, since we want to log a slave using its
 * IP address and its listening port which is more clear for the user, for
 * example: "Closing connection with replica 10.1.2.3:6380". */
char *replicationGetSlaveName(client *c) {
    static char buf[NET_HOST_PORT_STR_LEN];
    char ip[NET_IP_STR_LEN];

    ip[0] = '\0';
    buf[0] = '\0';
    if (c->slave_addr ||
        connPeerToString(c->conn,ip,sizeof(ip),NULL) != -1)
    {
        char *addr = c->slave_addr ? c->slave_addr : ip;
        if (c->slave_listening_port)
            anetFormatAddr(buf,sizeof(buf),addr,c->slave_listening_port);
        else
            snprintf(buf,sizeof(buf),"%s:<unknown-replica-port>",addr);
    } else {
        snprintf(buf,sizeof(buf),"client id #%llu",
            (unsigned long long) c->id);
    }
    return buf;
}

/* Plain unlink() can block for quite some time in order to actually apply
 * the file deletion to the filesystem. This call removes the file in a
 * background thread instead. We actually just do close() in the thread,
 * by using the fact that if there is another instance of the same file open,
 * the foreground unlink() will only remove the fs name, and deleting the
 * file's storage space will only happen once the last reference is lost. */
// 普通的unlink()操作可能会阻塞一些时间，因为会执行文件的删除操作。
// 这里我们先open文件，增加一个引用，然后执行unlink()，此时解除关联后因为有文件的引用，所以并没有真正的执行文件删除。
// 只有我们后面关闭该文件时，才会真正的执行删除操作。所以这里我们可以将关闭文件异步执行，从而实现异步删除文件的操作。
int bg_unlink(const char *filename) {
    // 打开文件，增加引用
    int fd = open(filename,O_RDONLY|O_NONBLOCK);
    if (fd == -1) {
        /* Can't open the file? Fall back to unlinking in the main thread. */
        // 打开失败，我们只能回退直接处理删除了。
        return unlink(filename);
    } else {
        /* The following unlink() removes the name but doesn't free the
         * file contents because a process still has it open. */
        // 这里unlink只是移除了name，并没有删除文件释放空间，因为进程还在处理它。
        int retval = unlink(filename);
        if (retval == -1) {
            /* If we got an unlink error, we just return it, closing the
             * new reference we have to the file. */
            // 如果unlink出错了，我们直接返回，另外我们还需要关闭我们打开的文件引用。
            int old_errno = errno;
            // close也可能出错，从而重写errno（因为errno总是保存最后一个错误信息），所以这里我们先进行了保存处理。
            close(fd);  /* This would overwrite our errno. So we saved it. */
            errno = old_errno;
            return -1;
        }
        // 创建异步删除（关闭fd）的任务。
        bioCreateCloseJob(fd);
        return 0; /* Success. */
    }
}

/* ---------------------------------- MASTER -------------------------------- */

// 创建replication backlog缓存，初始化相关结构
void createReplicationBacklog(void) {
    serverAssert(server.repl_backlog == NULL);
    server.repl_backlog = zmalloc(server.repl_backlog_size);
    server.repl_backlog_histlen = 0;
    server.repl_backlog_idx = 0;

    /* We don't have any data inside our buffer, but virtually the first
     * byte we have is the next byte that will be generated for the
     * replication stream. */
    // 我们没有任何数据在缓存中，但是事实上repl_backlog_off指向的是复制流将要生成的下一个字节。
    // 因为小于repl_backlog_off的数据在backlog中都是无效的（被覆盖了），不能用于增量同步。
    server.repl_backlog_off = server.master_repl_offset+1;
}

/* This function is called when the user modifies the replication backlog
 * size at runtime. It is up to the function to both update the
 * server.repl_backlog_size and to resize the buffer and setup it so that
 * it contains the same data as the previous one (possibly less data, but
 * the most recent bytes, or the same data and more free space in case the
 * buffer is enlarged). */
void resizeReplicationBacklog(long long newsize) {
    if (newsize < CONFIG_REPL_BACKLOG_MIN_SIZE)
        newsize = CONFIG_REPL_BACKLOG_MIN_SIZE;
    if (server.repl_backlog_size == newsize) return;

    server.repl_backlog_size = newsize;
    if (server.repl_backlog != NULL) {
        /* What we actually do is to flush the old buffer and realloc a new
         * empty one. It will refill with new data incrementally.
         * The reason is that copying a few gigabytes adds latency and even
         * worse often we need to alloc additional space before freeing the
         * old buffer. */
        zfree(server.repl_backlog);
        server.repl_backlog = zmalloc(server.repl_backlog_size);
        server.repl_backlog_histlen = 0;
        server.repl_backlog_idx = 0;
        /* Next byte we have is... the next since the buffer is empty. */
        server.repl_backlog_off = server.master_repl_offset+1;
    }
}

void freeReplicationBacklog(void) {
    serverAssert(listLength(server.slaves) == 0);
    zfree(server.repl_backlog);
    server.repl_backlog = NULL;
}

/* Add data to the replication backlog.
 * This function also increments the global replication offset stored at
 * server.master_repl_offset, because there is no case where we want to feed
 * the backlog without incrementing the offset. */
// 添加数据到replication backlog。
// 这个函数同时也会增加存储在master_repl_offset中的全局复制offset
// 因为在任何情况下，我们只要添加数据到replication backlog，都需要增加这个offset值。
void feedReplicationBacklog(void *ptr, size_t len) {
    unsigned char *p = ptr;

    // 增加全局复制offset
    server.master_repl_offset += len;

    /* This is a circular buffer, so write as much data we can at every
     * iteration and rewind the "idx" index if we reach the limit. */
    // 复制backlog是一个循环的buffer，所以每次迭代尽可能多的写数据，如果达到了限制则回退到"idx"索引处。
    while(len) {
        // 计算这一轮写操作能够写入backlog的数据量。
        // 先算出当前backlog循环buf剩余可写空间。如果能够写下传入数据，则写入数据大小就是传入数据。否则最多也只能写满backlog。
        size_t thislen = server.repl_backlog_size - server.repl_backlog_idx;
        if (thislen > len) thislen = len;
        // 写入数据
        memcpy(server.repl_backlog+server.repl_backlog_idx,p,thislen);
        // 更新下一次写入位置指针repl_backlog_idx，如果该位置指向了循环buf的结束处，则重置到循环buf开始位置。
        server.repl_backlog_idx += thislen;
        if (server.repl_backlog_idx == server.repl_backlog_size)
            server.repl_backlog_idx = 0;
        // 计算剩余的需要写如backlog的数据量，以及剩余待写数据的起始位置。
        // 如果还有数据没有写完，则继续循环处理。
        len -= thislen;
        p += thislen;
        // 更新backlog中真实数据的大小
        server.repl_backlog_histlen += thislen;
    }
    // 如果真实数据超过了循环buffer的大小，显然数据装满了buffer，这里设置真实数据大小即为buffer大小。
    if (server.repl_backlog_histlen > server.repl_backlog_size)
        server.repl_backlog_histlen = server.repl_backlog_size;
    /* Set the offset of the first byte we have in the backlog. */
    // 设置 repl_backlog_off 为 backlog 中第一个有效字节在全局复制数据中的位置。
    // 这样当某个slave请求复制数据的offset小于这个值，就没办法使用backlog增量传输了，只能RDB全量同步。
    server.repl_backlog_off = server.master_repl_offset -
                              server.repl_backlog_histlen + 1;
}

/* Wrapper for feedReplicationBacklog() that takes Redis string objects
 * as input. */
// 处理redis字符串对象的写入backlog，这个函数是对feedReplicationBacklog函数的封装。
void feedReplicationBacklogWithObject(robj *o) {
    char llstr[LONG_STR_SIZE];
    void *p;
    size_t len;

    // 根据不同的编码，获取字符串对象格式化成字符串后的，长度以及字符串本身。
    if (o->encoding == OBJ_ENCODING_INT) {
        len = ll2string(llstr,sizeof(llstr),(long)o->ptr);
        p = llstr;
    } else {
        len = sdslen(o->ptr);
        p = o->ptr;
    }
    // 写入格式化后的字符串到backlog，参数为待写入字符串和需要写入的数据长度。
    feedReplicationBacklog(p,len);
}

int canFeedReplicaReplBuffer(client *replica) {
    /* Don't feed replicas that only want the RDB. */
    // 如果当前slave client标识时只接收RDB，那么不发送复制buffer中数据。
    if (replica->flags & CLIENT_REPL_RDBONLY) return 0;

    /* Don't feed replicas that are still waiting for BGSAVE to start. */
    // 如果当前slave client正在等待当前节点发送新的RDB数据，此时也不发送复制buffer中数据
    if (replica->replstate == SLAVE_STATE_WAIT_BGSAVE_START) return 0;

    return 1;
}

/* Propagate write commands to slaves, and populate the replication backlog
 * as well. This function is used if the instance is a master: we use
 * the commands received by our clients in order to create the replication
 * stream. Instead if the instance is a slave and has sub-slaves attached,
 * we use replicationFeedSlavesFromMasterStream() */
// 传播写命令给slaves，并同时写入复制backlog中。
// 这个函数用于当前节点是master的时候，master使用client发送过来的写操作命令创建数据复制流。
// 如果节点是slave，且有子slaves关联，我们使用replicationFeedSlavesFromMasterStream()来处理。
void replicationFeedSlaves(list *slaves, int dictid, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j, len;
    char llstr[LONG_STR_SIZE];

    /* If the instance is not a top level master, return ASAP: we'll just proxy
     * the stream of data we receive from our master instead, in order to
     * propagate *identical* replication stream. In this way this slave can
     * advertise the same replication ID as the master (since it shares the
     * master replication history and has the same backlog and offsets). */
    // 如果当前节点不是顶级的master，直接返回，因为我们只是代理从主服务器接收的数据流，来传播“相同”复制流。
    // 这样从节点能够告知和主节点相同的复制id，因为都是共享的主节点的复制历史，以及相同的backlog和offset。
    if (server.masterhost != NULL) return;

    /* If there aren't slaves, and there is no backlog buffer to populate,
     * we can return ASAP. */
    // 如果没有slaves，且没有backlog buffer，直接返回。
    if (server.repl_backlog == NULL && listLength(slaves) == 0) return;

    /* We can't have slaves attached and no backlog. */
    // 因为不可能存在"有slaves但却没有backlog buffer"的情况，这里断言下。
    serverAssert(!(listLength(slaves) != 0 && server.repl_backlog == NULL));

    /* Send SELECT command to every slave if needed. */
    // slaves的当前db不是当前执行命令db时，需要发送select db命令给所有slave。
    if (server.slaveseldb != dictid) {
        robj *selectcmd;

        /* For a few DBs we have pre-computed SELECT command. */
        if (dictid >= 0 && dictid < PROTO_SHARED_SELECT_CMDS) {
            // 对于db在[0, 10)之间时，我们有预写好的select命令直接获取就好了。
            selectcmd = shared.select[dictid];
        } else {
            int dictid_len;

            // dictid转成string，构建select命令的string对象。
            dictid_len = ll2string(llstr,sizeof(llstr),dictid);
            selectcmd = createObject(OBJ_STRING,
                sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, llstr));
        }

        /* Add the SELECT command into the backlog. */
        // 如果有repl_backlog，将select命令加入到backlog中。
        if (server.repl_backlog) feedReplicationBacklogWithObject(selectcmd);

        /* Send it to slaves. */
        // 遍历所有slaves，addReply将命令通过slave的client发送过去。
        listRewind(slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;

            // 如果slave不接受复制数据，跳过该salve
            if (!canFeedReplicaReplBuffer(slave)) continue;
            // 发送数据
            addReply(slave,selectcmd);
        }

        // 这里需要释放掉前面构建的selectcmd对象
        if (dictid < 0 || dictid >= PROTO_SHARED_SELECT_CMDS)
            decrRefCount(selectcmd);
    }
    // 更新服务端视角的全局slave select db。
    server.slaveseldb = dictid;

    /* Write the command to the replication backlog if any. */
    // 如果有repl_backlog，将当前命令构造数据写入replication backlog。
    if (server.repl_backlog) {
        // aux用于存储参数个数，即multi bulk数量，格式："*1\r\n"。
        // 数字转字符串最多LONG_STR_SIZE位，再加上3个格式字符。
        char aux[LONG_STR_SIZE+3];

        /* Add the multi bulk reply length. */
        aux[0] = '*';
        len = ll2string(aux+1,sizeof(aux)-1,argc);
        aux[len+1] = '\r';
        aux[len+2] = '\n';
        // 将参数个数写入backlog，即multi bulk数量
        feedReplicationBacklog(aux,len+3);

        // 针对每个参数，构造一个数据块。字符串长度+字符串本身，每部分"\r\n"结尾。如："$9\r\nchenqiang\r\n"
        for (j = 0; j < argc; j++) {
            // 获取对象的长度，用于后面写入
            long objlen = stringObjectLen(argv[j]);

            /* We need to feed the buffer with the object as a bulk reply
             * not just as a plain string, so create the $..CRLF payload len
             * and add the final CRLF */
            // 处理字符串长度，可以复用前面的aux，因为都是数字，存储需要的最大空间是一样的。
            aux[0] = '$';
            len = ll2string(aux+1,sizeof(aux)-1,objlen);
            aux[len+1] = '\r';
            aux[len+2] = '\n';
            // 写入参数长度部分，即aux
            feedReplicationBacklog(aux,len+3);
            // 写入当前处理的参数字符串对象，即argv[j]
            feedReplicationBacklogWithObject(argv[j]);
            // 写入"\r\n"结束符，即aux最后两个字符。
            feedReplicationBacklog(aux+len+1,2);
        }
    }

    /* Write the command to every slave. */
    // 遍历slave client，将replication backlog数据同步到slave。（跟前面发送select db操作一致）
    listRewind(slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        // 如果当前slave目前不接受增量复制数据，跳过该salve
        if (!canFeedReplicaReplBuffer(slave)) continue;

        /* Feed slaves that are waiting for the initial SYNC (so these commands
         * are queued in the output buffer until the initial SYNC completes),
         * or are already in sync with the master. */
        // 数据同步时，slave先发送SYNC命令，SYNC初始化完成后才会开始发送数据。
        // 在初始化SYNC期间，这些命令会先缓存到client的output buf或reply列表中。

        /* Add the multi bulk length. */
        // 数据格式化处理其实跟前面写入backlog是一致的。
        // 写入传输命令的multi bulk个数，格式："*<value>\r\n"
        addReplyArrayLen(slave,argc);

        /* Finally any additional argument that was not stored inside the
         * static buffer if any (from j to argc). */
        // 对于每个参数构造bulk加入回复buf或reply列表。
        // 每个bulk格式是3块字符串拼接："$9\r\n" + "chenqiang" + "\r\n"
        for (j = 0; j < argc; j++)
            addReplyBulk(slave,argv[j]);
    }
}

/* This is a debugging function that gets called when we detect something
 * wrong with the replication protocol: the goal is to peek into the
 * replication backlog and show a few final bytes to make simpler to
 * guess what kind of bug it could be. */
void showLatestBacklog(void) {
    if (server.repl_backlog == NULL) return;

    long long dumplen = 256;
    if (server.repl_backlog_histlen < dumplen)
        dumplen = server.repl_backlog_histlen;

    /* Identify the first byte to dump. */
    long long idx =
      (server.repl_backlog_idx + (server.repl_backlog_size - dumplen)) %
       server.repl_backlog_size;

    /* Scan the circular buffer to collect 'dumplen' bytes. */
    sds dump = sdsempty();
    while(dumplen) {
        long long thislen =
            ((server.repl_backlog_size - idx) < dumplen) ?
            (server.repl_backlog_size - idx) : dumplen;

        dump = sdscatrepr(dump,server.repl_backlog+idx,thislen);
        dumplen -= thislen;
        idx = 0;
    }

    /* Finally log such bytes: this is vital debugging info to
     * understand what happened. */
    serverLog(LL_WARNING,"Latest backlog is: '%s'", dump);
    sdsfree(dump);
}

/* This function is used in order to proxy what we receive from our master
 * to our sub-slaves. */
#include <ctype.h>
void replicationFeedSlavesFromMasterStream(list *slaves, char *buf, size_t buflen) {
    listNode *ln;
    listIter li;

    /* Debugging: this is handy to see the stream sent from master
     * to slaves. Disabled with if(0). */
    if (0) {
        printf("%zu:",buflen);
        for (size_t j = 0; j < buflen; j++) {
            printf("%c", isprint(buf[j]) ? buf[j] : '.');
        }
        printf("\n");
    }

    if (server.repl_backlog) feedReplicationBacklog(buf,buflen);
    listRewind(slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        if (!canFeedReplicaReplBuffer(slave)) continue;
        addReplyProto(slave,buf,buflen);
    }
}

void replicationFeedMonitors(client *c, list *monitors, int dictid, robj **argv, int argc) {
    if (!(listLength(server.monitors) && !server.loading)) return;
    listNode *ln;
    listIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    struct timeval tv;

    gettimeofday(&tv,NULL);
    cmdrepr = sdscatprintf(cmdrepr,"%ld.%06ld ",(long)tv.tv_sec,(long)tv.tv_usec);
    if (c->flags & CLIENT_LUA) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d lua] ",dictid);
    } else if (c->flags & CLIENT_UNIX_SOCKET) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d unix:%s] ",dictid,server.unixsocket);
    } else {
        cmdrepr = sdscatprintf(cmdrepr,"[%d %s] ",dictid,getClientPeerId(c));
    }

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == OBJ_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)argv[j]->ptr,
                        sdslen(argv[j]->ptr));
        }
        if (j != argc-1)
            cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    cmdrepr = sdscatlen(cmdrepr,"\r\n",2);
    cmdobj = createObject(OBJ_STRING,cmdrepr);

    listRewind(monitors,&li);
    while((ln = listNext(&li))) {
        client *monitor = ln->value;
        addReply(monitor,cmdobj);
    }
    decrRefCount(cmdobj);
}

/* Feed the slave 'c' with the replication backlog starting from the
 * specified 'offset' up to the end of the backlog. */
// 向slave client发送backlog中数据，从指定的offset开始，不超过backlog结束位置。
long long addReplyReplicationBacklog(client *c, long long offset) {
    long long j, skip, len;

    serverLog(LL_DEBUG, "[PSYNC] Replica request offset: %lld", offset);

    // 如果backlog中没有新的数据，返回。
    if (server.repl_backlog_histlen == 0) {
        serverLog(LL_DEBUG, "[PSYNC] Backlog history len is zero");
        return 0;
    }

    serverLog(LL_DEBUG, "[PSYNC] Backlog size: %lld",
             server.repl_backlog_size);
    serverLog(LL_DEBUG, "[PSYNC] First byte: %lld",
             server.repl_backlog_off);
    serverLog(LL_DEBUG, "[PSYNC] History len: %lld",
             server.repl_backlog_histlen);
    serverLog(LL_DEBUG, "[PSYNC] Current index: %lld",
             server.repl_backlog_idx);

    /* Compute the amount of bytes we need to discard. */
    // 计算backlog中最早可同步的数据偏移（skip肯定大于0）。后面计算起始位置j，加上这个skip即为数据同步位置。
    skip = offset - server.repl_backlog_off;
    serverLog(LL_DEBUG, "[PSYNC] Skipping: %lld", skip);

    /* Point j to the oldest byte, that is actually our
     * server.repl_backlog_off byte. */
    // j指向当前backlog中最老的可用于复制的数据。即repl_backlog_idx-repl_backlog_histlen，当前位置减去数据总量。
    // 因为backlog是循环的buffer，直接减可能为负，所以我们加上buffer总大小repl_backlog_size然后对它取余。
    // j实际指向repl_backlog_off在backlog中的位置。
    // 我们只知道backlog有效起始位置的复制offset，并不知道这个位置在哪里，所以这里这样计算j。
    j = (server.repl_backlog_idx +
        (server.repl_backlog_size-server.repl_backlog_histlen)) %
        server.repl_backlog_size;
    serverLog(LL_DEBUG, "[PSYNC] Index of first byte: %lld", j);

    /* Discard the amount of data to seek to the specified 'offset'. */
    // 计算我们需要发送数据的offset，即在backlog中的位置。
    j = (j + skip) % server.repl_backlog_size;

    /* Feed slave with data. Since it is a circular buffer we have to
     * split the reply in two parts if we are cross-boundary. */
    // 发送数据到slave。因为backlog是循环buffer，如果数据跨边界的话，我们这里分两次发送。
    len = server.repl_backlog_histlen - skip;
    serverLog(LL_DEBUG, "[PSYNC] Reply total length: %lld", len);
    while(len) {
        // 如果跨了边界，则我们最长只发送 server.repl_backlog_size-j 的数据。没有跨边界，则一次发送len数据。
        long long thislen =
            ((server.repl_backlog_size - j) < len) ?
            (server.repl_backlog_size - j) : len;

        serverLog(LL_DEBUG, "[PSYNC] addReply() length: %lld", thislen);
        // 发送
        addReplySds(c,sdsnewlen(server.repl_backlog + j, thislen));
        // 显然如果没发送完，len>0接着循环继续发送，j=0表示下次数据从buffer首位开始。
        len -= thislen;
        j = 0;
    }
    // 返回我们发送的数据量。
    return server.repl_backlog_histlen - skip;
}

/* Return the offset to provide as reply to the PSYNC command received
 * from the slave. The returned value is only valid immediately after
 * the BGSAVE process started and before executing any other command
 * from clients. */
// 返回offset用来回复slave请求的PSYNC命令。返回值仅在BGSAVE进程启动后，且在执行客户端任何其他命令之前有效。
// 因为BGSAVE copy on write，后面执行命令会导致该值变化，与子进程不一致，从而增量数据与全量数据接不上。
long long getPsyncInitialOffset(void) {
    return server.master_repl_offset;
}

/* Send a FULLRESYNC reply in the specific case of a full resynchronization,
 * as a side effect setup the slave for a full sync in different ways:
 *
 * 1) Remember, into the slave client structure, the replication offset
 *    we sent here, so that if new slaves will later attach to the same
 *    background RDB saving process (by duplicating this client output
 *    buffer), we can get the right offset from this slave.
 * 2) Set the replication state of the slave to WAIT_BGSAVE_END so that
 *    we start accumulating differences from this point.
 * 3) Force the replication stream to re-emit a SELECT statement so
 *    the new slave incremental differences will start selecting the
 *    right database number.
 *
 * Normally this function should be called immediately after a successful
 * BGSAVE for replication was started, or when there is one already in
 * progress that we attached our slave to. */
// 在完全重同步的特定情况下发送FULLRESYNC回复，另外函数会设置slave的全量同步方式（WAIT_BGSAVE_END）。
//  1、设置psync_initial_offset，这样再有新的slave也关联到当前BGSAVE，可以保证设置正确的offset。
//  2、设置复制状态为WAIT_BGSAVE_END，从而会累计增量diff到buffer中。
//  3、强制复制流重新发出SELECT语句，以便新slave传输增量差异时先select选择db。
// 通常，这个函数应该在一个用于复制传输的BGSAVE成功启动后立即调用，或者slave附加到一个已经启动的用于复制传输的BGSAVE时调用。
int replicationSetupSlaveForFullResync(client *slave, long long offset) {
    char buf[128];
    int buflen;

    // 设置offset和复制状态
    slave->psync_initial_offset = offset;
    slave->replstate = SLAVE_STATE_WAIT_BGSAVE_END;
    /* We are going to accumulate the incremental changes for this
     * slave as well. Set slaveseldb to -1 in order to force to re-emit
     * a SELECT statement in the replication stream. */
    // 我们要开始增量缓存数据了，设置slaveseldb为-1强制在开始数据流传输前加上select db操作。
    server.slaveseldb = -1;

    /* Don't send this reply to slaves that approached us with
     * the old SYNC command. */
    // 如果是使用的老的协议，我们不回复"+FULLRESYNC"给slave。
    if (!(slave->flags & CLIENT_PRE_PSYNC)) {
        buflen = snprintf(buf,sizeof(buf),"+FULLRESYNC %s %lld\r\n",
                          server.replid,offset);
        if (connWrite(slave->conn,buf,buflen) != buflen) {
            freeClientAsync(slave);
            return C_ERR;
        }
    }
    return C_OK;
}

/* This function handles the PSYNC command from the point of view of a
 * master receiving a request for partial resynchronization.
 *
 * On success return C_OK, otherwise C_ERR is returned and we proceed
 * with the usual full resync. */
// 这个函数从master视角来处理部分重同步PSYNC命令。成功返回ok，否则返回err表示需要进行全量重同步。
int masterTryPartialResynchronization(client *c) {
    long long psync_offset, psync_len;
    char *master_replid = c->argv[1]->ptr;
    char buf[128];
    int buflen;

    /* Parse the replication offset asked by the slave. Go to full sync
     * on parse error: this should never happen but we try to handle
     * it in a robust way compared to aborting. */
    // 解析slave请求的复制offset。如果解析失败则跳到full sync，当然这种情况应该不可能出现，不过为了可靠性，我们这样来处理。
    if (getLongLongFromObjectOrReply(c,c->argv[2],&psync_offset,NULL) !=
       C_OK) goto need_full_resync;

    /* Is the replication ID of this master the same advertised by the wannabe
     * slave via PSYNC? If the replication ID changed this master has a
     * different replication history, and there is no way to continue.
     *
     * Note that there are two potentially valid replication IDs: the ID1
     * and the ID2. The ID2 however is only valid up to a specific offset. */
    // 检查命令传的复制id是否与我们服务当前replid是否一致。
    // 如果不一致，可能服务当前是新的复制历史数据，从而生成了新的replid，导致不能继续部分重同步了。
    // 注意：有两个潜在有效的复制id：ID1和ID2，ID2仅当offset在指定的offset值以内时有效。
    if (strcasecmp(master_replid, server.replid) &&
        (strcasecmp(master_replid, server.replid2) ||
         psync_offset > server.second_replid_offset))
    {
        // 进入当前if，表示replid无效。要么与ID1和ID2都不匹配，要么与ID2匹配但是offset不在指定偏移以内。
        /* Replid "?" is used by slaves that want to force a full resync. */
        // 如果slave指定replid为'?'，表示想要强制进行全量重同步。
        if (master_replid[0] != '?') {
            if (strcasecmp(master_replid, server.replid) &&
                strcasecmp(master_replid, server.replid2))
            {
                serverLog(LL_NOTICE,"Partial resynchronization not accepted: "
                    "Replication ID mismatch (Replica asked for '%s', my "
                    "replication IDs are '%s' and '%s')",
                    master_replid, server.replid, server.replid2);
            } else {
                serverLog(LL_NOTICE,"Partial resynchronization not accepted: "
                    "Requested offset for second ID was %lld, but I can reply "
                    "up to %lld", psync_offset, server.second_replid_offset);
            }
        } else {
            serverLog(LL_NOTICE,"Full resync requested by replica %s",
                replicationGetSlaveName(c));
        }
        goto need_full_resync;
    }

    /* We still have the data our slave is asking for? */
    // 检查我们backlog中是否有slave请求的数据。
    if (!server.repl_backlog ||
        psync_offset < server.repl_backlog_off ||
        psync_offset > (server.repl_backlog_off + server.repl_backlog_histlen))
    {
        // 如果backlog为NULL，或者请求的偏移量不在backlog中，显然我们只能进行全量重同步了。
        serverLog(LL_NOTICE,
            "Unable to partial resync with replica %s for lack of backlog (Replica request was: %lld).", replicationGetSlaveName(c), psync_offset);
        if (psync_offset > server.master_repl_offset) {
            serverLog(LL_WARNING,
                "Warning: replica %s tried to PSYNC with an offset that is greater than the master replication offset.", replicationGetSlaveName(c));
        }
        goto need_full_resync;
    }

    /* If we reached this point, we are able to perform a partial resync:
     * 1) Set client state to make it a slave.
     * 2) Inform the client we can continue with +CONTINUE
     * 3) Send the backlog data (from the offset to the end) to the slave. */
    // 如果执行到这里，说明我们可能进行部分重同步了。
    // 1、设置client的状态，标记为slave。
    // 2、通知client（发生+CONTINUE），可以继续部分同步了。
    // 3、发生backlog数据（从指定offset起，所有数据）给slave。
    c->flags |= CLIENT_SLAVE;
    c->replstate = SLAVE_STATE_ONLINE;
    c->repl_ack_time = server.unixtime;
    c->repl_put_online_on_ack = 0;
    listAddNodeTail(server.slaves,c);
    /* We can't use the connection buffers since they are used to accumulate
     * new commands at this stage. But we are sure the socket send buffer is
     * empty so this write will never fail actually. */
    // 我们不能使用conn的buffers，因为它们在目前阶段用于缓存待发送的新的命令。
    // 但是我们确认socket的发送缓冲是空的，所以这里写事实上将永不会失败。
    if (c->slave_capa & SLAVE_CAPA_PSYNC2) {
        buflen = snprintf(buf,sizeof(buf),"+CONTINUE %s\r\n", server.replid);
    } else {
        buflen = snprintf(buf,sizeof(buf),"+CONTINUE\r\n");
    }
    if (connWrite(c->conn,buf,buflen) != buflen) {
        freeClientAsync(c);
        return C_OK;
    }
    // 回复psync数据到slave client
    psync_len = addReplyReplicationBacklog(c,psync_offset);
    serverLog(LL_NOTICE,
        "Partial resynchronization request from %s accepted. Sending %lld bytes of backlog starting from offset %lld.",
            replicationGetSlaveName(c),
            psync_len, psync_offset);
    /* Note that we don't need to set the selected DB at server.slaveseldb
     * to -1 to force the master to emit SELECT, since the slave already
     * has this state from the previous connection with the master. */
    // 注意：我们这里不需要将server.slaveseldb中指定的DB设置为-1来强制主服务器发送SELECT
    // 因为从服务器在与主服务器的之前连接中已经获取了此状态。

    // 更新当前服务的好的slaves数量。
    refreshGoodSlavesCount();

    /* Fire the replica change modules event. */
    // slave状态变更为online，事件通知module
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);

    // 不需要全量重同步，返回ok
    return C_OK; /* The caller can return, no full resync needed. */

need_full_resync:
    /* We need a full resync for some reason... Note that we can't
     * reply to PSYNC right now if a full SYNC is needed. The reply
     * must include the master offset at the time the RDB file we transfer
     * is generated, so we need to delay the reply to that moment. */
    // 由于某种原因，我们需要进行全量重同步，返回err。
    // 注意：如果需要进行全量同步的话，我们现在还不能回复PSYNC。因为回复必须包含master offset，
    // 而这个offset实际对应我们RDB文件生成时的offset，因为我们要先发送RDB文件，然后才接着进行部分增量同步。
    // 所以我们推迟这个回复，知道我们全量同步的RDB文件生成好时，再回复。
    return C_ERR;
}

/* Start a BGSAVE for replication goals, which is, selecting the disk or
 * socket target depending on the configuration, and making sure that
 * the script cache is flushed before to start.
 *
 * The mincapa argument is the bitwise AND among all the slaves capabilities
 * of the slaves waiting for this BGSAVE, so represents the slave capabilities
 * all the slaves support. Can be tested via SLAVE_CAPA_* macros.
 *
 * Side effects, other than starting a BGSAVE:
 *
 * 1) Handle the slaves in WAIT_START state, by preparing them for a full
 *    sync if the BGSAVE was successfully started, or sending them an error
 *    and dropping them from the list of slaves.
 *
 * 2) Flush the Lua scripting script cache if the BGSAVE was actually
 *    started.
 *
 * Returns C_OK on success or C_ERR otherwise. */
// 启动BGSAVE来实现数据复制同步，使用磁盘存储还是不落盘socket直接传输取决于当前server的配置，另外确保启动前清空了脚本缓存。
// mincapa 表示当前等待BGSAVE同步复制数据的salves都支持的传输协议，具体见SLAVE_CAPA_*宏定义。
// 除了开启BGSAVE外的副作用：
//  1、处理处于WAIT_START状态的slaves。
//      如果BGSAVE成功启动，则准备好进行完全同步（设置slaves相关状态）；如果启动失败，则回复它们err信息，并将它们从slaves中移除。
//  2、当BGSAVE成功启动，需要清空Lua脚本缓存。
int startBgsaveForReplication(int mincapa) {
    int retval;
    // 如果支持，优先选用无盘传输
    int socket_target = server.repl_diskless_sync && (mincapa & SLAVE_CAPA_EOF);
    listIter li;
    listNode *ln;

    serverLog(LL_NOTICE,"Starting BGSAVE for SYNC with target: %s",
        socket_target ? "replicas sockets" : "disk");

    rdbSaveInfo rsi, *rsiptr;
    // 填充复制信息
    rsiptr = rdbPopulateSaveInfo(&rsi);
    /* Only do rdbSave* when rsiptr is not NULL,
     * otherwise slave will miss repl-stream-db. */
    // 只有在rsiptr非NULL时才处理rdbSave*，否则rsiptr为NULL，slave将取不到repl-stream-db，后面stream传的部分数据会db错乱。
    if (rsiptr) {
        if (socket_target)
            // 不存盘，socket target
            retval = rdbSaveToSlavesSockets(rsiptr);
        else
            // 存盘，file target
            retval = rdbSaveBackground(server.rdb_filename,rsiptr);
    } else {
        serverLog(LL_WARNING,"BGSAVE for replication: replication information not available, can't generate the RDB file right now. Try later.");
        retval = C_ERR;
    }

    /* If we succeeded to start a BGSAVE with disk target, let's remember
     * this fact, so that we can later delete the file if needed. Note
     * that we don't set the flag to 1 if the feature is disabled, otherwise
     * it would never be cleared: the file is not deleted. This way if
     * the user enables it later with CONFIG SET, we are fine. */
    // 如果我们成功的启动了一个存盘的BGSAVE来复制，且配置的是复制后移除文件，这里我们做个标记，后面用于将RDB文件删除。
    // 注意如果该功能被禁用（即rdb_del_sync_files为0），我们不会设置标识为1，从而文件不会被删除。
    // 可以通过CONFIG SET来在运行时设置rdb_del_sync_files的值，从而可以仍然可以在replicationCron中处理删除。
    if (retval == C_OK && !socket_target && server.rdb_del_sync_files)
        RDBGeneratedByReplication = 1;

    /* If we failed to BGSAVE, remove the slaves waiting for a full
     * resynchronization from the list of slaves, inform them with
     * an error about what happened, close the connection ASAP. */
    // 如果我们BGSAVE失败，将等待全量重同步的slaves从slaves列表中移除，并通知他们发生错误，然后关闭连接。
    if (retval == C_ERR) {
        serverLog(LL_WARNING,"BGSAVE for replication failed");
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;

            // BGSAVE失败，遍历slaves，如果是在等待BGSAVE_START，移除该slave client。
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                slave->replstate = REPL_STATE_NONE;
                slave->flags &= ~CLIENT_SLAVE;
                listDelNode(server.slaves,ln);
                addReplyError(slave,
                    "BGSAVE failed, replication can't continue");
                slave->flags |= CLIENT_CLOSE_AFTER_REPLY;
            }
        }
        return retval;
    }

    /* If the target is socket, rdbSaveToSlavesSockets() already setup
     * the slaves for a full resync. Otherwise for disk target do it now.*/
    // 如果RDB不存盘使用socket传输，rdbSaveToSlavesSockets()已经处理好了slaves连接offset和状态等更新，从而可以直接进行全量重同步。
    // 如果是先存磁盘，前面rdbSaveBackground()已经存好文件，这里需要设置好slave传输处理配置，从而进行数据同步。
    if (!socket_target) {
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;

            // 遍历slaves，设置slaves同步信息。
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                    replicationSetupSlaveForFullResync(slave,
                            getPsyncInitialOffset());
            }
        }
    }

    /* Flush the script cache, since we need that slave differences are
     * accumulated without requiring slaves to match our cached scripts. */
    // 清空脚本缓存，因为后面我们传播增量diff数据，执行脚本命令时，可能从服务器没有加载这个脚本导致无法执行。
    // https://blog.csdn.net/qq_40594696/article/details/105628349
    // 当主服务器成功在本机执行完一个EVALSHA命令之后，它将根据EVALSHA命令指定的SHA1校验和是否存在于repl_scriptcache_dict字典，
    // 来决定事项从服务器传播EVALSHA命令还是EVAL命令：
    //  1）存在，传播EVALSHA命令
    //  2）不存在，将EVALSHA转化成EVAL，然后传播EVAL，并将EVALSHA指定的SHA1校验和添加到repl_scriptcache_dict字典
    if (retval == C_OK) replicationScriptCacheFlush();
    return retval;
}

/* SYNC and PSYNC command implementation. */
// SYNC 和 PSYNC 命令实现
void syncCommand(client *c) {
    /* ignore SYNC if already slave or in monitor mode */
    // 如果当前client已经是slave或monitor模式，我们忽略SYNC指令。
    if (c->flags & CLIENT_SLAVE) return;

    /* Check if this is a failover request to a replica with the same replid and
     * become a master if so. */
    // 检查当前是否是failover使用相同的replid请求复制同步，如果是则设置当前节点为master
    if (c->argc > 3 && !strcasecmp(c->argv[0]->ptr,"psync") && 
        !strcasecmp(c->argv[3]->ptr,"failover"))
    {
        serverLog(LL_WARNING, "Failover request received for replid %s.",
            (unsigned char *)c->argv[1]->ptr);
        // 当前节点是master，PSYNC FAILOVER不能指定master来进行故障转移
        if (!server.masterhost) {
            addReplyError(c, "PSYNC FAILOVER can't be sent to a master.");
            return;
        }

        if (!strcasecmp(c->argv[1]->ptr,server.replid)) {
            // 如果指定的复制ID与当前服务一致，那么设置当前节点为master
            replicationUnsetMaster();
            sds client = catClientInfoString(sdsempty(),c);
            serverLog(LL_NOTICE,
                "MASTER MODE enabled (failover request from '%s')",client);
            sdsfree(client);
        } else {
            addReplyError(c, "PSYNC FAILOVER replid must match my replid.");
            return;            
        }
    }

    /* Don't let replicas sync with us while we're failing over */
    // 当我们正在进行故障转移时，不能处理SYNC
    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c,"-NOMASTERLINK Can't SYNC while failing over");
        return;
    }

    /* Refuse SYNC requests if we are a slave but the link with our master
     * is not ok... */
    // 如果我们是slave，但是与master的连接还没有建立好，暂时不能处理SYNC
    if (server.masterhost && server.repl_state != REPL_STATE_CONNECTED) {
        addReplyError(c,"-NOMASTERLINK Can't SYNC while not connected with my master");
        return;
    }

    /* SYNC can't be issued when the server has pending data to send to
     * the client about already issued commands. We need a fresh reply
     * buffer registering the differences between the BGSAVE and the current
     * dataset, so that we can copy to other slaves if needed. */
    // 当服务器有该client执行命令的结果等待回复时，我们不能处理SYNC。
    // 我们需要一个新的回复缓冲区来记录BGSAVE和当前数据集之间的差异，以便我们在需要时复制给其他slaves。
    if (clientHasPendingReplies(c)) {
        addReplyError(c,"SYNC and PSYNC are invalid with pending output");
        return;
    }

    serverLog(LL_NOTICE,"Replica %s asks for synchronization",
        replicationGetSlaveName(c));

    /* Try a partial resynchronization if this is a PSYNC command.
     * If it fails, we continue with usual full resynchronization, however
     * when this happens masterTryPartialResynchronization() already
     * replied with:
     *
     * +FULLRESYNC <replid> <offset>
     *
     * So the slave knows the new replid and offset to try a PSYNC later
     * if the connection with the master is lost. */
    // 如果是PSYNC命令，尝试进行部分重同步。如果失败，再使用全量重同步处理。
    // 失败时，masterTryPartialResynchronization()会回复+FULLRESYNC <replid> <offset>，通知slave使用全量重同步。
    // 通过该指令，slave在与master断开连接后进行重连时，就可以知道新的replid 和 offset来进行PSYNC操作。
    if (!strcasecmp(c->argv[0]->ptr,"psync")) {
        if (masterTryPartialResynchronization(c) == C_OK) {
            server.stat_sync_partial_ok++;
            // psync处理完成，不需要全量重同步，直接返回。
            return; /* No full resync needed, return. */
        } else {
            char *master_replid = c->argv[1]->ptr;

            /* Increment stats for failed PSYNCs, but only if the
             * replid is not "?", as this is used by slaves to force a full
             * resync on purpose when they are not albe to partially
             * resync. */
            // 更新psync err数。'?'不算作err，因为这个是slave不支持psync，而强制要求全量重同步。
            if (master_replid[0] != '?') server.stat_sync_partial_err++;
        }
    } else {
        /* If a slave uses SYNC, we are dealing with an old implementation
         * of the replication protocol (like redis-cli --slave). Flag the client
         * so that we don't expect to receive REPLCONF ACK feedbacks. */
        // 如果slave使用的是SYNC命令，我们正在处理旧的复制协议实现。
        // 设置client的 CLIENT_PRE_PSYNC标识，表示我们不希望收到REPLCONF ACK回复。
        c->flags |= CLIENT_PRE_PSYNC;
    }

    /* Full resynchronization. */
    // 全量重同步
    server.stat_sync_full++;

    /* Setup the slave as one waiting for BGSAVE to start. The following code
     * paths will change the state if we handle the slave differently. */
    // 设置slave的复制状态为等待BGSAVE启动。后面会根据我们处理slave数据同步方式的不同，来改变这个状态。
    c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
    if (server.repl_disable_tcp_nodelay)
        // 禁止nodelay，禁止小包传输
        connDisableTcpNoDelay(c->conn); /* Non critical if it fails. */
    c->repldbfd = -1;
    c->flags |= CLIENT_SLAVE;
    listAddNodeTail(server.slaves,c);

    /* Create the replication backlog if needed. */
    // 如果首次有slave加入，且backlog为NULL的话，这里要创建backlog缓冲区。
    if (listLength(server.slaves) == 1 && server.repl_backlog == NULL) {
        /* When we create the backlog from scratch, we always use a new
         * replication ID and clear the ID2, since there is no valid
         * past history. */
        // 当我们新创建backlog时，我们总是使用一个新的复制ID，并清除掉ID2，因为在这之前没有有效的历史数据同步。
        changeReplicationId();
        clearReplicationId2();
        createReplicationBacklog();
        serverLog(LL_NOTICE,"Replication backlog created, my new "
                            "replication IDs are '%s' and '%s'",
                            server.replid, server.replid2);
    }

    /* CASE 1: BGSAVE is in progress, with disk target. */
    // 情形1：有子进程正在进行BGSAVE，且是落盘处理。
    if (server.child_type == CHILD_TYPE_RDB &&
        server.rdb_child_type == RDB_CHILD_TYPE_DISK)
    {
        /* Ok a background save is in progress. Let's check if it is a good
         * one for replication, i.e. if there is another slave that is
         * registering differences since the server forked to save. */
        // 当前有后台RDB处理存盘操作，我们需要检查它是否能够用于这次的数据同步。
        // 比如对应client是否有使用缓冲区来传输增量数据，是否传输协议一致等。
        client *slave;
        listNode *ln;
        listIter li;

        // 遍历slaves列表检查
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            slave = ln->value;
            /* If the client needs a buffer of commands, we can't use
             * a replica without replication buffer. */
            // 只有两种情况我们可以复用当前后台BGSAVE的文件：
            // 1、后台BGSAVE用于传输的slave，不是采用的RDBONLY方式获取数据。（说明除了RDB文件外，buffer也会缓存命令）
            //      那么对于当前的SYNC同步，不管是不是RDBONLY，我们都可以获取完整数据，所以显然是可以复用的。
            // 2、后台BGSAVE传输对应的slave，是RDBONLY获取（没有使用buffer存储增量数据），且当前slave也是RDBONLY。
            //      都是只同步RDB文件，不用管buffer新增数据，所以也可以复用。
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
                (!(slave->flags & CLIENT_REPL_RDBONLY) ||
                 (c->flags & CLIENT_REPL_RDBONLY)))
                break;
        }
        /* To attach this slave, we check that it has at least all the
         * capabilities of the slave that triggered the current BGSAVE. */
        // 如果找到了可以复用当前BGSAVE文件的slave，这里需要判断这个client是否完全兼容当前BGSAVE对应slave的复制协议。
        if (ln && ((c->slave_capa & slave->slave_capa) == slave->slave_capa)) {
            /* Perfect, the server is already registering differences for
             * another slave. Set the right state, and copy the buffer.
             * We don't copy buffer if clients don't want. */
            // 到这里，说明我们可以复用当前的BGSAVE。
            // 如果当前client不是RDBONLY，显然当前BGSAVE的slave也不是RDBONLY，那么我们可以把增量数据跟当前client同步一份。
            if (!(c->flags & CLIENT_REPL_RDBONLY)) copyClientOutputBuffer(c,slave);
            // 将当前client状态设置为等待BGSAVE结束
            replicationSetupSlaveForFullResync(c,slave->psync_initial_offset);
            serverLog(LL_NOTICE,"Waiting for end of BGSAVE for SYNC");
        } else {
            /* No way, we need to wait for the next BGSAVE in order to
             * register differences. */
            // 当前BGSAVE不可复用，我们等待下一次BGSAVE处理。
            serverLog(LL_NOTICE,"Can't attach the replica to the current BGSAVE. Waiting for next BGSAVE for SYNC");
        }

    /* CASE 2: BGSAVE is in progress, with socket target. */
    // 情形2：有一个进程在做BGSAVE，但是是不存盘，socket传输。我们需要等待这个传输完成再启动新的BGSAVE处理。
    } else if (server.child_type == CHILD_TYPE_RDB &&
               server.rdb_child_type == RDB_CHILD_TYPE_SOCKET)
    {
        /* There is an RDB child process but it is writing directly to
         * children sockets. We need to wait for the next BGSAVE
         * in order to synchronize. */
        // 当前有RDB子进程，但是是写socket传输，我们需要等待下一次的BGSAVE来同步。
        serverLog(LL_NOTICE,"Current BGSAVE has socket target. Waiting for next BGSAVE for SYNC");

    /* CASE 3: There is no BGSAVE is progress. */
    // 情形3：没有在进行BGSAVE。
    } else {
        if (server.repl_diskless_sync && (c->slave_capa & SLAVE_CAPA_EOF) &&
            server.repl_diskless_sync_delay)
        {
            /* Diskless replication RDB child is created inside
             * replicationCron() since we want to delay its start a
             * few seconds to wait for more slaves to arrive. */
            // 因为设置了delay值，这里我们希望延迟几秒启动，从而等待更多slave到达（未超时）。
            // 所以这种情况的无盘复制RDB子进程在 replicationCron() 中创建处理。
            serverLog(LL_NOTICE,"Delay next BGSAVE for diskless SYNC");
        } else {
            /* We don't have a BGSAVE in progress, let's start one. Diskless
             * or disk-based mode is determined by replica's capacity. */
            // 这里需要开启BGSAVE进程处理。是否无盘传输取决于slave的capacity。
            if (!hasActiveChildProcess()) {
                startBgsaveForReplication(c->slave_capa);
            } else {
                serverLog(LL_NOTICE,
                    "No BGSAVE in progress, but another BG operation is active. "
                    "BGSAVE for replication delayed");
            }
        }
    }
    return;
}

/* REPLCONF <option> <value> <option> <value> ...
 * This command is used by a replica in order to configure the replication
 * process before starting it with the SYNC command.
 * This command is also used by a master in order to get the replication
 * offset from a replica.
 *
 * Currently we support these options:
 *
 * - listening-port <port>
 * - ip-address <ip>
 * What is the listening ip and port of the Replica redis instance, so that
 * the master can accurately lists replicas and their listening ports in the
 * INFO output.
 *
 * - capa <eof|psync2>
 * What is the capabilities of this instance.
 * eof: supports EOF-style RDB transfer for diskless replication.
 * psync2: supports PSYNC v2, so understands +CONTINUE <new repl ID>.
 *
 * - ack <offset>
 * Replica informs the master the amount of replication stream that it
 * processed so far.
 *
 * - getack
 * Unlike other subcommands, this is used by master to get the replication
 * offset from a replica.
 *
 * - rdb-only
 * Only wants RDB snapshot without replication buffer. */
// REPLCONF <option> <value> <option> <value> ... 命令执行函数
// 这个命令通常用于slave在开始SYNC同步之前对数据复制的选项配置操作。也用于master从slave获取复制offset。
// 目前支持这些复制选项：
//  - listening-port <port>
//  - ip-address <ip>
//      表示slave的监听ip和端口。用于master在INFO命令中准确展示slaves的相关信息。
//  - capa <eof|psync2>
//      表示slave支持什么传输方式。eof支持EOF-style的RDB无盘传输；psync2支持PSYNC v2，即支持+CONTINUE <new repl ID>命令。
//  - ack <offset>
//      slave告知master当前所处理的复制数据流的offset。
//  - getack
//      不同于其他的子命令，这个用于master主动从slave获取复制offset。
//  - rdb-only
//      只想要RDB快照，不需要复制缓冲增量同步。
void replconfCommand(client *c) {
    int j;

    if ((c->argc % 2) == 0) {
        /* Number of arguments must be odd to make sure that every
         * option has a corresponding value. */
        // 命令的数量必须是奇数，保证每个选项都有一个相应的值。
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* Process every option-value pair. */
    // 处理每一对 选项-值。
    for (j = 1; j < c->argc; j+=2) {
        if (!strcasecmp(c->argv[j]->ptr,"listening-port")) {
            long port;

            // 解析参数为Long整数，即为port
            if ((getLongFromObjectOrReply(c,c->argv[j+1],
                    &port,NULL) != C_OK))
                return;
            c->slave_listening_port = port;
        } else if (!strcasecmp(c->argv[j]->ptr,"ip-address")) {
            sds addr = c->argv[j+1]->ptr;
            // 释放原来的addr，写入新的
            if (sdslen(addr) < NET_HOST_STR_LEN) {
                if (c->slave_addr) sdsfree(c->slave_addr);
                c->slave_addr = sdsdup(addr);
            } else {
                addReplyErrorFormat(c,"REPLCONF ip-address provided by "
                    "replica instance is too long: %zd bytes", sdslen(addr));
                return;
            }
        } else if (!strcasecmp(c->argv[j]->ptr,"capa")) {
            /* Ignore capabilities not understood by this master. */
            // 设置slave的capa，如果master不识别则忽略。
            if (!strcasecmp(c->argv[j+1]->ptr,"eof"))
                c->slave_capa |= SLAVE_CAPA_EOF;
            else if (!strcasecmp(c->argv[j+1]->ptr,"psync2"))
                c->slave_capa |= SLAVE_CAPA_PSYNC2;
        } else if (!strcasecmp(c->argv[j]->ptr,"ack")) {
            /* REPLCONF ACK is used by slave to inform the master the amount
             * of replication stream that it processed so far. It is an
             * internal only command that normal clients should never use. */
            // REPLCONF ACK 用于slave通知master当前总的处理复制流的offset。只用于内部命令，普通client永远不应使用。
            long long offset;

            // 如果不是slave连接，不应该执行这个命令，直接返回
            if (!(c->flags & CLIENT_SLAVE)) return;
            // 从参数中获取offset值
            if ((getLongLongFromObject(c->argv[j+1], &offset) != C_OK))
                return;
            // 更新client中的复制ack的offset和时间。
            if (offset > c->repl_ack_off)
                c->repl_ack_off = offset;
            c->repl_ack_time = server.unixtime;
            /* If this was a diskless replication, we need to really put
             * the slave online when the first ACK is received (which
             * confirms slave is online and ready to get more data). This
             * allows for simpler and less CPU intensive EOF detection
             * when streaming RDB files.
             * There's a chance the ACK got to us before we detected that the
             * bgsave is done (since that depends on cron ticks), so run a
             * quick check first (instead of waiting for the next ACK. */
            // 如果当前是一个不落盘的副本同步，我们需要在收到client的第一个ACK（连接状态，期待更多数据）时将slave设置为online状态。
            // 这样处理更简单，且在流式RDB文件传播时，也不需要CPU过多的去做EOF检测，因为第一个ACK表示RDB文件传输完成了。
            // 有可能在我们收到ACK之前，我们已经检测到bgsave已经完成了（这取决于定时任务执行时间）。
            // 所以我们先做一个快速的检查，而不是去等待下一个ACK。
            if (server.child_type == CHILD_TYPE_RDB && c->replstate == SLAVE_STATE_WAIT_BGSAVE_END)
                // 检查子进程RDB传输是否结束，如果结束了则处理清理工作
                checkChildrenDone();
            if (c->repl_put_online_on_ack && c->replstate == SLAVE_STATE_ONLINE)
                // 设置对应slave为online状态
                putSlaveOnline(c);
            /* Note: this command does not reply anything! */
            return;
        } else if (!strcasecmp(c->argv[j]->ptr,"getack")) {
            /* REPLCONF GETACK is used in order to request an ACK ASAP
             * to the slave. */
            // 如果当前节点是slave，有对应的master，则向master发送我们的复制offset信息。
            if (server.masterhost && server.master) replicationSendAck();
            return;
        } else if (!strcasecmp(c->argv[j]->ptr,"rdb-only")) {
           /* REPLCONF RDB-ONLY is used to identify the client only wants
            * RDB snapshot without replication buffer. */
           // 取到rdb_only值，该值表示slave是否只需要RDB快照，而不需要增量传递复制数据。
            long rdb_only = 0;
            if (getRangeLongFromObjectOrReply(c,c->argv[j+1],
                    0,1,&rdb_only,NULL) != C_OK)
                return;
            if (rdb_only == 1) c->flags |= CLIENT_REPL_RDBONLY;
            else c->flags &= ~CLIENT_REPL_RDBONLY;
        } else {
            addReplyErrorFormat(c,"Unrecognized REPLCONF option: %s",
                (char*)c->argv[j]->ptr);
            return;
        }
    }
    // 这些命令都是返回ok。
    addReply(c,shared.ok);
}

/* This function puts a replica in the online state, and should be called just
 * after a replica received the RDB file for the initial synchronization, and
 * we are finally ready to send the incremental stream of commands.
 *
 * It does a few things:
 * 1) Close the replica's connection async if it doesn't need replication
 *    commands buffer stream, since it actually isn't a valid replica.
 * 2) Put the slave in ONLINE state. Note that the function may also be called
 *    for a replicas that are already in ONLINE state, but having the flag
 *    repl_put_online_on_ack set to true: we still have to install the write
 *    handler in that case. This function will take care of that.
 * 3) Make sure the writable event is re-installed, since calling the SYNC
 *    command disables it, so that we can accumulate output buffer without
 *    sending it to the replica.
 * 4) Update the count of "good replicas". */
// 这个函数应该在slave完全收到的初始全量同步的RDB文件后被调用，用于将slave client真正置于online状态。
// （主要是设置conn的写handler，从而进行增量的命令数据流传输）
// 函数主要做了如下的事情：
// 1、如果slave不需要增量传输指令流复制数据，这个slave不算是一个有效的副本，所以我们异步断开该连接。
// 2、将slaves设置为的online状态。
//  注意当slave已经处于ONLINE状态但repl_put_online_on_ack为true时，也需要调用这个方法来安装conn的写handler。
// 3、确保安装slave conn的写处理事件，因为我们完成RDB全量同步时是将handler置NULL了的，
//  这里要重新设置增量复制数据发送处理器，从而使slave处于真正online状态。
// 4、当前处理的slave处于online状态了，我们需要更新当前"good replicas"的总数量。
// 5、触发slave online的module事件。
void putSlaveOnline(client *slave) {
    // 更新连接的复制状态为ONLINE
    slave->replstate = SLAVE_STATE_ONLINE;
    slave->repl_put_online_on_ack = 0;
    slave->repl_ack_time = server.unixtime; /* Prevent false timeout. */

    if (slave->flags & CLIENT_REPL_RDBONLY) {
        // 如果连接是只想要RDB文件，而不想后面增量同步，我们需要处理异步断开连接，因为RDB已经发送完成了。
        serverLog(LL_NOTICE,
            "Close the connection with replica %s as RDB transfer is complete",
            replicationGetSlaveName(slave));
        freeClientAsync(slave);
        return;
    }
    // 为slave conn设置写handler，用于发送增量的复制数据到slaves。
    if (connSetWriteHandler(slave->conn, sendReplyToClient) == C_ERR) {
        serverLog(LL_WARNING,"Unable to register writable event for replica bulk transfer: %s", strerror(errno));
        freeClient(slave);
        return;
    }
    // 更新好的slave连接数。
    refreshGoodSlavesCount();
    /* Fire the replica change modules event. */
    // 触发slave online的modlues事件
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);
    serverLog(LL_NOTICE,"Synchronization with replica %s succeeded",
        replicationGetSlaveName(slave));
}

/* We call this function periodically to remove an RDB file that was
 * generated because of replication, in an instance that is otherwise
 * without any persistence. We don't want instances without persistence
 * to take RDB files around, this violates certain policies in certain
 * environments. */
// 当我们节点没有配置任何持久化时，cron中定期调用这个方法来删除因为全量同步复制生成的RDB文件。
// 我们不想在没有配置持久化的机器上保存RDB文件（这会导致重启时自动加载？），从而破坏了我们特定环境的一些策略。
void removeRDBUsedToSyncReplicas(void) {
    /* If the feature is disabled, return ASAP but also clear the
     * RDBGeneratedByReplication flag in case it was set. Otherwise if the
     * feature was enabled, but gets disabled later with CONFIG SET, the
     * flag may remain set to one: then next time the feature is re-enabled
     * via CONFIG SET we have have it set even if no RDB was generated
     * because of replication recently. */
    // 如果我们设置的不移除RDB sync file，那么清空RDBGeneratedByReplication标识，尽早返回。
    // 否则如果设置的是移除RDB sync file，那么BGSAVE结束后会设置该标识为1，但后面通过CONFIG SET改成了不移除，该标识可能仍保持为1。
    // 然后再下一次我们通过CONFIG SET又设置为移除RDB时，标识仍然为1，即使我们最近并没有生成RDB文件。
    if (!server.rdb_del_sync_files) {
        RDBGeneratedByReplication = 0;
        return;
    }

    // 如果所有持久化都禁止了，且有RDB文件标识，需要处理RDB移除。
    if (allPersistenceDisabled() && RDBGeneratedByReplication) {
        client *slave;
        listNode *ln;
        listIter li;

        int delrdb = 1;
        listRewind(server.slaves,&li);
        // 遍历slaves
        while((ln = listNext(&li))) {
            slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START ||
                slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END ||
                slave->replstate == SLAVE_STATE_SEND_BULK)
            {
                // 如果有slave还在使用该RDB传输，显然不能删除。
                delrdb = 0;
                break; /* No need to check the other replicas. */
            }
        }
        // 判断是否能够删除，能则进行处理。
        if (delrdb) {
            struct stat sb;
            if (lstat(server.rdb_filename,&sb) != -1) {
                RDBGeneratedByReplication = 0;
                serverLog(LL_NOTICE,
                    "Removing the RDB file used to feed replicas "
                    "in a persistence-less instance");
                // 异步删除
                bg_unlink(server.rdb_filename);
            }
        }
    }
}

// RDB文件发送到slave
void sendBulkToSlave(connection *conn) {
    client *slave = connGetPrivateData(conn);
    char buf[PROTO_IOBUF_LEN];
    ssize_t nwritten, buflen;

    /* Before sending the RDB file, we send the preamble as configured by the
     * replication process. Currently the preamble is just the bulk count of
     * the file in the form "$<length>\r\n". */
    // 在发送RDB文件前，我们先发送设置的前文信息。当前的前文信息仅包含文件大小，格式为"$<length>\r\n"。
    if (slave->replpreamble) {
        nwritten = connWrite(conn,slave->replpreamble,sdslen(slave->replpreamble));
        if (nwritten == -1) {
            serverLog(LL_VERBOSE,
                "Write error sending RDB preamble to replica: %s",
                connGetLastError(conn));
            freeClient(slave);
            return;
        }
        atomicIncr(server.stat_net_output_bytes, nwritten);
        // 前文信息发送完了清理掉，然后我们可以接着继续发送数据；没发送完则直接返回，等待下一次socket可写接着发送。
        sdsrange(slave->replpreamble,nwritten,-1);
        if (sdslen(slave->replpreamble) == 0) {
            sdsfree(slave->replpreamble);
            slave->replpreamble = NULL;
            /* fall through sending data. */
        } else {
            return;
        }
    }

    /* If the preamble was already transferred, send the RDB bulk data. */
    // 前文数据发送完了，这里开始发送RDB bulk数据。
    // 先设置当前slave处理到的offset位置。lseek见：https://blog.csdn.net/jaken99/article/details/77686427/
    lseek(slave->repldbfd,slave->repldboff,SEEK_SET);
    // 从fd读PROTO_IOBUF_LEN=16k数据放入局部缓冲buf中。
    buflen = read(slave->repldbfd,buf,PROTO_IOBUF_LEN);
    if (buflen <= 0) {
        serverLog(LL_WARNING,"Read error sending DB to replica: %s",
            (buflen == 0) ? "premature EOF" : strerror(errno));
        freeClient(slave);
        return;
    }
    // 将读取到buf中的数据再写入socket。
    if ((nwritten = connWrite(conn,buf,buflen)) == -1) {
        if (connGetState(conn) != CONN_STATE_CONNECTED) {
            serverLog(LL_WARNING,"Write error sending DB to replica: %s",
                connGetLastError(conn));
            freeClient(slave);
        }
        return;
    }
    // 更新数据发送offset及统计信息
    slave->repldboff += nwritten;
    atomicIncr(server.stat_net_output_bytes, nwritten);
    if (slave->repldboff == slave->repldbsize) {
        // 如果发送完成，做一些清理，并设置slave online。
        close(slave->repldbfd);
        slave->repldbfd = -1;
        connSetWriteHandler(slave->conn,NULL);
        // 设置slave真正online
        putSlaveOnline(slave);
    }
}

/* Remove one write handler from the list of connections waiting to be writable
 * during rdb pipe transfer. */
// 移除conn的写handler，不再处理conn的写事件监听。
// 因为有多个slave conn在等待处理全局server.rdb_pipe_buff数据，所以还需要更新全局rdb_pipe_numconns_writing计数。
void rdbPipeWriteHandlerConnRemoved(struct connection *conn) {
    if (!connHasWriteHandler(conn))
        return;
    // 置空conn的写handler
    connSetWriteHandler(conn, NULL);
    client *slave = connGetPrivateData(conn);
    // 更新slave和服务状态
    slave->repl_last_partial_write = 0;
    server.rdb_pipe_numconns_writing--;
    /* if there are no more writes for now for this conn, or write error: */
    // 当没有等待写的conn时，即server.rdb_pipe_buff里的数据所有连接的slaves都处理完了，
    // 此时需要再次监听server.rdb_pipe_read可读，从而再读取pipe的数据到rdb_pipe_buff中，进行下一轮的数据传输。
    if (server.rdb_pipe_numconns_writing == 0) {
        if (aeCreateFileEvent(server.el, server.rdb_pipe_read, AE_READABLE, rdbPipeReadHandler,NULL) == AE_ERR) {
            serverPanic("Unrecoverable error creating server.rdb_pipe_read file event.");
        }
    }
}

/* Called in diskless master during transfer of data from the rdb pipe, when
 * the replica becomes writable again. */
// pipe rdb（不写磁盘）传输时，slave连接一次没有写完所有的buf数据，会监听conn写事件，当conn可写时，会执行这个rdb pipe写操作。
void rdbPipeWriteHandler(struct connection *conn) {
    // 这里肯定有需要发送到slave的rdb_pipe_bufflen数据
    serverAssert(server.rdb_pipe_bufflen>0);
    client *slave = connGetPrivateData(conn);
    int nwritten;
    // 往slave conn写数据。前面写偏移是repldboff，所以从buff中这个位置开始处理。待写数据量为总buf数据-当前slave已处理的数据。
    if ((nwritten = connWrite(conn, server.rdb_pipe_buff + slave->repldboff,
                              server.rdb_pipe_bufflen - slave->repldboff)) == -1)
    {
        // 如果写数据err，但连接正常，返回等待下次再写。否则连接不正常就释放slave client。
        if (connGetState(conn) == CONN_STATE_CONNECTED)
            return; /* equivalent to EAGAIN */
        serverLog(LL_WARNING,"Write error sending DB to replica: %s",
            connGetLastError(conn));
        freeClient(slave);
        return;
    } else {
        // 写了部分数据到conn中，这里需要更新处理buf的偏移量。
        slave->repldboff += nwritten;
        atomicIncr(server.stat_net_output_bytes, nwritten);
        if (slave->repldboff < server.rdb_pipe_bufflen) {
            // 如果还是没有处理完buf数据，返回等待下一次再处理写
            slave->repl_last_partial_write = server.unixtime;
            return; /* more data to write.. */
        }
    }
    // 执行到这里，说明当前slave conn对server.rdb_pipe_buff中数据处理完了，可以移除当前conn的写事件了。
    rdbPipeWriteHandlerConnRemoved(conn);
}

/* Called in diskless master, when there's data to read from the child's rdb pipe */
// 主进程调用，当子进程有数据发送到父进程时，监听事件可读，父进程会执行这个函数来将数据发送到slaves。
void rdbPipeReadHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    UNUSED(mask);
    UNUSED(clientData);
    UNUSED(eventLoop);
    int i;
    // 确保创建server.rdb_pipe_buff
    if (!server.rdb_pipe_buff)
        server.rdb_pipe_buff = zmalloc(PROTO_IOBUF_LEN);
    serverAssert(server.rdb_pipe_numconns_writing==0);

    while (1) {
        // fd可读才会调用这个handler，这里循环从fd中读取数据到server.rdb_pipe_buff中。
        server.rdb_pipe_bufflen = read(fd, server.rdb_pipe_buff, PROTO_IOBUF_LEN);
        if (server.rdb_pipe_bufflen < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;
            serverLog(LL_WARNING,"Diskless rdb transfer, read error sending DB to replicas: %s", strerror(errno));
            // 如果读数据出错了，这里释放所有的等待RDB的slaves，并kill掉RDB子进程。
            for (i=0; i < server.rdb_pipe_numconns; i++) {
                connection *conn = server.rdb_pipe_conns[i];
                if (!conn)
                    continue;
                client *slave = connGetPrivateData(conn);
                freeClient(slave);
                server.rdb_pipe_conns[i] = NULL;
            }
            // kill掉RDB子进程
            killRDBChild();
            return;
        }

        if (server.rdb_pipe_bufflen == 0) {
            /* EOF - write end was closed. */
            // 如果读到的数据大小为0，则说明遇到的EOF，数据读取结束，进行相关处理。
            int stillUp = 0;
            // 移除对管道fd server.rdb_pipe_read的事件监听
            aeDeleteFileEvent(server.el, server.rdb_pipe_read, AE_READABLE);
            // 统计最终有多少个slaves conn传输RDB完成。
            for (i=0; i < server.rdb_pipe_numconns; i++)
            {
                connection *conn = server.rdb_pipe_conns[i];
                if (!conn)
                    continue;
                stillUp++;
            }
            serverLog(LL_WARNING,"Diskless rdb transfer, done reading from pipe, %d replicas still up.", stillUp);
            /* Now that the replicas have finished reading, notify the child that it's safe to exit. 
             * When the server detectes the child has exited, it can mark the replica as online, and
             * start streaming the replication buffers. */
            // 执行到这里，读数据发送到slave完成，通知子进程可以安全退出了。
            // 当服务端探测到子进程退出时，标记slaves为在线状态，并开始正常传输复制buffer里的数据。
            close(server.rdb_child_exit_pipe);
            server.rdb_child_exit_pipe = -1;
            return;
        }

        int stillAlive = 0;
        // 遍历slaves conn连接，发送读取到server.rdb_pipe_buff中的数据过去。
        for (i=0; i < server.rdb_pipe_numconns; i++)
        {
            int nwritten;
            connection *conn = server.rdb_pipe_conns[i];
            if (!conn)
                continue;

            client *slave = connGetPrivateData(conn);
            // 向conn写数据。
            if ((nwritten = connWrite(conn, server.rdb_pipe_buff, server.rdb_pipe_bufflen)) == -1) {
                if (connGetState(conn) != CONN_STATE_CONNECTED) {
                    // 如果写数据报错，且连接不是CONNECTED状态，说明连接断开了，这里释放client。
                    serverLog(LL_WARNING,"Diskless rdb transfer, write error sending DB to replica: %s",
                        connGetLastError(conn));
                    freeClient(slave);
                    server.rdb_pipe_conns[i] = NULL;
                    continue;
                }
                /* An error and still in connected state, is equivalent to EAGAIN */
                // 写到slave数据报错，但连接还是正常的，可能是EAGAIN错误，conn没准备好。
                // 这里当前slave传输server.rdb_pipe_buff里的复制数据的偏移量为0，因为没传任何数据。
                slave->repldboff = 0;
            } else {
                /* Note: when use diskless replication, 'repldboff' is the offset
                 * of 'rdb_pipe_buff' sent rather than the offset of entire RDB. */
                // 写入成功了，更新slave复制数据的偏移量，注意这里repldboff是当前处理rdb_pipe_buff的偏移，而不是整个RDB文件的偏移。
                slave->repldboff = nwritten;
                // 更新服务向外传输数据量的统计值
                atomicIncr(server.stat_net_output_bytes, nwritten);
            }
            /* If we were unable to write all the data to one of the replicas,
             * setup write handler (and disable pipe read handler, below) */
            // 如果我们当前slave没有传输完rdb_pipe_buff中的所有数据，则对应slave conn安装写事件监听，等待可写的时候再传输数据。
            // 后面如果有这样pending write的conn（即rdb_pipe_numconns_writing!=0），需要移除server.rdb_pipe_read的读处理，避免数据覆盖。
            if (nwritten != server.rdb_pipe_bufflen) {
                slave->repl_last_partial_write = server.unixtime;
                server.rdb_pipe_numconns_writing++;
                // 设置slave conn读handler。
                connSetWriteHandler(conn, rdbPipeWriteHandler);
            }
            stillAlive++;
        }

        if (stillAlive == 0) {
            serverLog(LL_WARNING,"Diskless rdb transfer, last replica dropped, killing fork child.");
            // 没有活跃的slave conns需要再传输RDB数据了，这里kill掉RDB子进程
            killRDBChild();
        }
        /*  Remove the pipe read handler if at least one write handler was set. */
        // 如果有pending write的conns，或者没有活跃的slave conns了，我们这里移除server.rdb_pipe_read读事件处理。
        // 要么buf数据有slave没处理完，不能读pipe数据覆盖buf；要么没有活跃slave conn，RDB子进程kill掉了，我们不需要监听读pipe了。
        if (server.rdb_pipe_numconns_writing || stillAlive == 0) {
            aeDeleteFileEvent(server.el, server.rdb_pipe_read, AE_READABLE);
            break;
        }
    }
}

/* This function is called at the end of every background saving,
 * or when the replication RDB transfer strategy is modified from
 * disk to socket or the other way around.
 *
 * The goal of this function is to handle slaves waiting for a successful
 * background saving in order to perform non-blocking synchronization, and
 * to schedule a new BGSAVE if there are slaves that attached while a
 * BGSAVE was in progress, but it was not a good one for replication (no
 * other slave was accumulating differences).
 *
 * The argument bgsaveerr is C_OK if the background saving succeeded
 * otherwise C_ERR is passed to the function.
 * The 'type' argument is the type of the child that terminated
 * (if it had a disk or socket target). */
// 这个函数在每次bgsave结束时调用，或者当RDB复制传输策略由disk改为socket或反过来改变时会调用。
// 这个函数用于处理正在等待bg save成功的slaves，向他们非阻塞的同步生成的RDB数据数据。
// 如果在bg save期间有slaves连接进来，且当前保存的RDB不适用的话，会重新为这些slaves再调度一个bg save子进程来处理。
// bgsaveerr参数，如果bg save成功的话传入C_OK，否则传入C_ERR。type表示后台进程bg save处理类型，落盘disk还是socket直接传输。
void updateSlavesWaitingBgsave(int bgsaveerr, int type) {
    listNode *ln;
    listIter li;

    // 遍历当前slaves列表，如果slave的复制状态是等待BGSAVE end的话，我们需要进行相应的操作。
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END) {
            // slave连接的复制状态是WAIT_BGSAVE_END，现在我们BGSAVE完成了。
            // 1、对于socket传输，我们是整个RDB文件传输完了，下一步就是正常数据同步了，
            //  所以我们这里只需要将slave设置为online，然后在收到slave发送的第一个ACK时，
            //  调用putSlaveOnline安装写监听处理，使其真正在线可传播数据。
            // 2、对于RDB落盘传输，BGSAVE结束了只表示RDB落盘成功，我们还需要真正向对应的slaves发送。
            //  所以这里我们处理是每个slave连接都打开RDB文件，并安装sendBulkToSlave勇敢发送RDB数据。
            //  当所有数据发送完成时调用putSlaveOnline来将slave设置为真正在线状态。
            struct redis_stat buf;

            if (bgsaveerr != C_OK) {
                freeClient(slave);
                serverLog(LL_WARNING,"SYNC failed. BGSAVE child returned an error");
                continue;
            }

            /* If this was an RDB on disk save, we have to prepare to send
             * the RDB from disk to the slave socket. Otherwise if this was
             * already an RDB -> Slaves socket transfer, used in the case of
             * diskless replication, our work is trivial, we can just put
             * the slave online. */
            // 如果当前是RDB保存在磁盘上再传输，我们需要准备好从磁盘发送到slave的连接。打开文件，然后对conn设置写handler。
            // 否则，如果已经有了RDB->slave流式传输socket（无盘复制），我们不需要做什么工作，只需要收到ack后设置slave为online状态即可。
            if (type == RDB_CHILD_TYPE_SOCKET) {
                serverLog(LL_NOTICE,
                    "Streamed RDB transfer with replica %s succeeded (socket). Waiting for REPLCONF ACK from slave to enable streaming",
                        replicationGetSlaveName(slave));
                /* Note: we wait for a REPLCONF ACK message from the replica in
                 * order to really put it online (install the write handler
                 * so that the accumulated data can be transferred). However
                 * we change the replication state ASAP, since our slave
                 * is technically online now.
                 *
                 * So things work like that:
                 *
                 * 1. We end trasnferring the RDB file via socket.
                 * 2. The replica is put ONLINE but the write handler
                 *    is not installed.
                 * 3. The replica however goes really online, and pings us
                 *    back via REPLCONF ACK commands.
                 * 4. Now we finally install the write handler, and send
                 *    the buffers accumulated so far to the replica.
                 *
                 * But why we do that? Because the replica, when we stream
                 * the RDB directly via the socket, must detect the RDB
                 * EOF (end of file), that is a special random string at the
                 * end of the RDB (for streamed RDBs we don't know the length
                 * in advance). Detecting such final EOF string is much
                 * simpler and less CPU intensive if no more data is sent
                 * after such final EOF. So we don't want to glue the end of
                 * the RDB trasfer with the start of the other replication
                 * data. */
                // 我们等待来自slave的REPLCONF ACK消息，从而设置slave状态为真正online（安装conn的写handler），保证继续进行传输数据。
                // 不过在这里我们尽可能早的设置replstate为online，因为我们的slave在技术上已经是连接的了。
                // 所以这里整个流程如下：
                // 1、我们通过socket处理完了RDB文件传输。
                // 2、slave是online状态了，但是client的连接conn还没有设置写handler。
                // 3、slave节点处理完同步的RDB数据，达到了真正的online状态，通过REPLCONF ACK回复master。
                // 4、我们master得到确认slave的确认，设置slave为真正online，即安装slave连接的写handler，从而可以后续同步数据。
                // 为什么我们需要这么做？
                // 因为我们通过socket进行流式RDB传输时，事先不知道传输数据长度，RDB结束位置不知道的。
                // 所以对于传输结束的判断，我们可以有如下三种选择（commit log中的）：
                // 1) Scan all the stream searching for the EOF mark. Sucks CPU-wise. 即每次都是检查EOF
                // 2) Start to send the replication stream only after an acknowledge. 即依赖于slave的ack
                // 3) Implement a proper chunked encoding. 即实现一种合适的chunked编码，自己处理结束标识。
                // 显然这里选择了第二种方式。既简单，又不需要多少CPU。所以我们将RDB传输结束和开启其他命令数据复制分开来处理了。

                // 设置了online状态，等待第一次的ACK后将slave设置为真正的online。
                slave->replstate = SLAVE_STATE_ONLINE;
                slave->repl_put_online_on_ack = 1;
                slave->repl_ack_time = server.unixtime; /* Timeout otherwise. */
            } else {
                // 如果是落盘后再传输，这里需要打开RDB文件，并初始化传输状态。
                if ((slave->repldbfd = open(server.rdb_filename,O_RDONLY)) == -1 ||
                    redis_fstat(slave->repldbfd,&buf) == -1) {
                    freeClient(slave);
                    serverLog(LL_WARNING,"SYNC failed. Can't open/stat DB after BGSAVE: %s", strerror(errno));
                    continue;
                }
                slave->repldboff = 0;
                slave->repldbsize = buf.st_size;
                // 更新复制状态，设置前文信息（这里是RDB文件总长度）
                slave->replstate = SLAVE_STATE_SEND_BULK;
                slave->replpreamble = sdscatprintf(sdsempty(),"$%lld\r\n",
                    (unsigned long long) slave->repldbsize);

                // 更新slave连接的写handler。
                connSetWriteHandler(slave->conn,NULL);
                if (connSetWriteHandler(slave->conn,sendBulkToSlave) == C_ERR) {
                    freeClient(slave);
                    continue;
                }
            }
        }
    }
}

/* Change the current instance replication ID with a new, random one.
 * This will prevent successful PSYNCs between this master and other
 * slaves, so the command should be called when something happens that
 * alters the current story of the dataset. */
// 更改当前实例的复制ID为一个新的随机ID。这会使得slaves与当前master之间PSYNCs失败。
// 所以当实例的数据集发生变更时，需要调用这个方法来阻止PSYNCs。
void changeReplicationId(void) {
    getRandomHexChars(server.replid,CONFIG_RUN_ID_SIZE);
    server.replid[CONFIG_RUN_ID_SIZE] = '\0';
}

/* Clear (invalidate) the secondary replication ID. This happens, for
 * example, after a full resynchronization, when we start a new replication
 * history. */
// 清除掉辅助ID/offset。这通常在我们进行完一次全量重同步操作之后调用，因为我们已经开始了一个新的历史数据同步，老的就不需要了。
void clearReplicationId2(void) {
    memset(server.replid2,'0',sizeof(server.replid));
    server.replid2[CONFIG_RUN_ID_SIZE] = '\0';
    server.second_replid_offset = -1;
}

/* Use the current replication ID / offset as secondary replication
 * ID, and change the current one in order to start a new history.
 * This should be used when an instance is switched from slave to master
 * so that it can serve PSYNC requests performed using the master
 * replication ID. */
// 使用当前的复制ID/offset作为第二复制ID，并随机生成一个新的复制ID来开启复制。
// 当我们从slave变成master时，这样处理是很有用的，因为我们可以使用master的复制ID继续处理PSYNC请求。
void shiftReplicationId(void) {
    // 将当前的replid赋值给replid2。
    memcpy(server.replid2,server.replid,sizeof(server.replid));
    /* We set the second replid offset to the master offset + 1, since
     * the slave will ask for the first byte it has not yet received, so
     * we need to add one to the offset: for example if, as a slave, we are
     * sure we have the same history as the master for 50 bytes, after we
     * are turned into a master, we can accept a PSYNC request with offset
     * 51, since the slave asking has the same history up to the 50th
     * byte, and is asking for the new bytes starting at offset 51. */
    // 我们设置 second_replid_offset 为 master 的 offset+1，因为slave将请求它没有收到的第一个byte，所以我们需要加1。
    // 例如：作为slave时，我们确定跟master有一样的历史数据，50字节。其他的slave也一样有相同的50字节历史数据，并将请求新的第51字节数据。
    // 所以当我们变成master后，我们能够接受PSYNC请求的offset为51。
    server.second_replid_offset = server.master_repl_offset+1;
    // 重新生成新的复制id赋值给server.replid
    changeReplicationId();
    serverLog(LL_WARNING,"Setting secondary replication ID to %s, valid up to offset: %lld. New replication ID is %s", server.replid2, server.second_replid_offset, server.replid);
}

/* ----------------------------------- SLAVE -------------------------------- */

/* Returns 1 if the given replication state is a handshake state,
 * 0 otherwise. */
int slaveIsInHandshakeState(void) {
    return server.repl_state >= REPL_STATE_RECEIVE_PING_REPLY &&
           server.repl_state <= REPL_STATE_RECEIVE_PSYNC_REPLY;
}

/* Avoid the master to detect the slave is timing out while loading the
 * RDB file in initial synchronization. We send a single newline character
 * that is valid protocol but is guaranteed to either be sent entirely or
 * not, since the byte is indivisible.
 *
 * The function is called in two contexts: while we flush the current
 * data with emptyDb(), and while we load the new data received as an
 * RDB file from the master. */
// slave在初始同步中加载RDB文件时，隔一段时间向master发送空行，避免被master检测到连接超时。
// 空行是有效的协议，且字节不可分隔，可保证要么发送要么不发送。
// 该函数在两种环境下执行：1、emptyDb()使用清空当前的数据。2、加载从master接收到的新的RDB文件数据。
void replicationSendNewlineToMaster(void) {
    static time_t newline_sent;
    if (time(NULL) != newline_sent) {
        newline_sent = time(NULL);
        /* Pinging back in this stage is best-effort. */
        // 这个阶段的回复是尽最大努力处理。
        if (server.repl_transfer_s) connWrite(server.repl_transfer_s, "\n", 1);
    }
}

/* Callback used by emptyDb() while flushing away old data to load
 * the new dataset received by the master. */
// emptyDb()中使用的回调函数。主要用于我们在清空db数据的时候，隔一段时间向master发送空行，表示连接还活跃着。
void replicationEmptyDbCallback(void *privdata) {
    UNUSED(privdata);
    if (server.repl_state == REPL_STATE_TRANSFER)
        replicationSendNewlineToMaster();
}

/* Once we have a link with the master and the synchronization was
 * performed, this function materializes the master client we store
 * at server.master, starting from the specified file descriptor. */
// 一旦我们与master建立连接并完成了全量同步，这个函数会复用前面的conn，构建master client进行后续增量数据同步。
void replicationCreateMasterClient(connection *conn, int dbid) {
    // 使用conn构建client，赋值给server.master
    server.master = createClient(conn);
    if (conn)
        // 设置数据处理handler
        connSetReadHandler(server.master->conn, readQueryFromClient);

    /**
     * Important note:
     * The CLIENT_DENY_BLOCKING flag is not, and should not, be set here.
     * For commands like BLPOP, it makes no sense to block the master
     * connection, and such blocking attempt will probably cause deadlock and
     * break the replication. We consider such a thing as a bug because
     * commands as BLPOP should never be sent on the replication link.
     * A possible use-case for blocking the replication link is if a module wants
     * to pass the execution to a background thread and unblock after the
     * execution is done. This is the reason why we allow blocking the replication
     * connection. */

    // 重要提示：CLIENT_DENY_BLOCKING标识不在这里设置，也不应该在这里设置。
    // 对于像BLPOP这样的命令，阻塞master连接没有任何意义，并且这种阻塞可能会导致死锁并破坏复制继续进行。
    // 我们将这看作是bug，因为BLPOP之类的命令永远不应该发送到复制连接上。
    // 阻塞复制链接一个可能的用途是，模块希望后台线程执行该命令，并在命令执行完后解除阻塞。这就是为什么我们允许阻塞复制连接的原因。
    server.master->flags |= CLIENT_MASTER;

    server.master->authenticated = 1;
    // 设置master全局offset和slave读offset
    server.master->reploff = server.master_initial_offset;
    server.master->read_reploff = server.master->reploff;
    // 默认user，可以执行任何命令。
    server.master->user = NULL; /* This client can do everything. */
    // 设置复制id
    memcpy(server.master->replid, server.master_replid,
        sizeof(server.master_replid));
    /* If master offset is set to -1, this master is old and is not
     * PSYNC capable, so we flag it accordingly. */
    // 如果master的复制offset为-1，则该master较旧且不支持PSYNC，因此这里做标识。
    if (server.master->reploff == -1)
        server.master->flags |= CLIENT_PRE_PSYNC;
    // 设置db
    if (dbid != -1) selectDb(server.master,dbid);
}

/* This function will try to re-enable the AOF file after the
 * master-replica synchronization: if it fails after multiple attempts
 * the replica cannot be considered reliable and exists with an
 * error. */
// 这个函数用于在主从全量同步后重新启用AOF。如果多次尝试都失败了，则当前slave会被认为不可用并退出。
void restartAOFAfterSYNC() {
    unsigned int tries, max_tries = 10;
    for (tries = 0; tries < max_tries; ++tries) {
        // 启用AOF
        if (startAppendOnly() == C_OK) break;
        serverLog(LL_WARNING,
            "Failed enabling the AOF after successful master synchronization! "
            "Trying it again in one second.");
        sleep(1);
    }
    if (tries == max_tries) {
        serverLog(LL_WARNING,
            "FATAL: this replica instance finished the synchronization with "
            "its master, but the AOF can't be turned on. Exiting now.");
        exit(1);
    }
}

static int useDisklessLoad() {
    /* compute boolean decision to use diskless load */
    // 判断是否使用无盘加载
    int enabled = server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB ||
           (server.repl_diskless_load == REPL_DISKLESS_LOAD_WHEN_DB_EMPTY && dbTotalServerKeyCount()==0);
    /* Check all modules handle read errors, otherwise it's not safe to use diskless load. */
    // 需要检查所有的modules handler是否有处理读err，如果没有处理则不安全，我们将不使用无盘加载。
    if (enabled && !moduleAllDatatypesHandleErrors()) {
        serverLog(LL_WARNING,
            "Skipping diskless-load because there are modules that don't handle read errors.");
        enabled = 0;
    }
    return enabled;
}

/* Helper function for readSyncBulkPayload() to make backups of the current
 * databases before socket-loading the new ones. The backups may be restored
 * by disklessLoadRestoreBackup or freed by disklessLoadDiscardBackup later. */
// readSyncBulkPayload()的帮助函数，用于在使用socket-loading方式加载时，备份当前的数据库。
// backups的数据库将在后面disklessLoadRestoreBackup中还原，或者在disklessLoadDiscardBackup中释放掉。
dbBackup *disklessLoadMakeBackup(void) {
    return backupDb();
}

/* Helper function for readSyncBulkPayload(): when replica-side diskless
 * database loading is used, Redis makes a backup of the existing databases
 * before loading the new ones from the socket.
 *
 * If the socket loading went wrong, we want to restore the old backups
 * into the server databases. */
void disklessLoadRestoreBackup(dbBackup *buckup) {
    restoreDbBackup(buckup);
}

/* Helper function for readSyncBulkPayload() to discard our old backups
 * when the loading succeeded. */
void disklessLoadDiscardBackup(dbBackup *buckup, int flag) {
    discardDbBackup(buckup, flag, replicationEmptyDbCallback);
}

/* Asynchronously read the SYNC payload we receive from a master */
// 异步读取从master收到的SYNC数据
#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8) /* 8 MB */
void readSyncBulkPayload(connection *conn) {
    char buf[PROTO_IOBUF_LEN];
    ssize_t nread, readlen, nwritten;
    // 是否无盘加载，RDB不存盘
    int use_diskless_load = useDisklessLoad();
    dbBackup *diskless_load_backup = NULL;
    // 清空DB是否异步执行
    int empty_db_flags = server.repl_slave_lazy_flush ? EMPTYDB_ASYNC :
                                                        EMPTYDB_NO_FLAGS;
    off_t left;

    /* Static vars used to hold the EOF mark, and the last bytes received
     * from the server: when they match, we reached the end of the transfer. */
    // 静态变量用于存储从master收到的EOF标识，以及最后的字节数据。当它们相匹配时，表示我们RDB transfer结束了。
    static char eofmark[CONFIG_RUN_ID_SIZE];
    static char lastbytes[CONFIG_RUN_ID_SIZE];
    // usemark只是标识master传输方式，而use_diskless_load表示slave的load数据方式？二者是可以交叉使用？
    static int usemark = 0;

    /* If repl_transfer_size == -1 we still have to read the bulk length
     * from the master reply. */
    // 当repl_transfer_size为-1时，我们需要等待master的回复，从而获取传输的数据总长度（传输时的前文信息）。
    if (server.repl_transfer_size == -1) {
        // 同步读取。能执行到本函数，显然可读事件是触发了的，说明有数据可读。
        if (connSyncReadLine(conn,buf,1024,server.repl_syncio_timeout*1000) == -1) {
            serverLog(LL_WARNING,
                "I/O error reading bulk count from MASTER: %s",
                strerror(errno));
            goto error;
        }

        if (buf[0] == '-') {
            // '-'开头的表示error信息。具体可见RESP协议。
            serverLog(LL_WARNING,
                "MASTER aborted replication with an error: %s",
                buf+1);
            goto error;
        } else if (buf[0] == '\0') {
            /* At this stage just a newline works as a PING in order to take
             * the connection live. So we refresh our last interaction
             * timestamp. */
            // 如果是发送空串，可能是master为了保证conn活着，而发送的。这里我们更新一下当前最新io数据，避免超时。
            server.repl_transfer_lastio = server.unixtime;
            return;
        } else if (buf[0] != '$') {
            // 目前repl_transfer_size为-1，还没有获取到长度，而数据又不是'$'开头的话，显然协议出错了。
            serverLog(LL_WARNING,"Bad protocol from MASTER, the first byte is not '$' (we received '%s'), are you sure the host and port are right?", buf);
            goto error;
        }

        /* There are two possible forms for the bulk payload. One is the
         * usual $<count> bulk format. The other is used for diskless transfers
         * when the master does not know beforehand the size of the file to
         * transfer. In the latter case, the following format is used:
         *
         * $EOF:<40 bytes delimiter>
         *
         * At the end of the file the announced delimiter is transmitted. The
         * delimiter is long and random enough that the probability of a
         * collision with the actual file content can be ignored. */
        // bulk数据有两种可能的格式，一种是先传输$<count>，之后传输bulk数据格式。
        // 另一种是用于无盘传输，master事先不知道传输文件的长度，所以无法预先传输数据总长度。
        // 无盘传输时，会先发送 $EOF:<40 bytes delimiter>，预设置结束分隔符，后面传输数据。
        // 当文件数据传输结束时，会发送这个已声明的结束符。该结束符足够长且随机，可以忽略与实际文件内容发生冲突的可能性。
        if (strncmp(buf+1,"EOF:",4) == 0 && strlen(buf+5) >= CONFIG_RUN_ID_SIZE) {
            usemark = 1;
            // 无盘方式传输，我们将结束符写入eofmark。
            memcpy(eofmark,buf+5,CONFIG_RUN_ID_SIZE);
            memset(lastbytes,0,CONFIG_RUN_ID_SIZE);
            /* Set any repl_transfer_size to avoid entering this code path
             * at the next call. */
            // 设置repl_transfer_size为其他值，避免进入函数时被当作首次进入处理。
            server.repl_transfer_size = 0;
            serverLog(LL_NOTICE,
                "MASTER <-> REPLICA sync: receiving streamed RDB from master with EOF %s",
                use_diskless_load? "to parser":"to disk");
        } else {
            usemark = 0;
            // 不是无盘传输，事先知道文件数据长度，这里直接获取。
            // 因为首次进来处理是使用的同步读取，读一行数据，而前文传输长度是'\r\n'结束，所以这里肯定读到的数据为长度，直接转换。
            server.repl_transfer_size = strtol(buf+1,NULL,10);
            serverLog(LL_NOTICE,
                "MASTER <-> REPLICA sync: receiving %lld bytes from master %s",
                (long long) server.repl_transfer_size,
                use_diskless_load? "to parser":"to disk");
        }
        return;
    }

    if (!use_diskless_load) {
        /* Read the data from the socket, store it to a file and search
         * for the EOF. */
        // 不是无盘加载，这里需要从socket读取数据，并存储到文件中。使用读取数据长度来判断是否读完。
        if (usemark) {
            // master使用无盘流式传输，一次读取整个buf长度数据。
            readlen = sizeof(buf);
        } else {
            // 计算传输剩余的数据量，然后算出一次读取的长度。
            left = server.repl_transfer_size - server.repl_transfer_read;
            readlen = (left < (signed)sizeof(buf)) ? left : (signed)sizeof(buf);
        }

        // 从conn读数据到buf中。
        nread = connRead(conn,buf,readlen);
        if (nread <= 0) {
            if (connGetState(conn) == CONN_STATE_CONNECTED) {
                // 没读到数据，如果还是连接状态，返回等待。
                /* equivalent to EAGAIN */
                return;
            }
            serverLog(LL_WARNING,"I/O error trying to sync with MASTER: %s",
                (nread == -1) ? strerror(errno) : "connection lost");
            // 有error的话cancel当前连接，处理重连。
            cancelReplicationHandshake(1);
            return;
        }
        // 更新网络读取数据的统计
        atomicIncr(server.stat_net_input_bytes, nread);

        /* When a mark is used, we want to detect EOF asap in order to avoid
         * writing the EOF mark into the file... */
        // 如果使用的是特殊字符串标记结束，我们需要尽快检测EOF，以避免EOF标记字符串写入文件。
        int eof_reached = 0;

        if (usemark) {
            /* Update the last bytes array, and check if it matches our
             * delimiter. */
            // 无盘流式传输，我们要更新last bytes数组，检查是否与我们的结束标识字符串一致。
            if (nread >= CONFIG_RUN_ID_SIZE) {
                // 如果当前读取的数据长度大于CONFIG_RUN_ID_SIZE，则结束符肯定只可能在当前读的数据中，直接获取。
                memcpy(lastbytes,buf+nread-CONFIG_RUN_ID_SIZE,
                       CONFIG_RUN_ID_SIZE);
            } else {
                // 反之，如果小于CONFIG_RUN_ID_SIZE，我们需要使用lastbytes原有的数据与当前读的数据组合，来得到新的lastbytes。
                int rem = CONFIG_RUN_ID_SIZE-nread;
                // 将lastbytes的最后rem个字符移到开头。
                memmove(lastbytes,lastbytes+nread,rem);
                // 将buf中读到的nread个字符加到lastbytes后面位置。
                memcpy(lastbytes+rem,buf,nread);
            }
            // 对比 lastbytes 和 eofmark 是否一致，如果一致则我们数据transfer同步完成。
            if (memcmp(lastbytes,eofmark,CONFIG_RUN_ID_SIZE) == 0)
                eof_reached = 1;
        }

        /* Update the last I/O time for the replication transfer (used in
         * order to detect timeouts during replication), and write what we
         * got from the socket to the dump file on disk. */
        // 更新最近的复制transfer的io时间（用于复制期间检查超时），并将我们从socket读取到的数据写入磁盘文件中。
        server.repl_transfer_lastio = server.unixtime;
        // 读到的数据写文件。注意这里如果是无盘流式传输，结束标识字符串也是写入到了文件中的，后面需要处理。
        if ((nwritten = write(server.repl_transfer_fd,buf,nread)) != nread) {
            serverLog(LL_WARNING,
                "Write error or short write writing to the DB dump file "
                "needed for MASTER <-> REPLICA synchronization: %s",
                (nwritten == -1) ? strerror(errno) : "short write");
            goto error;
        }
        // 更新SYNC总的读取到的RDB数据大小
        server.repl_transfer_read += nread;

        /* Delete the last 40 bytes from the file if we reached EOF. */
        // 如果是使用的是无盘流式传输，且数据传输结束了，这里我们需要将40个字节的结束标识从文件末尾移除。
        if (usemark && eof_reached) {
            // 移除结束标识
            if (ftruncate(server.repl_transfer_fd,
                server.repl_transfer_read - CONFIG_RUN_ID_SIZE) == -1)
            {
                serverLog(LL_WARNING,
                    "Error truncating the RDB file received from the master "
                    "for SYNC: %s", strerror(errno));
                goto error;
            }
        }

        /* Sync data on disk from time to time, otherwise at the end of the
         * transfer we may suffer a big delay as the memory buffers are copied
         * into the actual disk. */
        // 当我们取到的数据达到一定大小（8MB），fsync到磁盘。否则等到传输结束，可能要写文件的数据很大，导致刷盘会有很长延时。
        if (server.repl_transfer_read >=
            server.repl_transfer_last_fsync_off + REPL_MAX_WRITTEN_BEFORE_FSYNC)
        {
            // 计算刷盘数据大小
            off_t sync_size = server.repl_transfer_read -
                              server.repl_transfer_last_fsync_off;
            // 从上次刷盘位置开始，写入sync_size到磁盘文件
            rdb_fsync_range(server.repl_transfer_fd,
                server.repl_transfer_last_fsync_off, sync_size);
            // 更新新的刷盘offset
            server.repl_transfer_last_fsync_off += sync_size;
        }

        /* Check if the transfer is now complete */
        // 如果master不是无盘传输数据，这里我们要使用总数据长度来判断是否传输结束。
        if (!usemark) {
            if (server.repl_transfer_read == server.repl_transfer_size)
                eof_reached = 1;
        }

        /* If the transfer is yet not complete, we need to read more, so
         * return ASAP and wait for the handler to be called again. */
        // 如果传输没有结束，我们需要继续读取，然后写入文件。所以这里直接返回，等待下一次conn可读时，再进入这个handler处理。
        if (!eof_reached) return;
    }

    /* We reach this point in one of the following cases:
     *
     * 1. The replica is using diskless replication, that is, it reads data
     *    directly from the socket to the Redis memory, without using
     *    a temporary RDB file on disk. In that case we just block and
     *    read everything from the socket.
     *
     * 2. Or when we are done reading from the socket to the RDB file, in
     *    such case we want just to read the RDB file in memory. */
    // 如果满足下面两种情况的一种，我们会执行到这里：
    // 1、slave使用无盘加载，直接从socket读取数据更新到内存，不写入磁盘临时文件。这种情况，我们阻塞并一直从socket读取数据。
    // 2、slave已经完成了RDB数据传输，并且全部写入到了RDB文件。这种情况，我们直接读取文件到内存中。
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Flushing old data");

    /* We need to stop any AOF rewriting child before flusing and parsing
     * the RDB, otherwise we'll create a copy-on-write disaster. */
    // 在刷新和解析RDB之前，我们需要停掉AOF重写子进程，否则会引起copy-on-write灾难（内存数据全变了，全部都要copy了再写）。
    if (server.aof_state != AOF_OFF) stopAppendOnly();

    /* When diskless RDB loading is used by replicas, it may be configured
     * in order to save the current DB instead of throwing it away,
     * so that we can restore it in case of failed transfer. */
    // 当slave使用无盘RDB加载时，可以设置保存当前数据库而不是直接丢弃，这样在传输失败的情况下可以将其还原。
    if (use_diskless_load &&
        server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB)
    {
        /* Create a backup of server.db[] and initialize to empty
         * dictionaries. */
        // 创建server.db[]的备份，并将当前使用的server.db[]置空。
        diskless_load_backup = disklessLoadMakeBackup();
    }
    /* We call to emptyDb even in case of REPL_DISKLESS_LOAD_SWAPDB
     * (Where disklessLoadMakeBackup left server.db empty) because we
     * want to execute all the auxiliary logic of emptyDb (Namely,
     * fire module events) */
    // 调用emptyDb进行置空DB处理。
    // 注意即使是无盘加载，在前面我们备份了数据，置空的DB，这里还是一样调用emptyDb。因为我们还想执行一些辅助逻辑，如触发模块事件等。
    emptyDb(-1,empty_db_flags,replicationEmptyDbCallback);

    /* Before loading the DB into memory we need to delete the readable
     * handler, otherwise it will get called recursively since
     * rdbLoad() will call the event loop to process events from time to
     * time for non blocking loading. */
    // 在将DB加载到内存之前，我们需要删除读handler，否则它将被递归调用，因为rdbLoad()会调用事件循环处理事件，从而进行非阻塞加载。
    connSetReadHandler(conn, NULL);
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Loading DB in memory");
    rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
    if (use_diskless_load) {
        // 使用无盘加载，初始化基于socket conn的rio对象
        rio rdb;
        rioInitWithConn(&rdb,conn,server.repl_transfer_size);

        /* Put the socket in blocking mode to simplify RDB transfer.
         * We'll restore it when the RDB is received. */
        // 将套接字设置为阻塞模式以简化RDB传输。当处理完RDB传输后再还原成非阻塞。
        connBlock(conn);
        // 设置conn超时时间
        connRecvTimeout(conn, server.repl_timeout*1000);
        // 标记为数据loading状态，触发一些事件。
        startLoading(server.repl_transfer_size, RDBFLAGS_REPLICATION);

        if (rdbLoadRio(&rdb,RDBFLAGS_REPLICATION,&rsi) != C_OK) {
            /* RDB loading failed. */
            // 无盘加载RDB失败了
            stopLoading(0);
            serverLog(LL_WARNING,
                "Failed trying to load the MASTER synchronization DB "
                "from socket");
            // 取消当前正在进行的RDB传输，尝试再重新建立连接
            cancelReplicationHandshake(1);
            // 释放处理conn读数据的rio
            rioFreeConn(&rdb, NULL);

            /* Remove the half-loaded data in case we started with
             * an empty replica. */
            // 清空当前加载了一半的数据
            emptyDb(-1,empty_db_flags,replicationEmptyDbCallback);

            if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
                /* Restore the backed up databases. */
                // 使用无盘交换的方式加载master RDB数据，这里需要还原老的DB数据（原DB本来就在内存中，只需要切换指针指向就可以了）。
                disklessLoadRestoreBackup(diskless_load_backup);
            }

            /* Note that there's no point in restarting the AOF on SYNC
             * failure, it'll be restarted when sync succeeds or the replica
             * gets promoted. */
            // 注意这里在SYNC失败时重新启动AOF是没有意义的，而当后面再次同步成功或slave升级时将重新启动AOF。
            return;
        }

        /* RDB loading succeeded if we reach this point. */
        // 执行到这里时，RDB肯定加载成功了
        if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
            /* Delete the backup databases we created before starting to load
             * the new RDB. Now the RDB was loaded with success so the old
             * data is useless. */
            // 因为新的RDB加载成功了，那么之前备份的数据就没有用了，这里删除之前备份的DB
            disklessLoadDiscardBackup(diskless_load_backup, empty_db_flags);
        }

        /* Verify the end mark is correct. */
        // 确认结束标识是否正确。
        if (usemark) {
            // 如果读结束符失败，或者结束符跟预设的不一致。结束，返回。
            if (!rioRead(&rdb,buf,CONFIG_RUN_ID_SIZE) ||
                memcmp(buf,eofmark,CONFIG_RUN_ID_SIZE) != 0)
            {
                stopLoading(0);
                serverLog(LL_WARNING,"Replication stream EOF marker is broken");
                cancelReplicationHandshake(1);
                rioFreeConn(&rdb, NULL);
                return;
            }
        }

        stopLoading(1);

        /* Cleanup and restore the socket to the original state to continue
         * with the normal replication. */
        // 清理并重置socket到原本状态，从而继续正常的数据复制。
        rioFreeConn(&rdb, NULL);
        connNonBlock(conn);
        connRecvTimeout(conn,0);
    } else {
        /* Ensure background save doesn't overwrite synced data */
        // 这里是从已经存储的RDB文件中加载，确保没有BG save来重新同步的数据。
        if (server.child_type == CHILD_TYPE_RDB) {
            serverLog(LL_NOTICE,
                "Replica is about to load the RDB file received from the "
                "master, but there is a pending RDB child running. "
                "Killing process %ld and removing its temp file to avoid "
                "any race",
                (long) server.child_pid);
            // 如果当前有RDB子进程在执行，kill掉
            killRDBChild();
        }

        /* Make sure the new file (also used for persistence) is fully synced
         * (not covered by earlier calls to rdb_fsync_range). */
        // 确保新文件已完全刷盘（持久化），因为只有数据达到一定量后才会调用rdb_fsync_range刷盘，所以可能最后还有部分数据没有持久化到磁盘。。
        if (fsync(server.repl_transfer_fd) == -1) {
            serverLog(LL_WARNING,
                "Failed trying to sync the temp DB to disk in "
                "MASTER <-> REPLICA synchronization: %s",
                strerror(errno));
            cancelReplicationHandshake(1);
            return;
        }

        /* Rename rdb like renaming rewrite aof asynchronously. */
        // 和异步重写aof一样，这里重命令RDB文件。
        int old_rdb_fd = open(server.rdb_filename,O_RDONLY|O_NONBLOCK);
        // open老的rdb文件。rename因为老的rdb文件存在，会先删掉老的文件，然后改名，类似mv命令。
        // 其实这里因为打开了老rdb文件，删除unlink这里并不会真的删除文件，只是对文件连接数-1，直到所有连接数减为0了才真的删除，这样异步才有效。
        if (rename(server.repl_transfer_tmpfile,server.rdb_filename) == -1) {
            serverLog(LL_WARNING,
                "Failed trying to rename the temp DB into %s in "
                "MASTER <-> REPLICA synchronization: %s",
                server.rdb_filename, strerror(errno));
            cancelReplicationHandshake(1);
            if (old_rdb_fd != -1) close(old_rdb_fd);
            return;
        }
        /* Close old rdb asynchronously. */
        // 异步关闭老的rdb文件
        if (old_rdb_fd != -1) bioCreateCloseJob(old_rdb_fd);

        // 加载RDB文件
        if (rdbLoad(server.rdb_filename,&rsi,RDBFLAGS_REPLICATION) != C_OK) {
            serverLog(LL_WARNING,
                "Failed trying to load the MASTER synchronization "
                "DB from disk");
            cancelReplicationHandshake(1);
            if (server.rdb_del_sync_files && allPersistenceDisabled()) {
                serverLog(LL_NOTICE,"Removing the RDB file obtained from "
                                    "the master. This replica has persistence "
                                    "disabled");
                // 如果配置删除RDB同步文件，且当前所有持久化配置都关闭的话，我们要删除这个RDB文件。
                bg_unlink(server.rdb_filename);
            }
            /* Note that there's no point in restarting the AOF on sync failure,
               it'll be restarted when sync succeeds or replica promoted. */
            return;
        }

        /* Cleanup. */
        // 清理工作。如果实例没有开启持久化，且有配置删除SYNC文件，则这里异步删除文件。
        if (server.rdb_del_sync_files && allPersistenceDisabled()) {
            serverLog(LL_NOTICE,"Removing the RDB file obtained from "
                                "the master. This replica has persistence "
                                "disabled");
            bg_unlink(server.rdb_filename);
        }

        zfree(server.repl_transfer_tmpfile);
        close(server.repl_transfer_fd);
        server.repl_transfer_fd = -1;
        server.repl_transfer_tmpfile = NULL;
    }

    /* Final setup of the connected slave <- master link */
    // 复用RDB传输的conn，作为master后续向slave传输数据的连接。这里使用这个conn创建master client。
    replicationCreateMasterClient(server.repl_transfer_s,rsi.repl_stream_db);
    // 复制连接已建立，可以增量同步数据了
    server.repl_state = REPL_STATE_CONNECTED;
    server.repl_down_since = 0;

    /* Fire the master link modules event. */
    // master连接建立，事件通知module
    moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                          REDISMODULE_SUBEVENT_MASTER_LINK_UP,
                          NULL);

    /* After a full resynchronization we use the replication ID and
     * offset of the master. The secondary ID / offset are cleared since
     * we are starting a new history. */
    // 当作为一个全量重同步后，我们使用master的复制id和offser来更新当前节点服务属性。
    memcpy(server.replid,server.master->replid,sizeof(server.replid));
    server.master_repl_offset = server.master->reploff;
    // 因为我们开始了一个新的历史数据同步，所以这里清除掉辅助ID/offset。
    clearReplicationId2();

    /* Let's create the replication backlog if needed. Slaves need to
     * accumulate the backlog regardless of the fact they have sub-slaves
     * or not, in order to behave correctly if they are promoted to
     * masters after a failover. */
    // 如果backlog为NULL的话，需要进行创建初始化。
    // 无论当前slave节点是否有sub-slaves，都需要这个backlog，以便在故障转移升级为master时能正常处理数据同步到slave。
    if (server.repl_backlog == NULL) createReplicationBacklog();
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Finished with success");

    // 发送信息到管理服务
    if (server.supervised_mode == SUPERVISED_SYSTEMD) {
        redisCommunicateSystemd("STATUS=MASTER <-> REPLICA sync: Finished with success. Ready to accept connections in read-write mode.\n");
    }

    /* Send the initial ACK immediately to put this replica in online state. */
    // 如果master是无盘socket传输，这里全量重同步完成需要发送ACK给master，通知master把我们设置为online状态。
    if (usemark) replicationSendAck();

    /* Restart the AOF subsystem now that we finished the sync. This
     * will trigger an AOF rewrite, and when done will start appending
     * to the new file. */
    // 数据同步完成，重启AOF子系统。这将触发AOF重写，重写结束后再向新文件追加新数据。
    if (server.aof_enabled) restartAOFAfterSYNC();
    return;

error:
    cancelReplicationHandshake(1);
    return;
}

// 同步读取response数据
char *receiveSynchronousResponse(connection *conn) {
    char buf[256];
    /* Read the reply from the server. */
    // 同步读取conn数据，要么读256字节，要么读到1行结束。超时时间5*1000ms
    if (connSyncReadLine(conn,buf,sizeof(buf),server.repl_syncio_timeout*1000) == -1)
    {
        return sdscatprintf(sdsempty(),"-Reading from master: %s",
                strerror(errno));
    }
    // 更新最近一次交互时间，即对端活跃时间。
    server.repl_transfer_lastio = server.unixtime;
    return sdsnew(buf);
}

/* Send a pre-formatted multi-bulk command to the connection. */
// 发送预格式化的multi-bulk命令到conn。如果报错了返回'-'开头的err信息。
// 注意这里都是同步写，即slave连接master阶段命令发送都是同步写处理的。
char* sendCommandRaw(connection *conn, sds cmd) {
    if (connSyncWrite(conn,cmd,sdslen(cmd),server.repl_syncio_timeout*1000) == -1) {
        return sdscatprintf(sdsempty(),"-Writing to master: %s",
                connGetLastError(conn));
    }
    return NULL;
}

/* Compose a multi-bulk command and send it to the connection.
 * Used to send AUTH and REPLCONF commands to the master before starting the
 * replication.
 *
 * Takes a list of char* arguments, terminated by a NULL argument.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
// 构造一个multi-bulk命令，然后发送到conn中。主要用于slave在开始复制时发送AUTH和REPLCONF命令给master。
// 可变数量的char*类型参数，NULL参数表示结束。函数报错会返回'-'开头的err信息。
char *sendCommand(connection *conn, ...) {
    va_list ap;
    sds cmd = sdsempty();
    sds cmdargs = sdsempty();
    size_t argslen = 0;
    char *arg;

    /* Create the command to send to the master, we use redis binary
     * protocol to make sure correct arguments are sent. This function
     * is not safe for all binary data. */
    // 创建要发送给master的命令，使用redis二进制协议来确保发送正确的参数。这个函数并不是对于所有二进制数据都安全。
    va_start(ap,conn);
    while(1) {
        // 遍历获取参数，如果是NULL说明处理晚了，跳出循环。
        arg = va_arg(ap, char*);
        if (arg == NULL) break;
        // 构造命令参数部分的字符串，除了multi-bulk数量外的部分。
        cmdargs = sdscatprintf(cmdargs,"$%zu\r\n%s\r\n",strlen(arg),arg);
        argslen++;
    }

    // multi-bulk数量，加上命令参数字符串，一起组成需要发送的数据。
    cmd = sdscatprintf(cmd,"*%zu\r\n",argslen);
    cmd = sdscatsds(cmd,cmdargs);
    sdsfree(cmdargs);

    va_end(ap);
    // 发送命令字符串到conn
    char* err = sendCommandRaw(conn, cmd);
    sdsfree(cmd);
    if(err)
        return err;
    return NULL;
}

/* Compose a multi-bulk command and send it to the connection. 
 * Used to send AUTH and REPLCONF commands to the master before starting the
 * replication.
 *
 * argv_lens is optional, when NULL, strlen is used.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
// 构造一个multi-bulk命令，然后发送到conn中。主要用于slave在开始复制时发送AUTH和REPLCONF命令给master。
// argv_lens参数是可选的，当它为NULL时，使用strlen计算长度。
char *sendCommandArgv(connection *conn, int argc, char **argv, size_t *argv_lens) {
    sds cmd = sdsempty();
    char *arg;
    int i;

    /* Create the command to send to the master. */
    // 构造第一部分multi-bulk len结构。
    cmd = sdscatfmt(cmd,"*%i\r\n",argc);
    for (i=0; i<argc; i++) {
        int len;
        // 遍历，对每个参数构造bulk，追加到cmd中
        arg = argv[i];
        len = argv_lens ? argv_lens[i] : strlen(arg);
        cmd = sdscatfmt(cmd,"$%i\r\n",len);
        cmd = sdscatlen(cmd,arg,len);
        cmd = sdscatlen(cmd,"\r\n",2);
    }
    // 发送命令到conn
    char* err = sendCommandRaw(conn, cmd);
    sdsfree(cmd);
    if (err)
        return err;
    return NULL;
}

/* Try a partial resynchronization with the master if we are about to reconnect.
 * If there is no cached master structure, at least try to issue a
 * "PSYNC ? -1" command in order to trigger a full resync using the PSYNC
 * command in order to obtain the master replid and the master replication
 * global offset.
 *
 * This function is designed to be called from syncWithMaster(), so the
 * following assumptions are made:
 *
 * 1) We pass the function an already connected socket "fd".
 * 2) This function does not close the file descriptor "fd". However in case
 *    of successful partial resynchronization, the function will reuse
 *    'fd' as file descriptor of the server.master client structure.
 *
 * The function is split in two halves: if read_reply is 0, the function
 * writes the PSYNC command on the socket, and a new function call is
 * needed, with read_reply set to 1, in order to read the reply of the
 * command. This is useful in order to support non blocking operations, so
 * that we write, return into the event loop, and read when there are data.
 *
 * When read_reply is 0 the function returns PSYNC_WRITE_ERR if there
 * was a write error, or PSYNC_WAIT_REPLY to signal we need another call
 * with read_reply set to 1. However even when read_reply is set to 1
 * the function may return PSYNC_WAIT_REPLY again to signal there were
 * insufficient data to read to complete its work. We should re-enter
 * into the event loop and wait in such a case.
 *
 * The function returns:
 *
 * PSYNC_CONTINUE: If the PSYNC command succeeded and we can continue.
 * PSYNC_FULLRESYNC: If PSYNC is supported but a full resync is needed.
 *                   In this case the master replid and global replication
 *                   offset is saved.
 * PSYNC_NOT_SUPPORTED: If the server does not understand PSYNC at all and
 *                      the caller should fall back to SYNC.
 * PSYNC_WRITE_ERROR: There was an error writing the command to the socket.
 * PSYNC_WAIT_REPLY: Call again the function with read_reply set to 1.
 * PSYNC_TRY_LATER: Master is currently in a transient error condition.
 *
 * Notable side effects:
 *
 * 1) As a side effect of the function call the function removes the readable
 *    event handler from "fd", unless the return value is PSYNC_WAIT_REPLY.
 * 2) server.master_initial_offset is set to the right value according
 *    to the master reply. This will be used to populate the 'server.master'
 *    structure replication offset.
 */
// 如果我们是重新连接，尝试与master进行部分重同步。
// 如果没有cached master，尝试发出"PSYNC ? -1"命令触发全量重同步，进而获取master的 replid 和 全局复制offset。
// 这个函数是用于在syncWithMaster()中调用的，所以我们做了以下假设：
//      1、我们传递给函数的是一个已经建立好socket连接的"fd"。
//      2、该函数不会关闭这个"fd"。但是在部分重同步完成后，该函数会复用这个"fd"作为server.master client结构中的连接"fd"。
// 函数的功能分为两个部分：
//      1、read_reply为0时，构造相应的PSYNC命令发送到master，然后返回并等待回复。
//      2、有回复需要处理时，会使用read_reply为1再调用这个函数来处理回复。
// 这样在支持非阻塞操作时是很有用的，我们发送命令后返回事件循环等待回复，有回复时再通知我们进行处理（耗时很短），处理完就立即发送再进入等待。

// 当read_reply为0时，如果发送了写错误，会返回PSYNC_WRITE_ERR。
// 发送成功会返回PSYNC_WAIT_REPLY，让我们等待回复，下一次使用read_reply为1再调用这个函数处理回复。
// 但是，即使read_reply=1调用时，函数也可能再次返回PSYNC_WAIT_REPLY，表示没有足够的数据读取。此时，我们应该重新进入事件循环并等待数据。

// 函数返回：
//  PSYNC_CONTINUE，表示PSYNC命令成功，即部分重同步被接受了，我们可以继续处理。
//  PSYNC_FULLRESYNC，表示支持PSYNC，但需要进行全量的重同步，之后保存master的replid和全局复制offset，供后面部分重同步使用。
//  PSYNC_NOT_SUPPORTED，表示master不支持PSYNC，需要slave通过SYNC来同步。
//  PSYNC_WRITE_ERROR，表示数据写入conn socket出错。
//  PSYNC_WAIT_REPLY，等待master的回复，下次使用read_reply=1重新调用这个函数处理master的回复。
//  PSYNC_TRY_LATER，master暂时处于错误状态，一会再进行尝试。
// 函数明显的副作用：
//  1、除非返回值是PSYNC_WAIT_REPLY，否则会移除"fd"的可读事件监听。
//  2、server.master_initial_offset将根据master的回复设置为正确的值，这将用于填充server.master结构的复制offset。
#define PSYNC_WRITE_ERROR 0
#define PSYNC_WAIT_REPLY 1
#define PSYNC_CONTINUE 2
#define PSYNC_FULLRESYNC 3
#define PSYNC_NOT_SUPPORTED 4
#define PSYNC_TRY_LATER 5
int slaveTryPartialResynchronization(connection *conn, int read_reply) {
    char *psync_replid;
    char psync_offset[32];
    sds reply;

    /* Writing half */
    // read_reply=0调用，表示不是读取回复，而是发送PSYNC命令。
    if (!read_reply) {
        /* Initially set master_initial_offset to -1 to mark the current
         * master replid and offset as not valid. Later if we'll be able to do
         * a FULL resync using the PSYNC command we'll set the offset at the
         * right value, so that this information will be propagated to the
         * client structure representing the master into server.master. */
        // 初始设置master_initial_offset为-1，表示当前master的 replid 和 offset 都是无效的。
        // 后面如果我们通过PSYNC做一个全量的重同步，我们将重新设置offset为正确值，以便信息传递到master的client连接server.master中。
        server.master_initial_offset = -1;

        if (server.cached_master) {
            // 如果有缓存的master，使用该master的 replid 和 offset进行同步。
            psync_replid = server.cached_master->replid;
            snprintf(psync_offset,sizeof(psync_offset),"%lld", server.cached_master->reploff+1);
            serverLog(LL_NOTICE,"Trying a partial resynchronization (request %s:%s).", psync_replid, psync_offset);
        } else {
            // 没有缓存的master，需要通过PSYNC进行全量重同步。
            serverLog(LL_NOTICE,"Partial resynchronization not possible (no cached master)");
            psync_replid = "?";
            memcpy(psync_offset,"-1",3);
        }

        /* Issue the PSYNC command, if this is a master with a failover in
         * progress then send the failover argument to the replica to cause it
         * to become a master */
        // 构造PSYNC命令发出。如果当前是一个master且正在进行故障转移，发送failover参数给对应的slave来让它成为master。
        if (server.failover_state == FAILOVER_IN_PROGRESS) {
            reply = sendCommand(conn,"PSYNC",psync_replid,psync_offset,"FAILOVER",NULL);
        } else {
            reply = sendCommand(conn,"PSYNC",psync_replid,psync_offset,NULL);
        }

        if (reply != NULL) {
            serverLog(LL_WARNING,"Unable to send PSYNC to master: %s",reply);
            sdsfree(reply);
            connSetReadHandler(conn, NULL);
            return PSYNC_WRITE_ERROR;
        }
        return PSYNC_WAIT_REPLY;
    }

    /* Reading half */
    // 读取master回复进行处理。
    reply = receiveSynchronousResponse(conn);
    if (sdslen(reply) == 0) {
        /* The master may send empty newlines after it receives PSYNC
         * and before to reply, just to keep the connection alive. */
        // 为了保持conn连接活着，master在收到PSYNC后，回复前，可能发送空行数据。这里什么都不做，等待下一次收到回复再处理。
        sdsfree(reply);
        return PSYNC_WAIT_REPLY;
    }

    // 开始处理读到的数据。这里先删除conn的读事件监听。
    connSetReadHandler(conn, NULL);

    // 收到回复是"+FULLRESYNC ID OFFSET"，表示要全量重新同步。
    if (!strncmp(reply,"+FULLRESYNC",11)) {
        char *replid = NULL, *offset = NULL;

        /* FULL RESYNC, parse the reply in order to extract the replid
         * and the replication offset. */
        // 全量重同步，解析master的回复，提取replid和复制offset。
        // replid指向复制ID字符串的第一个字符。offset指向复制偏移的第一个字符。
        replid = strchr(reply,' ');
        if (replid) {
            replid++;
            offset = strchr(replid,' ');
            if (offset) offset++;
        }
        if (!replid || !offset || (offset-replid-1) != CONFIG_RUN_ID_SIZE) {
            // offset-replid-1，最后-1是因为复制ID后有一个空格，计算ID长度需要去掉。
            serverLog(LL_WARNING,
                "Master replied with wrong +FULLRESYNC syntax.");
            /* This is an unexpected condition, actually the +FULLRESYNC
             * reply means that the master supports PSYNC, but the reply
             * format seems wrong. To stay safe we blank the master
             * replid to make sure next PSYNCs will fail. */
            // 这是非预期的情况，事实上master回复"+FULLRESYNC"表示支持PSYNC，但是回复格式不对。
            // 为了安全，我们把master的replid置空，确保下一次的PSYNCs失败。
            memset(server.master_replid,0,CONFIG_RUN_ID_SIZE+1);
        } else {
            // master传的复制ID格式对的，这里写入server.master_replid中。
            // 然后将offset解析成数字写入server.master_initial_offset。
            memcpy(server.master_replid, replid, offset-replid-1);
            server.master_replid[CONFIG_RUN_ID_SIZE] = '\0';
            server.master_initial_offset = strtoll(offset,NULL,10);
            serverLog(LL_NOTICE,"Full resync from master: %s:%lld",
                server.master_replid,
                server.master_initial_offset);
        }
        /* We are going to full resync, discard the cached master structure. */
        // 将要进行全量重同步，这里我们释放掉cached master。
        // 因为cached master是用于重连后部分重同步的，这里都要全量重同步了，所以这个结构也没什么用了。
        replicationDiscardCachedMaster();
        sdsfree(reply);
        return PSYNC_FULLRESYNC;
    }

    // 收到回复是"+CONTINUE ID OFFSET"，表示部分重同步被接受了。
    if (!strncmp(reply,"+CONTINUE",9)) {
        /* Partial resync was accepted. */
        serverLog(LL_NOTICE,
            "Successful partial resynchronization with master.");

        /* Check the new replication ID advertised by the master. If it
         * changed, we need to set the new ID as primary ID, and set or
         * secondary ID as the old master ID up to the current offset, so
         * that our sub-slaves will be able to PSYNC with us after a
         * disconnection. */
        // 检查master通知的新复制ID，如果有变化，我们需要设置新的ID为主ID，设置老的复制ID为辅助ID，对应的老的offset为当前offset+1
        // 这样我们的sub-slaves将能够在断开重连时使用PSYNC从我们这同步数据。
        char *start = reply+10;
        char *end = reply+9;
        // end定位到结束符位置
        while(end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;
        if (end-start == CONFIG_RUN_ID_SIZE) {
            // 提取新的ID
            char new[CONFIG_RUN_ID_SIZE+1];
            memcpy(new,start,CONFIG_RUN_ID_SIZE);
            new[CONFIG_RUN_ID_SIZE] = '\0';

            if (strcmp(new,server.cached_master->replid)) {
                /* Master ID changed. */
                // 对比新ID和老的cached master的replid，如果新ID有变化，进行处理
                serverLog(LL_WARNING,"Master replication ID changed to %s",new);

                /* Set the old ID as our ID2, up to the current offset+1. */
                // 将老的ID放到server.replid2中，且second offset设为当前offset+1
                memcpy(server.replid2,server.cached_master->replid,
                    sizeof(server.replid2));
                server.second_replid_offset = server.master_repl_offset+1;

                /* Update the cached master ID and our own primary ID to the
                 * new one. */
                // 更新我们主要复制ID以及cached master的复制ID为新的ID。
                memcpy(server.replid,new,sizeof(server.replid));
                memcpy(server.cached_master->replid,new,sizeof(server.replid));

                /* Disconnect all the sub-slaves: they need to be notified. */
                // 断开所有的sub-slaves，通知他们重新连接，重新进行同步。
                disconnectSlaves();
            }
        }

        /* Setup the replication to continue. */
        // 如果能够使用部分重同步，这里设置复制传输继续，会重用cached master。
        sdsfree(reply);
        replicationResurrectCachedMaster(conn);

        /* If this instance was restarted and we read the metadata to
         * PSYNC from the persistence file, our replication backlog could
         * be still not initialized. Create it. */
        // 如果当前节点重启了，我们从持久化文件读取到了PSYNC的信息，当前复制backlog可能还没有初始化，这里创建它。
        if (server.repl_backlog == NULL) createReplicationBacklog();
        return PSYNC_CONTINUE;
    }

    /* If we reach this point we received either an error (since the master does
     * not understand PSYNC or because it is in a special state and cannot
     * serve our request), or an unexpected reply from the master.
     *
     * Return PSYNC_NOT_SUPPORTED on errors we don't understand, otherwise
     * return PSYNC_TRY_LATER if we believe this is a transient error. */

    // 执行到这里，说明从master收到了一个error，或者收到非预期的回复。如不支持PSYNC，或当前状态不能处理我们请求。
    // 返回 PSYNC_NOT_SUPPORTED 表示master不支持PSYNC。返回PSYNC_TRY_LATER 表示master暂时不能处理，稍后再试。
    if (!strncmp(reply,"-NOMASTERLINK",13) ||
        !strncmp(reply,"-LOADING",8))
    {
        serverLog(LL_NOTICE,
            "Master is currently unable to PSYNC "
            "but should be in the future: %s", reply);
        sdsfree(reply);
        return PSYNC_TRY_LATER;
    }

    if (strncmp(reply,"-ERR",4)) {
        /* If it's not an error, log the unexpected event. */
        // 不是返回的"-ERR"，非预期的回复，打印一条日志。
        serverLog(LL_WARNING,
            "Unexpected reply to PSYNC from master: %s", reply);
    } else {
        serverLog(LL_NOTICE,
            "Master does not support PSYNC or is in "
            "error state (reply: %s)", reply);
    }
    sdsfree(reply);
    // 不支持PSYNC，所以cached master没用，可以释放掉了
    replicationDiscardCachedMaster();
    return PSYNC_NOT_SUPPORTED;
}

/* This handler fires when the non blocking connect was able to
 * establish a connection with the master. */
// 当slave主动与master建立conn连接时，连接建立会触发事件可写，进而调用ae_handler，然后处理conn_handler即当前函数。
// 只有第一次创建连接时，才会将本函数作为写事件监听函数（conn连接事件是可写触发）。
// 连接建立后就只监听可读事件了，本函数后面都是在收到master数据，执行读处理handler，然后一直向后推进的。
// 读取数据，以及需要发命令给master都使用的同步方式处理的。
void syncWithMaster(connection *conn) {
    char tmpfile[256], *err = NULL;
    int dfd = -1, maxtries = 5;
    int psync_result;

    /* If this event fired after the user turned the instance into a master
     * with SLAVEOF NO ONE we must just return ASAP. */
    // 如果这个事件在节点变为master，但没有slaves时触发，我们尽早返回。
    if (server.repl_state == REPL_STATE_NONE) {
        connClose(conn);
        return;
    }

    /* Check for errors in the socket: after a non blocking connect() we
     * may find that the socket is in error state. */
    // 检查socket中的error：使用非阻塞connect()，socket可能处于err状态。
    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_WARNING,"Error condition on socket for SYNC: %s",
                connGetLastError(conn));
        goto error;
    }

    /* Send a PING to check the master is able to reply without errors. */
    // 这里REPL级别连接建立阶段，当前底层的conn连接已经建立了，我们发送PING检查master是否能正常收到并回复。
    // 连接分层：socket->conn->REPL
    if (server.repl_state == REPL_STATE_CONNECTING) {
        serverLog(LL_NOTICE,"Non blocking connect for SYNC fired the event.");
        /* Delete the writable event so that the readable event remains
         * registered and we can wait for the PONG reply. */
        // 删除可写事件，增加可读事件监听，等到master回复的PONG消息，更新REPL连接状态。
        connSetReadHandler(conn, syncWithMaster);
        connSetWriteHandler(conn, NULL);
        server.repl_state = REPL_STATE_RECEIVE_PING_REPLY;
        /* Send the PING, don't check for errors at all, we have the timeout
         * that will take care about this. */
        // 发送PING指令，不检查errors，在timeout超时处理时，我们会处理
        err = sendCommand(conn,"PING",NULL);
        if (err) goto write_error;
        return;
    }

    /* Receive the PONG command. */
    // REPL级别连接建立阶段，前面发送PING，设置了conn可读事件监听，执行到这里说明conn可读，同步读取PONG回复数据。
    if (server.repl_state == REPL_STATE_RECEIVE_PING_REPLY) {
        // 同步读取数据返回
        err = receiveSynchronousResponse(conn);

        /* We accept only two replies as valid, a positive +PONG reply
         * (we just check for "+") or an authentication error.
         * Note that older versions of Redis replied with "operation not
         * permitted" instead of using a proper error code, so we test
         * both. */
        // 我们仅接受两个有效的回复，"+PONG"回复(我们只检查'+')或身份验证错误。
        // 注意，旧版本的Redis回复为"operation not permitted"，而不是使用合适的error code，所以我们对这两个都进行处理。
        if (err[0] != '+' &&
            strncmp(err,"-NOAUTH",7) != 0 &&
            strncmp(err,"-NOPERM",7) != 0 &&
            strncmp(err,"-ERR operation not permitted",28) != 0)
        {
            // 只有这几种情况认为是master传回了err，其他情况都认为返回ok？
            serverLog(LL_WARNING,"Error reply to PING from master: '%s'",err);
            sdsfree(err);
            goto error;
        } else {
            serverLog(LL_NOTICE,
                "Master replied to PING, replication can continue...");
        }
        sdsfree(err);
        err = NULL;
        // 认为连接是通的，可以进行与master握手了
        server.repl_state = REPL_STATE_SEND_HANDSHAKE;
    }

    // handshake阶段
    if (server.repl_state == REPL_STATE_SEND_HANDSHAKE) {
        /* AUTH with the master if required. */
        // 如果master需要密码验证，这里向master发送AUTH命令处理
        if (server.masterauth) {
            char *args[3] = {"AUTH",NULL,NULL};
            size_t lens[3] = {4,0,0};
            int argc = 1;
            if (server.masteruser) {
                args[argc] = server.masteruser;
                lens[argc] = strlen(server.masteruser);
                argc++;
            }
            args[argc] = server.masterauth;
            lens[argc] = sdslen(server.masterauth);
            argc++;
            // 构造AUTH命令发送
            err = sendCommandArgv(conn, argc, args, lens);
            if (err) goto write_error;
        }

        /* Set the slave port, so that Master's INFO command can list the
         * slave listening port correctly. */
        // 通过REPLCONF命令告知master我们的监听端口。从而master的INFO命令可以正确的列出slaves的监听端口。
        {
            int port;
            if (server.slave_announce_port)
                port = server.slave_announce_port;
            else if (server.tls_replication && server.tls_port)
                port = server.tls_port;
            else
                port = server.port;
            sds portstr = sdsfromlonglong(port);
            // 构造REPLCONF命令，发送监听端口信息
            err = sendCommand(conn,"REPLCONF",
                    "listening-port",portstr, NULL);
            sdsfree(portstr);
            if (err) goto write_error;
        }

        /* Set the slave ip, so that Master's INFO command can list the
         * slave IP address port correctly in case of port forwarding or NAT.
         * Skip REPLCONF ip-address if there is no slave-announce-ip option set. */
        // 通过REPLCONF命令告知master我们的IP地址，以便在端口转发或NAT的情况下，Master的INFO命令可以正确列出slave的IP地址端口。
        // 如果我们没有设置slave-announce-ip，则不发送master。
        if (server.slave_announce_ip) {
            err = sendCommand(conn,"REPLCONF",
                    "ip-address",server.slave_announce_ip, NULL);
            if (err) goto write_error;
        }

        /* Inform the master of our (slave) capabilities.
         *
         * EOF: supports EOF-style RDB transfer for diskless replication.
         * PSYNC2: supports PSYNC v2, so understands +CONTINUE <new repl ID>.
         *
         * The master will ignore capabilities it does not understand. */
        // 通过REPLCONF命令告知master，我们可以支持的复制数据传输方式。
        // EOF：支持EOF风格的RDB不落盘复制传输。
        // PSYNC2：支持v2版本的PSYNC，即可处理 +CONTINUE <new repl ID> 命令。
        // 但是如果master不支持对应方式，将会忽略，可能就使用最原始的先落盘在读取传输。
        err = sendCommand(conn,"REPLCONF",
                "capa","eof","capa","psync2",NULL);
        if (err) goto write_error;

        // handshake，我们发送了好几个命令过去，现在要挨个等待处理回复了。
        server.repl_state = REPL_STATE_RECEIVE_AUTH_REPLY;
        return;
    }

    // 如果我们不需要AUTH验证，则跳过这一阶段，进入等待port的回复阶段。
    if (server.repl_state == REPL_STATE_RECEIVE_AUTH_REPLY && !server.masterauth)
        server.repl_state = REPL_STATE_RECEIVE_PORT_REPLY;

    /* Receive AUTH reply. */
    // 如果需要AUTH，这里处理AUTH回复
    if (server.repl_state == REPL_STATE_RECEIVE_AUTH_REPLY) {
        // 同步读取回复。判断第一个字符，如果是'-'说明有err。
        err = receiveSynchronousResponse(conn);
        if (err[0] == '-') {
            serverLog(LL_WARNING,"Unable to AUTH to MASTER: %s",err);
            sdsfree(err);
            goto error;
        }
        sdsfree(err);
        err = NULL;
        // 没有err，连接状态进入等待port的回复
        server.repl_state = REPL_STATE_RECEIVE_PORT_REPLY;
        return;
    }

    /* Receive REPLCONF listening-port reply. */
    // 处理master对port的回复
    if (server.repl_state == REPL_STATE_RECEIVE_PORT_REPLY) {
        // 同步读。判断第一个字符是'-'，则说明有err。
        err = receiveSynchronousResponse(conn);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF listening-port. */
        // 这里对应err，只打印日志并忽略掉。因为并不是所有的redis版本都支持REPLCONF listening-port命令。
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                "REPLCONF listening-port: %s", err);
        }
        sdsfree(err);
        // 进入等待ip回复阶段
        server.repl_state = REPL_STATE_RECEIVE_IP_REPLY;
        return;
    }

    // 如果本机没有slave_announce_ip，则没有发ip给master，跳过等待ip回复阶段，进入等待capabilities回复阶段。
    if (server.repl_state == REPL_STATE_RECEIVE_IP_REPLY && !server.slave_announce_ip)
        server.repl_state = REPL_STATE_RECEIVE_CAPA_REPLY;

    /* Receive REPLCONF ip-address reply. */
    // 处理master对ip的回复
    if (server.repl_state == REPL_STATE_RECEIVE_IP_REPLY) {
        err = receiveSynchronousResponse(conn);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF listening-port. */
        // 同步读，检查err。同样忽略错误，因为命令不是所有版本支持。
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                "REPLCONF ip-address: %s", err);
        }
        sdsfree(err);
        // 进入等待capabilities回复阶段。
        server.repl_state = REPL_STATE_RECEIVE_CAPA_REPLY;
        return;
    }

    /* Receive CAPA reply. */
    // 处理master对CAPA回复
    if (server.repl_state == REPL_STATE_RECEIVE_CAPA_REPLY) {
        err = receiveSynchronousResponse(conn);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF capa. */
        // 同步读，检查err。忽略错误，命令不是所有版本支持。
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                  "REPLCONF capa: %s", err);
        }
        sdsfree(err);
        err = NULL;
        // 准备进入PSYNC阶段，因为不需要等待master消息，所以这里不需要return，代码执行直接向前推进就可以。
        server.repl_state = REPL_STATE_SEND_PSYNC;
    }

    /* Try a partial resynchonization. If we don't have a cached master
     * slaveTryPartialResynchronization() will at least try to use PSYNC
     * to start a full resynchronization so that we get the master replid
     * and the global offset, to try a partial resync at the next
     * reconnection attempt. */
    // 尝试进行部分重同步。
    // 如果我们没有一个cached master，slaveTryPartialResynchronization()将尝试使用PSYNC来开启一个全量重同步。
    // 全量重同步完了之后，我们会从master那获取到replid和全局offset，从而在下一次尝试重连时就可以进行部分重同步了。
    if (server.repl_state == REPL_STATE_SEND_PSYNC) {
        // 尝试进行部分重同步。第二个参数为0，表示向master发起PSYNC命令。
        if (slaveTryPartialResynchronization(conn,0) == PSYNC_WRITE_ERROR) {
            err = sdsnew("Write error sending the PSYNC command.");
            abortFailover("Write error to failover target");
            goto write_error;
        }
        // 等待PSYNC命令的回复。
        server.repl_state = REPL_STATE_RECEIVE_PSYNC_REPLY;
        return;
    }

    /* If reached this point, we should be in REPL_STATE_RECEIVE_PSYNC. */
    // 代码执行到这里，我们应该处于等待PSYNC回复的阶段，否则出bug了。
    if (server.repl_state != REPL_STATE_RECEIVE_PSYNC_REPLY) {
        serverLog(LL_WARNING,"syncWithMaster(): state machine error, "
                             "state should be RECEIVE_PSYNC but is %d",
                             server.repl_state);
        goto error;
    }

    // 再次进来应该是conn可读，我们这里正等待PSYNC回复。
    // 调用这个函数处理master的回复消息，第二个参数为1表示我们处理回复，而不是发送PSYNC命令。
    psync_result = slaveTryPartialResynchronization(conn,1);
    // 如果返回PSYNC_WAIT_REPLY，表示没读到，返回，等待下次再读。
    if (psync_result == PSYNC_WAIT_REPLY) return; /* Try again later... */

    // 对于几种返回结果的处理：
    // PSYNC_WAIT_REPLY，表示我们需要读取master的回复，进行后面的REPL握手环节。
    //      所以我们conn的读处理handler还是当前函数，直接返回等待读事件再次触发。
    // PSYNC_TRY_LATER，表示master可能临时出错或无法服务请求，我们需要后面从头开始再进行握手。（conn的handler已经在前面置NULL了）
    //      所以我们跳到error处理，清理相关操作退出。等待下一次定时任务再与master连接？
    // PSYNC_CONTINUE，表示我们PSYNC已被master接受，且我们在前面已经把conn复用给master连接了，且设置了命令读取和回复的handler监听。
    //      所以这里我们直接返回即可，后面的streams复制处理都在正常的client连接命令执行流程中。
    // PSYNC_FULLRESYNC，表示master支持PSYNC，但现在需要全量重同步，注意前面我们已经把conn的handler置空了。
    //      所以这里我们对于自己的slave需要断开所有的连接，并释放repl_backlog，让我们的slave来进行全量重同步（因为可能数据集都不一样了）。
    //      另外还需要安装数据处理handler（复用的当前conn，返回后就不会再进这个函数了），对于是否落盘传输的处理，对于复制状态的更新等。
    // PSYNC_NOT_SUPPORTED，表示master不支持PSYNC，我们只能进行全量重同步了。
    //      所以我们需要先同步发送一个SYNC命令通知master开始传数据，后面的处理方式与PSYNC_FULLRESYNC是一致的了。

    /* Check the status of the planned failover. We expect PSYNC_CONTINUE,
     * but there is nothing technically wrong with a full resync which
     * could happen in edge cases. */
    // 检查是否正在进行计划的故障转移。我们期望PSYNC_CONTINUE，但是如果结果是需要全量重同步(在某些情况下会发生)，这在技术上也是没问题的。
    if (server.failover_state == FAILOVER_IN_PROGRESS) {
        if (psync_result == PSYNC_CONTINUE || psync_result == PSYNC_FULLRESYNC) {
            // 如果我们正在进行故障转移，收到可以从master进行数据同步，则故障转移完成？清除故障转移状态。
            clearFailoverState();
        } else {
            abortFailover("Failover target rejected psync request");
            return;
        }
    }

    /* If the master is in an transient error, we should try to PSYNC
     * from scratch later, so go to the error path. This happens when
     * the server is loading the dataset or is not connected with its
     * master and so forth. */
    // 如果master处于暂时错误，我们稍后应该再从头开始进行PSYNC，所以这里跳到error处理。
    // 一般当master正在加载数据 或 没有与它的master连接时，会发生这种情况。
    if (psync_result == PSYNC_TRY_LATER) goto error;

    /* Note: if PSYNC does not return WAIT_REPLY, it will take care of
     * uninstalling the read handler from the file descriptor. */
    // 注意：如果PSYNC不是返回WAIT_REPLY，前面我们会将conn的读处理handler（当前函数）移除掉。

    // 如果是PSYNC_CONTINUE，我们会将conn给server.master复用，并安装readQueryFromClient、sendReplyToClient读写处理handler。
    // 这里建立起了PSYNC连接就直接返回，后面通过server.master来进行部分strams同步处理，slave其实也就是解析master传的命令执行。
    if (psync_result == PSYNC_CONTINUE) {
        serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Master accepted a Partial Resynchronization.");
        if (server.supervised_mode == SUPERVISED_SYSTEMD) {
            redisCommunicateSystemd("STATUS=MASTER <-> REPLICA sync: Partial Resynchronization accepted. Ready to accept connections in read-write mode.\n");
        }
        return;
    }

    /* PSYNC failed or is not supported: we want our slaves to resync with us
     * as well, if we have any sub-slaves. The master may transfer us an
     * entirely different data set and we have no way to incrementally feed
     * our slaves after that. */
    // 当PSYNC失败或不支持时，如果我们有sub-slaves，我们希望它们也与我们重新同步。
    // 因为master可能向我们传输完全不同的数据集，后续我们没办法再增量的为sub-slaves提供数据了。
    // 这里强制我们的slaves断开连接，然后进行重新连接来全量重新同步数据。
    disconnectSlaves(); /* Force our slaves to resync with us as well. */
    // 释放掉server.repl_backlog，不允许我们的slaves使用PSYNC，从而进行全量同步。
    freeReplicationBacklog(); /* Don't allow our chained slaves to PSYNC. */

    /* Fall back to SYNC if needed. Otherwise psync_result == PSYNC_FULLRESYNC
     * and the server.master_replid and master_initial_offset are
     * already populated. */
    // 如果PSYNC不支持，我们需要使用SYNC。这里发送SYNC指令给master，从而获取复制的replid和offset。
    // 如果是PSYNC_FULLRESYNC，支持PSYNC但需要全量重同步，我们在前面函数中已经拿到了复制的replid和offset，这里不需要再处理了。
    if (psync_result == PSYNC_NOT_SUPPORTED) {
        serverLog(LL_NOTICE,"Retrying with SYNC...");
        // 同步发送SYNC给master
        if (connSyncWrite(conn,"SYNC\r\n",6,server.repl_syncio_timeout*1000) == -1) {
            serverLog(LL_WARNING,"I/O error writing to MASTER: %s",
                strerror(errno));
            goto error;
        }
    }

    /* Prepare a suitable temp file for bulk transfer */
    // 如果不能使用无盘加载，只能先将传过来的数据存储到文件，再进行读取加载。这里需要创建打开一个临时文件用于写。
    if (!useDisklessLoad()) {
        while(maxtries--) {
            snprintf(tmpfile,256,
                "temp-%d.%ld.rdb",(int)server.unixtime,(long int)getpid());
            // 创建文件。
            dfd = open(tmpfile,O_CREAT|O_WRONLY|O_EXCL,0644);
            if (dfd != -1) break;
            sleep(1);
        }
        if (dfd == -1) {
            serverLog(LL_WARNING,"Opening the temp file needed for MASTER <-> REPLICA synchronization: %s",strerror(errno));
            goto error;
        }
        // 设置RDB文件传输的临时file 和 临时fd。
        server.repl_transfer_tmpfile = zstrdup(tmpfile);
        server.repl_transfer_fd = dfd;
    }

    /* Setup the non blocking download of the bulk file. */
    // 启动非阻塞下载RDB文件存到本地。
    if (connSetReadHandler(conn, readSyncBulkPayload)
            == C_ERR)
    {
        char conninfo[CONN_INFO_LEN];
        serverLog(LL_WARNING,
            "Can't create readable event for SYNC: %s (%s)",
            strerror(errno), connGetInfo(conn, conninfo, sizeof(conninfo)));
        goto error;
    }

    // 设置传输状态，返回。因为当前conn的handler已设置为readSyncBulkPayload，后面不会再进入这个函数了。
    server.repl_state = REPL_STATE_TRANSFER;
    server.repl_transfer_size = -1;
    server.repl_transfer_read = 0;
    server.repl_transfer_last_fsync_off = 0;
    server.repl_transfer_lastio = server.unixtime;
    return;

error:
    // 出错了，关闭连接，重置相关属性。
    if (dfd != -1) close(dfd);
    connClose(conn);
    server.repl_transfer_s = NULL;
    if (server.repl_transfer_fd != -1)
        close(server.repl_transfer_fd);
    if (server.repl_transfer_tmpfile)
        zfree(server.repl_transfer_tmpfile);
    server.repl_transfer_tmpfile = NULL;
    server.repl_transfer_fd = -1;
    server.repl_state = REPL_STATE_CONNECT;
    return;

write_error: /* Handle sendCommand() errors. */
    serverLog(LL_WARNING,"Sending command to master in replication handshake: %s", err);
    sdsfree(err);
    goto error;
}

// slave主动与master建立连接。
int connectWithMaster(void) {
    // 创建connection
    server.repl_transfer_s = server.tls_replication ? connCreateTLS() : connCreateSocket();
    // 建立连接，安装连接事件处理函数。连接建立后会执行 syncWithMaster 同步数据。
    if (connConnect(server.repl_transfer_s, server.masterhost, server.masterport,
                NET_FIRST_BIND_ADDR, syncWithMaster) == C_ERR) {
        serverLog(LL_WARNING,"Unable to connect to MASTER: %s",
                connGetLastError(server.repl_transfer_s));
        connClose(server.repl_transfer_s);
        server.repl_transfer_s = NULL;
        return C_ERR;
    }


    // 更新状态信息
    server.repl_transfer_lastio = server.unixtime;
    server.repl_state = REPL_STATE_CONNECTING;
    serverLog(LL_NOTICE,"MASTER <-> REPLICA sync started");
    return C_OK;
}

/* This function can be called when a non blocking connection is currently
 * in progress to undo it.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void undoConnectWithMaster(void) {
    connClose(server.repl_transfer_s);
    server.repl_transfer_s = NULL;
}

/* Abort the async download of the bulk dataset while SYNC-ing with master.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void replicationAbortSyncTransfer(void) {
    serverAssert(server.repl_state == REPL_STATE_TRANSFER);
    undoConnectWithMaster();
    if (server.repl_transfer_fd!=-1) {
        close(server.repl_transfer_fd);
        bg_unlink(server.repl_transfer_tmpfile);
        zfree(server.repl_transfer_tmpfile);
        server.repl_transfer_tmpfile = NULL;
        server.repl_transfer_fd = -1;
    }
}

/* This function aborts a non blocking replication attempt if there is one
 * in progress, by canceling the non-blocking connect attempt or
 * the initial bulk transfer.
 *
 * If there was a replication handshake in progress 1 is returned and
 * the replication state (server.repl_state) set to REPL_STATE_CONNECT.
 *
 * Otherwise zero is returned and no operation is performed at all. */
// 如果有一个slave正在进行非阻塞的握手连接，或者在进行RDB数据传输，调用这个函数来取消操作。
int cancelReplicationHandshake(int reconnect) {
    if (server.repl_state == REPL_STATE_TRANSFER) {
        // 如果正在进行rdb数据传输，取消操作，关闭conn，重置transfer相关属性。
        replicationAbortSyncTransfer();
        server.repl_state = REPL_STATE_CONNECT;
    } else if (server.repl_state == REPL_STATE_CONNECTING ||
               slaveIsInHandshakeState())
    {
        // 如果slave处于SYNC连接的handshake阶段，取消操作，重置连接
        undoConnectWithMaster();
        server.repl_state = REPL_STATE_CONNECT;
    } else {
        return 0;
    }

    // 如果入参指定不重新连接，则直接返回。
    if (!reconnect)
        return 1;

    /* try to re-connect without waiting for replicationCron, this is needed
     * for the "diskless loading short read" test. */
    serverLog(LL_NOTICE,"Reconnecting to MASTER %s:%d after failure",
        server.masterhost, server.masterport);
    // 有需要的话，尝试重新连接，不等replicationCron处理。
    connectWithMaster();

    return 1;
}

/* Set replication to the specified master address and port. */
// 将本节点slave关联到指定的master（ip，port）
void replicationSetMaster(char *ip, int port) {
    // 原server.masterhost为NULL，标识当前节点之前是master
    int was_master = server.masterhost == NULL;

    // 清除当前节点有关之前master的信息
    sdsfree(server.masterhost);
    server.masterhost = NULL;
    if (server.master) {
        freeClient(server.master);
    }
    // 断开所有Blocked的client，因为我们当前节点发送了变化，阻塞的client可能不安全了。
    // 例如节点可能之前是其他master的slave，现在要设置成另外一个master的slave，肯定需要清除掉阻塞的client的。
    disconnectAllBlockedClients(); /* Clients blocked in master, now slave. */

    /* Setting masterhost only after the call to freeClient since it calls
     * replicationHandleMasterDisconnection which can trigger a re-connect
     * directly from within that call. */
    // 仅在调用了freeClient之后设置masterhost，因为它会调用ReplicationHandleMasterDisconnection，直接在该函数内部触发重新连接。
    server.masterhost = sdsnew(ip);
    server.masterport = port;

    /* Update oom_score_adj */
    // 更新oom_score_adj
    setOOMScoreAdj(-1);

    /* Force our slaves to resync with us as well. They may hopefully be able
     * to partially resync with us, but we can notify the replid change. */
    // 强制我们的slaves来与我们重新同步。 slaves可能希望能够进行部分重新同步，但是我们可以通知slaves replid的变更。
    disconnectSlaves();
    // 重置正在进行中的SYNC连接（不管握手阶段或传输阶段，都取消掉操作）
    cancelReplicationHandshake(0);
    /* Before destroying our master state, create a cached master using
     * our own parameters, to later PSYNC with the new master. */
    // 如果当前节点之前是master，我们暂时不直接销毁，先把之前master状态缓存起来。等待后面与新master进行psync操作
    if (was_master) {
        // 清空cached master，将当前master状态写入cache master中。
        replicationDiscardCachedMaster();
        replicationCacheMasterUsingMyself();
    }

    /* Fire the role change modules event. */
    // 触发role变更的module事件
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICATION_ROLE_CHANGED,
                          REDISMODULE_EVENT_REPLROLECHANGED_NOW_REPLICA,
                          NULL);

    /* Fire the master link modules event. */
    // 触发master link变更的module事件
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    // 设置当前节点的repl状态
    server.repl_state = REPL_STATE_CONNECT;
    serverLog(LL_NOTICE,"Connecting to MASTER %s:%d",
        server.masterhost, server.masterport);
    // 使用前面设置的server.masterhost，server.masterport进行与master连接。
    connectWithMaster();
}

/* Cancel replication, setting the instance as a master itself. */
// 取消所有的复制，设置当前实例为master
void replicationUnsetMaster(void) {
    // 如果当前节点没有关联到master，什么都不需要处理。
    if (server.masterhost == NULL) return; /* Nothing to do. */

    /* Fire the master link modules event. */
    // 如果当前节点是slave，且repl_state复制状态是CONNECTED，因为我们需要断开，所以要触发master link变更事件给module处理。
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    /* Clear masterhost first, since the freeClient calls
     * replicationHandleMasterDisconnection which can attempt to re-connect. */
    // 先清除masterhost，因为后面freeClient中会调用replicationHandleMasterDisconnection，进一步通过这个参数来重新连接对应的节点。
    sdsfree(server.masterhost);
    server.masterhost = NULL;
    // 有master连接的clinet，这里调用freeClient释放掉。
    if (server.master) freeClient(server.master);
    // cached master也一并清理
    replicationDiscardCachedMaster();
    // 重置正在进行中的SYNC连接（不管握手阶段或传输阶段，都取消掉操作）
    cancelReplicationHandshake(0);
    /* When a slave is turned into a master, the current replication ID
     * (that was inherited from the master at synchronization time) is
     * used as secondary ID up to the current offset, and a new replication
     * ID is created to continue with a new replication history.
     *
     * NOTE: this function MUST be called after we call
     * freeClient(server.master), since there we adjust the replication
     * offset trimming the final PINGs. See Github issue #7320. */
    // 当一个slave变成master时，当前的复制ID（在同步时从master继承的）将用作辅助ID，对应的offset更新为当前的offset。
    // 同时会随机生成一个新的复制ID，进行新的复制流程。
    // 注意：必须在调用freeClient(server.master)之后调用此函数，因为在那里我们调整复制偏移量去掉最后的PINGs。参见Github问题＃7320。
    shiftReplicationId();
    /* Disconnecting all the slaves is required: we need to inform slaves
     * of the replication ID change (see shiftReplicationId() call). However
     * the slaves will be able to partially resync with us, so it will be
     * a very fast reconnection. */
    // 断开所有的slaves是必须的，因为我们需要通知所有的slaves，复制ID改变了（shiftReplicationId中处理的）。
    // 不过断开代价不高，因为slaves能跟我们进行部分重新同步，这样能够很快速的进行重新连接同步。
    disconnectSlaves();
    server.repl_state = REPL_STATE_NONE;

    /* We need to make sure the new master will start the replication stream
     * with a SELECT statement. This is forced after a full resync, but
     * with PSYNC version 2, there is no need for full resync after a
     * master switch. */
    // 我们需要确保新的master从SELECT语句开始进行复制。当在一次完全重同步之后，开始进行流式复制时，这是强制性的。
    // 但是对于PSYNC2版本来说，在master切换之后无需进行完全重同步。
    server.slaveseldb = -1;

    /* Update oom_score_adj */
    // 更新oom_score_adj
    setOOMScoreAdj(-1);

    /* Once we turn from slave to master, we consider the starting time without
     * slaves (that is used to count the replication backlog time to live) as
     * starting from now. Otherwise the backlog will be freed after a
     * failover if slaves do not connect immediately. */
    // 一旦我们从slave变为master，我们将自己的no_slaves开始时间设为当前时间。
    // repl_no_slaves_since主要用于处理复制backlog的存活使用，没有slave的话也就不需要复制backlog了。
    // 这里我们不设置的话，当故障转移后如果没有slaves立即连接，复制backlog将会被释放了。
    server.repl_no_slaves_since = server.unixtime;
    
    /* Reset down time so it'll be ready for when we turn into replica again. */
    // 重置与master连接断开时间，当我们再次成为slave时，就不用再处理这个字段了。
    server.repl_down_since = 0;

    /* Fire the role change modules event. */
    // 通知module处理role change事件
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICATION_ROLE_CHANGED,
                          REDISMODULE_EVENT_REPLROLECHANGED_NOW_MASTER,
                          NULL);

    /* Restart the AOF subsystem in case we shut it down during a sync when
     * we were still a slave. */
    // 如果我们之前是slave时，在SYNC期间关闭了AOF的话，这里重启AOF子系统。
    if (server.aof_enabled && server.aof_state == AOF_OFF) restartAOFAfterSYNC();
}

/* This function is called when the slave lose the connection with the
 * master into an unexpected way. */
// 当slave因为异常或错误断开了与master的连接时，会调用这个方法处理。
void replicationHandleMasterDisconnection(void) {
    /* Fire the master link modules event. */
    // 触发master link的module事件
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    // 当前节点的master设置为NULL，repl状态设置为待连接，repl down时间更新为当前时间
    server.master = NULL;
    server.repl_state = REPL_STATE_CONNECT;
    server.repl_down_since = server.unixtime;
    /* We lost connection with our master, don't disconnect slaves yet,
     * maybe we'll be able to PSYNC with our master later. We'll disconnect
     * the slaves only if we'll have to do a full resync with our master. */
    // 我们失去了与master的连接，但并没有完全断开slaves连接，因为可能我们接下来会请求master执行PSYNC操作。
    // 我们只有在请求master做一个全量重同步时，才会断开slaves。

    /* Try to re-connect immediately rather than wait for replicationCron
     * waiting 1 second may risk backlog being recycled. */
    // 尝试重新进行连接，而不是等待定时任务replicationCron执行处理。
    // 这个定时任务1s执行一次，等待可能会导致backlog被覆盖，从而进行全量同步。
    if (server.masterhost) {
        serverLog(LL_NOTICE,"Reconnecting to MASTER %s:%d",
            server.masterhost, server.masterport);
        connectWithMaster();
    }
}

void replicaofCommand(client *c) {
    /* SLAVEOF is not allowed in cluster mode as replication is automatically
     * configured using the current address of the master node. */
    if (server.cluster_enabled) {
        addReplyError(c,"REPLICAOF not allowed in cluster mode.");
        return;
    }

    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c,"REPLICAOF not allowed while failing over.");
        return;
    }

    /* The special host/port combination "NO" "ONE" turns the instance
     * into a master. Otherwise the new master address is set. */
    if (!strcasecmp(c->argv[1]->ptr,"no") &&
        !strcasecmp(c->argv[2]->ptr,"one")) {
        if (server.masterhost) {
            replicationUnsetMaster();
            sds client = catClientInfoString(sdsempty(),c);
            serverLog(LL_NOTICE,"MASTER MODE enabled (user request from '%s')",
                client);
            sdsfree(client);
        }
    } else {
        long port;

        if (c->flags & CLIENT_SLAVE)
        {
            /* If a client is already a replica they cannot run this command,
             * because it involves flushing all replicas (including this
             * client) */
            addReplyError(c, "Command is not valid when client is a replica.");
            return;
        }

        if ((getLongFromObjectOrReply(c, c->argv[2], &port, NULL) != C_OK))
            return;

        /* Check if we are already attached to the specified master */
        if (server.masterhost && !strcasecmp(server.masterhost,c->argv[1]->ptr)
            && server.masterport == port) {
            serverLog(LL_NOTICE,"REPLICAOF would result into synchronization "
                                "with the master we are already connected "
                                "with. No operation performed.");
            addReplySds(c,sdsnew("+OK Already connected to specified "
                                 "master\r\n"));
            return;
        }
        /* There was no previous master or the user specified a different one,
         * we can continue. */
        replicationSetMaster(c->argv[1]->ptr, port);
        sds client = catClientInfoString(sdsempty(),c);
        serverLog(LL_NOTICE,"REPLICAOF %s:%d enabled (user request from '%s')",
            server.masterhost, server.masterport, client);
        sdsfree(client);
    }
    addReply(c,shared.ok);
}

/* ROLE command: provide information about the role of the instance
 * (master or slave) and additional information related to replication
 * in an easy to process format. */
void roleCommand(client *c) {
    if (server.masterhost == NULL) {
        listIter li;
        listNode *ln;
        void *mbcount;
        int slaves = 0;

        addReplyArrayLen(c,3);
        addReplyBulkCBuffer(c,"master",6);
        addReplyLongLong(c,server.master_repl_offset);
        mbcount = addReplyDeferredLen(c);
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;
            char ip[NET_IP_STR_LEN], *slaveaddr = slave->slave_addr;

            if (!slaveaddr) {
                if (connPeerToString(slave->conn,ip,sizeof(ip),NULL) == -1)
                    continue;
                slaveaddr = ip;
            }
            if (slave->replstate != SLAVE_STATE_ONLINE) continue;
            addReplyArrayLen(c,3);
            addReplyBulkCString(c,slaveaddr);
            addReplyBulkLongLong(c,slave->slave_listening_port);
            addReplyBulkLongLong(c,slave->repl_ack_off);
            slaves++;
        }
        setDeferredArrayLen(c,mbcount,slaves);
    } else {
        char *slavestate = NULL;

        addReplyArrayLen(c,5);
        addReplyBulkCBuffer(c,"slave",5);
        addReplyBulkCString(c,server.masterhost);
        addReplyLongLong(c,server.masterport);
        if (slaveIsInHandshakeState()) {
            slavestate = "handshake";
        } else {
            switch(server.repl_state) {
            case REPL_STATE_NONE: slavestate = "none"; break;
            case REPL_STATE_CONNECT: slavestate = "connect"; break;
            case REPL_STATE_CONNECTING: slavestate = "connecting"; break;
            case REPL_STATE_TRANSFER: slavestate = "sync"; break;
            case REPL_STATE_CONNECTED: slavestate = "connected"; break;
            default: slavestate = "unknown"; break;
            }
        }
        addReplyBulkCString(c,slavestate);
        addReplyLongLong(c,server.master ? server.master->reploff : -1);
    }
}

/* Send a REPLCONF ACK command to the master to inform it about the current
 * processed offset. If we are not connected with a master, the command has
 * no effects. */
// 发送REPLCONF ACK命令给master，通知master当前我们处理的offset。如果当前与master没有连接，这个函数什么也不做。
void replicationSendAck(void) {
    client *c = server.master;

    if (c != NULL) {
        c->flags |= CLIENT_MASTER_FORCE_REPLY;
        // 发送REPLCONF ACK offset给master
        addReplyArrayLen(c,3);
        addReplyBulkCString(c,"REPLCONF");
        addReplyBulkCString(c,"ACK");
        addReplyBulkLongLong(c,c->reploff);
        c->flags &= ~CLIENT_MASTER_FORCE_REPLY;
    }
}

/* ---------------------- MASTER CACHING FOR PSYNC -------------------------- */

/* In order to implement partial synchronization we need to be able to cache
 * our master's client structure after a transient disconnection.
 * It is cached into server.cached_master and flushed away using the following
 * functions. */

/* This function is called by freeClient() in order to cache the master
 * client structure instead of destroying it. freeClient() will return
 * ASAP after this function returns, so every action needed to avoid problems
 * with a client that is really "suspended" has to be done by this function.
 *
 * The other functions that will deal with the cached master are:
 *
 * replicationDiscardCachedMaster() that will make sure to kill the client
 * as for some reason we don't want to use it in the future.
 *
 * replicationResurrectCachedMaster() that is used after a successful PSYNC
 * handshake in order to reactivate the cached master.
 */
// 在一个短暂的连接断开时，为了实现部分重同步，我们需要能缓存master连接，后面接着从断开点继续同步。
// 我们使用server.cached_master来缓存master连接，这里的几个函数就是处理相关缓存操作的。

// 这个函数由freeClient()调用。当释放的连接是master连接时，用于缓存该client而不是直接销毁。
// freeClient()调用这个方法后会尽早返回，所有避免"挂起"的client出现问题的一些操作都需要在这个函数里执行。
// 其他的几个处理cached master的函数有：
//      replicationDiscardCachedMaster()，当我们因为一些原因不会再使用这个缓存的master client时，调用这个函数来真正的销毁它。
//      replicationResurrectCachedMaster()，当与master成功进行完PSYNC握手，可以复用原复制信息，调用这个方法重新启用缓存的master连接。
void replicationCacheMaster(client *c) {
    serverAssert(server.master != NULL && server.cached_master == NULL);
    serverLog(LL_NOTICE,"Caching the disconnected master state.");

    /* Unlink the client from the server structures. */
    // 从当前server结构中，移除该client。有很多的队列中可能存在该client，都要一一处理。
    unlinkClient(c);

    /* Reset the master client so that's ready to accept new commands:
     * we want to discard te non processed query buffers and non processed
     * offsets, including pending transactions, already populated arguments,
     * pending outputs to the master. */
    // 重置master client相关属性，后面重新使用时将是全新的状态来处理新的命令。
    // 这里的清理包括查询缓存、回复缓冲、复制处理offset、在处理的事务以及client的各种标识等。
    sdsclear(server.master->querybuf);
    sdsclear(server.master->pending_querybuf);
    // 更新已处理的复制offset
    server.master->read_reploff = server.master->reploff;
    // 处理事务的清理
    if (c->flags & CLIENT_MULTI) discardTransaction(c);
    listEmpty(c->reply);
    c->sentlen = 0;
    c->reply_bytes = 0;
    c->bufpos = 0;
    // 重置client各种flags
    resetClient(c);

    /* Save the master. Server.master will be set to null later by
     * replicationHandleMasterDisconnection(). */
    // 缓存当前master client
    server.cached_master = server.master;

    /* Invalidate the Peer ID cache. */
    // 清空client连接的对端标识
    if (c->peerid) {
        sdsfree(c->peerid);
        c->peerid = NULL;
    }
    /* Invalidate the Sock Name cache. */
    // 清空client本地socket标识
    if (c->sockname) {
        sdsfree(c->sockname);
        c->sockname = NULL;
    }

    /* Caching the master happens instead of the actual freeClient() call,
     * so make sure to adjust the replication state. This function will
     * also set server.master to NULL. */
    // 缓存master并没有真的释放，所以我们还需要更新当前服务的复制状态，同时设置master字段为NULL。
    // 因为可能临时断开的连接，所以如果有master host的话我们这里会处理尝试重新连接。
    replicationHandleMasterDisconnection();
}

/* This function is called when a master is turend into a slave, in order to
 * create from scratch a cached master for the new client, that will allow
 * to PSYNC with the slave that was promoted as the new master after a
 * failover.
 *
 * Assuming this instance was previously the master instance of the new master,
 * the new master will accept its replication ID, and potentiall also the
 * current offset if no data was lost during the failover. So we use our
 * current replication ID and offset in order to synthesize a cached master. */
// 当master变成slave时，调用这个函数创建一个新的client作为cached master。
// 这样我们执行failover后，当前节点与新的master通信处理PSYNC以及后续streams传输流程就可以统一处理了。

// 假设当前节点之前是新master的master，则故障转移时新的master将接受老master的复制ID，如果故障转移时没有数据丢失，则还可能接受当前offset。
// 所以这里使用我们当前的复制ID和offset来合成一个缓存的master，从而后面进行PSYNC，尽可能减少全量的重同步。
void replicationCacheMasterUsingMyself(void) {
    serverLog(LL_NOTICE,
        "Before turning into a replica, using my own master parameters "
        "to synthesize a cached master: I may be able to synchronize with "
        "the new master with just a partial transfer.");

    /* This will be used to populate the field server.master->reploff
     * by replicationCreateMasterClient(). We'll later set the created
     * master as server.cached_master, so the replica will use such
     * offset for PSYNC. */
    // 在replicationCreateMasterClient()创建master client时会使用该字段填充server.master->reploff。
    // 后面会将创建的master设置为server.cached_master，所以会使用该offset来与master进行PSYNC同步。
    server.master_initial_offset = server.master_repl_offset;

    /* The master client we create can be set to any DBID, because
     * the new master will start its replication stream with SELECT. */
    // 当前我们新建的master client不需要选择db，因为新的master在传输复制流时会先写一个SELECT指令。
    replicationCreateMasterClient(NULL,-1);

    /* Use our own ID / offset. */
    // 用我们自己的复制ID作为master的复制ID。
    memcpy(server.master->replid, server.replid, sizeof(server.replid));

    /* Set as cached master. */
    // 将创建的master作为cached master存起来。
    unlinkClient(server.master);
    server.cached_master = server.master;
    server.master = NULL;
}

/* Free a cached master, called when there are no longer the conditions for
 * a partial resync on reconnection. */
// 释放掉cached master。没办法复用原来的复制信息（id/offset）时，缓存的master没必要了，调用这个方法清理掉。
void replicationDiscardCachedMaster(void) {
    if (server.cached_master == NULL) return;

    serverLog(LL_NOTICE,"Discarding previously cached master state.");
    server.cached_master->flags &= ~CLIENT_MASTER;
    freeClient(server.cached_master);
    server.cached_master = NULL;
}

/* Turn the cached master into the current master, using the file descriptor
 * passed as argument as the socket for the new master.
 *
 * This function is called when successfully setup a partial resynchronization
 * so the stream of data that we'll receive will start from were this
 * master left. */
// 将cached master转为当前master，并使用传入的conn作为master的连接。
// 这个函数用于成功建立部分重同步后调用，这样我们就可以从断开连接的master(缓存的master)断的地方进行重新streams同步。
void replicationResurrectCachedMaster(connection *conn) {
    // 取出缓存的master使用，设置使用传入的conn连接
    server.master = server.cached_master;
    server.cached_master = NULL;
    server.master->conn = conn;
    connSetPrivateData(server.master->conn, server.master);
    // 更新master client的标识及属性
    server.master->flags &= ~(CLIENT_CLOSE_AFTER_REPLY|CLIENT_CLOSE_ASAP);
    server.master->authenticated = 1;
    server.master->lastinteraction = server.unixtime;
    // 更新复制连接状态，已连接，可以进行传输了
    server.repl_state = REPL_STATE_CONNECTED;
    server.repl_down_since = 0;

    /* Fire the master link modules event. */
    // master连接变更通知module事件处理
    moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                          REDISMODULE_SUBEVENT_MASTER_LINK_UP,
                          NULL);

    /* Re-add to the list of clients. */
    // 重新将当前master加入当前clients列表
    linkClient(server.master);
    // 设置read handler加入事件监听，其实就是跟正常连接读命令处理流程一样了。同步数据不允许使用IO多线程处理。
    if (connSetReadHandler(server.master->conn, readQueryFromClient)) {
        serverLog(LL_WARNING,"Error resurrecting the cached master, impossible to add the readable handler: %s", strerror(errno));
        freeClientAsync(server.master); /* Close ASAP. */
    }

    /* We may also need to install the write handler as well if there is
     * pending data in the write buffers. */
    // 如果有pending writes数据，这里同样设置write handler加入事件监听。
    if (clientHasPendingReplies(server.master)) {
        if (connSetWriteHandler(server.master->conn, sendReplyToClient)) {
            serverLog(LL_WARNING,"Error resurrecting the cached master, impossible to add the writable handler: %s", strerror(errno));
            freeClientAsync(server.master); /* Close ASAP. */
        }
    }
}

/* ------------------------- MIN-SLAVES-TO-WRITE  --------------------------- */

/* This function counts the number of slaves with lag <= min-slaves-max-lag.
 * If the option is active, the server will prevent writes if there are not
 * enough connected slaves with the specified lag (or less). */
// 这个函数用于计算当前good slaves（lag <= min-slaves-max-lag）的数量。
// 如果min-slaves-max-lag有设置，则当good slaves少于一定数量时，我们会阻止写入命令的执行。
void refreshGoodSlavesCount(void) {
    listIter li;
    listNode *ln;
    int good = 0;

    // 这两个参数只要有一个没设置，我们就不用管good slaves相关逻辑。
    if (!server.repl_min_slaves_to_write ||
        !server.repl_min_slaves_max_lag) return;

    listRewind(server.slaves,&li);
    // 遍历当前节点的slaves列表，判断lag（当前时间-上一次ack时间）是否在repl_min_slaves_max_lag之内。如果是，则该slave是good slave。
    while((ln = listNext(&li))) {
        client *slave = ln->value;
        time_t lag = server.unixtime - slave->repl_ack_time;

        if (slave->replstate == SLAVE_STATE_ONLINE &&
            lag <= server.repl_min_slaves_max_lag) good++;
    }
    server.repl_good_slaves_count = good;
}

/* ----------------------- REPLICATION SCRIPT CACHE --------------------------
 * The goal of this code is to keep track of scripts already sent to every
 * connected slave, in order to be able to replicate EVALSHA as it is without
 * translating it to EVAL every time it is possible.
 *
 * We use a capped collection implemented by a hash table for fast lookup
 * of scripts we can send as EVALSHA, plus a linked list that is used for
 * eviction of the oldest entry when the max number of items is reached.
 *
 * We don't care about taking a different cache for every different slave
 * since to fill the cache again is not very costly, the goal of this code
 * is to avoid that the same big script is transmitted a big number of times
 * per second wasting bandwidth and processor speed, but it is not a problem
 * if we need to rebuild the cache from scratch from time to time, every used
 * script will need to be transmitted a single time to reappear in the cache.
 *
 * This is how the system works:
 *
 * 1) Every time a new slave connects, we flush the whole script cache.
 * 2) We only send as EVALSHA what was sent to the master as EVALSHA, without
 *    trying to convert EVAL into EVALSHA specifically for slaves.
 * 3) Every time we transmit a script as EVAL to the slaves, we also add the
 *    corresponding SHA1 of the script into the cache as we are sure every
 *    slave knows about the script starting from now.
 * 4) On SCRIPT FLUSH command, we replicate the command to all the slaves
 *    and at the same time flush the script cache.
 * 5) When the last slave disconnects, flush the cache.
 * 6) We handle SCRIPT LOAD as well since that's how scripts are loaded
 *    in the master sometimes.
 */

/* Initialize the script cache, only called at startup. */
void replicationScriptCacheInit(void) {
    server.repl_scriptcache_size = 10000;
    server.repl_scriptcache_dict = dictCreate(&replScriptCacheDictType,NULL);
    server.repl_scriptcache_fifo = listCreate();
}

/* Empty the script cache. Should be called every time we are no longer sure
 * that every slave knows about all the scripts in our set, or when the
 * current AOF "context" is no longer aware of the script. In general we
 * should flush the cache:
 *
 * 1) Every time a new slave reconnects to this master and performs a
 *    full SYNC (PSYNC does not require flushing).
 * 2) Every time an AOF rewrite is performed.
 * 3) Every time we are left without slaves at all, and AOF is off, in order
 *    to reclaim otherwise unused memory.
 */
// 清空脚本缓存。当我们不再确定每个slave都知道集合中的所有脚本时，或者当前AOF上下文不再了解该脚本时，应调用该方法来清空脚本缓存。
// 一般以下情况需要清空缓存：
// 1、每次新的slave重新与master连接，并执行一个全量SYNC时。（PSYNC不需要清空脚本缓存）
// 2、每次AOF重写执行时。
// 3、每次我们没有slaves，且AOF是关闭的时，主要用于回收没有使用的内存。
void replicationScriptCacheFlush(void) {
    dictEmpty(server.repl_scriptcache_dict,NULL);
    listRelease(server.repl_scriptcache_fifo);
    server.repl_scriptcache_fifo = listCreate();
}

/* Add an entry into the script cache, if we reach max number of entries the
 * oldest is removed from the list. */
void replicationScriptCacheAdd(sds sha1) {
    int retval;
    sds key = sdsdup(sha1);

    /* Evict oldest. */
    if (listLength(server.repl_scriptcache_fifo) == server.repl_scriptcache_size)
    {
        listNode *ln = listLast(server.repl_scriptcache_fifo);
        sds oldest = listNodeValue(ln);

        retval = dictDelete(server.repl_scriptcache_dict,oldest);
        serverAssert(retval == DICT_OK);
        listDelNode(server.repl_scriptcache_fifo,ln);
    }

    /* Add current. */
    retval = dictAdd(server.repl_scriptcache_dict,key,NULL);
    listAddNodeHead(server.repl_scriptcache_fifo,key);
    serverAssert(retval == DICT_OK);
}

/* Returns non-zero if the specified entry exists inside the cache, that is,
 * if all the slaves are aware of this script SHA1. */
int replicationScriptCacheExists(sds sha1) {
    return dictFind(server.repl_scriptcache_dict,sha1) != NULL;
}

/* ----------------------- SYNCHRONOUS REPLICATION --------------------------
 * Redis synchronous replication design can be summarized in points:
 *
 * - Redis masters have a global replication offset, used by PSYNC.
 * - Master increment the offset every time new commands are sent to slaves.
 * - Slaves ping back masters with the offset processed so far.
 *
 * So synchronous replication adds a new WAIT command in the form:
 *
 *   WAIT <num_replicas> <milliseconds_timeout>
 *
 * That returns the number of replicas that processed the query when
 * we finally have at least num_replicas, or when the timeout was
 * reached.
 *
 * The command is implemented in this way:
 *
 * - Every time a client processes a command, we remember the replication
 *   offset after sending that command to the slaves.
 * - When WAIT is called, we ask slaves to send an acknowledgement ASAP.
 *   The client is blocked at the same time (see blocked.c).
 * - Once we receive enough ACKs for a given offset or when the timeout
 *   is reached, the WAIT command is unblocked and the reply sent to the
 *   client.
 */
// client可以在每次执行命令后调用wait来进行同步复制。
// Redis同步复制设计可以总结如下：
//  1、redis master有一个全局的复制offset，用于PSYNC。
//  2、master每次发送命令到slaves时，会增加该offset值。
//  3、slaves会响应ACK ping来告知master当前处理的offset。
// 所以同步复制增加了一个新的WAIT命令：WAIT <num_replicas> <milliseconds_timeout>。
// 当我们有 至少num_replicas处理到指定offset 或者 该命令超时时，返回最终处理到该offset的slaves数量。
// 该命令的实现方式如下：
//  1、每次client处理一个命令，我们在发送命令到slaves后记录复制offset。
//  2、当WAIT被调用时，我们请求slaves尽快返回当前处理offset的ACK。此时该client是会处于阻塞状态。
//  3、对于指定的offset，一旦我们收到足够的ACKs，或者WAIT超时，会解除block，发送回复给client。

/* This just set a flag so that we broadcast a REPLCONF GETACK command
 * to all the slaves in the beforeSleep() function. Note that this way
 * we "group" all the clients that want to wait for synchronous replication
 * in a given event loop iteration, and send a single GETACK for them all. */
// 这个函数仅用于设置一个flag，这样我们就可以在beforeSleep()中向所有的slaves广播REPLCONF GETACK命令来搜集ACKs。
// 注意通过这种方式，我们将所有等待同步复制的客户端"分组"，并为它们单独发送GETACK。
void replicationRequestAckFromSlaves(void) {
    server.get_ack_from_slaves = 1;
}

/* Return the number of slaves that already acknowledged the specified
 * replication offset. */
// 返回已经ACK确认了指定offset的slaves数量。
int replicationCountAcksByOffset(long long offset) {
    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        // 遍历slaves列表，如果是ONLINE状态且ack_off>指定offset，则计数+1。
        if (slave->replstate != SLAVE_STATE_ONLINE) continue;
        if (slave->repl_ack_off >= offset) count++;
    }
    return count;
}

/* WAIT for N replicas to acknowledge the processing of our latest
 * write command (and all the previous commands). */
// 等待N个slaves ACK确认处理完成我们最近的写命令（当然因为命令顺序传播处理的，所以之前的所有命令也都处理完）。
void waitCommand(client *c) {
    mstime_t timeout;
    long numreplicas, ackreplicas;
    long long offset = c->woff;

    // 如果当前不是master，WAIT命令不可用
    if (server.masterhost) {
        addReplyError(c,"WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.");
        return;
    }

    /* Argument parsing. */
    // 参数解析，获取需要等待的slaves数，以及timeout
    if (getLongFromObjectOrReply(c,c->argv[1],&numreplicas,NULL) != C_OK)
        return;
    if (getTimeoutFromObjectOrReply(c,c->argv[2],&timeout,UNIT_MILLISECONDS)
        != C_OK) return;

    /* First try without blocking at all. */
    // 先检查当前slaves已经回复的ACKs offset，看已处理达到指定offset的slaves数是否满足。满足则直接回复client，不需要阻塞。
    ackreplicas = replicationCountAcksByOffset(c->woff);
    if (ackreplicas >= numreplicas || c->flags & CLIENT_MULTI) {
        addReplyLongLong(c,ackreplicas);
        return;
    }

    /* Otherwise block the client and put it into our list of clients
     * waiting for ack from slaves. */
    // 同步复制没有达到指定的slaves数，当前client需要进入阻塞。设置阻塞状态，加入waiting_acks队列。
    c->bpop.timeout = timeout;
    c->bpop.reploffset = offset;
    c->bpop.numreplicas = numreplicas;
    listAddNodeHead(server.clients_waiting_acks,c);
    blockClient(c,BLOCKED_WAIT);

    /* Make sure that the server will send an ACK request to all the slaves
     * before returning to the event loop. */
    // 设置get_ack_from_slaves标识，从而在事件循环beforeSleep()中向slaves发送获取ACK的请求。
    replicationRequestAckFromSlaves();
}

/* This is called by unblockClient() to perform the blocking op type
 * specific cleanup. We just remove the client from the list of clients
 * waiting for replica acks. Never call it directly, call unblockClient()
 * instead. */
// 该函数由unblockClient()调用来执行特定类型阻塞的清理。
// 这里只是从等待副本确认的客户端列表中删除客户端。注意永远不要直接调用它，而应调用unblockClient() 。
void unblockClientWaitingReplicas(client *c) {
    // 从clients_waiting_acks列表找到指定client，然后删除
    listNode *ln = listSearchKey(server.clients_waiting_acks,c);
    serverAssert(ln != NULL);
    listDelNode(server.clients_waiting_acks,ln);
}

/* Check if there are clients blocked in WAIT that can be unblocked since
 * we received enough ACKs from slaves. */
// 该函数在事件循环的beforeSleep中调用。
// 我们上一轮的事件处理已经收到了一些slave的ACKs，所以需要检查下是否有阻塞在WAIT状态的clients可以解除阻塞，继续处理命令执行。
void processClientsWaitingReplicas(void) {
    long long last_offset = 0;
    int last_numreplicas = 0;

    listIter li;
    listNode *ln;

    // 遍历clients_waiting_acks列表
    listRewind(server.clients_waiting_acks,&li);
    while((ln = listNext(&li))) {
        client *c = ln->value;

        /* Every time we find a client that is satisfied for a given
         * offset and number of replicas, we remember it so the next client
         * may be unblocked without calling replicationCountAcksByOffset()
         * if the requested offset / replicas were equal or less. */
        // 每次我们遍历到一个client，如果当前满足等待的offset和slaves数量，则解除阻塞。
        // 另外我们记录下该满足条件的值，这样在处理下一个client时，可能直接就可以判断，而不再需要调用count函数进行计算。
        if (last_offset && last_offset >= c->bpop.reploffset &&
                           last_numreplicas >= c->bpop.numreplicas)
        {
            // 上一次查到的offser和slaves数量，能够覆盖本次处理的client，则说明达到了当前client的要求，从而不用计算，直接可以解除阻塞。
            unblockClient(c);
            addReplyLongLong(c,last_numreplicas);
        } else {
            // 没有last_offset，或者上一次查到的数据满足不了当前client，我们需要再已当前client指定的offset去计算达到条件的slaves数。
            int numreplicas = replicationCountAcksByOffset(c->bpop.reploffset);

            if (numreplicas >= c->bpop.numreplicas) {
                // 如果slaves数达到了，除了解除阻塞返回外，还记录本次的数值供处理后面client使用。
                last_offset = c->bpop.reploffset;
                last_numreplicas = numreplicas;
                unblockClient(c);
                addReplyLongLong(c,numreplicas);
            }
        }
    }
}

/* Return the slave replication offset for this instance, that is
 * the offset for which we already processed the master replication stream. */
// 返回本节点实例的复制offset。即我们已经处理的master复制数据流的offset。
long long replicationGetSlaveOffset(void) {
    long long offset = 0;

    // server.masterhost不为NULL时，我们才有与master连接进行复制，才有offset
    if (server.masterhost != NULL) {
        // 如果正常有master的复制数据连接client，则从该client中取。
        // 没有正常client的话，可能在做PSYNC操作，则从缓存的PSYNC master client中取offset。
        if (server.master) {
            offset = server.master->reploff;
        } else if (server.cached_master) {
            offset = server.cached_master->reploff;
        }
    }
    /* offset may be -1 when the master does not support it at all, however
     * this function is designed to return an offset that can express the
     * amount of data processed by the master, so we return a positive
     * integer. */
    // 当主机完全不支持复制时，其偏移量可能为-1，但是此函数用于返回从master复制数据的偏移量，因此返回一个非负数。
    if (offset < 0) offset = 0;
    return offset;
}

/* --------------------------- REPLICATION CRON  ---------------------------- */

/* Replication cron function, called 1 time per second. */
// 复制相关的定时处理函数，每1s中执行一次
void replicationCron(void) {
    // 记录总的处理次数，静态变量初始化。
    static long long replication_cron_loops = 0;

    /* Check failover status first, to see if we need to start
     * handling the failover. */
    // 检查更新failover状态，看我们是否需要处理一个故障转移。
    updateFailoverStatus();

    /* Non blocking connection timeout? */
    // [slave]检查与master建立连接或handshake阶段是否有超时，如果超时则取消当前连接，并进行重连（cancel函数参数传1表示会进行重连）。
    if (server.masterhost &&
        (server.repl_state == REPL_STATE_CONNECTING ||
         slaveIsInHandshakeState()) &&
         (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"Timeout connecting to the MASTER...");
        cancelReplicationHandshake(1);
    }

    /* Bulk transfer I/O timeout? */
    // [slave]检查当前节点接收RDB传输期间是否发生超时，如果超时则断开连接，再重连重新处理复制。
    if (server.masterhost && server.repl_state == REPL_STATE_TRANSFER &&
        (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"Timeout receiving bulk data from MASTER... If the problem persists try to set the 'repl-timeout' parameter in redis.conf to a larger value.");
        cancelReplicationHandshake(1);
    }

    /* Timed out master when we are an already connected slave? */
    // [slave]当我们是与master建立好的连接，正常传输复制流数据时，发生了超时。
    // 我们调用freeClient处理，将master client缓存起来，并重新与master建立conn，后面使用缓存的master client来进行重新同步数据。
    if (server.masterhost && server.repl_state == REPL_STATE_CONNECTED &&
        (time(NULL)-server.master->lastinteraction) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"MASTER timeout: no data nor PING received...");
        freeClient(server.master);
    }

    /* Check if we should connect to a MASTER */
    // [slave]当前是CONNECT状态，我们需要与master建立连接，调用connectWithMaster()进行处理。
    if (server.repl_state == REPL_STATE_CONNECT) {
        serverLog(LL_NOTICE,"Connecting to MASTER %s:%d",
            server.masterhost, server.masterport);
        connectWithMaster();
    }

    /* Send ACK to master from time to time.
     * Note that we do not send periodic acks to masters that don't
     * support PSYNC and replication offsets. */
    // [slave]每次定时任务执行的时候都向master发送我们的ACK信息。注意对于不支持PSYNC和复制offsets的master们我们这里不发送ACKs。
    if (server.masterhost && server.master &&
        !(server.master->flags & CLIENT_PRE_PSYNC))
        replicationSendAck();

    /* If we have attached slaves, PING them from time to time.
     * So slaves can implement an explicit timeout to masters, and will
     * be able to detect a link disconnection even if the TCP connection
     * will not actually go down. */
    // [master]如果我们有slaves，我们需要定期PING下它们，这样slaves可以显式的处理与master连接的超时。
    // slave可以在TCP没有断开的情况下，探测到与master的复制link的连接问题，从而处理重连。
    listIter li;
    listNode *ln;
    robj *ping_argv[1];

    /* First, send PING according to ping_slave_period. */
    // 1、因为我们每秒执行一次cron循环，所以这里表示每repl_ping_slave_period秒PING一下slaves。
    if ((replication_cron_loops % server.repl_ping_slave_period) == 0 &&
        listLength(server.slaves))
    {
        /* Note that we don't send the PING if the clients are paused during
         * a Redis Cluster manual failover: the PING we send will otherwise
         * alter the replication offsets of master and slave, and will no longer
         * match the one stored into 'mf_master_offset' state. */
        // 注意如果client在Redis集群手动故障转移期间暂停了，我们不会发送PING消息过去。
        // 因为我们发送PING消息的话，会改变主从的复制偏移量，且将会与缓存的'mf_master_offset'值不一致。
        int manual_failover_in_progress =
            ((server.cluster_enabled &&
              server.cluster->mf_end) ||
            server.failover_end_time) &&
            checkClientPauseTimeoutAndReturnIfPaused();

        if (!manual_failover_in_progress) {
            ping_argv[0] = shared.ping;
            // 发送ping消息给所有slaves
            replicationFeedSlaves(server.slaves, server.slaveseldb,
                ping_argv, 1);
        }
    }

    /* Second, send a newline to all the slaves in pre-synchronization
     * stage, that is, slaves waiting for the master to create the RDB file.
     *
     * Also send the a newline to all the chained slaves we have, if we lost
     * connection from our master, to keep the slaves aware that their
     * master is online. This is needed since sub-slaves only receive proxied
     * data from top-level masters, so there is no explicit pinging in order
     * to avoid altering the replication offsets. This special out of band
     * pings (newlines) can be sent, they will have no effect in the offset.
     *
     * The newline will be ignored by the slave but will refresh the
     * last interaction timer preventing a timeout. In this case we ignore the
     * ping period and refresh the connection once per second since certain
     * timeouts are set at a few seconds (example: PSYNC response). */
    // 2、发送一个空行给所有在等待master创建RDB文件的slave（预同步阶段，即等待全量RDB同步，但非socket流处理）。
    // 同时也相当于发送一个空行给所有我们自己的下一级slaves，告知它们自己还存活，这在我们与master断开连接时很有必要。
    // 因为sub-slaves接收的数据都是我们中转的，与顶层master是一致的，所以我们不能擅自发PING命令过去，只能发送空行，不改变它们的offset。

    // 空行将被slave忽略，但是会刷新最近交互时间，用来作超时检测处理。
    // 这种情况我们实际上是每秒做一下交互，比前面的PING周期处理更频繁，主要用于PSYNC响应这种更短的超时时间的处理。
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        int is_presync =
            (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START ||
            (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
             server.rdb_child_type != RDB_CHILD_TYPE_SOCKET));

        if (is_presync) {
            // 遍历节点的slaves，对于presync状态的slave，发送空行过去。
            connWrite(slave->conn, "\n", 1);
        }
    }

    /* Disconnect timedout slaves. */
    // [master]对于超时的slaves，我们释放连接资源，更新good slaves数量等处理。
    if (listLength(server.slaves)) {
        listIter li;
        listNode *ln;

        // 遍历slaves
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;

            if (slave->replstate == SLAVE_STATE_ONLINE) {
                // ONLINE状态，如果不支持PSYNC，不会进行增量同步数据，所以不需要管超时。
                if (slave->flags & CLIENT_PRE_PSYNC)
                    continue;
                if ((server.unixtime - slave->repl_ack_time) > server.repl_timeout) {
                    // 收到ACK时间超时了，释放slave连接。
                    serverLog(LL_WARNING, "Disconnecting timedout replica (streaming sync): %s",
                          replicationGetSlaveName(slave));
                    freeClient(slave);
                    continue;
                }
            }
            /* We consider disconnecting only diskless replicas because disk-based replicas aren't fed
             * by the fork child so if a disk-based replica is stuck it doesn't prevent the fork child
             * from terminating. */
            // 非ONLINE状态，处理RDB传输情况。
            // 这里只考虑无盘RDB传输的超时，slave有partial_write，不能让某个slave一直阻塞buf，从而导致所有slave的同步受到阻塞。
            // 而基于磁盘RDB的传输，子进程是写文件的，如果某个slave卡住了，并不影响slave完成RDB文件落盘退出，所以这里不处理。
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END && server.rdb_child_type == RDB_CHILD_TYPE_SOCKET) {
                if (slave->repl_last_partial_write != 0 &&
                    (server.unixtime - slave->repl_last_partial_write) > server.repl_timeout)
                {
                    // 两次partial_write时间超时，释放slave连接。
                    // freeClient 里的 unlinkClient 会将该conn从rdb_pipe_conns移除，从而避免对整体RDB传输产生影响。
                    serverLog(LL_WARNING, "Disconnecting timedout replica (full sync): %s",
                          replicationGetSlaveName(slave));
                    freeClient(slave);
                    continue;
                }
            }
        }
    }

    /* If this is a master without attached slaves and there is a replication
     * backlog active, in order to reclaim memory we can free it after some
     * (configured) time. Note that this cannot be done for slaves: slaves
     * without sub-slaves attached should still accumulate data into the
     * backlog, in order to reply to PSYNC queries if they are turned into
     * masters after a failover. */
    // [master]如果当前节点没有slaves了，但有backlog，为了回收内存，我们可以配置一定的时间后回收它。
    // 注意对于没有sub-slaves的slaves节点，我们不能回收，因为slaves需要累计数据进backlog中，用于在故障转移变成master后处理PSYNC请求。
    // 所以这里除了判断没有slaves外，还加上server.masterhost==NULL来确保当前节点是顶层master节点。
    if (listLength(server.slaves) == 0 && server.repl_backlog_time_limit &&
        server.repl_backlog && server.masterhost == NULL)
    {
        time_t idle = server.unixtime - server.repl_no_slaves_since;

        // 当没有slaves持续的时间大于配置的backlog_time_limit时，处理backlog清除。
        if (idle > server.repl_backlog_time_limit) {
            /* When we free the backlog, we always use a new
             * replication ID and clear the ID2. This is needed
             * because when there is no backlog, the master_repl_offset
             * is not updated, but we would still retain our replication
             * ID, leading to the following problem:
             *
             * 1. We are a master instance.
             * 2. Our slave is promoted to master. It's repl-id-2 will
             *    be the same as our repl-id.
             * 3. We, yet as master, receive some updates, that will not
             *    increment the master_repl_offset.
             * 4. Later we are turned into a slave, connect to the new
             *    master that will accept our PSYNC request by second
             *    replication ID, but there will be data inconsistency
             *    because we received writes. */
            // 当释放backlog时，我们总是重新生成一个新的复制ID，并清除ID2。（这样故障转移连接新master的时候会进行全量重同步处理）。
            // 因为如果我们保留复制ID，当backlog置为NULL时，master_repl_offset不会更新了，此时会导致如下的问题：
            // 1、我们是master节点。
            // 2、故障转移，我们的slave提升为master，它将使用我们的repl-id作为它的repl-id-2。
            // 3、我们目前还是master，收到了一些更新操作，但不会再更新当前master_repl_offset了。
            // 4、随后我们变为了slave，与前面新master进行连接时，因为我们传过去的复制ID与新master的repl-id-2一样，
            //  所以新master将使用repl-id-2来接受我们的PSYNC请求，但是因为我们多写了一部分数据，从而与新master数据不一致了。
            changeReplicationId();
            clearReplicationId2();
            freeReplicationBacklog();
            serverLog(LL_NOTICE,
                "Replication backlog freed after %d seconds "
                "without connected replicas.",
                (int) server.repl_backlog_time_limit);
        }
    }

    /* If AOF is disabled and we no longer have attached slaves, we can
     * free our Replication Script Cache as there is no need to propagate
     * EVALSHA at all. */
    // [master]如果AOF被禁用并且我们没有slaves了，那么可以释放复制脚本缓存，因为不再需要传播EVALSHA命令了。
    if (listLength(server.slaves) == 0 &&
        server.aof_state == AOF_OFF &&
        listLength(server.repl_scriptcache_fifo) != 0)
    {
        replicationScriptCacheFlush();
    }

    // [master]检查是否有需要开启后台进程来处理RDB复制同步，有则处理。
    replicationStartPendingFork();

    /* Remove the RDB file used for replication if Redis is not running
     * with any persistence. */
    // 如果当前实例没有设置任何持久化配置，那么我们可以移除用于全量复制的RDB文件
    removeRDBUsedToSyncReplicas();

    /* Refresh the number of slaves with lag <= min-slaves-max-lag. */
    // 更新good slaves数量，用于Quorum判断。https://zhuanlan.zhihu.com/p/46551227
    refreshGoodSlavesCount();
    // 一次cron处理完成，增加统计
    replication_cron_loops++; /* Incremented with frequency 1 HZ. */
}

void replicationStartPendingFork(void) {
    /* Start a BGSAVE good for replication if we have slaves in
     * WAIT_BGSAVE_START state.
     *
     * In case of diskless replication, we make sure to wait the specified
     * number of seconds (according to configuration) so that other slaves
     * have the time to arrive before we start streaming. */
    // 如果有处于WAIT_BGSAVE_START状态的slaves，需要视情况为该slaves开启新的BGSAVE。
    // 在无盘复制的情况下，需要检查每个slave上次交互时间，有slave超时才进行重新开启BGSAVE处理。
    if (!hasActiveChildProcess()) {
        time_t idle, max_idle = 0;
        int slaves_waiting = 0;
        int mincapa = -1;
        listNode *ln;
        listIter li;

        // 遍历当前slaves
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                // 状态是BGSAVE_START，需要进行BGSAVE。
                // 如果是无盘传输的话，状态也是BGSAVE_START，所以我们需要判断无盘传输时，是否有超时，有超时才重新开始bgsave。
                idle = server.unixtime - slave->lastinteraction;
                if (idle > max_idle) max_idle = idle;
                // 记录等待slaves数，记录slaves能接收的传输方式。
                slaves_waiting++;
                mincapa = (mincapa == -1) ? slave->slave_capa :
                                            (mincapa & slave->slave_capa);
            }
        }

        if (slaves_waiting &&
            (!server.repl_diskless_sync ||
             max_idle >= server.repl_diskless_sync_delay))
        {
            /* Start the BGSAVE. The called function may start a
             * BGSAVE with socket target or disk target depending on the
             * configuration and slaves capabilities. */
            // 判断是否需要开始bgsave。有slaves在等待，如果不是无盘传输，显然需要开始bgsave；另外是无盘传输，但超时了，也需要重新开始。
            startBgsaveForReplication(mincapa);
        }
    }
}

/* Find replica at IP:PORT from replica list */
// 从slaves列表中找到对应IP、PORT的slave
static client *findReplica(char *host, int port) {
    listIter li;
    listNode *ln;
    client *replica;

    // 遍历slaves
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        replica = ln->value;
        char ip[NET_IP_STR_LEN], *replicaip = replica->slave_addr;

        // slave client中有保存该slave的ip则直接使用，否则通过client的conn去获取
        if (!replicaip) {
            if (connPeerToString(replica->conn, ip, sizeof(ip), NULL) == -1)
                continue;
            replicaip = ip;
        }

        // 比较host和port，如果一致则返回对应的slave，否则跳过对比下一个。
        if (!strcasecmp(host, replicaip) &&
                (port == replica->slave_listening_port))
            return replica;
    }

    return NULL;
}

const char *getFailoverStateString() {
    switch(server.failover_state) {
        case NO_FAILOVER: return "no-failover";
        case FAILOVER_IN_PROGRESS: return "failover-in-progress";
        case FAILOVER_WAIT_FOR_SYNC: return "waiting-for-sync";
        default: return "unknown";
    }
}

/* Resets the internal failover configuration, this needs
 * to be called after a failover either succeeds or fails
 * as it includes the client unpause. */
// 重置内部故障转移配置，在故障转移成功或失败之后需要调用此函数处理，因为它还会解除暂停的client。
void clearFailoverState() {
    server.failover_end_time = 0;
    server.force_failover = 0;
    zfree(server.target_replica_host);
    server.target_replica_host = NULL;
    server.target_replica_port = 0;
    server.failover_state = NO_FAILOVER;
    unpauseClients();
}

/* Abort an ongoing failover if one is going on. */
// 中止一个正在进行的故障转移
void abortFailover(const char *err) {
    // 没有在故障转移状态，直接返回
    if (server.failover_state == NO_FAILOVER) return;

    if (server.target_replica_host) {
        serverLog(LL_NOTICE,"FAILOVER to %s:%d aborted: %s",
            server.target_replica_host,server.target_replica_port,err);  
    } else {
        serverLog(LL_NOTICE,"FAILOVER to any replica aborted: %s",err);  
    }
    if (server.failover_state == FAILOVER_IN_PROGRESS) {
        // 正在进行故障转移，我们需要中止。取消所有进行中的复制，设置当前节点为master。
        replicationUnsetMaster();
    }
    // 清除故障转移状态
    clearFailoverState();
}

/* 
 * FAILOVER [TO <HOST> <PORT> [FORCE]] [ABORT] [TIMEOUT <timeout>]
 * 
 * This command will coordinate a failover between the master and one
 * of its replicas. The happy path contains the following steps:
 * 1) The master will initiate a client pause write, to stop replication
 * traffic.
 * 2) The master will periodically check if any of its replicas has
 * consumed the entire replication stream through acks. 
 * 3) Once any replica has caught up, the master will itself become a replica.
 * 4) The master will send a PSYNC FAILOVER request to the target replica, which
 * if accepted will cause the replica to become the new master and start a sync.
 * 
 * FAILOVER ABORT is the only way to abort a failover command, as replicaof
 * will be disabled. This may be needed if the failover is unable to progress. 
 * 
 * The optional arguments [TO <HOST> <IP>] allows designating a specific replica
 * to be failed over to.
 * 
 * FORCE flag indicates that even if the target replica is not caught up,
 * failover to it anyway. This must be specified with a timeout and a target
 * HOST and IP.
 * 
 * TIMEOUT <timeout> indicates how long should the primary wait for 
 * a replica to sync up before aborting. If not specified, the failover
 * will attempt forever and must be manually aborted.
 */
// FAILOVER [TO <HOST> <PORT> [FORCE]] [ABORT] [TIMEOUT <timeout>]
// 这个命令用于在master和它的一个slave之间进行协调故障转移。处理步骤如下：
// 1、master暂停client的写，停止复制流量。
// 2、master定期检查ACKs，看是否有slave已经处理了整个已传播的复制数据流。
// 3、一旦某个slave处理完，追上了master的复制offset，当前master将该slave设置为故障转移目标节点，并设置自己为slave。
// 4、master将发送PSYNC FAILOVER请求给目标slave，当该目标slave接受后，会处理变成新的master，并开始数据同步。

// FAILOVER ABORT是中止故障转移命令的唯一方法，因为故障转移期间，replicaof命令将会被禁止。当故障转移不能继续进行时，我们需要该命令来中止。
// 可选参数[TO <HOST> <IP>]允许我们指定一个特定的slave来进行故障转移成新master。
// FORCE标识表示无论目标slave是否追上master的复制offset，都故障转移到该slave。使用该标识时必须同时指定timeout和目标ip和port。
// TIMEOUT <timeout>指示主节点在中止之前应等待slave同步多长时间来追赶master的复制offset。
// 如果未指定timeout，将会一直等到slave追上offset再进行故障转移，这种情况必须手动使用FAILOVER ABORT来中止。
void failoverCommand(client *c) {
    // 集群模式不支持FAILOVER指令，需要使用CLUSTER FAILOVER
    if (server.cluster_enabled) {
        addReplyError(c,"FAILOVER not allowed in cluster mode. "
                        "Use CLUSTER FAILOVER command instead.");
        return;
    }
    
    /* Handle special case for abort */
    // 处理特殊的FAILOVER ABORT命令
    if ((c->argc == 2) && !strcasecmp(c->argv[1]->ptr,"abort")) {
        if (server.failover_state == NO_FAILOVER) {
            // 没有在故障转移，回复err
            addReplyError(c, "No failover in progress.");
            return;
        }

        // 处理abort
        abortFailover("Failover manually aborted");
        addReply(c,shared.ok);
        return;
    }

    long timeout_in_ms = 0;
    int force_flag = 0;
    long port = 0;
    char *host = NULL;

    /* Parse the command for syntax and arguments. */
    // 解析FAILOVER命令语法参数。
    for (int j = 1; j < c->argc; j++) {
        if (!strcasecmp(c->argv[j]->ptr,"timeout") && (j + 1 < c->argc) &&
            timeout_in_ms == 0)
        {
            // 解析timeout参数
            if (getLongFromObjectOrReply(c,c->argv[j + 1],
                        &timeout_in_ms,NULL) != C_OK) return;
            if (timeout_in_ms <= 0) {
                addReplyError(c,"FAILOVER timeout must be greater than 0");
                return;
            }
            j++;
        } else if (!strcasecmp(c->argv[j]->ptr,"to") && (j + 2 < c->argc) &&
            !host) 
        {
            // 解析故障转移目标slave的ip和port
            if (getLongFromObjectOrReply(c,c->argv[j + 2],&port,NULL) != C_OK)
                return;
            host = c->argv[j + 1]->ptr;
            j += 2;
        } else if (!strcasecmp(c->argv[j]->ptr,"force") && !force_flag) {
            // 是否带有force标识
            force_flag = 1;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    // 已经在故障转移了，当前指令不能处理。
    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c,"FAILOVER already in progress.");
        return;
    }

    // 如果节点是slave，不能执行故障转移指令。
    if (server.masterhost) {
        addReplyError(c,"FAILOVER is not valid when server is a replica.");
        return;
    }

    // 节点没有slaves，无法进行故障转移
    if (listLength(server.slaves) == 0) {
        addReplyError(c,"FAILOVER requires connected replicas.");
        return; 
    }

    // 有force标识，但是没有指定target slave或timeout，参数不合法。
    if (force_flag && (!timeout_in_ms || !host)) {
        addReplyError(c,"FAILOVER with force option requires both a timeout "
            "and target HOST and IP.");
        return;     
    }

    /* If a replica address was provided, validate that it is connected. */
    // 如果指定了target slave，我们还需要验证该slave是否是连接online的。
    if (host) {
        // 根据ip、port从server.slaves找对应slave
        client *replica = findReplica(host, port);

        if (replica == NULL) {
            addReplyError(c,"FAILOVER target HOST and PORT is not "
                            "a replica.");
            return;
        }

        /* Check if requested replica is online */
        // 检查slave的状态是否是online
        if (replica->replstate != SLAVE_STATE_ONLINE) {
            addReplyError(c,"FAILOVER target replica is not online.");
            return;
        }

        // 该slave可以进行故障转移，则设置为target。
        server.target_replica_host = zstrdup(host);
        server.target_replica_port = port;
        serverLog(LL_NOTICE,"FAILOVER requested to %s:%ld.",host,port);
    } else {
        serverLog(LL_NOTICE,"FAILOVER requested to any replica.");
    }

    // 通过timeout，计算出故障转移处理的截止时间。
    mstime_t now = mstime();
    if (timeout_in_ms) {
        server.failover_end_time = now + timeout_in_ms;
    }

    // 设置故障转移相关状态，等待cron任务中来检查处理故障转移，因为我们需要等待slave追上master的复制offset才能开始。
    server.force_failover = force_flag;
    server.failover_state = FAILOVER_WAIT_FOR_SYNC;
    /* Cluster failover will unpause eventually */
    // 暂停client的写命令执行，故障转移结束后最终会解除client暂停
    pauseClients(LLONG_MAX,CLIENT_PAUSE_WRITE);
    addReply(c,shared.ok);
}

/* Failover cron function, checks coordinated failover state. 
 *
 * Implementation note: The current implementation calls replicationSetMaster()
 * to start the failover request, this has some unintended side effects if the
 * failover doesn't work like blocked clients will be unblocked and replicas will
 * be disconnected. This could be optimized further.
 */
// 故障转移定时处理函数，检查协调故障转移状态进行处理。
// 实现说明：当前实现调用的replicationSetMaster()方法来开启故障转移请求，当故障转移失败时，会产生一些不好的副作用。
// 如阻塞的clients将全部解除阻塞，所有slaves将短暂的断开连接。这些可以进一步优化。
void updateFailoverStatus(void) {
    // 如果不是WAIT_FOR_SYNC状态，当前不需要故障转移，或者已经在处理中了，直接返回。
    if (server.failover_state != FAILOVER_WAIT_FOR_SYNC) return;
    mstime_t now = server.mstime;

    /* Check if failover operation has timed out */
    // 检查故障转移是否已经超时。
    if (server.failover_end_time && server.failover_end_time <= now) {
        if (server.force_failover) {
            // 如果超时了，但是我们设置的是强制故障转移，我们还是要处理。
            serverLog(LL_NOTICE,
                "FAILOVER to %s:%d time out exceeded, failing over.",
                server.target_replica_host, server.target_replica_port);
            // 设置故障转移状态。
            server.failover_state = FAILOVER_IN_PROGRESS;
            /* If timeout has expired force a failover if requested. */
            // 强制处理，这里调用replicationSetMaster来进行故障转移。
            replicationSetMaster(server.target_replica_host,
                server.target_replica_port);
            return;
        } else {
            /* Force was not requested, so timeout. */
            // 超时且不强制进行，abort。
            abortFailover("Replica never caught up before timeout");
            return;
        }
    }

    /* Check to see if the replica has caught up so failover can start */
    // 检查slave复制offset是否赶上进度，以便决定是否能开始故障转移。
    client *replica = NULL;
    if (server.target_replica_host) {
        // 如果有目标slave节点，则从slaves列表里查找目标client。
        replica = findReplica(server.target_replica_host, 
            server.target_replica_port);
    } else {
        listIter li;
        listNode *ln;

        listRewind(server.slaves,&li);
        /* Find any replica that has matched our repl_offset */
        // 没有目标节点，这里遍历slaves，从中找一个能匹配我们repl_offset的节点来进行故障转移。
        while((ln = listNext(&li))) {
            replica = ln->value;
            // 判断复制offset是否匹配。
            if (replica->repl_ack_off == server.master_repl_offset) {
                char ip[NET_IP_STR_LEN], *replicaaddr = replica->slave_addr;

                if (!replicaaddr) {
                    if (connPeerToString(replica->conn,ip,sizeof(ip),NULL) == -1)
                        continue;
                    replicaaddr = ip;
                }

                /* We are now failing over to this specific node */
                // 找到offset匹配的节点，将target指定为该节点的ip、port，后面使用该节点进行故障转移处理
                server.target_replica_host = zstrdup(replicaaddr);
                server.target_replica_port = replica->slave_listening_port;
                break;
            }
        }
    }

    /* We've found a replica that is caught up */
    // 到这如果我们找到了一个slave的复制offset与我们一致，则可以开始进行故障转移了。
    if (replica && (replica->repl_ack_off == server.master_repl_offset)) {
        // 更新故障转移状态
        server.failover_state = FAILOVER_IN_PROGRESS;
        serverLog(LL_NOTICE,
                "Failover target %s:%d is synced, failing over.",
                server.target_replica_host, server.target_replica_port);
        /* Designated replica is caught up, failover to it. */
        // 指定slave复制offset以追赶上，故障转移到该节点。
        replicationSetMaster(server.target_replica_host,
            server.target_replica_port);
    }
}
