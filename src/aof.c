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
#include "bio.h"
#include "rio.h"

#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <sys/param.h>

void aofUpdateCurrentSize(void);
void aofClosePipes(void);

/* ----------------------------------------------------------------------------
 * AOF rewrite buffer implementation.
 *
 * The following code implement a simple buffer used in order to accumulate
 * changes while the background process is rewriting the AOF file.
 *
 * We only need to append, but can't just use realloc with a large block
 * because 'huge' reallocs are not always handled as one could expect
 * (via remapping of pages at OS level) but may involve copying data.
 *
 * For this reason we use a list of blocks, every block is
 * AOF_RW_BUF_BLOCK_SIZE bytes.
 * ------------------------------------------------------------------------- */

#define AOF_RW_BUF_BLOCK_SIZE (1024*1024*10)    /* 10 MB per block */

typedef struct aofrwblock {
    unsigned long used, free;
    char buf[AOF_RW_BUF_BLOCK_SIZE];
} aofrwblock;

/* This function free the old AOF rewrite buffer if needed, and initialize
 * a fresh new one. It tests for server.aof_rewrite_buf_blocks equal to NULL
 * so can be used for the first initialization as well. */
void aofRewriteBufferReset(void) {
    if (server.aof_rewrite_buf_blocks)
        listRelease(server.aof_rewrite_buf_blocks);

    server.aof_rewrite_buf_blocks = listCreate();
    listSetFreeMethod(server.aof_rewrite_buf_blocks,zfree);
}

/* Return the current size of the AOF rewrite buffer. */
// 返回当前使用的AOF rewrite缓存大小
unsigned long aofRewriteBufferSize(void) {
    listNode *ln;
    listIter li;
    unsigned long size = 0;

    // AOF rewrite缓存是一块块内存组成的链表，这里挨个遍历连接节点，累加每个block中使用的内存量。
    listRewind(server.aof_rewrite_buf_blocks,&li);
    while((ln = listNext(&li))) {
        aofrwblock *block = listNodeValue(ln);
        size += block->used;
    }
    return size;
}

/* Event handler used to send data to the child process doing the AOF
 * rewrite. We send pieces of our AOF differences buffer so that the final
 * write when the child finishes the rewrite will be small. */
// 事件处理函数，用于发送数据到正在做AOF重写的子进程。
// 我们一部分一部分的发送差异数据，这样子进程处理完重写后，需要写入的数据就会较小，不会有很大延时。
void aofChildWriteDiffData(aeEventLoop *el, int fd, void *privdata, int mask) {
    listNode *ln;
    aofrwblock *block;
    ssize_t nwritten;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(privdata);
    UNUSED(mask);

    while(1) {
        // 依次获取AOF rewrite buf blocks
        ln = listFirst(server.aof_rewrite_buf_blocks);
        block = ln ? ln->value : NULL;
        // 如果有停止发送AOF accumulated diffs标识，则删除事件监听，返回。
        // 另外如果拿到的block为NULL，说明当前差异数据已经处理完了，也删除事件监听，返回。
        if (server.aof_stop_sending_diff || !block) {
            aeDeleteFileEvent(server.el,server.aof_pipe_write_data_to_child,
                              AE_WRITABLE);
            return;
        }
        // 处理当前拿到的block
        if (block->used > 0) {
            // 如果block有数据，则向fd中写入数据。
            nwritten = write(server.aof_pipe_write_data_to_child,
                             block->buf,block->used);
            // 没写成功，可能fd没准备好？直接返回
            if (nwritten <= 0) return;
            // 写成功了，从block中移除掉已处理的数据，更新block使用量，剩余可写量
            memmove(block->buf,block->buf+nwritten,block->used-nwritten);
            block->used -= nwritten;
            block->free += nwritten;
        }
        // 如果当前处理的block处理后，没数据需要再处理，将该block节点从AOF rewrite buf blocks队列删除
        if (block->used == 0) listDelNode(server.aof_rewrite_buf_blocks,ln);
    }
}

/* Append data to the AOF rewrite buffer, allocating new blocks if needed. */
// 追加数据到AOF重写buffer中，如果需要的话会重写分配新的blocks。
void aofRewriteBufferAppend(unsigned char *s, unsigned long len) {
    // 找到最后一个block，如果链表没有元素则block=NULL，后续需要进行空间分配。
    listNode *ln = listLast(server.aof_rewrite_buf_blocks);
    aofrwblock *block = ln ? ln->value : NULL;

    // 只要还有数据需要写入，就循环处理。
    while(len) {
        /* If we already got at least an allocated block, try appending
         * at least some piece into it. */
        if (block) {
            // 如果我们找到了一个block，看block中剩余多少空间，我们尽可能写入部分数据。
            // 这里根据block剩余空间大小和待写入数据大小，确定当前应该写入这个block数据量。
            unsigned long thislen = (block->free < len) ? block->free : len;
            if (thislen) {  /* The current block is not already full. */
                // 应该写入当前block数据大于0，说明当前block还能写入，这里处理写入，更新一些计数量。
                memcpy(block->buf+block->used, s, thislen);
                // 更新block的使用量，剩余可写量。
                block->used += thislen;
                block->free -= thislen;
                // 更新下次待写入的数据位置。
                s += thislen;
                // 更新剩余待写入数据量
                len -= thislen;
            }
        }

        if (len) { /* First block to allocate, or need another block. */
            // 走到这里，说明前面的blcok都写满了（或者之前没有block），还有新的数据要写入，需要分配一个block。
            int numblocks;

            // 分配一个新的block，每个block预分配10M空间。
            block = zmalloc(sizeof(*block));
            block->free = AOF_RW_BUF_BLOCK_SIZE;
            block->used = 0;
            // 将该block加入到aof_rewrite_buf_blocks队列中
            listAddNodeTail(server.aof_rewrite_buf_blocks,block);

            /* Log every time we cross more 10 or 100 blocks, respectively
             * as a notice or warning. */
            // 获取当前block数量，每10次分配block输出一下日志，每100次分配block调整日志级别为warning。
            numblocks = listLength(server.aof_rewrite_buf_blocks);
            if (((numblocks+1) % 10) == 0) {
                int level = ((numblocks+1) % 100) == 0 ? LL_WARNING :
                                                         LL_NOTICE;
                serverLog(level,"Background AOF buffer size: %lu MB",
                    aofRewriteBufferSize()/(1024*1024));
            }
        }
    }

    /* Install a file event to send data to the rewrite child if there is
     * not one already. */
    // 如果没有事件监听，创建一个事件监听用于与rewrite的子进程进行通信，传输AOF rewrite buffer数据。
    if (aeGetFileEvents(server.el,server.aof_pipe_write_data_to_child) == 0) {
        aeCreateFileEvent(server.el, server.aof_pipe_write_data_to_child,
            AE_WRITABLE, aofChildWriteDiffData, NULL);
    }
}

/* Write the buffer (possibly composed of multiple blocks) into the specified
 * fd. If a short write or any other error happens -1 is returned,
 * otherwise the number of bytes written is returned. */
// 将缓冲区（可能由多个块组成）数据写入指定的fd。如果发生部分写入或任何其他错误，返回-1，否则返回写入的字节数。
ssize_t aofRewriteBufferWrite(int fd) {
    listNode *ln;
    listIter li;
    ssize_t count = 0;

    // 遍历aof rewrite buf列表
    listRewind(server.aof_rewrite_buf_blocks,&li);
    while((ln = listNext(&li))) {
        aofrwblock *block = listNodeValue(ln);
        ssize_t nwritten;

        if (block->used) {
            // 如果block有存储数据，则将该block的数据一次性写入fd。
            nwritten = write(fd,block->buf,block->used);
            if (nwritten != (ssize_t)block->used) {
                if (nwritten == 0) errno = EIO;
                return -1;
            }
            count += nwritten;
        }
    }
    return count;
}

/* ----------------------------------------------------------------------------
 * AOF file implementation
 * ------------------------------------------------------------------------- */

/* Return true if an AOf fsync is currently already in progress in a
 * BIO thread. */
// 如果当前后台线程在处理aof刷盘（即pending队列中有job待处理），则返回true。
int aofFsyncInProgress(void) {
    return bioPendingJobsOfType(BIO_AOF_FSYNC) != 0;
}

/* Starts a background task that performs fsync() against the specified
 * file descriptor (the one of the AOF file) in another thread. */
void aof_background_fsync(int fd) {
    bioCreateFsyncJob(fd);
}

/* Kills an AOFRW child process if exists */
void killAppendOnlyChild(void) {
    int statloc;
    /* No AOFRW child? return. */
    if (server.child_type != CHILD_TYPE_AOF) return;
    /* Kill AOFRW child, wait for child exit. */
    serverLog(LL_NOTICE,"Killing running AOF rewrite child: %ld",
        (long) server.child_pid);
    if (kill(server.child_pid,SIGUSR1) != -1) {
        while(waitpid(-1, &statloc, 0) != server.child_pid);
    }
    /* Reset the buffer accumulating changes while the child saves. */
    aofRewriteBufferReset();
    aofRemoveTempFile(server.child_pid);
    resetChildState();
    server.aof_rewrite_time_start = -1;
    /* Close pipes used for IPC between the two processes. */
    aofClosePipes();
}

/* Called when the user switches from "appendonly yes" to "appendonly no"
 * at runtime using the CONFIG command. */
void stopAppendOnly(void) {
    serverAssert(server.aof_state != AOF_OFF);
    flushAppendOnlyFile(1);
    if (redis_fsync(server.aof_fd) == -1) {
        serverLog(LL_WARNING,"Fail to fsync the AOF file: %s",strerror(errno));
    } else {
        server.aof_fsync_offset = server.aof_current_size;
        server.aof_last_fsync = server.unixtime;
    }
    close(server.aof_fd);

    server.aof_fd = -1;
    server.aof_selected_db = -1;
    server.aof_state = AOF_OFF;
    server.aof_rewrite_scheduled = 0;
    killAppendOnlyChild();
    sdsfree(server.aof_buf);
    server.aof_buf = sdsempty();
}

/* Called when the user switches from "appendonly no" to "appendonly yes"
 * at runtime using the CONFIG command. */
// 当用户在运行时使用CONFIG命令将服务由"appendonly no"切换为"appendonly yes"，即启用AOF时，调用这个函数。
int startAppendOnly(void) {
    // 存储当前进程的工作目录
    char cwd[MAXPATHLEN]; /* Current working dir path for error messages. */
    int newfd;

    // 打开aof文件，确认当前AOF状态是关闭的
    newfd = open(server.aof_filename,O_WRONLY|O_APPEND|O_CREAT,0644);
    serverAssert(server.aof_state == AOF_OFF);
    if (newfd == -1) {
        // 文件打开失败，获取当前进程工作目录，打印日志。
        char *cwdp = getcwd(cwd,MAXPATHLEN);

        serverLog(LL_WARNING,
            "Redis needs to enable the AOF but can't open the "
            "append only file %s (in server root dir %s): %s",
            server.aof_filename,
            cwdp ? cwdp : "unknown",
            strerror(errno));
        return C_ERR;
    }
    if (hasActiveChildProcess() && server.child_type != CHILD_TYPE_AOF) {
        // 如果当前有后台进程，但子进程不是AOF重写子进程。则将AOF重写标记为待调度处理。
        server.aof_rewrite_scheduled = 1;
        serverLog(LL_WARNING,"AOF was enabled but there is already another background operation. An AOF background was scheduled to start when possible.");
    } else {
        /* If there is a pending AOF rewrite, we need to switch it off and
         * start a new one: the old one cannot be reused because it is not
         * accumulating the AOF buffer. */
        // 如果当前已经有一个正在执行的AOF重写子进程，我们需要kill掉，并开启一个新的AOF重写调度任务，因为旧的AOF文件并不是完整最新的数据。
        if (server.child_type == CHILD_TYPE_AOF) {
            serverLog(LL_WARNING,"AOF was enabled but there is already an AOF rewriting in background. Stopping background AOF and starting a rewrite now.");
            killAppendOnlyChild();
        }
        // 启动后台AOF重写进程处理
        if (rewriteAppendOnlyFileBackground() == C_ERR) {
            close(newfd);
            serverLog(LL_WARNING,"Redis needs to enable the AOF but can't trigger a background AOF rewrite operation. Check the above logs for more info about the error.");
            return C_ERR;
        }
    }
    /* We correctly switched on AOF, now wait for the rewrite to be complete
     * in order to append data on disk. */
    // 我们已经正确的启用AOF，现在等待AOF重写完成，然后将数据追加到磁盘上。
    server.aof_state = AOF_WAIT_REWRITE;
    server.aof_last_fsync = server.unixtime;
    server.aof_fd = newfd;

    /* If AOF fsync error in bio job, we just ignore it and log the event. */
    // 如果后台io job中AOF fsync出错了，这里忽略并记录日志。
    int aof_bio_fsync_status;
    atomicGet(server.aof_bio_fsync_status, aof_bio_fsync_status);
    if (aof_bio_fsync_status == C_ERR) {
        serverLog(LL_WARNING,
            "AOF reopen, just ignore the AOF fsync error in bio job");
        atomicSet(server.aof_bio_fsync_status,C_OK);
    }

    /* If AOF was in error state, we just ignore it and log the event. */
    // 如果AOF出错了，忽略并记录日志。
    if (server.aof_last_write_status == C_ERR) {
        serverLog(LL_WARNING,"AOF reopen, just ignore the last error.");
        server.aof_last_write_status = C_OK;
    }
    return C_OK;
}

/* This is a wrapper to the write syscall in order to retry on short writes
 * or if the syscall gets interrupted. It could look strange that we retry
 * on short writes given that we are writing to a block device: normally if
 * the first call is short, there is a end-of-space condition, so the next
 * is likely to fail. However apparently in modern systems this is no longer
 * true, and in general it looks just more resilient to retry the write. If
 * there is an actual error condition we'll get it at the next try. */
// 这是write系统调用的包装函数，用于在部分写或写中断时进行重试。
// 我们正在写入一个块设备，对于部分写入进行重试看起来很奇怪，因为通常第一次只写入很少数据时，可能空间快要写满了，那么下一次写很可能也是失败的。
// 但是在现代系统中不再是这样，并且一般重试write也更具有弹性。如果write确实发生了错误，我们将在下一次重试中检测处理。
ssize_t aofWrite(int fd, const char *buf, size_t len) {
    ssize_t nwritten = 0, totwritten = 0;

    while(len) {
        // 循环处理写入，直到全部数据都写完，或者写出错了退出。
        nwritten = write(fd, buf, len);

        if (nwritten < 0) {
            if (errno == EINTR) continue;
            return totwritten ? totwritten : -1;
        }

        len -= nwritten;
        buf += nwritten;
        totwritten += nwritten;
    }

    return totwritten;
}

/* Write the append only file buffer on disk.
 *
 * Since we are required to write the AOF before replying to the client,
 * and the only way the client socket can get a write is entering when the
 * the event loop, we accumulate all the AOF writes in a memory
 * buffer and write it on disk using this function just before entering
 * the event loop again.
 *
 * About the 'force' argument:
 *
 * When the fsync policy is set to 'everysec' we may delay the flush if there
 * is still an fsync() going on in the background thread, since for instance
 * on Linux write(2) will be blocked by the background fsync anyway.
 * When this happens we remember that there is some aof buffer to be
 * flushed ASAP, and will try to do that in the serverCron() function.
 *
 * However if force is set to 1 we'll write regardless of the background
 * fsync. */
// 将AOF buffer数据写到磁盘文件上。
// 由于我们要求写AOF要在回复client之前，并且client socket写数据的唯一途径是在event loop中，
// 所以我们累积所有的AOF写操作在内存buffer中，并在再次进入事件循环前的beforeSleep中调用这个方法来写入磁盘。

// 'force'参数：当fsync的策略是每秒时，如果当前有一个后台线程正在进行fsync()，我们可能会延时进行flush，因为write(2)会被后台fsync阻塞。
// 所以当有后台线程在fsync时，我们记录标识，然后后续在serverCron()中尽快处理。当然如果force设置为1，我们不会管后台fsync，这里直接write。
#define AOF_WRITE_LOG_ERROR_RATE 30 /* Seconds between errors logging. */
void flushAppendOnlyFile(int force) {
    ssize_t nwritten;
    int sync_in_progress = 0;
    mstime_t latency;

    if (sdslen(server.aof_buf) == 0) {
        /* Check if we need to do fsync even the aof buffer is empty,
         * because previously in AOF_FSYNC_EVERYSEC mode, fsync is
         * called only when aof buffer is not empty, so if users
         * stop write commands before fsync called in one second,
         * the data in page cache cannot be flushed in time. */
        // aof_buf没数据，这里检查aof buf为空的时候我们是否需还要fsync。
        // 因为之前在AOF_FSYNC_EVERYSEC模式，fsync仅在aof buffer不是空的情况下才调用，
        // 所以如果用户1s内在fsync之前停止写命令，页面缓存里的数据将不会被及时刷盘。
        if (server.aof_fsync == AOF_FSYNC_EVERYSEC &&
            server.aof_fsync_offset != server.aof_current_size &&
            server.unixtime > server.aof_last_fsync &&
            !(sync_in_progress = aofFsyncInProgress())) {
            // 如果是everysec策略，上一次刷盘后有新的aof数据且时间过了1s，
            // 并且当前后台没有正在处理aof刷盘job，则尝试调用fsync处理，否则直接return。
            goto try_fsync;
        } else {
            return;
        }
    }

    // everysec策略，需要查看后台aof fsync线程是否在处理刷盘任务。
    if (server.aof_fsync == AOF_FSYNC_EVERYSEC)
        sync_in_progress = aofFsyncInProgress();

    if (server.aof_fsync == AOF_FSYNC_EVERYSEC && !force) {
        /* With this append fsync policy we do background fsyncing.
         * If the fsync is still in progress we can try to delay
         * the write for a couple of seconds. */
        // everysec策略时，如果后台线程正在处理aof刷盘，而这里我们又没有要求强制处理，所以可以将当前的write延迟几秒处理。
        if (sync_in_progress) {
            if (server.aof_flush_postponed_start == 0) {
                /* No previous write postponing, remember that we are
                 * postponing the flush and return. */
                // 如果之前没有被推迟的flush，记录我们当前被推迟的处理的开始时间，然后返回。
                server.aof_flush_postponed_start = server.unixtime;
                return;
            } else if (server.unixtime - server.aof_flush_postponed_start < 2) {
                /* We were already waiting for fsync to finish, but for less
                 * than two seconds this is still ok. Postpone again. */
                // 如果我们之前已经有被推迟的flush，但持续时间没有超过2s，不是很严重，继续推迟。
                return;
            }
            /* Otherwise fall trough, and go write since we can't wait
             * over two seconds. */
            // 当我们被推迟flush时间超过了2s，问题就很严重了，增加统计计数并打日志后，继续向后执行，准备开始处理write。
            server.aof_delayed_fsync++;
            serverLog(LL_NOTICE,"Asynchronous AOF fsync is taking too long (disk is busy?). Writing the AOF buffer without waiting for fsync to complete, this may slow down Redis.");
        }
    }
    /* We want to perform a single write. This should be guaranteed atomic
     * at least if the filesystem we are writing is a real physical one.
     * While this will save us against the server being killed I don't think
     * there is much to do about the whole server stopping for power problems
     * or alike */
    // 我们想执行一个write操作，如果当前我们操作write的文件系统是一个物理文件系统，那么至少应该保证写入原子性。
    // 这样可以保证服务被kill掉时，数据不会错乱。不过如果突然断电，那我们也没什么可做的，怎么做也没用。

    if (server.aof_flush_sleep && sdslen(server.aof_buf)) {
        // flush前sleep，用于测试
        usleep(server.aof_flush_sleep);
    }

    // 监控延迟write延时
    latencyStartMonitor(latency);
    // 调用write将aof_buf中的数据写入aof_fd中。
    nwritten = aofWrite(server.aof_fd,server.aof_buf,sdslen(server.aof_buf));
    latencyEndMonitor(latency);
    /* We want to capture different events for delayed writes:
     * when the delay happens with a pending fsync, or with a saving child
     * active, and when the above two conditions are missing.
     * We also use an additional event name to save all samples which is
     * useful for graphing / monitoring purposes. */
    // 我们使用捕获的不同事件，来将该delayed writes的延时加入不同的统计中。
    if (sync_in_progress) {
        latencyAddSampleIfNeeded("aof-write-pending-fsync",latency);
    } else if (hasActiveChildProcess()) {
        latencyAddSampleIfNeeded("aof-write-active-child",latency);
    } else {
        latencyAddSampleIfNeeded("aof-write-alone",latency);
    }
    latencyAddSampleIfNeeded("aof-write",latency);

    /* We performed the write so reset the postponed flush sentinel to zero. */
    // 我们执行了write操作，所以延时刷新开始时间重置为0
    server.aof_flush_postponed_start = 0;

    if (nwritten != (ssize_t)sdslen(server.aof_buf)) {
        // 如果我们write报错了，导致aof_buf中数据只写入了部分。因为我们不允许部分写，所以这里需要做一些清理。
        static time_t last_write_error_log = 0;
        int can_log = 0;

        /* Limit logging rate to 1 line per AOF_WRITE_LOG_ERROR_RATE seconds. */
        // 限制日志记录的频率为 AOF_WRITE_LOG_ERROR_RATE 秒1次。
        if ((server.unixtime - last_write_error_log) > AOF_WRITE_LOG_ERROR_RATE) {
            can_log = 1;
            last_write_error_log = server.unixtime;
        }

        /* Log the AOF write error and record the error code. */
        // AOF写入出错了，记录日志及出错信息
        if (nwritten == -1) {
            // 这里第一次写入就报错了，不需要做数据清理。
            if (can_log) {
                serverLog(LL_WARNING,"Error writing to the AOF file: %s",
                    strerror(errno));
                server.aof_last_write_errno = errno;
            }
        } else {
            // 写进去了部分数据，需要做一些清理。
            if (can_log) {
                serverLog(LL_WARNING,"Short write while writing to "
                                       "the AOF file: (nwritten=%lld, "
                                       "expected=%lld)",
                                       (long long)nwritten,
                                       (long long)sdslen(server.aof_buf));
            }

            // ftruncate，截取server.aof_current_size大小的数据，本次write的数据都清除掉。
            if (ftruncate(server.aof_fd, server.aof_current_size) == -1) {
                if (can_log) {
                    serverLog(LL_WARNING, "Could not remove short write "
                             "from the append-only file.  Redis may refuse "
                             "to load the AOF the next time it starts.  "
                             "ftruncate: %s", strerror(errno));
                }
            } else {
                /* If the ftruncate() succeeded we can set nwritten to
                 * -1 since there is no longer partial data into the AOF. */
                // ftruncate()成功了，这里设置nwritten为-1标识AOF中目前没有部分写入。
                nwritten = -1;
            }
            // 设置err为空间不足错误
            server.aof_last_write_errno = ENOSPC;
        }

        /* Handle the AOF write error. */
        // 这里处理AOF write err
        if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
            /* We can't recover when the fsync policy is ALWAYS since the reply
             * for the client is already in the output buffers (both writes and
             * reads), and the changes to the db can't be rolled back. Since we
             * have a contract with the user that on acknowledged or observed
             * writes are is synced on disk, we must exit. */
            // 当fsync策略为ALWAYS时，我们无法恢复，因为client的回复已经在输出缓冲区中，并且无法回滚对db的更改。
            // 因为我们发送给用户回复就表示确认变更已经同步到磁盘了，所以这里我们必须退出（有点耍无赖啊- -）。
            serverLog(LL_WARNING,"Can't recover from AOF write error when the AOF fsync policy is 'always'. Exiting...");
            exit(1);
        } else {
            /* Recover from failed write leaving data into the buffer. However
             * set an error to stop accepting writes as long as the error
             * condition is not cleared. */
            // 将数据保留在缓冲区中，并进行恢复处理。这里我们设置错误状态，来停止接受client新的命令数据写入。
            server.aof_last_write_status = C_ERR;

            /* Trim the sds buffer if there was a partial write, and there
             * was no way to undo it with ftruncate(2). */
            // nwritten > 0 表示我们前面ftruncate失败了，所以没办法，只能将就部分写操作。
            // 这里需要更新aof_current_size，并trim掉aof_buf中以部分写入的那块。
            // 而nwritten = -1，则表示ftruncate成功了，这里就不用处理了。所以也只是尽可能保证写入的原子性。
            if (nwritten > 0) {
                server.aof_current_size += nwritten;
                sdsrange(server.aof_buf,nwritten,-1);
            }
            // 出错了，只能清理完成后返回，等待下一次重新调用这个方法再写入。
            return; /* We'll try again on the next call... */
        }
    } else {
        /* Successful write(2). If AOF was in error state, restore the
         * OK state and log the event. */
        // 这里我们成功执行了write(2)，aof_buf数据是全部write完的。这里我们检查AOF状态，如果有err则重置为ok
        if (server.aof_last_write_status == C_ERR) {
            serverLog(LL_WARNING,
                "AOF write error looks solved, Redis can write again.");
            server.aof_last_write_status = C_OK;
        }
    }
    // 更新当前aof大小aof_current_size。
    server.aof_current_size += nwritten;

    /* Re-use AOF buffer when it is small enough. The maximum comes from the
     * arena size of 4k minus some overhead (but is otherwise arbitrary). */
    // AOF缓冲区的数据flush完了，这里处理重新使用AOF buffer空间。
    // 如果总的aof_buf空间大小小于4k，则置空，重复使用。如果超出了4k，则释放掉，新构建空的sds来作为aof buffer。避免过多空间的浪费。
    if ((sdslen(server.aof_buf)+sdsavail(server.aof_buf)) < 4000) {
        sdsclear(server.aof_buf);
    } else {
        sdsfree(server.aof_buf);
        server.aof_buf = sdsempty();
    }

try_fsync:
    /* Don't fsync if no-appendfsync-on-rewrite is set to yes and there are
     * children doing I/O in the background. */
    // 如果设置了在rewrite不fsync，那么当有子进程在后台做RDB save或AOF rewrite时，我们不做fsync。
    if (server.aof_no_fsync_on_rewrite && hasActiveChildProcess())
        return;

    /* Perform the fsync if needed. */
    // 如果需要，执行fsync
    if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
        // always表示每执行命令总是进行fsync，所以这里需要处理。
        /* redis_fsync is defined as fdatasync() for Linux in order to avoid
         * flushing metadata. */
        // redis_fsync在linux下定义为fdatasync()，以避免刷新元数据。

        // https://blog.csdn.net/sishuiliunian0710/article/details/37739385
        //  sync: 把page cache中的高速缓存的所有文件的脏页，super block, 以及inode本身刷新到磁盘。
        //  fsync: 把指定文件的脏页写到磁盘，包括page cache和inode本身。
        //  fdatasync: 只把制定文件的page cache写入磁盘，忽略inode本身。
        //  fflush: 强制将该文件对应的用户空间glibc里面的的4KB的buffer写入内核缓存。
        latencyStartMonitor(latency);
        /* Let's try to get this data on the disk. To guarantee data safe when
         * the AOF fsync policy is 'always', we should exit if failed to fsync
         * AOF (see comment next to the exit(1) after write error above). */
        // 尝试fsync数据到磁盘。为了保证数据安全，当刷盘策略是'always'时，如果刷盘失败需要直接退出。
        if (redis_fsync(server.aof_fd) == -1) {
            serverLog(LL_WARNING,"Can't persist AOF for fsync error when the "
              "AOF fsync policy is 'always': %s. Exiting...", strerror(errno));
            exit(1);
        }
        // 刷盘成功，加入延时统计。
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-fsync-always",latency);
        // 更新fsync相关信息
        server.aof_fsync_offset = server.aof_current_size;
        server.aof_last_fsync = server.unixtime;
    } else if ((server.aof_fsync == AOF_FSYNC_EVERYSEC &&
                server.unixtime > server.aof_last_fsync)) {
        // 如果是每秒刷盘，且时间过了1s，我们要处理刷盘，使用异步线程来执行真正刷盘。
        if (!sync_in_progress) {
            // 只有后台AOF刷盘线程没有任务在执行时，这里才创建提交AOF刷盘job。
            aof_background_fsync(server.aof_fd);
            server.aof_fsync_offset = server.aof_current_size;
        }
        // 当前有AOF刷盘任务在排队的话，下一个任务处理的时候，所有数据都会一起刷盘，所有不需要单独在创建提交job，直接更新fsync时间就好了。
        server.aof_last_fsync = server.unixtime;
    }
}

sds catAppendOnlyGenericCommand(sds dst, int argc, robj **argv) {
    char buf[32];
    int len, j;
    robj *o;

    buf[0] = '*';
    len = 1+ll2string(buf+1,sizeof(buf)-1,argc);
    buf[len++] = '\r';
    buf[len++] = '\n';
    dst = sdscatlen(dst,buf,len);

    for (j = 0; j < argc; j++) {
        o = getDecodedObject(argv[j]);
        buf[0] = '$';
        len = 1+ll2string(buf+1,sizeof(buf)-1,sdslen(o->ptr));
        buf[len++] = '\r';
        buf[len++] = '\n';
        dst = sdscatlen(dst,buf,len);
        dst = sdscatlen(dst,o->ptr,sdslen(o->ptr));
        dst = sdscatlen(dst,"\r\n",2);
        decrRefCount(o);
    }
    return dst;
}

/* Create the sds representation of a PEXPIREAT command, using
 * 'seconds' as time to live and 'cmd' to understand what command
 * we are translating into a PEXPIREAT.
 *
 * This command is used in order to translate EXPIRE and PEXPIRE commands
 * into PEXPIREAT command so that we retain precision in the append only
 * file, and the time is always absolute and not relative. */
sds catAppendOnlyExpireAtCommand(sds buf, struct redisCommand *cmd, robj *key, robj *seconds) {
    long long when;
    robj *argv[3];

    /* Make sure we can use strtoll */
    seconds = getDecodedObject(seconds);
    when = strtoll(seconds->ptr,NULL,10);
    /* Convert argument into milliseconds for EXPIRE, SETEX, EXPIREAT */
    if (cmd->proc == expireCommand || cmd->proc == setexCommand ||
        cmd->proc == expireatCommand)
    {
        when *= 1000;
    }
    /* Convert into absolute time for EXPIRE, PEXPIRE, SETEX, PSETEX */
    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == setexCommand || cmd->proc == psetexCommand)
    {
        when += mstime();
    }
    decrRefCount(seconds);

    argv[0] = shared.pexpireat;
    argv[1] = key;
    argv[2] = createStringObjectFromLongLong(when);
    buf = catAppendOnlyGenericCommand(buf, 3, argv);
    decrRefCount(argv[2]);
    return buf;
}

// 命令传播写入AOF
void feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc) {
    sds buf = sdsempty();
    /* The DB this command was targeting is not the same as the last command
     * we appended. To issue a SELECT command is needed. */
    // 如果当前执行命令的DB与上一次写入AOF的命令的DB不一致的话，需要先发送一个select命令重新选择DB。
    if (dictid != server.aof_selected_db) {
        char seldb[64];

        // 将dictid格式化后放在seldb中，末尾补'\n'。如果格式化后长度超过sizeof(seldb)则截取后补'\n'。
        snprintf(seldb,sizeof(seldb),"%d",dictid);
        // 往buf中写入select命令。使用RESP协议。MULTIBULK请求。
        buf = sdscatprintf(buf,"*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n",
            (unsigned long)strlen(seldb),seldb);
        // 更新全局AOF选择的DB
        server.aof_selected_db = dictid;
    }

    // 写入AOF前命令处理
    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == expireatCommand) {
        /* Translate EXPIRE/PEXPIRE/EXPIREAT into PEXPIREAT */
        // 如果是设置key过期时间的命令，统一转换为PEXPIREAT命令。
        buf = catAppendOnlyExpireAtCommand(buf,cmd,argv[1],argv[2]);
    } else if (cmd->proc == setCommand && argc > 3) {
        robj *pxarg = NULL;
        /* When SET is used with EX/PX argument setGenericCommand propagates them with PX millisecond argument.
         * So since the command arguments are re-written there, we can rely here on the index of PX being 3. */
        // 当SET带有EX/PX参数时，使用setGenericCommand中会统一换成PX毫秒参数。
        // 因为这个命令的参数在setCommand中通过parseExtendedStringArgumentsOrReply进行解析，然后在setGenericCommand中重写了。
        // 所以这里我们可以依赖于前面重写的操作，认为PX参数在index=3的位置，如果该位置没有则没设置过期时间。（SET K V px...）
        if (!strcasecmp(argv[3]->ptr, "px")) {
            // 如果第index=3的位置是px，则px时间数据就在第4位。
            pxarg = argv[4];
        }
        /* For AOF we convert SET key value relative time in milliseconds to SET key value absolute time in
         * millisecond. Whenever the condition is true it implies that original SET has been transformed
         * to SET PX with millisecond time argument so we do not need to worry about unit here.*/
        // 对于AOF，我们将SET命令的过期相对时间转换为绝对时间。SET PX改为SET PXAT
        // 只要这个if条件为true，就意味着原来的SET命令被重写为（SET PX 毫秒时间）了，所以这里我们不需要考虑单位。
        if (pxarg) {
            // 转换为数字类型
            robj *millisecond = getDecodedObject(pxarg);
            long long when = strtoll(millisecond->ptr,NULL,10);
            when += mstime();

            decrRefCount(millisecond);

            // 重写命令的参数
            robj *newargs[5];
            newargs[0] = argv[0];
            newargs[1] = argv[1];
            newargs[2] = argv[2];
            newargs[3] = shared.pxat;
            newargs[4] = createStringObjectFromLongLong(when);
            // 构建新的SET PXAT命令写到buf中
            buf = catAppendOnlyGenericCommand(buf,5,newargs);
            decrRefCount(newargs[4]);
        } else {
            // 没有过期时间参数，直接使用原SET命令写入buf
            buf = catAppendOnlyGenericCommand(buf,argc,argv);
        }
    } else {
        /* All the other commands don't need translation or need the
         * same translation already operated in the command vector
         * for the replication itself. */
        // 除SET带过期时间的命令外，其他的命令都原样传输，不需要在进行转换。
        // 或者有需要转换的，slave自己会解析后放在命令参数向量中。
        buf = catAppendOnlyGenericCommand(buf,argc,argv);
    }

    /* Append to the AOF buffer. This will be flushed on disk just before
     * of re-entering the event loop, so before the client will get a
     * positive reply about the operation performed. */
    // 如果服务的AOF是开启状态，这里将命令组装的buf数据追加到服务的aof_buf中。
    // 等待重新进入event loop前before_sleep函数中处理刷盘。即会在client获得成功执行的回复前刷盘。
    if (server.aof_state == AOF_ON)
        server.aof_buf = sdscatlen(server.aof_buf,buf,sdslen(buf));

    /* If a background append only file rewriting is in progress we want to
     * accumulate the differences between the child DB and the current one
     * in a buffer, so that when the child process will do its work we
     * can append the differences to the new append only file. */
    // 如果后台正在处理AOF rewrite，我们想将子进程DB和当前DB数据的差异累积存储在一个buffer中。
    // 等待子进程处理完成AOF重写后，我们可以将这些差异追加到新的AOF文件中。
    if (server.child_type == CHILD_TYPE_AOF)
        aofRewriteBufferAppend((unsigned char*)buf,sdslen(buf));

    sdsfree(buf);
}

/* ----------------------------------------------------------------------------
 * AOF loading
 * ------------------------------------------------------------------------- */

/* In Redis commands are always executed in the context of a client, so in
 * order to load the append only file we need to create a fake client. */
struct client *createAOFClient(void) {
    struct client *c = zmalloc(sizeof(*c));

    selectDb(c,0);
    c->id = CLIENT_ID_AOF; /* So modules can identify it's the AOF client. */
    c->conn = NULL;
    c->name = NULL;
    c->querybuf = sdsempty();
    c->querybuf_peak = 0;
    c->argc = 0;
    c->argv = NULL;
    c->original_argc = 0;
    c->original_argv = NULL;
    c->argv_len_sum = 0;
    c->bufpos = 0;

    /*
     * The AOF client should never be blocked (unlike master
     * replication connection).
     * This is because blocking the AOF client might cause
     * deadlock (because potentially no one will unblock it).
     * Also, if the AOF client will be blocked just for
     * background processing there is a chance that the
     * command execution order will be violated.
     */
    c->flags = CLIENT_DENY_BLOCKING;

    c->btype = BLOCKED_NONE;
    /* We set the fake client as a slave waiting for the synchronization
     * so that Redis will not try to send replies to this client. */
    // 我们设置这个构造的client作为一个slave，正等待RDB同步。这样Redis就不会发送复制数据到这个client。
    c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
    c->reply = listCreate();
    c->reply_bytes = 0;
    c->obuf_soft_limit_reached_time = 0;
    c->watched_keys = listCreate();
    c->peerid = NULL;
    c->sockname = NULL;
    c->resp = 2;
    c->user = NULL;
    listSetFreeMethod(c->reply,freeClientReplyValue);
    listSetDupMethod(c->reply,dupClientReplyValue);
    initClientMultiState(c);
    return c;
}

void freeFakeClientArgv(struct client *c) {
    int j;

    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    zfree(c->argv);
    c->argv_len_sum = 0;
}

void freeFakeClient(struct client *c) {
    sdsfree(c->querybuf);
    listRelease(c->reply);
    listRelease(c->watched_keys);
    freeClientMultiState(c);
    freeClientOriginalArgv(c);
    zfree(c);
}

/* Replay the append log file. On success C_OK is returned. On non fatal
 * error (the append only file is zero-length) C_ERR is returned. On
 * fatal error an error message is logged and the program exists. */
int loadAppendOnlyFile(char *filename) {
    struct client *fakeClient;
    FILE *fp = fopen(filename,"r");
    struct redis_stat sb;
    int old_aof_state = server.aof_state;
    long loops = 0;
    off_t valid_up_to = 0; /* Offset of latest well-formed command loaded. */
    off_t valid_before_multi = 0; /* Offset before MULTI command loaded. */

    if (fp == NULL) {
        serverLog(LL_WARNING,"Fatal error: can't open the append log file for reading: %s",strerror(errno));
        exit(1);
    }

    /* Handle a zero-length AOF file as a special case. An empty AOF file
     * is a valid AOF because an empty server with AOF enabled will create
     * a zero length file at startup, that will remain like that if no write
     * operation is received. */
    if (fp && redis_fstat(fileno(fp),&sb) != -1 && sb.st_size == 0) {
        server.aof_current_size = 0;
        server.aof_fsync_offset = server.aof_current_size;
        fclose(fp);
        return C_ERR;
    }

    /* Temporarily disable AOF, to prevent EXEC from feeding a MULTI
     * to the same file we're about to read. */
    server.aof_state = AOF_OFF;

    fakeClient = createAOFClient();
    startLoadingFile(fp, filename, RDBFLAGS_AOF_PREAMBLE);

    /* Check if this AOF file has an RDB preamble. In that case we need to
     * load the RDB file and later continue loading the AOF tail. */
    char sig[5]; /* "REDIS" */
    if (fread(sig,1,5,fp) != 5 || memcmp(sig,"REDIS",5) != 0) {
        /* No RDB preamble, seek back at 0 offset. */
        if (fseek(fp,0,SEEK_SET) == -1) goto readerr;
    } else {
        /* RDB preamble. Pass loading the RDB functions. */
        rio rdb;

        serverLog(LL_NOTICE,"Reading RDB preamble from AOF file...");
        if (fseek(fp,0,SEEK_SET) == -1) goto readerr;
        rioInitWithFile(&rdb,fp);
        if (rdbLoadRio(&rdb,RDBFLAGS_AOF_PREAMBLE,NULL) != C_OK) {
            serverLog(LL_WARNING,"Error reading the RDB preamble of the AOF file, AOF loading aborted");
            goto readerr;
        } else {
            serverLog(LL_NOTICE,"Reading the remaining AOF tail...");
        }
    }

    /* Read the actual AOF file, in REPL format, command by command. */
    while(1) {
        int argc, j;
        unsigned long len;
        robj **argv;
        char buf[128];
        sds argsds;
        struct redisCommand *cmd;

        /* Serve the clients from time to time */
        if (!(loops++ % 1000)) {
            loadingProgress(ftello(fp));
            processEventsWhileBlocked();
            processModuleLoadingProgressEvent(1);
        }

        if (fgets(buf,sizeof(buf),fp) == NULL) {
            if (feof(fp))
                break;
            else
                goto readerr;
        }
        if (buf[0] != '*') goto fmterr;
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1) goto fmterr;

        /* Load the next command in the AOF as our fake client
         * argv. */
        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;

        for (j = 0; j < argc; j++) {
            /* Parse the argument len. */
            char *readres = fgets(buf,sizeof(buf),fp);
            if (readres == NULL || buf[0] != '$') {
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                if (readres == NULL)
                    goto readerr;
                else
                    goto fmterr;
            }
            len = strtol(buf+1,NULL,10);

            /* Read it into a string object. */
            argsds = sdsnewlen(SDS_NOINIT,len);
            if (len && fread(argsds,len,1,fp) == 0) {
                sdsfree(argsds);
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
            argv[j] = createObject(OBJ_STRING,argsds);

            /* Discard CRLF. */
            if (fread(buf,2,1,fp) == 0) {
                fakeClient->argc = j+1; /* Free up to j. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
        }

        /* Command lookup */
        cmd = lookupCommand(argv[0]->ptr);
        if (!cmd) {
            serverLog(LL_WARNING,
                "Unknown command '%s' reading the append only file",
                (char*)argv[0]->ptr);
            exit(1);
        }

        if (cmd == server.multiCommand) valid_before_multi = valid_up_to;

        /* Run the command in the context of a fake client */
        fakeClient->cmd = fakeClient->lastcmd = cmd;
        if (fakeClient->flags & CLIENT_MULTI &&
            fakeClient->cmd->proc != execCommand)
        {
            queueMultiCommand(fakeClient);
        } else {
            cmd->proc(fakeClient);
        }

        /* The fake client should not have a reply */
        serverAssert(fakeClient->bufpos == 0 &&
                     listLength(fakeClient->reply) == 0);

        /* The fake client should never get blocked */
        serverAssert((fakeClient->flags & CLIENT_BLOCKED) == 0);

        /* Clean up. Command code may have changed argv/argc so we use the
         * argv/argc of the client instead of the local variables. */
        freeFakeClientArgv(fakeClient);
        fakeClient->cmd = NULL;
        if (server.aof_load_truncated) valid_up_to = ftello(fp);
        if (server.key_load_delay)
            debugDelay(server.key_load_delay);
    }

    /* This point can only be reached when EOF is reached without errors.
     * If the client is in the middle of a MULTI/EXEC, handle it as it was
     * a short read, even if technically the protocol is correct: we want
     * to remove the unprocessed tail and continue. */
    if (fakeClient->flags & CLIENT_MULTI) {
        serverLog(LL_WARNING,
            "Revert incomplete MULTI/EXEC transaction in AOF file");
        valid_up_to = valid_before_multi;
        goto uxeof;
    }

loaded_ok: /* DB loaded, cleanup and return C_OK to the caller. */
    fclose(fp);
    freeFakeClient(fakeClient);
    server.aof_state = old_aof_state;
    stopLoading(1);
    aofUpdateCurrentSize();
    server.aof_rewrite_base_size = server.aof_current_size;
    server.aof_fsync_offset = server.aof_current_size;
    return C_OK;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
    if (!feof(fp)) {
        if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
        fclose(fp);
        serverLog(LL_WARNING,"Unrecoverable error reading the append only file: %s", strerror(errno));
        exit(1);
    }

uxeof: /* Unexpected AOF end of file. */
    if (server.aof_load_truncated) {
        serverLog(LL_WARNING,"!!! Warning: short read while loading the AOF file !!!");
        serverLog(LL_WARNING,"!!! Truncating the AOF at offset %llu !!!",
            (unsigned long long) valid_up_to);
        if (valid_up_to == -1 || truncate(filename,valid_up_to) == -1) {
            if (valid_up_to == -1) {
                serverLog(LL_WARNING,"Last valid command offset is invalid");
            } else {
                serverLog(LL_WARNING,"Error truncating the AOF file: %s",
                    strerror(errno));
            }
        } else {
            /* Make sure the AOF file descriptor points to the end of the
             * file after the truncate call. */
            if (server.aof_fd != -1 && lseek(server.aof_fd,0,SEEK_END) == -1) {
                serverLog(LL_WARNING,"Can't seek the end of the AOF file: %s",
                    strerror(errno));
            } else {
                serverLog(LL_WARNING,
                    "AOF loaded anyway because aof-load-truncated is enabled");
                goto loaded_ok;
            }
        }
    }
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    fclose(fp);
    serverLog(LL_WARNING,"Unexpected end of file reading the append only file. You can: 1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename>. 2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.");
    exit(1);

fmterr: /* Format error. */
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    fclose(fp);
    serverLog(LL_WARNING,"Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
    exit(1);
}

/* ----------------------------------------------------------------------------
 * AOF rewrite
 * ------------------------------------------------------------------------- */

/* Delegate writing an object to writing a bulk string or bulk long long.
 * This is not placed in rio.c since that adds the server.h dependency. */
// 代理写一个bulk对象，可以处理string和long long数据。
// 这个函数没有放在rio.c，是因为我们要使用robj结构，增加了server.h依赖。
int rioWriteBulkObject(rio *r, robj *obj) {
    /* Avoid using getDecodedObject to help copy-on-write (we are often
     * in a child process when this function is called). */
    // 避免使用getDecodedObject，因为该函数会创建新的对象，对copy-on-write不友好。
    // 这里针对不同的字符串编码，我们使用不同的rio函数来处理Bulk数据写入。
    if (obj->encoding == OBJ_ENCODING_INT) {
        return rioWriteBulkLongLong(r,(long)obj->ptr);
    } else if (sdsEncodedObject(obj)) {
        return rioWriteBulkString(r,obj->ptr,sdslen(obj->ptr));
    } else {
        serverPanic("Unknown string encoding");
    }
}

/* Emit the commands needed to rebuild a list object.
 * The function returns 0 on error, 1 on success. */
int rewriteListObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = listTypeLength(o);

    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklist *list = o->ptr;
        quicklistIter *li = quicklistGetIterator(list, AL_START_HEAD);
        quicklistEntry entry;

        while (quicklistNext(li,&entry)) {
            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                    AOF_REWRITE_ITEMS_PER_CMD : items;
                if (!rioWriteBulkCount(r,'*',2+cmd_items) ||
                    !rioWriteBulkString(r,"RPUSH",5) ||
                    !rioWriteBulkObject(r,key)) 
                {
                    quicklistReleaseIterator(li);
                    return 0;
                }
            }

            if (entry.value) {
                if (!rioWriteBulkString(r,(char*)entry.value,entry.sz)) {
                    quicklistReleaseIterator(li);
                    return 0;
                }
            } else {
                if (!rioWriteBulkLongLong(r,entry.longval)) {
                    quicklistReleaseIterator(li);
                    return 0;
                }
            }
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        quicklistReleaseIterator(li);
    } else {
        serverPanic("Unknown list encoding");
    }
    return 1;
}

/* Emit the commands needed to rebuild a set object.
 * The function returns 0 on error, 1 on success. */
int rewriteSetObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = setTypeSize(o);

    if (o->encoding == OBJ_ENCODING_INTSET) {
        int ii = 0;
        int64_t llval;

        while(intsetGet(o->ptr,ii++,&llval)) {
            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                    AOF_REWRITE_ITEMS_PER_CMD : items;

                if (!rioWriteBulkCount(r,'*',2+cmd_items) ||
                    !rioWriteBulkString(r,"SADD",4) ||
                    !rioWriteBulkObject(r,key)) 
                {
                    return 0;
                }
            }
            if (!rioWriteBulkLongLong(r,llval)) return 0;
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        dictIterator *di = dictGetIterator(o->ptr);
        dictEntry *de;

        while((de = dictNext(di)) != NULL) {
            sds ele = dictGetKey(de);
            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                    AOF_REWRITE_ITEMS_PER_CMD : items;

                if (!rioWriteBulkCount(r,'*',2+cmd_items) ||
                    !rioWriteBulkString(r,"SADD",4) ||
                    !rioWriteBulkObject(r,key)) 
                {
                    dictReleaseIterator(di);
                    return 0;
                }
            }
            if (!rioWriteBulkString(r,ele,sdslen(ele))) {
                dictReleaseIterator(di);
                return 0;          
            }
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        dictReleaseIterator(di);
    } else {
        serverPanic("Unknown set encoding");
    }
    return 1;
}

/* Emit the commands needed to rebuild a sorted set object.
 * The function returns 0 on error, 1 on success. */
int rewriteSortedSetObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = zsetLength(o);

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = o->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vll;
        double score;

        eptr = ziplistIndex(zl,0);
        serverAssert(eptr != NULL);
        sptr = ziplistNext(zl,eptr);
        serverAssert(sptr != NULL);

        while (eptr != NULL) {
            serverAssert(ziplistGet(eptr,&vstr,&vlen,&vll));
            score = zzlGetScore(sptr);

            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                    AOF_REWRITE_ITEMS_PER_CMD : items;

                if (!rioWriteBulkCount(r,'*',2+cmd_items*2) ||
                    !rioWriteBulkString(r,"ZADD",4) ||
                    !rioWriteBulkObject(r,key)) 
                {
                    return 0;
                }
            }
            if (!rioWriteBulkDouble(r,score)) return 0;
            if (vstr != NULL) {
                if (!rioWriteBulkString(r,(char*)vstr,vlen)) return 0;
            } else {
                if (!rioWriteBulkLongLong(r,vll)) return 0;
            }
            zzlNext(zl,&eptr,&sptr);
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = o->ptr;
        dictIterator *di = dictGetIterator(zs->dict);
        dictEntry *de;

        while((de = dictNext(di)) != NULL) {
            sds ele = dictGetKey(de);
            double *score = dictGetVal(de);

            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                    AOF_REWRITE_ITEMS_PER_CMD : items;

                if (!rioWriteBulkCount(r,'*',2+cmd_items*2) ||
                    !rioWriteBulkString(r,"ZADD",4) ||
                    !rioWriteBulkObject(r,key)) 
                {
                    dictReleaseIterator(di);
                    return 0;
                }
            }
            if (!rioWriteBulkDouble(r,*score) ||
                !rioWriteBulkString(r,ele,sdslen(ele)))
            {
                dictReleaseIterator(di);
                return 0;
            }
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        dictReleaseIterator(di);
    } else {
        serverPanic("Unknown sorted zset encoding");
    }
    return 1;
}

/* Write either the key or the value of the currently selected item of a hash.
 * The 'hi' argument passes a valid Redis hash iterator.
 * The 'what' filed specifies if to write a key or a value and can be
 * either OBJ_HASH_KEY or OBJ_HASH_VALUE.
 *
 * The function returns 0 on error, non-zero on success. */
static int rioWriteHashIteratorCursor(rio *r, hashTypeIterator *hi, int what) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr)
            return rioWriteBulkString(r, (char*)vstr, vlen);
        else
            return rioWriteBulkLongLong(r, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        sds value = hashTypeCurrentFromHashTable(hi, what);
        return rioWriteBulkString(r, value, sdslen(value));
    }

    serverPanic("Unknown hash encoding");
    return 0;
}

/* Emit the commands needed to rebuild a hash object.
 * The function returns 0 on error, 1 on success. */
int rewriteHashObject(rio *r, robj *key, robj *o) {
    hashTypeIterator *hi;
    long long count = 0, items = hashTypeLength(o);

    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != C_ERR) {
        if (count == 0) {
            int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                AOF_REWRITE_ITEMS_PER_CMD : items;

            if (!rioWriteBulkCount(r,'*',2+cmd_items*2) ||
                !rioWriteBulkString(r,"HMSET",5) ||
                !rioWriteBulkObject(r,key)) 
            {
                hashTypeReleaseIterator(hi);
                return 0;
            }
        }

        if (!rioWriteHashIteratorCursor(r, hi, OBJ_HASH_KEY) ||
            !rioWriteHashIteratorCursor(r, hi, OBJ_HASH_VALUE))
        {
            hashTypeReleaseIterator(hi);
            return 0;           
        }
        if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
        items--;
    }

    hashTypeReleaseIterator(hi);

    return 1;
}

/* Helper for rewriteStreamObject() that generates a bulk string into the
 * AOF representing the ID 'id'. */
int rioWriteBulkStreamID(rio *r,streamID *id) {
    int retval;

    sds replyid = sdscatfmt(sdsempty(),"%U-%U",id->ms,id->seq);
    retval = rioWriteBulkString(r,replyid,sdslen(replyid));
    sdsfree(replyid);
    return retval;
}

/* Helper for rewriteStreamObject(): emit the XCLAIM needed in order to
 * add the message described by 'nack' having the id 'rawid', into the pending
 * list of the specified consumer. All this in the context of the specified
 * key and group. */
int rioWriteStreamPendingEntry(rio *r, robj *key, const char *groupname, size_t groupname_len, streamConsumer *consumer, unsigned char *rawid, streamNACK *nack) {
     /* XCLAIM <key> <group> <consumer> 0 <id> TIME <milliseconds-unix-time>
               RETRYCOUNT <count> JUSTID FORCE. */
    streamID id;
    streamDecodeID(rawid,&id);
    if (rioWriteBulkCount(r,'*',12) == 0) return 0;
    if (rioWriteBulkString(r,"XCLAIM",6) == 0) return 0;
    if (rioWriteBulkObject(r,key) == 0) return 0;
    if (rioWriteBulkString(r,groupname,groupname_len) == 0) return 0;
    if (rioWriteBulkString(r,consumer->name,sdslen(consumer->name)) == 0) return 0;
    if (rioWriteBulkString(r,"0",1) == 0) return 0;
    if (rioWriteBulkStreamID(r,&id) == 0) return 0;
    if (rioWriteBulkString(r,"TIME",4) == 0) return 0;
    if (rioWriteBulkLongLong(r,nack->delivery_time) == 0) return 0;
    if (rioWriteBulkString(r,"RETRYCOUNT",10) == 0) return 0;
    if (rioWriteBulkLongLong(r,nack->delivery_count) == 0) return 0;
    if (rioWriteBulkString(r,"JUSTID",6) == 0) return 0;
    if (rioWriteBulkString(r,"FORCE",5) == 0) return 0;
    return 1;
}

/* Helper for rewriteStreamObject(): emit the XGROUP CREATECONSUMER is
 * needed in order to create consumers that do not have any pending entries.
 * All this in the context of the specified key and group. */
int rioWriteStreamEmptyConsumer(rio *r, robj *key, const char *groupname, size_t groupname_len, streamConsumer *consumer) {
    /* XGROUP CREATECONSUMER <key> <group> <consumer> */
    if (rioWriteBulkCount(r,'*',5) == 0) return 0;
    if (rioWriteBulkString(r,"XGROUP",6) == 0) return 0;
    if (rioWriteBulkString(r,"CREATECONSUMER",14) == 0) return 0;
    if (rioWriteBulkObject(r,key) == 0) return 0;
    if (rioWriteBulkString(r,groupname,groupname_len) == 0) return 0;
    if (rioWriteBulkString(r,consumer->name,sdslen(consumer->name)) == 0) return 0;
    return 1;
}

/* Emit the commands needed to rebuild a stream object.
 * The function returns 0 on error, 1 on success. */
int rewriteStreamObject(rio *r, robj *key, robj *o) {
    stream *s = o->ptr;
    streamIterator si;
    streamIteratorStart(&si,s,NULL,NULL,0);
    streamID id;
    int64_t numfields;

    if (s->length) {
        /* Reconstruct the stream data using XADD commands. */
        while(streamIteratorGetID(&si,&id,&numfields)) {
            /* Emit a two elements array for each item. The first is
             * the ID, the second is an array of field-value pairs. */

            /* Emit the XADD <key> <id> ...fields... command. */
            if (!rioWriteBulkCount(r,'*',3+numfields*2) || 
                !rioWriteBulkString(r,"XADD",4) ||
                !rioWriteBulkObject(r,key) ||
                !rioWriteBulkStreamID(r,&id)) 
            {
                streamIteratorStop(&si);
                return 0;
            }
            while(numfields--) {
                unsigned char *field, *value;
                int64_t field_len, value_len;
                streamIteratorGetField(&si,&field,&value,&field_len,&value_len);
                if (!rioWriteBulkString(r,(char*)field,field_len) ||
                    !rioWriteBulkString(r,(char*)value,value_len)) 
                {
                    streamIteratorStop(&si);
                    return 0;                  
                }
            }
        }
    } else {
        /* Use the XADD MAXLEN 0 trick to generate an empty stream if
         * the key we are serializing is an empty string, which is possible
         * for the Stream type. */
        id.ms = 0; id.seq = 1; 
        if (!rioWriteBulkCount(r,'*',7) ||
            !rioWriteBulkString(r,"XADD",4) ||
            !rioWriteBulkObject(r,key) ||
            !rioWriteBulkString(r,"MAXLEN",6) ||
            !rioWriteBulkString(r,"0",1) ||
            !rioWriteBulkStreamID(r,&id) ||
            !rioWriteBulkString(r,"x",1) ||
            !rioWriteBulkString(r,"y",1))
        {
            streamIteratorStop(&si);
            return 0;     
        }
    }

    /* Append XSETID after XADD, make sure lastid is correct,
     * in case of XDEL lastid. */
    if (!rioWriteBulkCount(r,'*',3) ||
        !rioWriteBulkString(r,"XSETID",6) ||
        !rioWriteBulkObject(r,key) ||
        !rioWriteBulkStreamID(r,&s->last_id)) 
    {
        streamIteratorStop(&si);
        return 0; 
    }


    /* Create all the stream consumer groups. */
    if (s->cgroups) {
        raxIterator ri;
        raxStart(&ri,s->cgroups);
        raxSeek(&ri,"^",NULL,0);
        while(raxNext(&ri)) {
            streamCG *group = ri.data;
            /* Emit the XGROUP CREATE in order to create the group. */
            if (!rioWriteBulkCount(r,'*',5) ||
                !rioWriteBulkString(r,"XGROUP",6) ||
                !rioWriteBulkString(r,"CREATE",6) ||
                !rioWriteBulkObject(r,key) ||
                !rioWriteBulkString(r,(char*)ri.key,ri.key_len) ||
                !rioWriteBulkStreamID(r,&group->last_id)) 
            {
                raxStop(&ri);
                streamIteratorStop(&si);
                return 0;
            }

            /* Generate XCLAIMs for each consumer that happens to
             * have pending entries. Empty consumers would be generated with
             * XGROUP CREATECONSUMER. */
            raxIterator ri_cons;
            raxStart(&ri_cons,group->consumers);
            raxSeek(&ri_cons,"^",NULL,0);
            while(raxNext(&ri_cons)) {
                streamConsumer *consumer = ri_cons.data;
                /* If there are no pending entries, just emit XGROUP CREATECONSUMER */
                if (raxSize(consumer->pel) == 0) {
                    if (rioWriteStreamEmptyConsumer(r,key,(char*)ri.key,
                                                    ri.key_len,consumer) == 0)
                    {
                        raxStop(&ri_cons);
                        raxStop(&ri);
                        streamIteratorStop(&si);
                        return 0;
                    }
                    continue;
                }
                /* For the current consumer, iterate all the PEL entries
                 * to emit the XCLAIM protocol. */
                raxIterator ri_pel;
                raxStart(&ri_pel,consumer->pel);
                raxSeek(&ri_pel,"^",NULL,0);
                while(raxNext(&ri_pel)) {
                    streamNACK *nack = ri_pel.data;
                    if (rioWriteStreamPendingEntry(r,key,(char*)ri.key,
                                                   ri.key_len,consumer,
                                                   ri_pel.key,nack) == 0)
                    {
                        raxStop(&ri_pel);
                        raxStop(&ri_cons);
                        raxStop(&ri);
                        streamIteratorStop(&si);
                        return 0;
                    }
                }
                raxStop(&ri_pel);
            }
            raxStop(&ri_cons);
        }
        raxStop(&ri);
    }

    streamIteratorStop(&si);
    return 1;
}

/* Call the module type callback in order to rewrite a data type
 * that is exported by a module and is not handled by Redis itself.
 * The function returns 0 on error, 1 on success. */
int rewriteModuleObject(rio *r, robj *key, robj *o) {
    RedisModuleIO io;
    moduleValue *mv = o->ptr;
    moduleType *mt = mv->type;
    moduleInitIOContext(io,mt,r,key);
    mt->aof_rewrite(&io,key,mv->value);
    if (io.ctx) {
        moduleFreeContext(io.ctx);
        zfree(io.ctx);
    }
    return io.error ? 0 : 1;
}

/* This function is called by the child rewriting the AOF file to read
 * the difference accumulated from the parent into a buffer, that is
 * concatenated at the end of the rewrite. */
// 子进程接收父进程通过管道传输过来的AOF diff增量数据，用于追加到重写的AOF文件中。
ssize_t aofReadDiffFromParent(void) {
    // 大部分linux系统默认的pipe缓存大小64k。
    char buf[65536]; /* Default pipe buffer size on most Linux systems. */
    ssize_t nread, total = 0;

    while ((nread =
            read(server.aof_pipe_read_data_from_parent,buf,sizeof(buf))) > 0) {
        // 数据从管道读出来追加到aof_child_diff中。
        server.aof_child_diff = sdscatlen(server.aof_child_diff,buf,nread);
        total += nread;
    }
    return total;
}

// AOF rewrite处理，遍历所有db，对于所有entry，构造multi bulk格式写入（RESP协议格式）。
int rewriteAppendOnlyFileRio(rio *aof) {
    dictIterator *di = NULL;
    dictEntry *de;
    size_t processed = 0;
    int j;
    long key_count = 0;
    long long updated_time = 0;

    // 挨个DB遍历处理
    for (j = 0; j < server.dbnum; j++) {
        // 先select db
        char selectcmd[] = "*2\r\n$6\r\nSELECT\r\n";
        redisDb *db = server.db+j;
        dict *d = db->dict;
        // 如果该db没有数据，跳过
        if (dictSize(d) == 0) continue;
        // 使用安全的迭代器
        di = dictGetSafeIterator(d);

        /* SELECT the new DB */
        // 写入select db命令。分两部分写入，先写multi bulk数量和select字符串，再写dbnum数字。
        if (rioWrite(aof,selectcmd,sizeof(selectcmd)-1) == 0) goto werr;
        if (rioWriteBulkLongLong(aof,j) == 0) goto werr;

        /* Iterate this DB writing every entry */
        // 挨个迭代db中的所有entry进行处理。
        while((de = dictNext(di)) != NULL) {
            sds keystr;
            robj key, *o;
            long long expiretime;

            keystr = dictGetKey(de);
            o = dictGetVal(de);
            // 初始化栈对象key
            initStaticStringObject(key,keystr);

            // 获取过期时间
            expiretime = getExpire(db,&key);

            /* Save the key and associated value */
            // 保存key以及关联的值到AOF
            if (o->type == OBJ_STRING) {
                /* Emit a SET command */
                // 字符串类型使用SET处理，multi bulk有3部分。SET key value
                char cmd[]="*3\r\n$3\r\nSET\r\n";
                if (rioWrite(aof,cmd,sizeof(cmd)-1) == 0) goto werr;
                /* Key and value */
                if (rioWriteBulkObject(aof,&key) == 0) goto werr;
                if (rioWriteBulkObject(aof,o) == 0) goto werr;
            } else if (o->type == OBJ_LIST) {
                // list类型处理，使用RPUSH命令写入。
                if (rewriteListObject(aof,&key,o) == 0) goto werr;
            } else if (o->type == OBJ_SET) {
                // set类型处理，使用SADD命令写入。
                if (rewriteSetObject(aof,&key,o) == 0) goto werr;
            } else if (o->type == OBJ_ZSET) {
                // zset类型，使用ZADD命令写入。
                if (rewriteSortedSetObject(aof,&key,o) == 0) goto werr;
            } else if (o->type == OBJ_HASH) {
                // hash类型，使用hMSET命令。
                if (rewriteHashObject(aof,&key,o) == 0) goto werr;
            } else if (o->type == OBJ_STREAM) {
                // stream类型，比较复杂，详细见下面函数实现。
                if (rewriteStreamObject(aof,&key,o) == 0) goto werr;
            } else if (o->type == OBJ_MODULE) {
                // module对象处理，每类module对象都有自己类型的aof_rewrite方法，直接调用处理。
                if (rewriteModuleObject(aof,&key,o) == 0) goto werr;
            } else {
                serverPanic("Unknown object type");
            }
            /* Save the expire time */
            // 设置key的过期时间，同一使用PEXPIREAT指令。
            if (expiretime != -1) {
                char cmd[]="*3\r\n$9\r\nPEXPIREAT\r\n";
                // PEXPIREAT key expiretime，命令分三部分写入。
                if (rioWrite(aof,cmd,sizeof(cmd)-1) == 0) goto werr;
                if (rioWriteBulkObject(aof,&key) == 0) goto werr;
                if (rioWriteBulkLongLong(aof,expiretime) == 0) goto werr;
            }
            /* Read some diff from the parent process from time to time. */
            // AOF重写每写入AOF_READ_DIFF_INTERVAL_BYTES=10kb的数据，处理下父进程写入的AOF diff增量数据。
            if (aof->processed_bytes > processed+AOF_READ_DIFF_INTERVAL_BYTES) {
                processed = aof->processed_bytes;
                // 读取AOF diff增量数据到子进程缓存
                aofReadDiffFromParent();
            }

            /* Update info every 1 second (approximately).
             * in order to avoid calling mstime() on each iteration, we will
             * check the diff every 1024 keys */
            // 发送当前子进程信息给父进程，大概每1s发送一次。
            // 这里为了避免频繁调用mstime()来获取时间，我们每处理1024个key检查下。
            if ((key_count++ & 1023) == 0) {
                long long now = mstime();
                if (now - updated_time >= 1000) {
                    // 发送子进程信息
                    sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, key_count, "AOF rewrite");
                    updated_time = now;
                }
            }
        }
        //当前db迭代完成，释放迭代器。
        dictReleaseIterator(di);
        di = NULL;
    }
    return C_OK;

werr:
    if (di) dictReleaseIterator(di);
    return C_ERR;
}

/* Write a sequence of commands able to fully rebuild the dataset into
 * "filename". Used both by REWRITEAOF and BGREWRITEAOF.
 *
 * In order to minimize the number of commands needed in the rewritten
 * log Redis uses variadic commands when possible, such as RPUSH, SADD
 * and ZADD. However at max AOF_REWRITE_ITEMS_PER_CMD items per time
 * are inserted using a single command. */
// 将一系列基于当前数据集重新构建的命令写入filename文件中。该函数是实现 REWRITEAOF 和 BGREWRITEAOF 的公共函数。
// 为了最大程度的减少重新文件的命令数量，redis会尽量使用可变参数命令，如RPUSH、SADD、ZADD等。
// 但是每次单个命令插入数据，最多只能写入AOF_REWRITE_ITEMS_PER_CMD个。
int rewriteAppendOnlyFile(char *filename) {
    rio aof;
    FILE *fp = NULL;
    char tmpfile[256];
    char byte;

    /* Note that we have to use a different temp name here compared to the
     * one used by rewriteAppendOnlyFileBackground() function. */
    // 注意这里我们必须使用与rewriteAppendOnlyFileBackground()函数中不同的临时文件名。后面写完后再重命名为filename。
    snprintf(tmpfile,256,"temp-rewriteaof-%d.aof", (int) getpid());
    // 打开临时文件
    fp = fopen(tmpfile,"w");
    if (!fp) {
        serverLog(LL_WARNING, "Opening the temp file for AOF rewrite in rewriteAppendOnlyFile(): %s", strerror(errno));
        return C_ERR;
    }

    // 初始化父进程传过来的aof diff数据，初始化文件rio
    server.aof_child_diff = sdsempty();
    rioInitWithFile(&aof,fp);

    // 渐进fsync到磁盘，这里我们设置每写入REDIS_AUTOSYNC_BYTES=32MB数据时，自动刷盘。
    if (server.aof_rewrite_incremental_fsync)
        rioSetAutoSync(&aof,REDIS_AUTOSYNC_BYTES);

    // 启动AOF重写，触发事件通知module
    startSaving(RDBFLAGS_AOF_PREAMBLE);

    if (server.aof_use_rdb_preamble) {
        // 如果使用rdb前文，则这里会先将内存数据以RDB形式写入文件。后面增量的数据再以AOF形式追加。
        int error;
        if (rdbSaveRio(&aof,&error,RDBFLAGS_AOF_PREAMBLE,NULL) == C_ERR) {
            errno = error;
            goto werr;
        }
    } else {
        // 不使用RDB前文数据混合持久化。所有数据都是以AOF格式写入。
        if (rewriteAppendOnlyFileRio(&aof) == C_ERR) goto werr;
    }

    /* Do an initial slow fsync here while the parent is still sending
     * data, in order to make the next final fsync faster. */
    // 因为父进程还在传输增量的AOF diff数据，后面我们需要处理追加到文件中。所以这里先执行一次数据刷盘操作。
    // 这样后面数据写入后刷盘就比较快了，从而避免太多AOF diff数据堆积在master的aof_rewrite_buf中。
    if (fflush(fp) == EOF) goto werr;
    if (fsync(fileno(fp)) == -1) goto werr;

    /* Read again a few times to get more data from the parent.
     * We can't read forever (the server may receive data from clients
     * faster than it is able to send data to the child), so we try to read
     * some more data in a loop as soon as there is a good chance more data
     * will come. If it looks like we are wasting time, we abort (this
     * happens after 20 ms without new data). */
    // 重复的多读几次，以确保父进程测停止了往管道写diff数据。这里处理最多也只给了1s时间，没有处理完的，后面还会再处理。
    // 接收端收数据要比发送放发数据快的，但是我们又不可能永远等待读下去，所以我们尝试循环读取多次，尽可能的等待数据到来。
    // 如果持续大约20ms（20次循环，一次循环中等待1ms）都没有收到数据，可能父进程发送的数据我们接收完了，所以这里我们会停止处理。
    int nodata = 0;
    mstime_t start = mstime();
    while(mstime()-start < 1000 && nodata < 20) {
        if (aeWait(server.aof_pipe_read_data_from_parent, AE_READABLE, 1) <= 0)
        {
            nodata++;
            continue;
        }
        // 可读事件触发，说明有数据，我们需要重复计数。
        nodata = 0; /* Start counting from zero, we stop on N *contiguous*
                       timeouts. */
        // 从管道读数据到子进程的AOF diff缓存
        aofReadDiffFromParent();
    }

    /* Ask the master to stop sending diffs. */
    // 通知主进程停止向子进程传输AOF diff增量数据。
    if (write(server.aof_pipe_write_ack_to_parent,"!",1) != 1) goto werr;
    if (anetNonBlock(NULL,server.aof_pipe_read_ack_from_parent) != ANET_OK)
        goto werr;
    /* We read the ACK from the server using a 5 seconds timeout. Normally
     * it should reply ASAP, but just in case we lose its reply, we are sure
     * the child will eventually get terminated. */
    // 我们使用5s超时时间去同步读取主进程的ACK回复。正常情况主进程应该很快回复的，但如果异常情况没有收到回复，我们也应该确保子进程最终能结束。
    if (syncRead(server.aof_pipe_read_ack_from_parent,&byte,1,5000) != 1 ||
        byte != '!') goto werr;
    serverLog(LL_NOTICE,"Parent agreed to stop sending diffs. Finalizing AOF...");

    /* Read the final diff if any. */
    // 再从管道中读取最后发送过来的数据
    aofReadDiffFromParent();

    /* Write the received diff to the file. */
    serverLog(LL_NOTICE,
        "Concatenating %.2f MB of AOF diff received from parent.",
        (double) sdslen(server.aof_child_diff) / (1024*1024));

    /* Now we write the entire AOF buffer we received from the parent
     * via the pipe during the life of this fork child.
     * once a second, we'll take a break and send updated COW info to the parent */
    // 写入从父进程接收的整个AOF buffer数据到AOF文件中。每1s钟我们会发送当前子进程的信息给父进程。
    size_t bytes_to_write = sdslen(server.aof_child_diff);
    const char *buf = server.aof_child_diff;
    // 统计数据，用于发送给父进程使用
    long long cow_updated_time = mstime();
    long long key_count = dbTotalServerKeyCount();
    while (bytes_to_write) {
        /* We write the AOF buffer in chunk of 8MB so that we can check the time in between them */
        // 循环写入数据。我们每次写8MB的数据，这样我们就不需要频繁的检查处理时间。因为我们每1s中会向父进程发送信息。
        size_t chunk_size = bytes_to_write < (8<<20) ? bytes_to_write : (8<<20);

        // 写入AOF文件。
        if (rioWrite(&aof,buf,chunk_size) == 0)
            goto werr;

        // 更新剩余待写的数据量，以及下一次处理数据位置
        bytes_to_write -= chunk_size;
        buf += chunk_size;

        /* Update COW info */
        long long now = mstime();
        // 检查是否执行超过1s，从而向父进程发送信息。
        if (now - cow_updated_time >= 1000) {
            sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, key_count, "AOF rewrite");
            cow_updated_time = now;
        }
    }

    /* Make sure data will not remain on the OS's output buffers */
    // 数据写到文件后刷盘处理，写完关闭文件。
    if (fflush(fp)) goto werr;
    if (fsync(fileno(fp))) goto werr;
    if (fclose(fp)) { fp = NULL; goto werr; }
    fp = NULL;

    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. */
    // 仅当生产的DB文件正常时，我们使用rename确保DB文件修改的原子性。
    if (rename(tmpfile,filename) == -1) {
        serverLog(LL_WARNING,"Error moving temp append only file on the final destination: %s", strerror(errno));
        unlink(tmpfile);
        stopSaving(0);
        return C_ERR;
    }
    serverLog(LL_NOTICE,"SYNC append only file rewrite performed");
    stopSaving(1);
    return C_OK;

werr:
    serverLog(LL_WARNING,"Write error writing append only file on disk: %s", strerror(errno));
    if (fp) fclose(fp);
    unlink(tmpfile);
    stopSaving(0);
    return C_ERR;
}

/* ----------------------------------------------------------------------------
 * AOF rewrite pipes for IPC
 * -------------------------------------------------------------------------- */

/* This event handler is called when the AOF rewriting child sends us a
 * single '!' char to signal we should stop sending buffer diffs. The
 * parent sends a '!' as well to acknowledge. */
// 当AOF重写子进程向我们消息时，会触发该handler执行。
// 我们读取到单个'!'字符表示应该停止发送AOF rewrite增加buf给子进程，同时我们也发送'!'给子进程表示收到了消息。
void aofChildPipeReadable(aeEventLoop *el, int fd, void *privdata, int mask) {
    char byte;
    UNUSED(el);
    UNUSED(privdata);
    UNUSED(mask);

    if (read(fd,&byte,1) == 1 && byte == '!') {
        // 读取1字节数据，如果是'!'，我们停止发送增量AOF diff数据
        serverLog(LL_NOTICE,"AOF rewrite child asks to stop sending diffs.");
        server.aof_stop_sending_diff = 1;
        // 给子进程发送'!'字符过去进行确认
        if (write(server.aof_pipe_write_ack_to_child,"!",1) != 1) {
            /* If we can't send the ack, inform the user, but don't try again
             * since in the other side the children will use a timeout if the
             * kernel can't buffer our write, or, the children was
             * terminated. */
            // 如果无法发送ack，通知用户，不要再次尝试。因为另一侧子进程会超时或被终止。
            serverLog(LL_WARNING,"Can't send ACK to AOF child: %s",
                strerror(errno));
        }
    }
    /* Remove the handler since this can be called only one time during a
     * rewrite. */
    // 一次AOF重写，这个事件只会触发一次，所以这里我们移除这个事件。
    aeDeleteFileEvent(server.el,server.aof_pipe_read_ack_from_child,AE_READABLE);
}

/* Create the pipes used for parent - child process IPC during rewrite.
 * We have a data pipe used to send AOF incremental diffs to the child,
 * and two other pipes used by the children to signal it finished with
 * the rewrite so no more data should be written, and another for the
 * parent to acknowledge it understood this new condition. */
// 创建管道用于父子进程在AOF重写期间进行通信。
// 我们有一个数据管道用于发送AOF增量的变化给子进程处理。
// 另外两个管道，一个用于子进程完成AOF重写后通知父进程，另一个用于父进程收到结束消息后ACK通知子进程。
int aofCreatePipes(void) {
    int fds[6] = {-1, -1, -1, -1, -1, -1};
    int j;

    if (pipe(fds) == -1) goto error; /* parent -> children data. */
    if (pipe(fds+2) == -1) goto error; /* children -> parent ack. */
    if (pipe(fds+4) == -1) goto error; /* parent -> children ack. */
    /* Parent -> children data is non blocking. */
    // 数据管道通信是非阻塞的。
    if (anetNonBlock(NULL,fds[0]) != ANET_OK) goto error;
    if (anetNonBlock(NULL,fds[1]) != ANET_OK) goto error;
    // fds[2]是读端，父进程监控可读，用于接收子进程AOF重写结束的ack。
    if (aeCreateFileEvent(server.el, fds[2], AE_READABLE, aofChildPipeReadable, NULL) == AE_ERR) goto error;

    server.aof_pipe_write_data_to_child = fds[1];
    server.aof_pipe_read_data_from_parent = fds[0];
    server.aof_pipe_write_ack_to_parent = fds[3];
    server.aof_pipe_read_ack_from_child = fds[2];
    server.aof_pipe_write_ack_to_child = fds[5];
    server.aof_pipe_read_ack_from_parent = fds[4];
    server.aof_stop_sending_diff = 0;
    return C_OK;

error:
    serverLog(LL_WARNING,"Error opening /setting AOF rewrite IPC pipes: %s",
        strerror(errno));
    for (j = 0; j < 6; j++) if(fds[j] != -1) close(fds[j]);
    return C_ERR;
}

void aofClosePipes(void) {
    // 移除两个事件监听，关闭管道
    aeDeleteFileEvent(server.el,server.aof_pipe_read_ack_from_child,AE_READABLE);
    aeDeleteFileEvent(server.el,server.aof_pipe_write_data_to_child,AE_WRITABLE);
    close(server.aof_pipe_write_data_to_child);
    close(server.aof_pipe_read_data_from_parent);
    close(server.aof_pipe_write_ack_to_parent);
    close(server.aof_pipe_read_ack_from_child);
    close(server.aof_pipe_write_ack_to_child);
    close(server.aof_pipe_read_ack_from_parent);
}

/* ----------------------------------------------------------------------------
 * AOF background rewrite
 * ------------------------------------------------------------------------- */

/* This is how rewriting of the append only file in background works:
 *
 * 1) The user calls BGREWRITEAOF
 * 2) Redis calls this function, that forks():
 *    2a) the child rewrite the append only file in a temp file.
 *    2b) the parent accumulates differences in server.aof_rewrite_buf.
 * 3) When the child finished '2a' exists.
 * 4) The parent will trap the exit code, if it's OK, will append the
 *    data accumulated into server.aof_rewrite_buf into the temp file, and
 *    finally will rename(2) the temp file in the actual file name.
 *    The the new file is reopened as the new append only file. Profit!
 */
// AOF后台重写处理流程：
// 1、用户执行 BGREWRITEAOF 命令
// 2、Redis服务调用这个函数，进行forks()。子进程重写AOF到一个临时文件，父进程执行操作数据的命令会写入server.aof_rewrite_buf中。
// 3、子进程完成AOF重写后退出。
// 4、父进程捕获到子进程完成，会将server.aof_rewrite_buf中数据追加到AOF tmp文件中，然后对临时文件重命令，重新打开作为当前AOF使用。
int rewriteAppendOnlyFileBackground(void) {
    pid_t childpid;

    // 当前有正在执行的子进程，则返回err
    if (hasActiveChildProcess()) return C_ERR;
    // AOF创建父子进程通信的管道。
    if (aofCreatePipes() != C_OK) return C_ERR;
    // fork AOF重写子进程
    if ((childpid = redisFork(CHILD_TYPE_AOF)) == 0) {
        char tmpfile[256];

        /* Child */
        // 子进程设置进程属性
        redisSetProcTitle("redis-aof-rewrite");
        redisSetCpuAffinity(server.aof_rewrite_cpulist);
        snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof", (int) getpid());
        // 子进程处理AOF重写
        if (rewriteAppendOnlyFile(tmpfile) == C_OK) {
            sendChildCowInfo(CHILD_INFO_TYPE_AOF_COW_SIZE, "AOF rewrite");
            exitFromChild(0);
        } else {
            exitFromChild(1);
        }
    } else {
        /* Parent */
        // 父进程，如果fork失败返回err
        if (childpid == -1) {
            serverLog(LL_WARNING,
                "Can't rewrite append only file in background: fork: %s",
                strerror(errno));
            aofClosePipes();
            return C_ERR;
        }
        serverLog(LL_NOTICE,
            "Background append only file rewriting started by pid %ld",(long) childpid);
        server.aof_rewrite_scheduled = 0;
        server.aof_rewrite_time_start = time(NULL);

        /* We set appendseldb to -1 in order to force the next call to the
         * feedAppendOnlyFile() to issue a SELECT command, so the differences
         * accumulated by the parent into server.aof_rewrite_buf will start
         * with a SELECT statement and it will be safe to merge. */
        // 这里将appendseldb设置为-1，强制下一次feedAppendOnlyFile()调用时，在写入rewrite增量AOF前先执行select命令。
        // 这样父进程在将server.aof_rewrite_buf数据追加到重写的AOF文件时，开始必定是select命令，合并的话就是安全的。
        server.aof_selected_db = -1;
        // AOF重写，需要清空脚本缓存
        replicationScriptCacheFlush();
        return C_OK;
    }
    return C_OK; /* unreached */
}

// bgrewriteaof命令处理函数
void bgrewriteaofCommand(client *c) {
    if (server.child_type == CHILD_TYPE_AOF) {
        // 已有一个AOF rewrite进程在处理
        addReplyError(c,"Background append only file rewriting already in progress");
    } else if (hasActiveChildProcess()) {
        // 当前有后台子进程，加入调度。
        server.aof_rewrite_scheduled = 1;
        addReplyStatus(c,"Background append only file rewriting scheduled");
    } else if (rewriteAppendOnlyFileBackground() == C_OK) {
        // 开启子进程处理AOF rewrite
        addReplyStatus(c,"Background append only file rewriting started");
    } else {
        addReplyError(c,"Can't execute an AOF background rewriting. "
                        "Please check the server logs for more information.");
    }
}

// 移除AOF rewrite的临时文件
void aofRemoveTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof", (int) childpid);
    bg_unlink(tmpfile);

    snprintf(tmpfile,256,"temp-rewriteaof-%d.aof", (int) childpid);
    bg_unlink(tmpfile);
}

/* Update the server.aof_current_size field explicitly using stat(2)
 * to check the size of the file. This is useful after a rewrite or after
 * a restart, normally the size is updated just adding the write length
 * to the current length, that is much faster. */
// 使用fstat显示更新新的AOF文件的大小server.aof_current_size。
// 一般在AOF重写，或者服务重启后准确写入这个数据，后面当添加数据到AOF文件时，只需要对该属性增加相应写入数据大小就可以了。
void aofUpdateCurrentSize(void) {
    struct redis_stat sb;
    mstime_t latency;

    latencyStartMonitor(latency);
    // 通过调用fstat获取文件的信息，然后更新aof_current_size文件大小。
    if (redis_fstat(server.aof_fd,&sb) == -1) {
        serverLog(LL_WARNING,"Unable to obtain the AOF file length. stat: %s",
            strerror(errno));
    } else {
        server.aof_current_size = sb.st_size;
    }
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("aof-fstat",latency);
}

/* A background append only file rewriting (BGREWRITEAOF) terminated its work.
 * Handle this. */
// 后台AOF重新完成后，调用这个handler进行处理
void backgroundRewriteDoneHandler(int exitcode, int bysignal) {
    if (!bysignal && exitcode == 0) {
        // 正常结束
        int newfd, oldfd;
        char tmpfile[256];
        long long now = ustime();
        mstime_t latency;

        serverLog(LL_NOTICE,
            "Background AOF rewrite terminated with success");

        /* Flush the differences accumulated by the parent to the
         * rewritten AOF. */
        // 需要将父进程中AOF diff增量数据，追加到重写的AOF文件末尾。
        latencyStartMonitor(latency);
        snprintf(tmpfile,256,"temp-rewriteaof-bg-%d.aof",
            (int)server.child_pid);
        // 打开子进程AOF重写生成的文件
        newfd = open(tmpfile,O_WRONLY|O_APPEND);
        if (newfd == -1) {
            serverLog(LL_WARNING,
                "Unable to open the temporary AOF produced by the child: %s", strerror(errno));
            goto cleanup;
        }

        // 想该文件追加AOF diff增量数据
        if (aofRewriteBufferWrite(newfd) == -1) {
            serverLog(LL_WARNING,
                "Error trying to flush the parent diff to the rewritten AOF: %s", strerror(errno));
            close(newfd);
            goto cleanup;
        }
        // 监控延时统计
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-rewrite-diff-write",latency);
  
        if (server.aof_fsync == AOF_FSYNC_EVERYSEC) {
            // aof每秒刷盘策略，加入异步任务队列
            aof_background_fsync(newfd);
        } else if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
            latencyStartMonitor(latency);
            // AOF每执行指令刷盘，则直接处理刷盘。
            if (redis_fsync(newfd) == -1) {
                serverLog(LL_WARNING,
                    "Error trying to fsync the parent diff to the rewritten AOF: %s", strerror(errno));
                close(newfd);
                goto cleanup;
            }
            latencyEndMonitor(latency);
            latencyAddSampleIfNeeded("aof-rewrite-done-fsync",latency);
        }

        // 因为我们redis都是单线程处理的。这里处理aof rewrite buf写入文件，不可能存在边写文件边有新数据加入buf的情况。
        // 所以写完处理好文件替换，我们就结束了aof重写的处理。
        serverLog(LL_NOTICE,
            "Residual parent diff successfully flushed to the rewritten AOF (%.2f MB)", (double) aofRewriteBufferSize() / (1024*1024));

        /* The only remaining thing to do is to rename the temporary file to
         * the configured file and switch the file descriptor used to do AOF
         * writes. We don't want close(2) or rename(2) calls to block the
         * server on old file deletion.
         *
         * There are two possible scenarios:
         *
         * 1) AOF is DISABLED and this was a one time rewrite. The temporary
         * file will be renamed to the configured file. When this file already
         * exists, it will be unlinked, which may block the server.
         *
         * 2) AOF is ENABLED and the rewritten AOF will immediately start
         * receiving writes. After the temporary file is renamed to the
         * configured file, the original AOF file descriptor will be closed.
         * Since this will be the last reference to that file, closing it
         * causes the underlying file to be unlinked, which may block the
         * server.
         *
         * To mitigate the blocking effect of the unlink operation (either
         * caused by rename(2) in scenario 1, or by close(2) in scenario 2), we
         * use a background thread to take care of this. First, we
         * make scenario 1 identical to scenario 2 by opening the target file
         * when it exists. The unlink operation after the rename(2) will then
         * be executed upon calling close(2) for its descriptor. Everything to
         * guarantee atomicity for this switch has already happened by then, so
         * we don't care what the outcome or duration of that close operation
         * is, as long as the file descriptor is released again. */
        // 现在还剩下需要处理的就是重命名临时文件，然后切换AOF写入的文件描述符。我们不想在操作时阻塞服务，所以删除是异步处理的。
        // 目前有两种可能的情况：
        //  1、AOF禁用了，本次是AOF重写。临时文件重命名为原配置文件名，老的文件将被删除（可能阻塞服务）。
        //  2、AOF已启用，重写后的AOF将立即开始接收写入。临时文件重命名后，老的AOF文件描述符将被close。
        //      因为这可能是该fd最后一个引用，close将导致底层执行unlink操作，从而进行文件删除，阻塞服务。
        // 为了尽可能减少这里的阻塞，我们使用后台线程来处理。
        if (server.aof_fd == -1) {
            /* AOF disabled */

            /* Don't care if this fails: oldfd will be -1 and we handle that.
             * One notable case of -1 return is if the old file does
             * not exist. */
            // AOF禁用，我们这里打开老的AOF文件。
            // 不用管这里错误，返回-1我们就认为老文件不存在，正常打开会返回老文件的fd
            oldfd = open(server.aof_filename,O_RDONLY|O_NONBLOCK);
        } else {
            /* AOF enabled */
            // AOF是启用的，这里我们设置oldfd为-1。启用状态，老的AOF文件是打开，所以这里不需要像前面那样还专门open。
            oldfd = -1; /* We'll set this to the current AOF filedes later. */
        }

        /* Rename the temporary file. This will not unlink the target file if
         * it exists, because we reference it with "oldfd". */
        // 重命名临时文件。老的AOF文件如果存在都是打开状态，这里rename不会直接删除老的文件，直接移除link。
        latencyStartMonitor(latency);
        if (rename(tmpfile,server.aof_filename) == -1) {
            serverLog(LL_WARNING,
                "Error trying to rename the temporary AOF file %s into %s: %s",
                tmpfile,
                server.aof_filename,
                strerror(errno));
            close(newfd);
            if (oldfd != -1) close(oldfd);
            goto cleanup;
        }
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-rename",latency);

        if (server.aof_fd == -1) {
            /* AOF disabled, we don't need to set the AOF file descriptor
             * to this new file, so we can close it. */
            // 如果AOF是禁用的，我们这里也不需要新文件来处理AOF，所以直接close新文件。
            close(newfd);
        } else {
            /* AOF enabled, replace the old fd with the new one. */
            // AOF是启用的，我们需要使用新的AOF fd来作为服务AOF数据写入。同时要保存就的fd，后面进行异步close删除。
            oldfd = server.aof_fd;
            server.aof_fd = newfd;
            server.aof_selected_db = -1; /* Make sure SELECT is re-issued */
            aofUpdateCurrentSize();
            server.aof_rewrite_base_size = server.aof_current_size;
            server.aof_fsync_offset = server.aof_current_size;
            server.aof_last_fsync = server.unixtime;

            /* Clear regular AOF buffer since its contents was just written to
             * the new AOF from the background rewrite buffer. */
            // 换了文件后，我们需要清空之前的AOF buffer里的内容。
            sdsfree(server.aof_buf);
            server.aof_buf = sdsempty();
        }

        server.aof_lastbgrewrite_status = C_OK;

        serverLog(LL_NOTICE, "Background AOF rewrite finished successfully");
        /* Change state from WAIT_REWRITE to ON if needed */
        // 更新AOF状态
        if (server.aof_state == AOF_WAIT_REWRITE)
            server.aof_state = AOF_ON;

        /* Asynchronously close the overwritten AOF. */
        // 如果oldfd!=-1表示存在老的AOF文件，我们这里创建异步close任务，异步进行删除。
        if (oldfd != -1) bioCreateCloseJob(oldfd);

        serverLog(LL_VERBOSE,
            "Background AOF rewrite signal handler took %lldus", ustime()-now);
    } else if (!bysignal && exitcode != 0) {
        // AOF rewrite异常结束。
        server.aof_lastbgrewrite_status = C_ERR;

        serverLog(LL_WARNING,
            "Background AOF rewrite terminated with error");
    } else {
        /* SIGUSR1 is whitelisted, so we have a way to kill a child without
         * triggering an error condition. */
        // 子进程是被kill掉的
        if (bysignal != SIGUSR1)
            server.aof_lastbgrewrite_status = C_ERR;

        serverLog(LL_WARNING,
            "Background AOF rewrite terminated by signal %d", bysignal);
    }

cleanup:
    aofClosePipes();
    aofRewriteBufferReset();
    aofRemoveTempFile(server.child_pid);
    server.aof_rewrite_time_last = time(NULL)-server.aof_rewrite_time_start;
    server.aof_rewrite_time_start = -1;
    /* Schedule a new rewrite if we are waiting for it to switch the AOF ON. */
    // aof重写失败了，这里重新将AOF重写加入任务调度。
    if (server.aof_state == AOF_WAIT_REWRITE)
        server.aof_rewrite_scheduled = 1;
}
