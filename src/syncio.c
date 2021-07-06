/* Synchronous socket and file I/O operations useful across the core.
 *
 * Copyright (c) 2009-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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

/* ----------------- Blocking sockets I/O with timeouts --------------------- */

/* Redis performs most of the I/O in a nonblocking way, with the exception
 * of the SYNC command where the slave does it in a blocking way, and
 * the MIGRATE command that must be blocking in order to be atomic from the
 * point of view of the two instances (one migrating the key and one receiving
 * the key). This is why need the following blocking I/O functions.
 *
 * All the functions take the timeout in milliseconds. */

#define SYNCIO__RESOLUTION 10 /* Resolution in milliseconds */

/* Write the specified payload to 'fd'. If writing the whole payload will be
 * done within 'timeout' milliseconds the operation succeeds and 'size' is
 * returned. Otherwise the operation fails, -1 is returned, and an unspecified
 * partial write could be performed against the file descriptor. */
// 将指定ptr的数据写入fd，不超时且完全写完则返回size；否则就失败返回-1，可能会有部分数据写入fd，具体数量未知。
ssize_t syncWrite(int fd, char *ptr, ssize_t size, long long timeout) {
    ssize_t nwritten, ret = size;
    long long start = mstime();
    long long remaining = timeout;

    while(1) {
        long long wait = (remaining > SYNCIO__RESOLUTION) ?
                          remaining : SYNCIO__RESOLUTION;
        long long elapsed;

        /* Optimistically try to write before checking if the file descriptor
         * is actually writable. At worst we get EAGAIN. */
        // 乐观的直接写，不check是否可写，最坏情况get EAGAIN err
        nwritten = write(fd,ptr,size);
        if (nwritten == -1) {
            if (errno != EAGAIN) return -1;
        } else {
            // 写了nwritten字节，这里ptr指针执行下一轮写的位置，size需要减去nwritten数。
            ptr += nwritten;
            size -= nwritten;
        }
        // 全部写完了，返回总的写入数
        if (size == 0) return ret;

        /* Wait */
        // 一次write结束，可能缓冲写满，fd暂时不可写，需要等待fd可写，同步等待。
        // 等待事件是前面计算的wait，即距离超时还剩的时间。
        aeWait(fd,AE_WRITABLE,wait);
        // fd可写了，等待结束，计算是否超时。超时则直接返回err，没超时则计算剩余时间。
        elapsed = mstime() - start;
        if (elapsed >= timeout) {
            errno = ETIMEDOUT;
            return -1;
        }
        remaining = timeout - elapsed;
    }
}

/* Read the specified amount of bytes from 'fd'. If all the bytes are read
 * within 'timeout' milliseconds the operation succeed and 'size' is returned.
 * Otherwise the operation fails, -1 is returned, and an unspecified amount of
 * data could be read from the file descriptor. */
// 同步读数据。失败时，可能有未知数量的数据被读取出来。
// 从fd中读size数据到ptr中。
ssize_t syncRead(int fd, char *ptr, ssize_t size, long long timeout) {
    ssize_t nread, totread = 0;
    long long start = mstime();
    long long remaining = timeout;

    if (size == 0) return 0;
    while(1) {
        long long wait = (remaining > SYNCIO__RESOLUTION) ?
                          remaining : SYNCIO__RESOLUTION;
        long long elapsed;

        /* Optimistically try to read before checking if the file descriptor
         * is actually readable. At worst we get EAGAIN. */
        nread = read(fd,ptr,size);
        if (nread == 0) return -1; /* short read. */
        if (nread == -1) {
            if (errno != EAGAIN) return -1;
        } else {
            ptr += nread;
            size -= nread;
            totread += nread;
        }
        if (size == 0) return totread;

        /* Wait */
        aeWait(fd,AE_READABLE,wait);
        elapsed = mstime() - start;
        if (elapsed >= timeout) {
            errno = ETIMEDOUT;
            return -1;
        }
        remaining = timeout - elapsed;
    }
}

/* Read a line making sure that every char will not require more than 'timeout'
 * milliseconds to be read.
 *
 * On success the number of bytes read is returned, otherwise -1.
 * On success the string is always correctly terminated with a 0 byte. */
// 读取一行。每个字节读取时间不超过timeout。
// 两个限制：行限制，字符串读取最大长度限制。要么读一行，要么读size后返回。最终读取的数据作为一个字符串。
// 成功时返回读取的字节数，且读的字符串总是以'\0'结束。
ssize_t syncReadLine(int fd, char *ptr, ssize_t size, long long timeout) {
    ssize_t nread = 0;

    size--;
    while(size) {
        char c;

        // 每次读1字节
        if (syncRead(fd,&c,1,timeout) == -1) return -1;
        if (c == '\n') {
            // 如果遇到'\n'，添加'\0'作为字符串结束
            *ptr = '\0';
            // 处理'\r\n'结束符
            if (nread && *(ptr-1) == '\r') *(ptr-1) = '\0';
            return nread;
        } else {
            // 没遇到换行符，该字符加到ptr后面，更新读取数量。
            *ptr++ = c;
            *ptr = '\0';
            nread++;
        }
        size--;
    }
    return nread;
}
