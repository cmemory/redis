/*
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2009-2019, Salvatore Sanfilippo <antirez at gmail dot com>
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


#ifndef __REDIS_RIO_H
#define __REDIS_RIO_H

#include <stdio.h>
#include <stdint.h>
#include "sds.h"
#include "connection.h"

#define RIO_FLAG_READ_ERROR (1<<0)
#define RIO_FLAG_WRITE_ERROR (1<<1)

struct _rio {
    /* Backend functions.
     * Since this functions do not tolerate short writes or reads the return
     * value is simplified to: zero on error, non zero on complete success. */
    // 后台接口。由于这些函数不允许部分数据的读写，因此返回值简化为：错误为零，成功为非零。
    size_t (*read)(struct _rio *, void *buf, size_t len);
    size_t (*write)(struct _rio *, const void *buf, size_t len);
    off_t (*tell)(struct _rio *);
    int (*flush)(struct _rio *);
    /* The update_cksum method if not NULL is used to compute the checksum of
     * all the data that was read or written so far. The method should be
     * designed so that can be called with the current checksum, and the buf
     * and len fields pointing to the new block of data to add to the checksum
     * computation. */
    // update_cksum方法如果不为NULL，用于计算目前读或者写的所有数据的checksum。
    // 这个方法应该设计成能够使用当前checksum来调用（传入的rio中保存着？），buf和len标识新的需要加入进行checksum计算的数据。
    void (*update_cksum)(struct _rio *, const void *buf, size_t len);

    /* The current checksum and flags (see RIO_FLAG_*) */
    // 当前的checksum 和 flags
    uint64_t cksum, flags;

    /* number of bytes read or written */
    // 写入或读取的字节数
    size_t processed_bytes;

    /* maximum single read or write chunk size */
    // 最大单次读或写的chunk大小
    size_t max_processing_chunk;

    /* Backend-specific vars. */
    // 后台指定的变量，union结构，指定读写目标（buf，文件指针，conn，fd）
    union {
        /* In-memory buffer target. */
        // 内存buffer
        struct {
            sds ptr;
            off_t pos;
        } buffer;
        /* Stdio file pointer target. */
        // Stdio 文件指针
        struct {
            FILE *fp;
            // 当前待刷盘的数据量
            off_t buffered; /* Bytes written since last fsync. */
            // 设置自动刷盘，如果当前写入后buffered>=autosync，则执行自动刷盘操作。
            off_t autosync; /* fsync after 'autosync' bytes written. */
        } file;
        /* Connection object (used to read from socket) */
        // Connection对象，用于socket传数据
        struct {
            connection *conn;   /* Connection */
            off_t pos;    /* pos in buf that was returned */
            sds buf;      /* buffered data */
            // 限制从conn写/读的数据量
            size_t read_limit;  /* don't allow to buffer/read more than that */
            // 当前已经从rio读出的数据，如果加上buf数据以及当前需要读的数据后，超过了read_limit，则报错
            size_t read_so_far; /* amount of data read from the rio (not buffered) */
        } conn;
        /* FD target (used to write to pipe). */
        // FD，主要用于pipe传数据
        struct {
            int fd;       /* File descriptor. */
            off_t pos;
            sds buf;
        } fd;
    } io;
};

typedef struct _rio rio;

/* The following functions are our interface with the stream. They'll call the
 * actual implementation of read / write / tell, and will update the checksum
 * if needed. */
// 下面的函数都是我们对外调用的处理stream数据的方法。他们都会调用真正实现的read/write/tell方法，如果需要的话还会更新checksum。

// 将buf数据写到rio目标中。写入len自己数据。
static inline size_t rioWrite(rio *r, const void *buf, size_t len) {
    // 如果rio目前有写err，则写失败返回0。
    if (r->flags & RIO_FLAG_WRITE_ERROR) return 0;
    // 如果buf数据没有写完就一直循环处理写入
    while (len) {
        // 计算一次读/写的最大字节数
        size_t bytes_to_write = (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len;
        // 如果有update_cksum，这里使用本次待写数据更新checksum。
        if (r->update_cksum) r->update_cksum(r,buf,bytes_to_write);
        // 写入数据，如果写失败了，设置rio的err标识，返回0
        if (r->write(r,buf,bytes_to_write) == 0) {
            r->flags |= RIO_FLAG_WRITE_ERROR;
            return 0;
        }
        // 写成功了，更新buf下次处理的位置，以及还需要处理写入的字节数。
        buf = (char*)buf + bytes_to_write;
        len -= bytes_to_write;
        // 更新rio中已经处理的总字节数。
        r->processed_bytes += bytes_to_write;
    }
    // 写成功，返回1
    return 1;
}

// 将rio目标中数据读到buf中。读出len字节的数据。
static inline size_t rioRead(rio *r, void *buf, size_t len) {
    // 如果有err，返回0
    if (r->flags & RIO_FLAG_READ_ERROR) return 0;
    // 如果没有读够，一直循环处理。
    while (len) {
        // 计算一次能读取的字节数。
        size_t bytes_to_read = (r->max_processing_chunk && r->max_processing_chunk < len) ? r->max_processing_chunk : len;
        // 读出数据，如果读失败了，设置rio的err标识，返回0
        if (r->read(r,buf,bytes_to_read) == 0) {
            r->flags |= RIO_FLAG_READ_ERROR;
            return 0;
        }
        // 读出成功了，如果有update_cksum，这里使用本次读出的数据更新checksum。
        if (r->update_cksum) r->update_cksum(r,buf,bytes_to_read);
        // 更新读出数据下次写入buf的位置，更新还需要读出的数据大小。
        buf = (char*)buf + bytes_to_read;
        len -= bytes_to_read;
        // 更新rio中已经处理读出的总字节数。
        r->processed_bytes += bytes_to_read;
    }
    // 读成功，返回1
    return 1;
}

// 获取当前读/写rio中的处理位置
static inline off_t rioTell(rio *r) {
    return r->tell(r);
}

// 将rio的数据刷到target中。
static inline int rioFlush(rio *r) {
    return r->flush(r);
}

/* This function allows to know if there was a read error in any past
 * operation, since the rio stream was created or since the last call
 * to rioClearError(). */
// 判断当前rio中是否有read err。
static inline int rioGetReadError(rio *r) {
    return (r->flags & RIO_FLAG_READ_ERROR) != 0;
}

/* Like rioGetReadError() but for write errors. */
// 判断当前rio中是否有write err。
static inline int rioGetWriteError(rio *r) {
    return (r->flags & RIO_FLAG_WRITE_ERROR) != 0;
}

// 清空rio的err
static inline void rioClearErrors(rio *r) {
    r->flags &= ~(RIO_FLAG_READ_ERROR|RIO_FLAG_WRITE_ERROR);
}

void rioInitWithFile(rio *r, FILE *fp);
void rioInitWithBuffer(rio *r, sds s);
void rioInitWithConn(rio *r, connection *conn, size_t read_limit);
void rioInitWithFd(rio *r, int fd);

void rioFreeFd(rio *r);
void rioFreeConn(rio *r, sds* out_remainingBufferedData);

size_t rioWriteBulkCount(rio *r, char prefix, long count);
size_t rioWriteBulkString(rio *r, const char *buf, size_t len);
size_t rioWriteBulkLongLong(rio *r, long long l);
size_t rioWriteBulkDouble(rio *r, double d);

struct redisObject;
int rioWriteBulkObject(rio *r, struct redisObject *obj);

void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len);
void rioSetAutoSync(rio *r, off_t bytes);

#endif
