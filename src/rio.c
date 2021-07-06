/* rio.c is a simple stream-oriented I/O abstraction that provides an interface
 * to write code that can consume/produce data using different concrete input
 * and output devices. For instance the same rdb.c code using the rio
 * abstraction can be used to read and write the RDB format using in-memory
 * buffers or files.
 *
 * A rio object provides the following methods:
 *  read: read from stream.
 *  write: write to stream.
 *  tell: get the current offset.
 *
 * It is also possible to set a 'checksum' method that is used by rio.c in order
 * to compute a checksum of the data written or read, or to query the rio object
 * for the current checksum.
 *
 * rio.c是一个简单的面向流式处理的I/O抽象，提供了接口来编写(可以使用不同的具体输入输出设备)处理consume/produce数据的代码。
 * 例如，rdb.c中代码使用了rio抽象，从而实现了基于内存缓冲区或文件格式的RDB数据的读取和写入。
 * 一个rio对应提供如下的方法：
 *  read：从stream中读数据。
 *  write：写数据到stream。
 *  tell：获取当前处理的offset。
 * 也可以设置'checksum'方法，用于计算写入或读取数据的校验和，或查询rio当前已处理数据的校验和。
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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


#include "fmacros.h"
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "rio.h"
#include "util.h"
#include "crc64.h"
#include "config.h"
#include "server.h"

/* ------------------------- Buffer I/O implementation ----------------------- */

/* Returns 1 or 0 for success/failure. */
// 将buf中的数据，写入len长度到rio的buffer中。成功返回1，失败返回0。
static size_t rioBufferWrite(rio *r, const void *buf, size_t len) {
    r->io.buffer.ptr = sdscatlen(r->io.buffer.ptr,(char*)buf,len);
    r->io.buffer.pos += len;
    return 1;
}

/* Returns 1 or 0 for success/failure. */
// 从rio的buffer读取len数据到参数buf中。成功返回1，失败返回0。
static size_t rioBufferRead(rio *r, void *buf, size_t len) {
    if (sdslen(r->io.buffer.ptr)-r->io.buffer.pos < len)
        // 如果没有足够的数据读取，则读失败，返回0
        return 0; /* not enough buffer to return len bytes. */
    memcpy(buf,r->io.buffer.ptr+r->io.buffer.pos,len);
    r->io.buffer.pos += len;
    return 1;
}

/* Returns read/write position in buffer. */
// 返回当前rio处理的buffer位置。
static off_t rioBufferTell(rio *r) {
    return r->io.buffer.pos;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
// 将rio的数据刷到目标设备。
static int rioBufferFlush(rio *r) {
    UNUSED(r);
    // 使用内存buf，什么都不需要做
    return 1; /* Nothing to do, our write just appends to the buffer. */
}

// rio的基于buffer的实现
static const rio rioBufferIO = {
    rioBufferRead,
    rioBufferWrite,
    rioBufferTell,
    rioBufferFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* flags */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

void rioInitWithBuffer(rio *r, sds s) {
    *r = rioBufferIO;
    r->io.buffer.ptr = s;
    r->io.buffer.pos = 0;
}

/* --------------------- Stdio file pointer implementation ------------------- */

/* Returns 1 or 0 for success/failure. */
// 将buf数据写入stdio文件指针。使用fwrite
static size_t rioFileWrite(rio *r, const void *buf, size_t len) {
    size_t retval;

    // 写入数据到用户空间文件缓存
    retval = fwrite(buf,len,1,r->io.file.fp);
    r->io.file.buffered += len;

    // 达到自动刷盘数量，处理刷盘
    if (r->io.file.autosync &&
        r->io.file.buffered >= r->io.file.autosync)
    {
        // 先将该文件在用户空间glibc里面的的4KB的buffer数据写入内存缓存。
        fflush(r->io.file.fp);
        // 将该文件的脏页写到磁盘，包括page cache和inode本身。
        if (redis_fsync(fileno(r->io.file.fp)) == -1) return 0;
        // 重置待刷盘数据
        r->io.file.buffered = 0;
    }
    return retval;
}

/* Returns 1 or 0 for success/failure. */
// 从文件指针读数据到buf中，使用fread。
static size_t rioFileRead(rio *r, void *buf, size_t len) {
    return fread(buf,len,1,r->io.file.fp);
}

/* Returns read/write position in file. */
// 获取当前处理文件的pos
static off_t rioFileTell(rio *r) {
    return ftello(r->io.file.fp);
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
// 刷新数据到目标设备，这里只是刷新用户空间数据到内核空间文件缓存中，等待操作系统刷盘。
static int rioFileFlush(rio *r) {
    return (fflush(r->io.file.fp) == 0) ? 1 : 0;
}

static const rio rioFileIO = {
    rioFileRead,
    rioFileWrite,
    rioFileTell,
    rioFileFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* flags */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

void rioInitWithFile(rio *r, FILE *fp) {
    *r = rioFileIO;
    r->io.file.fp = fp;
    r->io.file.buffered = 0;
    r->io.file.autosync = 0;
}

/* ------------------- Connection implementation -------------------
 * We use this RIO implementation when reading an RDB file directly from
 * the connection to the memory via rdbLoadRio(), thus this implementation
 * only implements reading from a connection that is, normally,
 * just a socket. */
// 当通过rdbLoadRio()从conn中直接读取RDB文件数据到内存中时，我们使用这个Connection RIO 实现。
// 因此这个实现只实现了从socket conn读数据的处理。

// 当前target不支持写，所以返回0。
static size_t rioConnWrite(rio *r, const void *buf, size_t len) {
    UNUSED(r);
    UNUSED(buf);
    UNUSED(len);
    return 0; /* Error, this target does not yet support writing. */
}

/* Returns 1 or 0 for success/failure. */
// 从conn RIO中读数据到buf中。
static size_t rioConnRead(rio *r, void *buf, size_t len) {
    // 计算当前conn RIO中可读的数据量
    size_t avail = sdslen(r->io.conn.buf)-r->io.conn.pos;

    /* If the buffer is too small for the entire request: realloc. */
    // 如果conn buf对于请求的数据长度len来说太小，则重新分配内存。
    if (sdslen(r->io.conn.buf) + sdsavail(r->io.conn.buf) < len)
        r->io.conn.buf = sdsMakeRoomFor(r->io.conn.buf, len - sdslen(r->io.conn.buf));

    /* If the remaining unused buffer is not large enough: memmove so that we
     * can read the rest. */
    // 如果conn的buffer中 剩余未读取的数据量+没使用的长度 不够长，则将数据往前移。
    if (len > avail && sdsavail(r->io.conn.buf) < len - avail) {
        sdsrange(r->io.conn.buf, r->io.conn.pos, -1);
        r->io.conn.pos = 0;
    }

    /* If we don't already have all the data in the sds, read more */
    // 如果conn buffer中没有足够的数据读取，则需要从conn的socket中先读取数据到conn buffer中，然后再处理数据读出。
    while (len > sdslen(r->io.conn.buf) - r->io.conn.pos) {
        // 计算conn buf中已有的数据量，以及还需要从socket读取的数据量
        size_t buffered = sdslen(r->io.conn.buf) - r->io.conn.pos;
        size_t needs = len - buffered;
        /* Read either what's missing, or PROTO_IOBUF_LEN, the bigger of
         * the two. */
        // 一次尽可能多的从socket读取数据，填满conn buffer。
        size_t toread = needs < PROTO_IOBUF_LEN ? PROTO_IOBUF_LEN: needs;
        if (toread > sdsavail(r->io.conn.buf)) toread = sdsavail(r->io.conn.buf);
        // 判断读取数据是否会超过限制
        if (r->io.conn.read_limit != 0 &&
            r->io.conn.read_so_far + buffered + toread > r->io.conn.read_limit)
        {
            /* Make sure the caller didn't request to read past the limit.
             * If they didn't we'll buffer till the limit, if they did, we'll
             * return an error. */
            // 确保调用者读取数据后没有超出限制。没有超限，则我们将尽可能取读最多的数据；而如果超限了，我们将返回错误。
            if (r->io.conn.read_limit >= r->io.conn.read_so_far + len)
                toread = r->io.conn.read_limit - r->io.conn.read_so_far - buffered;
            else {
                errno = EOVERFLOW;
                return 0;
            }
        }
        // 处理socket数据读取
        int retval = connRead(r->io.conn.conn,
                          (char*)r->io.conn.buf + sdslen(r->io.conn.buf),
                          toread);
        if (retval <= 0) {
            if (errno == EWOULDBLOCK) errno = ETIMEDOUT;
            return 0;
        }
        sdsIncrLen(r->io.conn.buf, retval);
    }

    // 数据从conn buffer中读取到返回的buf中
    memcpy(buf, (char*)r->io.conn.buf + r->io.conn.pos, len);
    r->io.conn.read_so_far += len;
    r->io.conn.pos += len;
    return len;
}

/* Returns read/write position in file. */
static off_t rioConnTell(rio *r) {
    return r->io.conn.read_so_far;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
static int rioConnFlush(rio *r) {
    /* Our flush is implemented by the write method, that recognizes a
     * buffer set to NULL with a count of zero as a flush request. */
    // 这里flush通过write方法实现，当识别参数中buffer为NULL，len为0时，处理flush到socket。目前没有使用。
    return rioConnWrite(r,NULL,0);
}

static const rio rioConnIO = {
    rioConnRead,
    rioConnWrite,
    rioConnTell,
    rioConnFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* flags */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

/* Create an RIO that implements a buffered read from an fd
 * read_limit argument stops buffering when the reaching the limit. */
// 使用socket conn创建rio，read_limit参数表示最大数据读取限制。
void rioInitWithConn(rio *r, connection *conn, size_t read_limit) {
    *r = rioConnIO;
    r->io.conn.conn = conn;
    r->io.conn.pos = 0;
    r->io.conn.read_limit = read_limit;
    r->io.conn.read_so_far = 0;
    r->io.conn.buf = sdsnewlen(NULL, PROTO_IOBUF_LEN);
    sdsclear(r->io.conn.buf);
}

/* Release the RIO stream. Optionally returns the unread buffered data
 * when the SDS pointer 'remaining' is passed. */
// 释放socket conn的rio。如果传入了remaining，则将通过该字段返回conn buffer中的未读取的数据。
void rioFreeConn(rio *r, sds *remaining) {
    if (remaining && (size_t)r->io.conn.pos < sdslen(r->io.conn.buf)) {
        if (r->io.conn.pos > 0) sdsrange(r->io.conn.buf, r->io.conn.pos, -1);
        *remaining = r->io.conn.buf;
    } else {
        sdsfree(r->io.conn.buf);
        if (remaining) *remaining = NULL;
    }
    r->io.conn.buf = NULL;
}

/* ------------------- File descriptor implementation ------------------
 * This target is used to write the RDB file to pipe, when the master just
 * streams the data to the replicas without creating an RDB on-disk image
 * (diskless replication option).
 * It only implements writes. */
// fd目标读写的实现，主要用于写RDB文件到pipe中。当master不在磁盘创建RDB文件，只通过stream传数据到slave时，使用该实现。
// 目前只实现了写方法。

/* Returns 1 or 0 for success/failure.
 *
 * When buf is NULL and len is 0, the function performs a flush operation
 * if there is some pending buffer, so this function is also used in order
 * to implement rioFdFlush(). */
// 当rio中有待刷盘的数据，如果传入参数buf为NULL，len为0时，函数执行flush操作。所以这个函数也可以用于实现rioFdFlush()。
static size_t rioFdWrite(rio *r, const void *buf, size_t len) {
    ssize_t retval;
    // p指向需要flush的数据。当前默认先指向传入的buf。
    unsigned char *p = (unsigned char*) buf;
    // 是否flush操作
    int doflush = (buf == NULL && len == 0);

    /* For small writes, we rather keep the data in user-space buffer, and flush
     * it only when it grows. however for larger writes, we prefer to flush
     * any pre-existing buffer, and write the new one directly without reallocs
     * and memory copying. */
    // 对于较小的写入，我们倾向于保存数据在用户空间缓存中，当达到一定数量后才进行flush。
    // 对于较大的写入，我们会先将已经存在用户空间缓存的数据flush后，再将数据直接写入到fd中。
    if (len > PROTO_IOBUF_LEN) {
        /* First, flush any pre-existing buffered data. */
        // 当前写入数据较大。先把用户空间缓存的数据flush。这里调用的是rioFdWrite，传入NULL和0。
        if (sdslen(r->io.fd.buf)) {
            if (rioFdWrite(r, NULL, 0) == 0)
                return 0;
        }
        /* Write the new data, keeping 'p' and 'len' from the input. */
    } else {
        if (len) {
            // 写入数据比较小（非0，不是flush），数据追加到用户空间缓存中。
            r->io.fd.buf = sdscatlen(r->io.fd.buf,buf,len);
            // 判断缓存数据是否达到一定数量，决定是否进行flush。
            if (sdslen(r->io.fd.buf) > PROTO_IOBUF_LEN)
                doflush = 1;
            // 如果用户缓存数据还是比较小，不需要flush，直接返回。
            if (!doflush)
                return 1;
        }
        /* Flusing the buffered data. set 'p' and 'len' accordintly. */
        // len=0指定要flush，或者当前小数据写入后，需要进行flush，这里准备好要flush的数据。
        // p指向待flush到fd的数据。len表示写入fd数据长度。
        p = (unsigned char*) r->io.fd.buf;
        len = sdslen(r->io.fd.buf);
    }

    // 能执行到这里，说明数据需要flush到fd，且p指向待flush数据，len为数据长度。
    size_t nwritten = 0;
    while(nwritten != len) {
        // 写数据到fd，只要没写完就循环处理。
        retval = write(r->io.fd.fd,p+nwritten,len-nwritten);
        if (retval <= 0) {
            /* With blocking io, which is the sole user of this
             * rio target, EWOULDBLOCK is returned only because of
             * the SO_SNDTIMEO socket option, so we translate the error
             * into one more recognizable by the user. */
            if (retval == -1 && errno == EWOULDBLOCK) errno = ETIMEDOUT;
            return 0; /* error. */
        }
        nwritten += retval;
    }

    // 更新pos，清空用户空间中的buf
    r->io.fd.pos += len;
    sdsclear(r->io.fd.buf);
    return 1;
}

/* Returns 1 or 0 for success/failure. */
// 从fd读数据，目前不支持，没有实现
static size_t rioFdRead(rio *r, void *buf, size_t len) {
    UNUSED(r);
    UNUSED(buf);
    UNUSED(len);
    return 0; /* Error, this target does not support reading. */
}

/* Returns read/write position in file. */
static off_t rioFdTell(rio *r) {
    return r->io.fd.pos;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
// flush缓存数据到fd，通过rioFdWrite来实现的
static int rioFdFlush(rio *r) {
    /* Our flush is implemented by the write method, that recognizes a
     * buffer set to NULL with a count of zero as a flush request. */
    return rioFdWrite(r,NULL,0);
}

static const rio rioFdIO = {
    rioFdRead,
    rioFdWrite,
    rioFdTell,
    rioFdFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* flags */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

void rioInitWithFd(rio *r, int fd) {
    *r = rioFdIO;
    r->io.fd.fd = fd;
    r->io.fd.pos = 0;
    r->io.fd.buf = sdsempty();
}

/* release the rio stream. */
void rioFreeFd(rio *r) {
    sdsfree(r->io.fd.buf);
}

/* ---------------------------- Generic functions ---------------------------- */

/* This function can be installed both in memory and file streams when checksum
 * computation is needed. */
// 这个函数可以直接安装到内存或文件流式数据处理中，佣金计算checksum
void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len) {
    r->cksum = crc64(r->cksum,buf,len);
}

/* Set the file-based rio object to auto-fsync every 'bytes' file written.
 * By default this is set to zero that means no automatic file sync is
 * performed.
 *
 * This feature is useful in a few contexts since when we rely on OS write
 * buffers sometimes the OS buffers way too much, resulting in too many
 * disk I/O concentrated in very little time. When we fsync in an explicit
 * way instead the I/O pressure is more distributed across time. */
// 将基于文件的rio对象设置为自动fsync。参数bytes为自动刷盘的数量，当达到该数量时自动刷盘。
// 默认情况下，autosync值设置为零，表示不进行自动刷盘。
// 这个功能在某些情况下很有用，因为当我们依赖OS刷盘时，有时OS缓冲区的数据太多，刷盘时可能导致短时间内太多的磁盘I/O。
// 当我们以显式方式进行fsync时，I/O压力将在整个时间范围内分布得更加分散。
void rioSetAutoSync(rio *r, off_t bytes) {
    // 如果当前write不是文件io write，直接返回。
    if(r->write != rioFileIO.write) return;
    r->io.file.autosync = bytes;
}

/* --------------------------- Higher level interface --------------------------
 *
 * The following higher level functions use lower level rio.c functions to help
 * generating the Redis protocol for the Append Only File. */
// 下面的高级别方法，都是使用低级别的rio.c方法，来帮助AOF生成Redis协议数据。

/* Write multi bulk count in the format: "*<count>\r\n". */
// 写入multi bulk count格式数据。prefix可以是'*'或'$'。
size_t rioWriteBulkCount(rio *r, char prefix, long count) {
    char cbuf[128];
    int clen;

    // 先写prefix，在写数字的字符串，最后写入"\r\n"。
    cbuf[0] = prefix;
    clen = 1+ll2string(cbuf+1,sizeof(cbuf)-1,count);
    cbuf[clen++] = '\r';
    cbuf[clen++] = '\n';
    // 数据写入rio目标中
    if (rioWrite(r,cbuf,clen) == 0) return 0;
    return clen;
}

/* Write binary-safe string in the format: "$<count>\r\n<payload>\r\n". */
// 写入二进制安全的字符串到rio目标中。字符串包含 字符长度count 和 字符串本身 两部分。
size_t rioWriteBulkString(rio *r, const char *buf, size_t len) {
    size_t nwritten;

    // 先写入count（包含了结束符'\r\n'），再写入字符串本身，再写入字符串结束符'\r\n'
    if ((nwritten = rioWriteBulkCount(r,'$',len)) == 0) return 0;
    if (len > 0 && rioWrite(r,buf,len) == 0) return 0;
    if (rioWrite(r,"\r\n",2) == 0) return 0;
    return nwritten+len+2;
}

/* Write a long long value in format: "$<count>\r\n<payload>\r\n". */
// 写入整型数字到rio目标。需要先将数字转换为字符串格式，然后按字符串写入。
size_t rioWriteBulkLongLong(rio *r, long long l) {
    char lbuf[32];
    unsigned int llen;

    // 格式化数字到字符串，在写入
    llen = ll2string(lbuf,sizeof(lbuf),l);
    return rioWriteBulkString(r,lbuf,llen);
}

/* Write a double value in the format: "$<count>\r\n<payload>\r\n" */
// 写入浮点型数字到rio目标。需要先将数字转换为字符串格式，然后按字符串写入。
size_t rioWriteBulkDouble(rio *r, double d) {
    char dbuf[128];
    unsigned int dlen;

    // 格式化数字到字符串，在写入
    dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
    return rioWriteBulkString(r,dbuf,dlen);
}
