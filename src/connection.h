
/*
 * Copyright (c) 2019, Redis Labs
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

#ifndef __REDIS_CONNECTION_H
#define __REDIS_CONNECTION_H

#define CONN_INFO_LEN   32

struct aeEventLoop;
typedef struct connection connection;

typedef enum {
    CONN_STATE_NONE = 0,
    CONN_STATE_CONNECTING,
    CONN_STATE_ACCEPTING,
    CONN_STATE_CONNECTED,
    CONN_STATE_CLOSED,
    CONN_STATE_ERROR
} ConnectionState;

#define CONN_FLAG_CLOSE_SCHEDULED   (1<<0)      /* Closed scheduled by a handler */
#define CONN_FLAG_WRITE_BARRIER     (1<<1)      /* Write barrier requested */

#define CONN_TYPE_SOCKET            1
#define CONN_TYPE_TLS               2

// 函数指针别名
typedef void (*ConnectionCallbackFunc)(struct connection *conn);

// Connection 类型接口，本文件中有对这些接口的封装对外提供的方法。
// 参数都是conn，调用conn->type->方法 实现。
typedef struct ConnectionType {
    // 一批的函数指针
    void (*ae_handler)(struct aeEventLoop *el, int fd, void *clientData, int mask);
    int (*connect)(struct connection *conn, const char *addr, int port, const char *source_addr, ConnectionCallbackFunc connect_handler);
    int (*write)(struct connection *conn, const void *data, size_t data_len);
    int (*read)(struct connection *conn, void *buf, size_t buf_len);
    void (*close)(struct connection *conn);
    int (*accept)(struct connection *conn, ConnectionCallbackFunc accept_handler);
    int (*set_write_handler)(struct connection *conn, ConnectionCallbackFunc handler, int barrier);
    int (*set_read_handler)(struct connection *conn, ConnectionCallbackFunc handler);
    const char *(*get_last_error)(struct connection *conn);
    int (*blocking_connect)(struct connection *conn, const char *addr, int port, long long timeout);
    ssize_t (*sync_write)(struct connection *conn, char *ptr, ssize_t size, long long timeout);
    ssize_t (*sync_read)(struct connection *conn, char *ptr, ssize_t size, long long timeout);
    ssize_t (*sync_readline)(struct connection *conn, char *ptr, ssize_t size, long long timeout);
    int (*get_type)(struct connection *conn);
} ConnectionType;

struct connection {
    ConnectionType *type;   // conn类型
    ConnectionState state;  // conn状态
    short int flags;        // conn的一些标识flag
    short int refs;         // conn的引用，为0时close？
    int last_errno;         // conn最后一次报错的errno
    void *private_data;     // 私有数据，socket-type中是连接的client对象
    ConnectionCallbackFunc conn_handler;    // 处理conn连接的handler
    ConnectionCallbackFunc write_handler;   // 处理conn写数据的handler
    ConnectionCallbackFunc read_handler;    // 处理conn读数据的handler
    int fd;     // conn关联的文件描述符。
};

/* The connection module does not deal with listening and accepting sockets,
 * so we assume we have a socket when an incoming connection is created.
 *
 * connection模块不处理listing和accepting状态的sockets，所以conn有关联socket，那么底层连接已经建立起来了。
 * 提供的fd应该是被关联到已经accepted的socket。
 *
 * The fd supplied should therefore be associated with an already accept()ed
 * socket.
 *
 * connAccept() may directly call accept_handler(), or return and call it
 * at a later time. This behavior is a bit awkward but aims to reduce the need
 * to wait for the next event loop, if no additional handshake is required.
 *
 * connAccept() 可能直接调用accept_handler（socket-type），也可能先return后面再调用（tls-type）。
 * 这种行为可能有点不好处理，但为了减少下一次事件循环的等待，没有进一步握手需求的话直接返回。
 *
 * IMPORTANT: accept_handler may decide to close the connection, calling connClose().
 * To make this safe, the connection is only marked with CONN_FLAG_CLOSE_SCHEDULED
 * in this case, and connAccept() returns with an error.
 *
 * 重要：accept_handler可能会close conn，调用connClose()。
 * 为了安全，只有在flag为CONN_FLAG_CLOSE_SCHEDULED会close，此时connAccept会返回error。
 * 调用connAccept时总是必须检查返回值，如果时error时，需要调用connClose关闭conn。
 *
 * connAccept() callers must always check the return value and on error (C_ERR)
 * a connClose() must be called.
 */

static inline int connAccept(connection *conn, ConnectionCallbackFunc accept_handler) {
    return conn->type->accept(conn, accept_handler);
}

/* Establish a connection.  The connect_handler will be called when the connection
 * is established, or if an error has occurred.
 *
 * The connection handler will be responsible to set up any read/write handlers
 * as needed.
 *
 * If C_ERR is returned, the operation failed and the connection handler shall
 * not be expected.
 */
// 建立一个连接，当连接建立时connect_handler将被调用。
// socket-type是通过非阻塞connect，监听WRITABLE事件来实现。事件处理函数connSocketEventHandler。
// 当监听socket可写时，要么有error要么建立conn成功，后面connSocketEventHandler中调用connect_handler处理。

// connect_handler负责注入真正的read/write处理函数。connSetReadHandler或connSetWriteHandler。
static inline int connConnect(connection *conn, const char *addr, int port, const char *src_addr,
        ConnectionCallbackFunc connect_handler) {
    return conn->type->connect(conn, addr, port, src_addr, connect_handler);
}

/* Blocking connect.
 *
 * NOTE: This is implemented in order to simplify the transition to the abstract
 * connections, but should probably be refactored out of cluster.c and replication.c,
 * in favor of a pure async implementation.
 */
// 建立阻塞连接，这个实现只是一个简单的过度方式。后面重构支持全部去掉，使用完全的异步实现。
static inline int connBlockingConnect(connection *conn, const char *addr, int port, long long timeout) {
    return conn->type->blocking_connect(conn, addr, port, timeout);
}

/* Write to connection, behaves the same as write(2).
 *
 * Like write(2), a short write is possible. A -1 return indicates an error.
 *
 * The caller should NOT rely on errno. Testing for an EAGAIN-like condition, use
 * connGetState() to see if the connection state is still CONN_STATE_CONNECTED.
 */
// 写操作。这里不应该依赖于errno，因为非阻塞操作时是允许出现EAGAIN类型（阻塞类型）错误的。
// 写没完成就返回，就会报EAGAIN错，非阻塞时是正常的。此时可以使用connGetState来看连接是否正常。
static inline int connWrite(connection *conn, const void *data, size_t data_len) {
    return conn->type->write(conn, data, data_len);
}

/* Read from the connection, behaves the same as read(2).
 * 
 * Like read(2), a short read is possible.  A return value of 0 will indicate the
 * connection was closed, and -1 will indicate an error.
 *
 * The caller should NOT rely on errno. Testing for an EAGAIN-like condition, use
 * connGetState() to see if the connection state is still CONN_STATE_CONNECTED.
 */
// 读操作。类似写操作。也不能依赖于errno
static inline int connRead(connection *conn, void *buf, size_t buf_len) {
    return conn->type->read(conn, buf, buf_len);
}

/* Register a write handler, to be called when the connection is writable.
 * If NULL, the existing handler is removed.
 */
// 注册写处理函数。连接可写时，event loop会调用事件处理函数connSocketEventHandler。
// 这个handler函数会调用注册的写处理函数处理写事件。
static inline int connSetWriteHandler(connection *conn, ConnectionCallbackFunc func) {
    return conn->type->set_write_handler(conn, func, 0);
}

/* Register a read handler, to be called when the connection is readable.
 * If NULL, the existing handler is removed.
 */
// 注册读处理函数。连接可读时，event loop会调用事件处理函数connSocketEventHandler。
// 这个handler函数会调用注册的读处理函数处理读事件。
static inline int connSetReadHandler(connection *conn, ConnectionCallbackFunc func) {
    return conn->type->set_read_handler(conn, func);
}

/* Set a write handler, and possibly enable a write barrier, this flag is
 * cleared when write handler is changed or removed.
 * With barrier enabled, we never fire the event if the read handler already
 * fired in the same event loop iteration. Useful when you want to persist
 * things to disk before sending replies, and want to do that in a group fashion. */
// 注册写处理函数，可能会设置写屏障标识，当写处理函数变更或移除时，该标识会被清除。
// 有写屏障时，在同一个事件循环中，当读事件已经处理时不会触发写事件。
// 主要用于先处理读事件（计算或操作），事件loop完处理持久化（或其他处理），下一轮再处理写事件（回复client等）。
static inline int connSetWriteHandlerWithBarrier(connection *conn, ConnectionCallbackFunc func, int barrier) {
    return conn->type->set_write_handler(conn, func, barrier);
}

// 关闭连接
static inline void connClose(connection *conn) {
    conn->type->close(conn);
}

/* Returns the last error encountered by the connection, as a string.  If no error,
 * a NULL is returned.
 */
// 获取最后一次的error信息
static inline const char *connGetLastError(connection *conn) {
    return conn->type->get_last_error(conn);
}

// 同步写，socket-type的具体实现见syncio.c
// 这三个接口使用的地方最好可以考虑使用异步操作来替代。
static inline ssize_t connSyncWrite(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return conn->type->sync_write(conn, ptr, size, timeout);
}

// 同步读，socket-type的具体实现见syncio.c
static inline ssize_t connSyncRead(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return conn->type->sync_read(conn, ptr, size, timeout);
}

// 同步读一行，socket-type的具体实现见syncio.c
static inline ssize_t connSyncReadLine(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return conn->type->sync_readline(conn, ptr, size, timeout);
}

/* Return CONN_TYPE_* for the specified connection */
// 查询当前conn的类型，目前只有socket和tls两类。
static inline int connGetType(connection *conn) {
    return conn->type->get_type(conn);
}

// socket-type的conn创建方法
connection *connCreateSocket();
connection *connCreateAcceptedSocket(int fd);

// tls-type的conn创建方法
connection *connCreateTLS();
connection *connCreateAcceptedTLS(int fd, int require_auth);

// conn公用的部分方法
void connSetPrivateData(connection *conn, void *data);
void *connGetPrivateData(connection *conn);
int connGetState(connection *conn);
int connHasWriteHandler(connection *conn);
int connHasReadHandler(connection *conn);
int connGetSocketError(connection *conn);

/* anet-style wrappers to conns */
// anet底层函数的封装，主要socket-type类型conn调用？
int connBlock(connection *conn);
int connNonBlock(connection *conn);
int connEnableTcpNoDelay(connection *conn);
int connDisableTcpNoDelay(connection *conn);
int connKeepAlive(connection *conn, int interval);
int connSendTimeout(connection *conn, long long ms);
int connRecvTimeout(connection *conn, long long ms);
int connPeerToString(connection *conn, char *ip, size_t ip_len, int *port);
int connFormatFdAddr(connection *conn, char *buf, size_t buf_len, int fd_to_str_type);
int connSockName(connection *conn, char *ip, size_t ip_len, int *port);
const char *connGetInfo(connection *conn, char *buf, size_t buf_len);

/* Helpers for tls special considerations */
// tls的一些特殊方法
sds connTLSGetPeerCert(connection *conn);
int tlsHasPendingData();
int tlsProcessPendingData();

#endif  /* __REDIS_CONNECTION_H */
