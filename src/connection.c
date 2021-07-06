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

#include "server.h"
#include "connhelpers.h"

/* The connections module provides a lean abstraction of network connections
 * to avoid direct socket and async event management across the Redis code base.
 * connections模块提供了一个network connections的抽象，避免redis代码直接对socket和异步事件管理。
 *
 * It does NOT provide advanced connection features commonly found in similar
 * libraries such as complete in/out buffer management, throttling, etc. These
 * functions remain in networking.c.
 * 它不提供进深入的connection功能，如完全的in/out buffer管理，限流等，这些都还是在networking.c中处理。
 *
 * The primary goal is to allow transparent handling of TCP and TLS based
 * connections. To do so, connections have the following properties:
 *
 * 1. A connection may live before its corresponding socket exists.  This
 *    allows various context and configuration setting to be handled before
 *    establishing the actual connection.
 * 2. The caller may register/unregister logical read/write handlers to be
 *    called when the connection has data to read from/can accept writes.
 *    These logical handlers may or may not correspond to actual AE events,
 *    depending on the implementation (for TCP they are; for TLS they aren't).
 *
 * 主要提供透明的TCP、TLS的connections处理。因此conn需要有以下约定：
 * 1、connection可能先于它所关联的socket存在。这样可以允许在真正建立连接前，进行多种的conn的context及配置设置。
 * 2、调用放可以register/unregister 实际的 read/write handlers。
 */

ConnectionType CT_Socket;

/* When a connection is created we must know its type already, but the
 * underlying socket may or may not exist:
 * 创建一个connection时，我们必须知道它的type，但是底层的socket可能不存在。
 * 1、对于accepted connections，已经存在了socket，所以在调用connCreateSocket后使用connAccept，不需要listen/accept步骤。
 * 2、对于outgoing connections，socket是conneciton自己创建的，所以需要connCreateSocket后使用connConnect。
 *    这种需要注册一个connect回调函数，用于底层建立连接后处理。
 *
 * - For accepted connections, it exists as we do not model the listen/accept
 *   part; So caller calls connCreateSocket() followed by connAccept().
 * - For outgoing connections, the socket is created by the connection module
 *   itself; So caller calls connCreateSocket() followed by connConnect(),
 *   which registers a connect callback that fires on connected/error state
 *   (and after any transport level handshake was done).
 *
 * NOTE: An earlier version relied on connections being part of other structs
 * and not independently allocated. This could lead to further optimizations
 * like using container_of(), etc.  However it was discontinued in favor of
 * this approach for these reasons:
 * 早期版本，依赖的connections是作为相应structs的一部分，而不是单独进行内存分配的。
 * 会导致一些问题，这里单独提出来，理由如下：
 *
 * 1. In some cases conns are created/handled outside the context of the
 * containing struct, in which case it gets a bit awkward to copy them.
 * 2. Future implementations may wish to allocate arbitrary data for the
 * connection.
 * 3. The container_of() approach is anyway risky because connections may
 * be embedded in different structs, not just client.
 */

// 创建一个新的socket-type conn。不关联fd。
connection *connCreateSocket() {
    connection *conn = zcalloc(sizeof(connection));
    conn->type = &CT_Socket;
    conn->fd = -1;

    return conn;
}

/* Create a new socket-type connection that is already associated with
 * an accepted connection.
 *
 * The socket is not ready for I/O until connAccept() was called and
 * invoked the connection-level accept handler.
 *
 * Callers should use connGetState() and verify the created connection
 * is not in an error state (which is not possible for a socket connection,
 * but could but possible with other protocols).
 */
// 创建一个新的socket-type conn，关联到已经accepted conn fd。
// conn还需要调用connAccept()，执行conn级别的accept handler才可以处理io。
// 此时conn级别的状态是accepting
connection *connCreateAcceptedSocket(int fd) {
    connection *conn = connCreateSocket();
    conn->fd = fd;
    conn->state = CONN_STATE_ACCEPTING;
    return conn;
}

// 对于socket-type conn，如果调用connCreateSocket创建conn，可能还需要调用connSocketConnect来建立连接进行fd关联。
// socket-type conn（CT_Socket）的connect方法。
static int connSocketConnect(connection *conn, const char *addr, int port, const char *src_addr,
        ConnectionCallbackFunc connect_handler) {
    // 建立连接，尽可能bind。src_addr主动于add建立连接，src_addr作为client的话，可能不需要bind，所以这里best effort bind。
    int fd = anetTcpNonBlockBestEffortBindConnect(NULL,addr,port,src_addr);
    if (fd == -1) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = errno;
        return C_ERR;
    }

    conn->fd = fd;
    // 设置为connecting，后面事件处理函数connSocketEventHandler中判断state来处理。
    conn->state = CONN_STATE_CONNECTING;

    // 设置连接的handler，connSocketEventHandler中会调用conn_handler。
    conn->conn_handler = connect_handler;
    // socket使用非阻塞connect连接成功或者失败以后，文件描述符会变成可写状态。
    // 所以这里创建事件监听可写。事件处理函数ae_handler为connSocketEventHandler
    aeCreateFileEvent(server.el, conn->fd, AE_WRITABLE,
            conn->type->ae_handler, conn);

    return C_OK;
}

/* Returns true if a write handler is registered */
// 判断是否有write_handler
int connHasWriteHandler(connection *conn) {
    return conn->write_handler != NULL;
}

/* Returns true if a read handler is registered */
// 判断是否有read_handler
int connHasReadHandler(connection *conn) {
    return conn->read_handler != NULL;
}

/* Associate a private data pointer with the connection */
// 设置conn的私有数据，一般连接是clinet信息、集群连接是link信息、slave连接是master信息（也是一个client）。
void connSetPrivateData(connection *conn, void *data) {
    conn->private_data = data;
}

/* Get the associated private data pointer */
// 获取conn关联的私有数据
void *connGetPrivateData(connection *conn) {
    return conn->private_data;
}

/* ------ Pure socket connections ------- */

/* A very incomplete list of implementation-specific calls.  Much of the above shall
 * move here as we implement additional connection types.
 */
// 完全的socket connections方法。不完整，前面的一些方法也应该放这里，完全实现conn type级别方法。

/* Close the connection and free resources. */
// 关闭socket conn
static void connSocketClose(connection *conn) {
    if (conn->fd != -1) {
        // 如果关联到fd，先移除fd的事件监听，再关闭fd。
        aeDeleteFileEvent(server.el,conn->fd, AE_READABLE | AE_WRITABLE);
        close(conn->fd);
        conn->fd = -1;
    }

    /* If called from within a handler, schedule the close but
     * keep the connection until the handler returns.
     */
    // 如果通过某个handler中来关闭的，可能handler中还会用conn，所以不能直接free掉。
    // 这里通过一个ref来标识是否可free，进入handler前计数+1，执行完handler计数-1。
    // 这样handler中判断ref肯定是大于0的，因而这里只是添加close调度标识，后面出了handler判断ref==0和标识来进行defer close。
    if (connHasRefs(conn)) {
        conn->flags |= CONN_FLAG_CLOSE_SCHEDULED;
        return;
    }

    // 如果ref为0，说明可以直接free
    zfree(conn);
}

// socket conn写方法
static int connSocketWrite(connection *conn, const void *data, size_t data_len) {
    // 调用write写数据，返回真正写的byte数。因为非阻塞操作，EAGAIN并不作为err处理。
    int ret = write(conn->fd, data, data_len);
    if (ret < 0 && errno != EAGAIN) {
        // 有err，写入last_errno
        conn->last_errno = errno;

        /* Don't overwrite the state of a connection that is not already
         * connected, not to mess with handler callbacks.
         */
        // 当conn是connected时，状态改写为error。
        // 其他状态时连接还没建立，可能还处于调用建立连接handler阶段，不应该直接标识为error。
        if (conn->state == CONN_STATE_CONNECTED)
            conn->state = CONN_STATE_ERROR;
    }

    return ret;
}

// socket conn读方法
static int connSocketRead(connection *conn, void *buf, size_t buf_len) {
    // 调用read，返回0则表示连接关闭，需要设置conn状态为close。
    int ret = read(conn->fd, buf, buf_len);
    if (!ret) {
        conn->state = CONN_STATE_CLOSED;
    } else if (ret < 0 && errno != EAGAIN) {
        conn->last_errno = errno;

        /* Don't overwrite the state of a connection that is not already
         * connected, not to mess with handler callbacks.
         */
        // 有error时，处理跟write一样，connected状态时置为error状态
        if (conn->state == CONN_STATE_CONNECTED)
            conn->state = CONN_STATE_ERROR;
    }

    return ret;
}

// socket类型conn的accept方法
static int connSocketAccept(connection *conn, ConnectionCallbackFunc accept_handler) {
    int ret = C_OK;

    // 如果conn状态不是accepting，不应该调用accept方法，返回err
    if (conn->state != CONN_STATE_ACCEPTING) return C_ERR;
    // 否则，设置conn状态为connected，并执行 connAccept 传入的accept_handler进行处理。
    conn->state = CONN_STATE_CONNECTED;

    // 为了防止conn在callHandler中过早free，这里先增加conn引用。
    connIncrRefs(conn);
    // 通过callHandler来调用accept_handler。
    if (!callHandler(conn, accept_handler)) ret = C_ERR;
    connDecrRefs(conn);

    return ret;
}

/* Register a write handler, to be called when the connection is writable.
 * If NULL, the existing handler is removed.
 *
 * The barrier flag indicates a write barrier is requested, resulting with
 * CONN_FLAG_WRITE_BARRIER set. This will ensure that the write handler is
 * always called before and not after the read handler in a single event
 * loop.
 */
// 注册write handler，当conn可写时调用该handler处理。
// barrier标识设置到conn的flag中，最终保证在一个event loop中写handler在读handler之前或不晚于读handler的执行。
static int connSocketSetWriteHandler(connection *conn, ConnectionCallbackFunc func, int barrier) {
    // 设置conn的write_handler
    if (func == conn->write_handler) return C_OK;

    conn->write_handler = func;
    // 设置barrier写屏障
    if (barrier)
        conn->flags |= CONN_FLAG_WRITE_BARRIER;
    else
        conn->flags &= ~CONN_FLAG_WRITE_BARRIER;
    // 如果conn->write_handler为NULL，即SetWriteHandler置handler为空时，删除fd的WRITABLE监听事件。
    // 否则如果有write_handler，创建WRITABLE监听事件。
    // 事件处理函数ae_handler为connSocketEventHandler。
    // 在这个EventHandler中通过callHandler调用conn->write_handler处理写事件。
    if (!conn->write_handler)
        aeDeleteFileEvent(server.el,conn->fd,AE_WRITABLE);
    else
        if (aeCreateFileEvent(server.el,conn->fd,AE_WRITABLE,
                    conn->type->ae_handler,conn) == AE_ERR) return C_ERR;
    return C_OK;
}

/* Register a read handler, to be called when the connection is readable.
 * If NULL, the existing handler is removed.
 */
// 注册read handler，在conn可读时调用这个handler
static int connSocketSetReadHandler(connection *conn, ConnectionCallbackFunc func) {
    // 设置conn的read_handler
    if (func == conn->read_handler) return C_OK;

    conn->read_handler = func;
    // 如果conn->read_handler为NULL，即SetWriteHandler置handler为空时，删除fd的READABLE监听事件。
    // 否则如果有read_handler，创建READABLE监听事件。
    // 事件处理函数ae_handler为connSocketEventHandler。
    // 在这个EventHandler中通过callHandler调用conn->read_handler处理写事件。
    if (!conn->read_handler)
        aeDeleteFileEvent(server.el,conn->fd,AE_READABLE);
    else
        if (aeCreateFileEvent(server.el,conn->fd,
                    AE_READABLE,conn->type->ae_handler,conn) == AE_ERR) return C_ERR;
    return C_OK;
}

// 获取conn中最后一个出错的error信息
static const char *connSocketGetLastError(connection *conn) {
    return strerror(conn->last_errno);
}

static void connSocketEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask)
{
    UNUSED(el);
    UNUSED(fd);
    connection *conn = clientData;

    // 如果conn状态是connecting，说明是监听建立连接的事件。
    // 描述符是可写，且有conn_handler
    if (conn->state == CONN_STATE_CONNECTING &&
            (mask & AE_WRITABLE) && conn->conn_handler) {

        // 查看并清除conn的err
        int conn_error = connGetSocketError(conn);
        if (conn_error) {
            conn->last_errno = conn_error;
            conn->state = CONN_STATE_ERROR;
        } else {
            // 没有err，说明建立连接成功。修改状态
            conn->state = CONN_STATE_CONNECTED;
        }

        // 如果没有write_handler，即不需要处理写事件。只是监听连接建立是否成功。直接删除监听的事件。
        if (!conn->write_handler) aeDeleteFileEvent(server.el,conn->fd,AE_WRITABLE);

        // 调用conn_handler，处理连接建立后的操作。
        if (!callHandler(conn, conn->conn_handler)) return;
        // 置空conn_handler
        conn->conn_handler = NULL;
    }

    /* Normally we execute the readable event first, and the writable
     * event later. This is useful as sometimes we may be able
     * to serve the reply of a query immediately after processing the
     * query.
     *
     * However if WRITE_BARRIER is set in the mask, our application is
     * asking us to do the reverse: never fire the writable event
     * after the readable. In such a case, we invert the calls.
     * This is useful when, for instance, we want to do things
     * in the beforeSleep() hook, like fsync'ing a file to disk,
     * before replying to a client. */
    // 非connecting状态的话就是已经建立好了连接，处理读写io事件。
    // 如果有写屏障标识，则需要invert事件处理顺序，即先写后读。
    int invert = conn->flags & CONN_FLAG_WRITE_BARRIER;

    int call_write = (mask & AE_WRITABLE) && conn->write_handler;
    int call_read = (mask & AE_READABLE) && conn->read_handler;

    /* Handle normal I/O flows */
    // 不需要invert，先执行read_handler
    if (!invert && call_read) {
        if (!callHandler(conn, conn->read_handler)) return;
    }
    /* Fire the writable event. */
    // 执行write_handler。显然如果有写屏障，invert为true时，前面读不会执行，从而实现先执行写。
    if (call_write) {
        if (!callHandler(conn, conn->write_handler)) return;
    }
    /* If we have to invert the call, fire the readable event now
     * after the writable one. */
    // 有写屏障，在写处理完后处理读
    if (invert && call_read) {
        if (!callHandler(conn, conn->read_handler)) return;
    }
}

// 阻塞形式创建connect
static int connSocketBlockingConnect(connection *conn, const char *addr, int port, long long timeout) {
    // NonBlock连接，默认不bind。
    int fd = anetTcpNonBlockConnect(NULL,addr,port);
    if (fd == -1) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = errno;
        return C_ERR;
    }

    // 这里阻塞等待fd可写事件触发。建立连接完成或出错fd都会触发可写。
    if ((aeWait(fd, AE_WRITABLE, timeout) & AE_WRITABLE) == 0) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = ETIMEDOUT;
    }

    // 如果连接成功，则设置fd及conn状态。
    conn->fd = fd;
    conn->state = CONN_STATE_CONNECTED;
    return C_OK;
}

/* Connection-based versions of syncio.c functions.
 * NOTE: This should ideally be refactored out in favor of pure async work.
 */
// 3个同步操作，具体实现见syncio.c
static ssize_t connSocketSyncWrite(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return syncWrite(conn->fd, ptr, size, timeout);
}

static ssize_t connSocketSyncRead(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return syncRead(conn->fd, ptr, size, timeout);
}

static ssize_t connSocketSyncReadLine(connection *conn, char *ptr, ssize_t size, long long timeout) {
    return syncReadLine(conn->fd, ptr, size, timeout);
}

// 获取当前conn type，这里时socket-type
static int connSocketGetType(connection *conn) {
    (void) conn;

    return CONN_TYPE_SOCKET;
}

// socket-type的类型方法
ConnectionType CT_Socket = {
    .ae_handler = connSocketEventHandler,
    .close = connSocketClose,
    .write = connSocketWrite,
    .read = connSocketRead,
    .accept = connSocketAccept,
    .connect = connSocketConnect,
    .set_write_handler = connSocketSetWriteHandler,
    .set_read_handler = connSocketSetReadHandler,
    .get_last_error = connSocketGetLastError,
    .blocking_connect = connSocketBlockingConnect,
    .sync_write = connSocketSyncWrite,
    .sync_read = connSocketSyncRead,
    .sync_readline = connSocketSyncReadLine,
    .get_type = connSocketGetType
};

// 获取socket error返回，getsockopt获取后会清除当前socket的error
int connGetSocketError(connection *conn) {
    int sockerr = 0;
    socklen_t errlen = sizeof(sockerr);

    if (getsockopt(conn->fd, SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1)
        sockerr = errno;
    return sockerr;
}

// 下面都是对anet底层函数的封装
// 获取对端的ip和端口。
int connPeerToString(connection *conn, char *ip, size_t ip_len, int *port) {
    return anetFdToString(conn ? conn->fd : -1, ip, ip_len, port, FD_TO_PEER_NAME);
}

// 获取自己的ip和端口
int connSockName(connection *conn, char *ip, size_t ip_len, int *port) {
    return anetFdToString(conn->fd, ip, ip_len, port, FD_TO_SOCK_NAME);
}

// 提取socket's peer或sockname的ip、port格式化到buf。
int connFormatFdAddr(connection *conn, char *buf, size_t buf_len, int fd_to_str_type) {
    return anetFormatFdAddr(conn ? conn->fd : -1, buf, buf_len, fd_to_str_type);
}

// 设置conn为block
int connBlock(connection *conn) {
    if (conn->fd == -1) return C_ERR;
    return anetBlock(NULL, conn->fd);
}

// 设置conn为非block
int connNonBlock(connection *conn) {
    if (conn->fd == -1) return C_ERR;
    return anetNonBlock(NULL, conn->fd);
}

// 禁用Nagle，实时发送，允许小包
int connEnableTcpNoDelay(connection *conn) {
    if (conn->fd == -1) return C_ERR;
    return anetEnableTcpNoDelay(NULL, conn->fd);
}

// 开始Nagle
int connDisableTcpNoDelay(connection *conn) {
    if (conn->fd == -1) return C_ERR;
    return anetDisableTcpNoDelay(NULL, conn->fd);
}

// 设置keepalive
int connKeepAlive(connection *conn, int interval) {
    if (conn->fd == -1) return C_ERR;
    return anetKeepAlive(NULL, conn->fd, interval);
}

// 设置send timeout
int connSendTimeout(connection *conn, long long ms) {
    return anetSendTimeout(NULL, conn->fd, ms);
}

// 设置recv timeout
int connRecvTimeout(connection *conn, long long ms) {
    return anetRecvTimeout(NULL, conn->fd, ms);
}

// 获取conn 状态
int connGetState(connection *conn) {
    return conn->state;
}

/* Return a text that describes the connection, suitable for inclusion
 * in CLIENT LIST and similar outputs.
 *
 * For sockets, we always return "fd=<fdnum>" to maintain compatibility.
 */
// 格式化输出conn，对于socket类型conn总是返回"fd=<fdnum>"格式。
const char *connGetInfo(connection *conn, char *buf, size_t buf_len) {
    snprintf(buf, buf_len-1, "fd=%i", conn == NULL ? -1 : conn->fd);
    return buf;
}

