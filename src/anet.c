/* anet.c -- Basic TCP socket stuff made a bit less boring
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <netdb.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>

#include "anet.h"

static void anetSetError(char *err, const char *fmt, ...)
{
    va_list ap;

    if (!err) return;
    va_start(ap, fmt);
    vsnprintf(err, ANET_ERR_LEN, fmt, ap);
    va_end(ap);
}

// 添加释放block标识
int anetSetBlock(char *err, int fd, int non_block) {
    int flags;

    /* Set the socket blocking (if non_block is zero) or non-blocking.
     * Note that fcntl(2) for F_GETFL and F_SETFL can't be
     * interrupted by a signal. */
    // 获取fd上的flags
    if ((flags = fcntl(fd, F_GETFL)) == -1) {
        anetSetError(err, "fcntl(F_GETFL): %s", strerror(errno));
        return ANET_ERR;
    }

    /* Check if this flag has been set or unset, if so, 
     * then there is no need to call fcntl to set/unset it again. */
    // 检查对应标识是否与想要设置的一致
    if (!!(flags & O_NONBLOCK) == !!non_block)
        return ANET_OK;

    // 根据non_block，来为fd设置对应的flag
    if (non_block)
        flags |= O_NONBLOCK;
    else
        flags &= ~O_NONBLOCK;

    // 设置fd的flags
    if (fcntl(fd, F_SETFL, flags) == -1) {
        anetSetError(err, "fcntl(F_SETFL,O_NONBLOCK): %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

int anetNonBlock(char *err, int fd) {
    return anetSetBlock(err,fd,1);
}

int anetBlock(char *err, int fd) {
    return anetSetBlock(err,fd,0);
}

/* Enable the FD_CLOEXEC on the given fd to avoid fd leaks. 
 * This function should be invoked for fd's on specific places 
 * where fork + execve system calls are called. */
// fork时，子进程与父进程一样的地址空间。
// 子进程执行execve时会替换新的地址空间信息，之前指向父进程的文件描述符就不存在了，但也无法关闭。
// 所以这里对于这样的fd加上FD_CLOEXEC标识，子进程执行exec会自动关闭该fd。
int anetCloexec(int fd) {
    int r;
    int flags;

    do {
        r = fcntl(fd, F_GETFD);
    } while (r == -1 && errno == EINTR);

    if (r == -1 || (r & FD_CLOEXEC))
        return r;

    flags = r | FD_CLOEXEC;

    do {
        r = fcntl(fd, F_SETFD, flags);
    } while (r == -1 && errno == EINTR);

    return r;
}

/* Set TCP keep alive option to detect dead peers. The interval option
 * is only used for Linux as we are using Linux-specific APIs to set
 * the probe send time, interval, and count. */
// 设置TCP的keep alive属性，来检测dead peers
int anetKeepAlive(char *err, int fd, int interval)
{
    int val = 1;

    // 设置socket的SO_KEEPALIVE为1
    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val)) == -1)
    {
        anetSetError(err, "setsockopt SO_KEEPALIVE: %s", strerror(errno));
        return ANET_ERR;
    }

#ifdef __linux__
    /* Default settings are more or less garbage, with the keepalive time
     * set to 7200 by default on Linux. Modify settings to make the feature
     * actually useful. */

    /* Send first probe after interval. */
    // net.ipv4.tcp_keepalive_time = 7200
    // net.ipv4.tcp_keepalive_probes = 9
    // net.ipv4.tcp_keepalive_intvl = 75
    // linux默认keepalive有上面三个选项，通过设置TCP_KEEPIDLE、TCP_KEEPCNT、TCP_KEEPINTVL来改变这些默认值。
    val = interval;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &val, sizeof(val)) < 0) {
        anetSetError(err, "setsockopt TCP_KEEPIDLE: %s\n", strerror(errno));
        return ANET_ERR;
    }

    /* Send next probes after the specified interval. Note that we set the
     * delay as interval / 3, as we send three probes before detecting
     * an error (see the next setsockopt call). */
    val = interval/3;
    if (val == 0) val = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof(val)) < 0) {
        anetSetError(err, "setsockopt TCP_KEEPINTVL: %s\n", strerror(errno));
        return ANET_ERR;
    }

    /* Consider the socket in error state after three we send three ACK
     * probes without getting a reply. */
    val = 3;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &val, sizeof(val)) < 0) {
        anetSetError(err, "setsockopt TCP_KEEPCNT: %s\n", strerror(errno));
        return ANET_ERR;
    }
#else
    ((void) interval); /* Avoid unused var warning for non Linux systems. */
#endif

    return ANET_OK;
}

static int anetSetTcpNoDelay(char *err, int fd, int val)
{
    // TCP/IP协议中对TCP默认开启Nagle算法，缓存数据包，等达到一定数量或时间后一起发送。减少小包的发送，优化网络。
    // 设置TCP_NODELAY，禁止延迟发送，来禁用Nagle算法。所有包会立即发送，即允许小包。
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) == -1)
    {
        anetSetError(err, "setsockopt TCP_NODELAY: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

int anetEnableTcpNoDelay(char *err, int fd)
{
    return anetSetTcpNoDelay(err, fd, 1);
}

int anetDisableTcpNoDelay(char *err, int fd)
{
    return anetSetTcpNoDelay(err, fd, 0);
}

/* Set the socket send timeout (SO_SNDTIMEO socket option) to the specified
 * number of milliseconds, or disable it if the 'ms' argument is zero. */
// 设置socket send timeout
int anetSendTimeout(char *err, int fd, long long ms) {
    struct timeval tv;

    tv.tv_sec = ms/1000;
    tv.tv_usec = (ms%1000)*1000;
    if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == -1) {
        anetSetError(err, "setsockopt SO_SNDTIMEO: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

/* Set the socket receive timeout (SO_RCVTIMEO socket option) to the specified
 * number of milliseconds, or disable it if the 'ms' argument is zero. */
// 设置socket receive timeout
int anetRecvTimeout(char *err, int fd, long long ms) {
    struct timeval tv;

    tv.tv_sec = ms/1000;
    tv.tv_usec = (ms%1000)*1000;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == -1) {
        anetSetError(err, "setsockopt SO_RCVTIMEO: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

/* Resolve the hostname "host" and set the string representation of the
 * IP address into the buffer pointed by "ipbuf".
 *
 * If flags is set to ANET_IP_ONLY the function only resolves hostnames
 * that are actually already IPv4 or IPv6 addresses. This turns the function
 * into a validating / normalizing function. */
// 解析host中的ip放在ipbuf中
int anetResolve(char *err, char *host, char *ipbuf, size_t ipbuf_len,
                       int flags)
{
    struct addrinfo hints, *info;
    int rv;

    memset(&hints,0,sizeof(hints));
    if (flags & ANET_IP_ONLY) hints.ai_flags = AI_NUMERICHOST;  // host就是ip，只做校验标准化
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;  /* specify socktype to avoid dups */

    // hints参数表示期望返回的数据信息，如ai_socktype设置为SOCK_STREAM表示返回TCP数据信息
    if ((rv = getaddrinfo(host, NULL, &hints, &info)) != 0) {
        anetSetError(err, "%s", gai_strerror(rv));
        return ANET_ERR;
    }
    // ipv4
    if (info->ai_family == AF_INET) {
        struct sockaddr_in *sa = (struct sockaddr_in *)info->ai_addr;
        // p和n分别代表表达（presentation)和数值（numeric)，
        // inet_pton和inet_ntop互相转换，新的方法，ipv4和ipv6都支持。
        // inet_aton和inet_ntoa是老的转换方法。
        inet_ntop(AF_INET, &(sa->sin_addr), ipbuf, ipbuf_len);
    } else {
        struct sockaddr_in6 *sa = (struct sockaddr_in6 *)info->ai_addr;
        inet_ntop(AF_INET6, &(sa->sin6_addr), ipbuf, ipbuf_len);
    }

    freeaddrinfo(info);
    return ANET_OK;
}

static int anetSetReuseAddr(char *err, int fd) {
    int yes = 1;
    /* Make sure connection-intensive things like the redis benchmark
     * will be able to close/open sockets a zillion of times */
    // 设置socket reuse，防止频繁建立、断开连接出现过多time-wait，导致无法再建立新的连接。
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
        anetSetError(err, "setsockopt SO_REUSEADDR: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

static int anetCreateSocket(char *err, int domain) {
    int s;
    // 创建socket
    if ((s = socket(domain, SOCK_STREAM, 0)) == -1) {
        anetSetError(err, "creating socket: %s", strerror(errno));
        return ANET_ERR;
    }

    /* Make sure connection-intensive things like the redis benchmark
     * will be able to close/open sockets a zillion of times */
    // 设置socket reuse
    if (anetSetReuseAddr(err,s) == ANET_ERR) {
        close(s);
        return ANET_ERR;
    }
    return s;
}

#define ANET_CONNECT_NONE 0
#define ANET_CONNECT_NONBLOCK 1
#define ANET_CONNECT_BE_BINDING 2 /* Best effort binding. */
// addr 目标节点地址，port 目标节点端口，source_addr本地源地址。
static int anetTcpGenericConnect(char *err, const char *addr, int port,
                                 const char *source_addr, int flags)
{
    int s = ANET_ERR, rv;
    char portstr[6];  /* strlen("65535") + 1; */
    struct addrinfo hints, *servinfo, *bservinfo, *p, *b;

    // 格式化字符串放在portstr中
    snprintf(portstr,sizeof(portstr),"%d",port);
    memset(&hints,0,sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    // 按hints指示，从目标addr,portstr中获取addrinfo放在servinfo中，servinfo是一个addrinfo链表
    if ((rv = getaddrinfo(addr,portstr,&hints,&servinfo)) != 0) {
        anetSetError(err, "%s", gai_strerror(rv));
        return ANET_ERR;
    }
    for (p = servinfo; p != NULL; p = p->ai_next) {
        /* Try to create the socket and to connect it.
         * If we fail in the socket() call, or on connect(), we retry with
         * the next entry in servinfo. */
        // 从servinfo中依次取出addr信息，创建socket。这里使用的是对端地址解析出来的协议？
        if ((s = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1)
            continue;

        // 设置reuse属性
        if (anetSetReuseAddr(err,s) == ANET_ERR) goto error;
        // 设置no blcok属性
        if (flags & ANET_CONNECT_NONBLOCK && anetNonBlock(err,s) != ANET_OK)
            goto error;
        if (source_addr) {
            int bound = 0;
            /* Using getaddrinfo saves us from self-determining IPv4 vs IPv6 */
            // 获取需要bind的addr info
            if ((rv = getaddrinfo(source_addr, NULL, &hints, &bservinfo)) != 0)
            {
                anetSetError(err, "%s", gai_strerror(rv));
                goto error;
            }
            for (b = bservinfo; b != NULL; b = b->ai_next) {
                // 从bservinfo中找一个addr来bind
                if (bind(s,b->ai_addr,b->ai_addrlen) != -1) {
                    bound = 1;
                    break;
                }
            }
            freeaddrinfo(bservinfo);
            if (!bound) {
                anetSetError(err, "bind: %s", strerror(errno));
                goto error;
            }
        }
        // 与远端地址建立连接
        if (connect(s,p->ai_addr,p->ai_addrlen) == -1) {
            /* If the socket is non-blocking, it is ok for connect() to
             * return an EINPROGRESS error here. */
            // 非阻塞连接，会立即返回EINPROGRESS error
            if (errno == EINPROGRESS && flags & ANET_CONNECT_NONBLOCK)
                goto end;
            close(s);
            s = ANET_ERR;
            continue;
        }

        /* If we ended an iteration of the for loop without errors, we
         * have a connected socket. Let's return to the caller. */
        goto end;
    }
    if (p == NULL)
        anetSetError(err, "creating socket: %s", strerror(errno));

error:
    if (s != ANET_ERR) {
        close(s);
        s = ANET_ERR;
    }

end:
    freeaddrinfo(servinfo);

    /* Handle best effort binding: if a binding address was used, but it is
     * not possible to create a socket, try again without a binding address. */
    // 尽最大努力binding，如果bind失败，就不绑定再试一次。
    // 一般主动连接方是客户端的话，其实不需要bind。但对于集群，可能互为客户/服务端，需要bind地址端口，相互好识别进行通信。
    if (s == ANET_ERR && source_addr && (flags & ANET_CONNECT_BE_BINDING)) {
        return anetTcpGenericConnect(err,addr,port,NULL,flags);
    } else {
        return s;
    }
}

int anetTcpNonBlockConnect(char *err, const char *addr, int port)
{
    return anetTcpGenericConnect(err,addr,port,NULL,ANET_CONNECT_NONBLOCK);
}

int anetTcpNonBlockBestEffortBindConnect(char *err, const char *addr, int port,
                                         const char *source_addr)
{
    return anetTcpGenericConnect(err,addr,port,source_addr,
            ANET_CONNECT_NONBLOCK|ANET_CONNECT_BE_BINDING);
}

int anetUnixGenericConnect(char *err, const char *path, int flags)
{
    int s;
    struct sockaddr_un sa;

    // 建立本地进程socket通信
    if ((s = anetCreateSocket(err,AF_LOCAL)) == ANET_ERR)
        return ANET_ERR;

    sa.sun_family = AF_LOCAL;
    strncpy(sa.sun_path,path,sizeof(sa.sun_path)-1);
    // 设置nonblock标识
    if (flags & ANET_CONNECT_NONBLOCK) {
        if (anetNonBlock(err,s) != ANET_OK) {
            close(s);
            return ANET_ERR;
        }
    }
    // 建立连接
    if (connect(s,(struct sockaddr*)&sa,sizeof(sa)) == -1) {
        if (errno == EINPROGRESS &&
            flags & ANET_CONNECT_NONBLOCK)
            return s;

        anetSetError(err, "connect: %s", strerror(errno));
        close(s);
        return ANET_ERR;
    }
    return s;
}

// 先bind，再listen；backlog是accept队列长度
// syn队列 -- 存放SYN RECEIVED状态的套接字；accept队列 -- 存放ESTABLISHED状态的套接字
static int anetListen(char *err, int s, struct sockaddr *sa, socklen_t len, int backlog) {
    // bind绑定地址端口
    if (bind(s,sa,len) == -1) {
        anetSetError(err, "bind: %s", strerror(errno));
        close(s);
        return ANET_ERR;
    }

    // listen监听socket
    if (listen(s, backlog) == -1) {
        anetSetError(err, "listen: %s", strerror(errno));
        close(s);
        return ANET_ERR;
    }
    return ANET_OK;
}

// 设置ipv6 only
static int anetV6Only(char *err, int s) {
    int yes = 1;
    if (setsockopt(s,IPPROTO_IPV6,IPV6_V6ONLY,&yes,sizeof(yes)) == -1) {
        anetSetError(err, "setsockopt: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

// 启动TCPServer，与anetTcpGenericConnect相比有listen。
static int _anetTcpServer(char *err, int port, char *bindaddr, int af, int backlog)
{
    int s = -1, rv;
    char _port[6];  /* strlen("65535") */
    struct addrinfo hints, *servinfo, *p;

    snprintf(_port,6,"%d",port);
    memset(&hints,0,sizeof(hints));
    hints.ai_family = af;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;    /* No effect if bindaddr != NULL */
    // 根据传入参数bindaddr判断，是默认值的话表示没有提供绑定地址，设置bindaddr=NULL
    if (bindaddr && !strcmp("*", bindaddr))
        bindaddr = NULL;
    if (af == AF_INET6 && bindaddr && !strcmp("::*", bindaddr))
        bindaddr = NULL;

    // getaddrinfo，从bindaddr和_port中获取地址信息存储到servinfo中，servinfo是一个addrinfo类型链表。
    // _port：可以是十进制的端口号，也可以是已定义的服务名称，如ftp、http等
    // hints：可以是一个空指针，也可以是一个指向某个addrinfo结构体的指针，填入期望返回的信息类型。
    if ((rv = getaddrinfo(bindaddr,_port,&hints,&servinfo)) != 0) {
        anetSetError(err, "%s", gai_strerror(rv));
        return ANET_ERR;
    }
    // 遍历解析的地址，进行处理
    for (p = servinfo; p != NULL; p = p->ai_next) {
        // 创建socket，3个参数如下：
        // 地址族（Address Family），即IP地址类型，常用的有 AF_INET 和 AF_INET6
        // 数据传输方式/套接字类型，常用的有SOCK_STREAM（流格式套接字/面向连接的套接字）和SOCK_DGRAM（数据报套接字/无连接的套接字）
        // 传输协议，常用的有 IPPROTO_TCP 和 IPPTOTO_UDP
        if ((s = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1)
            continue;

        if (af == AF_INET6 && anetV6Only(err,s) == ANET_ERR) goto error;
        // 设置socket reuse属性
        if (anetSetReuseAddr(err,s) == ANET_ERR) goto error;
        // 对socket进行listen监听，backlog是listen accept队列
        if (anetListen(err,s,p->ai_addr,p->ai_addrlen,backlog) == ANET_ERR) s = ANET_ERR;
        goto end;
    }
    if (p == NULL) {
        anetSetError(err, "unable to bind socket, errno: %d", errno);
        goto error;
    }

error:
    if (s != -1) close(s);
    s = ANET_ERR;
end:
    freeaddrinfo(servinfo);
    return s;
}

int anetTcpServer(char *err, int port, char *bindaddr, int backlog)
{
    return _anetTcpServer(err, port, bindaddr, AF_INET, backlog);
}

int anetTcp6Server(char *err, int port, char *bindaddr, int backlog)
{
    return _anetTcpServer(err, port, bindaddr, AF_INET6, backlog);
}

// 启动UnixServer
int anetUnixServer(char *err, char *path, mode_t perm, int backlog)
{
    int s;
    struct sockaddr_un sa;

    // 创建本地unix域socket
    if ((s = anetCreateSocket(err,AF_LOCAL)) == ANET_ERR)
        return ANET_ERR;

    memset(&sa,0,sizeof(sa));
    sa.sun_family = AF_LOCAL;
    strncpy(sa.sun_path,path,sizeof(sa.sun_path)-1);
    // bind and listen监听
    if (anetListen(err,s,(struct sockaddr*)&sa,sizeof(sa),backlog) == ANET_ERR)
        return ANET_ERR;
    if (perm)
        chmod(sa.sun_path, perm);
    return s;
}

// 开启accept阻塞
static int anetGenericAccept(char *err, int s, struct sockaddr *sa, socklen_t *len) {
    int fd;
    while(1) {
        // accept()，s为当前监听的socket，sa返回获取到的对端ip信息。返回的fd为新创建的文件描述符，用于与对端进行收发消息。
        fd = accept(s,sa,len);
        if (fd == -1) {
            if (errno == EINTR)
                continue;
            else {
                anetSetError(err, "accept: %s", strerror(errno));
                return ANET_ERR;
            }
        }
        break;
    }
    return fd;
}

int anetTcpAccept(char *err, int s, char *ip, size_t ip_len, int *port) {
    int fd;
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);
    if ((fd = anetGenericAccept(err,s,(struct sockaddr*)&sa,&salen)) == -1)
        return ANET_ERR;

    if (sa.ss_family == AF_INET) {
        struct sockaddr_in *s = (struct sockaddr_in *)&sa;
        if (ip) inet_ntop(AF_INET,(void*)&(s->sin_addr),ip,ip_len);
        if (port) *port = ntohs(s->sin_port);
    } else {
        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&sa;
        if (ip) inet_ntop(AF_INET6,(void*)&(s->sin6_addr),ip,ip_len);
        if (port) *port = ntohs(s->sin6_port);
    }
    return fd;
}

int anetUnixAccept(char *err, int s) {
    int fd;
    struct sockaddr_un sa;
    socklen_t salen = sizeof(sa);
    if ((fd = anetGenericAccept(err,s,(struct sockaddr*)&sa,&salen)) == -1)
        return ANET_ERR;

    return fd;
}

int anetFdToString(int fd, char *ip, size_t ip_len, int *port, int fd_to_str_type) {
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);

    if (fd_to_str_type == FD_TO_PEER_NAME) {
        // getpeername 获取连接对端的socket地址信息；getsockname 获取本地的socker地址信息
        if (getpeername(fd, (struct sockaddr *)&sa, &salen) == -1) goto error;
    } else {
        if (getsockname(fd, (struct sockaddr *)&sa, &salen) == -1) goto error;
    }
    if (ip_len == 0) goto error;

    if (sa.ss_family == AF_INET) {
        struct sockaddr_in *s = (struct sockaddr_in *)&sa;
        if (ip) inet_ntop(AF_INET,(void*)&(s->sin_addr),ip,ip_len);
        // ntohs 网络字节顺序转换为本地主机字节顺序，大端小端？
        if (port) *port = ntohs(s->sin_port);
    } else if (sa.ss_family == AF_INET6) {
        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&sa;
        if (ip) inet_ntop(AF_INET6,(void*)&(s->sin6_addr),ip,ip_len);
        if (port) *port = ntohs(s->sin6_port);
    } else if (sa.ss_family == AF_UNIX) {
        if (ip) snprintf(ip, ip_len, "/unixsocket");
        if (port) *port = 0;
    } else {
        goto error;
    }
    return 0;

error:
    if (ip) {
        if (ip_len >= 2) {
            ip[0] = '?';
            ip[1] = '\0';
        } else if (ip_len == 1) {
            ip[0] = '\0';
        }
    }
    if (port) *port = 0;
    return -1;
}

/* Format an IP,port pair into something easy to parse. If IP is IPv6
 * (matches for ":"), the ip is surrounded by []. IP and port are just
 * separated by colons. This the standard to display addresses within Redis. */
// 格式化ip，port信息到buf
int anetFormatAddr(char *buf, size_t buf_len, char *ip, int port) {
    return snprintf(buf,buf_len, strchr(ip,':') ?
           "[%s]:%d" : "%s:%d", ip, port);
}

/* Like anetFormatAddr() but extract ip and port from the socket's peer/sockname. */
// 提取socket's peer或sockname的ip、port格式化到buf。
int anetFormatFdAddr(int fd, char *buf, size_t buf_len, int fd_to_str_type) {
    char ip[INET6_ADDRSTRLEN];
    int port;

    anetFdToString(fd,ip,sizeof(ip),&port,fd_to_str_type);
    return anetFormatAddr(buf, buf_len, ip, port);
}
