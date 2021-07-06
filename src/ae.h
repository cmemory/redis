/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
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

#ifndef __AE_H__
#define __AE_H__

#include "monotonic.h"

#define AE_OK 0
#define AE_ERR -1

//  判断文件描述符是否可读、可写、异常影响着select。
//
// 以下的情况sokcet是可读的：
//1、socket内核接收缓存区中的字节数大于或等于其低水位SO_RCVLOWAT.读操作的字节数大于0；
//2、socket通信的对方关闭连接；对socket的读操作返回值为0；
//3、监听socket上有新的请求，需要进行accept；
//4、socket上有未处理的错误；可以使用getsockopt读取和清除该错误；

//以下的socket是可写的：
//1、socket内核发送缓冲区的可用字节数大于或等于其低水位SO_RCVLOWAT。此时可以对socket进行写操作，并且返回值为0；
//2、socket的写操作被关闭。对写操作被关闭的socket执行写操作返回一个SIGPIPE信号；
//3、socket使用非阻塞connect连接成功或者失败以后
//4、socket上有未处理的错误.可以用getsockopt消除该错误。

//select能处理的异常情况只有：socket上接收的带外数据

// 初始时默认都是AE_NONE。设置监听类型时，如果检查是AE_NONE，就是创建，否则就是更新。
#define AE_NONE 0       /* No events registered. */
#define AE_READABLE 1   /* Fire when descriptor is readable. */
#define AE_WRITABLE 2   /* Fire when descriptor is writable. */
// 默认处理是先读后写，如果是AE_BARRIER，则先写后读。
#define AE_BARRIER 4    /* With WRITABLE, never fire the event if the
                           READABLE event already fired in the same event
                           loop iteration. Useful when you want to persist
                           things to disk before sending replies, and want
                           to do that in a group fashion. */

#define AE_FILE_EVENTS (1<<0)   // 文件事件
#define AE_TIME_EVENTS (1<<1)   // 时间事件
#define AE_ALL_EVENTS (AE_FILE_EVENTS|AE_TIME_EVENTS)
#define AE_DONT_WAIT (1<<2)     // 表示检查事件触发时不wait
#define AE_CALL_BEFORE_SLEEP (1<<3) // 事件循环中 poll前执行的操作。因为poll中可能sleep
#define AE_CALL_AFTER_SLEEP (1<<4)  // 事件循环中 poll后执行的操作。因为poll中可能sleep

#define AE_NOMORE -1    // 表示时间事件后面不会再执行，可以移除监听
#define AE_DELETED_EVENT_ID -1  // 若事件类型是AE_NOMORE，则设置该时间事件id为-1，等待下一轮处理时清除。

/* Macros */
#define AE_NOTUSED(V) ((void) V)    // 表示不使用该参数或方法返回值。避免编译警告？

struct aeEventLoop;

/* Types and data structures */
// 定义函数变量的别名，注意与函数指针的区别。
typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
typedef void aeEventFinalizerProc(struct aeEventLoop *eventLoop, void *clientData);
typedef void aeBeforeSleepProc(struct aeEventLoop *eventLoop);

/* File event structure */
// 文件事件结构
typedef struct aeFileEvent {
    // 标识符mask，可读/可写/写屏障
    int mask; /* one of AE_(READABLE|WRITABLE|BARRIER) */
    // 文件读/写事件处理handler
    aeFileProc *rfileProc;
    aeFileProc *wfileProc;
    // 事件私有数据，可能是conn、client、link等。
    void *clientData;
} aeFileEvent;

/* Time event structure */
// 时间事件结构
typedef struct aeTimeEvent {
    // 时间事件id
    long long id; /* time event identifier. */
    // 什么时候执行
    monotime when;
    // 时间事件处理函数
    aeTimeProc *timeProc;
    // 事件清理函数
    aeEventFinalizerProc *finalizerProc;
    // 事件私有数据
    void *clientData;
    // 前向和后向指针。目前时间事件很少，所以使用的是链表串起来的。
    struct aeTimeEvent *prev;
    struct aeTimeEvent *next;
    // 时间事件引用计数。递归调用时防止被free。
    int refcount; /* refcount to prevent timer events from being
  		   * freed in recursive time event calls. */
} aeTimeEvent;

/* A fired event */
// 触发的事件结构，只有fd，和监听类型mask
typedef struct aeFiredEvent {
    int fd;
    int mask;
} aeFiredEvent;

/* State of an event based program */
// 基于事件轮询程序的状态。
typedef struct aeEventLoop {
    // 程序中最大的fd
    int maxfd;   /* highest file descriptor currently registered */
    // event loop所监听的最大fd数量
    int setsize; /* max number of file descriptors tracked */
    // 下一个时间事件的id，统一生成的唯一id
    long long timeEventNextId;
    // 注册的事件列表，固定数量的数组（setsize个aeFileEvent）
    // maxfd <= setsize的。数组下标就是对应的fd，方便通过fd找到对应event属性。
    aeFileEvent *events; /* Registered events */
    // 触发的事件列表，固定数量的数组（setsize个aeFiredEvent）
    // 这里不用下标映射了，aeFiredEvent中有fd字段，把触发的事件依次加入到数组中，待后面遍历处理。
    // 因为最多的情况是所有setsize个事件触发，所以分配也是setsize个事件。
    aeFiredEvent *fired; /* Fired events */
    // 时间事件的链表头
    aeTimeEvent *timeEventHead;
    // 标识event loop是否结束
    int stop;
    // 不同polling API自己的私有数据
    void *apidata; /* This is used for polling API specific data */
    // polling（sleep）前后执行的函数。可以在这里处理一些指定事情。
    aeBeforeSleepProc *beforesleep;
    aeBeforeSleepProc *aftersleep;
    // 事件flag，目前有AE_DONT_WAIT
    int flags;
} aeEventLoop;

/* Prototypes */
aeEventLoop *aeCreateEventLoop(int setsize);
void aeDeleteEventLoop(aeEventLoop *eventLoop);
void aeStop(aeEventLoop *eventLoop);
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
        aeFileProc *proc, void *clientData);
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask);
int aeGetFileEvents(aeEventLoop *eventLoop, int fd);
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc);
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id);
int aeProcessEvents(aeEventLoop *eventLoop, int flags);
int aeWait(int fd, int mask, long long milliseconds);
void aeMain(aeEventLoop *eventLoop);
char *aeGetApiName(void);
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep);
void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep);
int aeGetSetSize(aeEventLoop *eventLoop);
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize);
void aeSetDontWait(aeEventLoop *eventLoop, int noWait);

#endif
