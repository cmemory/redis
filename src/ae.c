/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include "ae.h"
#include "anet.h"

#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#include "zmalloc.h"
#include "config.h"

/* Include the best multiplexing layer supported by this system.
 * The following should be ordered by performances, descending. */
#ifdef HAVE_EVPORT
#include "ae_evport.c"
#else
    #ifdef HAVE_EPOLL
    #include "ae_epoll.c"
    #else
        #ifdef HAVE_KQUEUE
        #include "ae_kqueue.c"
        #else
        #include "ae_select.c"
        #endif
    #endif
#endif

// 创建evnet loop
aeEventLoop *aeCreateEventLoop(int setsize) {
    aeEventLoop *eventLoop;
    int i;

    // 使用monotonic time计算event loop时间间隔，更高效准确
    monotonicInit();    /* just in case the calling app didn't initialize */

    // 分配存储
    if ((eventLoop = zmalloc(sizeof(*eventLoop))) == NULL) goto err;
    eventLoop->events = zmalloc(sizeof(aeFileEvent)*setsize);
    eventLoop->fired = zmalloc(sizeof(aeFiredEvent)*setsize);
    if (eventLoop->events == NULL || eventLoop->fired == NULL) goto err;
    // 初始化各种属性
    eventLoop->setsize = setsize;
    // 时间时间head设置为NULL，有时间事件加入时会单独处理这里head。
    eventLoop->timeEventHead = NULL;
    eventLoop->timeEventNextId = 0;
    eventLoop->stop = 0;
    eventLoop->maxfd = -1;
    // polling sleep前后的执行函数需要的话会单独设置。
    eventLoop->beforesleep = NULL;
    eventLoop->aftersleep = NULL;
    eventLoop->flags = 0;
    // eventLoop所使用的底层IO多路复用库 的 私有数据初始化。
    // 数据放在apidata中。包含底层管理者的fd，以及底层管理的各种event数据。
    // apidata中的event是内核的事件，注意不要跟eventLoop中event混了。
    if (aeApiCreate(eventLoop) == -1) goto err;
    /* Events with mask == AE_NONE are not set. So let's initialize the
     * vector with it. */
    // 初始化eventLoop所有event的mask为AE_NONE。
    // 因为events数组包含所有的事件，后面可以直接通过fd找到对应事件，通过事件mask值是否为AE_NONE判断是否设置了监听类型。
    for (i = 0; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return eventLoop;

err:
    // err后释放前面分配的内存。
    if (eventLoop) {
        zfree(eventLoop->events);
        zfree(eventLoop->fired);
        zfree(eventLoop);
    }
    return NULL;
}

/* Return the current set size. */
// 获取当前event loop管理的事件集合大小。
int aeGetSetSize(aeEventLoop *eventLoop) {
    return eventLoop->setsize;
}

/* Tells the next iteration/s of the event processing to set timeout of 0. */
// 设置下一次迭代时poll是sleep等待还是立即返回。
void aeSetDontWait(aeEventLoop *eventLoop, int noWait) {
    if (noWait)
        eventLoop->flags |= AE_DONT_WAIT;
    else
        eventLoop->flags &= ~AE_DONT_WAIT;
}

/* Resize the maximum set size of the event loop.
 * If the requested set size is smaller than the current set size, but
 * there is already a file descriptor in use that is >= the requested
 * set size minus one, AE_ERR is returned and the operation is not
 * performed at all.
 *
 * Otherwise AE_OK is returned and the operation is successful. */
// 重新设置event loop管理的最大事件集合大小。
// 如果新设置的newsize小于当前集合大小setsize，但是当前有一个正在使用的fd>=newsize时，显然无法进行缩容操作，会返回err
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize) {
    int i;

    if (setsize == eventLoop->setsize) return AE_OK;
    // 校验最大fd>=setsize是否成立
    if (eventLoop->maxfd >= setsize) return AE_ERR;
    // 底层io api管理的事件集合也需要resize容量
    if (aeApiResize(eventLoop,setsize) == -1) return AE_ERR;

    // resize eventLoop 的 events 和 fired容量。
    eventLoop->events = zrealloc(eventLoop->events,sizeof(aeFileEvent)*setsize);
    eventLoop->fired = zrealloc(eventLoop->fired,sizeof(aeFiredEvent)*setsize);
    eventLoop->setsize = setsize;

    /* Make sure that if we created new slots, they are initialized with
     * an AE_NONE mask. */
    // 如果是扩容，需要对于新增event的mask 设置 AE_NONE
    for (i = eventLoop->maxfd+1; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return AE_OK;
}

// 删除event loop，对于堆上手动分配的内存，这里需要手动释放。
void aeDeleteEventLoop(aeEventLoop *eventLoop) {
    // 底层apidata free
    aeApiFree(eventLoop);
    // free eventLoop 的 events 和 fired
    zfree(eventLoop->events);
    zfree(eventLoop->fired);

    /* Free the time events list. */
    // 释放时间事件，list遍历挨个释放。
    aeTimeEvent *next_te, *te = eventLoop->timeEventHead;
    while (te) {
        next_te = te->next;
        zfree(te);
        te = next_te;
    }
    // 释放event loop本身
    zfree(eventLoop);
}

// 设置stop标识，下一轮循环时候判断退出
void aeStop(aeEventLoop *eventLoop) {
    eventLoop->stop = 1;
}

// 创建文件事件
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
        aeFileProc *proc, void *clientData)
{
    // 如果fd >= event loop 的 setsize，显然装不下这个fd了，返回error
    if (fd >= eventLoop->setsize) {
        errno = ERANGE;
        return AE_ERR;
    }
    // 获取events数组中fd对应的事件
    aeFileEvent *fe = &eventLoop->events[fd];

    // 按传入的mask调用底层api，将该fd加入对应的事件监听类型
    if (aeApiAddEvent(eventLoop, fd, mask) == -1)
        return AE_ERR;
    // 设置eventLoop中event的mask，便于后面polling取到对应触发事件后，再次进行检查监听事件。
    fe->mask |= mask;
    // 针对监听的可读/写，设置对应的读/写处理函数。
    if (mask & AE_READABLE) fe->rfileProc = proc;
    if (mask & AE_WRITABLE) fe->wfileProc = proc;
    // 设置event的私有数据，如连接的conn，通信的client，集群的link等。
    fe->clientData = clientData;
    if (fd > eventLoop->maxfd)
        // 更新当前所在使用的最大fd
        eventLoop->maxfd = fd;
    return AE_OK;
}

// 删除某个fd的监听事件
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask)
{
    // fd大于setsize时，显然不合法，直接返回
    if (fd >= eventLoop->setsize) return;
    // 取出对应fd的event，检查mask如果是AE_NONE，说明没有被监听，不需要删除，直接返回。
    aeFileEvent *fe = &eventLoop->events[fd];
    if (fe->mask == AE_NONE) return;

    /* We want to always remove AE_BARRIER if set when AE_WRITABLE
     * is removed. */
    // 当删除WRITABLE监听事件时，如果有BARRIER标识，总是一起删除。
    if (mask & AE_WRITABLE) mask |= AE_BARRIER;

    // 调用底层api移除事件的监听
    aeApiDelEvent(eventLoop, fd, mask);
    // 更新eventLoop中fd对应event的mask
    fe->mask = fe->mask & (~mask);
    // 如果移除的fd时当前在使用的最大fd，并且更新后的该fd事件mask还原到AE_NONE。那么需要更新maxfd。
    // 从maxfd-1往下遍历，找出第一个在监听使用(即mask != AE_NONE)的fd，赋值给maxfd。
    // 虽然遍历，但一般很快就可以找到。在LFU中也可以使用一个变量来标识最小使用次数。
    if (fd == eventLoop->maxfd && fe->mask == AE_NONE) {
        /* Update the max fd */
        int j;

        for (j = eventLoop->maxfd-1; j >= 0; j--)
            if (eventLoop->events[j].mask != AE_NONE) break;
        eventLoop->maxfd = j;
    }
}

// 获取指定fd对应的event监听事件类型。为AE_NONE 0 时表示没使用监听。
int aeGetFileEvents(aeEventLoop *eventLoop, int fd) {
    if (fd >= eventLoop->setsize) return 0;
    aeFileEvent *fe = &eventLoop->events[fd];

    return fe->mask;
}

// 创建时间事件。返回事件id
// milliseconds表示多久后执行。proc是执行handler。
// finalizerProc 清理handler 和 clientData 目前都没使用。
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc)
{
    // 取得even loop全局时间事件自增ID
    long long id = eventLoop->timeEventNextId++;
    aeTimeEvent *te;

    te = zmalloc(sizeof(*te));
    if (te == NULL) return AE_ERR;
    te->id = id;
    // 设置执行时间，当前Monotonic时间加milliseconds。Monotonic时间，并不是当前真实时间。
    te->when = getMonotonicUs() + milliseconds * 1000;
    te->timeProc = proc;
    te->finalizerProc = finalizerProc;
    te->clientData = clientData;
    te->prev = NULL;
    // 当前事件加入时间事件队列，并更新eventLoop->timeEventHead。头插法加入链表。
    te->next = eventLoop->timeEventHead;
    te->refcount = 0;
    if (te->next)
        te->next->prev = te;
    eventLoop->timeEventHead = te;
    return id;
}

// 根据id删除事件时间
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id)
{
    // 遍历链表，对比id，找到对应事件。
    // 这里不是真的删除，只是将该id标记为AE_DELETED_EVENT_ID，等待时间事件循环中做链表删除处理。
    aeTimeEvent *te = eventLoop->timeEventHead;
    while(te) {
        if (te->id == id) {
            te->id = AE_DELETED_EVENT_ID;
            return AE_OK;
        }
        te = te->next;
    }
    return AE_ERR; /* NO event with the specified ID found */
}

/* How many microseconds until the first timer should fire.
 * If there are no timers, -1 is returned.
 *
 * Note that's O(N) since time events are unsorted.
 * Possible optimizations (not needed by Redis so far, but...):
 * 1) Insert the event in order, so that the nearest is just the head.
 *    Much better but still insertion or deletion of timers is O(N).
 * 2) Use a skiplist to have this operation as O(1) and insertion as O(log(N)).
 */
// 计算最早的时间事件还需要多少微秒。没有时间事件则返回-1。
// 因为时间事件很少，所以没有排序，需要遍历链表挨个计算处理，O(n)复杂度，不过不影响效率
static int64_t usUntilEarliestTimer(aeEventLoop *eventLoop) {
    aeTimeEvent *te = eventLoop->timeEventHead;
    if (te == NULL) return -1;

    // earliest 标记最早要执行的时间事件。遍历更新，最终找出最早执行的事件。
    aeTimeEvent *earliest = NULL;
    while (te) {
        if (!earliest || te->when < earliest->when)
            earliest = te;
        te = te->next;
    }

    // 计算最早执行的时间事件还需要的微秒数。
    monotime now = getMonotonicUs();
    return (now >= earliest->when) ? 0 : earliest->when - now;
}

/* Process time events */
// 处理时间事件
static int processTimeEvents(aeEventLoop *eventLoop) {
    int processed = 0;
    aeTimeEvent *te;
    long long maxId;

    te = eventLoop->timeEventHead;
    maxId = eventLoop->timeEventNextId-1;
    monotime now = getMonotonicUs();
    // 依次遍历时间事件链表节点进行处理
    while(te) {
        long long id;

        /* Remove events scheduled for deletion. */
        // 先判断当前处理节点是否标记删除，是则处理删除逻辑。
        if (te->id == AE_DELETED_EVENT_ID) {
            aeTimeEvent *next = te->next;
            /* If a reference exists for this timer event,
             * don't free it. This is currently incremented
             * for recursive timerProc calls */
            // 如果当前事件refcount>0，可能是递归调用事件处理proc。
            // 暂时不能删除，也不需要执行proc处理逻辑。跳到下一个节点处理。
            if (te->refcount) {
                te = next;
                continue;
            }
            // 删除逻辑
            // 有prev节点则处理prev->next。没有则删除的是头节点，需要将头指针指向下一个节点。
            if (te->prev)
                te->prev->next = te->next;
            else
                eventLoop->timeEventHead = te->next;
            // 有next节点则处理next->prev。
            if (te->next)
                te->next->prev = te->prev;
            // 如果需要清理clientData，则需要设置finalizerProc清理函数，这里执行。
            if (te->finalizerProc) {
                te->finalizerProc(eventLoop, te->clientData);
                // 更新now时间，是考虑到finalizerProc可能执行时间较长，所以放这个if里？
                now = getMonotonicUs();
            }
            zfree(te);
            te = next;
            continue;
        }

        /* Make sure we don't process time events created by time events in
         * this iteration. Note that this check is currently useless: we always
         * add new timers on the head, however if we change the implementation
         * detail, this check may be useful again: we keep it here for future
         * defense. */
        // 循环遍历前设置处理的最大id，从而限制在遍历中不会处理被时间事件创建的新时间事件。
        // 因为时间事件都是头插入链表，而遍历是从头向后遍历，所以这里的检测是无用的。
        // 但是如果未来改变实现方式，如sort排序处理，就需要检测了。因而这里保留检测。
        if (te->id > maxId) {
            te = te->next;
            continue;
        }

        // 如果事件的执行时间小于当前时间，说明该执行了，进入处理逻辑。
        if (te->when <= now) {
            int retval;

            id = te->id;
            // 处理前增加refcount，防止timeProc递归调用中free调当前时间事件。
            te->refcount++;
            // 执行时间事件处理函数，返回-1表示不再执行，返回>0表示下一次执行时间间隔。
            retval = te->timeProc(eventLoop, id, te->clientData);
            te->refcount--;
            processed++;
            now = getMonotonicUs();
            // 如果后面还执行，则设置新的执行时间。否则标记该时间事件删除，等待后面再处理时间事件的时候删除。
            if (retval != AE_NOMORE) {
                te->when = now + retval * 1000;
            } else {
                te->id = AE_DELETED_EVENT_ID;
            }
        }
        te = te->next;
    }
    return processed;
}

/* Process every pending time event, then every pending file event
 * (that may be registered by time event callbacks just processed).
 * Without special flags the function sleeps until some file event
 * fires, or when the next time event occurs (if any).
 *
 * If flags is 0, the function does nothing and returns.
 * if flags has AE_ALL_EVENTS set, all the kind of events are processed.
 * if flags has AE_FILE_EVENTS set, file events are processed.
 * if flags has AE_TIME_EVENTS set, time events are processed.
 * if flags has AE_DONT_WAIT set the function returns ASAP until all
 * the events that's possible to process without to wait are processed.
 * if flags has AE_CALL_AFTER_SLEEP set, the aftersleep callback is called.
 * if flags has AE_CALL_BEFORE_SLEEP set, the beforesleep callback is called.
 *
 * The function returns the number of events processed. */
// eventLoop的核心执行函数
int aeProcessEvents(aeEventLoop *eventLoop, int flags)
{
    int processed = 0, numevents;

    /* Nothing to do? return ASAP */
    // 不执行时间时间，也不执行文件事件，直接返回。这个flag有什么用？
    if (!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS)) return 0;

    /* Note that we want to call select() even if there are no
     * file events to process as long as we want to process time
     * events, in order to sleep until the next time event is ready
     * to fire. */
    // eventLoop->maxfd != -1 说明有file event，需要select检查。
    // 即使没有file event，如果要处理时间事件，且select不是立即返回，那么就需要sleep直到time event发生。
    // 巧妙的借助select的timeout来进行时间事件的sleep阻塞。
    if (eventLoop->maxfd != -1 ||
        ((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT))) {
        int j;
        struct timeval tv, *tvp;
        int64_t usUntilTimer = -1;

        // 需要处理time event，且不是立即返回，查询最近的时间事件还要多少微秒
        if (flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT))
            usUntilTimer = usUntilEarliestTimer(eventLoop);
        // 如果大于0，计算需要sleep的时间
        if (usUntilTimer >= 0) {
            tv.tv_sec = usUntilTimer / 1000000;
            tv.tv_usec = usUntilTimer % 1000000;
            tvp = &tv;
        } else {
            /* If we have to check for events but need to return
             * ASAP because of AE_DONT_WAIT we need to set the timeout
             * to zero */
            // 没有时间事件，但flag是不等待，则tvp中等待时间都置为0，select的时候立即返回。
            if (flags & AE_DONT_WAIT) {
                tv.tv_sec = tv.tv_usec = 0;
                tvp = &tv;
            } else {
                // 没有有时间事件，且没有不等待标识，则select一直等待文件事件
                /* Otherwise we can block */
                tvp = NULL; /* wait forever */
            }
        }

        // 前面处理的是函数传入的flags，这里处理eventLoop全局的flag。
        // eventLoop中flags 标识不需要等待时，tvp中等待时间都置为0
        if (eventLoop->flags & AE_DONT_WAIT) {
            tv.tv_sec = tv.tv_usec = 0;
            tvp = &tv;
        }

        // select前，执行beforesleep相关操作
        if (eventLoop->beforesleep != NULL && flags & AE_CALL_BEFORE_SLEEP)
            eventLoop->beforesleep(eventLoop);

        /* Call the multiplexing API, will return only on timeout or when
         * some event fires. */
        // 不同平台选择最优的IO多路复用API，要么有事件触发返回，要么等待时间到了返回。
        // tvp == NULL 表示阻塞等待
        numevents = aeApiPoll(eventLoop, tvp);

        /* After sleep callback. */
        // select返回后，执行aftersleep相关操作
        if (eventLoop->aftersleep != NULL && flags & AE_CALL_AFTER_SLEEP)
            eventLoop->aftersleep(eventLoop);

        // 对于select返回的触发的事件依次进行处理，相当与遍历fired中保存的触发事件。
        for (j = 0; j < numevents; j++) {
            // 取出fired中触发事件，根据触发事件的fd找到eventLoop->events中的原始事件。
            aeFileEvent *fe = &eventLoop->events[eventLoop->fired[j].fd];
            int mask = eventLoop->fired[j].mask;
            int fd = eventLoop->fired[j].fd;
            int fired = 0; /* Number of events fired for current fd. */

            /* Normally we execute the readable event first, and the writable
             * event later. This is useful as sometimes we may be able
             * to serve the reply of a query immediately after processing the
             * query.
             * fd可读可写时，我们通常先处理读，再处理写。
             * 先读数据处理后，因为立即可写，所以后面处理写事件触发时能立即写入回复。
             *
             * 增加了AE_BARRIER，相反的方式处理：优先处理可写，再处理可读。
             * 后面处理可读，读完之后会处理数据，此时因为写已经处理了，所以当前loop不会写回复给client，
             * 下一轮之前可以在beforeSleep中做一些事情，如同步数据到disk，这样可以保证先持久化，再在写事件中写入回复client。
             *
             * However if AE_BARRIER is set in the mask, our application is
             * asking us to do the reverse: never fire the writable event
             * after the readable. In such a case, we invert the calls.
             * This is useful when, for instance, we want to do things
             * in the beforeSleep() hook, like fsyncing a file to disk,
             * before replying to a client. */
            int invert = fe->mask & AE_BARRIER;

            /* Note the "fe->mask & mask & ..." code: maybe an already
             * processed event removed an element that fired and we still
             * didn't processed, so we check if the event is still valid.
             *
             * Fire the readable event if the call sequence is not
             * inverted. */
            // fe->mask 是原本event预设的可触发的类型。
            // mask 是实际触发的类型。
            // fe->mask & mask & ... 可能前面事件处理移除了fe->mask的部分属性，所以这样判断不用处理已经移除的事件。
            // 当不是AE_BARRIER，且读触发，则先执行读处理
            // 如果设置AE_BARRIER，则这里跳过读，等写处理完了再处理读。
            if (!invert && fe->mask & mask & AE_READABLE) {
                fe->rfileProc(eventLoop,fd,fe->clientData,mask);
                fired++;
                // eventLoop resize 可能导致fe指向地址释放了导致空指针？
                // 这里重新刷新下fe
                fe = &eventLoop->events[fd]; /* Refresh in case of resize. */
            }

            /* Fire the writable event. */
            // 可写触发
            // 如果前面处理了可读，且读写处理函数一样，那么就不用再执行写处理了。
            // fired && fe->wfileProc == fe->rfileProc，跳过写处理。取反则处理写。
            if (fe->mask & mask & AE_WRITABLE) {
                if (!fired || fe->wfileProc != fe->rfileProc) {
                    fe->wfileProc(eventLoop,fd,fe->clientData,mask);
                    fired++;
                }
            }

            /* If we have to invert the call, fire the readable event now
             * after the writable one. */
            // 如果设置AE_BARRIER，到这里写处理已经完成了，可以处理读了。
            if (invert) {
                // 这里重新刷新下fe
                fe = &eventLoop->events[fd]; /* Refresh in case of resize. */
                // 这里逻辑与前面处理写基本一致
                if ((fe->mask & mask & AE_READABLE) &&
                    (!fired || fe->wfileProc != fe->rfileProc))
                {
                    fe->rfileProc(eventLoop,fd,fe->clientData,mask);
                    fired++;
                }
            }

            processed++;
        }
    }
    /* Check time events */
    // 处理时间事件
    if (flags & AE_TIME_EVENTS)
        processed += processTimeEvents(eventLoop);

    return processed; /* return the number of processed file/time events */
}

/* Wait for milliseconds until the given file descriptor becomes
 * writable/readable/exception */
// 阻塞等待指定fd的指定mask事件触发。超时时间milliseconds。
int aeWait(int fd, int mask, long long milliseconds) {
    struct pollfd pfd;
    int retmask = 0, retval;

    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = fd;
    // 设置监听事件类型
    if (mask & AE_READABLE) pfd.events |= POLLIN;
    if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

    // poll等待，第一个参数表示需要监听的pollfd结构数组（监听多个fd），第二个参数是监听的fd个数。
    // 返回值>0：数组fds中准备好读、写或出错状态的那些socket描述符的总数量；
    // 返回值=0：表示已达到超时时间，且没有任何fd准备好或出错。
    // 返回值<0：poll函数调用失败，同时会自动设置全局变量errno？
    // 当超时时间为timeout==0，那么就立即返回，不管有没有fd准备好或出错。
    // 如果超时时间timeout==INFTIM，那么poll()函数会一直阻塞下去，直到有事件触发。
    if ((retval = poll(&pfd, 1, milliseconds))== 1) {
        if (pfd.revents & POLLIN) retmask |= AE_READABLE;
        if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLERR) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLHUP) retmask |= AE_WRITABLE;
        // 如果有事件触发，返回触发的事件类型mask
        return retmask;
    } else {
        // poll会调用失败返回-1？外面要判断？
        return retval;
    }
}

// 事件处理Main函数
void aeMain(aeEventLoop *eventLoop) {
    eventLoop->stop = 0;
    // 如果不stop就一直循环处理，这里的aeProcessEvents调用会处理所有事件，以及sleep前后处理函数。
    while (!eventLoop->stop) {
        aeProcessEvents(eventLoop, AE_ALL_EVENTS|
                                   AE_CALL_BEFORE_SLEEP|
                                   AE_CALL_AFTER_SLEEP);
    }
}

// 获取底层处理多路复用io的库name
char *aeGetApiName(void) {
    return aeApiName();
}

// 设置beforesleep处理函数
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep) {
    eventLoop->beforesleep = beforesleep;
}

// 设置aftersleep处理函数
void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep) {
    eventLoop->aftersleep = aftersleep;
}
