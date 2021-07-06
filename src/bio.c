/* Background I/O service for Redis.
 *
 * This file implements operations that we need to perform in the background.
 * Currently there is only a single operation, that is a background close(2)
 * system call. This is needed as when the process is the last owner of a
 * reference to a file closing it means unlinking it, and the deletion of the
 * file is slow, blocking the server.
 *
 * In the future we'll either continue implementing new things we need or
 * we'll switch to libeio. However there are probably long term uses for this
 * file as we may want to put here Redis specific background tasks (for instance
 * it is not impossible that we'll need a non blocking FLUSHDB/FLUSHALL
 * implementation).
 *
 * DESIGN
 * ------
 *
 * The design is trivial, we have a structure representing a job to perform
 * and a different thread and job queue for every job type.
 * Every thread waits for new jobs in its queue, and process every job
 * sequentially.
 * 有一个job结构，不同类型job都有一个线程和一个队列。每个线程都等待对应队列中的job，并按顺序执行。
 * 每种类型的job保证按加入的时间顺序执行。
 * 目前job创建者在job执行完时不会被通知，后面如果需要可能会加上。
 *
 * Jobs of the same type are guaranteed to be processed from the least
 * recently inserted to the most recently inserted (older jobs processed
 * first).
 *
 * Currently there is no way for the creator of the job to be notified about
 * the completion of the operation, this will only be added when/if needed.
 *
 * ----------------------------------------------------------------------------
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
#include "bio.h"

static pthread_t bio_threads[BIO_NUM_OPS];  // 工作线程，每类bio一个工作线程
static pthread_mutex_t bio_mutex[BIO_NUM_OPS];  // 工作线程的锁
static pthread_cond_t bio_newjob_cond[BIO_NUM_OPS]; // 当任务队列为空，线程等待被唤醒的cond
static pthread_cond_t bio_step_cond[BIO_NUM_OPS];   // 其他线程等待某任务执行时，监听的cond以试图被唤醒
static list *bio_jobs[BIO_NUM_OPS]; // 存放工作的队列，链表形式，用于存放准备执行的任务
/* The following array is used to hold the number of pending jobs for every
 * OP type. This allows us to export the bioPendingJobsOfType() API that is
 * useful when the main thread wants to perform some operation that may involve
 * objects shared with the background thread. The main thread will just wait
 * that there are no longer jobs of this type to be executed before performing
 * the sensible operation. This data is also useful for reporting. */
// 存储每种类型的pending job数，当主线程做某些操作时可能需要等待这些job执行完。
static unsigned long long bio_pending[BIO_NUM_OPS];

/* This structure represents a background Job. It is only used locally to this
 * file as the API does not expose the internals at all. */
// Job结构，只在本文件中使用。
struct bio_job {
    // job创建时间
    time_t time; /* Time at which the job was created. */
    /* Job specific arguments.*/
    // job相关参数
    int fd; /* Fd for file based background jobs */
    // 根据传入的free方法来进行free
    lazy_free_fn *free_fn; /* Function that will free the provided arguments */
    // free方法需要的参数
    void *free_args[]; /* List of arguments to be passed to the free function */
};

void *bioProcessBackgroundJobs(void *arg);

/* Make sure we have enough stack to perform all the things we do in the
 * main thread. */
// 定义redis线程栈最小大小为4MB
#define REDIS_THREAD_STACK_SIZE (1024*1024*4)

/* Initialize the background system, spawning the thread. */
// 初始化background system，创建线程
void bioInit(void) {
    pthread_attr_t attr;
    pthread_t thread;
    size_t stacksize;
    int j;

    /* Initialization of state vars and objects */
    // 初始化前面定义的线程相关的信息
    for (j = 0; j < BIO_NUM_OPS; j++) {
        pthread_mutex_init(&bio_mutex[j],NULL);
        pthread_cond_init(&bio_newjob_cond[j],NULL);
        pthread_cond_init(&bio_step_cond[j],NULL);
        bio_jobs[j] = listCreate();
        bio_pending[j] = 0;
    }

    /* Set the stack size as by default it may be small in some system */
    // 初始化线程属性attr
    pthread_attr_init(&attr);
    // 获取系统初始默认的栈大小
    pthread_attr_getstacksize(&attr,&stacksize);
    // 如果栈太小，小于4M，则一直成倍增加，直到大于4M，并设置
    if (!stacksize) stacksize = 1; /* The world is full of Solaris Fixes */
    while (stacksize < REDIS_THREAD_STACK_SIZE) stacksize *= 2;
    pthread_attr_setstacksize(&attr, stacksize);

    /* Ready to spawn our threads. We use the single argument the thread
     * function accepts in order to pass the job ID the thread is
     * responsible of. */
    // 每种类型创建一个线程，使用前面初始化的属性attr，类型id作为参数传入线程中，每种线程只负责对应的类型job
    // 目前只有三种类型，0-close file，1-aof fsync，2-lazy free
    for (j = 0; j < BIO_NUM_OPS; j++) {
        void *arg = (void*)(unsigned long) j;
        if (pthread_create(&thread,&attr,bioProcessBackgroundJobs,arg) != 0) {
            serverLog(LL_WARNING,"Fatal: Can't initialize Background Jobs.");
            exit(1);
        }
        bio_threads[j] = thread;
    }
}

// 提交job
void bioSubmitJob(int type, struct bio_job *job) {
    job->time = time(NULL);
    // job入队时，需要加锁。加入队尾，更新等待数量+1
    pthread_mutex_lock(&bio_mutex[type]);
    listAddNodeTail(bio_jobs[type],job);
    bio_pending[type]++;
    // 新job入队，需要通知当前正在等待该类型新job的线程
    pthread_cond_signal(&bio_newjob_cond[type]);
    pthread_mutex_unlock(&bio_mutex[type]);
}

// 创建懒删除类型job
void bioCreateLazyFreeJob(lazy_free_fn free_fn, int arg_count, ...) {
    va_list valist;
    /* Allocate memory for the job structure and all required
     * arguments */
    // 为job分配内存
    struct bio_job *job = zmalloc(sizeof(*job) + sizeof(void *) * (arg_count));
    // 设置对应obj释放所需的free方法
    job->free_fn = free_fn;

    // 不同的free函数需要的参数数量不同，这里使用可变参数处理，提取后放入job的free_args中。
    va_start(valist, arg_count);
    for (int i = 0; i < arg_count; i++) {
        job->free_args[i] = va_arg(valist, void *);
    }
    va_end(valist);
    // 提交job
    bioSubmitJob(BIO_LAZY_FREE, job);
}

// 创建异步关闭fd的job
void bioCreateCloseJob(int fd) {
    struct bio_job *job = zmalloc(sizeof(*job));
    job->fd = fd;

    bioSubmitJob(BIO_CLOSE_FILE, job);
}

// 创建异步刷盘的job
void bioCreateFsyncJob(int fd) {
    struct bio_job *job = zmalloc(sizeof(*job));
    job->fd = fd;

    bioSubmitJob(BIO_AOF_FSYNC, job);
}

// 处理异步后台任务的逻辑，前面init创建的多个线程所执行的处理逻辑。
void *bioProcessBackgroundJobs(void *arg) {
    struct bio_job *job;
    unsigned long type = (unsigned long) arg;
    sigset_t sigset;

    /* Check that the type is within the right interval. */
    // 检查线程类型是否合法
    if (type >= BIO_NUM_OPS) {
        serverLog(LL_WARNING,
            "Warning: bio thread started with wrong type %lu",type);
        return NULL;
    }

    // 设置线程title
    switch (type) {
    case BIO_CLOSE_FILE:
        redis_set_thread_title("bio_close_file");
        break;
    case BIO_AOF_FSYNC:
        redis_set_thread_title("bio_aof_fsync");
        break;
    case BIO_LAZY_FREE:
        redis_set_thread_title("bio_lazy_free");
        break;
    }

    // 设置cpu亲和性
    // https://www.cnblogs.com/wenqiang/p/6049978.html
    redisSetCpuAffinity(server.bio_cpulist);

    // 设置当前线程cancel的一些条件
    // https://www.cnblogs.com/lijunamneg/archive/2013/01/25/2877211.html
    makeThreadKillable();

    // 加锁处理
    pthread_mutex_lock(&bio_mutex[type]);
    /* Block SIGALRM so we are sure that only the main thread will
     * receive the watchdog signal. */
    // 只监测SIGALRM时钟中断信号，确保其他watchdog信号只能由主线程接收
    // watchdog：https://blog.csdn.net/yhb1047818384/article/details/70833825
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGALRM);
    if (pthread_sigmask(SIG_BLOCK, &sigset, NULL))
        serverLog(LL_WARNING,
            "Warning: can't mask SIGALRM in bio.c thread: %s", strerror(errno));

    // 死循环处理事件
    while(1) {
        listNode *ln;

        /* The loop always starts with the lock hold. */
        // 循环开始时总是获取到了锁，如果当前类型没有job时，wait
        // 多线程编程中，当有多个线程等待时，如果有惊群效应，所有线程都被唤醒但只有一个线程拿到锁，所以需要用while，再判断是否拿到锁。
        // 这里continue实际上也是实现这个效果。
        if (listLength(bio_jobs[type]) == 0) {
            // wait会将当前线程阻塞在cond条件变量上（即加入到阻塞队列）并释放mutex锁，后面cond满足被唤醒时，会再加mutex锁，然后接着执行。
            // 一般pthread_cond_wait和pthread_cond_signal成对处理。
            // https://www.zhihu.com/question/24116967
            pthread_cond_wait(&bio_newjob_cond[type],&bio_mutex[type]);
            continue;
        }
        /* Pop the job from the queue. */
        // 取对头job元素
        ln = listFirst(bio_jobs[type]);
        job = ln->value;
        /* It is now possible to unlock the background system as we know have
         * a stand alone job structure to process.*/
        // 拿到一个独立的job后，处理job是独立的，可以解锁了
        pthread_mutex_unlock(&bio_mutex[type]);

        /* Process the job accordingly to its type. */
        // 处理job
        if (type == BIO_CLOSE_FILE) {
            // close file
            close(job->fd);
        } else if (type == BIO_AOF_FSYNC) {
            /* The fd may be closed by main thread and reused for another
             * socket, pipe, or file. We just ignore these errno because
             * aof fsync did not really fail. */
            // 处理fsync，忽略部分fd报错。
            if (redis_fsync(job->fd) == -1 &&
                errno != EBADF && errno != EINVAL)
            {
                int last_status;
                atomicGet(server.aof_bio_fsync_status,last_status);
                atomicSet(server.aof_bio_fsync_status,C_ERR);
                atomicSet(server.aof_bio_fsync_errno,errno);
                // 第一次出错时输出错误日志
                if (last_status == C_OK) {
                    serverLog(LL_WARNING,
                        "Fail to fsync the AOF file: %s",strerror(errno));
                }
            } else {
                // 成功的话，设置status
                atomicSet(server.aof_bio_fsync_status,C_OK);
            }
        } else if (type == BIO_LAZY_FREE) {
            // lazy free
            job->free_fn(job->free_args);
        } else {
            serverPanic("Wrong job type in bioProcessBackgroundJobs().");
        }
        // job处理完，free掉
        zfree(job);

        /* Lock again before reiterating the loop, if there are no longer
         * jobs to process we'll block again in pthread_cond_wait(). */
        // 继续加锁，保证进入循环时是锁着状态，如果没有job了，就会在循环内pthread_cond_wait中阻塞
        pthread_mutex_lock(&bio_mutex[type]);
        // 前面free job只是释放了节点的value，这里还要将list节点free掉。
        listDelNode(bio_jobs[type],ln);
        // 更新待处理job数量
        bio_pending[type]--;

        /* Unblock threads blocked on bioWaitStepOfType() if any. */
        // 通知所有blocked在bio_step_cond的线程。
        pthread_cond_broadcast(&bio_step_cond[type]);
    }
}

/* Return the number of pending jobs of the specified type. */
// 返回指定类型的pending jobs数量
unsigned long long bioPendingJobsOfType(int type) {
    unsigned long long val;
    pthread_mutex_lock(&bio_mutex[type]);
    val = bio_pending[type];
    pthread_mutex_unlock(&bio_mutex[type]);
    return val;
}

/* If there are pending jobs for the specified type, the function blocks
 * and waits that the next job was processed. Otherwise the function
 * does not block and returns ASAP.
 *
 * The function returns the number of jobs still to process of the
 * requested type.
 *
 * This function is useful when from another thread, we want to wait
 * a bio.c thread to do more work in a blocking way.
 */
// 当指定类型有pending jobs时，函数会阻塞，并等待一个job处理完后被唤醒。
// 函数返回剩余需要处理的jobs数。
// 当需要等待一个job处理完再做一些工作时，调用这个方法。目前已弃用。
unsigned long long bioWaitStepOfType(int type) {
    unsigned long long val;
    pthread_mutex_lock(&bio_mutex[type]);
    val = bio_pending[type];
    if (val != 0) {
        // 有pending jobs，进入wait
        pthread_cond_wait(&bio_step_cond[type],&bio_mutex[type]);
        val = bio_pending[type];
    }
    pthread_mutex_unlock(&bio_mutex[type]);
    return val;
}

/* Kill the running bio threads in an unclean way. This function should be
 * used only when it's critical to stop the threads for some reason.
 * Currently Redis does this only on crash (for instance on SIGSEGV) in order
 * to perform a fast memory check without other threads messing with memory. */
// 强行kill运行中的bio线程，不干净的方式，没做相关保存等操作。
// 目前只用于服务crash时，kill掉其他线程，对memory进行快速check，防止内存数据被干扰？
void bioKillThreads(void) {
    int err, j;

    for (j = 0; j < BIO_NUM_OPS; j++) {
        if (bio_threads[j] == pthread_self()) continue;
        if (bio_threads[j] && pthread_cancel(bio_threads[j]) == 0) {
            if ((err = pthread_join(bio_threads[j],NULL)) != 0) {
                serverLog(LL_WARNING,
                    "Bio thread for job type #%d can not be joined: %s",
                        j, strerror(err));
            } else {
                serverLog(LL_WARNING,
                    "Bio thread for job type #%d terminated",j);
            }
        }
    }
}
