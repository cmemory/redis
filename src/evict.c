/* Maxmemory directive handling (LRU eviction and other policies).
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2016, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "atomicvar.h"
#include <math.h>

/* ----------------------------------------------------------------------------
 * Data structures
 * --------------------------------------------------------------------------*/

/* To improve the quality of the LRU approximation we take a set of keys
 * that are good candidate for eviction across performEvictions() calls.
 *
 * Entries inside the eviction pool are taken ordered by idle time, putting
 * greater idle times to the right (ascending order).
 *
 * When an LFU policy is used instead, a reverse frequency indication is used
 * instead of the idle time, so that we still evict by larger value (larger
 * inverse frequency means to evict keys with the least frequent accesses).
 *
 * Empty entries have the key pointer set to NULL. */
// 为了优化近似LRU质量，我们通过evictionPoolPopulate()函数选取了一个很好的驱逐候选者的keys集合。
// 驱逐池中的元素都是按空闲值升序排列的。当使用LFU时，使用逆向频率作为idel值，这样我们处理时也是驱逐idel值更大的元素，使得处理方式一致。
// 空的元素的key指针设置为NULL。cached字段是预先分配的内存，避免每次处理key时都去做内存的分配与回收，提升效率。

// 驱逐池的大小，默认16个
#define EVPOOL_SIZE 16
// 预分配的缓存sds大小，默认255字节
#define EVPOOL_CACHED_SDS_SIZE 255
struct evictionPoolEntry {
    // 对象空闲值，LRU-空闲时间，LFU-逆向频率，TTL-逆向过期时间
    unsigned long long idle;    /* Object idle time (inverse frequency for LFU) */
    // 需要释放的key
    sds key;                    /* Key name. */
    // 预分配的缓存sds，用于存储key，避免频繁分配销毁sds
    sds cached;                 /* Cached SDS object for key name. */
    // key所在的db id
    int dbid;                   /* Key DB number. */
};

static struct evictionPoolEntry *EvictionPoolLRU;

/* ----------------------------------------------------------------------------
 * Implementation of eviction, aging and LRU
 * --------------------------------------------------------------------------*/

/* Return the LRU clock, based on the clock resolution. This is a time
 * in a reduced-bits format that can be used to set and check the
 * object->lru field of redisObject structures. */
// 获取LRU时钟，使用24bit表示，主要用于存储在obj的lru字段中，对象驱逐时使用。
unsigned int getLRUClock(void) {
    return (mstime()/LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX;
}

/* This function is used to obtain the current LRU clock.
 * If the current resolution is lower than the frequency we refresh the
 * LRU clock (as it should be in production servers) we return the
 * precomputed value, otherwise we need to resort to a system call. */
// 获取当前LRU时钟。如果当前resolution小于我们刷新LRU时钟的频率则从缓存中获取，否则实时获取。
// 我们更新server.lruclock都是在serverCron中，而该定时任务每1000/server.hz毫秒执行一次，即每1000/server.hz毫秒更新一次缓存lru时间。
// 根据getLRUClock中我们知道，LRU保存的时间是秒为单位的。
// 如果1000/server.hz <= LRU_CLOCK_RESOLUTION时，显然我们通过定时任务更新server.lruclock是完全够的，因为1秒内最少会更新1次。
// 而如果1000/server.hz > LRU_CLOCK_RESOLUTION时，定时任务超过1s才执行，显然更新是不及时的，所以实时获取。
unsigned int LRU_CLOCK(void) {
    unsigned int lruclock;
    if (1000/server.hz <= LRU_CLOCK_RESOLUTION) {
        atomicGet(server.lruclock,lruclock);
    } else {
        lruclock = getLRUClock();
    }
    return lruclock;
}

/* Given an object returns the min number of milliseconds the object was never
 * requested, using an approximated LRU algorithm. */
// 给定一个obj，返回最近一次访问到当前的毫秒时差，用于近似LRU算法。
unsigned long long estimateObjectIdleTime(robj *o) {
    unsigned long long lruclock = LRU_CLOCK();
    if (lruclock >= o->lru) {
        return (lruclock - o->lru) * LRU_CLOCK_RESOLUTION;
    } else {
        return (lruclock + (LRU_CLOCK_MAX - o->lru)) *
                    LRU_CLOCK_RESOLUTION;
    }
}

/* LRU approximation algorithm
 *
 * Redis uses an approximation of the LRU algorithm that runs in constant
 * memory. Every time there is a key to expire, we sample N keys (with
 * N very small, usually in around 5) to populate a pool of best keys to
 * evict of M keys (the pool size is defined by EVPOOL_SIZE).
 *
 * The N keys sampled are added in the pool of good keys to expire (the one
 * with an old access time) if they are better than one of the current keys
 * in the pool.
 *
 * After the pool is populated, the best key we have in the pool is expired.
 * However note that we don't remove keys from the pool when they are deleted
 * so the pool may contain keys that no longer exist.
 *
 * When we try to evict a key, and all the entries in the pool don't exist
 * we populate it again. This time we'll be sure that the pool has at least
 * one key that can be evicted, if there is at least one key that can be
 * evicted in the whole database. */
// LRU 近似算法。
// 使用常量内存，每次要过期一个key，我们抽样N个key（N非常小，通常为5）来填充M个（默认16）最佳驱逐的key构成的pool。
// 对于随机抽样的N个key，如果空闲值大于驱逐池中任何一个key时，说明是淘汰的更优选择，我们需要将之加入到pool中。pool元素是按idel从小到大排序的。
// 当驱逐池填充满了后，pool中idel最大的key即为最优淘汰选择。但当key删除时我们并不将key从pool中移除，所以pool中可能包含不存在的keys。
// 当我们尝试驱逐一个key，并且所有在pool中的key都不存时，我们会再次填充pool。
// 因为填充pool使用的是dictGetSomeKeys，这个函数是有可能返回数量小于我们给定数量的，所以可能返回0个数据，此时pool中key都不存在。
// 这样我们pool中没有数据，我们会一直循环处理。如果整个db有key，会保证最终pool中至少有一个key能被驱逐。

/* Create a new eviction pool. */
// 创建一个新的驱逐池，大小默认为16。
void evictionPoolAlloc(void) {
    struct evictionPoolEntry *ep;
    int j;

    ep = zmalloc(sizeof(*ep)*EVPOOL_SIZE);
    for (j = 0; j < EVPOOL_SIZE; j++) {
        ep[j].idle = 0;
        ep[j].key = NULL;
        // 每个元素都分配一个默认为255字节大小的sds作为缓存，避免使用时频繁创建和销毁sds。
        ep[j].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        // 当前key所在的db
        ep[j].dbid = 0;
    }
    EvictionPoolLRU = ep;
}

/* This is an helper function for performEvictions(), it is used in order
 * to populate the evictionPool with a few entries every time we want to
 * expire a key. Keys with idle time bigger than one of the current
 * keys are added. Keys are always added if there are free entries.
 *
 * We insert keys on place in ascending order, so keys with the smaller
 * idle time are on the left, and keys with the higher idle time on the
 * right. */
// 这个函数是performEvictions的一个辅助方法，主要用于在每次我们想过期驱逐一个key时，使用很少的entries来填充evictionPool。
// 当key的空闲时间（未被操作的持续时长）大于任何一个pool中的item时，我们会将该key加入进去。
// 另外如果evictionPool有空闲位置，那么总是加入key。
// 我们以升序的方式插入数据，所以空闲时间短的数据在前面，而空闲时间长的在后面。即排在后面的数据将会优先被淘汰。
void evictionPoolPopulate(int dbid, dict *sampledict, dict *keydict, struct evictionPoolEntry *pool) {
    int j, k, count;
    // 抽样数组，默认抽样5个元素。
    dictEntry *samples[server.maxmemory_samples];

    // 随机采样元素填充samples数组。为了采样速度，实际上是先在dict中随机找一个位置，然后取出连续的元素，并不是所有元素完全随机。
    // 这里count<=maxmemory_samples，可能采样的元素少于maxmemory_samples。具体见dictGetSomeKeys()函数说明。
    count = dictGetSomeKeys(sampledict,samples,server.maxmemory_samples);
    // 遍历采样的每个数据，判断处理是否加入驱逐池。
    for (j = 0; j < count; j++) {
        unsigned long long idle;
        sds key;
        robj *o;
        dictEntry *de;

        de = samples[j];
        key = dictGetKey(de);

        /* If the dictionary we are sampling from is not the main
         * dictionary (but the expires one) we need to lookup the key
         * again in the key dictionary to obtain the value object. */
        // 如果不是VOLATILE_TTL，显然我们需要根据obj对象里的lru字段来处理时间对比。注意我们是使用的value对象的lru来处理过期的，而不是key对象。
        // 此时如果我们采样的dict是expires，这里需要去查询数据dict，来获取dict entry，进一步再获取value对象。
        if (server.maxmemory_policy != MAXMEMORY_VOLATILE_TTL) {
            if (sampledict != keydict) de = dictFind(keydict, key);
            o = dictGetVal(de);
        }

        /* Calculate the idle time according to the policy. This is called
         * idle just because the code initially handled LRU, but is in fact
         * just a score where an higher score means better candidate. */
        // 根据不同的策略计算空闲时间。LRU实际上就是通过访问时间与当前时间计算一个差值，该值越大，说明越应该淘汰，是更好的候选者。
        if (server.maxmemory_policy & MAXMEMORY_FLAG_LRU) {
            idle = estimateObjectIdleTime(o);
        } else if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
            /* When we use an LRU policy, we sort the keys by idle time
             * so that we expire keys starting from greater idle time.
             * However when the policy is an LFU one, we have a frequency
             * estimation, and we want to evict keys with lower frequency
             * first. So inside the pool we put objects using the inverted
             * frequency subtracting the actual frequency to the maximum
             * frequency of 255. */
            // 使用LRU策略时，按空闲时间排序，较大的空闲时间优先驱逐掉。
            // 使用LFU策略时，我们需要加上频率来评估，想优先驱逐低频访问的key。
            // 所以我们用最大频率255-对象访问频率得到的值作为空闲值，该值越大说明频率越低，越应淘汰。这样加入pool的处理逻辑与LRU一致了。
            idle = 255-LFUDecrAndReturn(o);
        } else if (server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL) {
            /* In this case the sooner the expire the better. */
            // VOLATILE_TTL策略时，过期时间越久的，应被优先淘汰。
            // 空闲值计算使用最大数字-obj的过期时间数字。处理后，加入pool逻辑与前面一致。
            idle = ULLONG_MAX - (long)dictGetVal(de);
        } else {
            serverPanic("Unknown eviction policy in evictionPoolPopulate()");
        }

        /* Insert the element inside the pool.
         * First, find the first empty bucket or the first populated
         * bucket that has an idle time smaller than our idle time. */
        // 将元素加入pool，从小到大的顺序排列。
        // 先找当前key应该加入的位置，从前往后遍历，找到第一个空位，或者idle值大于当前对象的位置，即为我们要加入的位置。
        k = 0;
        while (k < EVPOOL_SIZE &&
               pool[k].key &&
               pool[k].idle < idle) k++;
        if (k == 0 && pool[EVPOOL_SIZE-1].key != NULL) {
            /* Can't insert if the element is < the worst element we have
             * and there are no empty buckets. */
            // 如果k=0，且pool满了，显然当前obj比pool中所有key的idel都小，最不应被淘汰，跳过
            continue;
        } else if (k < EVPOOL_SIZE && pool[k].key == NULL) {
            /* Inserting into empty position. No setup needed before insert. */
            // 当找到一个位置k，且该位置没有对象，则应该加入该位置。
        } else {
            /* Inserting in the middle. Now k points to the first element
             * greater than the element to insert.  */
            // 这里待插入的位置上有对象，且为第一个idel大于当前对象的位置。
            // 此时处理需要分为pool满了和没满两种情况处理。
            if (pool[EVPOOL_SIZE-1].key == NULL) {
                // 如果pool没满，则插入位置为k，需要将k以及后面的元素后移，腾出空间，没有元素丢弃。
                /* Free space on the right? Insert at k shifting
                 * all the elements from k to end to the right. */

                /* Save SDS before overwriting. */
                // 这里是把最后的sds结构移到位置k，k及之后的结构顺着后移。
                // 注意我们将要覆盖的sds结构先缓存起来，不释放，因为频繁释放创建sds也会有较大开销，这里增加缓存可以提升20%性能。
                sds cached = pool[EVPOOL_SIZE-1].cached;
                memmove(pool+k+1,pool+k,
                    sizeof(pool[0])*(EVPOOL_SIZE-k-1));
                pool[k].cached = cached;
            } else {
                /* No free space on right? Insert at k-1 */
                // pool满了，需要找一个元素丢弃。因为最左边的元素idel最小，最不应被淘汰，所以最应丢弃。
                // 此时我们需要插入位置为k-1，k-1及前面的元素要统一左移处理。
                k--;
                /* Shift all elements on the left of k (included) to the
                 * left, so we discard the element with smaller idle time. */
                // 先缓存最左元素sds，再处理左移操作。
                sds cached = pool[0].cached; /* Save SDS before overwriting. */
                // 如果key与cached指向的不是同一个地址，即表示key过大，是重新分配的空间，丢弃数据时需要对其空间进行释放。
                if (pool[0].key != pool[0].cached) sdsfree(pool[0].key);
                memmove(pool,pool+1,sizeof(pool[0])*k);
                pool[k].cached = cached;
            }
        }

        /* Try to reuse the cached SDS string allocated in the pool entry,
         * because allocating and deallocating this object is costly
         * (according to the profiler, not my fantasy. Remember:
         * premature optimization bla bla bla. */
        // 尝试重复使用缓存的sds字符串，因为分配和释放这些对象开销很多。
        int klen = sdslen(key);
        if (klen > EVPOOL_CACHED_SDS_SIZE) {
            // 如果key的长度大于255字节，缓存的sds不够，需要重新分配处理。
            // 这里pool元素的key与cache不是同一个地址。后面丢弃时需要对key空间释放。
            pool[k].key = sdsdup(key);
        } else {
            // 缓存的sds足够写下key。直接填充，不用在分配空间。
            memcpy(pool[k].cached,key,klen+1);
            sdssetlen(pool[k].cached,klen);
            // 注意这里key与cache是指向同一个地址，后面从pool中丢弃一个key时，根据这个判断是否对key空间释放。
            pool[k].key = pool[k].cached;
        }
        // 设置当前元素的空闲时间，所在db。
        pool[k].idle = idle;
        pool[k].dbid = dbid;
    }
}

/* ----------------------------------------------------------------------------
 * LFU (Least Frequently Used) implementation.

 * We have 24 total bits of space in each object in order to implement
 * an LFU (Least Frequently Used) eviction policy, since we re-use the
 * LRU field for this purpose.
 *
 * We split the 24 bits into two fields:
 *
 *          16 bits      8 bits
 *     +----------------+--------+
 *     + Last decr time | LOG_C  |
 *     +----------------+--------+
 *
 * LOG_C is a logarithmic counter that provides an indication of the access
 * frequency. However this field must also be decremented otherwise what used
 * to be a frequently accessed key in the past, will remain ranked like that
 * forever, while we want the algorithm to adapt to access pattern changes.
 *
 * So the remaining 16 bits are used in order to store the "decrement time",
 * a reduced-precision Unix time (we take 16 bits of the time converted
 * in minutes since we don't care about wrapping around) where the LOG_C
 * counter is halved if it has an high value, or just decremented if it
 * has a low value.
 *
 * New keys don't start at zero, in order to have the ability to collect
 * some accesses before being trashed away, so they start at COUNTER_INIT_VAL.
 * The logarithmic increment performed on LOG_C takes care of COUNTER_INIT_VAL
 * when incrementing the key, so that keys starting at COUNTER_INIT_VAL
 * (or having a smaller value) have a very high chance of being incremented
 * on access.
 *
 * During decrement, the value of the logarithmic counter is halved if
 * its current value is greater than two times the COUNTER_INIT_VAL, otherwise
 * it is just decremented by one.
 * --------------------------------------------------------------------------*/
// LFU实现，因为我们之前LRU是使用24bit来保存访问时间处理的，这里我们复用这24bit来实现LFU。
// 将这24bit分为 高16位（Last decr time） 和 低8位（LOG_C）。
// LOG_C是一个对数计数器表示访问频率，这个字段必须能够递减，否则它将只能表示过去访问的次数，不能随着访问模式变化而调整。
// 所以我们使用剩下的高16位来存储"last decrement time"，每次减少count的时候更新这个值。
// 当要减少count时，我们使用当前时间与上一次递减时间之间的差值，来计算需要减少的count。
// 新key的count并不是从0开始的，而是从LFU_INIT_VAL开始，主要是想能够在key丢弃时来收集一些访问信息。处理LOG_C时应小心这一点。

/* Return the current time in minutes, just taking the least significant
 * 16 bits. The returned time is suitable to be stored as LDT (last decrement
 * time) for the LFU implementation. */
// 以分钟为单位，获取当前时间，然后取最低16位。
// 返回的时间存储到obj的lru字段的的高16位，作为LDT(last decrement time)部分，用于实现LFU。
unsigned long LFUGetTimeInMinutes(void) {
    return (server.unixtime/60) & 65535;
}

/* Given an object last access time, compute the minimum number of minutes
 * that elapsed since the last access. Handle overflow (ldt greater than
 * the current 16 bits minutes time) considering the time as wrapping
 * exactly once. */
// 给定一个对象的最后访问时间，计算最后访问到现在间隔的分钟数。需要考虑溢出的情况，即ldt>current 16 bits minutes time情况。
unsigned long LFUTimeElapsed(unsigned long ldt) {
    // 获取当前时间分钟数，16bit存储
    unsigned long now = LFUGetTimeInMinutes();
    // 数据看起来正常，直接相减求差值
    if (now >= ldt) return now-ldt;
    // 处理溢出情况。
    return 65535-ldt+now;
}

/* Logarithmically increment a counter. The greater is the current counter value
 * the less likely is that it gets really implemented. Saturate it at 255. */
// 对数方式的增加一个counter，counter是存储在lru字段的低8位中，所以最大255。
uint8_t LFULogIncr(uint8_t counter) {
    // 如果达到最大，则返回255
    if (counter == 255) return 255;
    // 随机取一个double数字
    double r = (double)rand()/RAND_MAX;
    // 对于固定的lfu_log_factor，count越大，则baseval越大，p越小。
    // 当counter<=LFU_INIT_VAL时，p始终为1，当counter更大时，p会随着越来越小，
    double baseval = counter - LFU_INIT_VAL;
    if (baseval < 0) baseval = 0;
    double p = 1.0/(baseval*server.lfu_log_factor+1);
    // 这里当r<p，才counter+1。可知当counter越来越大时，想要加1概率越来越低。
    if (r < p) counter++;
    return counter;
}

/* If the object decrement time is reached decrement the LFU counter but
 * do not update LFU fields of the object, we update the access time
 * and counter in an explicit way when the object is really accessed.
 * And we will times halve the counter according to the times of
 * elapsed time than server.lfu_decay_time.
 * Return the object frequency counter.
 *
 * This function is used in order to scan the dataset for the best object
 * to fit: as we check for the candidate, we incrementally decrement the
 * counter of the scanned objects if needed. */
// 如果达到了obj的递减时间，处理LFUcounter的递减，注意这里并没有更新obj的LFU字段，我们只在该对象被访问时去更新访问时间和counter值。
// 如果有设置lfu_decay_time，则每经过这么长时间就将count-1。根据时间间隔计算需要减去的数量num_periods，最终返回现有减了之后的count。
unsigned long LFUDecrAndReturn(robj *o) {
    unsigned long ldt = o->lru >> 8;
    unsigned long counter = o->lru & 255;
    // 计算需要递减的数量
    unsigned long num_periods = server.lfu_decay_time ? LFUTimeElapsed(ldt) / server.lfu_decay_time : 0;
    if (num_periods)
        // 这里计算得到最新的count，注意减为负数的情况
        counter = (num_periods > counter) ? 0 : counter - num_periods;
    return counter;
}

/* We don't want to count AOF buffers and slaves output buffers as
 * used memory: the eviction should use mostly data size. This function
 * returns the sum of AOF and slaves buffer. */
// 我们不想将AOF/slaves缓存计入内存使用量中，因为数据驱逐应该仅保护存储数据打下。
// 这个函数返回AOF/slaves缓存的总大小
size_t freeMemoryGetNotCountedMemory(void) {
    size_t overhead = 0;
    int slaves = listLength(server.slaves);

    if (slaves) {
        listIter li;
        listNode *ln;

        // 如果有slaves，遍历server.slaves链表，累加每个slave client使用的缓存大小。
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = listNodeValue(ln);
            overhead += getClientOutputBufferMemoryUsage(slave);
        }
    }
    if (server.aof_state != AOF_OFF) {
        // 如果服务是开启AOF的，那么再加上AOF缓存以及AOF rewrite缓存
        overhead += sdsalloc(server.aof_buf)+aofRewriteBufferSize();
    }
    return overhead;
}

/* Get the memory status from the point of view of the maxmemory directive:
 * if the memory used is under the maxmemory setting then C_OK is returned.
 * Otherwise, if we are over the memory limit, the function returns
 * C_ERR.
 *
 * The function may return additional info via reference, only if the
 * pointers to the respective arguments is not NULL. Certain fields are
 * populated only when C_ERR is returned:
 *
 *  'total'     total amount of bytes used.
 *              (Populated both for C_ERR and C_OK)
 *
 *  'logical'   the amount of memory used minus the slaves/AOF buffers.
 *              (Populated when C_ERR is returned)
 *
 *  'tofree'    the amount of memory that should be released
 *              in order to return back into the memory limits.
 *              (Populated when C_ERR is returned)
 *
 *  'level'     this usually ranges from 0 to 1, and reports the amount of
 *              memory currently used. May be > 1 if we are over the memory
 *              limit.
 *              (Populated both for C_ERR and C_OK)
 */
// 从maxmemory管理的视角活的内存状态：如果内存使用在maxmemory之下则返回ok，否则如果超出了内存限制，返回err。
// 当传入参数不为NULL时，这个函数可能通过参数引用返回额外的信息。某些字段仅在返回err时填充：
// total：总的bytes使用量。在ok和err时都返回。
// logical：总的内存使用减去slaves/AOF缓存。在err时返回。
// tofree：当内存降到限制之下，需要释放的总存储量。在err时返回。
// level：通常在0-1之间，用于报告内存当前使用量。大于1表示超出了内存限制。在ok和err都返回。
int getMaxmemoryState(size_t *total, size_t *logical, size_t *tofree, float *level) {
    size_t mem_reported, mem_used, mem_tofree;

    /* Check if we are over the memory usage limit. If we are not, no need
     * to subtract the slaves output buffers. We can just return ASAP. */
    // 检查我们是否超出了内存使用限制，如果没有，不需要减去slaves/AOF缓存，并可以尽早返回。
    mem_reported = zmalloc_used_memory();
    if (total) *total = mem_reported;

    /* We may return ASAP if there is no need to compute the level. */
    // 如果没有设置最大内存限制，或者内存使用没有超限，而我们又不需要计算level时，直接返回。
    int return_ok_asap = !server.maxmemory || mem_reported <= server.maxmemory;
    if (return_ok_asap && !level) return C_OK;

    /* Remove the size of slaves output buffers and AOF buffer from the
     * count of used memory. */
    // 计算出当前AOF/slaves缓存使用量，从总内存使用量中减去。
    // 实际上，总的内存使用是计算的分配的总内存，而缓存使用内存是真实存数据用掉的，所以并没有减去一些碎片。
    mem_used = mem_reported;
    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used-overhead : 0;

    /* Compute the ratio of memory usage. */
    // 计算内存使用的比率。
    if (level) {
        if (!server.maxmemory) {
            // 没限制最大内存，则比率为0
            *level = 0;
        } else {
            // 有限制，就是 logical/server.maxmemory 值。
            *level = (float)mem_used / (float)server.maxmemory;
        }
    }

    if (return_ok_asap) return C_OK;

    /* Check if we are still over the memory limit. */
    // 如果内存使用正常，直接返回
    if (mem_used <= server.maxmemory) return C_OK;

    /* Compute how much memory we need to free. */
    // 如果需要释放内存，计算需要释放的大小。
    mem_tofree = mem_used - server.maxmemory;

    if (logical) *logical = mem_used;
    if (tofree) *tofree = mem_tofree;

    return C_ERR;
}

/* Return 1 if used memory is more than maxmemory after allocating more memory,
 * return 0 if not. Redis may reject user's requests or evict some keys if used
 * memory exceeds maxmemory, especially, when we allocate huge memory at once. */
// 通常用于在一次性要分配很大内存的时候，来判断是否能分配空间。如果分配后总的使用内存大于maxmemory，超出限制则返回1，否则返回0
// 当我们使用内存超出限制maxmemory时，redis可能会拒绝用户请求或者驱逐一些key。
int overMaxmemoryAfterAlloc(size_t moremem) {
    // 如果没有限制，直接返回0
    if (!server.maxmemory) return  0; /* No limit. */

    /* Check quickly. */
    // 快速检查。直接用当前系统使用内存做判断，如果没有超出限制，返回0
    size_t mem_used = zmalloc_used_memory();
    if (mem_used + moremem <= server.maxmemory) return 0;

    // 这里查询出AOF/slave缓存所使用内存的大小，从总当前系统使用内存大小中减去这部分，然后再判断。
    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used - overhead : 0;
    return mem_used + moremem > server.maxmemory;
}

/* The evictionTimeProc is started when "maxmemory" has been breached and
 * could not immediately be resolved.  This will spin the event loop with short
 * eviction cycles until the "maxmemory" condition has resolved or there are no
 * more evictable items.  */
// evictionTimeProc用于在事件循环的定时任务中执行。
// 在performEvictions中，如果驱逐处理超过了一定时间，但仍然没有处理内存使用超限的问题，我们就会创建这个定时任务来在事件循环中再处理。
static int isEvictionProcRunning = 0;
static int evictionTimeProc(
        struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    // 这里会调用performEvictions来做驱逐操作，如果执行完后还需要再次调度该任务，则返回0
    if (performEvictions() == EVICT_RUNNING) return 0;  /* keep evicting */

    /* For EVICT_OK - things are good, no need to keep evicting.
     * For EVICT_FAIL - there is nothing left to evict.  */
    // 返回EVICT_OK，表示驱逐完成，不需要任务继续。返回EVICT_FAIL，没有数据需要驱逐，也不需要再处理。
    // 最终返回AE_NOMORE，表示定时任务不需要再继续。
    isEvictionProcRunning = 0;
    return AE_NOMORE;
}

/* Check if it's safe to perform evictions.
 *   Returns 1 if evictions can be performed
 *   Returns 0 if eviction processing should be skipped
 */
// 检查当前是否安全执行数据驱逐。返回1表示能执行，0表示需要跳过驱逐。
static int isSafeToPerformEvictions(void) {
    /* - There must be no script in timeout condition.
     * - Nor we are loading data right now.  */
    // 当执行lua脚本超时期间 或者 目前在loading数据时，不能执行驱逐。
    if (server.lua_timedout || server.loading) return 0;

    /* By default replicas should ignore maxmemory
     * and just be masters exact copies. */
    // 如果当前时slave，且忽略maxmemory，不做驱逐。
    // 默认slave应该不主动处理驱逐，而只是master的副本数据。
    if (server.masterhost && server.repl_slave_ignore_maxmemory) return 0;

    /* When clients are paused the dataset should be static not just from the
     * POV of clients not being able to write, but also from the POV of
     * expires and evictions of keys not being performed. */
    // 当clients处于暂停状态，不仅在clients的视角，数据应该是静态的不能被写入；而在过期或驱逐的视角，keys也不能喝被处理。
    if (checkClientPauseTimeoutAndReturnIfPaused()) return 0;

    return 1;
}

/* Algorithm for converting tenacity (0-100) to a time limit.  */
// 算法将0-100强度转换为一个时间限制
static unsigned long evictionTimeLimitUs() {
    // 驱逐强度应该在0-100之间
    serverAssert(server.maxmemory_eviction_tenacity >= 0);
    serverAssert(server.maxmemory_eviction_tenacity <= 100);

    if (server.maxmemory_eviction_tenacity <= 10) {
        /* A linear progression from 0..500us */
        // 强度小于10时，线性增加
        return 50uL * server.maxmemory_eviction_tenacity;
    }

    if (server.maxmemory_eviction_tenacity < 100) {
        // 15%的几何增加，使得当强度=99时，结果限制在大约2分钟左右。
        /* A 15% geometric progression, resulting in a limit of ~2 min at tenacity==99  */
        return (unsigned long)(500.0 * pow(1.15, server.maxmemory_eviction_tenacity - 10.0));
    }

    return ULONG_MAX;   /* No limit to eviction time */
}

/* Check that memory usage is within the current "maxmemory" limit.  If over
 * "maxmemory", attempt to free memory by evicting data (if it's safe to do so).
 *
 * It's possible for Redis to suddenly be significantly over the "maxmemory"
 * setting.  This can happen if there is a large allocation (like a hash table
 * resize) or even if the "maxmemory" setting is manually adjusted.  Because of
 * this, it's important to evict for a managed period of time - otherwise Redis
 * would become unresponsive while evicting.
 *
 * The goal of this function is to improve the memory situation - not to
 * immediately resolve it.  In the case that some items have been evicted but
 * the "maxmemory" limit has not been achieved, an aeTimeProc will be started
 * which will continue to evict items until memory limits are achieved or
 * nothing more is evictable.
 *
 * This should be called before execution of commands.  If EVICT_FAIL is
 * returned, commands which will result in increased memory usage should be
 * rejected.
 *
 * Returns:
 *   EVICT_OK       - memory is OK or it's not possible to perform evictions now
 *   EVICT_RUNNING  - memory is over the limit, but eviction is still processing
 *   EVICT_FAIL     - memory is over the limit, and there's nothing to evict
 * */
// 检查内存使用是否在maxmemory限制以内，如果超出了则尝试驱逐数据释放内存。
// redis可能突然显著超出最大内存限制，如在一个大的内存分配（rehash）或者手动调整maxmemory时。
// 所以定时的去检查驱逐数据是很重要的，否则redis存储超限时，可能每次处理数据都要做驱逐操作，从而导致响应很慢。
// 这个函数的目的是改善内存情况，而不是立即解决问题。
// 当一些item被驱逐，但还是超过内存限制时，会启动aeTimeProc，将继续驱逐items直到达到限制之下，或者没有数据可驱逐。
// 这个函数应该在执行命令前被执行。如果返回EVICT_FAIL，而待执行命令会增加内存，那么命令会被拒绝执行。
// 函数返回值：
//   EVICT_OK，内存使用是ok的，或者目前不需要进行驱逐操作。
//   EVICT_RUNNING，内存使用超限，但驱逐数据操作正在进行。
//   EVICT_FAIL，内存使用超限，没办法做数据驱逐
int performEvictions(void) {
    // 如果当前状态不能做驱逐操作，返回ok
    if (!isSafeToPerformEvictions()) return EVICT_OK;

    int keys_freed = 0;
    size_t mem_reported, mem_tofree;
    long long mem_freed; /* May be negative */
    mstime_t latency, eviction_latency;
    long long delta;
    int slaves = listLength(server.slaves);
    int result = EVICT_FAIL;

    // 获取当前最大内存状态，如果是ok的，则返回ok。
    if (getMaxmemoryState(&mem_reported,NULL,&mem_tofree,NULL) == C_OK)
        return EVICT_OK;

    // 内存满了需要释放，但策略是不驱逐，这里返回fail
    if (server.maxmemory_policy == MAXMEMORY_NO_EVICTION)
        return EVICT_FAIL;  /* We need to free memory, but policy forbids. */

    unsigned long eviction_time_limit_us = evictionTimeLimitUs();

    mem_freed = 0;

    // 延时监控
    latencyStartMonitor(latency);

    // 使用相对时间来计算，使得执行时间限制更准确
    monotime evictionTimer;
    elapsedStart(&evictionTimer);

    // 一直循环，直到释放的内存量达到需求
    while (mem_freed < (long long)mem_tofree) {
        int j, k, i;
        static unsigned int next_db = 0;
        sds bestkey = NULL;
        int bestdbid;
        redisDb *db;
        dict *dict;
        dictEntry *de;

        if (server.maxmemory_policy & (MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_LFU) ||
            server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL)
        {
            // 如果策略是LRU或LFU或VOLATILE_TTL，需要找寻一个bestkey释放掉。
            struct evictionPoolEntry *pool = EvictionPoolLRU;

            while(bestkey == NULL) {
                // 注意如果一直获取不到bestkey，可能是因为我们驱逐池生成中并没有数据，这里会一直遍历直到有拿到数据为止。
                // 因为所有db中肯定是有数据的，total_keys=0时，我们会退出循环。
                unsigned long total_keys = 0, keys;

                /* We don't want to make local-db choices when expiring keys,
                 * so to start populate the eviction pool sampling keys from
                 * every DB. */
                // 我们不想在过期key时，再做db的选择，所以填充驱逐池时从所有DB抽样。
                for (i = 0; i < server.dbnum; i++) {
                    // 获取采样db
                    db = server.db+i;
                    // 如果策略是ALLKEYS，则从db->dict挑选，否则从db->expires中挑选。
                    // 如果是VOLATILE_TTL，那么不可能是ALLKEYS，所以从db->expires中挑选，逻辑上是一致的。
                    dict = (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) ?
                            db->dict : db->expires;
                    if ((keys = dictSize(dict)) != 0) {
                        // 如果当前选择的dict有数据，则根据规则从其中选出一些数据来填充驱逐池。
                        evictionPoolPopulate(i, dict, db->dict, pool);
                        // 计数所有db中的key数量
                        total_keys += keys;
                    }
                }
                // 如果检查完了，所有db都没有key，没办法驱逐，会走到cant_free。
                if (!total_keys) break; /* No keys to evict. */

                /* Go backward from best to worst element to evict. */
                // 从后往前遍历，后面的元素是最优的驱逐对象。
                for (k = EVPOOL_SIZE-1; k >= 0; k--) {
                    // 如果key为NULL，说明当前元素无效，要么是已经驱逐过了，要么是未填充满，跳过即可。
                    if (pool[k].key == NULL) continue;
                    bestdbid = pool[k].dbid;

                    // 如果策略是ALLKEYS，则从对应数据字典中取出entry，否则从过期时间字典中取出entry。
                    if (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) {
                        de = dictFind(server.db[pool[k].dbid].dict,
                            pool[k].key);
                    } else {
                        de = dictFind(server.db[pool[k].dbid].expires,
                            pool[k].key);
                    }

                    /* Remove the entry from the pool. */
                    // 取到了DB的数据，我们需要将驱逐池对应的数据剔除出去。
                    if (pool[k].key != pool[k].cached)
                        // 当key不是用的缓存sds时，需要手动释放空间
                        sdsfree(pool[k].key);
                    // 当前位置key已处理，需要清空属性
                    pool[k].key = NULL;
                    pool[k].idle = 0;

                    /* If the key exists, is our pick. Otherwise it is
                     * a ghost and we need to try the next element. */
                    // 如果获取的key在数据库中存在，则就是我们需要驱逐的对象。
                    if (de) {
                        // 拿到bestkey，跳出循环。
                        bestkey = dictGetKey(de);
                        break;
                    } else {
                        // 不应该出现不存在的情况，此时的话我们再找下一个。
                        /* Ghost... Iterate again. */
                    }
                }
            }
        }

        /* volatile-random and allkeys-random policy */
        else if (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM ||
                 server.maxmemory_policy == MAXMEMORY_VOLATILE_RANDOM)
        {
            /* When evicting a random key, we try to evict a key for
             * each DB, so we use the static 'next_db' variable to
             * incrementally visit all DBs. */
            // 随机选择key进行驱逐
            for (i = 0; i < server.dbnum; i++) {
                // 每次操作的db是轮训的。next_db是静态遍历，第一次进入循环时初始化，后续递增。
                j = (++next_db) % server.dbnum;
                db = server.db+j;
                // 根据策略选择dict或expires库
                dict = (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM) ?
                        db->dict : db->expires;
                // 如果dict中有数据，则随机选择一个作为bestkey来处理
                if (dictSize(dict) != 0) {
                    de = dictGetRandomKey(dict);
                    bestkey = dictGetKey(de);
                    bestdbid = j;
                    break;
                }
            }
        }

        /* Finally remove the selected key. */
        // 如果有最优驱逐对象。则需要处理
        if (bestkey) {
            // 获取指向对应db的指针
            db = server.db+bestdbid;
            // 使用该key创建string对象，并想AOF/slave传播该key过期的指令。
            robj *keyobj = createStringObject(bestkey,sdslen(bestkey));
            propagateExpire(db,keyobj,server.lazyfree_lazy_eviction);
            /* We compute the amount of memory freed by db*Delete() alone.
             * It is possible that actually the memory needed to propagate
             * the DEL in AOF and replication link is greater than the one
             * we are freeing removing the key, but we can't account for
             * that otherwise we would never exit the loop.
             *
             * Same for CSC invalidation messages generated by signalModifiedKey.
             *
             * AOF and Output buffer memory will be freed eventually so
             * we only care about memory used by the key space. */
            // 这里单独计算delete释放的内存。
            // 因为可能我们用于传播DEL到AOF/slaves时所需要的内存，比我们删除key释放的内存更多，如果不单独计算，可能我们就跳不出循环了。
            // 后面signalModifiedKey生成CSC无效消息也是一样的，不计算内存。
            // AOF和output buf使用的内存最终传输完成后将被释放，所以我们只用关心key空间所使用的内存。
            delta = (long long) zmalloc_used_memory();
            // 用于监控操作耗时
            latencyStartMonitor(eviction_latency);
            if (server.lazyfree_lazy_eviction)
                // 开启驱逐异步处理，这里会计算清除key需要做的操作次数，如果大于默认值64时，采用异步删除。
                dbAsyncDelete(db,keyobj);
            else
                // 同步删除key，可能大key会阻塞
                dbSyncDelete(db,keyobj);
            latencyEndMonitor(eviction_latency);
            // 监控耗时加入抽样统计
            latencyAddSampleIfNeeded("eviction-del",eviction_latency);
            // 计算删除释放的空间
            delta -= (long long) zmalloc_used_memory();
            // 计算总的释放空间，如果释放空间还是不足，会继续循环进入下一轮驱逐。
            mem_freed += delta;
            server.stat_evictedkeys++;
            // key有变更，要通知其他监控key的地方。如watch、track相关逻辑。
            signalModifiedKey(NULL,db,keyobj);
            // 要通知通过pub/sub监听keyspace事件的cleint
            notifyKeyspaceEvent(NOTIFY_EVICTED, "evicted",
                keyobj, db->id);
            // 释放前面创建的string对象keyobj
            decrRefCount(keyobj);
            keys_freed++;

            // 每当一次驱逐操作，释放掉的key达到16个时，做一些操作。
            if (keys_freed % 16 == 0) {
                /* When the memory to free starts to be big enough, we may
                 * start spending so much time here that is impossible to
                 * deliver data to the replicas fast enough, so we force the
                 * transmission here inside the loop. */
                // 当需要释放的内存很大时，我们可能会耗费很长的时间处理驱逐，从而无法尽快的传输数据给slaves。
                // 所以这里在处理驱逐的循环内，强制进行传输操作。
                if (slaves) flushSlavesOutputBuffers();

                /* Normally our stop condition is the ability to release
                 * a fixed, pre-computed amount of memory. However when we
                 * are deleting objects in another thread, it's better to
                 * check, from time to time, if we already reached our target
                 * memory, since the "mem_freed" amount is computed only
                 * across the dbAsyncDelete() call, while the thread can
                 * release the memory all the time. */
                // 通常我们在释放固定、预计算的内存容量后，停止循环处理。
                // 但是当我们异步处理删除对象时，我们需要每隔一定时间来检查下，看存储是否已经达到了预期。
                // 因为我们在计算mem_freed数量时，可能内存还没释放，计算数据偏小。而后台处理线程一直在释放内存空间。
                if (server.lazyfree_lazy_eviction) {
                    // 允许异步驱逐，我们需要检查内存状态，如果是ok的，则不必再驱逐了
                    if (getMaxmemoryState(NULL,NULL,NULL,NULL) == C_OK) {
                        break;
                    }
                }

                /* After some time, exit the loop early - even if memory limit
                 * hasn't been reached.  If we suddenly need to free a lot of
                 * memory, don't want to spend too much time here.  */
                // 不能因为驱逐数据操作耗费太长时间，否则影响redis单线程效率。
                // 所以这里我们使用之前根据驱逐强度转换的时间来进行限制。当超过了预定的时间（最大2min左右），则退出驱逐处理。
                if (elapsedUs(evictionTimer) > eviction_time_limit_us) {
                    // We still need to free memory - start eviction timer proc
                    // 虽然我们到时间要退出了，但是还有内存需要清理，这里我们创建定时任务来在事件循环中处理。
                    // 当然如果定时任务已经存在了，那么我们就直接跳出循环就好了。
                    if (!isEvictionProcRunning) {
                        isEvictionProcRunning = 1;
                        aeCreateTimeEvent(server.el, 0,
                                evictionTimeProc, NULL, NULL);
                    }
                    break;
                }
            }
        } else {
            // 可能没有找到要驱逐的key，没办法释放。
            goto cant_free; /* nothing to free... */
        }
    }
    /* at this point, the memory is OK, or we have reached the time limit */
    // 执行到这里，要么内存使用是ok的，要么我们达到了事件限制。
    result = (isEvictionProcRunning) ? EVICT_RUNNING : EVICT_OK;

cant_free:
    if (result == EVICT_FAIL) {
        /* At this point, we have run out of evictable items.  It's possible
         * that some items are being freed in the lazyfree thread.  Perform a
         * short wait here if such jobs exist, but don't wait long.  */
        // 这里如果是EVICT_FAIL，表示没找到item去删除。
        // 可能异步有在处理，这里我们看异步释放对象的线程队列中有没有任务，如果有任务，我们在这里等一小会儿，然后检查下内存。
        if (bioPendingJobsOfType(BIO_LAZY_FREE)) {
            usleep(eviction_time_limit_us);
            if (getMaxmemoryState(NULL,NULL,NULL,NULL) == C_OK) {
                result = EVICT_OK;
            }
        }
    }

    // 处理时间采样监控
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("eviction-cycle",latency);
    return result;
}

