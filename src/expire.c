/* Implementation of EXPIRE (keys with fixed time to live).
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

/*-----------------------------------------------------------------------------
 * Incremental collection of expired keys.
 *
 * When keys are accessed they are expired on-access. However we need a
 * mechanism in order to ensure keys are eventually removed when expired even
 * if no access is performed on them.
 *----------------------------------------------------------------------------*/

// 渐进式的keys过期收集处理
// 正常我们key在访问时会判断过期处理。但是当如果key一直不访问，我们也要有一种机制来保证key能被进行过期处理。

/* Helper function for the activeExpireCycle() function.
 * This function will try to expire the key that is stored in the hash table
 * entry 'de' of the 'expires' hash table of a Redis database.
 *
 * If the key is found to be expired, it is removed from the database and
 * 1 is returned. Otherwise no operation is performed and 0 is returned.
 *
 * When a key is expired, server.stat_expiredkeys is incremented.
 *
 * The parameter 'now' is the current time in milliseconds as is passed
 * to the function to avoid too many gettimeofday() syscalls. */
// activeExpireCycle()函数的辅助方法。这个函数尝试进行过期key的处理。
// db是存数据的dict，de是expire dict的元素。now作为参数传入主要是避免过多使用gettimeofday()系统调用来获取当前时间。
// 如果key被找到且过期了，我们需要将该key移除，返回1，另外还会增加server.stat_expiredkeys统计量。如果没有过期，不需要操作，返回0。
int activeExpireCycleTryExpire(redisDb *db, dictEntry *de, long long now) {
    // 取出expire元素的过期时间。
    long long t = dictGetSignedIntegerVal(de);
    mstime_t expire_latency;
    // 该key已过期，需要清除
    if (now > t) {
        // 构建key对象
        sds key = dictGetKey(de);
        robj *keyobj = createStringObject(key,sdslen(key));

        // 过期的删除的操作向AOF/slaves进行传播
        propagateExpire(db,keyobj,server.lazyfree_lazy_expire);
        // 操作延时监控
        latencyStartMonitor(expire_latency);
        if (server.lazyfree_lazy_expire)
            dbAsyncDelete(db,keyobj);
        else
            dbSyncDelete(db,keyobj);
        latencyEndMonitor(expire_latency);
        latencyAddSampleIfNeeded("expire-del",expire_latency);
        // 通知keyspace变动事件
        notifyKeyspaceEvent(NOTIFY_EXPIRED,
            "expired",keyobj,db->id);
        // key变更，需要处理watch和track该key的client，增加标识或发送消息。
        signalModifiedKey(NULL, db, keyobj);
        decrRefCount(keyobj);
        server.stat_expiredkeys++;
        return 1;
    } else {
        return 0;
    }
}

/* Try to expire a few timed out keys. The algorithm used is adaptive and
 * will use few CPU cycles if there are few expiring keys, otherwise
 * it will get more aggressive to avoid that too much memory is used by
 * keys that can be removed from the keyspace.
 *
 * Every expire cycle tests multiple databases: the next call will start
 * again from the next db. No more than CRON_DBS_PER_CALL databases are
 * tested at every iteration.
 *
 * The function can perform more or less work, depending on the "type"
 * argument. It can execute a "fast cycle" or a "slow cycle". The slow
 * cycle is the main way we collect expired cycles: this happens with
 * the "server.hz" frequency (usually 10 hertz).
 *
 * However the slow cycle can exit for timeout, since it used too much time.
 * For this reason the function is also invoked to perform a fast cycle
 * at every event loop cycle, in the beforeSleep() function. The fast cycle
 * will try to perform less work, but will do it much more often.
 *
 * The following are the details of the two expire cycles and their stop
 * conditions:
 *
 * If type is ACTIVE_EXPIRE_CYCLE_FAST the function will try to run a
 * "fast" expire cycle that takes no longer than ACTIVE_EXPIRE_CYCLE_FAST_DURATION
 * microseconds, and is not repeated again before the same amount of time.
 * The cycle will also refuse to run at all if the latest slow cycle did not
 * terminate because of a time limit condition.
 *
 * If type is ACTIVE_EXPIRE_CYCLE_SLOW, that normal expire cycle is
 * executed, where the time limit is a percentage of the REDIS_HZ period
 * as specified by the ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC define. In the
 * fast cycle, the check of every database is interrupted once the number
 * of already expired keys in the database is estimated to be lower than
 * a given percentage, in order to avoid doing too much work to gain too
 * little memory.
 *
 * The configured expire "effort" will modify the baseline parameters in
 * order to do more work in both the fast and slow expire cycles.
 */
// 尝试处理一些过期的keys。这个算法是自适应的，并且如果过期key很少时，只使用很少的CPU周期来处理。
// 否则当要过期的key很多时，会采取比较激进的方式来处理，避免需要过期删除的key占用太多内存。
// 每个过期处理周期，会检测多个db，下一次处理时将会接着上一次的db来处理。每次迭代处理的db数量不会超过CRON_DBS_PER_CALL。

// 这个功能能够处理的工作，取决于"type"参数，从而进行"fast cycle"或者"slow cycle"。
// "slow cycle"是处理过期的主要方式，执行频率是每秒server.hz（10）次。
// 但是"slow cycle"可能会很耗时间，从而超时结束本次处理。所以这个函数设置了一个"fast cycle"模式，在每次事件循环的beforeSleep()中调用。
// "fast cycle"会执行更少的工作，但是执行的更频繁。

// 下面是这两种执行方式的详细说明以及他们的停止条件：
// 1、type如果是ACTIVE_EXPIRE_CYCLE_FAST，即fast模式，执行时间不会超过ACTIVE_EXPIRE_CYCLE_FAST_DURATION（1000）微秒。
//  并且在相同的时间之前，不会再次执行。如果最近的一次执行因为执行比较慢而没有结束的话，我们也不会重新开始一个新的过期处理。
// 2、type如果是ACTIVE_EXPIRE_CYCLE_SLOW，即slow模式，正常周期性过期处理。
//  时间限制是ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC指定的REDIS_HZ周期百分比。
//  在处理中，会根据每次查询的过期key的数量，来预估当前db中剩余过期keys的百分比。
//  如果预估过期keys百分比小于了某个数值，则结束当前db的处理。避免花很多的时间，只释放很少的内存。

// 配置的active_expire_effort会改变过期处理基准参数，从而在过期处理中做更多的工作。

#define ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP 20 /* Keys for each DB loop. */
#define ACTIVE_EXPIRE_CYCLE_FAST_DURATION 1000 /* Microseconds. */
#define ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC 25 /* Max % of CPU to use. */
#define ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE 10 /* % of stale keys after which
                                                   we do extra efforts. */

void activeExpireCycle(int type) {
    /* Adjust the running parameters according to the configured expire
     * effort. The default effort is 1, and the maximum configurable effort
     * is 10. */
    // 根据配置的active_expire_effort来调整运行的基准参数。默认effort为1，最大可配置为10。
    unsigned long
    effort = server.active_expire_effort-1, /* Rescale from 0 to 9. */
    config_keys_per_loop = ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP +
                           ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP/4*effort,
    config_cycle_fast_duration = ACTIVE_EXPIRE_CYCLE_FAST_DURATION +
                                 ACTIVE_EXPIRE_CYCLE_FAST_DURATION/4*effort,
    config_cycle_slow_time_perc = ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC +
                                  2*effort,
    config_cycle_acceptable_stale = ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE-
                                    effort;

    /* This function has some global state in order to continue the work
     * incrementally across calls. */
    // 一些全局参数，用于多次调用时进行递进的处理，状态的传递。
    static unsigned int current_db = 0; /* Next DB to test. */
    static int timelimit_exit = 0;      /* Time limit hit in previous call? */
    static long long last_fast_cycle = 0; /* When last fast cycle ran. */

    int j, iteration = 0;
    int dbs_per_call = CRON_DBS_PER_CALL;
    long long start = ustime(), timelimit, elapsed;

    /* When clients are paused the dataset should be static not just from the
     * POV of clients not being able to write, but also from the POV of
     * expires and evictions of keys not being performed. */
    // 当client时暂停状态时，db中的数据需要保持静态不变，client角度不会修改数据，而过期驱逐清理也不应该被执行。。
    if (checkClientPauseTimeoutAndReturnIfPaused()) return;

    if (type == ACTIVE_EXPIRE_CYCLE_FAST) {
        /* Don't start a fast cycle if the previous cycle did not exit
         * for time limit, unless the percentage of estimated stale keys is
         * too high. Also never repeat a fast cycle for the same period
         * as the fast cycle total duration itself. */
        // 如果前一个处理周期没有达到时间限制（即处理工作很少，很快结束），除非估计要清理的过期key过多，否则不要开启这次的fast cycle。
        // 另外连续两次的fast cycle，之间的时间间隔要大于2倍的config_cycle_fast_duration周期。
        if (!timelimit_exit &&
            server.stat_expired_stale_perc < config_cycle_acceptable_stale)
            return;

        if (start < last_fast_cycle + (long long)config_cycle_fast_duration*2)
            return;

        last_fast_cycle = start;
    }

    /* We usually should test CRON_DBS_PER_CALL per iteration, with
     * two exceptions:
     *
     * 1) Don't test more DBs than we have.
     * 2) If last time we hit the time limit, we want to scan all DBs
     * in this iteration, as there is work to do in some DB and we don't want
     * expired keys to use memory for too much time. */
    // 通常我们每次迭代需要检测CRON_DBS_PER_CALL个db。两种情况例外：
    // 1、如果总的使用db数小于CRON_DBS_PER_CALL，则我们每轮检查所有db就可以了。
    // 2、如果上一次处理达到了时间限制，说明有较多的工作需要做，而我们也不希望过期的keys占用太多空间，所以本次迭代我们会扫描所有的DBs。
    if (dbs_per_call > server.dbnum || timelimit_exit)
        dbs_per_call = server.dbnum;

    /* We can use at max 'config_cycle_slow_time_perc' percentage of CPU
     * time per iteration. Since this function gets called with a frequency of
     * server.hz times per second, the following is the max amount of
     * microseconds we can spend in this function. */
    // 我们每次迭代能使用的CUP时间百分比最多为config_cycle_slow_time_perc。
    // 因为这个函数每秒执行server.hz次，所以该函数最大能使用的时间是 (1s/server.hz)*(config_cycle_slow_time_perc/100)
    timelimit = config_cycle_slow_time_perc*1000000/server.hz/100;
    timelimit_exit = 0;
    if (timelimit <= 0) timelimit = 1;  // 最小执行时间limit为1微秒

    if (type == ACTIVE_EXPIRE_CYCLE_FAST)
        // 如果是fast模式，则使用该模式的执行时间限制
        timelimit = config_cycle_fast_duration; /* in microseconds. */

    /* Accumulate some global stats as we expire keys, to have some idea
     * about the number of keys that are already logically expired, but still
     * existing inside the database. */
    // 记录这次过期处理中关于keys的一些全局状态，用于推测现在db中还有多少已过期但没清理的数据。
    long total_sampled = 0;
    long total_expired = 0;

    // 要么dbs_per_call次迭代处理完成退出，要么达到timelimit退出。
    for (j = 0; j < dbs_per_call && timelimit_exit == 0; j++) {
        /* Expired and checked in a single loop. */
        // 单个db中的采样keys和过期keys的数量
        unsigned long expired, sampled;

        // 定位到当前处理的db
        redisDb *db = server.db+(current_db % server.dbnum);

        /* Increment the DB now so we are sure if we run out of time
         * in the current DB we'll restart from the next. This allows to
         * distribute the time evenly across DBs. */
        // 在这里增加数据库，可以保证即使处理当前数据库超时了，下一次也将从下一个数据库开始处理。这样可以将时间平均分给所有DB。
        current_db++;

        /* Continue to expire if at the end of the cycle there are still
         * a big percentage of keys to expire, compared to the number of keys
         * we scanned. The percentage, stored in config_cycle_acceptable_stale
         * is not fixed, but depends on the Redis configured "expire effort". */
        // 如果一次处理结束时，根据已扫描的keys估算仍然有很大百分比的keys要被清理，我们继续接着处理。
        // config_cycle_acceptable_stale百分比并不是固定的，会根据redis配置active_expire_effort进行调整。
        do {
            unsigned long num, slots;
            long long now, ttl_sum;
            int ttl_samples;
            iteration++;

            /* If there is nothing to expire try next DB ASAP. */
            // 如果当前db没有需要处理，尽早跳过处理下一个db
            if ((num = dictSize(db->expires)) == 0) {
                db->avg_ttl = 0;
                break;
            }
            slots = dictSlots(db->expires);
            now = mstime();

            /* When there are less than 1% filled slots, sampling the key
             * space is expensive, so stop here waiting for better times...
             * The dictionary will be resized asap. */
            // 如果只有不到1%的hash slot在使用，采样数据消耗很大，跳过当前db，等待合适的时机再来处理。dict可能很快会resize。
            if (slots > DICT_HT_INITIAL_SIZE &&
                (num*100/slots < 1)) break;

            /* The main collection cycle. Sample random keys among keys
             * with an expire set, checking for expired ones. */
            // 主要处理逻辑。从带有过期时间的keys中，随机抽样一些，来检查过期处理。
            expired = 0;
            sampled = 0;
            ttl_sum = 0;
            ttl_samples = 0;

            // 设置本次处理的采样数据。最多为config_keys_per_loop。
            if (num > config_keys_per_loop)
                num = config_keys_per_loop;

            /* Here we access the low level representation of the hash table
             * for speed concerns: this makes this code coupled with dict.c,
             * but it hardly changed in ten years.
             *
             * Note that certain places of the hash table may be empty,
             * so we want also a stop condition about the number of
             * buckets that we scanned. However scanning for free buckets
             * is very fast: we are in the cache line scanning a sequential
             * array of NULL pointers, so we can scan a lot more buckets
             * than keys in the same time. */
            // 这里出于访问速度的考虑，我们使用hash表的底层表现形式处理。这样虽然使得代码与dict.c有了关联，但此处代码10年几乎没变。
            // 注意hash表的某些位置可能为空，所以我们还需要设置扫描buckets数的上限。
            // 由于扫描空的bucket是非常快的，缓存行扫描全是NULL的顺序数组，所以相比于keys我们能在相同时间内扫描更多buckets。
            long max_buckets = num*20;
            long checked_buckets = 0;

            // 一直循环处理，直到检查的keys数或buckets数达到上限。
            while (sampled < num && checked_buckets < max_buckets) {
                for (int table = 0; table < 2; table++) {
                    // 不在rehash状态，不用处理ht1了。
                    if (table == 1 && !dictIsRehashing(db->expires)) break;

                    // 从之前处理的slot处接着扫描。
                    unsigned long idx = db->expires_cursor;
                    idx &= db->expires->ht[table].sizemask;
                    // 取出bucket
                    dictEntry *de = db->expires->ht[table].table[idx];
                    long long ttl;

                    /* Scan the current bucket of the current table. */
                    // 扫描bucket中的元素列表
                    checked_buckets++;
                    while(de) {
                        /* Get the next entry now since this entry may get
                         * deleted. */
                        // 因为当前元素可能过期被删除，所以保存下一个entry用于下一次迭代。
                        dictEntry *e = de;
                        de = de->next;

                        ttl = dictGetSignedIntegerVal(e)-now;
                        // 处理过期，如果该key存在且过期被删除，返回1，这里记录expired数量。
                        if (activeExpireCycleTryExpire(db,e,now)) expired++;
                        if (ttl > 0) {
                            /* We want the average TTL of keys yet
                             * not expired. */
                            // 我们想统计还没过期的keys的平均TTL，所以这里进行统计计数
                            ttl_sum += ttl;
                            ttl_samples++;
                        }
                        sampled++;
                    }
                }
                // 对于同一个bucket，如果在rehash的话，我们是会对ht0和ht1两个表都处理的。
                db->expires_cursor++;
            }
            total_expired += expired;
            total_sampled += sampled;

            /* Update the average TTL stats for this database. */
            // 计算更新当前db的平均TTL统计
            if (ttl_samples) {
                long long avg_ttl = ttl_sum/ttl_samples;

                /* Do a simple running average with a few samples.
                 * We just use the current estimate with a weight of 2%
                 * and the previous estimate with a weight of 98%. */
                // 使用几个样本做一个动态的平均。当前采样的权重为2%，之前老的数据权重为98%。
                if (db->avg_ttl == 0) db->avg_ttl = avg_ttl;
                db->avg_ttl = (db->avg_ttl/50)*49 + (avg_ttl/50);
            }

            /* We can't block forever here even if there are many keys to
             * expire. So after a given amount of milliseconds return to the
             * caller waiting for the other active expire cycle. */
            // 即使需要处理过期的keys数量很大，我们也不能一直阻塞在这里。
            // 所以这里检查如果达到了时间限制，则停止处理，等待下一次的expire周期。（这里每16次迭代检查一次）
            if ((iteration & 0xf) == 0) { /* check once every 16 iterations. */
                elapsed = ustime()-start;
                if (elapsed > timelimit) {
                    // 设置当前处理超时标识，用于当前处理退出检测，下一次expire处理时也会使用。
                    timelimit_exit = 1;
                    server.stat_expired_time_cap_reached_count++;
                    break;
                }
            }
            /* We don't repeat the cycle for the current database if there are
             * an acceptable amount of stale keys (logically expired but yet
             * not reclaimed). */
            // 根据当前db处理的keys估算，如果仍然有大量的过期keys要处理，我们会继续当前db的处理。
        } while (sampled == 0 ||
                 (expired*100/sampled) > config_cycle_acceptable_stale);
    }

    // 一个轮次的过期处理完了，这里统计处理时长。
    elapsed = ustime()-start;
    server.stat_expire_cycle_time_used += elapsed;
    latencyAddSampleIfNeeded("expire-cycle",elapsed/1000);

    /* Update our estimate of keys existing but yet to be expired.
     * Running average with this sample accounting for 5%. */
    // 更新我们对需要过期的keys的估计。计算时，当前抽样统计的数据占比5%。
    double current_perc;
    if (total_sampled) {
        current_perc = (double)total_expired/total_sampled;
    } else
        current_perc = 0;
    server.stat_expired_stale_perc = (current_perc*0.05)+
                                     (server.stat_expired_stale_perc*0.95);
}

/*-----------------------------------------------------------------------------
 * Expires of keys created in writable slaves
 *
 * Normally slaves do not process expires: they wait the masters to synthesize
 * DEL operations in order to retain consistency. However writable slaves are
 * an exception: if a key is created in the slave and an expire is assigned
 * to it, we need a way to expire such a key, since the master does not know
 * anything about such a key.
 *
 * In order to do so, we track keys created in the slave side with an expire
 * set, and call the expireSlaveKeys() function from time to time in order to
 * reclaim the keys if they already expired.
 *
 * Note that the use case we are trying to cover here, is a popular one where
 * slaves are put in writable mode in order to compute slow operations in
 * the slave side that are mostly useful to actually read data in a more
 * processed way. Think at sets intersections in a tmp key, with an expire so
 * that it is also used as a cache to avoid intersecting every time.
 *
 * This implementation is currently not perfect but a lot better than leaking
 * the keys as implemented in 3.2.
 *----------------------------------------------------------------------------*/

// 可写的slaves中的keys过期处理。
// 通常slaves是不处理过期的，总是等待mastet同步DEL操作来保持主从一致性，但是可写的slaves是例外。
// 如果key是由slave创建的，且指定了过期时间，我们需要有一个方式来对这种key进行过期处理，因为master并不知道这key存在，也就不会处理。
// 为了做到这一点，我们跟踪slave创建的带有过期时间的keys，并定时调用expireSlaveKeys()函数，来处理这种key的过期。
// 当然这个过期实现目前并不完美，但是已经比3.2中会遗漏key要好多了。

// slave设置为可写模式，还是有一些用途的。最常用的是计算slave侧各种处理中的慢操作、慢查询。
// 另外对于集合操作，可以存储在本地，设置过期时间，这样当作一个本地缓存，避免每次都都做同样的集合处理。

/* The dictionary where we remember key names and database ID of keys we may
 * want to expire from the slave. Since this function is not often used we
 * don't even care to initialize the database at startup. We'll do it once
 * the feature is used the first time, that is, when rememberSlaveKeyWithExpire()
 * is called.
 *
 * The dictionary has an SDS string representing the key as the hash table
 * key, while the value is a 64 bit unsigned integer with the bits corresponding
 * to the DB where the keys may exist set to 1. Currently the keys created
 * with a DB id > 63 are not expired, but a trivial fix is to set the bitmap
 * to the max 64 bit unsigned value when we know there is a key with a DB
 * ID greater than 63, and check all the configured DBs in such a case. */
// 这个字典记录了我们想在slave中过期的keys的key name和db ID。因为这个功能并不常用，所以我们并没有在启动时对它进行初始化。
// 我们只在该功能第一次使用的时候做一次初始化处理，即在调用rememberSlaveKeyWithExpire()时。
// 这个字典的key是一个sds字符串与数据hash表中key一致，value是一个64bit的无符号整数，该key所在的DB对应的bit位的值置为1。
// 注意key可能对应多个DB。另外目前如果slave创建的key的DB id>63，将不会进行过期处理。
// 一个简单的解决方法是，当我们知道key的DB id>63时，我们置最高位64位为1。遇到这种情况时，我们就需要检查所有大于63的DBs。

// 可写的slave中需要存储过期keys的话，key使用这个dict存储，因为slave的主数据库的expire dict没用，所以过期时间还是用原来的db存储。
dict *slaveKeysWithExpire = NULL;

/* Check the set of keys created by the master with an expire set in order to
 * check if they should be evicted. */
void expireSlaveKeys(void) {
    // 如果slaveKeysWithExpire为空，不需要处理，返回。
    if (slaveKeysWithExpire == NULL ||
        dictSize(slaveKeysWithExpire) == 0) return;

    int cycles = 0, noexpire = 0;
    mstime_t start = mstime();
    while(1) {
        // 随机选取一个key处理
        dictEntry *de = dictGetRandomKey(slaveKeysWithExpire);
        sds keyname = dictGetKey(de);
        uint64_t dbids = dictGetUnsignedIntegerVal(de);
        uint64_t new_dbids = 0;

        /* Check the key against every database corresponding to the
         * bits set in the value bitmap. */
        int dbid = 0;
        while(dbids && dbid < server.dbnum) {
            // 检查每个db，看key的对应bit位标识是否为，从而判断key是否在该db中。
            if ((dbids & 1) != 0) {
                // 如果在该DB中，则从该db的expires中找到key的过期时间。
                redisDb *db = server.db+dbid;
                dictEntry *expire = dictFind(db->expires,keyname);
                int expired = 0;

                // 判断如果key过期了，进行清理。
                if (expire &&
                    activeExpireCycleTryExpire(server.db+dbid,expire,start))
                {
                    expired = 1;
                }

                /* If the key was not expired in this DB, we need to set the
                 * corresponding bit in the new bitmap we set as value.
                 * At the end of the loop if the bitmap is zero, it means we
                 * no longer need to keep track of this key. */
                // 如果key在这个DB中没有过期，我们需要在新的bitmap的相应bit位设置1。
                // 如果循环结束时，新的bitmap为0，意味着我们不需要再追踪这个key了。
                if (expire && !expired) {
                    // 如果有过期时间，但是还没到时间，没被删除。显然new_dbids中这个db key应保留。
                    noexpire++;
                    new_dbids |= (uint64_t)1 << dbid;
                }
            }
            dbid++;
            dbids >>= 1;
        }

        /* Set the new bitmap as value of the key, in the dictionary
         * of keys with an expire set directly in the writable slave. Otherwise
         * if the bitmap is zero, we no longer need to keep track of it. */
        if (new_dbids)
            // 如果new_dbids不为0，显然我们还有部分DB中对应的key没有过期，应该更新key的值为new_dbids。
            dictSetUnsignedIntegerVal(de,new_dbids);
        else
            // new_dbids为0，表示所有db对应的key都过期处理了，这里直接删除slaveKeysWithExpire对应key。
            dictDelete(slaveKeysWithExpire,keyname);

        /* Stop conditions: found 3 keys we can't expire in a row or
         * time limit was reached. */
        // 循环停止条件：
        // 1、随机找key，连续3次找到的都是不需要过期的key，此时我们认为集合中大部分key都没有过期，不再处理了。
        // 2、每轮循环会进行计数，每64次循环处理会检测下消耗时间，如果执行时间超过了1s，则处理太长时间了，先退出，下次再处理。
        // 3、当处理后slaveKeysWithExpire中没有元素，则结束处理。
        cycles++;
        if (noexpire > 3) break;
        if ((cycles % 64) == 0 && mstime()-start > 1) break;
        if (dictSize(slaveKeysWithExpire) == 0) break;
    }
}

/* Track keys that received an EXPIRE or similar command in the context
 * of a writable slave. */
// 在可写服务器中，追踪收到的带有EXPIRE类型命令的keys。
void rememberSlaveKeyWithExpire(redisDb *db, robj *key) {
    // 首次写入时，创建slaveKeysWithExpire字典。
    if (slaveKeysWithExpire == NULL) {
        static dictType dt = {
            dictSdsHash,                /* hash function */
            NULL,                       /* key dup */
            NULL,                       /* val dup */
            dictSdsKeyCompare,          /* key compare */
            dictSdsDestructor,          /* key destructor */
            NULL,                       /* val destructor */
            NULL                        /* allow to expand */
        };
        slaveKeysWithExpire = dictCreate(&dt,NULL);
    }
    // 目前只支持db0-63的处理，超过的不会expire。
    if (db->id > 63) return;

    // 添加或查询一个key。如果key存在则返回对应entry；如果key不存在，则返回新写入的entry。
    dictEntry *de = dictAddOrFind(slaveKeysWithExpire,key->ptr);
    /* If the entry was just created, set it to a copy of the SDS string
     * representing the key: we don't want to need to take those keys
     * in sync with the main DB. The keys will be removed by expireSlaveKeys()
     * as it scans to find keys to remove. */
    // 如果entry是我们新创建的（entry的key与我们传入的是同一个）。用一个副本来替换，因为我们不想这些keys与数据DB同步。
    // 这个key的过期移除将在expireSlaveKeys()中处理。
    if (de->key == key->ptr) {
        de->key = sdsdup(key->ptr);
        dictSetUnsignedIntegerVal(de,0);
    }

    // 更新key对应的db标识
    uint64_t dbids = dictGetUnsignedIntegerVal(de);
    dbids |= (uint64_t)1 << db->id;
    dictSetUnsignedIntegerVal(de,dbids);
}

/* Return the number of keys we are tracking. */
// 返回slave当前追踪的过期keys数量。
size_t getSlaveKeyWithExpireCount(void) {
    if (slaveKeysWithExpire == NULL) return 0;
    return dictSize(slaveKeysWithExpire);
}

/* Remove the keys in the hash table. We need to do that when data is
 * flushed from the server. We may receive new keys from the master with
 * the same name/db and it is no longer a good idea to expire them.
 *
 * Note: technically we should handle the case of a single DB being flushed
 * but it is not worth it since anyway race conditions using the same set
 * of key names in a writable slave and in its master will lead to
 * inconsistencies. This is just a best-effort thing we do. */
// 移除slaveKeysWithExpire表中的keys。当服务的数据被清空时，这个表中的数据也应该被清空。
// 因为我们会从master收到新的keys，可能有的key与原来的有相同name/db，避免被误过期处理，需要置空slave keys表。
// 注意：从技术上来讲，我们应该处理清空单个DB的情况，而不是清空所有，但是这样是不值得的。
// 因为无论如何在可写的slave和master中使用相同的key名，会引起数据争用，可能导致数据不一致。我们只是尽力而为。
void flushSlaveKeysWithExpireList(void) {
    if (slaveKeysWithExpire) {
        dictRelease(slaveKeysWithExpire);
        slaveKeysWithExpire = NULL;
    }
}

// 检查是否已经到了过期时间
int checkAlreadyExpired(long long when) {
    /* EXPIRE with negative TTL, or EXPIREAT with a timestamp into the past
     * should never be executed as a DEL when load the AOF or in the context
     * of a slave instance.
     *
     * Instead we add the already expired key to the database with expire time
     * (possibly in the past) and wait for an explicit DEL from the master. */
    // 当在加载AOF时，或者实例是slave时，不应该执行带负的TTL参数的EXPIRE命令，或者是带有过去时间戳的EXPIREAT指令。
    // 相反我们应该将这种已经过期的key也加入到db中，等待master传递一个明确的DEL指令。
    return (when <= mstime() && !server.loading && !server.masterhost);
}

/*-----------------------------------------------------------------------------
 * Expires Commands
 *----------------------------------------------------------------------------*/

/* This is the generic command implementation for EXPIRE, PEXPIRE, EXPIREAT
 * and PEXPIREAT. Because the command second argument may be relative or absolute
 * the "basetime" argument is used to signal what the base time is (either 0
 * for *AT variants of the command, or the current time for relative expires).
 *
 * unit is either UNIT_SECONDS or UNIT_MILLISECONDS, and is only used for
 * the argv[2] parameter. The basetime is always specified in milliseconds. */
// 这是一个为EXPIRE, PEXPIRE, EXPIREAT 和 PEXPIREAT命令实现的通用方法。
// 因为命令的第二个参数可能是相对时间也可能是绝对时间，所以使用参数basetime来指定基准时间是什么（传0表示参数为绝对时间，否则应该传当前时间）。
// unit 时间单位是 UNIT_SECONDS 或者 UNIT_MILLISECONDS，且只用于argv[2]参数。basetime参数的单位总是指定为毫秒。
void expireGenericCommand(client *c, long long basetime, int unit) {
    robj *key = c->argv[1], *param = c->argv[2];
    long long when; /* unix time in milliseconds when the key will expire. */

    // 从命令第二个参数中提取数字，作为毫秒过期时间。然后进行单位处理，相对时间转绝对时间。
    if (getLongLongFromObjectOrReply(c, param, &when, NULL) != C_OK)
        return;
    int negative_when = when < 0;
    if (unit == UNIT_SECONDS) when *= 1000;
    when += basetime;
    if (((when < 0) && !negative_when) || ((when-basetime > 0) && negative_when)) {
        /* EXPIRE allows negative numbers, but we can at least detect an
         * overflow by either unit conversion or basetime addition. */
        // EXPIRE是支持负数的，但这里我们检测出了溢出。
        // 开始when>0但计算后when<0了；或者开始when<0，最后when-basetime即为开始的when却又>0了。
        addReplyErrorFormat(c, "invalid expire time in %s", c->cmd->name);
        return;
    }
    /* No key, return zero. */
    // 没有key，回复0。
    if (lookupKeyWrite(c->db,key) == NULL) {
        addReply(c,shared.czero);
        return;
    }

    if (checkAlreadyExpired(when)) {
        // 如果已经过期了，我们需要删除该key
        robj *aux;

        int deleted = server.lazyfree_lazy_expire ? dbAsyncDelete(c->db,key) :
                                                    dbSyncDelete(c->db,key);
        serverAssertWithInfo(c,key,deleted);
        server.dirty++;

        /* Replicate/AOF this as an explicit DEL or UNLINK. */
        // 需要根据我们当前是同步还是异步删除，重新对命令进行处理，使用DEL或者UNLINK。
        aux = server.lazyfree_lazy_expire ? shared.unlink : shared.del;
        rewriteClientCommandVector(c,2,aux,key);
        // key变更需要通知相关client。
        signalModifiedKey(c,c->db,key);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
        addReply(c, shared.cone);
        return;
    } else {
        // 没过期，我们需要将过期时间写入DB的expire表中。
        setExpire(c,c->db,key,when);
        addReply(c,shared.cone);
        // 同样key变更，需要通知相关client。
        signalModifiedKey(c,c->db,key);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"expire",key,c->db->id);
        server.dirty++;
        return;
    }
}

/* EXPIRE key seconds */
void expireCommand(client *c) {
    expireGenericCommand(c,mstime(),UNIT_SECONDS);
}

/* EXPIREAT key time */
void expireatCommand(client *c) {
    expireGenericCommand(c,0,UNIT_SECONDS);
}

/* PEXPIRE key milliseconds */
void pexpireCommand(client *c) {
    expireGenericCommand(c,mstime(),UNIT_MILLISECONDS);
}

/* PEXPIREAT key ms_time */
void pexpireatCommand(client *c) {
    expireGenericCommand(c,0,UNIT_MILLISECONDS);
}

/* Implements TTL and PTTL */
// TTL和PTTL的通用处理。
void ttlGenericCommand(client *c, int output_ms) {
    long long expire, ttl = -1;

    /* If the key does not exist at all, return -2 */
    // 如果key在db中不存在，回复-2
    if (lookupKeyReadWithFlags(c->db,c->argv[1],LOOKUP_NOTOUCH) == NULL) {
        addReplyLongLong(c,-2);
        return;
    }
    /* The key exists. Return -1 if it has no expire, or the actual
     * TTL value otherwise. */
    // key存在，如果有过期时间则返回相应的TTL，如果没有过期时间则返回-1。
    expire = getExpire(c->db,c->argv[1]);
    if (expire != -1) {
        // 计算剩余的TTL
        ttl = expire-mstime();
        if (ttl < 0) ttl = 0;
    }
    if (ttl == -1) {
        addReplyLongLong(c,-1);
    } else {
        addReplyLongLong(c,output_ms ? ttl : ((ttl+500)/1000));
    }
}

/* TTL key */
void ttlCommand(client *c) {
    ttlGenericCommand(c, 0);
}

/* PTTL key */
void pttlCommand(client *c) {
    ttlGenericCommand(c, 1);
}

/* PERSIST key */
void persistCommand(client *c) {
    if (lookupKeyWrite(c->db,c->argv[1])) {
        if (removeExpire(c->db,c->argv[1])) {
            // 找到key，移除过期时间。通知key变更。
            signalModifiedKey(c,c->db,c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"persist",c->argv[1],c->db->id);
            addReply(c,shared.cone);
            server.dirty++;
        } else {
            addReply(c,shared.czero);
        }
    } else {
        addReply(c,shared.czero);
    }
}

/* TOUCH key1 [key2 key3 ... keyN] */
void touchCommand(client *c) {
    int touched = 0;
    for (int j = 1; j < c->argc; j++)
        // 实际上就是更新了key的访问时间。LRU、LFU时间。key如果过期会清除。
        if (lookupKeyRead(c->db,c->argv[j]) != NULL) touched++;
    addReplyLongLong(c,touched);
}

