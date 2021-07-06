/* Hash Tables Implementation.
 *
 * This file implements in memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto resize if needed
 * tables of power of two in size are used, collisions are handled by
 * chaining. See the source code for more information... :)
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

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <sys/time.h>

#include "dict.h"
#include "zmalloc.h"
#include "redisassert.h"

/* Using dictEnableResize() / dictDisableResize() we make possible to
 * enable/disable resizing of the hash table as needed. This is very important
 * for Redis, as we use copy-on-write and don't want to move too much memory
 * around when there is a child performing saving operations.
 *
 * Note that even when dict_can_resize is set to 0, not all resizes are
 * prevented: a hash table is still allowed to grow if the ratio between
 * the number of elements and the buckets > dict_force_resize_ratio. */
// 使用dictEnableResize() / dictDisableResize()来改变dict_can_resize的值，在需要的时候启用/禁用hash表的resize。
// 我们使用copy-on-write的方式开启子进程进行持久化操作，当在执行该操作的时候，我们不希望进行过多的内存数据迁移，可以临时禁用resize。
// 注意：即使dict_can_resize=0，也并不是说所有的resizes都禁止。
// 当elements/buckets比率>dict_force_resize_ratio时，hash表也允许进行扩容。
static int dict_can_resize = 1;
static unsigned int dict_force_resize_ratio = 5;

/* -------------------------- private prototypes ---------------------------- */

static int _dictExpandIfNeeded(dict *ht);
static unsigned long _dictNextPower(unsigned long size);
static long _dictKeyIndex(dict *ht, const void *key, uint64_t hash, dictEntry **existing);
static int _dictInit(dict *ht, dictType *type, void *privDataPtr);

/* -------------------------- hash functions -------------------------------- */

static uint8_t dict_hash_function_seed[16];

void dictSetHashFunctionSeed(uint8_t *seed) {
    memcpy(dict_hash_function_seed,seed,sizeof(dict_hash_function_seed));
}

uint8_t *dictGetHashFunctionSeed(void) {
    return dict_hash_function_seed;
}

/* The default hashing function uses SipHash implementation
 * in siphash.c. */

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);
uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k);

uint64_t dictGenHashFunction(const void *key, int len) {
    return siphash(key,len,dict_hash_function_seed);
}

uint64_t dictGenCaseHashFunction(const unsigned char *buf, int len) {
    return siphash_nocase(buf,len,dict_hash_function_seed);
}

/* ----------------------------- API implementation ------------------------- */

/* Reset a hash table already initialized with ht_init().
 * NOTE: This function should only be called by ht_destroy(). */
// 重置初始化dictht，元素都置为零值。该函数应该仅用于ht_destroy()操作中。
static void _dictReset(dictht *ht)
{
    ht->table = NULL;
    ht->size = 0;
    ht->sizemask = 0;
    ht->used = 0;
}

/* Create a new hash table */
// 创建一个新的hash表
dict *dictCreate(dictType *type,
        void *privDataPtr)
{
    // 先分配内存
    dict *d = zmalloc(sizeof(*d));

    // 再进行成员初始化操作
    _dictInit(d,type,privDataPtr);
    return d;
}

/* Initialize the hash table */
// 初始化hash表
int _dictInit(dict *d, dictType *type,
        void *privDataPtr)
{
    // 初始化两个dict，及其他属性
    _dictReset(&d->ht[0]);
    _dictReset(&d->ht[1]);
    d->type = type;
    d->privdata = privDataPtr;
    d->rehashidx = -1;
    d->pauserehash = 0;
    return DICT_OK;
}

/* Resize the table to the minimal size that contains all the elements,
 * but with the invariant of a USED/BUCKETS ratio near to <= 1 */
// 调整hash表的size为能包含所有元素的最小大小。当然需要保证USED/BUCKETS比率大致<=1
int dictResize(dict *d)
{
    unsigned long minimal;

    // 如果不能resize或正在进行rehash，返回err
    if (!dict_can_resize || dictIsRehashing(d)) return DICT_ERR;
    // 设置最小size
    minimal = d->ht[0].used;
    if (minimal < DICT_HT_INITIAL_SIZE)
        minimal = DICT_HT_INITIAL_SIZE;
    // 进行size调整
    return dictExpand(d, minimal);
}

/* Expand or create the hash table,
 * when malloc_failed is non-NULL, it'll avoid panic if malloc fails (in which case it'll be set to 1).
 * Returns DICT_OK if expand was performed, and DICT_ERR if skipped. */
// 扩容或创建hash表。当malloc_failed非NULL时，将允许malloc失败，此时malloc_failed返回1。
// 这个函数成功扩容时返回ok，没做调整时返回err。
int _dictExpand(dict *d, unsigned long size, int* malloc_failed)
{
    if (malloc_failed) *malloc_failed = 0;

    /* the size is invalid if it is smaller than the number of
     * elements already inside the hash table */
    // 如果正在进行rehash，或者当前元素数量>size，不能进行调整，返回err
    if (dictIsRehashing(d) || d->ht[0].used > size)
        return DICT_ERR;

    dictht n; /* the new hash table */
    // 计算新的hash表大小。
    unsigned long realsize = _dictNextPower(size);

    /* Rehashing to the same table size is not useful. */
    // 新的size与原hash表大小一样，不做操作，返回err
    if (realsize == d->ht[0].size) return DICT_ERR;

    /* Allocate the new hash table and initialize all pointers to NULL */
    // 初始化新hash表相关参数
    n.size = realsize;
    n.sizemask = realsize-1;
    // 为新hash表分配内存
    if (malloc_failed) {
        n.table = ztrycalloc(realsize*sizeof(dictEntry*));
        *malloc_failed = n.table == NULL;
        if (*malloc_failed)
            return DICT_ERR;
    } else
        n.table = zcalloc(realsize*sizeof(dictEntry*));

    n.used = 0;

    /* Is this the first initialization? If so it's not really a rehashing
     * we just set the first hash table so that it can accept keys. */
    // 如果是hash表的第一次初始化，不需要进行rehash操作，初始化完成即返回。
    if (d->ht[0].table == NULL) {
        d->ht[0] = n;
        return DICT_OK;
    }

    /* Prepare a second hash table for incremental rehashing */
    // 新分配的hash表设置为ht1，初始化rehashidx=0，表示开始进行渐进rehash
    d->ht[1] = n;
    d->rehashidx = 0;
    return DICT_OK;
}

/* return DICT_ERR if expand was not performed */
// 调整hash表的size。这里不允许内存分配失败，失败会panic。
int dictExpand(dict *d, unsigned long size) {
    return _dictExpand(d, size, NULL);
}

/* return DICT_ERR if expand failed due to memory allocation failure */
// 尝试进行扩容，内存分配失败不panic，返回err
int dictTryExpand(dict *d, unsigned long size) {
    int malloc_failed;
    _dictExpand(d, size, &malloc_failed);
    return malloc_failed? DICT_ERR : DICT_OK;
}

/* Performs N steps of incremental rehashing. Returns 1 if there are still
 * keys to move from the old to the new hash table, otherwise 0 is returned.
 *
 * Note that a rehashing step consists in moving a bucket (that may have more
 * than one key as we use chaining) from the old to the new hash table, however
 * since part of the hash table may be composed of empty spaces, it is not
 * guaranteed that this function will rehash even a single bucket, since it
 * will visit at max N*10 empty buckets in total, otherwise the amount of
 * work it does would be unbound and the function may block for a long time. */
// 执行N步增量的rehash。如果仍然有元素需要rehash则返回1，否则返回0。
// 注意：一个rehash步骤包括从旧hash表迁移一个bucket（可能链表包含多余一个key）到新表，
// 但是由于hash表有的部分可能是空的空间（没有元素），而我们只会访问最多N*10个空的bucket，
// 为了避免长时间的处理工作，导致请求阻塞时间太长，这个函数不保证一定能rehash单个bucket，如果全是空的，检查10N个bucket直接返回了。
int dictRehash(dict *d, int n) {
    // 最大检查空bucket检查次数，正常命令执行时处理是10次，定时任务处理时最大限制是1000次
    int empty_visits = n*10; /* Max number of empty buckets to visit. */
    // 如果不在rehash，返回0
    if (!dictIsRehashing(d)) return 0;

    // 迭代n次，正常命令执行时处理迭代1次，定时任务迭代100次。
    while(n-- && d->ht[0].used != 0) {
        // 如果ht[0]还有元素在用，则需要迁移处理
        dictEntry *de, *nextde;

        /* Note that rehashidx can't overflow as we are sure there are more
         * elements because ht[0].used != 0 */
        // 注意rehashidx不会溢出，因为循环条件保证了原表中有元素，所以正在rehash处理，因而rehashidx!=-1。
        assert(d->ht[0].size > (unsigned long)d->rehashidx);
        while(d->ht[0].table[d->rehashidx] == NULL) {
            // 遇到空的bucket，rehashidx+1，empty_visits-1，当访问空bucket的次数达到上限，跳出处理。
            d->rehashidx++;
            if (--empty_visits == 0) return 1;
        }
        // 找到第一个非空的bucket，迁移所有该bucket的数据到新的hash表
        de = d->ht[0].table[d->rehashidx];
        /* Move all the keys in this bucket from the old to the new hash HT */
        // 遍历该bucket的链表数据处理。
        while(de) {
            uint64_t h;

            // 保存下一个待处理的entry
            nextde = de->next;
            /* Get the index in the new hash table */
            // 获取当前entry在新的hash表中的位置
            h = dictHashKey(d, de->key) & d->ht[1].sizemask;
            // 当前元素插入新hash表对应bucket链表的表头。
            de->next = d->ht[1].table[h];
            d->ht[1].table[h] = de;
            // 更新新老hash表的元素数量
            d->ht[0].used--;
            d->ht[1].used++;
            // 准备处理待迁移bucket链表的下一个元素
            de = nextde;
        }
        // 当前bucket迁移完成，在老hash表中设置该bucket为NULL
        d->ht[0].table[d->rehashidx] = NULL;
        // rehashidx前进一步
        d->rehashidx++;
    }

    /* Check if we already rehashed the whole table... */
    // 跳出循环后，检查我们是否已经完成了rehash。
    if (d->ht[0].used == 0) {
        // 如果完成了rehash，则原hash表dict空间释放。
        // d->ht[0] = d->ht[1]，方便我们每次使用时都处理ht0。
        // 重置ht1，rehashidx置为-1表示不再处于rehash状态。
        zfree(d->ht[0].table);
        d->ht[0] = d->ht[1];
        _dictReset(&d->ht[1]);
        d->rehashidx = -1;
        return 0;
    }

    /* More to rehash... */
    // 如果还有元素需要rehash，返回1
    return 1;
}

// 获取当前毫秒时间
long long timeInMilliseconds(void) {
    struct timeval tv;

    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000)+(tv.tv_usec/1000);
}

/* Rehash in ms+"delta" milliseconds. The value of "delta" is larger 
 * than 0, and is smaller than 1 in most cases. The exact upper bound 
 * depends on the running time of dictRehash(d,100).*/
// 执行rehash操作ms+"delta"时间。"delta"大多数情况是大于0小于1的。准确的上限取决于dictRehash(d,100)执行的时间。
// 也就是限制这个函数处理rehash的时间。这个函数用于severCron 的 databasesCron任务中处理渐进rehash操作。
int dictRehashMilliseconds(dict *d, int ms) {
    // 如果暂停rehash，直接返回
    if (d->pauserehash > 0) return 0;

    // 记录当前时间
    long long start = timeInMilliseconds();
    int rehashes = 0;

    // 如果有元素需要rehash，就一直执行dictRehash(d,100)
    while(dictRehash(d,100)) {
        rehashes += 100;
        // 如果执行的时间超过了ms值，跳出循环，不再处理。
        if (timeInMilliseconds()-start > ms) break;
    }
    return rehashes;
}

/* This function performs just a step of rehashing, and only if hashing has
 * not been paused for our hash table. When we have iterators in the
 * middle of a rehashing we can't mess with the two hash tables otherwise
 * some element can be missed or duplicated.
 *
 * This function is called by common lookup or update operations in the
 * dictionary so that the hash table automatically migrates from H1 to H2
 * while it is actively used. */
// 当前hash表没有暂停hash时，这个函数执行一步rehash操作。
// 当在rehash期间有迭代器在访问元素，我们不能弄乱两个hash表，否则某些元素可能丢失或重复。
// 这个函数一般用于公用的dict查询或更新操作，这样hash表在使用时能自动的从H1迁移到H2，即不影响正常使用。
static void _dictRehashStep(dict *d) {
    if (d->pauserehash == 0) dictRehash(d,1);
}

/* Add an element to the target hash table */
// 添加一个元素到指定的hash表中。如果表中存在该key，返回err。不存在则加入，返回ok。
int dictAdd(dict *d, void *key, void *val)
{
    // 先根据key从dict找entry，因为第三个参数为NULL，如果找到了函数会返回NULL，且不会在参数中返回对应的entry。
    // 如果没找到，会基于传入的key创建一个新的entry加入hash表。
    dictEntry *entry = dictAddRaw(d,key,NULL);

    // entry为NULL说明hash中已经存在了该key，返回err
    if (!entry) return DICT_ERR;
    // 原hash表中不存在该key，创建加入hash表后，这里填入value。返回ok
    dictSetVal(d, entry, val);
    return DICT_OK;
}

/* Low level add or find:
 * This function adds the entry but instead of setting a value returns the
 * dictEntry structure to the user, that will make sure to fill the value
 * field as they wish.
 *
 * This function is also directly exposed to the user API to be called
 * mainly in order to store non-pointers inside the hash value, example:
 *
 * entry = dictAddRaw(dict,mykey,NULL);
 * if (entry != NULL) dictSetSignedIntegerVal(entry,1000);
 *
 * Return values:
 *
 * If key already exists NULL is returned, and "*existing" is populated
 * with the existing entry if existing is not NULL.
 *
 * If key was added, the hash entry is returned to be manipulated by the caller.
 */
// 低级别的函数用于查找或添加元素。这个函数基于key添加entry但不设置value，返回对应的entry给用户，让用户来处理是否填充value。
// 当用户想要直接使用非指针类型数据作为value写入时，也可以直接调用这个函数。例子见上面，传入的常量1000。
// 当给定的key在dict中存在时，返回NULL，existing中保存找到的entry。当给定key不存在，则创建新的entry加入hash表，返回新建的entry。
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing)
{
    long index;
    dictEntry *entry;
    dictht *ht;

    // 如果在进行rehash，处理一次rehash操作
    if (dictIsRehashing(d)) _dictRehashStep(d);

    /* Get the index of the new element, or -1 if
     * the element already exists. */
    // 获取新插入元素的index，返回-1则说明元素已存在，存在的元素entry放在existing中。返回其他值即为新元素应该的插入的bucket位置。
    if ((index = _dictKeyIndex(d, key, dictHashKey(d,key), existing)) == -1)
        return NULL;

    /* Allocate the memory and store the new entry.
     * Insert the element in top, with the assumption that in a database
     * system it is more likely that recently added entries are accessed
     * more frequently. */
    // 如果是在rehash，我们不应该再往老的ht0中写数据了，所以选择ht1作为写入hash表。
    // 另外都是在bucket队头插入，因为时间局部性原理，新加入的数据可能会被更频繁的访问。
    ht = dictIsRehashing(d) ? &d->ht[1] : &d->ht[0];
    // 分配内存来存储新的enrty。将新entry加入对应bucket列表的队头。然后更新hash表元素个数。
    entry = zmalloc(sizeof(*entry));
    entry->next = ht->table[index];
    ht->table[index] = entry;
    ht->used++;

    /* Set the hash entry fields. */
    // 前面将entry加入hash表了，这里设置该entry的key。
    dictSetKey(d, entry, key);
    return entry;
}

/* Add or Overwrite:
 * Add an element, discarding the old value if the key already exists.
 * Return 1 if the key was added from scratch, 0 if there was already an
 * element with such key and dictReplace() just performed a value update
 * operation. */
// 添加或覆盖：添加一个元素，如果元素存在，新值覆盖旧值。
// 如果是新增元素，返回1，如果key已经存在，更新处理，则返回0。
int dictReplace(dict *d, void *key, void *val)
{
    dictEntry *entry, *existing, auxentry;

    /* Try to add the element. If the key
     * does not exists dictAdd will succeed. */
    // 尝试添加元素，key存在则返回的entry=NULL，existing中保存找到的entry。
    entry = dictAddRaw(d,key,&existing);
    if (entry) {
        // 如果entry非NULL，说明是新增元素，这里需要写入对应的value。新增成功返回1
        dictSetVal(d, entry, val);
        return 1;
    }

    /* Set the new value and free the old one. Note that it is important
     * to do that in this order, as the value may just be exactly the same
     * as the previous one. In this context, think to reference counting,
     * you want to increment (set), and then decrement (free), and not the
     * reverse. */
    // existing中返回的是已经存在的entry，这里写入新的value，释放旧的value。不是新增，返回0。
    // 注意先写新值再释放旧值，这个顺序很重要，因为有可能新旧值是一样的，先释放的话，可能obj就没了，没办法增加引用计数，空指针操作？
    // 结构体赋值，非指针，内部浅拷贝。
    // 虽然value可能是指针，不过set值后existing指向的地址与auxentry不一样了，所以可以直接释放auxentry的value域。
    auxentry = *existing;
    dictSetVal(d, existing, val);
    dictFreeVal(d, &auxentry);
    return 0;
}

/* Add or Find:
 * dictAddOrFind() is simply a version of dictAddRaw() that always
 * returns the hash entry of the specified key, even if the key already
 * exists and can't be added (in that case the entry of the already
 * existing key is returned.)
 *
 * See dictAddRaw() for more information. */
// 添加或查询元素：是dictAddRaw()的封装，总是返回key对应的entry，不管是新增的还是原来就有的。
dictEntry *dictAddOrFind(dict *d, void *key) {
    dictEntry *entry, *existing;
    // 如果指定key存在则entry=NULL，existing为存在的entry；如果指定key不存在，构建新的entry写入并返回。
    entry = dictAddRaw(d,key,&existing);
    // 根据返回entry是否为空来判断处理，总是返回key对应entry。
    // 注意这里返回的entry可能value值为空（新增的就为空）。
    return entry ? entry : existing;
}

/* Search and remove an element. This is an helper function for
 * dictDelete() and dictUnlink(), please check the top comment
 * of those functions. */
// 搜索并移除一个元素。这个函数是dictDelete() 和 dictUnlink()的辅助方法。
static dictEntry *dictGenericDelete(dict *d, const void *key, int nofree) {
    uint64_t h, idx;
    dictEntry *he, *prevHe;
    int table;

    // 如果hash表中没有元素，直接返回。
    if (d->ht[0].used == 0 && d->ht[1].used == 0) return NULL;

    // 如果在rehash，这里执行一步rehash操作
    if (dictIsRehashing(d)) _dictRehashStep(d);
    // 根据key计算hash
    h = dictHashKey(d, key);

    // ht0的table总是不为NULL，rehash没结束时ht0不会释放，rehash结束时会交换ht0和ht1然后使用ht0，所以ht0.table!=NULL。
    // 基于此，我们可以这样先遍历ht0，再遍历ht1，在循环结束的时候判断如果不处于rehash状态说明ht1的table为NULL，跳过返回。
    for (table = 0; table <= 1; table++) {
        // 定位到对应bucket
        idx = h & d->ht[table].sizemask;
        he = d->ht[table].table[idx];
        // prevHe用于保存当前处理entry节点的前一个节点指针，用于处理节点删除。非双向链表只能这样操作。
        prevHe = NULL;
        // 遍历bucket链表元素处理
        while(he) {
            if (key==he->key || dictCompareKeys(d, key, he->key)) {
                /* Unlink the element from the list */
                // 从dict中移除该元素，即从dict中对应bucket的元素列表list中移除该元素。
                // 找到了当前key，如果有pre节点，操作pre节点的next指针直接从链表中删除
                // 如果没有pre节点，说明当前处理的是头节点，将头指针也就是bucket指针直接指向下一个节点即可。
                if (prevHe)
                    prevHe->next = he->next;
                else
                    d->ht[table].table[idx] = he->next;
                // 根据传入参数nofree，判断是立即free，还是后面再free。
                if (!nofree) {
                    dictFreeKey(d, he);
                    dictFreeVal(d, he);
                    zfree(he);
                }
                // 更新hash表中元素个数
                d->ht[table].used--;
                // 返回删除entry的地址。虽然可能free了对应堆上分配的空间，但是这里he实际上还是保存的对应entry的地址，而不是NULL。
                return he;
            }
            // 当前节点不是所找元素，跳过准备处理下一个节点。
            prevHe = he;
            he = he->next;
        }
        // 如果不在rehash，不用处理ht1。
        if (!dictIsRehashing(d)) break;
    }
    // 没找到元素，返回NULL。
    return NULL; /* not found */
}

/* Remove an element, returning DICT_OK on success or DICT_ERR if the
 * element was not found. */
// 移除一个元素，如果元素存在且移除成功返回ok，如果元素不存在返回err。
int dictDelete(dict *ht, const void *key) {
    return dictGenericDelete(ht,key,0) ? DICT_OK : DICT_ERR;
}

/* Remove an element from the table, but without actually releasing
 * the key, value and dictionary entry. The dictionary entry is returned
 * if the element was found (and unlinked from the table), and the user
 * should later call `dictFreeUnlinkedEntry()` with it in order to release it.
 * Otherwise if the key is not found, NULL is returned.
 *
 * This function is useful when we want to remove something from the hash
 * table but want to use its value before actually deleting the entry.
 * Without this function the pattern would require two lookups:
 *
 *  entry = dictFind(...);
 *  // Do something with entry
 *  dictDelete(dictionary,entry);
 *
 * Thanks to this function it is possible to avoid this, and use
 * instead:
 *
 * entry = dictUnlink(dictionary,entry);
 * // Do something with entry
 * dictFreeUnlinkedEntry(entry); // <- This does not need to lookup again.
 */
// 移除一个元素，但并没有立即释放元素所占空间。
// 如果找到了元素，unlink掉，并返回待释放的entry。调用方需要调用dictFreeUnlinkedEntry()来手动释放空间。
// 如果元素不存在，函数将返回NULL。
// 当我们想要从hash表移除某元素，并在真正释放前使用该元素值时，可以使用这个方法。
// 没有这个方法的话，我们可能要先查出数据，然后进行处理，之后再delete该entry，需要前后去查询两次hash表。使用这个方法只需要查询一次。
dictEntry *dictUnlink(dict *ht, const void *key) {
    return dictGenericDelete(ht,key,1);
}

/* You need to call this function to really free the entry after a call
 * to dictUnlink(). It's safe to call this function with 'he' = NULL. */
// 调用dictUnlink()后，还需要调用这个方法来真正释放相应的entry。如果he==NULL，多次调用这个方法是安全的。
void dictFreeUnlinkedEntry(dict *d, dictEntry *he) {
    if (he == NULL) return;
    dictFreeKey(d, he);
    dictFreeVal(d, he);
    zfree(he);
}

/* Destroy an entire dictionary */
// 销毁整个dict
int _dictClear(dict *d, dictht *ht, void(callback)(void *)) {
    unsigned long i;

    /* Free all the elements */
    // 遍历hash slot处理每个buckte列表数据。
    for (i = 0; i < ht->size && ht->used > 0; i++) {
        dictEntry *he, *nextHe;

        // 每65K次删除操作，调用callback做一些事情。
        // redis hash表的操作大都是渐进式的，如rehash操作。
        // 但是删除一个大表之前是不支持暂停做一些事情的，这里利用callback进行了支持。
        if (callback && (i & 65535) == 0) callback(d->privdata);

        // 如果当前bucket为NULL，没有元素，跳过。
        if ((he = ht->table[i]) == NULL) continue;
        while(he) {
            // 遍历bucket列表，挨个元素释放。
            nextHe = he->next;
            dictFreeKey(d, he);
            dictFreeVal(d, he);
            zfree(he);
            ht->used--;
            he = nextHe;
        }
    }
    /* Free the table and the allocated cache structure */
    // 释放掉一维的hash slot数组结构结构。
    zfree(ht->table);
    /* Re-initialize the table */
    // 重新初始化ht，各个属性置为默认值
    _dictReset(ht);
    return DICT_OK; /* never fails */
}

/* Clear & Release the hash table */
// 清除并释放整个hash字典，目前除了dict整体结构外，只有两个ht需要调用_dictClear清理
void dictRelease(dict *d)
{
    _dictClear(d,&d->ht[0],NULL);
    _dictClear(d,&d->ht[1],NULL);
    zfree(d);
}

// 根据key从dict中查询对应的entry
dictEntry *dictFind(dict *d, const void *key)
{
    dictEntry *he;
    uint64_t h, idx, table;

    // 如果dict没有元素，直接返回NULL
    if (dictSize(d) == 0) return NULL; /* dict is empty */
    // 判断是否在rehash，当rehashidx != -1时表示在rehash。此时处理一次rehash操作。
    // rehash分散在每次hash操作的时候，平摊rehash的成本。
    if (dictIsRehashing(d)) _dictRehashStep(d);
    // 计算key的hash
    h = dictHashKey(d, key);
    // 查找元素，因为基本使用都是ht0，只有在rehash期间，ht1才有数据。所以我们先从ht0查找。
    for (table = 0; table <= 1; table++) {
        // 计算key hash到表的索引，找到buckte拿到对应链表
        idx = h & d->ht[table].sizemask;
        he = d->ht[table].table[idx];
        // 如果链表不为空，表示该bucket有数据。
        // 挨个遍历，对比key是否地址一致，或者经过compare后一致。
        // 如果一致，则说明找到了元素，直接返回。否则遍历直到处理完链表。
        while(he) {
            if (key==he->key || dictCompareKeys(d, key, he->key))
                return he;
            he = he->next;
        }
        // 当处理完ht0，没找到，而此时不在处理rehash，则说明此元素不存在，返回NULL
        if (!dictIsRehashing(d)) return NULL;
    }
    // 最终可能两个hash表都没找到，返回NULL
    return NULL;
}

// 根据key在指定dict中查找value
void *dictFetchValue(dict *d, const void *key) {
    dictEntry *he;

    // 先根据key找到对应entry
    he = dictFind(d,key);
    // 如果找到了则获取entry的value返回，没找到返回NULL
    return he ? dictGetVal(he) : NULL;
}

/* A fingerprint is a 64 bit number that represents the state of the dictionary
 * at a given time, it's just a few dict properties xored together.
 * When an unsafe iterator is initialized, we get the dict fingerprint, and check
 * the fingerprint again when the iterator is released.
 * If the two fingerprints are different it means that the user of the iterator
 * performed forbidden operations against the dictionary while iterating. */
// fingerprint是一个64bit的数字，代表一个给定时间下dict的状态，它仅仅是将dict的属性xored异或到一起。
long long dictFingerprint(dict *d) {
    long long integers[6], hash = 0;
    int j;

    // 获取dict两个ht的属性
    integers[0] = (long) d->ht[0].table;
    integers[1] = d->ht[0].size;
    integers[2] = d->ht[0].used;
    integers[3] = (long) d->ht[1].table;
    integers[4] = d->ht[1].size;
    integers[5] = d->ht[1].used;

    /* We hash N integers by summing every successive integer with the integer
     * hashing of the previous sum. Basically:
     *
     * Result = hash(hash(hash(int1)+int2)+int3) ...
     *
     * This way the same set of integers in a different order will (likely) hash
     * to a different number. */
    // 将N个数进行hash处理，前一个数的hash值加上当前数，他们的和作为当前hash参数，即：
    // Result = hash(hash(hash(int1)+int2)+int3) ...
    // 这种方式处理可以保证，相同集合的数字，如果顺序不一致的话，hash结果也不一致。
    for (j = 0; j < 6; j++) {
        hash += integers[j];
        /* For the hashing step we use Tomas Wang's 64 bit integer hash. */
        // 使用Tomas Wang's 64bit整数hash
        // hash函数相关可以看看：https://blog.csdn.net/jasper_xulei/article/details/18364313
        hash = (~hash) + (hash << 21); // hash = (hash << 21) - hash - 1;
        hash = hash ^ (hash >> 24);
        hash = (hash + (hash << 3)) + (hash << 8); // hash * 265
        hash = hash ^ (hash >> 14);
        hash = (hash + (hash << 2)) + (hash << 4); // hash * 21
        hash = hash ^ (hash >> 28);
        hash = hash + (hash << 31);
    }
    return hash;
}

// 创建一个字典的迭代器，不安全
dictIterator *dictGetIterator(dict *d)
{
    dictIterator *iter = zmalloc(sizeof(*iter));

    iter->d = d;
    iter->table = 0;
    iter->index = -1;
    iter->safe = 0;
    iter->entry = NULL;
    iter->nextEntry = NULL;
    return iter;
}

// 获取安全的迭代器，safe值为1
// 安全迭代器禁止rehash，只能为了保证数据迭代不重复，新增数据有可能迭代不到。
// https://zhuanlan.zhihu.com/p/42156903
dictIterator *dictGetSafeIterator(dict *d) {
    dictIterator *i = dictGetIterator(d);

    i->safe = 1;
    return i;
}

// 使用迭代器获取调用获取要处理的entry。
dictEntry *dictNext(dictIterator *iter)
{
    while (1) {
        if (iter->entry == NULL) {
            // entry为NULL，进入这个if主要是按规定找寻下一个entry再进行遍历，如果找不到了当然就直接跳出循环返回了。
            // 所以这里可以分为三种情况：
            // 1、迭代器首次调用，index=-1，此时table=0，需要进行一些初始化操作。
            // 2、每个bucket链表处理完了，此时index++，进入下一个bucket处理。在处理ht0、ht1时都一样。
            // 3、当前ht的最后一个bucket的最后一个元素都处理完了。此时index>=size，ht0和ht1处理不一样。
            dictht *ht = &iter->d->ht[iter->table];
            // 这个if表示前面的情形1，迭代器第一次进来，需要进行一些初始化操作。
            if (iter->index == -1 && iter->table == 0) {
                if (iter->safe)
                    // 如果是安全的迭代器，这里需要先暂停rehash操作。
                    dictPauseRehashing(iter->d);
                else
                    // 如果不是安全迭代器，这里记录该dict的指纹hash。
                    iter->fingerprint = dictFingerprint(iter->d);
            }
            // entry=NULL，表示当前bucket处理完了，index++跳到下一个bucket
            iter->index++;
            if (iter->index >= (long) ht->size) {
                // 此时当前ht的bucket遍历完了。如果dict在进行rehash，且当前处理的是ht0，则需要切换到ht1，再从第一个bucket开始处理。
                if (dictIsRehashing(iter->d) && iter->table == 0) {
                    iter->table++;
                    iter->index = 0;
                    ht = &iter->d->ht[1];
                } else {
                    // ht0和ht1都处理完了。或没有在rehash只需要处理ht0处理完了。即前面说的找不到entry了，跳出循环返回。
                    break;
                }
            }
            // 取出index对应的bucket链表，这里也就是首元素entry。
            iter->entry = ht->table[iter->index];
        } else {
            // 如果当前entry不为NULL处理完了，跳到下一个entry处理。
            iter->entry = iter->nextEntry;
        }
        if (iter->entry) {
            /* We need to save the 'next' here, the iterator user
             * may delete the entry we are returning. */
            // 如果entry非空，设置迭代器的nextEntry为当前bucket链表中entry节点的下一个节点。
            // 这里保存下一个entry的目的是为了支持外部对当前迭代访问的数据删除操作。
            iter->nextEntry = iter->entry->next;
            // 返回当前迭代找到的entry
            return iter->entry;
        }
    }
    // hash表遍历完后，迭代返回NULL
    return NULL;
}

// 释放迭代器
void dictReleaseIterator(dictIterator *iter)
{
    // 如果index或table不是最初属性，则说明迭代器使用过，需要释放。
    if (!(iter->index == -1 && iter->table == 0)) {
        if (iter->safe)
            // 对于安全迭代器，这里计数pauserehash--，当减为0时，没有安全迭代器了，又可以处理渐进式rehash了。
            dictResumeRehashing(iter->d);
        else
            assert(iter->fingerprint == dictFingerprint(iter->d));
    }
    zfree(iter);
}

/* Return a random entry from the hash table. Useful to
 * implement randomized algorithms */
// 随机的从hash表中返回一个entry，通常用于实现一些随机的算法。
dictEntry *dictGetRandomKey(dict *d)
{
    dictEntry *he, *orighe;
    unsigned long h;
    int listlen, listele;

    // 如果dict没有元素，直接返回NULL
    if (dictSize(d) == 0) return NULL;
    // 如果dict正在进行rehash，这里处理一步rehash操作
    if (dictIsRehashing(d)) _dictRehashStep(d);
    if (dictIsRehashing(d)) {
        do {
            /* We are sure there are no elements in indexes from 0
             * to rehashidx-1 */
            // 如果这里处于rehash状态，我们明确知道ht0的[0, rehashidx-1)间没有元素（因为都已经迁移了）
            // 所以我们这里随机在[rehashidx, dictSlots(d)]这之间取一个数作为index。
            h = d->rehashidx + (randomULong() % (dictSlots(d) - d->rehashidx));
            // 当该index超过了ht0的size，说明rehash到ht1，从ht1的 index-ht0.size 处取数据；否则直接在ht0的index处取数据。
            he = (h >= d->ht[0].size) ? d->ht[1].table[h - d->ht[0].size] :
                                      d->ht[0].table[h];
        } while(he == NULL);
    } else {
        do {
            // 如果dict不在rehash，那么很简单，直接随机找位置从ht0取就可以了。
            h = randomULong() & d->ht[0].sizemask;
            he = d->ht[0].table[h];
        } while(he == NULL);
    }

    /* Now we found a non empty bucket, but it is a linked
     * list and we need to get a random element from the list.
     * The only sane way to do so is counting the elements and
     * select a random index. */
    // 现在我们找到了一个非空的bucket，但是是一个链表。
    // 想在链表中找一个随机元素，只能先计算列表元素的数量，再选择一个随机的index取元素。
    listlen = 0;
    orighe = he;
    // 遍历计数
    while(he) {
        he = he->next;
        listlen++;
    }
    // 随机取index
    listele = random() % listlen;
    he = orighe;
    // 遍历到index处取元素返回
    while(listele--) he = he->next;
    return he;
}

/* This function samples the dictionary to return a few keys from random
 * locations.
 *
 * It does not guarantee to return all the keys specified in 'count', nor
 * it does guarantee to return non-duplicated elements, however it will make
 * some effort to do both things.
 *
 * Returned pointers to hash table entries are stored into 'des' that
 * points to an array of dictEntry pointers. The array must have room for
 * at least 'count' elements, that is the argument we pass to the function
 * to tell how many random elements we need.
 *
 * The function returns the number of items stored into 'des', that may
 * be less than 'count' if the hash table has less than 'count' elements
 * inside, or if not enough elements were found in a reasonable amount of
 * steps.
 *
 * Note that this function is not suitable when you need a good distribution
 * of the returned items, but only when you need to "sample" a given number
 * of continuous elements to run some kind of algorithm or to produce
 * statistics. However the function is much faster than dictGetRandomKey()
 * at producing N elements. */
// 这个函数从dict中随机抽样选出一部分数据。
// 该函数不保证返回足够count数量的keys，也不保证返回不充分的元素。当然也是会尽量做一些努力来处理的。
// 函数返回数据放在des中，des是存储dictEntry指针类型的数组，数组必须保证能装下至少count个元素。
// 当dict中元素少于count时，或者迭代了合理次数后没有找到足够的元素，返回的元素数量都会少于count。
// 注意：当想要返回很随机分散的元素时，这个函数并不适合使用。
// 这里只是随机选一个位置，然后采样一段连续的数据，所以不支持所有数据随机。
// 主要是足够快，比使用dictGetRandomKey()选N个元素要快很大，一般用于做一些统计来使用，如数据驱逐采样等。
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count) {
    unsigned long j; /* internal hash table id, 0 or 1. */
    unsigned long tables; /* 1 or 2 tables? */
    unsigned long stored = 0, maxsizemask;
    unsigned long maxsteps;

    // 如果需要返回的count大于dict中元素个数，则返回数量最多为dict元素个数。
    if (dictSize(d) < count) count = dictSize(d);
    // 最大迭代次数10倍的返回元素个数
    maxsteps = count*10;

    /* Try to do a rehashing work proportional to 'count'. */
    // 尝试按count比例来进行多轮的rehash
    for (j = 0; j < count; j++) {
        if (dictIsRehashing(d))
            _dictRehashStep(d);
        else
            break;
    }

    // 如果在rehash，我们从两个表中取数据，否则只在ht0中取。
    tables = dictIsRehashing(d) ? 2 : 1;
    // 取两个表sizemask的最大值
    maxsizemask = d->ht[0].sizemask;
    if (tables > 1 && maxsizemask < d->ht[1].sizemask)
        maxsizemask = d->ht[1].sizemask;

    /* Pick a random point inside the larger table. */
    // 在较大的hash表中找一个随机位置。
    unsigned long i = randomULong() & maxsizemask;
    unsigned long emptylen = 0; /* Continuous empty entries so far. */
    // 如果取到的元素少于count个，并且没达到迭代的最大次数，就一直循环取。
    while(stored < count && maxsteps--) {
        // 我们对于每个随机到的index，都尝试着从tables个表中取数据。不在rehash时取ht0，在rehash时两个表都取。
        for (j = 0; j < tables; j++) {
            /* Invariant of the dict.c rehashing: up to the indexes already
             * visited in ht[0] during the rehashing, there are no populated
             * buckets, so we can skip ht[0] for indexes between 0 and idx-1. */
            // rehash不变式：rehash遍历过的buckets都会置空，且后续rehash期间这些bucket不会再有元素，
            // 所以我们能够跳过[0, rehashidx)之间的元素。这里即处理这种情况：
            // tables == 2 表示正在rehash，j == 0 表示当前查找的是ht0。当i<rehashidx时需要跳过处理ht0的部分元素。
            if (tables == 2 && j == 0 && i < (unsigned long) d->rehashidx) {
                /* Moreover, if we are currently out of range in the second
                 * table, there will be no elements in both tables up to
                 * the current rehashing index, so we jump if possible.
                 * (this happens when going from big to small table). */
                // 当随机找的位置i大于等于新的ht1表的大小时，说明我们是在hash缩容。
                // 此时我们无法根据i定位到ht1去取数据，另外ht0中[i, rehashidx)也没数据可取，所以我们从ht0的rehashidx开始取。
                // 反之，随机位置小于新的ht1表的大小时，因为[i, rehashidx)已经rehash到新表了，我们直接跳过ht0，然后从ht1中取数据。
                if (i >= d->ht[1].size)
                    i = d->rehashidx;
                else
                    continue;
            }
            // 如果当前随机的index超出了当前查看表的size，进入下一个表的遍历，或者进入下一轮迭代。
            // 如扩容时，index选择可能大于ht0的size，到这里直接进入ht1表处理。
            if (i >= d->ht[j].size) continue; /* Out of range for this table. */
            // 根据随机的index，取到bucket对应的entry列表
            dictEntry *he = d->ht[j].table[i];

            /* Count contiguous empty buckets, and jump to other
             * locations if they reach 'count' (with a minimum of 5). */
            // 如果当前backet为NULL，则计算连续NULL的bucket数，达到一定数量后，再重新随机选择index。
            if (he == NULL) {
                emptylen++;
                if (emptylen >= 5 && emptylen > count) {
                    i = randomULong() & maxsizemask;
                    emptylen = 0;
                }
            } else {
                emptylen = 0;
                // 如果bucket不为NULL，将该bucket的元素依次加入到返回列表里。直到取够了返回。
                while (he) {
                    /* Collect all the elements of the buckets found non
                     * empty while iterating. */
                    *des = he;
                    des++;
                    he = he->next;
                    stored++;
                    // 取够了返回
                    if (stored == count) return stored;
                }
            }
        }
        // 当前index如果有数据处理了，则接着处理index+1位置的bucket
        i = (i+1) & maxsizemask;
    }
    // 这里迭代次数到了，返回采样的数量。
    // 走到这里应该stored!=count吧，因为如果相等，前面数据加入列表时就应该判断返回了。
    return stored;
}

/* This is like dictGetRandomKey() from the POV of the API, but will do more
 * work to ensure a better distribution of the returned element.
 *
 * This function improves the distribution because the dictGetRandomKey()
 * problem is that it selects a random bucket, then it selects a random
 * element from the chain in the bucket. However elements being in different
 * chain lengths will have different probabilities of being reported. With
 * this function instead what we do is to consider a "linear" range of the table
 * that may be constituted of N buckets with chains of different lengths
 * appearing one after the other. Then we report a random element in the range.
 * In this way we smooth away the problem of different chain lengths. */
// 这个函数在API视角与dictGetRandomKey()很像，但是会做更多的努力来保证返回元素有更好的随机分布。
// dictGetRandomKey()是先随机选一个bucket，再从bucket链表中随机选一个元素。
// 因为在不同链表长度中的元素，被选出的概率是不同的，所以dictGetRandomKey()对于不同元素在全局看来并不是完全随机的。
// 这个函数中，我们考虑将N个链长不同的连续bucket中元素组成一个线性序列（起始bucket随机选择），然后随机从这个序列中取出一个数据返回。
// 这样可以一定程度上使得数据更随机。
#define GETFAIR_NUM_ENTRIES 15
dictEntry *dictGetFairRandomKey(dict *d) {
    dictEntry *entries[GETFAIR_NUM_ENTRIES];
    // 通过dictGetSomeKeys采样连续bucket数据，构造一个线性列表。
    unsigned int count = dictGetSomeKeys(d,entries,GETFAIR_NUM_ENTRIES);
    /* Note that dictGetSomeKeys() may return zero elements in an unlucky
     * run() even if there are actually elements inside the hash table. So
     * when we get zero, we call the true dictGetRandomKey() that will always
     * yield the element if the hash table has at least one. */
    // 注意到dictGetSomeKeys()最坏情况可能返回0个元素。
    // 出现了这种情况我们调用dictGetRandomKey()，从而保证总是能返回一个元素。
    if (count == 0) return dictGetRandomKey(d);
    // 从线性列表中随机选择元素返回。
    unsigned int idx = rand() % count;
    return entries[idx];
}

/* Function to reverse bits. Algorithm from:
 * http://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel */
// 该函数用于bit翻转
static unsigned long rev(unsigned long v) {
    unsigned long s = CHAR_BIT * sizeof(v); // bit size; must be power of 2
    unsigned long mask = ~0UL;
    while ((s >>= 1) > 0) {
        mask ^= (mask << s);
        v = ((v >> s) & mask) | ((v << s) & ~mask);
    }
    return v;
}

/* dictScan() is used to iterate over the elements of a dictionary.
 *
 * Iterating works the following way:
 *
 * 1) Initially you call the function using a cursor (v) value of 0.
 * 2) The function performs one step of the iteration, and returns the
 *    new cursor value you must use in the next call.
 * 3) When the returned cursor is 0, the iteration is complete.
 *
 * The function guarantees all elements present in the
 * dictionary get returned between the start and end of the iteration.
 * However it is possible some elements get returned multiple times.
 *
 * For every element returned, the callback argument 'fn' is
 * called with 'privdata' as first argument and the dictionary entry
 * 'de' as second argument.
 *
 * HOW IT WORKS.
 *
 * The iteration algorithm was designed by Pieter Noordhuis.
 * The main idea is to increment a cursor starting from the higher order
 * bits. That is, instead of incrementing the cursor normally, the bits
 * of the cursor are reversed, then the cursor is incremented, and finally
 * the bits are reversed again.
 *
 * This strategy is needed because the hash table may be resized between
 * iteration calls.
 *
 * dict.c hash tables are always power of two in size, and they
 * use chaining, so the position of an element in a given table is given
 * by computing the bitwise AND between Hash(key) and SIZE-1
 * (where SIZE-1 is always the mask that is equivalent to taking the rest
 *  of the division between the Hash of the key and SIZE).
 *
 * For example if the current hash table size is 16, the mask is
 * (in binary) 1111. The position of a key in the hash table will always be
 * the last four bits of the hash output, and so forth.
 *
 * WHAT HAPPENS IF THE TABLE CHANGES IN SIZE?
 *
 * If the hash table grows, elements can go anywhere in one multiple of
 * the old bucket: for example let's say we already iterated with
 * a 4 bit cursor 1100 (the mask is 1111 because hash table size = 16).
 *
 * If the hash table will be resized to 64 elements, then the new mask will
 * be 111111. The new buckets you obtain by substituting in ??1100
 * with either 0 or 1 can be targeted only by keys we already visited
 * when scanning the bucket 1100 in the smaller hash table.
 *
 * By iterating the higher bits first, because of the inverted counter, the
 * cursor does not need to restart if the table size gets bigger. It will
 * continue iterating using cursors without '1100' at the end, and also
 * without any other combination of the final 4 bits already explored.
 *
 * Similarly when the table size shrinks over time, for example going from
 * 16 to 8, if a combination of the lower three bits (the mask for size 8
 * is 111) were already completely explored, it would not be visited again
 * because we are sure we tried, for example, both 0111 and 1111 (all the
 * variations of the higher bit) so we don't need to test it again.
 *
 * WAIT... YOU HAVE *TWO* TABLES DURING REHASHING!
 *
 * Yes, this is true, but we always iterate the smaller table first, then
 * we test all the expansions of the current cursor into the larger
 * table. For example if the current cursor is 101 and we also have a
 * larger table of size 16, we also test (0)101 and (1)101 inside the larger
 * table. This reduces the problem back to having only one table, where
 * the larger one, if it exists, is just an expansion of the smaller one.
 *
 * LIMITATIONS
 *
 * This iterator is completely stateless, and this is a huge advantage,
 * including no additional memory used.
 *
 * The disadvantages resulting from this design are:
 *
 * 1) It is possible we return elements more than once. However this is usually
 *    easy to deal with in the application level.
 * 2) The iterator must return multiple elements per call, as it needs to always
 *    return all the keys chained in a given bucket, and all the expansions, so
 *    we are sure we don't miss keys moving during rehashing.
 * 3) The reverse cursor is somewhat hard to understand at first, but this
 *    comment is supposed to help.
 */
// dictScan用于在dict上对元素进行迭代。迭代方式如下：
// 1、初始时调用该函数使用游标v=0。
// 2、函数执行一次迭代，并返回新的游标，用于下一次迭代传入。
// 3、当迭代返回的新游标为0时，表示迭代完成。
// 通过这个迭代，该函数保证dict中的所有元素都能返回。但是有的元素可能会返回多次。
// 在迭代到每个bucket时，都会以"privdata"作为第一个参数，并以bucket链表首指针作为第二个参数，调用回调函数"bucketfn"进行一些操作。
// 在迭代到的每个元素，都会以"privdata"作为第一个参数，并以遍历到的元素"de"作为第二个参数，调用回调函数"fn"进行一些操作。

// 这个迭代算法由Pieter Noordhuis设计。主要思路是是游标从bit高位相加并向低位进位来处理bucket遍历。
// 这样操作实际上可以看成是将游标高低位翻转，然后每次+1的递进遍历bucket。最终是可以保证每个bucket遍历一次的。
// 因为我们在迭代处理期间，dict可能进行rehash操作，所以这个策略是需要的。
// 我们hash表的大小总是2的幂次方，并在bucket上使用链表处理冲突。所以对于给定元素，总能使用hash(key)&(size-1)来计算在hash表中位置。
// 当hash表的大小改变时：
// 1、如果是扩容，老的bucket中的元素，总是会被重新hash到当前bucket*N的新bucket中。
// 例如原hash大小为16，当前bucket为3；当扩容到大小为64时，这个老得bucket元素将分散到??0011的bucket中，这里的?可由0和1自由组合，
// 一共有4种方式，对应扩容后的4个bucket，从而老bucket的元素会被分散rehash这4个新bucket中。
// 显然，如果我们仍然使用从低到高位游标递增处理，第一次我们遍历到bucket=3，扩容后如果要处理遍历的话，需要组合??[0011-1111]操作，
// 即从0011依次递增到1111，然后每一个都需要与4种??组合来定位bucket处理。
// 如果我们使用高位相加向低位进位的方式，直接对游标的高位补0，填充成为新的游标后遍历即可，低位的的组合通过高位递增进位可以完全覆盖到。
// 2、如果表是缩容16->4，假设缩容前遍历到了1011，缩容后我们只需要从11开始遍历就可以了，因为11包含了缩容前的4个bucket，
// 尽管数据可能重复遍历，但保证了不会漏掉数据。
// 注意扩容的时候可保证不会重复遍历，但缩容的是有可能重复遍历的。

// 注意：我们在rehash期间是有两张表的。处理时，我们总是先迭代较小的表，然后再对游标进行高位加0或1进行扩展去检查大表。
// 这样我们处理时，总像是只有一个表一样，因为大表只是小表的扩展。
// 这个迭代器是完全无状态的，这是一个很大的优点，因为它不额外使用任何内存。不过它也有一些设计上的缺点：
// 1、我们可能需要多次才能返回所有数据。这个在应用层调用时很好解决。
// 2、这个迭代调用的时候可能会返回较多数据，因为一次需要返回给定bucket（以及扩展bucket）的所有keys，来确保在rehash时不会丢失key。
// 3、游标值bit位逆转处理刚开始可能会较难理解。
unsigned long dictScan(dict *d,
                       unsigned long v,
                       dictScanFunction *fn,
                       dictScanBucketFunction* bucketfn,
                       void *privdata)
{
    dictht *t0, *t1;
    const dictEntry *de, *next;
    unsigned long m0, m1;

    // 如果dict没有元素，返回0，表示迭代结束。
    if (dictSize(d) == 0) return 0;

    /* This is needed in case the scan callback tries to do dictFind or alike. */
    // 这里需要暂停rehash，因为callback可能会调用dictFind等操作。
    dictPauseRehashing(d);

    if (!dictIsRehashing(d)) {
        // 如果不在rehash，只需要遍历ht0即可
        t0 = &(d->ht[0]);
        m0 = t0->sizemask;

        /* Emit entries at cursor */
        // 每次处理一个bucket时，调用bucketfn回调函数做一些其他事情。
        if (bucketfn) bucketfn(privdata, &t0->table[v & m0]);
        de = t0->table[v & m0];
        while (de) {
            // 遍历bucket的列表元素，对每个元素使用回调函数fn进行处理。
            // 这里scan返回所有元素的话，是使用scanCallback回调函数针对每个bucket元素回调处理来收集keys的。
            next = de->next;
            fn(privdata, de);
            de = next;
        }

        /* Set unmasked bits so incrementing the reversed cursor
         * operates on the masked bits */
        // 设置unmasked位为1
        v |= ~m0;

        /* Increment the reverse cursor */
        // 游标高位+1向低位进位，这里的处理是先逆转，+1，再逆转。
        // 要使这样处理后达到最高有效位+1效果，我们需要在高位补位时补1。
        // 而通常正数高位都是补0的，所以我们前面通过 v |= ~m0 来使得unmasked全置为1。
        v = rev(v);
        v++;
        v = rev(v);

    } else {
        // 如果处于rehash阶段，我们就需要先处理小表，然后再对小表扩展处理。
        t0 = &d->ht[0];
        t1 = &d->ht[1];

        /* Make sure t0 is the smaller and t1 is the bigger table */
        // 总是让t0指向小的那个表
        if (t0->size > t1->size) {
            t0 = &d->ht[1];
            t1 = &d->ht[0];
        }

        m0 = t0->sizemask;
        m1 = t1->sizemask;

        /* Emit entries at cursor */
        // 处理小的表t0，也是先找bucket，在遍历链表处理。
        if (bucketfn) bucketfn(privdata, &t0->table[v & m0]);
        de = t0->table[v & m0];
        while (de) {
            next = de->next;
            fn(privdata, de);
            de = next;
        }

        /* Iterate over indices in larger table that are the expansion
         * of the index pointed to by the cursor in the smaller table */
        // 遍历大表t1中的索引，这些索引是小表中光标指向索引的扩展。
        do {
            /* Emit entries at cursor */
            // 首次进入遍历相当于，高位全补0时的遍历。
            if (bucketfn) bucketfn(privdata, &t1->table[v & m1]);
            de = t1->table[v & m1];
            while (de) {
                next = de->next;
                fn(privdata, de);
                de = next;
            }

            /* Increment the reverse cursor not covered by the smaller mask.*/
            // 游标高位+1向低位进位，形成新的游标，老的处理方式：v = (((v | m0) + 1) & ~m0) | (v & m0);
            v |= ~m1;
            v = rev(v);
            v++;
            v = rev(v);

            /* Continue while bits covered by mask difference is non-zero */
            // 遍历直到v的扩展高位重新变成全0跳出循环。
            // 因为第一次进入循环do时，v的扩展高位即为全0，处理完一轮后v高位+1后进入下一轮处理。
            // 后面如果再出现了扩展高位全为0，则说明扩展高位的所有情况已处理完了，则结束该索引扩展处理。
            // 可见do{}while循环还是有可取之处的。
        } while (v & (m0 ^ m1));
    }

    // 一个bucket遍历完成，解除暂停。
    dictResumeRehashing(d);

    return v;
}

/* ------------------------- private functions ------------------------------ */

/* Because we may need to allocate huge memory chunk at once when dict
 * expands, we will check this allocation is allowed or not if the dict
 * type has expandAllowed member function. */
// 因为在扩容时我们需要一次性分配一大块的内存空间，所以这里根据dict type中的expandAllowed函数来检查这次分配是否允许。
// 当dict type中的expandAllowed为NULL的时候始终允许，否则就需要调用对应函数来判断了。
static int dictTypeExpandAllowed(dict *d) {
    if (d->type->expandAllowed == NULL) return 1;
    // expandAllowed传入参数，需要分配内存的大小和当前elements/buckets使用比。
    return d->type->expandAllowed(
                    _dictNextPower(d->ht[0].used + 1) * sizeof(dictEntry*),
                    (double)d->ht[0].used / d->ht[0].size);
}

/* Expand the hash table if needed */
// 判断是否需要扩容，如果需要则执行相关操作。
static int _dictExpandIfNeeded(dict *d)
{
    /* Incremental rehashing already in progress. Return. */
    // 已经在进行rehash了，直接返回ok，不需要处理
    if (dictIsRehashing(d)) return DICT_OK;

    /* If the hash table is empty expand it to the initial size. */
    // 如果hash表是空的，扩容到初始size=4大小
    if (d->ht[0].size == 0) return dictExpand(d, DICT_HT_INITIAL_SIZE);

    /* If we reached the 1:1 ratio, and we are allowed to resize the hash
     * table (global setting) or we should avoid it but the ratio between
     * elements/buckets is over the "safe" threshold, we resize doubling
     * the number of buckets. */
    // 如果我们达到了1:1的比率 &&（全局允许调整ht的size 或者 elements/buckets超过了安全比例）&& 该dict通过了内存分配计划
    if (d->ht[0].used >= d->ht[0].size &&
        (dict_can_resize ||
         d->ht[0].used/d->ht[0].size > dict_force_resize_ratio) &&
        dictTypeExpandAllowed(d))
    {
        // 执行扩容，扩容大小为 不小于 d->ht[0].used+1 的最小2的幂次方数。
        return dictExpand(d, d->ht[0].used + 1);
    }
    return DICT_OK;
}

/* Our hash table capability is a power of two */
// hash表的大小是2的幂次方，所以这里给定size，找出大于size的最小2的幂次方数。
static unsigned long _dictNextPower(unsigned long size)
{
    unsigned long i = DICT_HT_INITIAL_SIZE;

    // 传入size超出最大有符号数，调整
    if (size >= LONG_MAX) return LONG_MAX + 1LU;
    while(1) {
        // 循环每次乘以2，直到找到第一个不小于size的数。
        if (i >= size)
            return i;
        i *= 2;
    }
}

/* Returns the index of a free slot that can be populated with
 * a hash entry for the given 'key'.
 * If the key already exists, -1 is returned
 * and the optional output parameter may be filled.
 *
 * Note that if we are in the process of rehashing the hash table, the
 * index is always returned in the context of the second (new) hash table. */
// 给定一个key，返回一个空闲slot的索引，用来装填该key entry。
// 如果key已经存在了，返回-1，其他输出参数可能会被填充。
// 注意，如果我们正在处理rehash，总是返回新的hash表的的index。
static long _dictKeyIndex(dict *d, const void *key, uint64_t hash, dictEntry **existing)
{
    unsigned long idx, table;
    dictEntry *he;
    if (existing) *existing = NULL;

    /* Expand the hash table if needed */
    // 如果有需要的话，先对hash表扩容。
    if (_dictExpandIfNeeded(d) == DICT_ERR)
        return -1;
    // 分别在ht0和ht1中根据hash找bucket，然后在bucket链表中找元素。
    for (table = 0; table <= 1; table++) {
        idx = hash & d->ht[table].sizemask;
        /* Search if this slot does not already contain the given key */
        // 根据index取得bucket
        he = d->ht[table].table[idx];
        while(he) {
            // 遍历bucket链表比对key是否一致。如果一致，则找到对应的entry，填充进*existing中，返回-1。
            if (key==he->key || dictCompareKeys(d, key, he->key)) {
                if (existing) *existing = he;
                return -1;
            }
            he = he->next;
        }
        // 没有在rehash，处理完ht0就跳出循环。
        if (!dictIsRehashing(d)) break;
    }
    // 最终没hash表中没有对应key，返回即将插入的bucket的idx
    return idx;
}

// 清空dict，对dict中的几个属性置为默认值。
// 可以传入callback函数指针，用于在清空ht0和ht1时回调处理。
void dictEmpty(dict *d, void(callback)(void*)) {
    _dictClear(d,&d->ht[0],callback);
    _dictClear(d,&d->ht[1],callback);
    d->rehashidx = -1;
    d->pauserehash = 0;
}

// 允许dict resize
void dictEnableResize(void) {
    dict_can_resize = 1;
}

// 禁止dict resize
void dictDisableResize(void) {
    dict_can_resize = 0;
}

// 对指定dict中的key，利用dict对应类型的hash函数计算hash值。
uint64_t dictGetHash(dict *d, const void *key) {
    return dictHashKey(d, key);
}

/* Finds the dictEntry reference by using pointer and pre-calculated hash.
 * oldkey is a dead pointer and should not be accessed.
 * the hash value should be provided using dictGetHash.
 * no string / key comparison is performed.
 * return value is the reference to the dictEntry if found, or NULL if not found. */
// 使用一个指针oldptr和预计算的hash，从dict中寻找dictEntry。
// oldptr是一个不再使用的死指针，hash值应该先使用dictGetHash计算出来。
// 当前函数没有string/key的比较，如果找到了则方dictEntry的引用，没找到返回NULL。
dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, uint64_t hash) {
    dictEntry *he, **heref;
    unsigned long idx, table;

    // dict没元素，返回NULL
    if (dictSize(d) == 0) return NULL; /* dict is empty */
    for (table = 0; table <= 1; table++) {
        // 通过hash找bucket
        idx = hash & d->ht[table].sizemask;
        heref = &d->ht[table].table[idx];
        he = *heref;
        // 遍历bucket链表，找元素。
        while(he) {
            if (oldptr==he->key)
                // 如果地址相同，直接返回
                return heref;
            heref = &he->next;
            he = *heref;
        }
        // 没有在rehash，找完ht0没找到，直接防护NULL。
        if (!dictIsRehashing(d)) return NULL;
    }
    // 两个表都没找到，返回NULL
    return NULL;
}

/* ------------------------------- Debugging ---------------------------------*/

#define DICT_STATS_VECTLEN 50
size_t _dictGetStatsHt(char *buf, size_t bufsize, dictht *ht, int tableid) {
    unsigned long i, slots = 0, chainlen, maxchainlen = 0;
    unsigned long totchainlen = 0;
    unsigned long clvector[DICT_STATS_VECTLEN];
    size_t l = 0;

    if (ht->used == 0) {
        return snprintf(buf,bufsize,
            "No stats available for empty dictionaries\n");
    }

    /* Compute stats. */
    for (i = 0; i < DICT_STATS_VECTLEN; i++) clvector[i] = 0;
    for (i = 0; i < ht->size; i++) {
        dictEntry *he;

        if (ht->table[i] == NULL) {
            clvector[0]++;
            continue;
        }
        slots++;
        /* For each hash entry on this slot... */
        chainlen = 0;
        he = ht->table[i];
        while(he) {
            chainlen++;
            he = he->next;
        }
        clvector[(chainlen < DICT_STATS_VECTLEN) ? chainlen : (DICT_STATS_VECTLEN-1)]++;
        if (chainlen > maxchainlen) maxchainlen = chainlen;
        totchainlen += chainlen;
    }

    /* Generate human readable stats. */
    l += snprintf(buf+l,bufsize-l,
        "Hash table %d stats (%s):\n"
        " table size: %lu\n"
        " number of elements: %lu\n"
        " different slots: %lu\n"
        " max chain length: %lu\n"
        " avg chain length (counted): %.02f\n"
        " avg chain length (computed): %.02f\n"
        " Chain length distribution:\n",
        tableid, (tableid == 0) ? "main hash table" : "rehashing target",
        ht->size, ht->used, slots, maxchainlen,
        (float)totchainlen/slots, (float)ht->used/slots);

    for (i = 0; i < DICT_STATS_VECTLEN-1; i++) {
        if (clvector[i] == 0) continue;
        if (l >= bufsize) break;
        l += snprintf(buf+l,bufsize-l,
            "   %s%ld: %ld (%.02f%%)\n",
            (i == DICT_STATS_VECTLEN-1)?">= ":"",
            i, clvector[i], ((float)clvector[i]/ht->size)*100);
    }

    /* Unlike snprintf(), return the number of characters actually written. */
    if (bufsize) buf[bufsize-1] = '\0';
    return strlen(buf);
}

void dictGetStats(char *buf, size_t bufsize, dict *d) {
    size_t l;
    char *orig_buf = buf;
    size_t orig_bufsize = bufsize;

    l = _dictGetStatsHt(buf,bufsize,&d->ht[0],0);
    buf += l;
    bufsize -= l;
    if (dictIsRehashing(d) && bufsize > 0) {
        _dictGetStatsHt(buf,bufsize,&d->ht[1],1);
    }
    /* Make sure there is a NULL term at the end. */
    if (orig_bufsize) orig_buf[orig_bufsize-1] = '\0';
}

/* ------------------------------- Benchmark ---------------------------------*/

#ifdef REDIS_TEST

uint64_t hashCallback(const void *key) {
    return dictGenHashFunction((unsigned char*)key, strlen((char*)key));
}

int compareCallback(void *privdata, const void *key1, const void *key2) {
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = strlen((char*)key1);
    l2 = strlen((char*)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

void freeCallback(void *privdata, void *val) {
    DICT_NOTUSED(privdata);

    zfree(val);
}

char *stringFromLongLong(long long value) {
    char buf[32];
    int len;
    char *s;

    len = sprintf(buf,"%lld",value);
    s = zmalloc(len+1);
    memcpy(s, buf, len);
    s[len] = '\0';
    return s;
}

dictType BenchmarkDictType = {
    hashCallback,
    NULL,
    NULL,
    compareCallback,
    freeCallback,
    NULL,
    NULL
};

#define start_benchmark() start = timeInMilliseconds()
#define end_benchmark(msg) do { \
    elapsed = timeInMilliseconds()-start; \
    printf(msg ": %ld items in %lld ms\n", count, elapsed); \
} while(0)

/* ./redis-server test dict [<count> | --accurate] */
int dictTest(int argc, char **argv, int accurate) {
    long j;
    long long start, elapsed;
    dict *dict = dictCreate(&BenchmarkDictType,NULL);
    long count = 0;

    if (argc == 4) {
        if (accurate) {
            count = 5000000;
        } else {
            count = strtol(argv[3],NULL,10);
        }
    } else {
        count = 5000;
    }

    start_benchmark();
    for (j = 0; j < count; j++) {
        int retval = dictAdd(dict,stringFromLongLong(j),(void*)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Inserting");
    assert((long)dictSize(dict) == count);

    /* Wait for rehashing. */
    while (dictIsRehashing(dict)) {
        dictRehashMilliseconds(dict,100);
    }

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(j);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        zfree(key);
    }
    end_benchmark("Linear access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(j);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        zfree(key);
    }
    end_benchmark("Linear access of existing elements (2nd round)");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(rand() % count);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        zfree(key);
    }
    end_benchmark("Random access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++) {
        dictEntry *de = dictGetRandomKey(dict);
        assert(de != NULL);
    }
    end_benchmark("Accessing random keys");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(rand() % count);
        key[0] = 'X';
        dictEntry *de = dictFind(dict,key);
        assert(de == NULL);
        zfree(key);
    }
    end_benchmark("Accessing missing");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(j);
        int retval = dictDelete(dict,key);
        assert(retval == DICT_OK);
        key[0] += 17; /* Change first number to letter. */
        retval = dictAdd(dict,key,(void*)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Removing and adding");
    dictRelease(dict);
    return 0;
}
#endif
