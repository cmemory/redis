/*
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "intset.h"
#include "zmalloc.h"
#include "endianconv.h"
#include "redisassert.h"

/* Note that these encodings are ordered, so:
 * INTSET_ENC_INT16 < INTSET_ENC_INT32 < INTSET_ENC_INT64. */
// 编码类型：INT16、INT32、INT64，注意他们是有序的。
#define INTSET_ENC_INT16 (sizeof(int16_t))
#define INTSET_ENC_INT32 (sizeof(int32_t))
#define INTSET_ENC_INT64 (sizeof(int64_t))

/* Return the required encoding for the provided value. */
// 根据提供的value值确定我们应该编码的类型。
static uint8_t _intsetValueEncoding(int64_t v) {
    if (v < INT32_MIN || v > INT32_MAX)
        // 超出了INT32范围，则使用INT64
        return INTSET_ENC_INT64;
    else if (v < INT16_MIN || v > INT16_MAX)
        // 否则超出了INT16范围，使用INT32
        return INTSET_ENC_INT32;
    else
        // 否则使用INT16
        return INTSET_ENC_INT16;
}

/* Return the value at pos, given an encoding. */
// 对于指定编码方式的intset，获取pos index的数据。
static int64_t _intsetGetEncoded(intset *is, int pos, uint8_t enc) {
    int64_t v64;
    int32_t v32;
    int16_t v16;

    // 直接 is->contents+pos，指针运算得到对应位置的指针。注意不同类型指针+pos，指针前进的字节数量是不一样的。
    // 找到了pos索引处的指针，则直接memcpy复制连续字节，即为我们获取的数据。注意小端存储转换。
    if (enc == INTSET_ENC_INT64) {
        memcpy(&v64,((int64_t*)is->contents)+pos,sizeof(v64));
        memrev64ifbe(&v64);
        return v64;
    } else if (enc == INTSET_ENC_INT32) {
        memcpy(&v32,((int32_t*)is->contents)+pos,sizeof(v32));
        memrev32ifbe(&v32);
        return v32;
    } else {
        memcpy(&v16,((int16_t*)is->contents)+pos,sizeof(v16));
        memrev16ifbe(&v16);
        return v16;
    }
}

/* Return the value at pos, using the configured encoding. */
// 返回intset中pos索引处的值，编码intset中已指定。
static int64_t _intsetGet(intset *is, int pos) {
    return _intsetGetEncoded(is,pos,intrev32ifbe(is->encoding));
}

/* Set the value at pos, using the configured encoding. */
// 设置intset中pos索引位置的值为value，编码intset中已指定。
static void _intsetSet(intset *is, int pos, int64_t value) {
    // 获取intset中的编码
    uint32_t encoding = intrev32ifbe(is->encoding);

    // 根据遍历我们将数据写入pos位置。实际上就是int*类型的数组，pos下标索引处赋值。
    // 需要注意数据小端模式转换。
    if (encoding == INTSET_ENC_INT64) {
        ((int64_t*)is->contents)[pos] = value;
        memrev64ifbe(((int64_t*)is->contents)+pos);
    } else if (encoding == INTSET_ENC_INT32) {
        ((int32_t*)is->contents)[pos] = value;
        memrev32ifbe(((int32_t*)is->contents)+pos);
    } else {
        ((int16_t*)is->contents)[pos] = value;
        memrev16ifbe(((int16_t*)is->contents)+pos);
    }
}

/* Create an empty intset. */
// 创建一个空的intset，默认使用INT16编码，后续会根据插入的数据大小来扩展编码。
intset *intsetNew(void) {
    intset *is = zmalloc(sizeof(intset));
    is->encoding = intrev32ifbe(INTSET_ENC_INT16);
    is->length = 0;
    return is;
}

/* Resize the intset */
// resize intset，当我们新增一个元素或移除一个元素时，会调用这个函数来调整intset空间大小。
static intset *intsetResize(intset *is, uint32_t len) {
    uint32_t size = len*intrev32ifbe(is->encoding);
    is = zrealloc(is,sizeof(intset)+size);
    return is;
}

/* Search for the position of "value". Return 1 when the value was found and
 * sets "pos" to the position of the value within the intset. Return 0 when
 * the value is not present in the intset and sets "pos" to the position
 * where "value" can be inserted. */
// 根据value来搜索对应的pos索引位置。因为intset中数据是有序的，所以我们可能使用二分来进行查找该元素的位置，以及不存在时该元素应该插入的位置。
// 如果找到该value，则函数返回1，并在返回参数pos填充索引位置。如果没找到，则返回0，并在返回参数pos填充该value应该插入的位置。
static uint8_t intsetSearch(intset *is, int64_t value, uint32_t *pos) {
    int min = 0, max = intrev32ifbe(is->length)-1, mid = -1;
    int64_t cur = -1;

    /* The value can never be found when the set is empty */
    // 如果intset为空时，不会找到元素，则直接返回0，待插入位置索引也为0。
    if (intrev32ifbe(is->length) == 0) {
        if (pos) *pos = 0;
        return 0;
    } else {
        /* Check for the case where we know we cannot find the value,
         * but do know the insert position. */
        // 检查特殊情况。对比value是否大于最大元素，或小于最小元素，这两种情况，我们知道会在尾部或头部插入元素。
        if (value > _intsetGet(is,max)) {
            if (pos) *pos = intrev32ifbe(is->length);
            return 0;
        } else if (value < _intsetGet(is,0)) {
            if (pos) *pos = 0;
            return 0;
        }
    }

    // 二分查找。min之前的元素始终是小于value的，max之后的元素始终是大于value的。如果元素不存在，则我们插入位置为min，也即max后一个位置。
    while(max >= min) {
        mid = ((unsigned int)min + (unsigned int)max) >> 1;
        cur = _intsetGet(is,mid);
        if (value > cur) {
            min = mid+1;
        } else if (value < cur) {
            max = mid-1;
        } else {
            break;
        }
    }

    // 两种情况会退出前面的循环：1、value==cur，找到了value，则mid即为索引位置。2、max<min，没找到value，则min位置即为待插入位置。
    if (value == cur) {
        if (pos) *pos = mid;
        return 1;
    } else {
        if (pos) *pos = min;
        return 0;
    }
}

/* Upgrades the intset to a larger encoding and inserts the given integer. */
// 升级intset为更大的编码，并插入给定的数字。
static intset *intsetUpgradeAndAdd(intset *is, int64_t value) {
    // 获取当前编码，以及待升级的新编码
    uint8_t curenc = intrev32ifbe(is->encoding);
    uint8_t newenc = _intsetValueEncoding(value);
    // 获取原intset元素个数，扩容后元素个数为length+1
    int length = intrev32ifbe(is->length);
    // 因为新加入元素导致扩容的，所以新元素肯是要比当前intset中所有元素更大或者更小（负数）。这里处理是在头部还是尾部插入元素。
    int prepend = value < 0 ? 1 : 0;

    /* First set new encoding and resize */
    // 设置新的编码类型，并resize intset空间。
    is->encoding = intrev32ifbe(newenc);
    is = intsetResize(is,intrev32ifbe(is->length)+1);

    /* Upgrade back-to-front so we don't overwrite values.
     * Note that the "prepend" variable is used to make sure we have an empty
     * space at either the beginning or the end of the intset. */
    // 依次从后往前设置值，因为值的编码方式变了，所以我们不能单纯的memmove来内存移动处理，只能get一个值然后再set到最终位置上。
    // 如果value是负的，则会小于当前所有值，所以需要在头部插入，我们需要将所有数据后移1个位置（这里巧妙使用了prepend值）。
    // 如果value是正的，则大于当前所有值，需要在尾部插入，prepend=0，我们新写入的数据的索引位置都不变。
    while(length--)
        _intsetSet(is,length+prepend,_intsetGetEncoded(is,length,curenc));

    /* Set the value at the beginning or the end. */
    // 将待插入的value数据放到头部或者尾部。prepend为0表示尾部，为1表示头部。
    if (prepend)
        _intsetSet(is,0,value);
    else
        _intsetSet(is,intrev32ifbe(is->length),value);
    // intset中元素数+1
    is->length = intrev32ifbe(intrev32ifbe(is->length)+1);
    return is;
}

// 将intset中从from开始到末尾的元素，移动到to为开始的位置处。
static void intsetMoveTail(intset *is, uint32_t from, uint32_t to) {
    void *src, *dst;
    // bytes为移动的字节数，这里是元素个数，后面会乘以对应编码字节数来修正。
    uint32_t bytes = intrev32ifbe(is->length)-from;
    // 编码类型
    uint32_t encoding = intrev32ifbe(is->encoding);

    // 根据不同的编码类型获取src、dst位置的指针，以及移动的字节数。
    if (encoding == INTSET_ENC_INT64) {
        src = (int64_t*)is->contents+from;
        dst = (int64_t*)is->contents+to;
        bytes *= sizeof(int64_t);
    } else if (encoding == INTSET_ENC_INT32) {
        src = (int32_t*)is->contents+from;
        dst = (int32_t*)is->contents+to;
        bytes *= sizeof(int32_t);
    } else {
        src = (int16_t*)is->contents+from;
        dst = (int16_t*)is->contents+to;
        bytes *= sizeof(int16_t);
    }
    // memmove处理字节数据的移动，mommove不会因为内存覆盖导致数据问题。
    memmove(dst,src,bytes);
}

/* Insert an integer in the intset */
// 向intset插入一个数字，函数返回指向intset的指针，因为插入后可能扩容导致intset发生变化，所以我们总是需要使用返回的intset指针。
// 如果插入成功success会返回1；否则value存在，没有任何操作，我们返回0。
intset *intsetAdd(intset *is, int64_t value, uint8_t *success) {
    // 计算该value应该使用的编码
    uint8_t valenc = _intsetValueEncoding(value);
    uint32_t pos;
    if (success) *success = 1;

    /* Upgrade encoding if necessary. If we need to upgrade, we know that
     * this value should be either appended (if > 0) or prepended (if < 0),
     * because it lies outside the range of existing values. */
    // 如果value应该使用的编码 大于 当前intset使用的编码，则我们需要升级编码，且此时我们知道value应该是插入在头部或者尾部。
    if (valenc > intrev32ifbe(is->encoding)) {
        /* This always succeeds, so we don't need to curry *success. */
        // 升级编码插入数据总是成功的，所以不需要管*success，因为前面我们给的默认值就是1表示成功插入数据。
        return intsetUpgradeAndAdd(is,value);
    } else {
        /* Abort if the value is already present in the set.
         * This call will populate "pos" with the right position to insert
         * the value when it cannot be found. */
        // 在intset中search该value，如果找到了，则我们不需要插入，*success设为0。否则我们将要在pos处插入新元素。
        if (intsetSearch(is,value,&pos)) {
            if (success) *success = 0;
            return is;
        }

        // 需要插入一个元素，我们先resize intset。
        // 然后如果需要的话，将pos位置开始后面的元素全部后移一个index位置。注意如果我们插入位置是在末尾的话，则不需要移动元素。
        is = intsetResize(is,intrev32ifbe(is->length)+1);
        if (pos < intrev32ifbe(is->length)) intsetMoveTail(is,pos,pos+1);
    }

    // 前面找到了待插入的pos位置，也处理resize以及元素移动腾出了该位置的空间，则这里可以直接set，并更新总的元素个数。
    _intsetSet(is,pos,value);
    is->length = intrev32ifbe(intrev32ifbe(is->length)+1);
    return is;
}

/* Delete integer from intset */
// 从intset中删除一个元素。如如果删除成功返回1；否则元素不存在什么都没干，返回0。
intset *intsetRemove(intset *is, int64_t value, int *success) {
    // 计算value应使用的编码
    uint8_t valenc = _intsetValueEncoding(value);
    uint32_t pos;
    if (success) *success = 0;

    // 当value的编码超出了当前intset的编码时，当前intset不能容纳该value，可以快速返回，而不用去search查询。
    // 如果value编码在当前intset的编码内时，就需要使用二分进行查询了。
    if (valenc <= intrev32ifbe(is->encoding) && intsetSearch(is,value,&pos)) {
        uint32_t len = intrev32ifbe(is->length);

        /* We know we can delete */
        // 查到了对应元素，我们知道能删除，设置*success为1。
        if (success) *success = 1;

        /* Overwrite value with tail and update length */
        // 如果需要，则将查到的pos后面的元素都前移1个索引位置。当删除的是最后一个元素时，我们不需要移动数据
        if (pos < (len-1)) intsetMoveTail(is,pos+1,pos);
        // 移动数据完成，此时最后一个位置数据是无效的，直接resize缩容即可，length长度减1。
        is = intsetResize(is,len-1);
        is->length = intrev32ifbe(len-1);
    }
    return is;
}

/* Determine whether a value belongs to this set */
// 判断一个value是否属于当前intset。
uint8_t intsetFind(intset *is, int64_t value) {
    uint8_t valenc = _intsetValueEncoding(value);
    // 先保证value的编码在intset编码内，从而可以通过&&短路快速返回，不用search。
    // 如果在intset编码内，search能查到，则属于当前intset，否则不属于。
    return valenc <= intrev32ifbe(is->encoding) && intsetSearch(is,value,NULL);
}

/* Return random member */
// 随机返回intset中一个元素。[0, len]随机选一个索引，然后取该索引元素返回。
int64_t intsetRandom(intset *is) {
    uint32_t len = intrev32ifbe(is->length);
    assert(len); /* avoid division by zero on corrupt intset payload. */
    return _intsetGet(is,rand()%len);
}

/* Get the value at the given position. When this position is
 * out of range the function returns 0, when in range it returns 1. */
// 给定pos索引，返回对应的value。如果pos在范围外，函数返回0；否则返回1表示能查到元素。
uint8_t intsetGet(intset *is, uint32_t pos, int64_t *value) {
    if (pos < intrev32ifbe(is->length)) {
        *value = _intsetGet(is,pos);
        return 1;
    }
    return 0;
}

/* Return intset length */
// 返回intset中总元素个数。
uint32_t intsetLen(const intset *is) {
    return intrev32ifbe(is->length);
}

/* Return intset blob size in bytes. */
// 返回intset总的占用字节数。
size_t intsetBlobLen(intset *is) {
    return sizeof(intset)+(size_t)intrev32ifbe(is->length)*intrev32ifbe(is->encoding);
}

/* Validate the integrity of the data structure.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we make sure there are no duplicate or out of order records. */
// 验证intset结构的完整性。当deep为0时，只验证header；当deep为1时，需要验证数据没有重复，且数据是有序的。
int intsetValidateIntegrity(const unsigned char *p, size_t size, int deep) {
    intset *is = (intset *)p;
    /* check that we can actually read the header. */
    // 确保我们有一个完整的header。
    if (size < sizeof(*is))
        return 0;

    uint32_t encoding = intrev32ifbe(is->encoding);

    // 我们编码只能是3种中的一个。
    size_t record_size;
    if (encoding == INTSET_ENC_INT64) {
        record_size = INTSET_ENC_INT64;
    } else if (encoding == INTSET_ENC_INT32) {
        record_size = INTSET_ENC_INT32;
    } else if (encoding == INTSET_ENC_INT16){
        record_size = INTSET_ENC_INT16;
    } else {
        return 0;
    }

    /* check that the size matchies (all records are inside the buffer). */
    // 检查header+数据总长度与传入的size匹配，即所有的记录都在p指向的buffer中。
    uint32_t count = intrev32ifbe(is->length);
    if (sizeof(*is) + count*record_size != size)
        return 0;

    /* check that the set is not empty. */
    // 检查set非空
    if (count==0)
        return 0;

    // 如果不需要检查重复元素，则直接可以返回了。
    if (!deep)
        return 1;

    /* check that there are no dup or out of order records. */
    // 验证数据没有重复且是有序的。
    // 遍历元素判断，当前元素要比前一个元素大。如果发现当前元素小于或等于前一个元素，则ziplist数据结构不正确。
    int64_t prev = _intsetGet(is,0);
    for (uint32_t i=1; i<count; i++) {
        int64_t cur = _intsetGet(is,i);
        if (cur <= prev)
            return 0;
        prev = cur;
    }

    return 1;
}

#ifdef REDIS_TEST
#include <sys/time.h>
#include <time.h>

#if 0
static void intsetRepr(intset *is) {
    for (uint32_t i = 0; i < intrev32ifbe(is->length); i++) {
        printf("%lld\n", (uint64_t)_intsetGet(is,i));
    }
    printf("\n");
}

static void error(char *err) {
    printf("%s\n", err);
    exit(1);
}
#endif

static void ok(void) {
    printf("OK\n");
}

static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

static intset *createSet(int bits, int size) {
    uint64_t mask = (1<<bits)-1;
    uint64_t value;
    intset *is = intsetNew();

    for (int i = 0; i < size; i++) {
        if (bits > 32) {
            value = (rand()*rand()) & mask;
        } else {
            value = rand() & mask;
        }
        is = intsetAdd(is,value,NULL);
    }
    return is;
}

static void checkConsistency(intset *is) {
    for (uint32_t i = 0; i < (intrev32ifbe(is->length)-1); i++) {
        uint32_t encoding = intrev32ifbe(is->encoding);

        if (encoding == INTSET_ENC_INT16) {
            int16_t *i16 = (int16_t*)is->contents;
            assert(i16[i] < i16[i+1]);
        } else if (encoding == INTSET_ENC_INT32) {
            int32_t *i32 = (int32_t*)is->contents;
            assert(i32[i] < i32[i+1]);
        } else {
            int64_t *i64 = (int64_t*)is->contents;
            assert(i64[i] < i64[i+1]);
        }
    }
}

#define UNUSED(x) (void)(x)
int intsetTest(int argc, char **argv, int accurate) {
    uint8_t success;
    int i;
    intset *is;
    srand(time(NULL));

    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);

    printf("Value encodings: "); {
        assert(_intsetValueEncoding(-32768) == INTSET_ENC_INT16);
        assert(_intsetValueEncoding(+32767) == INTSET_ENC_INT16);
        assert(_intsetValueEncoding(-32769) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(+32768) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(-2147483648) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(+2147483647) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(-2147483649) == INTSET_ENC_INT64);
        assert(_intsetValueEncoding(+2147483648) == INTSET_ENC_INT64);
        assert(_intsetValueEncoding(-9223372036854775808ull) ==
                    INTSET_ENC_INT64);
        assert(_intsetValueEncoding(+9223372036854775807ull) ==
                    INTSET_ENC_INT64);
        ok();
    }

    printf("Basic adding: "); {
        is = intsetNew();
        is = intsetAdd(is,5,&success); assert(success);
        is = intsetAdd(is,6,&success); assert(success);
        is = intsetAdd(is,4,&success); assert(success);
        is = intsetAdd(is,4,&success); assert(!success);
        ok();
        zfree(is);
    }

    printf("Large number of random adds: "); {
        uint32_t inserts = 0;
        is = intsetNew();
        for (i = 0; i < 1024; i++) {
            is = intsetAdd(is,rand()%0x800,&success);
            if (success) inserts++;
        }
        assert(intrev32ifbe(is->length) == inserts);
        checkConsistency(is);
        ok();
        zfree(is);
    }

    printf("Upgrade from int16 to int32: "); {
        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        assert(intsetFind(is,32));
        assert(intsetFind(is,65535));
        checkConsistency(is);
        zfree(is);

        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,-65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        assert(intsetFind(is,32));
        assert(intsetFind(is,-65535));
        checkConsistency(is);
        ok();
        zfree(is);
    }

    printf("Upgrade from int16 to int64: "); {
        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,32));
        assert(intsetFind(is,4294967295));
        checkConsistency(is);
        zfree(is);

        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,-4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,32));
        assert(intsetFind(is,-4294967295));
        checkConsistency(is);
        ok();
        zfree(is);
    }

    printf("Upgrade from int32 to int64: "); {
        is = intsetNew();
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        is = intsetAdd(is,4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,65535));
        assert(intsetFind(is,4294967295));
        checkConsistency(is);
        zfree(is);

        is = intsetNew();
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        is = intsetAdd(is,-4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,65535));
        assert(intsetFind(is,-4294967295));
        checkConsistency(is);
        ok();
        zfree(is);
    }

    printf("Stress lookups: "); {
        long num = 100000, size = 10000;
        int i, bits = 20;
        long long start;
        is = createSet(bits,size);
        checkConsistency(is);

        start = usec();
        for (i = 0; i < num; i++) intsetSearch(is,rand() % ((1<<bits)-1),NULL);
        printf("%ld lookups, %ld element set, %lldusec\n",
               num,size,usec()-start);
        zfree(is);
    }

    printf("Stress add+delete: "); {
        int i, v1, v2;
        is = intsetNew();
        for (i = 0; i < 0xffff; i++) {
            v1 = rand() % 0xfff;
            is = intsetAdd(is,v1,NULL);
            assert(intsetFind(is,v1));

            v2 = rand() % 0xfff;
            is = intsetRemove(is,v2,NULL);
            assert(!intsetFind(is,v2));
        }
        checkConsistency(is);
        ok();
        zfree(is);
    }

    return 0;
}
#endif
