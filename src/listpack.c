/* Listpack -- A lists of strings serialization format
 *
 * This file implements the specification you can find at:
 *
 *  https://github.com/antirez/listpack
 *
 * Copyright (c) 2017, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2020, Redis Labs, Inc
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

#include <stdint.h>
#include <limits.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "listpack.h"
#include "listpack_malloc.h"
#include "redisassert.h"

// listpack的header大小，6字节，包含32bit的总字节长度 + 16bit的总元素个数。
#define LP_HDR_SIZE 6       /* 32 bit total len + 16 bit number of elements. */
// 当16bit总元素个数值为UINT16_MAX时，我们需要遍历整个listpack才能获取到总元素数。
#define LP_HDR_NUMELE_UNKNOWN UINT16_MAX
// 最大INT类型编码长度，9字节
#define LP_MAX_INT_ENCODING_LEN 9
// 前一个元素长度的编码长度。我们最大使用5字节来编码长度，首字节表示type，后面4字节存储长度。
#define LP_MAX_BACKLEN_SIZE 5
#define LP_MAX_ENTRY_BACKLEN 34359738367ULL
// 数据编码类型，数值还是字符串。
#define LP_ENCODING_INT 0
#define LP_ENCODING_STRING 1

// 7bit数值类型，表示小的非负整数0～127。
// 数据格式：0xxxxxxx
#define LP_ENCODING_7BIT_UINT 0
#define LP_ENCODING_7BIT_UINT_MASK 0x80
#define LP_ENCODING_IS_7BIT_UINT(byte) (((byte)&LP_ENCODING_7BIT_UINT_MASK)==LP_ENCODING_7BIT_UINT)

// 6bit字符串类型，小字符串，存储字符串长度0～63。
// 数据格式：10xxxxxx
#define LP_ENCODING_6BIT_STR 0x80
#define LP_ENCODING_6BIT_STR_MASK 0xC0
#define LP_ENCODING_IS_6BIT_STR(byte) (((byte)&LP_ENCODING_6BIT_STR_MASK)==LP_ENCODING_6BIT_STR)

// 13bit数值类型
// 数据格式：110xxxxx yyyyyyyy
#define LP_ENCODING_13BIT_INT 0xC0
#define LP_ENCODING_13BIT_INT_MASK 0xE0
#define LP_ENCODING_IS_13BIT_INT(byte) (((byte)&LP_ENCODING_13BIT_INT_MASK)==LP_ENCODING_13BIT_INT)

// 12bit字符串类型，中等长度字符串，存储字符串长度0～4095。
// 数据格式：1110xxxx yyyyyyyy
#define LP_ENCODING_12BIT_STR 0xE0
#define LP_ENCODING_12BIT_STR_MASK 0xF0
#define LP_ENCODING_IS_12BIT_STR(byte) (((byte)&LP_ENCODING_12BIT_STR_MASK)==LP_ENCODING_12BIT_STR)

// 16bit数值类型
// 数据格式：11110001 aaaaaaaa bbbbbbbb
#define LP_ENCODING_16BIT_INT 0xF1
#define LP_ENCODING_16BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_16BIT_INT(byte) (((byte)&LP_ENCODING_16BIT_INT_MASK)==LP_ENCODING_16BIT_INT)

// 24bit数值类型
// 数据格式：11110010 aaaaaaaa bbbbbbbb cccccccc
#define LP_ENCODING_24BIT_INT 0xF2
#define LP_ENCODING_24BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_24BIT_INT(byte) (((byte)&LP_ENCODING_24BIT_INT_MASK)==LP_ENCODING_24BIT_INT)

// 32bit数值类型
// 数据格式：11110011 aaaaaaaa bbbbbbbb cccccccc dddddddd
#define LP_ENCODING_32BIT_INT 0xF3
#define LP_ENCODING_32BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_32BIT_INT(byte) (((byte)&LP_ENCODING_32BIT_INT_MASK)==LP_ENCODING_32BIT_INT)

// 64bit数值类型
// 数据格式：11110100 aaaaaaaa ... hhhhhhhh
#define LP_ENCODING_64BIT_INT 0xF4
#define LP_ENCODING_64BIT_INT_MASK 0xFF
#define LP_ENCODING_IS_64BIT_INT(byte) (((byte)&LP_ENCODING_64BIT_INT_MASK)==LP_ENCODING_64BIT_INT)

// 32bit字符串类型，大字符串，需要单独4个字节存储字符串长度。
// 数据格式：11110000 aaaaaaaa bbbbbbbb cccccccc dddddddd
#define LP_ENCODING_32BIT_STR 0xF0
#define LP_ENCODING_32BIT_STR_MASK 0xFF
#define LP_ENCODING_IS_32BIT_STR(byte) (((byte)&LP_ENCODING_32BIT_STR_MASK)==LP_ENCODING_32BIT_STR)

// 结束符
#define LP_EOF 0xFF

// 获取6bit的字符串的长度
#define LP_ENCODING_6BIT_STR_LEN(p) ((p)[0] & 0x3F)
// 获取12bit的字符串长度
#define LP_ENCODING_12BIT_STR_LEN(p) ((((p)[0] & 0xF) << 8) | (p)[1])
// 获取32bit的字符串长度。注意第一个字符表示type类型，长度是后面4个字节，所以index从1开始。
#define LP_ENCODING_32BIT_STR_LEN(p) (((uint32_t)(p)[1]<<0) | \
                                      ((uint32_t)(p)[2]<<8) | \
                                      ((uint32_t)(p)[3]<<16) | \
                                      ((uint32_t)(p)[4]<<24))

// 获取listpack总的字节数
#define lpGetTotalBytes(p)           (((uint32_t)(p)[0]<<0) | \
                                      ((uint32_t)(p)[1]<<8) | \
                                      ((uint32_t)(p)[2]<<16) | \
                                      ((uint32_t)(p)[3]<<24))

// 获取listpack总的元素个数。
#define lpGetNumElements(p)          (((uint32_t)(p)[4]<<0) | \
                                      ((uint32_t)(p)[5]<<8))
// 设置listpack总的字节数
#define lpSetTotalBytes(p,v) do { \
    (p)[0] = (v)&0xff; \
    (p)[1] = ((v)>>8)&0xff; \
    (p)[2] = ((v)>>16)&0xff; \
    (p)[3] = ((v)>>24)&0xff; \
} while(0)

// 设置listpack总的元素数
#define lpSetNumElements(p,v) do { \
    (p)[4] = (v)&0xff; \
    (p)[5] = ((v)>>8)&0xff; \
} while(0)

/* Validates that 'p' is not ouside the listpack.
 * All function that return a pointer to an element in the listpack will assert
 * that this element is valid, so it can be freely used.
 * Generally functions such lpNext and lpDelete assume the input pointer is
 * already validated (since it's the return value of another function). */
// 验证p指向listpack的某个entry位置。
// 所有返回指向listpack某个元素指针的函数，都会assert确保指针有效，所以指针可以自由使用。
// 通常像lpNext和lpDelete都会认为传入的指针是有效的，已验证过的。因为他们都是其他函数返回的指向元素的指针。
#define ASSERT_INTEGRITY(lp, p) do { \
    assert((p) >= (lp)+LP_HDR_SIZE && (p) < (lp)+lpGetTotalBytes((lp))); \
} while (0)

/* Similar to the above, but validates the entire element lenth rather than just
 * it's pointer. */
// 与 ASSERT_INTEGRITY 类似，不过这里更严格，要求验证p指向的整个entry（长度为len）都在listpack中。
#define ASSERT_INTEGRITY_LEN(lp, p, len) do { \
    assert((p) >= (lp)+LP_HDR_SIZE && (p)+(len) < (lp)+lpGetTotalBytes((lp))); \
} while (0)

/* Convert a string into a signed 64 bit integer.
 * The function returns 1 if the string could be parsed into a (non-overflowing)
 * signed 64 bit int, 0 otherwise. The 'value' will be set to the parsed value
 * when the function returns success.
 *
 * Note that this function demands that the string strictly represents
 * a int64 value: no spaces or other characters before or after the string
 * representing the number are accepted, nor zeroes at the start if not
 * for the string "0" representing the zero number.
 *
 * Because of its strictness, it is safe to use this function to check if
 * you can convert a string into a long long, and obtain back the string
 * from the number without any loss in the string representation. *
 *
 * -----------------------------------------------------------------------------
 *
 * Credits: this function was adapted from the Redis source code, file
 * "utils.c", function string2ll(), and is copyright:
 *
 * Copyright(C) 2011, Pieter Noordhuis
 * Copyright(C) 2011, Salvatore Sanfilippo
 *
 * The function is released under the BSD 3-clause license.
 */
// 将一个string转为64位有符号数。能转成功，则值用value返回，函数返回1；否则函数返回0。
// 注意：这个函数要求字符串严格表示一个64位数字。在字符串开头或结尾不能有空格或其他字符。如果不是"0"，则开头也不应包含'0'。
// 因为它的严格性，使用此函数检查是否可以将字符串转为数字，然后将获取到的数字再转为string，这操作是安全的，因为转换后字符串表示形式一致。
int lpStringToInt64(const char *s, unsigned long slen, int64_t *value) {
    const char *p = s;
    unsigned long plen = 0;
    int negative = 0;
    uint64_t v;

    if (plen == slen)
        return 0;

    /* Special case: first and only digit is 0. */
    if (slen == 1 && p[0] == '0') {
        if (value != NULL) *value = 0;
        return 1;
    }

    if (p[0] == '-') {
        negative = 1;
        p++; plen++;

        /* Abort on only a negative sign. */
        if (plen == slen)
            return 0;
    }

    /* First digit should be 1-9, otherwise the string should just be 0. */
    if (p[0] >= '1' && p[0] <= '9') {
        v = p[0]-'0';
        p++; plen++;
    } else if (p[0] == '0' && slen == 1) {
        *value = 0;
        return 1;
    } else {
        return 0;
    }

    while (plen < slen && p[0] >= '0' && p[0] <= '9') {
        if (v > (UINT64_MAX / 10)) /* Overflow. */
            return 0;
        v *= 10;

        if (v > (UINT64_MAX - (p[0]-'0'))) /* Overflow. */
            return 0;
        v += p[0]-'0';

        p++; plen++;
    }

    /* Return if not all bytes were used. */
    if (plen < slen)
        return 0;

    if (negative) {
        if (v > ((uint64_t)(-(INT64_MIN+1))+1)) /* Overflow. */
            return 0;
        if (value != NULL) *value = -v;
    } else {
        if (v > INT64_MAX) /* Overflow. */
            return 0;
        if (value != NULL) *value = v;
    }
    return 1;
}

/* Create a new, empty listpack.
 * On success the new listpack is returned, otherwise an error is returned.
 * Pre-allocate at least `capacity` bytes of memory,
 * over-allocated memory can be shrinked by `lpShrinkToFit`.
 * */
// 创建一个新的空的listpack。
// 可预分配最少 LP_HDR_SIZE+1 字节的内存。过度分配的内存可以通过lpShrinkToFit来处理缩小。
unsigned char *lpNew(size_t capacity) {
    unsigned char *lp = lp_malloc(capacity > LP_HDR_SIZE+1 ? capacity : LP_HDR_SIZE+1);
    if (lp == NULL) return NULL;
    // 设置总的字节数，总元素数，以及结束符。
    lpSetTotalBytes(lp,LP_HDR_SIZE+1);
    lpSetNumElements(lp,0);
    lp[LP_HDR_SIZE] = LP_EOF;
    return lp;
}

/* Free the specified listpack. */
// 释放指定的listpack。
void lpFree(unsigned char *lp) {
    lp_free(lp);
}

/* Shrink the memory to fit. */
// 如果使用的总字节数 小于 分配的内存字节数，则realloc将没使用的空间释放掉。
unsigned char* lpShrinkToFit(unsigned char *lp) {
    size_t size = lpGetTotalBytes(lp);
    if (size < lp_malloc_size(lp)) {
        return lp_realloc(lp, size);
    } else {
        return lp;
    }
}

/* Given an element 'ele' of size 'size', determine if the element can be
 * represented inside the listpack encoded as integer, and returns
 * LP_ENCODING_INT if so. Otherwise returns LP_ENCODING_STR if no integer
 * encoding is possible.
 *
 * If the LP_ENCODING_INT is returned, the function stores the integer encoded
 * representation of the element in the 'intenc' buffer.
 *
 * Regardless of the returned encoding, 'enclen' is populated by reference to
 * the number of bytes that the string or integer encoded element will require
 * in order to be represented. */
// 给定一个元素ele以及该元素长度，判断是否能编码为数字或字符串，返回编码类型。
// 如果能编码为数字，则参数intenc返回编码后的内容，enclen返回编码数字需要的字节数。
// 如果不能编码为数字，那么只能用字符串存储了，enclen返回编码字符串长度需要的字节数。
int lpEncodeGetType(unsigned char *ele, uint32_t size, unsigned char *intenc, uint64_t *enclen) {
    int64_t v;
    if (lpStringToInt64((const char*)ele, size, &v)) {
        if (v >= 0 && v <= 127) {
            /* Single byte 0-127 integer. */
            intenc[0] = v;
            *enclen = 1;
        } else if (v >= -4096 && v <= 4095) {
            /* 13 bit integer. */
            if (v < 0) v = ((int64_t)1<<13)+v;
            intenc[0] = (v>>8)|LP_ENCODING_13BIT_INT;
            intenc[1] = v&0xff;
            *enclen = 2;
        } else if (v >= -32768 && v <= 32767) {
            /* 16 bit integer. */
            if (v < 0) v = ((int64_t)1<<16)+v;
            intenc[0] = LP_ENCODING_16BIT_INT;
            intenc[1] = v&0xff;
            intenc[2] = v>>8;
            *enclen = 3;
        } else if (v >= -8388608 && v <= 8388607) {
            /* 24 bit integer. */
            if (v < 0) v = ((int64_t)1<<24)+v;
            intenc[0] = LP_ENCODING_24BIT_INT;
            intenc[1] = v&0xff;
            intenc[2] = (v>>8)&0xff;
            intenc[3] = v>>16;
            *enclen = 4;
        } else if (v >= -2147483648 && v <= 2147483647) {
            /* 32 bit integer. */
            if (v < 0) v = ((int64_t)1<<32)+v;
            intenc[0] = LP_ENCODING_32BIT_INT;
            intenc[1] = v&0xff;
            intenc[2] = (v>>8)&0xff;
            intenc[3] = (v>>16)&0xff;
            intenc[4] = v>>24;
            *enclen = 5;
        } else {
            /* 64 bit integer. */
            uint64_t uv = v;
            intenc[0] = LP_ENCODING_64BIT_INT;
            intenc[1] = uv&0xff;
            intenc[2] = (uv>>8)&0xff;
            intenc[3] = (uv>>16)&0xff;
            intenc[4] = (uv>>24)&0xff;
            intenc[5] = (uv>>32)&0xff;
            intenc[6] = (uv>>40)&0xff;
            intenc[7] = (uv>>48)&0xff;
            intenc[8] = uv>>56;
            *enclen = 9;
        }
        return LP_ENCODING_INT;
    } else {
        if (size < 64) *enclen = 1+size;
        else if (size < 4096) *enclen = 2+size;
        else *enclen = 5+size;
        return LP_ENCODING_STRING;
    }
}

/* Store a reverse-encoded variable length field, representing the length
 * of the previous element of size 'l', in the target buffer 'buf'.
 * The function returns the number of bytes used to encode it, from
 * 1 to 5. If 'buf' is NULL the function just returns the number of bytes
 * needed in order to encode the backlen. */
// 因为listpack与ziplist最大不同是，长度放在了entry的后面。这里我们需要逆向编码来处理长度。
// 这样对于正向遍历，我们可以根据entry数据长度，从而确定len编码长度，进一步确定下一个entry的位置。
// 对于逆向遍历，我们逆向编码可以快速获取前一个entry总长度，从而获取到前一个entry的位置。
// 逆向编码：
//      小于127的数：0xxxxxxx。
//      128～16383：0xxxxxxx 1xxxxxxx。
//      依次：0xxxxxxx 1xxxxxxx ... 1xxxxxxx
// 我们读取解析时从后往前按字节读取，如果字节的首字符为1则没结束，为0则当前是长度的最后一个字节。
unsigned long lpEncodeBacklen(unsigned char *buf, uint64_t l) {
    if (l <= 127) {
        if (buf) buf[0] = l;
        return 1;
    } else if (l < 16383) {
        if (buf) {
            buf[0] = l>>7;
            buf[1] = (l&127)|128;
        }
        return 2;
    } else if (l < 2097151) {
        if (buf) {
            buf[0] = l>>14;
            buf[1] = ((l>>7)&127)|128;
            buf[2] = (l&127)|128;
        }
        return 3;
    } else if (l < 268435455) {
        if (buf) {
            buf[0] = l>>21;
            buf[1] = ((l>>14)&127)|128;
            buf[2] = ((l>>7)&127)|128;
            buf[3] = (l&127)|128;
        }
        return 4;
    } else {
        if (buf) {
            buf[0] = l>>28;
            buf[1] = ((l>>21)&127)|128;
            buf[2] = ((l>>14)&127)|128;
            buf[3] = ((l>>7)&127)|128;
            buf[4] = (l&127)|128;
        }
        return 5;
    }
}

/* Decode the backlen and returns it. If the encoding looks invalid (more than
 * 5 bytes are used), UINT64_MAX is returned to report the problem. */
// 解析前一个entry的长度。如果长度编码是无效的（如解析时发现大于了5字节），则返回UINT64_MAX来标识错误。
// 注意当前传入的参数p指向前一个entry的最后一个字节。
uint64_t lpDecodeBacklen(unsigned char *p) {
    uint64_t val = 0;
    uint64_t shift = 0;
    do {
        // 处理当前字节信息。后7位要追加到val左侧（即左移shift，与val或操作）。
        val |= (uint64_t)(p[0] & 127) << shift;
        // 如果当前处理的字节首位是0，则长度解析完了，跳出。
        if (!(p[0] & 128)) break;
        // 每处理1字节，我们处理的长度信息实际上是7位。
        shift += 7;
        // 跳到左边一个字节，继续处理。
        p--;
        // 如果已经解析了5字节，但最后首位不是0，则编码格式错误，返回UINT64_MAX。
        if (shift > 28) return UINT64_MAX;
    } while(1);
    // 成功解析长度，返回。
    return val;
}

/* Encode the string element pointed by 's' of size 'len' in the target
 * buffer 'buf'. The function should be called with 'buf' having always enough
 * space for encoding the string. This is done by calling lpEncodeGetType()
 * before calling this function. */
// 将s指向的长度为len的字符串编码到buf中，buf中需要确保有足够的空间来编码这个字符串（通过lpEncodeGetType计算需要的buf长度）。
void lpEncodeString(unsigned char *buf, unsigned char *s, uint32_t len) {
    if (len < 64) {
        buf[0] = len | LP_ENCODING_6BIT_STR;
        memcpy(buf+1,s,len);
    } else if (len < 4096) {
        buf[0] = (len >> 8) | LP_ENCODING_12BIT_STR;
        buf[1] = len & 0xff;
        memcpy(buf+2,s,len);
    } else {
        buf[0] = LP_ENCODING_32BIT_STR;
        buf[1] = len & 0xff;
        buf[2] = (len >> 8) & 0xff;
        buf[3] = (len >> 16) & 0xff;
        buf[4] = (len >> 24) & 0xff;
        memcpy(buf+5,s,len);
    }
}

/* Return the encoded length of the listpack element pointed by 'p'.
 * This includes the encoding byte, length bytes, and the element data itself.
 * If the element encoding is wrong then 0 is returned.
 * Note that this method may access additional bytes (in case of 12 and 32 bit
 * str), so should only be called when we know 'p' was already validated by
 * lpCurrentEncodedSizeBytes or ASSERT_INTEGRITY_LEN (possibly since 'p' is
 * a return value of another function that validated its return. */
// 返回p指向的entry中存储的数据长度（编码type/len长度+数据长度）。
// 注意p必须要是验证过的指向entry的指针。
uint32_t lpCurrentEncodedSizeUnsafe(unsigned char *p) {
    if (LP_ENCODING_IS_7BIT_UINT(p[0])) return 1;
    if (LP_ENCODING_IS_6BIT_STR(p[0])) return 1+LP_ENCODING_6BIT_STR_LEN(p);
    if (LP_ENCODING_IS_13BIT_INT(p[0])) return 2;
    if (LP_ENCODING_IS_16BIT_INT(p[0])) return 3;
    if (LP_ENCODING_IS_24BIT_INT(p[0])) return 4;
    if (LP_ENCODING_IS_32BIT_INT(p[0])) return 5;
    if (LP_ENCODING_IS_64BIT_INT(p[0])) return 9;
    if (LP_ENCODING_IS_12BIT_STR(p[0])) return 2+LP_ENCODING_12BIT_STR_LEN(p);
    if (LP_ENCODING_IS_32BIT_STR(p[0])) return 5+LP_ENCODING_32BIT_STR_LEN(p);
    if (p[0] == LP_EOF) return 1;
    return 0;
}

/* Return bytes needed to encode the length of the listpack element pointed by 'p'.
 * This includes just the encoding byte, and the bytes needed to encode the length
 * of the element (excluding the element data itself)
 * If the element encoding is wrong then 0 is returned. */
// 返回编码数据长度所需要的字节数，不包含存储的数据本身。
// 对于数字类型，我们只用type就可以知道编码长度，所以都是1字节。
// 对于字符串长度编码，不同长度编码所需字节数不一样。6bit的1字节，12bit的2字节，32bit的5字节。
uint32_t lpCurrentEncodedSizeBytes(unsigned char *p) {
    if (LP_ENCODING_IS_7BIT_UINT(p[0])) return 1;
    if (LP_ENCODING_IS_6BIT_STR(p[0])) return 1;
    if (LP_ENCODING_IS_13BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_16BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_24BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_32BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_64BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_12BIT_STR(p[0])) return 2;
    if (LP_ENCODING_IS_32BIT_STR(p[0])) return 5;
    if (p[0] == LP_EOF) return 1;
    return 0;
}

/* Skip the current entry returning the next. It is invalid to call this
 * function if the current element is the EOF element at the end of the
 * listpack, however, while this function is used to implement lpNext(),
 * it does not return NULL when the EOF element is encountered. */
// 跳过当前元素，返回指向下一个entry的指针。
// 注意当p指向结束符位置时，调用此函数是无效的。在lpNext()中如果传入结束符来调用此函数，最终将不会返回NULL。
unsigned char *lpSkip(unsigned char *p) {
    // 获取当前entry的总数据长度
    unsigned long entrylen = lpCurrentEncodedSizeUnsafe(p);
    // entry总长度 = 总数据长度 + 总数据长度的编码长度
    entrylen += lpEncodeBacklen(NULL,entrylen);
    p += entrylen;
    return p;
}

/* If 'p' points to an element of the listpack, calling lpNext() will return
 * the pointer to the next element (the one on the right), or NULL if 'p'
 * already pointed to the last element of the listpack. */
// 获取p的下一个元素的指针。如果当前p指向最后一个元素，则没有下一个元素了，返回NULL。
// 注意这里p必须要指向某个元素，否则结果可能异常。lpSkip要求p不能为NULL，也不能指向结束符。
unsigned char *lpNext(unsigned char *lp, unsigned char *p) {
    assert(p);
    p = lpSkip(p);
    ASSERT_INTEGRITY(lp, p);
    // 如果p指向的下一个元素是结束符，即p当前指向最后一个元素，则返回NULL。
    if (p[0] == LP_EOF) return NULL;
    return p;
}

/* If 'p' points to an element of the listpack, calling lpPrev() will return
 * the pointer to the previous element (the one on the left), or NULL if 'p'
 * already pointed to the first element of the listpack. */
// 获取listpack中p指向entry的前一个元素指针。如果当前p指向第一个元素，则没有前一个元素了，返回NULL。
unsigned char *lpPrev(unsigned char *lp, unsigned char *p) {
    assert(p);
    // p当前指向第一个元素，返回NULL。
    if (p-lp == LP_HDR_SIZE) return NULL;
    // p指向前一个元素的最后一个字符，用于后面解析前一个元素的长度。
    p--; /* Seek the first backlen byte of the last element. */
    // 解析前一个元素的数据长度。
    uint64_t prevlen = lpDecodeBacklen(p);
    // 前一个元素的数据长度 加 数据长度的编码长度，即为前一个entry的总长度。
    prevlen += lpEncodeBacklen(NULL,prevlen);
    // p指向前一个元素的首字节位置。
    p -= prevlen-1; /* Seek the first byte of the previous entry. */
    // 验证p的有效性。
    ASSERT_INTEGRITY(lp, p);
    return p;
}

/* Return a pointer to the first element of the listpack, or NULL if the
 * listpack has no elements. */
// 返回listpack的第一个元素的指针，如果没有元素则返回NULL。
unsigned char *lpFirst(unsigned char *lp) {
    lp += LP_HDR_SIZE; /* Skip the header. */
    if (lp[0] == LP_EOF) return NULL;
    return lp;
}

/* Return a pointer to the last element of the listpack, or NULL if the
 * listpack has no elements. */
// 返回listpack的最后一个元素的指针，如果没有元素则返回NULL。
unsigned char *lpLast(unsigned char *lp) {
    // 定位到结束符
    unsigned char *p = lp+lpGetTotalBytes(lp)-1; /* Seek EOF element. */
    // 使用lpPrev获取前一个元素的指针，即为最后一个元素的指针。
    return lpPrev(lp,p); /* Will return NULL if EOF is the only element. */
}

/* Return the number of elements inside the listpack. This function attempts
 * to use the cached value when within range, otherwise a full scan is
 * needed. As a side effect of calling this function, the listpack header
 * could be modified, because if the count is found to be already within
 * the 'numele' header field range, the new value is set. */
// 返回listpack中总的entry数。尝试使用16bit缓存的数据，如果值为UINT16_MAX，则需要遍历整个listpack才能获取总entry数。
uint32_t lpLength(unsigned char *lp) {
    // 16bit存储的数据有效，则直接返回。
    uint32_t numele = lpGetNumElements(lp);
    if (numele != LP_HDR_NUMELE_UNKNOWN) return numele;

    /* Too many elements inside the listpack. We need to scan in order
     * to get the total number. */
    // entry数量太多，16bit存不下，需要遍历来获取。
    uint32_t count = 0;
    // 定位到第一个entry。
    unsigned char *p = lpFirst(lp);
    // 向后遍历，count+1计数。
    while(p) {
        count++;
        p = lpNext(lp,p);
    }

    /* If the count is again within range of the header numele field,
     * set it. */
    // 如果得到的count可以用16bit表示，则重新更新。
    if (count < LP_HDR_NUMELE_UNKNOWN) lpSetNumElements(lp,count);
    return count;
}

/* Return the listpack element pointed by 'p'.
 *
 * The function changes behavior depending on the passed 'intbuf' value.
 * Specifically, if 'intbuf' is NULL:
 *
 * If the element is internally encoded as an integer, the function returns
 * NULL and populates the integer value by reference in 'count'. Otherwise if
 * the element is encoded as a string a pointer to the string (pointing inside
 * the listpack itself) is returned, and 'count' is set to the length of the
 * string.
 *
 * If instead 'intbuf' points to a buffer passed by the caller, that must be
 * at least LP_INTBUF_SIZE bytes, the function always returns the element as
 * it was a string (returning the pointer to the string and setting the
 * 'count' argument to the string length by reference). However if the element
 * is encoded as an integer, the 'intbuf' buffer is used in order to store
 * the string representation.
 *
 * The user should use one or the other form depending on what the value will
 * be used for. If there is immediate usage for an integer value returned
 * by the function, than to pass a buffer (and convert it back to a number)
 * is of course useless.
 *
 * If the function is called against a badly encoded listpack, so that there
 * is no valid way to parse it, the function returns like if there was an
 * integer encoded with value 12345678900000000 + <unrecognized byte>, this may
 * be an hint to understand that something is wrong. To crash in this case is
 * not sensible because of the different requirements of the application using
 * this lib.
 *
 * Similarly, there is no error returned since the listpack normally can be
 * assumed to be valid, so that would be a very high API cost. However a function
 * in order to check the integrity of the listpack at load time is provided,
 * check lpIsValid(). */
// 返回listpack中p指向的元素数据。参数intbuf可以改变该函数的行为。
// 1、当传入intbuf为NULL时，如果p指向的元素数据时数字编码，则函数返回NULL，数字值解析并填充到count中返回。
//      如果p指向的元素为字符串类型，则返回指向该字符串的指针，count中填充该字符串的长度。
// 2、当传入的intbuf不为NULL时（部分长度至少为LP_INTBUF_SIZE），函数总是返回p指向元素数据的字符串表示形式。
//      如果p指向的元素为数字，则转为字符串格式存储到intbuf中返回，count返回长度。
//      如果p指向元素是字符串，则函数返回指向字符串的指针，count返回长度。
// 调用者需要自己决定使用哪种方式来获取值。如果是直接使用integer类型的值，当然就不需要传buf进行获取，然后再转为数字使用了。

// 如果p指向的元素是错误的编码，我们无法进行解析，则返回 12345678900000000 + <unrecognized byte> 形式的数字，表示解析出错。
// 我们不直接crash，因为使用这个库的不同程序对于这种情况处理不一样，所以直接crash是不明智的选择。

// 同样的，因为listpack元素通常认为是有效的，所以我们没有错误返回。如果还要去检查有效性的话，会使得调用这个api的成本很高。
// 不过在load数据时我们会有对listpack的完整数据检查。见lpValidateIntegrity方法。
unsigned char *lpGet(unsigned char *p, int64_t *count, unsigned char *intbuf) {
    int64_t val;
    uint64_t uval, negstart, negmax;

    assert(p); /* assertion for valgrind (avoid NPD) */
    // 根据不同的编码类型，来获取数据。
    if (LP_ENCODING_IS_7BIT_UINT(p[0])) {
        // 数字类型。uval即为解析出来的无符号数。7位的正数都是正数，不需要处理符号，所以negstart设置为UINT64_MAX。
        // 因为数字类型有的是有符号数，而我们存储按bit位存储的，所以解析时我们还需要处理符号。
        // 当无符号数大于negstart时即表示负数，所以negstart是负数的起始值。negmax是当前编码能表示的无符号最大值，即负数最大值。
        negstart = UINT64_MAX; /* 7 bit ints are always positive. */
        negmax = 0;
        uval = p[0] & 0x7f;
    } else if (LP_ENCODING_IS_6BIT_STR(p[0])) {
        // 字符串类型，count为字符串长度。返回值为指向该字符串数据的指针。
        *count = LP_ENCODING_6BIT_STR_LEN(p);
        return p+1;
    } else if (LP_ENCODING_IS_13BIT_INT(p[0])) {
        uval = ((p[0]&0x1f)<<8) | p[1];
        negstart = (uint64_t)1<<12;
        negmax = 8191;
    } else if (LP_ENCODING_IS_16BIT_INT(p[0])) {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2]<<8;
        negstart = (uint64_t)1<<15;
        negmax = UINT16_MAX;
    } else if (LP_ENCODING_IS_24BIT_INT(p[0])) {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2]<<8 |
               (uint64_t)p[3]<<16;
        negstart = (uint64_t)1<<23;
        negmax = UINT32_MAX>>8;
    } else if (LP_ENCODING_IS_32BIT_INT(p[0])) {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2]<<8 |
               (uint64_t)p[3]<<16 |
               (uint64_t)p[4]<<24;
        negstart = (uint64_t)1<<31;
        negmax = UINT32_MAX;
    } else if (LP_ENCODING_IS_64BIT_INT(p[0])) {
        uval = (uint64_t)p[1] |
               (uint64_t)p[2]<<8 |
               (uint64_t)p[3]<<16 |
               (uint64_t)p[4]<<24 |
               (uint64_t)p[5]<<32 |
               (uint64_t)p[6]<<40 |
               (uint64_t)p[7]<<48 |
               (uint64_t)p[8]<<56;
        negstart = (uint64_t)1<<63;
        negmax = UINT64_MAX;
    } else if (LP_ENCODING_IS_12BIT_STR(p[0])) {
        *count = LP_ENCODING_12BIT_STR_LEN(p);
        return p+2;
    } else if (LP_ENCODING_IS_32BIT_STR(p[0])) {
        *count = LP_ENCODING_32BIT_STR_LEN(p);
        return p+5;
    } else {
        uval = 12345678900000000ULL + p[0];
        negstart = UINT64_MAX;
        negmax = 0;
    }

    /* We reach this code path only for integer encodings.
     * Convert the unsigned value to the signed one using two's complement
     * rule. */
    // 代码执行到这里，只能是数字编码。使用二进制补码规则将无符号数转为有符号数（按位取反后+1）。
    if (uval >= negstart) {
        /* This three steps conversion should avoid undefined behaviors
         * in the unsigned -> signed conversion. */
        // 如果解析的无符号数uval大于negstart，则说明当前值应该为负数，需要转换。
        uval = negmax-uval;
        val = uval;
        // 按位取反后+1，即为最终数值。negmax-uval相当于按位取反，但因为负数所以是-(negmax-uval+1)。
        val = -val-1;
    } else {
        val = uval;
    }

    /* Return the string representation of the integer or the value itself
     * depending on intbuf being NULL or not. */
    // 如果传入了intbuf，则需要将数字进行编码成字符串返回。否则直接通过count返回数字值。
    if (intbuf) {
        *count = snprintf((char*)intbuf,LP_INTBUF_SIZE,"%lld",(long long)val);
        return intbuf;
    } else {
        *count = val;
        return NULL;
    }
}

/* Insert, delete or replace the specified element 'ele' of length 'len' at
 * the specified position 'p', with 'p' being a listpack element pointer
 * obtained with lpFirst(), lpLast(), lpNext(), lpPrev() or lpSeek().
 *
 * The element is inserted before, after, or replaces the element pointed
 * by 'p' depending on the 'where' argument, that can be LP_BEFORE, LP_AFTER
 * or LP_REPLACE.
 *
 * If 'ele' is set to NULL, the function removes the element pointed by 'p'
 * instead of inserting one.
 *
 * Returns NULL on out of memory or when the listpack total length would exceed
 * the max allowed size of 2^32-1, otherwise the new pointer to the listpack
 * holding the new element is returned (and the old pointer passed is no longer
 * considered valid)
 *
 * If 'newp' is not NULL, at the end of a successful call '*newp' will be set
 * to the address of the element just added, so that it will be possible to
 * continue an interation with lpNext() and lpPrev().
 *
 * For deletion operations ('ele' set to NULL) 'newp' is set to the next
 * element, on the right of the deleted one, or to NULL if the deleted element
 * was the last one. */
// 使用size长度的ele数据在指定位置p进行插入、删除、替换元素。
// p是通过lpFirst()、lpLast()、lpNext()、lpPrev()或lpSeek()返回的listpack元素指针。
// 具体插入位置或是替换操作，通过where参数来指定。LP_BEFORE、LP_AFTER、LP_REPLACE分别表示插入之前、插入之后或替换操作。
// 如果传入的ele为NULL，则表示删除p指向的元素。

// 当OOM时，或者新的listpack长度大于UINT32_MAX时（长度无法再用4字节存储），函数返回NULL。否则函数返回指向新的listpack的指针。
// 如果newp不为NULL，则它返回我们刚插入的新元素在listpack中的位置。这样就可以继续与lpNext()和lpPrev()交互。
// 对于删除操作（ele为NULL），newp将会设置为下一个（next）元素返回。如果删除的是最后一个元素，则返回NULL。
unsigned char *lpInsert(unsigned char *lp, unsigned char *ele, uint32_t size, unsigned char *p, int where, unsigned char **newp) {
    // intenc用于存储数字类型编码后的数据。
    unsigned char intenc[LP_MAX_INT_ENCODING_LEN];
    // backlen用于存储当前元素数据长度的逆向编码。
    unsigned char backlen[LP_MAX_BACKLEN_SIZE];

    uint64_t enclen; /* The length of the encoded element. */

    /* An element pointer set to NULL means deletion, which is conceptually
     * replacing the element with a zero-length element. So whatever we
     * get passed as 'where', set it to LP_REPLACE. */
    // ele为NULL表示删除，实质上是使用长度为0的元素来替换。所以无论where传的什么，这里都设置为LP_REPLACE。
    if (ele == NULL) where = LP_REPLACE;

    /* If we need to insert after the current element, we just jump to the
     * next element (that could be the EOF one) and handle the case of
     * inserting before. So the function will actually deal with just two
     * cases: LP_BEFORE and LP_REPLACE. */
    // 如果我们需要在当前元素的后面插入，这里我们跳到next元素（可能为结束符），转为在next元素前面插入的形式。
    // 这样我们函数处理实际上只有 LP_BEFORE 和 LP_REPLACE 两种操作了。
    if (where == LP_AFTER) {
        p = lpSkip(p);
        where = LP_BEFORE;
        ASSERT_INTEGRITY(lp, p);
    }

    /* Store the offset of the element 'p', so that we can obtain its
     * address again after a reallocation. */
    // 保存p的offset，这样我们后面reallocation后可以重新定位到操作位置。
    unsigned long poff = p-lp;

    /* Calling lpEncodeGetType() results into the encoded version of the
     * element to be stored into 'intenc' in case it is representable as
     * an integer: in that case, the function returns LP_ENCODING_INT.
     * Otherwise if LP_ENCODING_STR is returned, we'll have to call
     * lpEncodeString() to actually write the encoded string on place later.
     *
     * Whatever the returned encoding is, 'enclen' is populated with the
     * length of the encoded element. */
    // 有传入ele，则需要调用lpEncodeGetType来尝试对ele数据进行编码，并获取编码类型和编码后数据的长度。没有传入ele则表示删除操作。
    // 如果是数字类型，则会通过intenc返回编码好的数字字节数据（包含type和数字值），然后函数返回LP_ENCODING_INT。
    // 如果是字符串类型，则函数返回LP_ENCODING_STR，后面我们需要调用lpEncodeString来实际写入编码字符串。
    // 无论lpEncodeGetType函数返回什么编码，enclen总是返回编码后的元素的长度。
    int enctype;
    if (ele) {
        enctype = lpEncodeGetType(ele,size,intenc,&enclen);
    } else {
        enctype = -1;
        enclen = 0;
    }

    /* We need to also encode the backward-parsable length of the element
     * and append it to the end: this allows to traverse the listpack from
     * the end to the start. */
    // 我们需要将前面获得的当前编码后元素的长度，进行逆向编码放到该元素的末尾，用于逆向遍历使用。
    unsigned long backlen_size = ele ? lpEncodeBacklen(backlen,enclen) : 0;
    uint64_t old_listpack_bytes = lpGetTotalBytes(lp);
    // 如果是replace，则计算原来当前元素的总长度。
    uint32_t replaced_len  = 0;
    if (where == LP_REPLACE) {
        replaced_len = lpCurrentEncodedSizeUnsafe(p);
        replaced_len += lpEncodeBacklen(NULL,replaced_len);
        ASSERT_INTEGRITY_LEN(lp, p, replaced_len);
    }

    // 计算我们操作后，新的listpack总字节数。即为老的listpack总字节数 + 新插入元素的总长度 - 需要替换的原来元素的总长度。
    // 对于插入，则replaced_len为0。对于删除则插入总长度为0。对于正常的替换，则两者都不为0。
    uint64_t new_listpack_bytes = old_listpack_bytes + enclen + backlen_size
                                  - replaced_len;
    // 操作后总长度超限，4字节表示不下了，这里返回NULL。
    if (new_listpack_bytes > UINT32_MAX) return NULL;

    /* We now need to reallocate in order to make space or shrink the
     * allocation (in case 'when' value is LP_REPLACE and the new element is
     * smaller). However we do that before memmoving the memory to
     * make room for the new element if the final allocation will get
     * larger, or we do it after if the final allocation will get smaller. */
    // 现在我们需要根据前面计算的所需字节数，来重新分配空间（扩容或缩容）。
    // 如果是扩容，我们会在memmove操作之前来realloc；但如果是缩容，我们会在memmove操作之后来realloc（避免有效数据被丢弃）。

    unsigned char *dst = lp + poff; /* May be updated after reallocation. */

    /* Realloc before: we need more room. */
    // 如果我们需要扩容，则处理realloc，并更新我们待操作位置元素的指针。
    if (new_listpack_bytes > old_listpack_bytes &&
        new_listpack_bytes > lp_malloc_size(lp)) {
        if ((lp = lp_realloc(lp,new_listpack_bytes)) == NULL) return NULL;
        dst = lp + poff;
    }

    /* Setup the listpack relocating the elements to make the exact room
     * we need to store the new one. */
    // memmove操作。我们需要将元素前移或这后移，为新元素腾出空间。
    if (where == LP_BEFORE) {
        // 插入操作，dst后面的数据统一向后移动 enclen+backlen_size 字节。
        memmove(dst+enclen+backlen_size,dst,old_listpack_bytes-poff);
    } else { /* LP_REPLACE. */
        long lendiff = (enclen+backlen_size)-replaced_len;
        // 替换操作，dst+replaced_len（即dst的next元素）后面的数据统一向前或向后移动abs(lendiff)字节。
        memmove(dst+replaced_len+lendiff,
                dst+replaced_len,
                old_listpack_bytes-poff-replaced_len);
    }

    /* Realloc after: we need to free space. */
    // 如果需要缩容，这里处理realloc。
    if (new_listpack_bytes < old_listpack_bytes) {
        if ((lp = lp_realloc(lp,new_listpack_bytes)) == NULL) return NULL;
        dst = lp + poff;
    }

    /* Store the entry. */
    // 如果newp不为NULL，则填充我们操作元素位置。
    if (newp) {
        *newp = dst;
        /* In case of deletion, set 'newp' to NULL if the next element is
         * the EOF element. */
        // 如果是删除操作，且删除后p指向结束符，则newp要设置为NULL。
        if (!ele && dst[0] == LP_EOF) *newp = NULL;
    }
    // ele有值，则这里将数据填充到我们前面挪出来的空间中。
    if (ele) {
        if (enctype == LP_ENCODING_INT) {
            // INT编码，直接将intenc数据填进去。
            memcpy(dst,intenc,enclen);
        } else {
            // 字符串编码，则调用lpEncodeString将数据填入。
            lpEncodeString(dst,ele,size);
        }
        // dst指向写入元素的末尾，填充逆向len的位置。
        dst += enclen;
        // 填入当前元素的逆向编码的长度数据。
        memcpy(dst,backlen,backlen_size);
        // dst指向下一个元素（没啥用了）
        dst += backlen_size;
    }

    /* Update header. */
    // 更新header信息。插入或删除元素需要更新总的元素数。
    if (where != LP_REPLACE || ele == NULL) {
        uint32_t num_elements = lpGetNumElements(lp);
        // 这里如果缓存的总元素数有效，则我们直接更新，因为不管插入还是删除，直接修改该字段就可以了，不需要遍历listpack。
        // 如果本来缓存的总元素数为UINT16_MAX，需要遍历才能获取准确的总元素数，所以这里不更新，当有查询总长度时，再更新。见lpLength函数。
        if (num_elements != LP_HDR_NUMELE_UNKNOWN) {
            if (ele)
                lpSetNumElements(lp,num_elements+1);
            else
                lpSetNumElements(lp,num_elements-1);
        }
    }
    // 更新listpack总的字节数。
    lpSetTotalBytes(lp,new_listpack_bytes);

#if 0
    /* This code path is normally disabled: what it does is to force listpack
     * to return *always* a new pointer after performing some modification to
     * the listpack, even if the previous allocation was enough. This is useful
     * in order to spot bugs in code using listpacks: by doing so we can find
     * if the caller forgets to set the new pointer where the listpack reference
     * is stored, after an update. */
    unsigned char *oldlp = lp;
    lp = lp_malloc(new_listpack_bytes);
    memcpy(lp,oldlp,new_listpack_bytes);
    if (newp) {
        unsigned long offset = (*newp)-oldlp;
        *newp = lp + offset;
    }
    /* Make sure the old allocation contains garbage. */
    memset(oldlp,'A',new_listpack_bytes);
    lp_free(oldlp);
#endif

    return lp;
}

/* Append the specified element 'ele' of length 'len' at the end of the
 * listpack. It is implemented in terms of lpInsert(), so the return value is
 * the same as lpInsert(). */
// 将长度为len的ele元素追加到listpack的尾部。本函数是基于lpInsert()实现的，所以返回值与lpInsert()类似。
unsigned char *lpAppend(unsigned char *lp, unsigned char *ele, uint32_t size) {
    uint64_t listpack_bytes = lpGetTotalBytes(lp);
    unsigned char *eofptr = lp + listpack_bytes - 1;
    return lpInsert(lp,ele,size,eofptr,LP_BEFORE,NULL);
}

/* Remove the element pointed by 'p', and return the resulting listpack.
 * If 'newp' is not NULL, the next element pointer (to the right of the
 * deleted one) is returned by reference. If the deleted element was the
 * last one, '*newp' is set to NULL. */
// 移除listpack中p指针处的元素，返回新的listpack。
// 如果newp非空，则利用该参数返回删除元素的下一个元素的指针。如果删除的是最后一个元素，则*newp返回NULL。
unsigned char *lpDelete(unsigned char *lp, unsigned char *p, unsigned char **newp) {
    return lpInsert(lp,NULL,0,p,LP_REPLACE,newp);
}

/* Return the total number of bytes the listpack is composed of. */
// 返回listpack总的字节数。
uint32_t lpBytes(unsigned char *lp) {
    return lpGetTotalBytes(lp);
}

/* Seek the specified element and returns the pointer to the seeked element.
 * Positive indexes specify the zero-based element to seek from the head to
 * the tail, negative indexes specify elements starting from the tail, where
 * -1 means the last element, -2 the penultimate and so forth. If the index
 * is out of range, NULL is returned. */
// 查询指定index的元素，并返回指向该元素的指针。
// 正值表示从头到尾，index从0开始。负值表示从尾到头，index从-1开始。如果index超出了范围，则返回NULL。
unsigned char *lpSeek(unsigned char *lp, long index) {
    int forward = 1; /* Seek forward by default. */

    /* We want to seek from left to right or the other way around
     * depending on the listpack length and the element position.
     * However if the listpack length cannot be obtained in constant time,
     * we always seek from left to right. */
    // 我们想要根据listpack的长度和指定的index位置，来确定遍历查询方向（从左往右，或从右往左）。
    // 但我们缓存的长度无效，需要遍历才能获取总长度时，我们按index正负来决定遍历方向。
    uint32_t numele = lpGetNumElements(lp);
    if (numele != LP_HDR_NUMELE_UNKNOWN) {
        // 能获取到准确长度。负index转为正数。
        if (index < 0) index = (long)numele+index;
        // 如果转了之后，index还是不合法，则返回NULL。
        if (index < 0) return NULL; /* Index still < 0 means out of range. */
        if (index >= (long)numele) return NULL; /* Out of range the other side. */
        /* We want to scan right-to-left if the element we are looking for
         * is past the half of the listpack. */
        // 判断index是靠前还是靠后，如果是靠后，则这里处理从后往前遍历，设置index为负值。
        if (index > (long)numele/2) {
            forward = 0;
            /* Right to left scanning always expects a negative index. Convert
             * our index to negative form. */
            index -= numele;
        }
    } else {
        /* If the listpack length is unspecified, for negative indexes we
         * want to always scan right-to-left. */
        // 如果无法快速得到总元素个数，则对于正的index，默认从前往后遍历。对于负的index，这里设置为从后往前遍历。
        if (index < 0) forward = 0;
    }

    /* Forward and backward scanning is trivially based on lpNext()/lpPrev(). */
    // 处理 Forward 或 backward 扫描。使用lpNext()/lpPrev()来辅助遍历。
    if (forward) {
        unsigned char *ele = lpFirst(lp);
        // 当遍历到了指定index处，或者遍历完了listpack，则跳出循环。
        while (index > 0 && ele) {
            ele = lpNext(lp,ele);
            index--;
        }
        return ele;
    } else {
        unsigned char *ele = lpLast(lp);
        // 当遍历到了指定index处，或者遍历完了listpack，则跳出循环。
        while (index < -1 && ele) {
            ele = lpPrev(lp,ele);
            index++;
        }
        return ele;
    }
}

/* Validate the integrity of a single listpack entry and move to the next one.
 * The input argument 'pp' is a reference to the current record and is advanced on exit.
 * Returns 1 if valid, 0 if invalid. */
// 验证单个listpack entry的结构。入参pp指向当前待验证的entry位置，当验证结束时pp返回下一个待验证的entry位置。
// 如果entry是有效的，返回1，否则返回0。
int lpValidateNext(unsigned char *lp, unsigned char **pp, size_t lpbytes) {
#define OUT_OF_RANGE(p) ( \
        (p) < lp + LP_HDR_SIZE || \
        (p) > lp + lpbytes - 1)
    unsigned char *p = *pp;
    // p为NULL，无效返回0
    if (!p)
        return 0;

    // p指向结束符，有效返回1。此时pp指向一个元素，没有元素了，所以pp返回NULL。
    if (*p == LP_EOF) {
        *pp = NULL;
        return 1;
    }

    /* check that we can read the encoded size */
    // 检查当前数据的编码type/len的长度。
    uint32_t lenbytes = lpCurrentEncodedSizeBytes(p);
    if (!lenbytes)
        return 0;

    /* make sure the encoded entry length doesn't rech outside the edge of the listpack */
    // 确保编码的entry的len在listpack的数据范围内
    if (OUT_OF_RANGE(p + lenbytes))
        return 0;

    /* get the entry length and encoded backlen. */
    // 获取entry数据的长度，以及编码的backlen（长度的逆向编码）。
    unsigned long entrylen = lpCurrentEncodedSizeUnsafe(p);
    unsigned long encodedBacklen = lpEncodeBacklen(NULL,entrylen);
    // 总entry长度
    entrylen += encodedBacklen;

    /* make sure the entry doesn't rech outside the edge of the listpack */
    // 确保整个entry在listpack的数据范围内
    if (OUT_OF_RANGE(p + entrylen))
        return 0;

    /* move to the next entry */
    // 指针跳到下一个entry位置
    p += entrylen;

    /* make sure the encoded length at the end patches the one at the beginning. */
    // 确保我们解码得到的总entry长度与前面得到的entrylen是一致的。
    uint64_t prevlen = lpDecodeBacklen(p-1);
    if (prevlen + encodedBacklen != entrylen)
        return 0;

    // *pp指向下一个元素，从而可以继续迭代校验。
    *pp = p;
    return 1;
#undef OUT_OF_RANGE
}

/* Validate the integrity of the data structure.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we scan all the entries one by one. */
// 验证整个listpack结构。当deep为0时，只验证header结构；当deep为1时，扫描所有entries并验证。
int lpValidateIntegrity(unsigned char *lp, size_t size, int deep){
    /* Check that we can actually read the header. (and EOF) */
    // 确保整个header可访问到
    if (size < LP_HDR_SIZE + 1)
        return 0;

    /* Check that the encoded size in the header must match the allocated size. */
    // 验证listpack总的字节数。
    size_t bytes = lpGetTotalBytes(lp);
    if (bytes != size)
        return 0;

    /* The last byte must be the terminator. */
    // 最后一个字节必须是结束符
    if (lp[size-1] != LP_EOF)
        return 0;

    // 如果是只验证header，这里header验证完成，返回。
    if (!deep)
        return 1;

    /* Validate the invividual entries. */
    // 遍历listpack所有的entries，挨个进行验证。
    uint32_t count = 0;
    // 从第一个元素开始迭代。
    unsigned char *p = lpFirst(lp);
    while(p) {
        // 验证遍历到的entry结构
        if (!lpValidateNext(lp, &p, bytes))
            return 0;
        count++;
    }

    /* Check that the count in the header is correct */
    // 验证header中的总entries数与我们遍历得到的count数是否一致。
    uint32_t numele = lpGetNumElements(lp);
    if (numele != LP_HDR_NUMELE_UNKNOWN && numele != count)
        return 0;

    return 1;
}
