/* The ziplist is a specially encoded dually linked list that is designed
 * to be very memory efficient. It stores both strings and integer values,
 * where integers are encoded as actual integers instead of a series of
 * characters. It allows push and pop operations on either side of the list
 * in O(1) time. However, because every operation requires a reallocation of
 * the memory used by the ziplist, the actual complexity is related to the
 * amount of memory used by the ziplist.
 *
 * ziplist是一个特殊编码的双向链表，设计用于提高内存效率。它能存储字符串和整数，且整数实际上被编码为数字而不是一系列字符。
 * 在ziplist的两端push或pop数据是O(1)复杂度。但由于每个操作都需要重新分配ziplist内存，所以实际复杂度与ziplist使用的内存量有关。
 *
 * ----------------------------------------------------------------------------
 *
 * ZIPLIST OVERALL LAYOUT
 * ======================
 *
 * The general layout of the ziplist is as follows:
 *
 * <zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
 *
 * NOTE: all fields are stored in little endian, if not specified otherwise.
 *
 * <uint32_t zlbytes> is an unsigned integer to hold the number of bytes that
 * the ziplist occupies, including the four bytes of the zlbytes field itself.
 * This value needs to be stored to be able to resize the entire structure
 * without the need to traverse it first.
 *
 * <uint32_t zltail> is the offset to the last entry in the list. This allows
 * a pop operation on the far side of the list without the need for full
 * traversal.
 *
 * <uint16_t zllen> is the number of entries. When there are more than
 * 2^16-2 entries, this value is set to 2^16-1 and we need to traverse the
 * entire list to know how many items it holds.
 *
 * <uint8_t zlend> is a special entry representing the end of the ziplist.
 * Is encoded as a single byte equal to 255. No other normal entry starts
 * with a byte set to the value of 255.
 *
 * ziplist的整体布局为：<zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
 * 注意如果没有特别指定，所有字段的存储都是小端模式。
 *  <uint32_t zlbytes>：一个无符号整数用于保存ziplist占用的总字节数，包括zlbytes本身的4字节。存储该值用于resize时直接使用，而不需遍历ziplist获取。
 *  <uint32_t zltail>：ziplist的最后一个entry的offset。主要用于快速定位最后一个节点，从而进行pop/push，而不用整个遍历。
 *  <uint16_t zllen>：entries总数。当总数大于2^16-2时，该字段值为2^16-1，此时我们需要遍历整个ziplist才能知道总共的entries数。
 *      另外当该值为UINT16_MAX时，并不表示总entry数一定>=UINT16_MAX，只是说明我们需要遍历来查询entry总数！！！
 *  <uint8_t zlend>：一个特殊的entry标记ziplist结束。一个字节编码，数值上等于255。注意正常entry的开头第一个字节不会为255的（见编码详情）。
 *
 * ZIPLIST ENTRIES
 * ===============
 *
 * Every entry in the ziplist is prefixed by metadata that contains two pieces
 * of information. First, the length of the previous entry is stored to be
 * able to traverse the list from back to front. Second, the entry encoding is
 * provided. It represents the entry type, integer or string, and in the case
 * of strings it also represents the length of the string payload.
 * So a complete entry is stored like this:
 *
 * <prevlen> <encoding> <entry-data>
 *
 * Sometimes the encoding represents the entry itself, like for small integers
 * as we'll see later. In such a case the <entry-data> part is missing, and we
 * could have just:
 *
 * <prevlen> <encoding>
 *
 * The length of the previous entry, <prevlen>, is encoded in the following way:
 * If this length is smaller than 254 bytes, it will only consume a single
 * byte representing the length as an unsinged 8 bit integer. When the length
 * is greater than or equal to 254, it will consume 5 bytes. The first byte is
 * set to 254 (FE) to indicate a larger value is following. The remaining 4
 * bytes take the length of the previous entry as value.
 *
 * So practically an entry is encoded in the following way:
 *
 * <prevlen from 0 to 253> <encoding> <entry>
 *
 * Or alternatively if the previous entry length is greater than 253 bytes
 * the following encoding is used:
 *
 * 0xFE <4 bytes unsigned little endian prevlen> <encoding> <entry>
 *
 * The encoding field of the entry depends on the content of the
 * entry. When the entry is a string, the first 2 bits of the encoding first
 * byte will hold the type of encoding used to store the length of the string,
 * followed by the actual length of the string. When the entry is an integer
 * the first 2 bits are both set to 1. The following 2 bits are used to specify
 * what kind of integer will be stored after this header. An overview of the
 * different types and encodings is as follows. The first byte is always enough
 * to determine the kind of entry.
 *
 * |00pppppp| - 1 byte
 *      String value with length less than or equal to 63 bytes (6 bits).
 *      "pppppp" represents the unsigned 6 bit length.
 * |01pppppp|qqqqqqqq| - 2 bytes
 *      String value with length less than or equal to 16383 bytes (14 bits).
 *      IMPORTANT: The 14 bit number is stored in big endian.
 * |10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
 *      String value with length greater than or equal to 16384 bytes.
 *      Only the 4 bytes following the first byte represents the length
 *      up to 2^32-1. The 6 lower bits of the first byte are not used and
 *      are set to zero.
 *      IMPORTANT: The 32 bit number is stored in big endian.
 * |11000000| - 3 bytes
 *      Integer encoded as int16_t (2 bytes).
 * |11010000| - 5 bytes
 *      Integer encoded as int32_t (4 bytes).
 * |11100000| - 9 bytes
 *      Integer encoded as int64_t (8 bytes).
 * |11110000| - 4 bytes
 *      Integer encoded as 24 bit signed (3 bytes).
 * |11111110| - 2 bytes
 *      Integer encoded as 8 bit signed (1 byte).
 * |1111xxxx| - (with xxxx between 0001 and 1101) immediate 4 bit integer.
 *      Unsigned integer from 0 to 12. The encoded value is actually from
 *      1 to 13 because 0000 and 1111 can not be used, so 1 should be
 *      subtracted from the encoded 4 bit value to obtain the right value.
 * |11111111| - End of ziplist special entry.
 *
 * Like for the ziplist header, all the integers are represented in little
 * endian byte order, even when this code is compiled in big endian systems.
 *
 * ziplist中的每个entry都以包含两个信息字段的元数据为前缀。
 * 第一个字段prevlen表示上一个entry的总长度，用于从后向前遍历使用。第二个字段encoding表示当前entry的类型（以及具体的编码方式）。
 * 一个完整的entry为：<prevlen> <encoding> [<entry-data>]，对于小的数字可能没有entry-data，因为可以直接表示在encoding中。
 * 1、prevlen字段编码形式：
 *  如果前一个entry长度小于254字节，则只需要单个字节（8bit）存储长度。
 *      <prevlen from 0 to 253> <encoding> <entry>
 *  反之如果大于254，则需要5字节来存储长度。第一个字节固定为254（FE），后面接着4个字节存储前一个entry的长度。
 *      0xFE <4 bytes unsigned little endian prevlen> <encoding> <entry>
 * 2、encoding字段形式取决于entry存储的内容：
 *  当entry存储字符串时，该字段第一个字节的前两位表示字符串长度的编码类型，长度根据该类型来进一步解析。
 *  当entry存储的是int数字时，该字段第一个字节的前两位为11，紧接着后面的6位表示存储的数字类型（决定具体使用多少字节来存储数字）。
 * 不同编码类型概述如下：
 *  |00pppppp| 首两位00，使用1字节编码长度。表示存储的是<=63（6bit）字节长度的字符串。
 *  |01pppppp|qqqqqqqq| 首两位01，使用2字节编码长度。表示存储<=16383（14bit）字节长度的字符串。注意14bit数字是大端存储。
 *  |10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| 首两位10，使用5字节编码长度。首字节的后6位没使用，默认全0，使用后面4字节表示长度。
 *      对于超出16383字节长度的字符串，使用额外的4字节来存储长度，最大可表示2^32-1字节长度的字符串。注意32bit数字是大端存储。
 *  |11000000| 11打头，表示数字类型。紧接着00，后面全0。表示使用3字节编码数字，使用后面的2字节存储int16_t类型数字。
 *  |11010000| 11打头，表示数字类型。紧接着01，后面全0。表示使用5字节编码数字，使用后面的4字节存储int32_t类型数字。
 *  |11100000| 11打头，表示数字类型。紧接着10，后面全0。表示使用9字节编码数字，使用后面的8字节存储int64_t类型数字。
 *  |11110000| 11打头，表示数字类型。紧接着11，然后后面4位全0。表示使用4字节编码数字，使用后面的3字节（24bit）存储数字。
 *  |11111110| 11打头，表示数字类型。紧接着11，然后后面4位为1110。表示使用2字节编码数字，使用后面的1字节（8bit）存储数字。
 *  |1111xxxx| xxxx为[0001～1101]，直接表示一个4bit的数字，无符号数0-12。
 *      实际上是1-13，但因为0000、1110、1111不能使用，所以我们对它-1，从而表示数字0-12。
 *  |11111111| ziplist的结束标识，特殊的entry。
 * 与ziplist的header一样，所有的整数都是小端字节序存储，即使代码可能是在大端系统中编译的。
 *
 * EXAMPLES OF ACTUAL ZIPLISTS
 * ===========================
 *
 * The following is a ziplist containing the two elements representing
 * the strings "2" and "5". It is composed of 15 bytes, that we visually
 * split into sections:
 *
 *  [0f 00 00 00] [0c 00 00 00] [02 00] [00 f3] [02 f6] [ff]
 *        |             |          |       |       |     |
 *     zlbytes        zltail    entries   "2"     "5"   end
 *
 * The first 4 bytes represent the number 15, that is the number of bytes
 * the whole ziplist is composed of. The second 4 bytes are the offset
 * at which the last ziplist entry is found, that is 12, in fact the
 * last entry, that is "5", is at offset 12 inside the ziplist.
 * The next 16 bit integer represents the number of elements inside the
 * ziplist, its value is 2 since there are just two elements inside.
 * Finally "00 f3" is the first entry representing the number 2. It is
 * composed of the previous entry length, which is zero because this is
 * our first entry, and the byte F3 which corresponds to the encoding
 * |1111xxxx| with xxxx between 0001 and 1101. We need to remove the "F"
 * higher order bits 1111, and subtract 1 from the "3", so the entry value
 * is "2". The next entry has a prevlen of 02, since the first entry is
 * composed of exactly two bytes. The entry itself, F6, is encoded exactly
 * like the first entry, and 6-1 = 5, so the value of the entry is 5.
 * Finally the special entry FF signals the end of the ziplist.
 *
 * Adding another element to the above string with the value "Hello World"
 * allows us to show how the ziplist encodes small strings. We'll just show
 * the hex dump of the entry itself. Imagine the bytes as following the
 * entry that stores "5" in the ziplist above:
 *
 * [02] [0b] [48 65 6c 6c 6f 20 57 6f 72 6c 64]
 *
 * The first byte, 02, is the length of the previous entry. The next
 * byte represents the encoding in the pattern |00pppppp| that means
 * that the entry is a string of length <pppppp>, so 0B means that
 * an 11 bytes string follows. From the third byte (48) to the last (64)
 * there are just the ASCII characters for "Hello World".
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2009-2017, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include "zmalloc.h"
#include "util.h"
#include "ziplist.h"
#include "config.h"
#include "endianconv.h"
#include "redisassert.h"

// ziplist的结束标识，总是在ziplist的最后一个字节，不论前面entry是否填充满。
#define ZIP_END 255         /* Special "end of ziplist" entry. */
// 前一个entry的最大长度小于ZIP_BIG_PREVLEN时，prevlen可以使用一个字节表示。
// 否则需要使用5个字节，第一个字节为ZIP_BIG_PREVLEN，接着4个字节表示长度。
#define ZIP_BIG_PREVLEN 254 /* ZIP_BIG_PREVLEN - 1 is the max number of bytes of
                               the previous entry, for the "prevlen" field prefixing
                               each entry, to be represented with just a single byte.
                               Otherwise it is represented as FE AA BB CC DD, where
                               AA BB CC DD are a 4 bytes unsigned integer
                               representing the previous entry len. */

/* Different encoding/length possibilities */
// 第一字节表示字符串或数字的掩码。1100 0000、0011 0000
#define ZIP_STR_MASK 0xc0
#define ZIP_INT_MASK 0x30
// 第一字节表示字符串类型的3种type，表示用1，2，5字节（分别是6bit、14bit、32bit）来编码字符串长度（包含type第一字节）。
#define ZIP_STR_06B (0 << 6)
#define ZIP_STR_14B (1 << 6)
#define ZIP_STR_32B (2 << 6)
// 第一字节表示数值类型的5种type，表示后面用1、2、3、4、8字节来编码数字（不包含type第一字节）。
#define ZIP_INT_16B (0xc0 | 0<<4)
#define ZIP_INT_32B (0xc0 | 1<<4)
#define ZIP_INT_64B (0xc0 | 2<<4)
#define ZIP_INT_24B (0xc0 | 3<<4)
#define ZIP_INT_8B 0xfe

/* 4 bit integer immediate encoding |1111xxxx| with xxxx between
 * 0001 and 1101. */
// 4位立即数的编码掩码。
#define ZIP_INT_IMM_MASK 0x0f   /* Mask to extract the 4 bits value. To add
                                   one is needed to reconstruct the value. */
// 4位立即数的最小最大值，用于立即数值的范围检测。
#define ZIP_INT_IMM_MIN 0xf1    /* 11110001 */
#define ZIP_INT_IMM_MAX 0xfd    /* 11111101 */

// 24位数字的最小最大值，用于范围检测。其他的1、2、4、8字节的数字范围标准库中都有定义，所以这里不需要再定义。
#define INT24_MAX 0x7fffff
#define INT24_MIN (-INT24_MAX - 1)

/* Macro to determine if the entry is a string. String entries never start
 * with "11" as most significant bits of the first byte. */
// 判断类型是字符串的宏。当首字节的最高2位 < 11时，即高2位是00或01或10时。
#define ZIP_IS_STR(enc) (((enc) & ZIP_STR_MASK) < ZIP_STR_MASK)

/* Utility macros.*/

/* Return total bytes a ziplist is composed of. */
// ziplist总长度zlbytes。直接zl指针指向的4个字节表示总长度。
#define ZIPLIST_BYTES(zl)       (*((uint32_t*)(zl)))

/* Return the offset of the last item inside the ziplist. */
// 最后一个entry的offset量zltail。zl+4（字节）指针指向该字段。
#define ZIPLIST_TAIL_OFFSET(zl) (*((uint32_t*)((zl)+sizeof(uint32_t))))

/* Return the length of a ziplist, or UINT16_MAX if the length cannot be
 * determined without scanning the whole ziplist. */
// zl+4*2 指针指向ziplist的entry总数字段。该字段占2字节，如果值为UINT16_MAX，则需要遍历整个ziplist来获取总entry数。
#define ZIPLIST_LENGTH(zl)      (*((uint16_t*)((zl)+sizeof(uint32_t)*2)))

/* The size of a ziplist header: two 32 bit integers for the total
 * bytes count and last item offset. One 16 bit integer for the number
 * of items field. */
// ziplist的header大小。2*4+2=10字节，总长度（4字节）+ offset（4字节）+ entry数（2字节）
#define ZIPLIST_HEADER_SIZE     (sizeof(uint32_t)*2+sizeof(uint16_t))

/* Size of the "end of ziplist" entry. Just one byte. */
// ziplist结束entry的size。就一个字节FF。
#define ZIPLIST_END_SIZE        (sizeof(uint8_t))

/* Return the pointer to the first entry of a ziplist. */
// 指向ziplist的第一个entry的指针。zl + ZIPLIST_HEADER_SIZE
#define ZIPLIST_ENTRY_HEAD(zl)  ((zl)+ZIPLIST_HEADER_SIZE)

/* Return the pointer to the last entry of a ziplist, using the
 * last entry offset inside the ziplist header. */
// 指向ziplist最后一个存储的有效entry的指针。zl + tail_offset，注意如果当前是大端模式的话，offset值需要转为小端形式再处理。
#define ZIPLIST_ENTRY_TAIL(zl)  ((zl)+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)))

/* Return the pointer to the last byte of a ziplist, which is, the
 * end of ziplist FF entry. */
// 返回ziplist指向最后一个字节（结束标识）的指针，zl+总长度-1，注意如果当前是大端模式的话，总长度值需要转为小端形式再处理。
#define ZIPLIST_ENTRY_END(zl)   ((zl)+intrev32ifbe(ZIPLIST_BYTES(zl))-1)

/* Increment the number of items field in the ziplist header. Note that this
 * macro should never overflow the unsigned 16 bit integer, since entries are
 * always pushed one at a time. When UINT16_MAX is reached we want the count
 * to stay there to signal that a full scan is needed to get the number of
 * items inside the ziplist. */
// 增加ziplist header中总的entry数。该字段只有2字节，只有在总数小于UINT16_MAX时，才会+incr。这里需要转换成小端，处理+incr后再转回去。
#define ZIPLIST_INCR_LENGTH(zl,incr) { \
    if (ZIPLIST_LENGTH(zl) < UINT16_MAX) \
        ZIPLIST_LENGTH(zl) = intrev16ifbe(intrev16ifbe(ZIPLIST_LENGTH(zl))+incr); \
}

/* We use this function to receive information about a ziplist entry.
 * Note that this is not how the data is actually encoded, is just what we
 * get filled by a function in order to operate more easily. */
// 我们使用zlentry来接收ziplist entry信息。虽然存储时会序列化最开始的那种结构，但是为了代码处理方便，这里使用这个节点来接收处理数据。
typedef struct zlentry {
    // 编码前一个entry长度所使用的字节数。
    unsigned int prevrawlensize; /* Bytes used to encode the previous entry len*/
    // 前一个entry的长度
    unsigned int prevrawlen;     /* Previous entry len. */
    // 编码当前entry的type/len所使用的字节数。对于字符串会有1、2、5这3种字节数，对于数字这里总是1个字节。
    unsigned int lensize;        /* Bytes used to encode this entry type/len.
                                    For example strings have a 1, 2 or 5 bytes
                                    header. Integers always use a single byte.*/
    // 当前entry实际数据的长度。对于字符串，即为字符串长度；对于数字，长度可以为1、2、3、4、8或0（4bit立即数1～12）。
    unsigned int len;            /* Bytes used to represent the actual entry.
                                    For strings this is just the string length
                                    while for integers it is 1, 2, 3, 4, 8 or
                                    0 (for 4 bit immediate) depending on the
                                    number range. */
    // 每个entry包含头和实际数据。头为prevlen和encoding，这里headersize表示entry头编码后的长度，即为 prevrawlensize + lensize。
    unsigned int headersize;     /* prevrawlensize + lensize. */
    // 根据entry数据的编码类型，该字段设置为 ZIP_STR_* 或 ZIP_INT_*。
    // 但是对于4bits的立即数，可根据这个encoding识别，从而知道范围，所以必须要做范围检查。
    unsigned char encoding;      /* Set to ZIP_STR_* or ZIP_INT_* depending on
                                    the entry encoding. However for 4 bits
                                    immediate integers this can assume a range
                                    of values and must be range-checked. */
    // 指向当前entry编码后存储的位置。即指向ziplist中当前entry的prevlen字段。
    unsigned char *p;            /* Pointer to the very start of the entry, that
                                    is, this points to prev-entry-len field. */
} zlentry;

// 初始化zlentry
#define ZIPLIST_ENTRY_ZERO(zle) { \
    (zle)->prevrawlensize = (zle)->prevrawlen = 0; \
    (zle)->lensize = (zle)->len = (zle)->headersize = 0; \
    (zle)->encoding = 0; \
    (zle)->p = NULL; \
}

/* Extract the encoding from the byte pointed by 'ptr' and set it into
 * 'encoding' field of the zlentry structure. */
// 从'ptr'指向的字节中提取编码并将其设置到zlentry结构的'encoding'字段中。
#define ZIP_ENTRY_ENCODING(ptr, encoding) do {  \
    (encoding) = ((ptr)[0]); \
    /* encoding>=ZIP_STR_MASK，表示是数字类型，直接返回。否则是字符串类型，通过encoding & ZIP_STR_MASK 后返回。*/ \
    if ((encoding) < ZIP_STR_MASK) (encoding) &= ZIP_STR_MASK; \
} while(0)

#define ZIP_ENCODING_SIZE_INVALID 0xff
/* Return the number of bytes required to encode the entry type + length.
 * On error, return ZIP_ENCODING_SIZE_INVALID */
// 根据encoding返回编码entry的type+length所需要的字节数（即zlentry的lensize字段值）。如果类型错误则返回ZIP_ENCODING_SIZE_INVALID。
static inline unsigned int zipEncodingLenSize(unsigned char encoding) {
    if (encoding == ZIP_INT_16B || encoding == ZIP_INT_32B ||
        encoding == ZIP_INT_24B || encoding == ZIP_INT_64B ||
        encoding == ZIP_INT_8B)
        // 如果是数字类型，type+length一共只需要1字节即可表示。立即数连entry值也编码在该字节中。
        return 1;
    if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)
        return 1;
    if (encoding == ZIP_STR_06B)
        // 6bit表示长度的字符串。type+length只需1字节（2位type，6位length）。
        return 1;
    if (encoding == ZIP_STR_14B)
        // 14bit表示长度的字符串。type+length需2字节（2位type，14位length）。
        return 2;
    if (encoding == ZIP_STR_32B)
        // 32bit表示长度的字符串。type+length需5字节（1字节type，4字节length）。
        return 5;
    return ZIP_ENCODING_SIZE_INVALID;
}

#define ZIP_ASSERT_ENCODING(encoding) do {                                     \
    assert(zipEncodingLenSize(encoding) != ZIP_ENCODING_SIZE_INVALID);         \
} while (0)

/* Return bytes needed to store integer encoded by 'encoding' */
// 根据encoding返回后面整数编码需要额外使用的字节数。
static inline unsigned int zipIntSize(unsigned char encoding) {
    switch(encoding) {
        // 有使用额外的1、2、3、4、8字节来存储数字。
    case ZIP_INT_8B:  return 1;
    case ZIP_INT_16B: return 2;
    case ZIP_INT_24B: return 3;
    case ZIP_INT_32B: return 4;
    case ZIP_INT_64B: return 8;
    }
    if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)
        // 4bit的立即数，不需要额外的字节来编码数字。所以返回0。
        return 0; /* 4 bit immediate */
    /* bad encoding, covered by a previous call to ZIP_ASSERT_ENCODING */
    redis_unreachable();
    return 0;
}

/* Write the encoding header of the entry in 'p'. If p is NULL it just returns
 * the amount of bytes required to encode such a length. Arguments:
 *
 * 'encoding' is the encoding we are using for the entry. It could be
 * ZIP_INT_* or ZIP_STR_* or between ZIP_INT_IMM_MIN and ZIP_INT_IMM_MAX
 * for single-byte small immediate integers.
 *
 * 'rawlen' is only used for ZIP_STR_* encodings and is the length of the
 * string that this entry represents.
 *
 * The function returns the number of bytes used by the encoding/length
 * header stored in 'p'. */
// 将entry的type/length编码写入到指针p位置。函数返回编码后使用的字节数。如果p为NULL，则只返回编码type/length的字节数。
// encoding：当前entry使用的编码方式。该值为ZIP_INT_* 或 ZIP_STR_* 或 ZIP_INT_IMM_MIN～ZIP_INT_IMM_MAX之间的值。
// rawlen：只用于 ZIP_STR_* 编码，用来表示entry数据字符串的长度。
unsigned int zipStoreEntryEncoding(unsigned char *p, unsigned char encoding, unsigned int rawlen) {
    unsigned char len = 1, buf[5];

    if (ZIP_IS_STR(encoding)) {
        /* Although encoding is given it may not be set for strings,
         * so we determine it here using the raw length. */
        // 尽管传入了编码，但只是对数字类型的编码，字符串类型该值无效，所以这里我们还是自己根据rawlen来判断。
        if (rawlen <= 0x3f) {
            // 长度小于64，使用1字节表示。
            if (!p) return len;
            buf[0] = ZIP_STR_06B | rawlen;
        } else if (rawlen <= 0x3fff) {
            // 长度在64～0x3fff（16383）时，2字节表示（注意这里是大端编码）。
            len += 1;
            if (!p) return len;
            buf[0] = ZIP_STR_14B | ((rawlen >> 8) & 0x3f);
            buf[1] = rawlen & 0xff;
        } else {
            // 长度>=16384时，5字节编码，使用额外4字节表示长度（注意这里是大端编码）。
            len += 4;
            if (!p) return len;
            buf[0] = ZIP_STR_32B;
            buf[1] = (rawlen >> 24) & 0xff;
            buf[2] = (rawlen >> 16) & 0xff;
            buf[3] = (rawlen >> 8) & 0xff;
            buf[4] = rawlen & 0xff;
        }
    } else {
        /* Implies integer encoding, so length is always 1. */
        // 如果时数字类型，长度直接由类型决定，总共只需要一个字节。
        if (!p) return len;
        buf[0] = encoding;
    }

    /* Store this length at p. */
    // 如果p不为NULL，则将编码后数据写入p位置。
    memcpy(p,buf,len);
    return len;
}

/* Decode the entry encoding type and data length (string length for strings,
 * number of bytes used for the integer for integer entries) encoded in 'ptr'.
 * The 'encoding' variable is input, extracted by the caller, the 'lensize'
 * variable will hold the number of bytes required to encode the entry
 * length, and the 'len' variable will hold the entry length.
 * On invalid encoding error, lensize is set to 0. */
// 解码ptr指针的数据，解析出编码类型和真实数据长度（对于字符串就是字符串长度，对于int类型就是数字占用字节数）。
// encoding传入ptr第一个字节的编码类型，由调用者解析。
// lensize用于返回编码 type和数据长度 所用的字节数。如果传入的encoding判断是无效编码，则lensize会返回0。
// len用于返回entry数据的长度（字符串长度，或数字占用字节数）。
#define ZIP_DECODE_LENGTH(ptr, encoding, lensize, len) do {                    \
    if ((encoding) < ZIP_STR_MASK) {                                           \
        /* 字符串类型编码。*/ \
        if ((encoding) == ZIP_STR_06B) {                                       \
            /* 1字节编码类型，6bit表示长度。*/ \
            (lensize) = 1;                                                     \
            (len) = (ptr)[0] & 0x3f;                                           \
        } else if ((encoding) == ZIP_STR_14B) {                                \
            /* 2字节编码类型，14bit表示长度（大端）。*/ \
            (lensize) = 2;                                                     \
            (len) = (((ptr)[0] & 0x3f) << 8) | (ptr)[1];                       \
        } else if ((encoding) == ZIP_STR_32B) {                                \
            /* 5字节编码类型，额外32bit表示长度（大端）。*/ \
            (lensize) = 5;                                                     \
            (len) = ((ptr)[1] << 24) |                                         \
                    ((ptr)[2] << 16) |                                         \
                    ((ptr)[3] <<  8) |                                         \
                    ((ptr)[4]);                                                \
        } else {                                                               \
            /* 错误的字符串编码类型，lensize返回0，len也设置为0. */ \
            (lensize) = 0; /* bad encoding, should be covered by a previous */ \
            (len) = 0;     /* ZIP_ASSERT_ENCODING / zipEncodingLenSize, or  */ \
                           /* match the lensize after this macro with 0.    */ \
        }                                                                      \
    } else {                                                                   \
        /* 数字的类型编码始终是1个字节 */ \
        (lensize) = 1;                                                         \
        /* 数字存储所需字节数len，根据数字大小划分不同的类型，所需字节数为0（立即数）、1、2、3、4、8。*/ \
        if ((encoding) == ZIP_INT_8B)  (len) = 1;                              \
        else if ((encoding) == ZIP_INT_16B) (len) = 2;                         \
        else if ((encoding) == ZIP_INT_24B) (len) = 3;                         \
        else if ((encoding) == ZIP_INT_32B) (len) = 4;                         \
        else if ((encoding) == ZIP_INT_64B) (len) = 8;                         \
        else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)   \
            (len) = 0; /* 4 bit immediate */                                   \
        else                                                                   \
            (lensize) = (len) = 0; /* bad encoding */                          \
    }                                                                          \
} while(0)

/* Encode the length of the previous entry and write it to "p". This only
 * uses the larger encoding (required in __ziplistCascadeUpdate). */
// 编码前一个entry的总长度，写入p位置，并返回存储该长度所需字节数。当>=254时才需要调这个函数处理写入。
int zipStorePrevEntryLengthLarge(unsigned char *p, unsigned int len) {
    uint32_t u32;
    if (p != NULL) {
        // 先写入第一个字节254，标识我们的长度需要用额外4字节存储。
        p[0] = ZIP_BIG_PREVLEN;
        u32 = len;
        // 写入4字节长度
        memcpy(p+1,&u32,sizeof(u32));
        // 如果当前是大端模式，需要转化为小端。否则什么都不做。
        memrev32ifbe(p+1);
    }
    return 1 + sizeof(uint32_t);
}

/* Encode the length of the previous entry and write it to "p". Return the
 * number of bytes needed to encode this length if "p" is NULL. */
// 编码前一个entry的总长度，写入p位置，并返回存储该长度所需字节数。
unsigned int zipStorePrevEntryLength(unsigned char *p, unsigned int len) {
    if (p == NULL) {
        // p为NULL时只需返回编码后的字节数。1或5。
        return (len < ZIP_BIG_PREVLEN) ? 1 : sizeof(uint32_t) + 1;
    } else {
        if (len < ZIP_BIG_PREVLEN) {
            // 如果前一个entry长度小于254，则一个字节存储就可以了。
            p[0] = len;
            return 1;
        } else {
            // 如果前一个entry长度>=254，则需要用额外4字节来存储长度，调用zipStorePrevEntryLengthLarge处理。
            return zipStorePrevEntryLengthLarge(p,len);
        }
    }
}

/* Return the number of bytes used to encode the length of the previous
 * entry. The length is returned by setting the var 'prevlensize'. */
// 根据ptr指向的（前一个entry长度的）编码后数据，解析出编码所用的字节数。
#define ZIP_DECODE_PREVLENSIZE(ptr, prevlensize) do {                          \
    /* 第一字节小于ZIP_BIG_PREVLEN时，即长度小于254，只需1字节。否则需要额外4字节存储长度，一共5字节。*/ \
    if ((ptr)[0] < ZIP_BIG_PREVLEN) {                                          \
        (prevlensize) = 1;                                                     \
    } else {                                                                   \
        (prevlensize) = 5;                                                     \
    }                                                                          \
} while(0)

/* Return the length of the previous element, and the number of bytes that
 * are used in order to encode the previous element length.
 * 'ptr' must point to the prevlen prefix of an entry (that encodes the
 * length of the previous entry in order to navigate the elements backward).
 * The length of the previous entry is stored in 'prevlen', the number of
 * bytes needed to encode the previous entry length are stored in
 * 'prevlensize'. */
// 根据ptr指向的（前一个entry长度的）编码后数据，解析出编码所用的字节数，以及前一个entry的长度。
#define ZIP_DECODE_PREVLEN(ptr, prevlensize, prevlen) do {                     \
    /* 解析编码长度所用的字节数 */ \
    ZIP_DECODE_PREVLENSIZE(ptr, prevlensize);                                  \
    /* 长度用1字节编码，则该字节即为前一个entry长度，否则提取后面4个字节解析长度（注意小端编码）。*/ \
    if ((prevlensize) == 1) {                                                  \
        (prevlen) = (ptr)[0];                                                  \
    } else { /* prevlensize == 5 */                                            \
        (prevlen) = ((ptr)[4] << 24) |                                         \
                    ((ptr)[3] << 16) |                                         \
                    ((ptr)[2] <<  8) |                                         \
                    ((ptr)[1]);                                                \
    }                                                                          \
} while(0)

/* Given a pointer 'p' to the prevlen info that prefixes an entry, this
 * function returns the difference in number of bytes needed to encode
 * the prevlen if the previous entry changes of size.
 *
 * So if A is the number of bytes used right now to encode the 'prevlen'
 * field.
 *
 * And B is the number of bytes that are needed in order to encode the
 * 'prevlen' if the previous element will be updated to one of size 'len'.
 *
 * Then the function returns B - A
 *
 * So the function returns a positive number if more space is needed,
 * a negative number if less space is needed, or zero if the same space
 * is needed. */
// 给定指向一个entry前缀prevlen的指针，以及这个entry的前一个entry变化后的长度len，计算存储新的len作为prevlen是否需要resize。
// 函数返回正值表示需要扩容才能存储下新值，返回0表示不需要处理，返回负值表示需要缩容。
int zipPrevLenByteDiff(unsigned char *p, unsigned int len) {
    unsigned int prevlensize;
    // 从p指向的位置解析原来存储prevlen所用字节数prevlensize。
    ZIP_DECODE_PREVLENSIZE(p, prevlensize);
    // 根据前一个entry新的长度len，判断编码进ziplist需要的字节数。减去原使用的字节数，决定是否需要reszie。
    return zipStorePrevEntryLength(NULL, len) - prevlensize;
}

/* Check if string pointed to by 'entry' can be encoded as an integer.
 * Stores the integer value in 'v' and its encoding in 'encoding'. */
// 检测entry指向的字符串是否可以编码为数字。如果可以则函数返回1，且编码类型存储在encoding中，解析的数字存储到v中。
int zipTryEncoding(unsigned char *entry, unsigned int entrylen, long long *v, unsigned char *encoding) {
    long long value;

    // 如果entry字符串长度为0或大于32，显然无法表示为数字，返回0。
    if (entrylen >= 32 || entrylen == 0) return 0;
    // 解析成数字到value中，解析成功返回1（util.c中的string2ll）。
    if (string2ll((char*)entry,entrylen,&value)) {
        /* Great, the string can be encoded. Check what's the smallest
         * of our encoding types that can hold this value. */
        // 解析成功了，下面根据解析值的范围来确定编码类型。
        if (value >= 0 && value <= 12) {
            // 立即数编码
            *encoding = ZIP_INT_IMM_MIN+value;
        } else if (value >= INT8_MIN && value <= INT8_MAX) {
            *encoding = ZIP_INT_8B;
        } else if (value >= INT16_MIN && value <= INT16_MAX) {
            *encoding = ZIP_INT_16B;
        } else if (value >= INT24_MIN && value <= INT24_MAX) {
            *encoding = ZIP_INT_24B;
        } else if (value >= INT32_MIN && value <= INT32_MAX) {
            *encoding = ZIP_INT_32B;
        } else {
            *encoding = ZIP_INT_64B;
        }
        *v = value;
        return 1;
    }
    return 0;
}

/* Store integer 'value' at 'p', encoded as 'encoding' */
// 使用传入的encoding编码方式，将整数value，编码进p指针位置。
void zipSaveInteger(unsigned char *p, int64_t value, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64;
    // 单字节存储，直接写入就好了。其他字节需要注意小端模式存储，特别要注意24bit存储类型的处理。
    if (encoding == ZIP_INT_8B) {
        ((int8_t*)p)[0] = (int8_t)value;
    } else if (encoding == ZIP_INT_16B) {
        i16 = value;
        memcpy(p,&i16,sizeof(i16));
        memrev16ifbe(p);
    } else if (encoding == ZIP_INT_24B) {
        i32 = value<<8;
        memrev32ifbe(&i32);
        // 小端存储（指针低位存储数字低位），这个+1，跳过低位全0（前面左移了8位，所以低位现在全0无效数据）。
        memcpy(p,((uint8_t*)&i32)+1,sizeof(i32)-sizeof(uint8_t));
    } else if (encoding == ZIP_INT_32B) {
        i32 = value;
        memcpy(p,&i32,sizeof(i32));
        memrev32ifbe(p);
    } else if (encoding == ZIP_INT_64B) {
        i64 = value;
        memcpy(p,&i64,sizeof(i64));
        memrev64ifbe(p);
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
        /* Nothing to do, the value is stored in the encoding itself. */
        // 立即数，不需要额外的存储，所以不用在p位置写数据
    } else {
        assert(NULL);
    }
}

/* Read integer encoded as 'encoding' from 'p' */
// 使用指定的encoding来解析指针p处的int数据
int64_t zipLoadInteger(unsigned char *p, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64, ret = 0;
    if (encoding == ZIP_INT_8B) {
        // 1字节直接转成数字
        ret = ((int8_t*)p)[0];
    } else if (encoding == ZIP_INT_16B) {
        // 16bit，按位copy到i16中。小端模式取字节数据，如果存储模式与当前系统模式不匹配，则需要转化才能得到真正的值。
        memcpy(&i16,p,sizeof(i16));
        memrev16ifbe(&i16);
        ret = i16;
    } else if (encoding == ZIP_INT_32B) {
        memcpy(&i32,p,sizeof(i32));
        memrev32ifbe(&i32);
        ret = i32;
    } else if (encoding == ZIP_INT_24B) {
        i32 = 0;
        // 24bit，先按小端模式取数据到i32。指针最低字节（对应数据最低字节）为0，后面会右移8位去掉该字节。
        memcpy(((uint8_t*)&i32)+1,p,sizeof(i32)-sizeof(uint8_t));
        // 如果当前系统是大端模式，则需要进行转换，否则什么都不处理。
        memrev32ifbe(&i32);
        ret = i32>>8;
    } else if (encoding == ZIP_INT_64B) {
        memcpy(&i64,p,sizeof(i64));
        memrev64ifbe(&i64);
        ret = i64;
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
        // 立即数，取encoding的低4位值，然后-1即可。
        ret = (encoding & ZIP_INT_IMM_MASK)-1;
    } else {
        assert(NULL);
    }
    return ret;
}

/* Fills a struct with all information about an entry.
 * This function is the "unsafe" alternative to the one blow.
 * Generally, all function that return a pointer to an element in the ziplist
 * will assert that this element is valid, so it can be freely used.
 * Generally functions such ziplistGet assume the input pointer is already
 * validated (since it's the return value of another function). */
// 用entry相关的所有信息来填充zlentry结构。该函数是zipEnrySafe方法的不安全替代方式。
// 通常，所有返回指向ziplist中元素的指针的函数，都会断言该元素是有效的，所以它可以自由使用。
// 所以，通常对于ziplistGet这类函数都假定输入指针已经验证过了，因为它是其他函数的返回值。
static inline void zipEntry(unsigned char *p, zlentry *e) {
    // 获取p指向ziplist中对应entry的 prelen（前一个entry的长度）的编码长度以及具体值。
    ZIP_DECODE_PREVLEN(p, e->prevrawlensize, e->prevrawlen);
    // 获取p指向entry的数据编码类型。
    ZIP_ENTRY_ENCODING(p + e->prevrawlensize, e->encoding);
    // 根据编码类型，获取p指向entry的数据 type/len 的编码长度，以及数据长度。
    ZIP_DECODE_LENGTH(p + e->prevrawlensize, e->encoding, e->lensize, e->len);
    assert(e->lensize != 0); /* check that encoding was valid. */
    // entry的header长度，包含prelen、encoding两个部分，长度为编码后的字节数，即e->prevrawlensize + e->lensize。
    e->headersize = e->prevrawlensize + e->lensize;
    e->p = p;
}

/* Fills a struct with all information about an entry.
 * This function is safe to use on untrusted pointers, it'll make sure not to
 * try to access memory outside the ziplist payload.
 * Returns 1 if the entry is valid, and 0 otherwise. */
// 用entry相关的所有信息来填充zlentry结构，这个函数是安全的，对于传入指针p，会检查确保访问的是ziplist中的entry部分数据。
// 如果entry有效则返回1，否则返回0。
static inline int zipEntrySafe(unsigned char* zl, size_t zlbytes, unsigned char *p, zlentry *e, int validate_prevlen) {
    // 拿到ziplist的第一个entry开始位置和最后一个entry结束的位置，用于检查传入指针p是否在这区间。
    unsigned char *zlfirst = zl + ZIPLIST_HEADER_SIZE;
    unsigned char *zllast = zl + zlbytes - ZIPLIST_END_SIZE;
#define OUT_OF_RANGE(p) (unlikely((p) < zlfirst || (p) > zllast))

    /* If threre's no possibility for the header to reach outside the ziplist,
     * take the fast path. (max lensize and prevrawlensize are both 5 bytes) */
    // 如果判断entry的header不可能在ziplist entry范围外，则走这个if快速处理。
    // lensize 和 prevrawlensize都最大为5字节，所以这里判断p+10 < zllast。
    if (p >= zlfirst && p + 10 < zllast) {
        // 快速处理这些数据获取与上面unsafe方法处理一致。
        ZIP_DECODE_PREVLEN(p, e->prevrawlensize, e->prevrawlen);
        ZIP_ENTRY_ENCODING(p + e->prevrawlensize, e->encoding);
        ZIP_DECODE_LENGTH(p + e->prevrawlensize, e->encoding, e->lensize, e->len);
        e->headersize = e->prevrawlensize + e->lensize;
        e->p = p;
        /* We didn't call ZIP_ASSERT_ENCODING, so we check lensize was set to 0. */
        // 检查entry数据长度不应为0。验证entry数据是否有效。
        if (unlikely(e->lensize == 0))
            return 0;
        /* Make sure the entry doesn't rech outside the edge of the ziplist */
        // 检查整个entry的结束位置不应该超出ziplist entry范围。验证整个enrty是否有效。
        if (OUT_OF_RANGE(p + e->headersize + e->len))
            return 0;
        /* Make sure prevlen doesn't rech outside the edge of the ziplist */
        // 检查当前entry的prelen指定的前一个entry的起始位置不应该超出ziplist entry的范围。也就是验证prelen是否有效。
        if (validate_prevlen && OUT_OF_RANGE(p - e->prevrawlen))
            return 0;
        return 1;
    }

    // 如果不是前面快速处理，则每次准备新取一个字段时都判断当前指针是否在ziplist entry的范围内。
    // 这里首先判断传入指针p是否在范围内。
    /* Make sure the pointer doesn't rech outside the edge of the ziplist */
    if (OUT_OF_RANGE(p))
        return 0;

    /* Make sure the encoded prevlen header doesn't reach outside the allocation */
    // p在范围内，则可以取它指向entry的第一个字段prelen 和 prelen的编码长度（ziplist中存储的是编码后的字节）。
    // 这里我们取到了prelen的编码长度存入e->prevrawlensize，然后根据这个编码长度找到下一个字段encoding起始位置，判断该位置是否在范围内。
    ZIP_DECODE_PREVLENSIZE(p, e->prevrawlensize);
    if (OUT_OF_RANGE(p + e->prevrawlensize))
        return 0;

    /* Make sure encoded entry header is valid. */
    // 解析encoding，根据编码类型获取entry实际数据的type/len的编码长度，其实也就是第二个字段encoding的编码长度。
    ZIP_ENTRY_ENCODING(p + e->prevrawlensize, e->encoding);
    e->lensize = zipEncodingLenSize(e->encoding);
    if (unlikely(e->lensize == ZIP_ENCODING_SIZE_INVALID))
        return 0;

    /* Make sure the encoded entry header doesn't reach outside the allocation */
    // 拿到了prelen 和 encoding的编码长度，我们就可以找到entry真实数据的起始位置了。检查该位置是否在范围内。
    if (OUT_OF_RANGE(p + e->prevrawlensize + e->lensize))
        return 0;

    /* Decode the prevlen and entry len headers. */
    // 解析entry的header数据，即获取prelen放入e->prevrawlen，以及从encoding域获取entry真实数据的长度放入e->len。
    // 并更新entry的header部分编码后的长度，放入e->headersize。
    ZIP_DECODE_PREVLEN(p, e->prevrawlensize, e->prevrawlen);
    ZIP_DECODE_LENGTH(p + e->prevrawlensize, e->encoding, e->lensize, e->len);
    e->headersize = e->prevrawlensize + e->lensize;

    /* Make sure the entry doesn't rech outside the edge of the ziplist */
    // 检查当前解析entry的结束位置是否在范围内。
    if (OUT_OF_RANGE(p + e->headersize + e->len))
        return 0;

    /* Make sure prevlen doesn't rech outside the edge of the ziplist */
    // 检查当前entry的prelen指定的前一个entry的起始位置是否在范围内。也就是验证prelen是否有效
    if (validate_prevlen && OUT_OF_RANGE(p - e->prevrawlen))
        return 0;

    // 指针指向当前entry在ziplist中的位置。
    e->p = p;
    return 1;
#undef OUT_OF_RANGE
}

/* Return the total number of bytes used by the entry pointed to by 'p'. */
// 返回指针p指向的ziplist中对应entry使用的总字节数（编码后存储长度）。
static inline unsigned int zipRawEntryLengthSafe(unsigned char* zl, size_t zlbytes, unsigned char *p) {
    zlentry e;
    assert(zipEntrySafe(zl, zlbytes, p, &e, 0));
    return e.headersize + e.len;
}

/* Return the total number of bytes used by the entry pointed to by 'p'. */
// 返回指针p对应entry使用的总字节数（编码后存储长度）。与zipRawEntryLengthSafe功能一致，只是这个是不安全的。
static inline unsigned int zipRawEntryLength(unsigned char *p) {
    zlentry e;
    zipEntry(p, &e);
    return e.headersize + e.len;
}

/* Validate that the entry doesn't reach outside the ziplist allocation. */
// 通过调用zipEntrySafe来验证entry是否在ziplist entry部分范围内。
static inline void zipAssertValidEntry(unsigned char* zl, size_t zlbytes, unsigned char *p) {
    zlentry e;
    assert(zipEntrySafe(zl, zlbytes, p, &e, 1));
}

/* Create a new empty ziplist. */
// 创建一个空的ziplist
unsigned char *ziplistNew(void) {
    // 初始时只有header部分（总长度、tail offset、总entry数）+ 结束符。4+4+2+1=11字节。暂时只分配这么多空间。
    unsigned int bytes = ZIPLIST_HEADER_SIZE+ZIPLIST_END_SIZE;
    unsigned char *zl = zmalloc(bytes);
    // 填充总长度，注意我们是使用小端存储。
    ZIPLIST_BYTES(zl) = intrev32ifbe(bytes);
    // 填充tail offset，没有元素，offset即为ZIPLIST_HEADER_SIZE。注意我们是使用小端存储。
    ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(ZIPLIST_HEADER_SIZE);
    // 填充entry总数。这里也是使用小端存储，但0无差别，所以不需要使用intrev32ifbe处理。
    ZIPLIST_LENGTH(zl) = 0;
    // 末尾填充结束符
    zl[bytes-1] = ZIP_END;
    return zl;
}

/* Resize the ziplist. */
// ziplist空间调整为len。
unsigned char *ziplistResize(unsigned char *zl, unsigned int len) {
    // 重新分配空间，原数据会原样copy过去。
    zl = zrealloc(zl,len);
    // 需要更新ziplist的总长度。
    ZIPLIST_BYTES(zl) = intrev32ifbe(len);
    // 写入结束符。
    zl[len-1] = ZIP_END;
    return zl;
}

/* When an entry is inserted, we need to set the prevlen field of the next
 * entry to equal the length of the inserted entry. It can occur that this
 * length cannot be encoded in 1 byte and the next entry needs to be grow
 * a bit larger to hold the 5-byte encoded prevlen. This can be done for free,
 * because this only happens when an entry is already being inserted (which
 * causes a realloc and memmove). However, encoding the prevlen may require
 * that this entry is grown as well. This effect may cascade throughout
 * the ziplist when there are consecutive entries with a size close to
 * ZIP_BIG_PREVLEN, so we need to check that the prevlen can be encoded in
 * every consecutive entry.
 *
 * Note that this effect can also happen in reverse, where the bytes required
 * to encode the prevlen field can shrink. This effect is deliberately ignored,
 * because it can cause a "flapping" effect where a chain prevlen fields is
 * first grown and then shrunk again after consecutive inserts. Rather, the
 * field is allowed to stay larger than necessary, because a large prevlen
 * field implies the ziplist is holding large entries anyway.
 *
 * The pointer "p" points to the first entry that does NOT need to be
 * updated, i.e. consecutive fields MAY need an update. */
// 当插入一个entry时，我们需要设置下一个entry的prevlen为插入entry的总长度，可能导致prevlen从1字节增加到5字节。
// 不过如果只是变更下一个entry，没什么影响，因为我们插入元素本来就要realloc和memmove的。
// 但是下一个entry的prevlen字节数变化会导致整个entry总长度变化，从而再后面的prevlen存储长度也可能变化，
// 如果开始所有的prevlen的值都接近ZIP_BIG_PREVLEN的话，就可能引起整个ziplist的级联变更反应。
// 所有我们需要检查每个连续entry中的prevlen是否仍能用原来的字节数保存。

// 注意，反过来prevlen减小，从5字节变为1字节存储，也可能会产生这种影响。
// 但是我们故意忽略了这种变化的处理，因为当我们插入/删除/修改数据时，prevlen可能先增加再减小，产生摇摆效应，每次都处理显然很耗时耗性能。
// 所以我们会保持使用5字节来存储，虽然会多使用一点内存，但减少了级联更新处理。

// 传入的指针p指向的entry不需要处理prevlen更新，我们需要检查该entry后面的连续entry并进行处理。
unsigned char *__ziplistCascadeUpdate(unsigned char *zl, unsigned char *p) {
    // 当前检查的entry
    zlentry cur;
    // 上一个更新的entry相关长度信息
    size_t prevlen, prevlensize, prevoffset; /* Informat of the last changed entry. */
    // 用于处理在头部插入数据的情况，此时p指向新插入的数据，而下一个entry为老的头，老的头entry中prevlen为0，后面需要更新。
    size_t firstentrylen; /* Used to handle insert at head. */
    size_t rawlen, curlen = intrev32ifbe(ZIPLIST_BYTES(zl));
    size_t extra = 0, cnt = 0, offset;
    // 更新一个entry的prevlen需要的额外字节数，因为忽略了prevlen缩减，所以这里都是增加字节。
    size_t delta = 4; /* Extra bytes needed to update a entry's prevlen (5-1). */
    // 最后一个entry位置指针
    unsigned char *tail = zl + intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl));

    /* Empty ziplist */
    // 如果p直接指向结束符，不需要处理，直接返回。
    if (p[0] == ZIP_END) return zl;

    // 获取p指向的entry。这里不需要使用safe方法，因为传入的指针p在调用函数中已经验证。
    zipEntry(p, &cur); /* no need for "safe" variant since the input pointer was validated by the function that returned it. */
    // 因为prelen的数据是前一个entry的长度，所以我们需要先计算当前entry的信息，用于后面entry更新。注意我们传入p指向的entry是不需要更新的。
    // 处理的第一个entry的长度，也就是下一个entry使用的prevlen。
    firstentrylen = prevlen = cur.headersize + cur.len;
    // 获取该prevlen编码字节数
    prevlensize = zipStorePrevEntryLength(NULL, prevlen);
    // entry起始指针相对ziplist的offset
    prevoffset = p - zl;
    // p指向下一个要处理的entry。
    p += prevlen;

    /* Iterate ziplist to find out how many extra bytes do we need to update it. */
    // 从第一个需要更新的entry起，迭代检查后面的连续entry，看我们总共需要在扩容多少额外的空间来进行级联更新。
    while (p[0] != ZIP_END) {
        // 获取当前p指向的entry
        assert(zipEntrySafe(zl, curlen, p, &cur, 0));

        /* Abort when "prevlen" has not changed. */
        // 如果当前处理entry的prevlen没有变化，显然当前entry不需要更新，跳出循环。
        if (cur.prevrawlen == prevlen) break;

        /* Abort when entry's "prevlensize" is big enough. */
        // 如果当前处理entry的prevlen编码长度足够大，能后容纳下新的长度值，则直接更新prevlen，并跳出循环。
        if (cur.prevrawlensize >= prevlensize) {
            if (cur.prevrawlensize == prevlensize) {
                // 如果编码长度一致，则正常写入长度。
                zipStorePrevEntryLength(p, prevlen);
            } else {
                /* This would result in shrinking, which we want to avoid.
                 * So, set "prevlen" in the available bytes. */
                // 如果已有的编码长度5字节，而我们新的prevlen只需要1字节就可以存储，这里直接调用Large方式强制按5字节处理。
                // 不处理缩容，只处理扩容更简单处理，且这样后面entry的prevlen都不会变化，不再需要级联更新处理了。
                zipStorePrevEntryLengthLarge(p, prevlen);
            }
            break;
        }

        /* cur.prevrawlen means cur is the former head entry. */
        // 执行到这里，显然我们当前entry是需要扩容的。引起的原因有：
        // 要么当前entry是之前的header，在头部插入的元素很长，导致当前entry需要扩容。cur.prevrawlen == 0 表示是原 header。
        // 要么前面一个entry的长度发生了变化，从而导致当前entry1字节存不下了。cur.prevrawlen + delta == prevlen 表示前一个entry长度+4，即也扩容了。
        assert(cur.prevrawlen == 0 || cur.prevrawlen + delta == prevlen);

        /* Update prev entry's info and advance the cursor. */
        // 获取当前entry的总长度
        rawlen = cur.headersize + cur.len;
        // 更新下一个entry的prevlen，因为需要扩容，所以值为当前总长度加delta。
        prevlen = rawlen + delta;
        // 更新下一个entry的prevlen的编码字节数。
        prevlensize = zipStorePrevEntryLength(NULL, prevlen);
        // 更新与ziplist首字符的offset
        prevoffset = p - zl;
        // 更新下一轮待处理的entry指针
        p += rawlen;
        // 更新需要扩容的字节数+delta。
        extra += delta;
        // 需要扩容的entry数+1
        cnt++;
    }

    /* Extra bytes is zero all update has been done(or no need to update). */
    // 如果不需要扩容（长度没变，只需更新prevlen数据），直接返回。
    if (extra == 0) return zl;

    /* Update tail offset after loop. */
    // 更新tail offset
    if (tail == zl + prevoffset) {
        /* When the the last entry we need to update is also the tail, update tail offset
         * unless this is the only entry that was updated (so the tail offset didn't change). */
        // 如果最后一个需要更新的entry是tail的话，因为tail entry的prevlen扩容不能计入tail offset中，所以特殊处理。
        // 另外如果只有tail需要扩容的话，offset不用变。
        if (extra - delta != 0) {
            ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+extra-delta);
        }
    } else {
        /* Update the tail offset in cases where the last entry we updated is not the tail. */
        // 如果最后一个更新的entry不是tail，那tail offset = 原offset + extra。
        ZIPLIST_TAIL_OFFSET(zl) =
            intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+extra);
    }

    /* Now "p" points at the first unchanged byte in original ziplist,
     * move data after that to new ziplist. */
    // 现在p指向原始ziplist的第一个不需要更改的entry位置，重新分配空间，然后将该位置后面的数据都移到新的ziplist中。
    offset = p - zl;
    zl = ziplistResize(zl, curlen + extra);
    p = zl + offset;
    memmove(p + extra, p, curlen - offset - 1);
    // p重新指向新ziplist的第一个不需要更改的entry位置
    p += extra;

    /* Iterate all entries that need to be updated tail to head. */
    // 迭代所有需要更新的entry处理。cnt表示剩余需要处理的entry数。
    while (cnt) {
        // prevoffset指向待处理的最后一个entry的位置。这里获取最后一个要处理的entry。不需要用safe方式，因为前面我们已经对这些entry进行过迭代。
        zipEntry(zl + prevoffset, &cur); /* no need for "safe" variant since we already iterated on all these entries above. */
        // 当前entry的总长度
        rawlen = cur.headersize + cur.len;
        /* Move entry to tail and reset prevlen. */
        // 将当前entry除了prevlen部分的数据后移，放置到最终位置。
        memmove(p - (rawlen - cur.prevrawlensize), 
                zl + prevoffset + cur.prevrawlensize, 
                rawlen - cur.prevrawlensize);
        // p指针指向新的entry的首字符处理。
        p -= (rawlen + delta);
        // 更新prevlen数据
        if (cur.prevrawlen == 0) {
            /* "cur" is the previous head entry, update its prevlen with firstentrylen. */
            // cur.prevrawlen == 0 表示当前entry是老的ziplist的head entry。这里我们需要把最开始不需要处理的那个节点的长度更新进去。
            zipStorePrevEntryLength(p, firstentrylen);
        } else {
            /* An entry's prevlen can only increment 4 bytes. */
            // 当前entry不是head，那么只能是因为前一个entry的prevlen字段扩容导致的，只能+4，所以也可以直接更新。
            zipStorePrevEntryLength(p, cur.prevrawlen+delta);
        }
        /* Foward to previous entry. */
        // prevoffset也回退一个entry长度，最终zl+prevoffset指向前面一个待处理的entry。
        prevoffset -= cur.prevrawlen;
        cnt--;
    }
    return zl;
}

/* Delete "num" entries, starting at "p". Returns pointer to the ziplist. */
// 删除最多num个entries（因为有可能p后面没有这么多元素），返回指向ziplist的指针，可能与原ziplist指针不同。
unsigned char *__ziplistDelete(unsigned char *zl, unsigned char *p, unsigned int num) {
    unsigned int i, totlen, deleted = 0;
    size_t offset;
    int nextdiff = 0;
    zlentry first, tail;
    // ziplist总长度
    size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));

    // 获取待删除的第一个entry
    zipEntry(p, &first); /* no need for "safe" variant since the input pointer was validated by the function that returned it. */
    // 注意这里因为参数num可能传-1，解释为无符号数则为最大值，因而这里我们判断循环结束时需要用ZIP_END标识和迭代次数num来作为结束。
    for (i = 0; p[0] != ZIP_END && i < num; i++) {
        // 向后遍历，计算总共可以删除的entry数
        p += zipRawEntryLengthSafe(zl, zlbytes, p);
        deleted++;
    }

    // p最终会指向最后一个待删除的entry的末尾。除非传入num=0，一个entry都不删，p=first.p；否则p>first.p。
    assert(p >= first.p);
    // 计算最终会释放的总字节数。
    totlen = p-first.p; /* Bytes taken by the element(s) to delete. */
    if (totlen > 0) {
        uint32_t set_tail;
        if (p[0] != ZIP_END) {
            // p不指向结束符，那么我们需要将p指向的entry，以及后面的entry数据都向前移。
            /* Storing `prevrawlen` in this entry may increase or decrease the
             * number of bytes required compare to the current `prevrawlen`.
             * There always is room to store this, because it was previously
             * stored by an entry that is now being deleted. */
            // 我们需要更新当前p指向的entry的prevlen数据。使用第一个删除的entry，即first中的prevlen值填充。
            // 因为更新该prevlen数据可能导致当前p指向entry的prevlen字段扩/缩容，所以这里对比计算一下。
            // nextdiff为4，表示需要扩容，-4表示需要缩容，0表示不需要处理。
            nextdiff = zipPrevLenByteDiff(p,first.prevrawlen);

            /* Note that there is always space when p jumps backward: if
             * the new previous entry is large, one of the deleted elements
             * had a 5 bytes prevlen header, so there is for sure at least
             * 5 bytes free and we need just 4. */
            // 如果需要扩容，p前面我们删除了entry所获得的空间肯定是至少5字节的，因为first中需要5字节来存储prevlen。所以可以p-4进行扩容。
            // 另外要缩容的话，得益于entry结构的设计，第一个字段就是prevlen，所以直接p+4就缩容了。
            p -= nextdiff;
            assert(p >= first.p && p<zl+zlbytes-1);
            // 空间搞定，写入prevlen数据。
            zipStorePrevEntryLength(p,first.prevrawlen);

            /* Update offset for tail */
            // 计算新的tail offset值。老的offset减去删除的数量。另外注意需要考虑扩缩容的影响。
            set_tail = intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))-totlen;

            /* When the tail contains more than one entry, we need to take
             * "nextdiff" in account as well. Otherwise, a change in the
             * size of prevlen doesn't have an effect on the *tail* offset. */
            // 取到p指向的entry，如果该entry是最后一个，计算offset的时候，tail entry的扩/缩容nextdiff不用计入。
            // 只有p指向的entry后面还有entry时，offset才需要考虑nextdiff。
            assert(zipEntrySafe(zl, zlbytes, p, &tail, 1));
            if (p[tail.headersize+tail.len] != ZIP_END) {
                set_tail = set_tail + nextdiff;
            }

            /* Move tail to the front of the ziplist */
            /* since we asserted that p >= first.p. we know totlen >= 0,
             * so we know that p > first.p and this is guaranteed not to reach
             * beyond the allocation, even if the entries lens are corrupted. */
            // 将p后面的数据前移，移动到first entry首字符对应的位置，移动字符数为zlbytes-(p-zl)-1。（-1是减去结束符）
            size_t bytes_to_move = zlbytes-(p-zl)-1;
            memmove(first.p,p,bytes_to_move);
        } else {
            /* The entire tail was deleted. No need to move memory. */
            // 如果最后的那个entry也在删除名单中，那么我们不需要移动数据。后面直接resize截取就好了。
            // 这里计算新的tail offset值。first entry的前一个entry即为新的tail。
            set_tail = (first.p-zl)-first.prevrawlen;
        }

        /* Resize the ziplist */
        // 缩容ziplist，因为前面已经处理了数据移动，所以不会导致数据丢。
        offset = first.p-zl;
        zlbytes -= totlen - nextdiff;
        zl = ziplistResize(zl, zlbytes);
        // p指向删除后，第一个已经处理好的entry，该entry后面的entry需要处理prevlen的级联更新。
        p = zl+offset;

        /* Update record count */
        // 更新ziplist中总的entry数
        ZIPLIST_INCR_LENGTH(zl,-deleted);

        /* Set the tail offset computed above */
        // 设置ziplist的tail offset。
        assert(set_tail <= zlbytes - ZIPLIST_END_SIZE);
        ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(set_tail);

        /* When nextdiff != 0, the raw length of the next entry has changed, so
         * we need to cascade the update throughout the ziplist */
        // 当nextdiff!=0时，p指向的entry长度已经改变了，需要级联更新后面entry的prevlen。
        if (nextdiff != 0)
            zl = __ziplistCascadeUpdate(zl,p);
    }
    return zl;
}

/* Insert item at "p". */
// 在p位置插入元素。返回指向ziplist的指针，可能与原ziplist指针不同。
unsigned char *__ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {
    // 获取当前ziplist长度，注意小端模式存储的。
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), reqlen, newlen;
    unsigned int prevlensize, prevlen = 0;
    size_t offset;
    int nextdiff = 0;
    unsigned char encoding = 0;
    long long value = 123456789; /* initialized to avoid warning. Using a value
                                    that is easy to see if for some reason
                                    we use it uninitialized. */
    zlentry tail;

    /* Find out prevlen for the entry that is inserted. */
    // 找到我们要插入点的前一个entry的长度，即获取prevlen。
    if (p[0] != ZIP_END) {
        // 如果要插入点不是末尾，则p肯定指向某个entry，直接从该entry获取prevlen。
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
    } else {
        // 如果插入点是末尾，则定位到最后一个entry位置。
        unsigned char *ptail = ZIPLIST_ENTRY_TAIL(zl);
        // 如果当前ziplist中一个元素都没有，则ptail显然也是指向ZIP_END，那么我们要插入的是第一个元素，prevlen为默认值0就可以，不需要设置。
        // 否则我们需要调用 zipRawEntryLengthSafe 先解析出ptail指向的entry，然后获取它的总长度作为prevlen。
        if (ptail[0] != ZIP_END) {
            // 为什么不使用指针减法来获取总长度？p指向ZIP_END，ptail指向最后一个entry的开始，p-ptail即为最后一个entry的len。
            prevlen = zipRawEntryLengthSafe(zl, curlen, ptail);
        }
    }

    /* See if the entry can be encoded */
    // 检查待写入的数据是否可以编码为数字。
    if (zipTryEncoding(s,slen,&value,&encoding)) {
        /* 'encoding' is set to the appropriate integer encoding */
        // 可以编码为数字，获取这样编码需要额外使用多少字节存储数字值。
        // 最终value为编码后的数字，encoding为数字编码类型，reqlen为数字存储需要额外的字节数。
        reqlen = zipIntSize(encoding);
    } else {
        /* 'encoding' is untouched, however zipStoreEntryEncoding will use the
         * string length to figure out how to encode it. */
        // 不能编码为数字，encoding目前是未知的，这里reqlen即为需要存储字符串需要的额外数据空间。后面需要通过这个长度来计算字符串编码类型。
        reqlen = slen;
    }
    /* We need space for both the length of the previous entry and
     * the length of the payload. */
    // 前面reqlen只加了存储entry真实数据（编码后）需要的额外空间。
    // 这里要再加上header部分长度，即存储prevlen需要的字节数（1或5），和存储encoding（编码方式type、entry数据长度len）需要的字节数。
    reqlen += zipStorePrevEntryLength(NULL,prevlen);
    reqlen += zipStoreEntryEncoding(NULL,encoding,slen);

    /* When the insert position is not equal to the tail, we need to
     * make sure that the next entry can hold this entry's length in
     * its prevlen field. */
    // 当插入位置不是tail时，我们需要确保下一个entry的prevlen位置能存储下当前entry的总长度。因为原先该字段可能只需要1字节，而现在可能需要5字节。
    int forcelarge = 0;
    nextdiff = (p[0] != ZIP_END) ? zipPrevLenByteDiff(p,reqlen) : 0;
    // nextdiff可以为0、4、-4。
    // 为0时要么p指向ZIP_END，要么新插入entry长度与原prevlen一致，最终直接更新数据就好了，不需要变更存储的字节数，不需要进行级联更新。
    // 为4说明新entry的长度比原prevlen大，插入元素后，该prevlen需要扩容才能写入新的数据。另外我们向后memmove数据时，需要增加4字节数据，用于长度填充。
    // 为-4说明新entry的长度比原prevlen小，需要对prevlen缩容，所以我们memmove数据的时候，考虑要减少4字节。
    // 后面移动数据时，如果prevlen没变，则使用memmove(p+reqlen, p, curlen-offset-1)，移动的时候没处理结束标识，最后添加。
    // 显然如果prevlen有变化，则通过传入的第二个参数为p-4或p+4来控制该字段的扩容缩容，相应的第三个参数也需要+4或-4来处理移动字节数。

    // 注意我们ziplist resize总是需要重新分配的空间大于当前已使用的空间，否则缩容可能会导致当前ziplist末尾数据被截掉，从而破坏末尾的entry。
    // 所以对于 nextdiff==-4 && reqlen<4 的情况，我们这里重新分配curlen+reqlen空间，只将已有的数据后移，不对prelen缩容，强制使用5字节存储。
    // 这样我们插入了新数据，后面数据编码长度又没有变，也不用进行级联更新操作。
    // 感觉这种情况，不需要重新分配空间，因为缩容减小的空间足够写入新插入的数据了，不知道为什么不这样处理？
    // 这里reqlen<4是可以理解的，虽然nextdiff=-4说明之前我们存储prevlen使用的是5字节，但可能是为了减少级联更新而强制使用5字节存储，真正len可能小于254。
    if (nextdiff == -4 && reqlen < 4) {
        // nextdiff设为0表示不需要对prevlen字节进行变更，因而也就不需要进行级联更新。forcelarge设为1表示强制使用5字节存储prevlen。
        nextdiff = 0;
        forcelarge = 1;
    }

    /* Store offset because a realloc may change the address of zl. */
    // 存储插入位置p与zl的偏移offset，因为重新分配空间可能换了一个zl地址，这样好再定位到插入位置。
    offset = p-zl;
    // 计算插入元素后，所需的总空间大小。
    newlen = curlen+reqlen+nextdiff;
    // ziplist resize，p重新指向插入位置。
    zl = ziplistResize(zl,newlen);
    p = zl+offset;

    /* Apply memory move when necessary and update tail offset. */
    // 处理entry后移为新元素腾出空间，并更新header中的tail offset值。
    if (p[0] != ZIP_END) {
        /* Subtract one because of the ZIP_END bytes */
        // 如果插入位置不是在末尾，则需要把p后面的数据后移reqlen位置，为新元素腾地方。
        memmove(p+reqlen,p-nextdiff,curlen-offset-1+nextdiff);

        /* Encode this entry's raw length in the next entry. */
        // 前面 nextdiff==-4 && reqlen<4 的情况，我们会强制使用5字节来存储prevlen。其他情况选择合适的方式存储。
        if (forcelarge)
            zipStorePrevEntryLengthLarge(p+reqlen,reqlen);
        else
            zipStorePrevEntryLength(p+reqlen,reqlen);

        /* Update offset for tail */
        // 更新ziplist新的tail offset。因为新增了reqlen，所以需在原来基础上加上reqlen。注意小端解析、小端存储。
        ZIPLIST_TAIL_OFFSET(zl) =
            intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+reqlen);

        /* When the tail contains more than one entry, we need to take
         * "nextdiff" in account as well. Otherwise, a change in the
         * size of prevlen doesn't have an effect on the *tail* offset. */
        // 因为nextdiff主要是为了扩/缩prevlen使用的。如果插入元素后面只有一个entry，则该entry就是tail，计算offset只需加reqlen就可以了。
        // 当插入的新元素的后面不止一个entry时，我们就需要考虑nextdiff影响，因此计算offset还需要加上nextdiff。
        assert(zipEntrySafe(zl, newlen, p+reqlen, &tail, 1));
        if (p[reqlen+tail.headersize+tail.len] != ZIP_END) {
            ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+nextdiff);
        }
    } else {
        /* This element will be the new tail. */
        // 插入位置为末尾，则tail offset即为p-zl。
        ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(p-zl);
    }

    /* When nextdiff != 0, the raw length of the next entry has changed, so
     * we need to cascade the update throughout the ziplist */
    // 当 nextdiff!=0 时，prevlen需要扩/缩容，则该entry长度有变动，所以需要对后面所有entry做级联更新。
    if (nextdiff != 0) {
        // 因为级联更新可能改变zl指针，所以这里先保存p的偏移量。
        offset = p-zl;
        // 级联更新处理
        zl = __ziplistCascadeUpdate(zl,p+reqlen);
        p = zl+offset;
    }

    /* Write the entry */
    // p位置写入待插入的元素。先写prevlen、再写编码type/len、最后写入数据。
    p += zipStorePrevEntryLength(p,prevlen);
    p += zipStoreEntryEncoding(p,encoding,slen);
    if (ZIP_IS_STR(encoding)) {
        // 字符串直接填充
        memcpy(p,s,slen);
    } else {
        // 数字按不同的编码类型处理。
        zipSaveInteger(p,value,encoding);
    }
    // ziplist总的entry数+1
    ZIPLIST_INCR_LENGTH(zl,1);
    return zl;
}

/* Merge ziplists 'first' and 'second' by appending 'second' to 'first'.
 *
 * NOTE: The larger ziplist is reallocated to contain the new merged ziplist.
 * Either 'first' or 'second' can be used for the result.  The parameter not
 * used will be free'd and set to NULL.
 *
 * After calling this function, the input parameters are no longer valid since
 * they are changed and free'd in-place.
 *
 * The result ziplist is the contents of 'first' followed by 'second'.
 *
 * On failure: returns NULL if the merge is impossible.
 * On success: returns the merged ziplist (which is expanded version of either
 * 'first' or 'second', also frees the other unused input ziplist, and sets the
 * input ziplist argument equal to newly reallocated ziplist return value. */
// 合并ziplist，将second追加到first后面。
// 注意我们使用较大的那个ziplist来进行扩容形成新的ziplist，另外一个ziplist将被释放并设置为NULL。
// 当这个函数调用后，传入的两个ziplist不再有效，因为他们都可能被改变。返回的ziplist元素为first+secend。
// 如果不能进行merge，则返回NULL。否则返回新的包含first+secend元素的ziplist。
unsigned char *ziplistMerge(unsigned char **first, unsigned char **second) {
    /* If any params are null, we can't merge, so NULL. */
    // 如果任何一个传入的ziplist为NULL，则我们不能进行merge。
    if (first == NULL || *first == NULL || second == NULL || *second == NULL)
        return NULL;

    /* Can't merge same list into itself. */
    // 如果传入的两个参数都指向同一个ziplist，我们不能merge同一个ziplist到它自身，所以也返回NULL
    if (*first == *second)
        return NULL;

    // 分别获取两个ziplist的总长度，以及总的entry数
    size_t first_bytes = intrev32ifbe(ZIPLIST_BYTES(*first));
    size_t first_len = intrev16ifbe(ZIPLIST_LENGTH(*first));

    size_t second_bytes = intrev32ifbe(ZIPLIST_BYTES(*second));
    size_t second_len = intrev16ifbe(ZIPLIST_LENGTH(*second));

    int append;
    unsigned char *source, *target;
    size_t target_bytes, source_bytes;
    /* Pick the largest ziplist so we can resize easily in-place.
     * We must also track if we are now appending or prepending to
     * the target ziplist. */
    // 选择较长的那个ziplist来进行扩容，可能更容易就地realloc。同时我们需要标记基于哪个是target进行的resize。
    if (first_len >= second_len) {
        /* retain first, append second to first. */
        target = *first;
        target_bytes = first_bytes;
        source = *second;
        source_bytes = second_bytes;
        append = 1;
    } else {
        /* else, retain second, prepend first to second. */
        target = *second;
        target_bytes = second_bytes;
        source = *first;
        source_bytes = first_bytes;
        append = 0;
    }

    /* Calculate final bytes (subtract one pair of metadata) */
    // 计算最终合并后总的字节数，及entry数。
    size_t zlbytes = first_bytes + second_bytes -
                     ZIPLIST_HEADER_SIZE - ZIPLIST_END_SIZE;
    size_t zllength = first_len + second_len;

    /* Combined zl length should be limited within UINT16_MAX */
    // 判断总的entry数是否达到UINT16_MAX。
    zllength = zllength < UINT16_MAX ? zllength : UINT16_MAX;

    /* Save offset positions before we start ripping memory apart. */
    // 保存原始两个ziplist的tail offset。
    size_t first_offset = intrev32ifbe(ZIPLIST_TAIL_OFFSET(*first));
    size_t second_offset = intrev32ifbe(ZIPLIST_TAIL_OFFSET(*second));

    /* Extend target to new zlbytes then append or prepend source. */
    // 将目标ziplist扩容
    target = zrealloc(target, zlbytes);
    if (append) {
        /* append == appending to target */
        /* Copy source after target (copying over original [END]):
         *   [TARGET - END, SOURCE - HEADER] */
        // append表示target是first扩容所得，直接将second中的entries追加到target即可。
        memcpy(target + target_bytes - ZIPLIST_END_SIZE,
               source + ZIPLIST_HEADER_SIZE,
               source_bytes - ZIPLIST_HEADER_SIZE);
    } else {
        /* !append == prepending to target */
        /* Move target *contents* exactly size of (source - [END]),
         * then copy source into vacated space (source - [END]):
         *   [SOURCE - END, TARGET - HEADER] */
        // target是second扩容所得，需要先将数据后移为first数据加入挪出空间。
        memmove(target + source_bytes - ZIPLIST_END_SIZE,
                target + ZIPLIST_HEADER_SIZE,
                target_bytes - ZIPLIST_HEADER_SIZE);
        // 将first数据填入。
        memcpy(target, source, source_bytes - ZIPLIST_END_SIZE);
    }

    /* Update header metadata. */
    // 更新最终新的ziplist的总长度和总entry个数
    ZIPLIST_BYTES(target) = intrev32ifbe(zlbytes);
    ZIPLIST_LENGTH(target) = intrev16ifbe(zllength);
    /* New tail offset is:
     *   + N bytes of first ziplist
     *   - 1 byte for [END] of first ziplist
     *   + M bytes for the offset of the original tail of the second ziplist
     *   - J bytes for HEADER because second_offset keeps no header. */
    // 更新新的ziplist的tail offset。
    ZIPLIST_TAIL_OFFSET(target) = intrev32ifbe(
                                   (first_bytes - ZIPLIST_END_SIZE) +
                                   (second_offset - ZIPLIST_HEADER_SIZE));

    /* __ziplistCascadeUpdate just fixes the prev length values until it finds a
     * correct prev length value (then it assumes the rest of the list is okay).
     * We tell CascadeUpdate to start at the first ziplist's tail element to fix
     * the merge seam. */
    // 追加合并后可能会引起来自second的entry级联更新prevlen。
    target = __ziplistCascadeUpdate(target, target+first_offset);

    /* Now free and NULL out what we didn't realloc */
    // 释放空间，返回新的ziplist。
    // 注意这里把target赋值给了前面选的扩容目标，而另一个则释放掉ziplist并设置为NULL。
    if (append) {
        zfree(*second);
        *second = NULL;
        *first = target;
    } else {
        zfree(*first);
        *first = NULL;
        *second = target;
    }
    return target;
}

unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where) {
    unsigned char *p;
    // 快速定位到ziplist中，要插入的位置。push只能从ziplist的头或者尾。
    p = (where == ZIPLIST_HEAD) ? ZIPLIST_ENTRY_HEAD(zl) : ZIPLIST_ENTRY_END(zl);
    return __ziplistInsert(zl,p,s,slen);
}

/* Returns an offset to use for iterating with ziplistNext. When the given
 * index is negative, the list is traversed back to front. When the list
 * doesn't contain an element at the provided index, NULL is returned. */
// 返回ziplist中指定index的offset，用于ziplistNext迭代使用。
// 当给定的index为负时，从后往前查找。当根据index找不到元素时，返回NULL。
unsigned char *ziplistIndex(unsigned char *zl, int index) {
    unsigned char *p;
    unsigned int prevlensize, prevlen = 0;
    // 总的字节数
    size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));
    if (index < 0) {
        // index小于0，逆向查找。
        index = (-index)-1;
        // 找到tail节点位置。
        p = ZIPLIST_ENTRY_TAIL(zl);
        if (p[0] != ZIP_END) {
            /* No need for "safe" check: when going backwards, we know the header
             * we're parsing is in the range, we just need to assert (below) that
             * the size we take doesn't cause p to go outside the allocation. */
            // 获取当前entry中的prevlen数据。
            ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
            while (prevlen > 0 && index--) {
                // 每次通过prevlen，向前跳着查找。如果prevlen=0表示到第一个entry了，或者index减为0表示找到了对应entry都退出循环。
                p -= prevlen;
                assert(p >= zl + ZIPLIST_HEADER_SIZE && p < zl + zlbytes - ZIPLIST_END_SIZE);
                ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
            }
        }
    } else {
        // 找到head节点的位置
        p = ZIPLIST_ENTRY_HEAD(zl);
        while (index--) {
            /* Use the "safe" length: When we go forward, we need to be careful
             * not to decode an entry header if it's past the ziplist allocation. */
            // 正向查找。要么index=0表示找到对应entry，要么到了结束符，都退出循环。
            p += zipRawEntryLengthSafe(zl, zlbytes, p);
            if (p[0] == ZIP_END)
                break;
        }
    }
    // 如果p指向结束符，或者index>0 都表示我们没找到对应index的entry。返回NULL
    if (p[0] == ZIP_END || index > 0)
        return NULL;
    // 否则验证p指向entry是否合法，然后返回p
    zipAssertValidEntry(zl, zlbytes, p);
    return p;
}

/* Return pointer to next entry in ziplist.
 *
 * zl is the pointer to the ziplist
 * p is the pointer to the current element
 *
 * The element after 'p' is returned, otherwise NULL if we are at the end. */
// 返回p指向的entry的下一个entry的指针。没有则返回NULL。
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p) {
    ((void) zl);
    // 获取ziplist总长度，用于判断下一个entry位置的合法性。
    size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));

    /* "p" could be equal to ZIP_END, caused by ziplistDelete,
     * and we should return NULL. Otherwise, we should return NULL
     * when the *next* element is ZIP_END (there is no next entry). */
    // 如果当前p指向的是结束符，则直接返回NULL。
    if (p[0] == ZIP_END) {
        return NULL;
    }

    // 如果p是tail节点，那么p指向的entry后面就是结束符，此时也返回NULL。
    p += zipRawEntryLength(p);
    if (p[0] == ZIP_END) {
        return NULL;
    }

    // 验证p下一个entry指针的合法性，返回p
    zipAssertValidEntry(zl, zlbytes, p);
    return p;
}

/* Return pointer to previous entry in ziplist. */
// 返回p指向entry的前一个entry的指针。没有则返回NULL
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p) {
    unsigned int prevlensize, prevlen = 0;

    /* Iterating backwards from ZIP_END should return the tail. When "p" is
     * equal to the first element of the list, we're already at the head,
     * and should return NULL. */
    if (p[0] == ZIP_END) {
        // 如果当前p指向结束符，则该ziplist有tail就返回tail指针，没有就返回NULL。
        p = ZIPLIST_ENTRY_TAIL(zl);
        return (p[0] == ZIP_END) ? NULL : p;
    } else if (p == ZIPLIST_ENTRY_HEAD(zl)) {
        // 如果当前p指向head entry，直接返回NULL
        return NULL;
    } else {
        // 解析prevlen，根据该值定位到前一个entry位置，验证位置合法性并返回。
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
        assert(prevlen > 0);
        p-=prevlen;
        size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));
        zipAssertValidEntry(zl, zlbytes, p);
        return p;
    }
}

/* Get entry pointed to by 'p' and store in either '*sstr' or 'sval' depending
 * on the encoding of the entry. '*sstr' is always set to NULL to be able
 * to find out whether the string pointer or the integer value was set.
 * Return 0 if 'p' points to the end of the ziplist, 1 otherwise. */
// 获取p指针指向的entry数据，并根据编码类型将值存储在*sstr或sval中。
// 最终我们根据*sstr的值是否为NULL来判断返回的值为字符串还是数字。如果*sstr为NULL表示是数字，否则是字符串。
// 当p指针不指向任何entry时，函数返回0，否则函数返回1。
unsigned int ziplistGet(unsigned char *p, unsigned char **sstr, unsigned int *slen, long long *sval) {
    zlentry entry;
    if (p == NULL || p[0] == ZIP_END) return 0;
    // 初始时始终设置*sstr为空，如果是字符串类型，则该值会更新为指向ziplist对应entry的数据部分。否则是数字，该值会为NULL返回。
    if (sstr) *sstr = NULL;

    // 解析p指针处的entry
    zipEntry(p, &entry); /* no need for "safe" variant since the input pointer was validated by the function that returned it. */
    if (ZIP_IS_STR(entry.encoding)) {
        if (sstr) {
            // 更新返回字符串数据的指针和长度
            *slen = entry.len;
            *sstr = p+entry.headersize;
        }
    } else {
        if (sval) {
            // 更新返回的数字数据。
            *sval = zipLoadInteger(p+entry.headersize,entry.encoding);
        }
    }
    return 1;
}

/* Insert an entry at "p". */
// 在p位置处插入元素。
unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {
    return __ziplistInsert(zl,p,s,slen);
}

/* Delete a single entry from the ziplist, pointed to by *p.
 * Also update *p in place, to be able to iterate over the
 * ziplist, while deleting entries. */
// 删除*p指向的单个entry，更新*p指向删除元素的下一个entry，用于边迭代边删除处理。
unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p) {
    size_t offset = *p-zl;
    zl = __ziplistDelete(zl,*p,1);

    /* Store pointer to current element in p, because ziplistDelete will
     * do a realloc which might result in a different "zl"-pointer.
     * When the delete direction is back to front, we might delete the last
     * entry and end up with "p" pointing to ZIP_END, so check this. */
    // 更新*p位置指向删除元素的后一个entry，因为删除元素会导致realloc，可能指向ziplist的指针变了。所以需要先保存offset，然后再更新回去。
    // 另外如果删除迭代是从尾到头，我们可能删除的是最后一个entry，然后*p会指向结束符，所以使用时需要检查。
    *p = zl+offset;
    return zl;
}

/* Delete a range of entries from the ziplist. */
// 删除ziplist中index之后的num个entry。方法的调用者可能传-1，表示一直删除直到结束，因为-1表示无符号最大数。
unsigned char *ziplistDeleteRange(unsigned char *zl, int index, unsigned int num) {
    unsigned char *p = ziplistIndex(zl,index);
    return (p == NULL) ? zl : __ziplistDelete(zl,p,num);
}

/* Replaces the entry at p. This is equivalent to a delete and an insert,
 * but avoids some overhead when replacing a value of the same size. */
// 替换指针p处的entry，这相当于删除该entry并插入新数据，但是在替换相同大小的数据时避免了一些开销。
unsigned char *ziplistReplace(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {

    /* get metadata of the current entry */
    // 获取p指向的当前entry的数据
    zlentry entry;
    zipEntry(p, &entry);

    /* compute length of entry to store, excluding prevlen */
    // 计算替换的新entry的长度，encoding（type/len）+ data长度。因为修改的entry的prelen不需要变动，所以这里不计入。
    unsigned int reqlen;
    unsigned char encoding = 0;
    long long value = 123456789; /* initialized to avoid warning. */
    // 计算数据的编码长度
    if (zipTryEncoding(s,slen,&value,&encoding)) {
        // 如果能编码为数字，则数据长度为存储数字所需的字节数，且我们这里会填充对应的encoding信息。
        reqlen = zipIntSize(encoding); /* encoding is set */
    } else {
        // 如果是字符串，则数据长度就是字符串长度。这里并没有进行编码，因而没有encoding信息。
        reqlen = slen; /* encoding == 0 */
    }
    // 获取encoding部分的长度。
    reqlen += zipStoreEntryEncoding(NULL,encoding,slen);

    if (reqlen == entry.lensize + entry.len) {
        /* Simply overwrite the element. */
        // 如果新替换entry所需的 encoding部分+数据部分 总长度与原entry一致。则不需要扩容处理，直接对应位置写入数据就可以了。
        // p先指向prevlen后面位置写入encoding部分。
        p += entry.prevrawlensize;
        p += zipStoreEntryEncoding(p,encoding,slen);
        // 写入数据部分。数字和字符串不同方式写入。
        if (ZIP_IS_STR(encoding)) {
            memcpy(p,s,slen);
        } else {
            zipSaveInteger(p,value,encoding);
        }
    } else {
        /* Fallback. */
        // 如果新写入的entry与原entry长度不一致，则通过先delete在insert进行处理。
        zl = ziplistDelete(zl,&p);
        zl = ziplistInsert(zl,p,s,slen);
    }
    return zl;
}

/* Compare entry pointer to by 'p' with 'sstr' of length 'slen'. */
/* Return 1 if equal. */
// 对比p指向的entry数据，与sstr指向的slen字节数据是否一致，一致则返回1。
unsigned int ziplistCompare(unsigned char *p, unsigned char *sstr, unsigned int slen) {
    zlentry entry;
    unsigned char sencoding;
    long long zval, sval;
    // p指向结束符，显然不一致，返回0。
    if (p[0] == ZIP_END) return 0;

    // 获取p指向的entry信息
    zipEntry(p, &entry); /* no need for "safe" variant since the input pointer was validated by the function that returned it. */
    if (ZIP_IS_STR(entry.encoding)) {
        /* Raw compare */
        // 如果是字符串数据，先比较长度一致，再挨个字符对比。
        if (entry.len == slen) {
            return memcmp(p+entry.headersize,sstr,slen) == 0;
        } else {
            return 0;
        }
    } else {
        /* Try to compare encoded values. Don't compare encoding because
         * different implementations may encoded integers differently. */
        // 如果p指向的entry是数字类型，则把传入的字符串编码为数字，然后与entry中加载的数字进行比对。
        // 数字类型我们不直接对编码字节进行比较，因为对于相同的数字，不同的编码方式得到的结果会不一致，直接对比编码字节不靠谱。
        if (zipTryEncoding(sstr,slen,&sval,&sencoding)) {
          zval = zipLoadInteger(p+entry.headersize,entry.encoding);
          return zval == sval;
        }
    }
    return 0;
}

/* Find pointer to the entry equal to the specified entry. Skip 'skip' entries
 * between every comparison. Returns NULL when the field could not be found. */
// 遍历ziplist，找到与指定数据相等的entry。skip参数表示每两次对比跳过的entry数，当ziplist存储map时，用于map的key查询数据。
// 找到匹配entry，则返回相应指针；否则没有找到对应entry时，返回NULL。
unsigned char *ziplistFind(unsigned char *zl, unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip) {
    // 指示当前遍历的entry是否跳过，为0表示不跳过，其他表示跳过。
    int skipcnt = 0;
    // 因为数据可能是数字或字符串，所以我们会尝试将传入字符串进行编码解析处理。编码方式存入vencoding中，解析后的值存入vll中。
    unsigned char vencoding = 0;
    long long vll = 0;
    // 获取ziplist总字节数，主要用于检测指向entry指针的合法性。
    size_t zlbytes = ziplistBlobLen(zl);

    // 从前往后遍历，直到遇到结束符，或者找到对应的entry为止。
    while (p[0] != ZIP_END) {
        struct zlentry e;
        unsigned char *q;

        // 获取p指向的entry。
        assert(zipEntrySafe(zl, zlbytes, p, &e, 1));
        // q指向entry的数据部分起始位置。
        q = p + e.prevrawlensize + e.lensize;

        if (skipcnt == 0) {
            /* Compare current entry with specified entry */
            // skipcnt为0，表示当前即为需要进行对比的entry。
            if (ZIP_IS_STR(e.encoding)) {
                // 如果是字符串，则先比对字符串长度，长度相等则再挨个比对字符，一致则返回entry指针。
                if (e.len == vlen && memcmp(q, vstr, vlen) == 0) {
                    return p;
                }
            } else {
                /* Find out if the searched field can be encoded. Note that
                 * we do it only the first time, once done vencoding is set
                 * to non-zero and vll is set to the integer value. */
                // 因为遍历到的entry数据是数字，所以我们这里尝试将传入的字节数据编码成数字，再进行对比。
                // vencoding表示是否已处理编码过。0表示未处理，UCHAR_MAX表示不能编码成数字，其他正常的数字编码ZIP_INT*表示成功编码数字。
                // 这里使用懒处理方式，需要时才处理。如果ziplist中entry全是字符串，就不会走到这里。
                if (vencoding == 0) {
                    if (!zipTryEncoding(vstr, vlen, &vll, &vencoding)) {
                        /* If the entry can't be encoded we set it to
                         * UCHAR_MAX so that we don't retry again the next
                         * time. */
                        // 尝试编码成数字失败，设置vencoding为UCHAR_MAX标识。
                        vencoding = UCHAR_MAX;
                    }
                    /* Must be non-zero by now */
                    // 此时vencoding肯定为非0。
                    assert(vencoding);
                }

                /* Compare current entry with specified entry, do it only
                 * if vencoding != UCHAR_MAX because if there is no encoding
                 * possible for the field it can't be a valid integer. */
                // 如果前面成功编码为数字了，这里加载出对应entry的数据，并与该数字进行比较，相等则返回entry指针。
                if (vencoding != UCHAR_MAX) {
                    long long ll = zipLoadInteger(q, e.encoding);
                    if (ll == vll) {
                        return p;
                    }
                }
            }

            /* Reset skip count */
            // 处理对比了当前entry，这里还原skipcnt，后面需要再跳过指定的entry后再进行比对。
            skipcnt = skip;
        } else {
            /* Skip entry */
            // 跳过当前entry。
            skipcnt--;
        }

        /* Move to next entry */
        // 处理完一个entry后，p指向下一个entry进行迭代处理。
        p = q + e.len;
    }

    // 最终如果没找到匹配的entry，返回NULL。
    return NULL;
}

/* Return length of ziplist. */
// 返回ziplist的总entry数。
unsigned int ziplistLen(unsigned char *zl) {
    unsigned int len = 0;
    if (intrev16ifbe(ZIPLIST_LENGTH(zl)) < UINT16_MAX) {
        // 如果长度小于 UINT16_MAX 时，则该值就是长度，直接返回。
        len = intrev16ifbe(ZIPLIST_LENGTH(zl));
    } else {
        // 否则，2字节无法表示总长度，我们需要遍历所有entry来获取总的entry数。
        unsigned char *p = zl+ZIPLIST_HEADER_SIZE;
        size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));
        while (*p != ZIP_END) {
            // 遍历获取entry数。
            p += zipRawEntryLengthSafe(zl, zlbytes, p);
            len++;
        }

        /* Re-store length if small enough */
        // 如果遍历查到的entry数小于了 UINT16_MAX，那么我们更新len值。什么时候会出现这种情况呢？
        if (len < UINT16_MAX) ZIPLIST_LENGTH(zl) = intrev16ifbe(len);
    }
    return len;
}

/* Return ziplist blob size in bytes. */
// 返回ziplist的总字节数
size_t ziplistBlobLen(unsigned char *zl) {
    return intrev32ifbe(ZIPLIST_BYTES(zl));
}

void ziplistRepr(unsigned char *zl) {
    unsigned char *p;
    int index = 0;
    zlentry entry;
    size_t zlbytes = ziplistBlobLen(zl);

    printf(
        "{total bytes %u} "
        "{num entries %u}\n"
        "{tail offset %u}\n",
        intrev32ifbe(ZIPLIST_BYTES(zl)),
        intrev16ifbe(ZIPLIST_LENGTH(zl)),
        intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)));
    p = ZIPLIST_ENTRY_HEAD(zl);
    while(*p != ZIP_END) {
        assert(zipEntrySafe(zl, zlbytes, p, &entry, 1));
        printf(
            "{\n"
                "\taddr 0x%08lx,\n"
                "\tindex %2d,\n"
                "\toffset %5lu,\n"
                "\thdr+entry len: %5u,\n"
                "\thdr len%2u,\n"
                "\tprevrawlen: %5u,\n"
                "\tprevrawlensize: %2u,\n"
                "\tpayload %5u\n",
            (long unsigned)p,
            index,
            (unsigned long) (p-zl),
            entry.headersize+entry.len,
            entry.headersize,
            entry.prevrawlen,
            entry.prevrawlensize,
            entry.len);
        printf("\tbytes: ");
        for (unsigned int i = 0; i < entry.headersize+entry.len; i++) {
            printf("%02x|",p[i]);
        }
        printf("\n");
        p += entry.headersize;
        if (ZIP_IS_STR(entry.encoding)) {
            printf("\t[str]");
            if (entry.len > 40) {
                if (fwrite(p,40,1,stdout) == 0) perror("fwrite");
                printf("...");
            } else {
                if (entry.len &&
                    fwrite(p,entry.len,1,stdout) == 0) perror("fwrite");
            }
        } else {
            printf("\t[int]%lld", (long long) zipLoadInteger(p,entry.encoding));
        }
        printf("\n}\n");
        p += entry.len;
        index++;
    }
    printf("{end}\n\n");
}

/* Validate the integrity of the data structure.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we scan all the entries one by one. */
// 验证ziplist的完整性。当deep为0时，只验证header的完整性；而当deep为1时，遍历所有的entries来验证。
int ziplistValidateIntegrity(unsigned char *zl, size_t size, int deep,
    ziplistValidateEntryCB entry_cb, void *cb_userdata) {
    /* check that we can actually read the header. (and ZIP_END) */
    // ziplist至少要包含header和结束符
    if (size < ZIPLIST_HEADER_SIZE + ZIPLIST_END_SIZE)
        return 0;

    /* check that the encoded size in the header must match the allocated size. */
    // 从header中解析的总字节数要与传入size一致。
    size_t bytes = intrev32ifbe(ZIPLIST_BYTES(zl));
    if (bytes != size)
        return 0;

    /* the last byte must be the terminator. */
    // 最后一个字符必须是结束标识。
    if (zl[size - ZIPLIST_END_SIZE] != ZIP_END)
        return 0;

    /* make sure the tail offset isn't reaching outside the allocation. */
    // 确保header中的tail offset不会超出ziplist数据范围。
    if (intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) > size - ZIPLIST_END_SIZE)
        return 0;

    // 前面检查完了header，如果deep为0，则不需要检查所有entries，直接返回。
    if (!deep)
        return 1;

    // count表示遍历到的总entries数
    unsigned int count = 0;
    // p指向当前遍历的entry，prev指向前一个遍历处理的entry。
    unsigned char *p = ZIPLIST_ENTRY_HEAD(zl);
    unsigned char *prev = NULL;
    // 前一个entry的总长度
    size_t prev_raw_size = 0;
    // 遍历entries，直到遇到结束符为止。
    while(*p != ZIP_END) {
        struct zlentry e;
        /* Decode the entry headers and fail if invalid or reaches outside the allocation */
        // 解析p指向的entry信息。
        if (!zipEntrySafe(zl, size, p, &e, 1))
            return 0;

        /* Make sure the record stating the prev entry size is correct. */
        // 检查entry的prevlen值是否正确。
        if (e.prevrawlen != prev_raw_size)
            return 0;

        /* Optionally let the caller validate the entry too. */
        // 可选的处理函数，由调用方设置来校验处理该entry。目前主要用于ziplist编码方式的hash、zset检查keys是否重复。
        if (entry_cb && !entry_cb(p, cb_userdata))
            return 0;

        /* Move to the next entry */
        // 当前entry检查完了，更新相关数据，准备检查下一个entry
        prev_raw_size = e.headersize + e.len;
        prev = p;
        p += e.headersize + e.len;
        count++;
    }

    /* Make sure the <zltail> entry really do point to the start of the last entry. */
    // 所有entry检查完了，这里检查header中tail offset是否指向最后一个entry。
    if (prev != ZIPLIST_ENTRY_TAIL(zl))
        return 0;

    /* Check that the count in the header is correct */
    // 检查header中总的entry数是否与遍历得到的count一致。
    // 注意当header中该值为UINT16_MAX时，并不表示总entry数一定>=UINT16_MAX，只是说明我们需要遍历来查询entry总数。
    unsigned int header_count = intrev16ifbe(ZIPLIST_LENGTH(zl));
    // ziplist中要么header_count值为UINT16_MAX，要么header_count与遍历得到的count一致。
    if (header_count != UINT16_MAX && count != header_count)
        return 0;

    // 最终全部检查通过，返回1.
    return 1;
}

/* Randomly select a pair of key and value.
 * total_count is a pre-computed length/2 of the ziplist (to avoid calls to ziplistLen)
 * 'key' and 'val' are used to store the result key value pair.
 * 'val' can be NULL if the value is not needed. */
// ziplist中随机选择一对key、value。
// total_count是一个预先计算的ziplist的length/2（即存储的key、value对的个数），避免再调用ziplistLen来获取。
// key、val参数用来接收我们随机取到的数据。如果我们不需要获取value可以将传入val设置为NULL。
void ziplistRandomPair(unsigned char *zl, unsigned long total_count, ziplistEntry *key, ziplistEntry *val) {
    int ret;
    unsigned char *p;

    /* Avoid div by zero on corrupt ziplist */
    // total_count应该>0。
    assert(total_count);

    /* Generate even numbers, because ziplist saved K-V pair */
    // 随机生成一个范围内的偶数，因为ziplist存储的是k-v对。
    int r = (rand() % total_count) * 2;
    // 根据偶数index，找到指向对应entry的指针
    p = ziplistIndex(zl, r);
    // 根据指针获取entry值作为key。
    ret = ziplistGet(p, &key->sval, &key->slen, &key->lval);
    assert(ret != 0);

    // 当传入val为NULL时，不需要获取val，直接返回。
    if (!val)
        return;
    // 获取next entry，来取得前面key对应的value。
    p = ziplistNext(zl, p);
    ret = ziplistGet(p, &val->sval, &val->slen, &val->lval);
    assert(ret != 0);
}

/* int compare for qsort */
// uint比较函数，用于qsort使用。
int uintCompare(const void *a, const void *b) {
    return (*(unsigned int *) a - *(unsigned int *) b);
}

/* Helper method to store a string into from val or lval into dest */
// 帮助函数，用于将值保存到ziplistEntry对象中。
static inline void ziplistSaveValue(unsigned char *val, unsigned int len, long long lval, ziplistEntry *dest) {
    dest->sval = val;
    dest->slen = len;
    dest->lval = lval;
}

/* Randomly select count of key value pairs and store into 'keys' and
 * 'vals' args. The order of the picked entries is random, and the selections
 * are non-unique (repetitions are possible).
 * The 'vals' arg can be NULL in which case we skip these. */
// 随机选择count个key、value对，并将key和value存储到keys和vals数组中（保持映射关系）。
// 注意这个函数选择到的k-v对可能重复。另外返回entries是按照我们生成的随机顺序返回的。
// 当传入参数vals为NULL时，表示我们不需要返回value值。
void ziplistRandomPairs(unsigned char *zl, unsigned int count, ziplistEntry *keys, ziplistEntry *vals) {
    unsigned char *p, *key, *value;
    unsigned int klen = 0, vlen = 0;
    long long klval = 0, vlval = 0;
    int valid = 0;

    /* Notice: the index member must be first due to the use in uintCompare */
    // 注意：index字段必须要在第一个位置，因为我们要使用uintCompare来比较然后排序。
    // 每次随机取数，如果我们都先生成一个index，再根据index拿ziplist中数据，那么可能需要count次遍历查询ziplist。
    // 这里我们是先随机生成index数组，再将index排序由小到大，这样一次遍历ziplist就可以取出所有随机结果。
    typedef struct {
        // index表示我们查找ziplist中entry的索引。我们会将随机生成的pick数组按index从小到大排序。
        // order表示随机生成的pick顺序。我们需要按这个顺序来返回数据。
        unsigned int index;
        unsigned int order;
    } rand_pick;
    // 分配pick数组空间
    rand_pick *picks = zmalloc(sizeof(rand_pick)*count);
    // 获取ziplist中k-v对的数量，该值应该>0
    unsigned int total_size = ziplistLen(zl)/2;

    /* Avoid div by zero on corrupt ziplist */
    assert(total_size);

    /* create a pool of random indexes (some may be duplicate). */
    // 随机生成index来填充数组，注意index必须是偶数，因为偶数的index才是指向key entry的。另外我们这里index是可以重复的。
    for (unsigned int i = 0; i < count; i++) {
        picks[i].index = (rand() % total_size) * 2; /* Generate even indexes */
        /* keep track of the order we picked them */
        // 记录随机生成的pick顺序，后面按这个顺序返回数据。
        picks[i].order = i;
    }

    /* sort by indexes. */
    // 调用qsort来根据index对picks进行排序。
    qsort(picks, count, sizeof(rand_pick), uintCompare);

    /* fetch the elements form the ziplist into a output array respecting the original order. */
    // 遍历一遍ziplist，按picks数组中的index取出k-v对，填充到keys、values数组中对应位置（位置由pick.order决定）。
    // zipindex表示当前遍历到的ziplist的索引，pickindex表示当前处理的picks数组的索引。
    unsigned int zipindex = 0, pickindex = 0;
    // p定位到第一个entry位置。
    p = ziplistIndex(zl, 0);
    // 当ziplist和picks数组任意一个处理完成使，跳出循环。
    while (p && pickindex < count) {
        valid = 0;
        // 前面ziplistGet获取到key和value数据，要填充到 pickindex对应pick对象 的 order属性 指定的 keys、value数组索引位置处。
        // （TODO）这里可以优化只在需要的时候才ziplistGet获取数据，因为会有很多entry我们解析了但没使用。
        // 如果随机选择到了多个相同的entry，前面按index排序会连续存储，这个循环根据picks指示来处理依次填充数据。
        while (pickindex < count && zipindex == picks[pickindex].index) {
            if (!valid) {
                assert(ziplistGet(p, &key, &klen, &klval));
                p = ziplistNext(zl, p);
                assert(ziplistGet(p, &value, &vlen, &vlval));
                valid = 1;
            }

            // 获取到应该放置在keys数组中的位置（order顺序）。
            int storeorder = picks[pickindex].order;
            // 设置key值到keys数组中对应位置
            ziplistSaveValue(key, klen, klval, &keys[storeorder]);
            if (vals)
                // 如果需要返回valus，这里填充value。
                ziplistSaveValue(value, vlen, vlval, &vals[storeorder]);
            // picks处理下一个index。
             pickindex++;
        }
        // zipindex更新为下一个处理的key entry的index。p指向下一个待处理的key entry。
        zipindex += 2;
        p = ziplistNext(zl, p);
    }

    // 最后释放picks数组
    zfree(picks);
}

/* Randomly select count of key value pairs and store into 'keys' and
 * 'vals' args. The selections are unique (no repetitions), and the order of
 * the picked entries is NOT-random.
 * The 'vals' arg can be NULL in which case we skip these.
 * The return value is the number of items picked which can be lower than the
 * requested count if the ziplist doesn't hold enough pairs. */
// 随机选择count个key、value对，并将key和value存储到keys和vals数组中（保持映射关系）。
// 注意这个函数选择到的k-v对都是唯一的，不会重复。另外返回的entries顺序不是随机的。
// 当传入参数vals为NULL时，表示我们不需要返回value值。函数返回值为我们随机选择的k-v对数量，当我们ziplist没有足够数据时，返回值会<count。
unsigned int ziplistRandomPairsUnique(unsigned char *zl, unsigned int count, ziplistEntry *keys, ziplistEntry *vals) {
    unsigned char *p, *key;
    unsigned int klen = 0;
    long long klval = 0;
    unsigned int total_size = ziplistLen(zl)/2;
    unsigned int index = 0;
    // ziplist总entries数不足时，最多只能返回所有。
    if (count > total_size)
        count = total_size;

    /* To only iterate once, every time we try to pick a member, the probability
     * we pick it is the quotient of the count left we want to pick and the
     * count still we haven't visited in the dict, this way, we could make every
     * member be equally picked.*/
    // 只迭代一次，每次我们想选择一个entry时，我们选择它的概率=我们还需要选择的remaining数/ziplist中我们还未访问的entry（k-v对）数量。
    // 这样每次概率相等都是 count/total_size。
    p = ziplistIndex(zl, 0);
    unsigned int picked = 0, remaining = count;
    // 遍历ziplist处理，直到取到足够数据或p为NULL时返回（当遍历到结束符时，ziplistNext会返回NULL；空表ziplistIndex(zl, 0)也会返回NULL）。
    while (picked < count && p) {
        double randomDouble = ((double)rand()) / RAND_MAX;
        double threshold = ((double)remaining) / (total_size - index);
        // randomDouble是随机浮点数，落在<threshold部分的概率显然就是threshold。
        // 所以randomDouble <= threshold 表示 以threshold的概率选择当前entry。
        if (randomDouble <= threshold) {
            // 我们选择了该k-v对，将值取出填充到返回列表中
            assert(ziplistGet(p, &key, &klen, &klval));
            ziplistSaveValue(key, klen, klval, &keys[picked]);
            p = ziplistNext(zl, p);
            assert(p);
            if (vals) {
                assert(ziplistGet(p, &key, &klen, &klval));
                ziplistSaveValue(key, klen, klval, &vals[picked]);
            }
            // 剩余待取数量remaining-1，keys/values填充位置picked+1。
            remaining--;
            picked++;
        } else {
            // 如果当前遍历到的entry按计算的概率没有被选择，则跳过处理下一个entry。
            p = ziplistNext(zl, p);
            assert(p);
        }
        // p指向下一个key entry。另外我们已访问的k-v对数量+1。
        p = ziplistNext(zl, p);
        index++;
    }
    return picked;
}

#ifdef REDIS_TEST
#include <sys/time.h>
#include "adlist.h"
#include "sds.h"

#define debug(f, ...) { if (DEBUG) printf(f, __VA_ARGS__); }

static unsigned char *createList() {
    unsigned char *zl = ziplistNew();
    zl = ziplistPush(zl, (unsigned char*)"foo", 3, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char*)"quux", 4, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char*)"hello", 5, ZIPLIST_HEAD);
    zl = ziplistPush(zl, (unsigned char*)"1024", 4, ZIPLIST_TAIL);
    return zl;
}

static unsigned char *createIntList() {
    unsigned char *zl = ziplistNew();
    char buf[32];

    sprintf(buf, "100");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "128000");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "-100");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_HEAD);
    sprintf(buf, "4294967296");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_HEAD);
    sprintf(buf, "non integer");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "much much longer non integer");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    return zl;
}

static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

static void stress(int pos, int num, int maxsize, int dnum) {
    int i,j,k;
    unsigned char *zl;
    char posstr[2][5] = { "HEAD", "TAIL" };
    long long start;
    for (i = 0; i < maxsize; i+=dnum) {
        zl = ziplistNew();
        for (j = 0; j < i; j++) {
            zl = ziplistPush(zl,(unsigned char*)"quux",4,ZIPLIST_TAIL);
        }

        /* Do num times a push+pop from pos */
        start = usec();
        for (k = 0; k < num; k++) {
            zl = ziplistPush(zl,(unsigned char*)"quux",4,pos);
            zl = ziplistDeleteRange(zl,0,1);
        }
        printf("List size: %8d, bytes: %8d, %dx push+pop (%s): %6lld usec\n",
            i,intrev32ifbe(ZIPLIST_BYTES(zl)),num,posstr[pos],usec()-start);
        zfree(zl);
    }
}

static unsigned char *pop(unsigned char *zl, int where) {
    unsigned char *p, *vstr;
    unsigned int vlen;
    long long vlong;

    p = ziplistIndex(zl,where == ZIPLIST_HEAD ? 0 : -1);
    if (ziplistGet(p,&vstr,&vlen,&vlong)) {
        if (where == ZIPLIST_HEAD)
            printf("Pop head: ");
        else
            printf("Pop tail: ");

        if (vstr) {
            if (vlen && fwrite(vstr,vlen,1,stdout) == 0) perror("fwrite");
        }
        else {
            printf("%lld", vlong);
        }

        printf("\n");
        return ziplistDelete(zl,&p);
    } else {
        printf("ERROR: Could not pop\n");
        exit(1);
    }
}

static int randstring(char *target, unsigned int min, unsigned int max) {
    int p = 0;
    int len = min+rand()%(max-min+1);
    int minval, maxval;
    switch(rand() % 3) {
    case 0:
        minval = 0;
        maxval = 255;
    break;
    case 1:
        minval = 48;
        maxval = 122;
    break;
    case 2:
        minval = 48;
        maxval = 52;
    break;
    default:
        assert(NULL);
    }

    while(p < len)
        target[p++] = minval+rand()%(maxval-minval+1);
    return len;
}

static void verify(unsigned char *zl, zlentry *e) {
    int len = ziplistLen(zl);
    zlentry _e;

    ZIPLIST_ENTRY_ZERO(&_e);

    for (int i = 0; i < len; i++) {
        memset(&e[i], 0, sizeof(zlentry));
        zipEntry(ziplistIndex(zl, i), &e[i]);

        memset(&_e, 0, sizeof(zlentry));
        zipEntry(ziplistIndex(zl, -len+i), &_e);

        assert(memcmp(&e[i], &_e, sizeof(zlentry)) == 0);
    }
}

static unsigned char *insertHelper(unsigned char *zl, char ch, size_t len, unsigned char *pos) {
    assert(len <= ZIP_BIG_PREVLEN);
    unsigned char data[ZIP_BIG_PREVLEN] = {0};
    memset(data, ch, len);
    return ziplistInsert(zl, pos, data, len);
}

static int compareHelper(unsigned char *zl, char ch, size_t len, int index) {
    assert(len <= ZIP_BIG_PREVLEN);
    unsigned char data[ZIP_BIG_PREVLEN] = {0};
    memset(data, ch, len);
    unsigned char *p = ziplistIndex(zl, index);
    assert(p != NULL);
    return ziplistCompare(p, data, len);
}

static size_t strEntryBytesSmall(size_t slen) {
    return slen + zipStorePrevEntryLength(NULL, 0) + zipStoreEntryEncoding(NULL, 0, slen);
}

static size_t strEntryBytesLarge(size_t slen) {
    return slen + zipStorePrevEntryLength(NULL, ZIP_BIG_PREVLEN) + zipStoreEntryEncoding(NULL, 0, slen);
}

/* ./redis-server test ziplist <randomseed> --accurate */
int ziplistTest(int argc, char **argv, int accurate) {
    unsigned char *zl, *p;
    unsigned char *entry;
    unsigned int elen;
    long long value;
    int iteration;

    /* If an argument is given, use it as the random seed. */
    if (argc >= 4)
        srand(atoi(argv[3]));

    zl = createIntList();
    ziplistRepr(zl);

    zfree(zl);

    zl = createList();
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_HEAD);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zfree(zl);

    printf("Get element at index 3:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 3);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index 3\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index 4 (out of range):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 4);
        if (p == NULL) {
            printf("No entry\n");
        } else {
            printf("ERROR: Out of range index should return NULL, returned offset: %ld\n", (long)(p-zl));
            return 1;
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -1 (last element):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index -1\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -4 (first element):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -4);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index -4\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -5 (reverse out of range):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -5);
        if (p == NULL) {
            printf("No entry\n");
        } else {
            printf("ERROR: Out of range index should return NULL, returned offset: %ld\n", (long)(p-zl));
            return 1;
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 0 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 0);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 1 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 2 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 2);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate starting out of range:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 4);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("No entry\n");
        } else {
            printf("ERROR\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate from back to front:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistPrev(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate from back to front, deleting all items:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            zl = ziplistDelete(zl,&p);
            p = ziplistPrev(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Delete inclusive range 0,0:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 0, 1);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete inclusive range 0,1:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 0, 2);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete inclusive range 1,2:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 1, 2);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete with start index out of range:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 5, 1);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete with num overflow:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 1, 5);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete foo while iterating:\n");
    {
        zl = createList();
        p = ziplistIndex(zl,0);
        while (ziplistGet(p,&entry,&elen,&value)) {
            if (entry && strncmp("foo",(char*)entry,elen) == 0) {
                printf("Delete foo\n");
                zl = ziplistDelete(zl,&p);
            } else {
                printf("Entry: ");
                if (entry) {
                    if (elen && fwrite(entry,elen,1,stdout) == 0)
                        perror("fwrite");
                } else {
                    printf("%lld",value);
                }
                p = ziplistNext(zl,p);
                printf("\n");
            }
        }
        printf("\n");
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Replace with same size:\n");
    {
        zl = createList(); /* "hello", "foo", "quux", "1024" */
        unsigned char *orig_zl = zl;
        p = ziplistIndex(zl, 0);
        zl = ziplistReplace(zl, p, (unsigned char*)"zoink", 5);
        p = ziplistIndex(zl, 3);
        zl = ziplistReplace(zl, p, (unsigned char*)"yy", 2);
        p = ziplistIndex(zl, 1);
        zl = ziplistReplace(zl, p, (unsigned char*)"65536", 5);
        p = ziplistIndex(zl, 0);
        assert(!memcmp((char*)p,
                       "\x00\x05zoink"
                       "\x07\xf0\x00\x00\x01" /* 65536 as int24 */
                       "\x05\x04quux" "\x06\x02yy" "\xff",
                       23));
        assert(zl == orig_zl); /* no reallocations have happened */
        zfree(zl);
        printf("SUCCESS\n\n");
    }

    printf("Replace with different size:\n");
    {
        zl = createList(); /* "hello", "foo", "quux", "1024" */
        p = ziplistIndex(zl, 1);
        zl = ziplistReplace(zl, p, (unsigned char*)"squirrel", 8);
        p = ziplistIndex(zl, 0);
        assert(!strncmp((char*)p,
                        "\x00\x05hello" "\x07\x08squirrel" "\x0a\x04quux"
                        "\x06\xc0\x00\x04" "\xff",
                        28));
        zfree(zl);
        printf("SUCCESS\n\n");
    }

    printf("Regression test for >255 byte strings:\n");
    {
        char v1[257] = {0}, v2[257] = {0};
        memset(v1,'x',256);
        memset(v2,'y',256);
        zl = ziplistNew();
        zl = ziplistPush(zl,(unsigned char*)v1,strlen(v1),ZIPLIST_TAIL);
        zl = ziplistPush(zl,(unsigned char*)v2,strlen(v2),ZIPLIST_TAIL);

        /* Pop values again and compare their value. */
        p = ziplistIndex(zl,0);
        assert(ziplistGet(p,&entry,&elen,&value));
        assert(strncmp(v1,(char*)entry,elen) == 0);
        p = ziplistIndex(zl,1);
        assert(ziplistGet(p,&entry,&elen,&value));
        assert(strncmp(v2,(char*)entry,elen) == 0);
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Regression test deleting next to last entries:\n");
    {
        char v[3][257] = {{0}};
        zlentry e[3] = {{.prevrawlensize = 0, .prevrawlen = 0, .lensize = 0,
                         .len = 0, .headersize = 0, .encoding = 0, .p = NULL}};
        size_t i;

        for (i = 0; i < (sizeof(v)/sizeof(v[0])); i++) {
            memset(v[i], 'a' + i, sizeof(v[0]));
        }

        v[0][256] = '\0';
        v[1][  1] = '\0';
        v[2][256] = '\0';

        zl = ziplistNew();
        for (i = 0; i < (sizeof(v)/sizeof(v[0])); i++) {
            zl = ziplistPush(zl, (unsigned char *) v[i], strlen(v[i]), ZIPLIST_TAIL);
        }

        verify(zl, e);

        assert(e[0].prevrawlensize == 1);
        assert(e[1].prevrawlensize == 5);
        assert(e[2].prevrawlensize == 1);

        /* Deleting entry 1 will increase `prevrawlensize` for entry 2 */
        unsigned char *p = e[1].p;
        zl = ziplistDelete(zl, &p);

        verify(zl, e);

        assert(e[0].prevrawlensize == 1);
        assert(e[1].prevrawlensize == 5);

        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Create long list and check indices:\n");
    {
        unsigned long long start = usec();
        zl = ziplistNew();
        char buf[32];
        int i,len;
        for (i = 0; i < 1000; i++) {
            len = sprintf(buf,"%d",i);
            zl = ziplistPush(zl,(unsigned char*)buf,len,ZIPLIST_TAIL);
        }
        for (i = 0; i < 1000; i++) {
            p = ziplistIndex(zl,i);
            assert(ziplistGet(p,NULL,NULL,&value));
            assert(i == value);

            p = ziplistIndex(zl,-i-1);
            assert(ziplistGet(p,NULL,NULL,&value));
            assert(999-i == value);
        }
        printf("SUCCESS. usec=%lld\n\n", usec()-start);
        zfree(zl);
    }

    printf("Compare strings with ziplist entries:\n");
    {
        zl = createList();
        p = ziplistIndex(zl,0);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl,3);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Merge test:\n");
    {
        /* create list gives us: [hello, foo, quux, 1024] */
        zl = createList();
        unsigned char *zl2 = createList();

        unsigned char *zl3 = ziplistNew();
        unsigned char *zl4 = ziplistNew();

        if (ziplistMerge(&zl4, &zl4)) {
            printf("ERROR: Allowed merging of one ziplist into itself.\n");
            return 1;
        }

        /* Merge two empty ziplists, get empty result back. */
        zl4 = ziplistMerge(&zl3, &zl4);
        ziplistRepr(zl4);
        if (ziplistLen(zl4)) {
            printf("ERROR: Merging two empty ziplists created entries.\n");
            return 1;
        }
        zfree(zl4);

        zl2 = ziplistMerge(&zl, &zl2);
        /* merge gives us: [hello, foo, quux, 1024, hello, foo, quux, 1024] */
        ziplistRepr(zl2);

        if (ziplistLen(zl2) != 8) {
            printf("ERROR: Merged length not 8, but: %u\n", ziplistLen(zl2));
            return 1;
        }

        p = ziplistIndex(zl2,0);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,3);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,4);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,7);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Stress with random payloads of different encoding:\n");
    {
        unsigned long long start = usec();
        int i,j,len,where;
        unsigned char *p;
        char buf[1024];
        int buflen;
        list *ref;
        listNode *refnode;

        /* Hold temp vars from ziplist */
        unsigned char *sstr;
        unsigned int slen;
        long long sval;

        iteration = accurate ? 20000 : 20;
        for (i = 0; i < iteration; i++) {
            zl = ziplistNew();
            ref = listCreate();
            listSetFreeMethod(ref,(void (*)(void*))sdsfree);
            len = rand() % 256;

            /* Create lists */
            for (j = 0; j < len; j++) {
                where = (rand() & 1) ? ZIPLIST_HEAD : ZIPLIST_TAIL;
                if (rand() % 2) {
                    buflen = randstring(buf,1,sizeof(buf)-1);
                } else {
                    switch(rand() % 3) {
                    case 0:
                        buflen = sprintf(buf,"%lld",(0LL + rand()) >> 20);
                        break;
                    case 1:
                        buflen = sprintf(buf,"%lld",(0LL + rand()));
                        break;
                    case 2:
                        buflen = sprintf(buf,"%lld",(0LL + rand()) << 20);
                        break;
                    default:
                        assert(NULL);
                    }
                }

                /* Add to ziplist */
                zl = ziplistPush(zl, (unsigned char*)buf, buflen, where);

                /* Add to reference list */
                if (where == ZIPLIST_HEAD) {
                    listAddNodeHead(ref,sdsnewlen(buf, buflen));
                } else if (where == ZIPLIST_TAIL) {
                    listAddNodeTail(ref,sdsnewlen(buf, buflen));
                } else {
                    assert(NULL);
                }
            }

            assert(listLength(ref) == ziplistLen(zl));
            for (j = 0; j < len; j++) {
                /* Naive way to get elements, but similar to the stresser
                 * executed from the Tcl test suite. */
                p = ziplistIndex(zl,j);
                refnode = listIndex(ref,j);

                assert(ziplistGet(p,&sstr,&slen,&sval));
                if (sstr == NULL) {
                    buflen = sprintf(buf,"%lld",sval);
                } else {
                    buflen = slen;
                    memcpy(buf,sstr,buflen);
                    buf[buflen] = '\0';
                }
                assert(memcmp(buf,listNodeValue(refnode),buflen) == 0);
            }
            zfree(zl);
            listRelease(ref);
        }
        printf("Done. usec=%lld\n\n", usec()-start);
    }

    printf("Stress with variable ziplist size:\n");
    {
        unsigned long long start = usec();
        int maxsize = accurate ? 16384 : 16;
        stress(ZIPLIST_HEAD,100000,maxsize,256);
        stress(ZIPLIST_TAIL,100000,maxsize,256);
        printf("Done. usec=%lld\n\n", usec()-start);
    }

    /* Benchmarks */
    {
        zl = ziplistNew();
        iteration = accurate ? 100000 : 100;
        for (int i=0; i<iteration; i++) {
            char buf[4096] = "asdf";
            zl = ziplistPush(zl, (unsigned char*)buf, 4, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)buf, 40, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)buf, 400, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)buf, 4000, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"1", 1, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"10", 2, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"100", 3, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"1000", 4, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"10000", 5, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"100000", 6, ZIPLIST_TAIL);
        }

        printf("Benchmark ziplistFind:\n");
        {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                unsigned char *fptr = ziplistIndex(zl, ZIPLIST_HEAD);
                fptr = ziplistFind(zl, fptr, (unsigned char*)"nothing", 7, 1);
            }
            printf("%lld\n", usec()-start);
        }

        printf("Benchmark ziplistIndex:\n");
        {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                ziplistIndex(zl, 99999);
            }
            printf("%lld\n", usec()-start);
        }

        printf("Benchmark ziplistValidateIntegrity:\n");
        {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                ziplistValidateIntegrity(zl, ziplistBlobLen(zl), 1, NULL, NULL);
            }
            printf("%lld\n", usec()-start);
        }

        zfree(zl);
    }

    printf("Stress __ziplistCascadeUpdate:\n");
    {
        char data[ZIP_BIG_PREVLEN];
        zl = ziplistNew();
        iteration = accurate ? 100000 : 100;
        for (int i = 0; i < iteration; i++) {
            zl = ziplistPush(zl, (unsigned char*)data, ZIP_BIG_PREVLEN-4, ZIPLIST_TAIL);
        }
        unsigned long long start = usec();
        zl = ziplistPush(zl, (unsigned char*)data, ZIP_BIG_PREVLEN-3, ZIPLIST_HEAD);
        printf("Done. usec=%lld\n\n", usec()-start);
        zfree(zl);
    }

    printf("Edge cases of __ziplistCascadeUpdate:\n");
    {
        /* Inserting a entry with data length greater than ZIP_BIG_PREVLEN-4 
         * will leads to cascade update. */
        size_t s1 = ZIP_BIG_PREVLEN-4, s2 = ZIP_BIG_PREVLEN-3;
        zl = ziplistNew();

        zlentry e[4] = {{.prevrawlensize = 0, .prevrawlen = 0, .lensize = 0,
                         .len = 0, .headersize = 0, .encoding = 0, .p = NULL}};

        zl = insertHelper(zl, 'a', s1, ZIPLIST_ENTRY_HEAD(zl));
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'a', s1, 0));
        ziplistRepr(zl);

        /* No expand. */
        zl = insertHelper(zl, 'b', s1, ZIPLIST_ENTRY_HEAD(zl));
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'b', s1, 0));

        assert(e[1].prevrawlensize == 1 && e[1].prevrawlen == strEntryBytesSmall(s1));
        assert(compareHelper(zl, 'a', s1, 1));

        ziplistRepr(zl);

        /* Expand(tail included). */
        zl = insertHelper(zl, 'c', s2, ZIPLIST_ENTRY_HEAD(zl));
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'c', s2, 0));

        assert(e[1].prevrawlensize == 5 && e[1].prevrawlen == strEntryBytesSmall(s2));
        assert(compareHelper(zl, 'b', s1, 1));

        assert(e[2].prevrawlensize == 5 && e[2].prevrawlen == strEntryBytesLarge(s1));
        assert(compareHelper(zl, 'a', s1, 2));

        ziplistRepr(zl);

        /* Expand(only previous head entry). */
        zl = insertHelper(zl, 'd', s2, ZIPLIST_ENTRY_HEAD(zl));
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'd', s2, 0));

        assert(e[1].prevrawlensize == 5 && e[1].prevrawlen == strEntryBytesSmall(s2));
        assert(compareHelper(zl, 'c', s2, 1));

        assert(e[2].prevrawlensize == 5 && e[2].prevrawlen == strEntryBytesLarge(s2));
        assert(compareHelper(zl, 'b', s1, 2));

        assert(e[3].prevrawlensize == 5 && e[3].prevrawlen == strEntryBytesLarge(s1));
        assert(compareHelper(zl, 'a', s1, 3));

        ziplistRepr(zl);

        /* Delete from mid. */
        unsigned char *p = ziplistIndex(zl, 2);
        zl = ziplistDelete(zl, &p);
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'd', s2, 0));

        assert(e[1].prevrawlensize == 5 && e[1].prevrawlen == strEntryBytesSmall(s2));
        assert(compareHelper(zl, 'c', s2, 1));

        assert(e[2].prevrawlensize == 5 && e[2].prevrawlen == strEntryBytesLarge(s2));
        assert(compareHelper(zl, 'a', s1, 2));

        ziplistRepr(zl);

        zfree(zl);
    }

    return 0;
}
#endif
