/* quicklist.h - A generic doubly linked quicklist implementation
 *
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this quicklist of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this quicklist of conditions and the following disclaimer in the
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

#include <stdint.h> // for UINTPTR_MAX

#ifndef __QUICKLIST_H__
#define __QUICKLIST_H__

/* Node, quicklist, and Iterator are the only data structures used currently. */

/* quicklistNode is a 32 byte struct describing a ziplist for a quicklist.
 * We use bit fields keep the quicklistNode at 32 bytes.
 * count: 16 bits, max 65536 (max zl bytes is 65k, so max count actually < 32k).
 * encoding: 2 bits, RAW=1, LZF=2.
 * container: 2 bits, NONE=1, ZIPLIST=2.
 * recompress: 1 bit, bool, true if node is temporary decompressed for usage.
 * attempted_compress: 1 bit, boolean, used for verifying during testing.
 * extra: 10 bits, free for future use; pads out the remainder of 32 bits */
// quicklistNode是一个32字节的结构，表示quicklist的节点信息（ziplist信息）。
typedef struct quicklistNode {
    // 前驱和后驱指针
    struct quicklistNode *prev;
    struct quicklistNode *next;
    // 指向当前节点对应的ziplist，如果节点被压缩则指向对应的quicklistLZF结构
    unsigned char *zl;
    // ziplist的字节数大小
    unsigned int sz;             /* ziplist size in bytes */
    // ziplist中包含的item数，16bits最大65536。（最大ziplist长度是64k，所以最大count是要<32k的，16bits足够了）
    unsigned int count : 16;     /* count of items in ziplist */
    // 表示是否使用LZF压缩算法压缩ziplist。
    unsigned int encoding : 2;   /* RAW==1 or LZF==2 */
    // 表示是否采用ziplist来保存数据。
    unsigned int container : 2;  /* NONE==1 or ZIPLIST==2 */
    // true表示我们对该节点进行临时解压缩来使用，该节点需要重新压缩。
    unsigned int recompress : 1; /* was this node previous compressed? */
    // 数据太小不能进行压缩。test使用。
    unsigned int attempted_compress : 1; /* node can't compress; too small */
    // 额外的10bits，未来扩展使用
    unsigned int extra : 10; /* more bits to steal for future usage */
} quicklistNode;

/* quicklistLZF is a 4+N byte struct holding 'sz' followed by 'compressed'.
 * 'sz' is byte length of 'compressed' field.
 * 'compressed' is LZF data with total (compressed) length 'sz'
 * NOTE: uncompressed length is stored in quicklistNode->sz.
 * When quicklistNode->zl is compressed, node->zl points to a quicklistLZF */
// quicklistLZF结构占用4+N字节。
// sz：标识compressed字段的字节长度。
// compressed：压缩后的LZF数据，长度为sz。
// 注意：压缩前数据的总长度存储在quicklistNode->sz中。当节点进行压缩时，quicklistNode->zl指向压缩结构quicklistLZF。
typedef struct quicklistLZF {
    unsigned int sz; /* LZF size in bytes*/
    char compressed[];
} quicklistLZF;

/* Bookmarks are padded with realloc at the end of of the quicklist struct.
 * They should only be used for very big lists if thousands of nodes were the
 * excess memory usage is negligible, and there's a real need to iterate on them
 * in portions.
 * When not used, they don't add any memory overhead, but when used and then
 * deleted, some overhead remains (to avoid resonance).
 * The number of bookmarks used should be kept to minimum since it also adds
 * overhead on node deletion (searching for a bookmark to update). */
// bookmarks会附加在quicklist结构的尾部，通过realloc quicklist来构建。
// bookmarks只应该在list节点数量非常多时（这样附加的这点内存几乎可以忽略），并且需要按部分分块来进行迭代时才使用。
// 当我们不使用这个功能时，将不会增加任何内存开销；但是一旦使用，在增加或删除时，都是会有一定的存储开销的，可能会realloc。
// 注意使用的bookmarks数应该尽量小，因为当我们在删除节点的时候，同时需要更新这里面保存的节点，线性遍历，会存在一定的开销。
typedef struct quicklistBookmark {
    quicklistNode *node;
    char *name;
} quicklistBookmark;

#if UINTPTR_MAX == 0xffffffff
/* 32-bit */
#   define QL_FILL_BITS 14
#   define QL_COMP_BITS 14
#   define QL_BM_BITS 4
#elif UINTPTR_MAX == 0xffffffffffffffff
/* 64-bit */
#   define QL_FILL_BITS 16
#   define QL_COMP_BITS 16
#   define QL_BM_BITS 4 /* we can encode more, but we rather limit the user
                           since they cause performance degradation. */
#else
#   error unknown arch bits count
#endif

/* quicklist is a 40 byte struct (on 64-bit systems) describing a quicklist.
 * 'count' is the number of total entries.
 * 'len' is the number of quicklist nodes.
 * 'compress' is: 0 if compression disabled, otherwise it's the number
 *                of quicklistNodes to leave uncompressed at ends of quicklist.
 * 'fill' is the user-requested (or default) fill factor.
 * 'bookmakrs are an optional feature that is used by realloc this struct,
 *      so that they don't consume memory when not used. */
// quicklist是一个40字节的结构（64位系统中）
// count：表示总的quicklistEntry数。
// len：表示list总的节点数。注意一个节点可能会有多个entry，所以count>=len
// compress：0表示不压缩，否则它表示在list两端不压缩的节点数（内部节点压缩，两端不压缩）。
// fill：用户设置（或默认）针对list节点的填充因子。
//  - 值为负数（-5～-1）时，对应配置项list-max-ziplist-size，表示ziplist最大不超过多少。
//  - 值为正数时，表示总的entries数（count）不能超过该值。
// bookmarks：可选功能，需要使用时会realloc这个结构，所以在不使用时不会额外占用内存。
typedef struct quicklist {
    quicklistNode *head;
    quicklistNode *tail;
    unsigned long count;        /* total count of all entries in all ziplists */
    unsigned long len;          /* number of quicklistNodes */
    int fill : QL_FILL_BITS;              /* fill factor for individual nodes */
    unsigned int compress : QL_COMP_BITS; /* depth of end nodes not to compress;0=off */
    unsigned int bookmark_count: QL_BM_BITS;
    quicklistBookmark bookmarks[];
} quicklist;

// quicklist迭代器。
typedef struct quicklistIter {
    // 迭代器所属的quicklist
    const quicklist *quicklist;
    // 当前迭代到的quicklist节点
    quicklistNode *current;
    // 当前迭代处理的节点中的ziplist的entry位置。
    unsigned char *zi;
    // 当前ziplist中的offset偏移。
    long offset; /* offset in current ziplist */
    // 迭代方向
    int direction;
} quicklistIter;

// quicklist中存储的元素entry结构。
typedef struct quicklistEntry {
    // 指向所属的list
    const quicklist *quicklist;
    // 指向所属的node
    quicklistNode *node;
    // 指向当前ziplist enrty位置
    unsigned char *zi;
    // 指向当前ziplist enrty中的字符串value成员位置
    unsigned char *value;
    // 表示当前ziplist enrty的整数value成员
    long long longval;
    // 保存当前ziplist enrty的字节数大小
    unsigned int sz;
    // 当前entry在每个节点自己ziplist中的偏移
    int offset;
} quicklistEntry;

#define QUICKLIST_HEAD 0
#define QUICKLIST_TAIL -1

/* quicklist node encodings */
// quicklist节点的编码方式，RAW表示不压缩，LZF表示压缩。
#define QUICKLIST_NODE_ENCODING_RAW 1
#define QUICKLIST_NODE_ENCODING_LZF 2

/* quicklist compression disable */
#define QUICKLIST_NOCOMPRESS 0

/* quicklist container formats */
// 设置quicklist节点使用什么存储数据。一般使用ZIPLIST。
#define QUICKLIST_NODE_CONTAINER_NONE 1
#define QUICKLIST_NODE_CONTAINER_ZIPLIST 2

// 返回true则表示节点是LZF压缩的。
#define quicklistNodeIsCompressed(node)                                        \
    ((node)->encoding == QUICKLIST_NODE_ENCODING_LZF)

/* Prototypes */
quicklist *quicklistCreate(void);
quicklist *quicklistNew(int fill, int compress);
void quicklistSetCompressDepth(quicklist *quicklist, int depth);
void quicklistSetFill(quicklist *quicklist, int fill);
void quicklistSetOptions(quicklist *quicklist, int fill, int depth);
void quicklistRelease(quicklist *quicklist);
int quicklistPushHead(quicklist *quicklist, void *value, const size_t sz);
int quicklistPushTail(quicklist *quicklist, void *value, const size_t sz);
void quicklistPush(quicklist *quicklist, void *value, const size_t sz,
                   int where);
void quicklistAppendZiplist(quicklist *quicklist, unsigned char *zl);
quicklist *quicklistAppendValuesFromZiplist(quicklist *quicklist,
                                            unsigned char *zl);
quicklist *quicklistCreateFromZiplist(int fill, int compress,
                                      unsigned char *zl);
void quicklistInsertAfter(quicklist *quicklist, quicklistEntry *node,
                          void *value, const size_t sz);
void quicklistInsertBefore(quicklist *quicklist, quicklistEntry *node,
                           void *value, const size_t sz);
void quicklistDelEntry(quicklistIter *iter, quicklistEntry *entry);
int quicklistReplaceAtIndex(quicklist *quicklist, long index, void *data,
                            int sz);
int quicklistDelRange(quicklist *quicklist, const long start, const long stop);
quicklistIter *quicklistGetIterator(const quicklist *quicklist, int direction);
quicklistIter *quicklistGetIteratorAtIdx(const quicklist *quicklist,
                                         int direction, const long long idx);
int quicklistNext(quicklistIter *iter, quicklistEntry *node);
void quicklistReleaseIterator(quicklistIter *iter);
quicklist *quicklistDup(quicklist *orig);
int quicklistIndex(const quicklist *quicklist, const long long index,
                   quicklistEntry *entry);
void quicklistRewind(quicklist *quicklist, quicklistIter *li);
void quicklistRewindTail(quicklist *quicklist, quicklistIter *li);
void quicklistRotate(quicklist *quicklist);
int quicklistPopCustom(quicklist *quicklist, int where, unsigned char **data,
                       unsigned int *sz, long long *sval,
                       void *(*saver)(unsigned char *data, unsigned int sz));
int quicklistPop(quicklist *quicklist, int where, unsigned char **data,
                 unsigned int *sz, long long *slong);
unsigned long quicklistCount(const quicklist *ql);
int quicklistCompare(unsigned char *p1, unsigned char *p2, int p2_len);
size_t quicklistGetLzf(const quicklistNode *node, void **data);

/* bookmarks */
int quicklistBookmarkCreate(quicklist **ql_ref, const char *name, quicklistNode *node);
int quicklistBookmarkDelete(quicklist *ql, const char *name);
quicklistNode *quicklistBookmarkFind(quicklist *ql, const char *name);
void quicklistBookmarksClear(quicklist *ql);

#ifdef REDIS_TEST
int quicklistTest(int argc, char *argv[], int accurate);
#endif

/* Directions for iterators */
#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif /* __QUICKLIST_H__ */
