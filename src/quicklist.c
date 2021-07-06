/* quicklist.c - A doubly linked list of ziplists
 *
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must start the above copyright notice,
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

#include <string.h> /* for memcpy */
#include "quicklist.h"
#include "zmalloc.h"
#include "config.h"
#include "ziplist.h"
#include "util.h" /* for ll2string */
#include "lzf.h"
#include "redisassert.h"

#if defined(REDIS_TEST) || defined(REDIS_TEST_VERBOSE)
#include <stdio.h> /* for printf (debug printing), snprintf (genstr) */
#endif

#ifndef REDIS_STATIC
#define REDIS_STATIC static
#endif

/* Optimization levels for size-based filling */
// ziplist最大长度限制，分5个级别。
static const size_t optimization_level[] = {4096, 8192, 16384, 32768, 65536};

/* Maximum size in bytes of any multi-element ziplist.
 * Larger values will live in their own isolated ziplists. */
// 当我们设置的fill为正值时，表示限制的是总的entries个数。
// 但为了安全，这里还是限制了这种ziplist表的最大字节数大小为8k。如果长度超出了这个限制，将重新创建一个单独的ziplist来存新数据。
#define SIZE_SAFETY_LIMIT 8192

/* Minimum ziplist size in bytes for attempting compression. */
// 最小达到这个size的字节数时，我们才尝试对ziplist进行压缩。
#define MIN_COMPRESS_BYTES 48

/* Minimum size reduction in bytes to store compressed quicklistNode data.
 * This also prevents us from storing compression if the compression
 * resulted in a larger size than the original data. */
// 压缩后最少需要减少8字节数量，我们才选择进行压缩。因为有的压缩后数据可能比原始数据还大，所以我们需要阻止这样的压缩存储。
#define MIN_COMPRESS_IMPROVE 8

/* If not verbose testing, remove all debug printing. */
#ifndef REDIS_TEST_VERBOSE
#define D(...)
#else
#define D(...)                                                                 \
    do {                                                                       \
        printf("%s:%s:%d:\t", __FILE__, __func__, __LINE__);                   \
        printf(__VA_ARGS__);                                                   \
        printf("\n");                                                          \
    } while (0)
#endif

/* Bookmarks forward declarations */
#define QL_MAX_BM ((1 << QL_BM_BITS)-1)
quicklistBookmark *_quicklistBookmarkFindByName(quicklist *ql, const char *name);
quicklistBookmark *_quicklistBookmarkFindByNode(quicklist *ql, quicklistNode *node);
void _quicklistBookmarkDelete(quicklist *ql, quicklistBookmark *bm);

/* Simple way to give quicklistEntry structs default values with one call. */
// 宏定义，使用默认值初始化给定的quicklistEntry。
#define initEntry(e)                                                           \
    do {                                                                       \
        (e)->zi = (e)->value = NULL;                                           \
        (e)->longval = -123456789;                                             \
        (e)->quicklist = NULL;                                                 \
        (e)->node = NULL;                                                      \
        (e)->offset = 123456789;                                               \
        (e)->sz = 0;                                                           \
    } while (0)

/* Create a new quicklist.
 * Free with quicklistRelease(). */
// 创建新的quicklist，使用quicklistRelease()来释放。
quicklist *quicklistCreate(void) {
    struct quicklist *quicklist;

    quicklist = zmalloc(sizeof(*quicklist));
    quicklist->head = quicklist->tail = NULL;
    quicklist->len = 0;
    quicklist->count = 0;
    quicklist->compress = 0;
    quicklist->fill = -2;
    quicklist->bookmark_count = 0;
    return quicklist;
}

#define COMPRESS_MAX ((1 << QL_COMP_BITS)-1)
// 设置quicklist压缩深度（从两端数，往内部多少个节点开始压缩），最大不超过COMPRESS_MAX，最小为0表示所有节点都不压缩。
void quicklistSetCompressDepth(quicklist *quicklist, int compress) {
    if (compress > COMPRESS_MAX) {
        compress = COMPRESS_MAX;
    } else if (compress < 0) {
        compress = 0;
    }
    quicklist->compress = compress;
}

#define FILL_MAX ((1 << (QL_FILL_BITS-1))-1)
// 设置我们每个ziplist节点最大长度是多少。fill为正数表示自定义，需要不超过FILL_MAX。
// fill为负数，-5～-1对应optimization_level中5各级别。-1: 4k -2: 8k -3: 16k -4: 32k -5: 64k
void quicklistSetFill(quicklist *quicklist, int fill) {
    if (fill > FILL_MAX) {
        fill = FILL_MAX;
    } else if (fill < -5) {
        fill = -5;
    }
    quicklist->fill = fill;
}

// quicklist设置options，主要是设置单个节点ziplist最大长度 和 quicklist的压缩深度。
void quicklistSetOptions(quicklist *quicklist, int fill, int depth) {
    quicklistSetFill(quicklist, fill);
    quicklistSetCompressDepth(quicklist, depth);
}

/* Create a new quicklist with some default parameters. */
// 使用默认值创建新的quicklist。可传入fill和compress来设置 ziplist最大长度 和 quicklist的压缩深度。
quicklist *quicklistNew(int fill, int compress) {
    quicklist *quicklist = quicklistCreate();
    quicklistSetOptions(quicklist, fill, compress);
    return quicklist;
}

// 创建quicklist节点。默认使用ziplist存储数据且不压缩。
REDIS_STATIC quicklistNode *quicklistCreateNode(void) {
    quicklistNode *node;
    node = zmalloc(sizeof(*node));
    node->zl = NULL;
    node->count = 0;
    node->sz = 0;
    node->next = node->prev = NULL;
    node->encoding = QUICKLIST_NODE_ENCODING_RAW;
    node->container = QUICKLIST_NODE_CONTAINER_ZIPLIST;
    node->recompress = 0;
    return node;
}

/* Return cached quicklist count */
// 返回quicklist中总的entries元素数量。
unsigned long quicklistCount(const quicklist *ql) { return ql->count; }

/* Free entire quicklist. */
// 释放整个quicklist
void quicklistRelease(quicklist *quicklist) {
    unsigned long len;
    quicklistNode *current, *next;

    current = quicklist->head;
    len = quicklist->len;
    // 遍历quicklist的节点。
    while (len--) {
        // 迭代中释放当前节点，所以先保存next用于下次迭代
        next = current->next;

        // 释放节点的ziplist。
        zfree(current->zl);
        quicklist->count -= current->count;

        // 释放该节点
        zfree(current);

        // quicklist总节点数-1
        quicklist->len--;
        current = next;
    }
    // 释放quicklist
    quicklistBookmarksClear(quicklist);
    zfree(quicklist);
}

/* Compress the ziplist in 'node' and update encoding details.
 * Returns 1 if ziplist compressed successfully.
 * Returns 0 if compression failed or if ziplist too small to compress. */
// 压缩该node的ziplist并更新encoding信息。如果节点成功压缩，返回1；否则压缩失败或ziplist太小或压缩后减少字节数太少，我们都返回0
REDIS_STATIC int __quicklistCompressNode(quicklistNode *node) {
#ifdef REDIS_TEST
    node->attempted_compress = 1;
#endif

    /* Don't bother compressing small values */
    // 小于48字节，不压缩
    if (node->sz < MIN_COMPRESS_BYTES)
        return 0;

    quicklistLZF *lzf = zmalloc(sizeof(*lzf) + node->sz);

    /* Cancel if compression fails or doesn't compress small enough */
    // 压缩数据放到压缩结构中，如果压缩失败 或 压缩后数据减少的字节小于8，则释放压缩结构，不压缩。
    if (((lzf->sz = lzf_compress(node->zl, node->sz, lzf->compressed,
                                 node->sz)) == 0) ||
        lzf->sz + MIN_COMPRESS_IMPROVE >= node->sz) {
        /* lzf_compress aborts/rejects compression if value not compressable. */
        zfree(lzf);
        return 0;
    }
    // 成功压，则lzf->sz < node->sz，需要缩容移除没有用的空间。
    lzf = zrealloc(lzf, sizeof(*lzf) + lzf->sz);
    // 释放原zl结构，更新为新的lzf。更新encoding和recompress信息。
    zfree(node->zl);
    node->zl = (unsigned char *)lzf;
    node->encoding = QUICKLIST_NODE_ENCODING_LZF;
    node->recompress = 0;
    return 1;
}

/* Compress only uncompressed nodes. */
// 压缩node，只对没有进行压缩过的节点（encoding为raw）的ziplist进行压缩。
#define quicklistCompressNode(_node)                                           \
    do {                                                                       \
        if ((_node) && (_node)->encoding == QUICKLIST_NODE_ENCODING_RAW) {     \
            __quicklistCompressNode((_node));                                  \
        }                                                                      \
    } while (0)

/* Uncompress the ziplist in 'node' and update encoding details.
 * Returns 1 on successful decode, 0 on failure to decode. */
// 解压缩node的ziplist，并更新encoding信息。成功返回1，失败返回0。
REDIS_STATIC int __quicklistDecompressNode(quicklistNode *node) {
#ifdef REDIS_TEST
    node->attempted_compress = 0;
#endif

    void *decompressed = zmalloc(node->sz);
    quicklistLZF *lzf = (quicklistLZF *)node->zl;
    // 解压缩数据放到decompressed中
    if (lzf_decompress(lzf->compressed, lzf->sz, decompressed, node->sz) == 0) {
        /* Someone requested decompress, but we can't decompress.  Not good. */
        // 解压缩失败了。
        zfree(decompressed);
        return 0;
    }
    // 释放原压缩结构lzf
    zfree(lzf);
    // 更新节点zl为解压缩后的数据，更新encoding为raw。
    node->zl = decompressed;
    node->encoding = QUICKLIST_NODE_ENCODING_RAW;
    return 1;
}

/* Decompress only compressed nodes. */
// 解压缩一个节点，只对已压缩的节点（encoding为lzf）处理解压缩。
#define quicklistDecompressNode(_node)                                         \
    do {                                                                       \
        if ((_node) && (_node)->encoding == QUICKLIST_NODE_ENCODING_LZF) {     \
            __quicklistDecompressNode((_node));                                \
        }                                                                      \
    } while (0)

/* Force node to not be immediately re-compresable */
// 解压缩节点来使用，会设置节点recompress标识为1，强制节点不可立即进行重新压缩。
#define quicklistDecompressNodeForUse(_node)                                   \
    do {                                                                       \
        if ((_node) && (_node)->encoding == QUICKLIST_NODE_ENCODING_LZF) {     \
            __quicklistDecompressNode((_node));                                \
            (_node)->recompress = 1;                                           \
        }                                                                      \
    } while (0)

/* Extract the raw LZF data from this quicklistNode.
 * Pointer to LZF data is assigned to '*data'.
 * Return value is the length of compressed LZF data. */
// 提取节点的lzf数据。压缩数据当道*data中，返回值为压缩数据的大小。
size_t quicklistGetLzf(const quicklistNode *node, void **data) {
    quicklistLZF *lzf = (quicklistLZF *)node->zl;
    *data = lzf->compressed;
    return lzf->sz;
}

// 判断list是否设置压缩深度进行内部节点压缩。
#define quicklistAllowsCompression(_ql) ((_ql)->compress != 0)

/* Force 'quicklist' to meet compression guidelines set by compress depth.
 * The only way to guarantee interior nodes get compressed is to iterate
 * to our "interior" compress depth then compress the next node we find.
 * If compress depth is larger than the entire list, we return immediately. */
// 强制list按照设置的压缩深度进行压缩。内部节点保证被压缩的唯一方法就是迭代到内部的压缩深度，然后再深入的节点就需要被压缩了。
// 如果压缩深度比整个链表都大，则当前所有节点都不压缩，我们直接返回。
// node传入NULL，表示我们删除节点时调用这个函数，如果删除的是未压缩的节点，可能导致一个节点解压缩。
// node传入非NULL，表示插入了某节点，可能会导致某一个节点压缩。
REDIS_STATIC void __quicklistCompress(const quicklist *quicklist,
                                      quicklistNode *node) {
    /* If length is less than our compress depth (from both sides),
     * we can't compress anything. */
    // 如果没有设置压缩深度，或者list的节点数小于压缩深度，那么我们不需要进行压缩，直接返回。
    // 注意这里等于我们不能返回，因为有可能是删除节点而调用这个函数处理解压缩的。
    // 当删除节点后恰好是等于 2倍压缩深度 个节点，我们需要处理某个节点的解压缩，所以还是需要后面的处理。
    // 而如果节点删除后是 2倍压缩深度-1 个节点，那么删除前我们就知道所有节点都是不压缩的，删除后也都不压缩，所以不需要后面处理，可直接返回。
    if (!quicklistAllowsCompression(quicklist) ||
        quicklist->len < (unsigned int)(quicklist->compress * 2))
        return;

#if 0
    /* Optimized cases for small depth counts */
    // 压缩深度为1、2的小case的优化处理。
    if (quicklist->compress == 1) {
        quicklistNode *h = quicklist->head, *t = quicklist->tail, *hn = h->next, *tp = t->prev;
        quicklistDecompressNode(h);
        quicklistDecompressNode(t);
        if (h != node && t != node)
            quicklistCompressNode(node);
        // 这里如果插入节点在头或尾时，当压缩深度为1，我们也需要处理内部临界节点的压缩的。（TODO）
        if (hn != t) {
            quicklistCompressNode(hn);
        }
        if (tp != h) {
            quicklistCompressNode(tp);
        }
        return;
    } else if (quicklist->compress == 2) {
        quicklistNode *h = quicklist->head, *hn = h->next, *hnn = hn->next;
        quicklistNode *t = quicklist->tail, *tp = t->prev, *tpp = tp->prev;
        quicklistDecompressNode(h);
        quicklistDecompressNode(hn);
        quicklistDecompressNode(t);
        quicklistDecompressNode(tp);
        if (h != node && hn != node && t != node && tp != node) {
            quicklistCompressNode(node);
        }
        // 因为前面判断的是总节点数<2倍压缩深度时直接返回。
        // 所以肯定有hnn!=t，但可能hnn==tp，这里可能会对tp进行压缩，显然不对，所以这里应该是hnn != tp才进行压缩。后面同理。
        // 同样，这里跟后面其他普通情形一样，为了处理插入节点的情况。如果插入节点是前面4个中的一个，那么我们需要对临界节点进行压缩。
        if (hnn != tp) {
            quicklistCompressNode(hnn);
        }
        if (tpp != hn) {
            quicklistCompressNode(tpp);
        }
        return;
    }
#endif

    /* Iterate until we reach compress depth for both sides of the list.a
     * Note: because we do length checks at the *top* of this function,
     *       we can skip explicit null checks below. Everything exists. */
    // 迭代列表直到我们两端达到压缩深度。因为我们前面检查了长度肯定是大于2倍的压缩深度的，所以我们这里没有检查NULL，因为节点肯定存在。
    quicklistNode *forward = quicklist->head;
    quicklistNode *reverse = quicklist->tail;
    int depth = 0;
    int in_depth = 0;
    // 遍历节点，直到达到压缩深度，跳出循环。
    while (depth++ < quicklist->compress) {
        // 两端压缩深度外侧的节点解压缩，非压缩状态的节点调用无影响，也没什么消耗。
        // 因为有可能是删除节点，调用这个函数来进行重新处理压缩深度解压缩，所有我们遍历到的节点都进行解压缩处理。
        quicklistDecompressNode(forward);
        quicklistDecompressNode(reverse);

        if (forward == node || reverse == node)
            // 如果还没达到压缩深度，遍历到了传入的节点，则该节点不应该被压缩，设置in_depth标识，表示当前节点在压缩深度外。
            in_depth = 1;

        /* We passed into compress depth of opposite side of the quicklist
         * so there's no need to compress anything and we can exit. */
        // 如果forward==reverse，显然总节点数<2倍压缩深度的，其实前面最开始我们直接返回了。
        // 而当遍历到最深的压缩深度时，如果forward->next==reverse，总节点数=2倍压缩深度，也都不需要压缩。
        if (forward == reverse || forward->next == reverse)
            return;

        forward = forward->next;
        reverse = reverse->prev;
    }

    // 前面没有遍历到node，说明需要对它进行压缩。
    if (!in_depth)
        quicklistCompressNode(node);

    /* At this point, forward and reverse are one node beyond depth */
    // 如果我们插入的节点在in_depth中，说明未被压缩的节点多了一个，显然对于左右临界点有一个节点要压缩，这里对这两个临界节点进行压缩处理。
    // 到这里，forward和reverse肯定是大于depth的第一个节点，即临界节点，应该要压缩处理的（当然如果该节点已压缩会忽略）。
    quicklistCompressNode(forward);
    quicklistCompressNode(reverse);
}

// 压缩某个节点，如果节点之前被压缩过（只是使用时解压缩了），那么我们直接重新压缩就可以了。
// 但如果是一个全新的没被压缩过的节点，我们需要判断该节点是否在压缩深度的内部，然后再决定是否压缩它。
#define quicklistCompress(_ql, _node)                                          \
    do {                                                                       \
        if ((_node)->recompress)                                               \
            quicklistCompressNode((_node));                                    \
        else                                                                   \
            __quicklistCompress((_ql), (_node));                               \
    } while (0)

/* If we previously used quicklistDecompressNodeForUse(), just recompress. */
// 如果我们使用了quicklistDecompressNodeForUse()来解压缩一个节点使用，则直接重新压缩就好。
#define quicklistRecompressOnly(_ql, _node)                                    \
    do {                                                                       \
        if ((_node)->recompress)                                               \
            quicklistCompressNode((_node));                                    \
    } while (0)

/* Insert 'new_node' after 'old_node' if 'after' is 1.
 * Insert 'new_node' before 'old_node' if 'after' is 0.
 * Note: 'new_node' is *always* uncompressed, so if we assign it to
 *       head or tail, we do not need to uncompress it. */
// 如果after为1，则将一个新节点new_node插入到old_node之后。否则after为0，则插入在之前。
// 这里我们不对new_node进行未压缩，由调用方处理。这样如果该节点是加入头或者尾我们就不需要对其压缩之后再解压缩操作了
REDIS_STATIC void __quicklistInsertNode(quicklist *quicklist,
                                        quicklistNode *old_node,
                                        quicklistNode *new_node, int after) {
    if (after) {
        // 在old_node之后插入new_node。更新两个节点的prev和next指针。
        new_node->prev = old_node;
        if (old_node) {
            new_node->next = old_node->next;
            if (old_node->next)
                old_node->next->prev = new_node;
            old_node->next = new_node;
        }
        // 如果old_node是tail节点，则更新新的tail。
        if (quicklist->tail == old_node)
            quicklist->tail = new_node;
    } else {
        // 在old_node之前插入new_node。更新两个节点的prev和next指针。
        new_node->next = old_node;
        if (old_node) {
            new_node->prev = old_node->prev;
            if (old_node->prev)
                old_node->prev->next = new_node;
            old_node->prev = new_node;
        }
        // 如果old_node是head节点，则更新新的head。
        if (quicklist->head == old_node)
            quicklist->head = new_node;
    }
    /* If this insert creates the only element so far, initialize head/tail. */
    // 如果之前是空list，则需要初始化head/tail。因为前面只会处理tail/head中的一个，但当len为0时，他们两个都需要初始化为new_node。
    if (quicklist->len == 0) {
        quicklist->head = quicklist->tail = new_node;
    }

    /* Update len first, so in __quicklistCompress we know exactly len */
    // 先更新总长度，然后再判断old_node是否需要压缩，因为处理压缩的函数__quicklistCompress需要使用当前准确的len。
    quicklist->len++;

    if (old_node)
        // 这里我们只判断old_node是否需要压缩。对于new_node调用方处理。
        quicklistCompress(quicklist, old_node);
}

/* Wrappers for node inserting around existing node. */
// 对在某个已知节点前后插入新节点的封装函数。
REDIS_STATIC void _quicklistInsertNodeBefore(quicklist *quicklist,
                                             quicklistNode *old_node,
                                             quicklistNode *new_node) {
    __quicklistInsertNode(quicklist, old_node, new_node, 0);
}

REDIS_STATIC void _quicklistInsertNodeAfter(quicklist *quicklist,
                                            quicklistNode *old_node,
                                            quicklistNode *new_node) {
    __quicklistInsertNode(quicklist, old_node, new_node, 1);
}

// 判断节点的ziplist大小是否达到限制。
REDIS_STATIC int
_quicklistNodeSizeMeetsOptimizationRequirement(const size_t sz,
                                               const int fill) {
    // 如果fill>=0，不能用这个函数判断，返回0。
    if (fill >= 0)
        return 0;

    // 为负，计算optimization_level对应的index。如果sz小于对应level的最大字节限制，足够写入数据返回1，否则返回0。
    size_t offset = (-fill) - 1;
    if (offset < (sizeof(optimization_level) / sizeof(*optimization_level))) {
        if (sz <= optimization_level[offset]) {
            return 1;
        } else {
            return 0;
        }
    } else {
        return 0;
    }
}

// 如果我们fill设置的是正值，表示限制entries数。但我们还是要有一个安全的size的限制的。这里返回true表示没达到限。
#define sizeMeetsSafetyLimit(sz) ((sz) <= SIZE_SAFETY_LIMIT)

// 判断node是否允许写入指定长度的数据。数据会封装成如下enrty写入，除了字符串长度外，还有额外的prevlen和encoding字段存储处理。
// struct entry {
//    int<var> prevlen; // 前一个entry的字节长度
//    int<var> encoding; // 元素类型编码
//    optional byte[] content; // 元素内容，为optional，可选的
//    // 对于很小的整数而言，已经内联到encoding字段尾部了，不需要content
//}
// 1、00xxxxxx 最大长度为63的短字符串，后面6位存储字符串的位数，剩余的字节就是字符串的内容。
// 2、01xxxxxx xxxxxxxx 中等长度的字符串，后面14位表示长度，剩余字节就是字符串的内容。
// 3、10000000 aaaaaaaa bbbbbbbb cccccccc dddddddd 特大字符串，需要使用额外4个字节来表示长度，长度后面跟着字符串内容。
//  第一个字节前缀是10，剩余6位没有使用，统一置为零。不过这样的大字符串是没有机会使用的，压缩列表通常只是用来存储小数据的。
REDIS_STATIC int _quicklistNodeAllowInsert(const quicklistNode *node,
                                           const int fill, const size_t sz) {
    // node为空，返回0，不能直接写入该节点。
    if (unlikely(!node))
        return 0;

    int ziplist_overhead;
    /* size of previous offset */
    // 处理prelen（前一个entry的字节长度）的存储。小于254，用一个字节存储长度，否则需要用5个字节来存储长度
    if (sz < 254)
        ziplist_overhead = 1;
    else
        ziplist_overhead = 5;

    /* size of forward offset */
    // 处理encoding字段的存储。字符串长度小于64时，1字节表示。字符串长度小于2^14是使用2个字节表示。否则需要使用5个字段表示。
    if (sz < 64)
        ziplist_overhead += 1;
    else if (likely(sz < 16384))
        ziplist_overhead += 2;
    else
        ziplist_overhead += 5;

    /* new_sz overestimates if 'sz' encodes to an integer type */
    // 计算加入新元素后，总长度是否超标。只有如下两种情况可以直接在当前ziplist中插入数据：
    // 1、fill为负数，表示对应optimization_level级别，新元素加入后总长度没超限，则返回1表示可插入当前ziplist。
    //  即：sz <= optimization_level[(-fill) - 1]时。
    // 2、fill值为正，表示限制entry数量，如果当前ziplist的总使用字节数没有超限，且节点entries数量也没有超过fill值，则可以直接插入。
    //  即：sizeMeetsSafetyLimit(new_sz) && ((int)node->count < fill)时。
    unsigned int new_sz = node->sz + sz + ziplist_overhead;
    if (likely(_quicklistNodeSizeMeetsOptimizationRequirement(new_sz, fill)))
        return 1;
    else if (!sizeMeetsSafetyLimit(new_sz))
        return 0;
    else if ((int)node->count < fill)
        return 1;
    else
        return 0;
}

// 判断两个node是否允许合并，即判断ziplist是否可以合并。
REDIS_STATIC int _quicklistNodeAllowMerge(const quicklistNode *a,
                                          const quicklistNode *b,
                                          const int fill) {
    // 任意一个节点为NULL，不需要合并，返回。
    if (!a || !b)
        return 0;

    /* approximate merged ziplist size (- 11 to remove one ziplist
     * header/trailer) */
    // 近似计算合并后的ziplist大小。-11是指：减去一个header（10）+结束标识（1）。
    unsigned int merge_sz = a->sz + b->sz - 11;
    // 判断逻辑就跟前面判断单node的ziplist是否超限一致。
    if (likely(_quicklistNodeSizeMeetsOptimizationRequirement(merge_sz, fill)))
        return 1;
    else if (!sizeMeetsSafetyLimit(merge_sz))
        return 0;
    else if ((int)(a->count + b->count) <= fill)
        return 1;
    else
        return 0;
}

// 更新节点的ziplist的总字节数
#define quicklistNodeUpdateSz(node)                                            \
    do {                                                                       \
        (node)->sz = ziplistBlobLen((node)->zl);                               \
    } while (0)

/* Add new entry to head node of quicklist.
 *
 * Returns 0 if used existing head.
 * Returns 1 if new head created. */
// 添加新的entry到quicklist的头节点。如果使用存在的头节点，则返回0；否则使用新创建的节点加入head，返回1。
int quicklistPushHead(quicklist *quicklist, void *value, size_t sz) {
    // 获取当前quicklist的头节点
    quicklistNode *orig_head = quicklist->head;
    // 先检查头节点的ziplist中加入该元素entry是否会超限。如果不超限则直接在头节点ziplist的head部位插入数据。
    // 否则我们需要新创建一个节点插入quicklist的头部，然后再在新节点ziplist的head部位写入数据。
    if (likely(
            _quicklistNodeAllowInsert(quicklist->head, quicklist->fill, sz))) {
        // 如果head节点空间足以添加新数据，则向head节点的ziplist的头部写入数据。
        quicklist->head->zl =
            ziplistPush(quicklist->head->zl, value, sz, ZIPLIST_HEAD);
        // 更新head节点的ziplist字节数大小。
        quicklistNodeUpdateSz(quicklist->head);
    } else {
        // 已有的head节点不足以写入新数据，则创建新的节点，写入数据。
        quicklistNode *node = quicklistCreateNode();
        node->zl = ziplistPush(ziplistNew(), value, sz, ZIPLIST_HEAD);
        // 更新新节点的ziplist字节数。
        quicklistNodeUpdateSz(node);
        // 新节点插入到quicklist->head前面。
        _quicklistInsertNodeBefore(quicklist, quicklist->head, node);
    }
    // 更新quicklist中总的元素数量。
    quicklist->count++;
    // 因为我们是在头节点（新创建的节点是现在的头节点）插入数据的，所以也要更新head节点的总元素数量，这样不用每次去解析ziplist来获取（有可能需要遍历ziplist）。
    quicklist->head->count++;
    // 如果创建了新节点，则返回值1，否则返回0。
    return (orig_head != quicklist->head);
}

/* Add new entry to tail node of quicklist.
 *
 * Returns 0 if used existing tail.
 * Returns 1 if new tail created. */
// 添加新的entry到quicklist的尾部。逻辑上与quicklistPushHead一致。
// 如果使用存在的尾节点，则返回0；否则使用新创建的节点加入tail，返回1。
int quicklistPushTail(quicklist *quicklist, void *value, size_t sz) {
    quicklistNode *orig_tail = quicklist->tail;
    if (likely(
            _quicklistNodeAllowInsert(quicklist->tail, quicklist->fill, sz))) {
        // 当前尾节点ziplist可以写入该数据，直接在ziplist的TAIL部位插入数据。并更新节点总的字节数。
        quicklist->tail->zl =
            ziplistPush(quicklist->tail->zl, value, sz, ZIPLIST_TAIL);
        quicklistNodeUpdateSz(quicklist->tail);
    } else {
        // 尾节点不能写入，则创建新节点作为尾节点，然后向新的尾节点插入数据。并更新节点总的字节数。
        quicklistNode *node = quicklistCreateNode();
        node->zl = ziplistPush(ziplistNew(), value, sz, ZIPLIST_TAIL);

        quicklistNodeUpdateSz(node);
        _quicklistInsertNodeAfter(quicklist, quicklist->tail, node);
    }
    // 更新quicklist总的entry数，以及尾节点中的entry数。
    quicklist->count++;
    quicklist->tail->count++;
    // 根据是否创建新的节点，返回1或0。
    return (orig_tail != quicklist->tail);
}

/* Create new node consisting of a pre-formed ziplist.
 * Used for loading RDBs where entire ziplists have been stored
 * to be retrieved later. */
// 使用一个已生成好的ziplist来创建新的quicklist节点，并加入quicklist中。
// 主要用于从RDB中加载数据。恢复时，我们会先还原ziplist存储，然后基于该ziplist构造节点从尾部加入quicklist中。
void quicklistAppendZiplist(quicklist *quicklist, unsigned char *zl) {
    quicklistNode *node = quicklistCreateNode();

    // 使用已有的ziplist更新节点的相关数据。
    node->zl = zl;
    node->count = ziplistLen(node->zl);
    node->sz = ziplistBlobLen(zl);

    // 新节点加入到quicklist尾部，并更新entries总数。
    _quicklistInsertNodeAfter(quicklist, quicklist->tail, node);
    quicklist->count += node->count;
}

/* Append all values of ziplist 'zl' individually into 'quicklist'.
 *
 * This allows us to restore old RDB ziplists into new quicklists
 * with smaller ziplist sizes than the saved RDB ziplist.
 *
 * Returns 'quicklist' argument. Frees passed-in ziplist 'zl' */
// 将ziplist中的所有元素单独加入到quicklist中。
// 我们以这种方式恢复老的RDB ziplist到新的quicklists中时，可以使得最终的ziplist字节数更少。
quicklist *quicklistAppendValuesFromZiplist(quicklist *quicklist,
                                            unsigned char *zl) {
    unsigned char *value;
    unsigned int sz;
    long long longval;
    char longstr[32] = {0};

    // p初始指向第一个entry位置
    unsigned char *p = ziplistIndex(zl, 0);
    // 遍历ziplist中的entry。
    while (ziplistGet(p, &value, &sz, &longval)) {
        if (!value) {
            /* Write the longval as a string so we can re-add it */
            // 如果value为NULL，则我们entry存储的是数字，这里需要再转换为字符串。
            sz = ll2string(longstr, sizeof(longstr), longval);
            value = (unsigned char *)longstr;
        }
        // 将值从quicklist尾部插入。
        quicklistPushTail(quicklist, value, sz);
        // 指向下一个entry，准备进入下一轮遍历。
        p = ziplistNext(zl, p);
    }
    // 释放传入的ziplist，并返回quicklist。
    zfree(zl);
    return quicklist;
}

/* Create new (potentially multi-node) quicklist from a single existing ziplist.
 *
 * Returns new quicklist.  Frees passed-in ziplist 'zl'. */
// 根据传入的ziplist来创建quicklist。
// 这里ziplist并不是直接作为节点的zl加入，而是遍历该ziplist，将entry一个个加入。
quicklist *quicklistCreateFromZiplist(int fill, int compress,
                                      unsigned char *zl) {
    return quicklistAppendValuesFromZiplist(quicklistNew(fill, compress), zl);
}

// 这个函数宏表示：当节点中没有元素时，从quicklist删除该节点。
#define quicklistDeleteIfEmpty(ql, n)                                          \
    do {                                                                       \
        if ((n)->count == 0) {                                                 \
            __quicklistDelNode((ql), (n));                                     \
            (n) = NULL;                                                        \
        }                                                                      \
    } while (0)

// 从quicklist中删除节点的逻辑。
REDIS_STATIC void __quicklistDelNode(quicklist *quicklist,
                                     quicklistNode *node) {
    /* Update the bookmark if any */
    // 在bookmark中查询待删除的节点，如果存在对应的bm，则从bookmark中删除该节点。
    quicklistBookmark *bm = _quicklistBookmarkFindByNode(quicklist, node);
    if (bm) {
        // 更新该bookmark指向待删除节点的后一个节点，因为节点是与quicklist公用，所有这里不用释放，后面从quicklist中删除时统一释放。
        bm->node = node->next;
        /* if the bookmark was to the last node, delete it. */
        // 如果当前bookmark指向的是quicklist的最后一个节点，则删除该bookmark。
        if (!bm->node)
            _quicklistBookmarkDelete(quicklist, bm);
    }

    // 如果待删除节点有next节点，则更新next节点的prev指针。
    if (node->next)
        node->next->prev = node->prev;
    // 如果待删除节点有prev节点，则更新prev节点的next指针。
    if (node->prev)
        node->prev->next = node->next;

    // 如果删除的是tail节点，则更新tail指针。
    if (node == quicklist->tail) {
        quicklist->tail = node->prev;
    }

    // 如果删除的是head节点，则更新head指针。
    if (node == quicklist->head) {
        quicklist->head = node->next;
    }

    /* Update len first, so in __quicklistCompress we know exactly len */
    // 先更新quicklist的节点个数以及总的entries数量。因为后面调用__quicklistCompress压缩节点时需要使用。
    quicklist->len--;
    quicklist->count -= node->count;

    /* If we deleted a node within our compress depth, we
     * now have compressed nodes needing to be decompressed. */
    // 如果我们删除的节点在<压缩深度的位置，即删除的节点是不压缩的，那么我们压缩深度需要向内扩展一个节点，即有一个节点需要解压缩。
    __quicklistCompress(quicklist, NULL);

    // 释放节点的ziplist，释放该节点。
    zfree(node->zl);
    zfree(node);
}

/* Delete one entry from list given the node for the entry and a pointer
 * to the entry in the node.
 *
 * Note: quicklistDelIndex() *requires* uncompressed nodes because you
 *       already had to get *p from an uncompressed node somewhere.
 *
 * Returns 1 if the entire node was deleted, 0 if node still exists.
 * Also updates in/out param 'p' with the next offset in the ziplist. */
// 给定节点，以及指向节点ziplist中entry的指针，删除该entry。
// 注意该函数的参数node需要是未压缩的节点，因为传入的参数*p是指向节点中未压缩的ziplist中entry位置。
// 如果entry所在的node没有元素了被删除，则返回1。否则返回0表示节点依然有元素。
// 另外删除entry后，参数p会指向下一个entry位置，这样可以迭代删除处理（还需要根据返回值来确定node，有可能当前node被删除，要跳到下一个）。
REDIS_STATIC int quicklistDelIndex(quicklist *quicklist, quicklistNode *node,
                                   unsigned char **p) {
    int gone = 0;

    // 删除p指向的entry，p指向下一个entry，并更新node中entry数量。
    node->zl = ziplistDelete(node->zl, p);
    node->count--;
    if (node->count == 0) {
        // 如果当前node的entry数为0个，则我们需要删除整个node。
        gone = 1;
        __quicklistDelNode(quicklist, node);
    } else {
        // 如果node还有entry，我们要更新node的ziplist总字节数
        quicklistNodeUpdateSz(node);
    }
    // 更新quicklist中总的entry数量。
    quicklist->count--;
    /* If we deleted the node, the original node is no longer valid */
    // 如果node被删除，返回1通知调用者节点无效了。
    return gone ? 1 : 0;
}

/* Delete one element represented by 'entry'
 *
 * 'entry' stores enough metadata to delete the proper position in
 * the correct ziplist in the correct quicklist node. */
// 删除entry指代的元素。entry中存储了足够多的元数据信息，是可以删除指定正确的quicklist中的ziplist中的适当位置元素的。
void quicklistDelEntry(quicklistIter *iter, quicklistEntry *entry) {
    // 获取到entry所在节点prev和next节点，因为entry删除后可能导致节点被删除，所以我们需要前后指针来更新迭代器iter。
    quicklistNode *prev = entry->node->prev;
    quicklistNode *next = entry->node->next;
    // 删除quicklist的指定节点的ziplist中指定位置的entry数据。deleted_node指示节点是否被删除。
    int deleted_node = quicklistDelIndex((quicklist *)entry->quicklist,
                                         entry->node, &entry->zi);

    /* after delete, the zi is now invalid for any future usage. */
    // 删除该元素后，当前zi无效了，置为NULL。
    // 实际上如果节点没被删除的话，我们可以设置iter->zi=entry->zi（反向迭代还要通过entry的prevlen计算得到）。
    // 不过因为我们记录了原offset，我们后面可以使用offset计算得到iter->zi。（正反向迭代处理方式不同）
    iter->zi = NULL;

    /* If current node is deleted, we must update iterator node and offset. */
    // 如果当前节点被删除了，那么我们需要更新迭代器的node和offset，用于计算下一次迭代处理的位置。
    if (deleted_node) {
        if (iter->direction == AL_START_HEAD) {
            // 如果是正向迭代，则迭代节点为next节点，offset为0表示节点中第一个entry。
            iter->current = next;
            iter->offset = 0;
        } else if (iter->direction == AL_START_TAIL) {
            // 如果是逆向迭代，则迭代节点为prev节点，offset为-1表示节点中最后一个entry。
            iter->current = prev;
            iter->offset = -1;
        }
    }
    // 如果节点没被删除，则我们什么都不需要更新。前面已经设置iter->zi为NULL了，而下一个entry的offset不用变。
    // 因为如果删除的不是tail entry，那么删除后原来的offset本身就指向下一个entry位置了，不需要处理。
    // 而如果删除的就是tail entry，那么offset指向了结束符的位置，后面调用quicklistNext()时将跳到下一个节点处理，所以这里也不需要处理。
    /* else if (!deleted_node), no changes needed.
     * we already reset iter->zi above, and the existing iter->offset
     * doesn't move again because:
     *   - [1, 2, 3] => delete offset 1 => [1, 3]: next element still offset 1
     *   - [1, 2, 3] => delete offset 0 => [2, 3]: next element still offset 0
     *  if we deleted the last element at offet N and now
     *  length of this ziplist is N-1, the next call into
     *  quicklistNext() will jump to the next node. */
}

/* Replace quicklist entry at offset 'index' by 'data' with length 'sz'.
 *
 * Returns 1 if replace happened.
 * Returns 0 if replace failed and no changes happened. */
// 使用数据data（长度为sz）替换quicklist的index位置的entry。
// 成功替换则返回1，否则失败没有替换返回0。
int quicklistReplaceAtIndex(quicklist *quicklist, long index, void *data,
                            int sz) {
    quicklistEntry entry;
    // 根据index获取到quicklist中对应的entry，且拿到的entry对应的quicklist节点是已经解压缩的。
    if (likely(quicklistIndex(quicklist, index, &entry))) {
        /* quicklistIndex provides an uncompressed node */
        // 调用ziplistReplace来处理对应节点的ziplist上entry位置数据的替换。
        // 数据长度相同则直接覆盖数据就可以了，否则先删除指定entry，再插入数据构造新entry。
        // 这里replace貌似没有ziplist最大长度限制，所以理论上我们可以通过replace构造很大的ziplist结构作为node？
        entry.node->zl = ziplistReplace(entry.node->zl, entry.zi, data, sz);
        // 更新节点ziplist的总字节数。
        quicklistNodeUpdateSz(entry.node);
        // 重新压缩该节点。
        quicklistCompress(quicklist, entry.node);
        return 1;
    } else {
        return 0;
    }
}

/* Given two nodes, try to merge their ziplists.
 *
 * This helps us not have a quicklist with 3 element ziplists if
 * our fill factor can handle much higher levels.
 *
 * Note: 'a' must be to the LEFT of 'b'.
 *
 * After calling this function, both 'a' and 'b' should be considered
 * unusable.  The return value from this function must be used
 * instead of re-using any of the quicklistNode input arguments.
 *
 * Returns the input node picked to merge against or NULL if
 * merging was not possible. */
// 给定两个节点，尝试合并他们的ziplist。这样我们可以合并较少元素的节点，从而保证ziplist中的元素不会太少。
// 注意：a节点必须是b的LEFT节点。当调用这个函数后，a和b两个节点应该都被认为不可用，我们不应该再去使用它们，而必须使用函数的返回节点作为替代。
// 成功合并，则函数返回新的合并后的节点；否则合并失败返回NULL。
REDIS_STATIC quicklistNode *_quicklistZiplistMerge(quicklist *quicklist,
                                                   quicklistNode *a,
                                                   quicklistNode *b) {
    D("Requested merge (a,b) (%u, %u)", a->count, b->count);

    // 解压缩a、b节点
    quicklistDecompressNode(a);
    quicklistDecompressNode(b);
    // 调用ziplistMerge，来对a、b的ziplist进行合并。
    if ((ziplistMerge(&a->zl, &b->zl))) {
        /* We merged ziplists! Now remove the unused quicklistNode. */
        // ziplist合并成功，下面我们移除其中一个无用的节点。
        // 我们根据节点的zl值来判断，值为NULL表示该节点需要移除，具体原因见ziplistMerge实现的末尾部分。
        quicklistNode *keep = NULL, *nokeep = NULL;
        // 选择保留和移除的节点。
        if (!a->zl) {
            nokeep = a;
            keep = b;
        } else if (!b->zl) {
            nokeep = b;
            keep = a;
        }
        // 更新保留的节点的总entry数 和 ziplist总字节数。
        keep->count = ziplistLen(keep->zl);
        quicklistNodeUpdateSz(keep);

        // 删除不需要保留的那个节点。
        nokeep->count = 0;
        __quicklistDelNode(quicklist, nokeep);
        // 对保留的节点进行压缩处理。
        quicklistCompress(quicklist, keep);
        return keep;
    } else {
        /* else, the merge returned NULL and nothing changed. */
        // merge失败，返回了NULL，什么都没改变
        return NULL;
    }
}

/* Attempt to merge ziplists within two nodes on either side of 'center'.
 *
 * We attempt to merge:
 *   - (center->prev->prev, center->prev)
 *   - (center->next, center->next->next)
 *   - (center->prev, center)
 *   - (center, center->next)
 */
// 尝试合并center左右两个节点范围内的节点，合并它们的ziplist，总共如上四种情形。
REDIS_STATIC void _quicklistMergeNodes(quicklist *quicklist,
                                       quicklistNode *center) {
    // quicklist节点的填充因子
    int fill = quicklist->fill;
    quicklistNode *prev, *prev_prev, *next, *next_next, *target;
    prev = prev_prev = next = next_next = target = NULL;

    // 拿到以center前后的四个节点指针。
    if (center->prev) {
        prev = center->prev;
        if (center->prev->prev)
            prev_prev = center->prev->prev;
    }

    if (center->next) {
        next = center->next;
        if (center->next->next)
            next_next = center->next->next;
    }

    /* Try to merge prev_prev and prev */
    // 尝试合并prev_prev和prev节点。合并前先判断是否允许合并，即两个节点有效（非NULL）且节点的ziplist长度总和不超过限制才能合并。
    if (_quicklistNodeAllowMerge(prev, prev_prev, fill)) {
        _quicklistZiplistMerge(quicklist, prev_prev, prev);
        prev_prev = prev = NULL; /* they could have moved, invalidate them. */
    }

    /* Try to merge next and next_next */
    // 尝试合并next和next_next节点。
    if (_quicklistNodeAllowMerge(next, next_next, fill)) {
        _quicklistZiplistMerge(quicklist, next, next_next);
        next = next_next = NULL; /* they could have moved, invalidate them. */
    }

    /* Try to merge center node and previous node */
    // 尝试合并center和center->prev节点（这里合并center的前一个节点不能再用前面的prev，因为前面合并后prev已经失效了）。
    if (_quicklistNodeAllowMerge(center, center->prev, fill)) {
        // 成功合并的话，我们需要这里合并的返回节点来跟前面合并后的center->next合并，所以这里需要保存当前合并后的节点为target。
        target = _quicklistZiplistMerge(quicklist, center->prev, center);
        center = NULL; /* center could have been deleted, invalidate it. */
    } else {
        /* else, we didn't merge here, but target needs to be valid below. */
        // 如果不能合并，显然我们的target还是center节点。
        target = center;
    }

    /* Use result of center merge (or original) to merge with next node. */
    // 最后我们合并target和target->next节点。
    if (_quicklistNodeAllowMerge(target, target->next, fill)) {
        _quicklistZiplistMerge(quicklist, target, target->next);
    }
}

/* Split 'node' into two parts, parameterized by 'offset' and 'after'.
 *
 * The 'after' argument controls which quicklistNode gets returned.
 * If 'after'==1, returned node has elements after 'offset'.
 *                input node keeps elements up to 'offset', including 'offset'.
 * If 'after'==0, returned node has elements up to 'offset'.
 *                input node keeps elements after 'offset', including 'offset'.
 *
 * Or in other words:
 * If 'after'==1, returned node will have elements after 'offset'.
 *                The returned node will have elements [OFFSET+1, END].
 *                The input node keeps elements [0, OFFSET].
 * If 'after'==0, returned node will keep elements up to but not including 'offset'.
 *                The returned node will have elements [0, OFFSET-1].
 *                The input node keeps elements [OFFSET, END].
 *
 * The input node keeps all elements not taken by the returned node.
 *
 * Returns newly created node or NULL if split not possible. */
// 将传入的节点分成两部分。offset指定切分位置，after参数指定返回哪个节点。
// 1、after=1表示返回offset之后的那部分的节点，即返回节点包含元素[OFFSET+1, END]，而入参节点则包含元素[0, OFFSET]。
// 2、after=0表示返回offset之前（包含offset）的那部分的节点，即返回节点包含元素[0, OFFSET-1]，而入参节点则包含元素[OFFSET, END]。
// 如果没有处理节点划分，则返回新创建的节点或者NULL。入参node会保留所有没放入返回节点中的元素，即元素要么在返回节点要么在入参节点。
REDIS_STATIC quicklistNode *_quicklistSplitNode(quicklistNode *node, int offset,
                                                int after) {
    size_t zl_sz = node->sz;

    // 构建新节点
    quicklistNode *new_node = quicklistCreateNode();
    new_node->zl = zmalloc(zl_sz);

    /* Copy original ziplist so we can split it */
    // 复制ziplist的所有数据到新节点ziplist中。
    memcpy(new_node->zl, node->zl, zl_sz);

    /* Ranges to be trimmed: -1 here means "continue deleting until the list ends" */
    // 现在两个节点中都有了所有的数据，这里我们需要决定对两个节点的ziplist各自如何截取了。
    // 如果after=1，则原始node保留前半部分，需要trim的范围为[offset+1, -1)，保留[0, offset]
    //      而返回节点需要trim的部分为[0, offset+1)，保留[offset+1, -1)
    // 如果after=0，则原始node保留后半部分，需要trim的范围为[0, offset)，保留[offset, -1)
    //      而返回节点需要trim的部分为[offset, -1)，保留[0, offset-1]
    // 注意这里单独的-1作为num传入ziplistDeleteRange中会被解释为最大的无符号数，因而表示删除节点直到列表结束。
    int orig_start = after ? offset + 1 : 0;
    int orig_extent = after ? -1 : offset;
    int new_start = after ? 0 : offset;
    int new_extent = after ? offset + 1 : -1;

    D("After %d (%d); ranges: [%d, %d], [%d, %d]", after, offset, orig_start,
      orig_extent, new_start, new_extent);

    // 对原始节点进行trim操作。并更新ziplist总的entry数和总字节数。
    node->zl = ziplistDeleteRange(node->zl, orig_start, orig_extent);
    node->count = ziplistLen(node->zl);
    quicklistNodeUpdateSz(node);

    // 对新的需要返回的节点进行trim操作。并更新ziplist总的entry数和总字节数。
    new_node->zl = ziplistDeleteRange(new_node->zl, new_start, new_extent);
    new_node->count = ziplistLen(new_node->zl);
    quicklistNodeUpdateSz(new_node);

    D("After split lengths: orig (%d), new (%d)", node->count, new_node->count);
    // 返回新节点
    return new_node;
}

/* Insert a new entry before or after existing entry 'entry'.
 *
 * If after==1, the new value is inserted after 'entry', otherwise
 * the new value is inserted before 'entry'. */
// 在指定entry的前面（after=1） 或 后面（after=0）插入新的元素。
REDIS_STATIC void _quicklistInsert(quicklist *quicklist, quicklistEntry *entry,
                                   void *value, const size_t sz, int after) {
    int full = 0, at_tail = 0, at_head = 0, full_next = 0, full_prev = 0;
    int fill = quicklist->fill;
    quicklistNode *node = entry->node;
    quicklistNode *new_node = NULL;

    if (!node) {
        /* we have no reference node, so let's create only node in the list */
        // 如果entry没有关联的node，这里我们需要创建新的node，并将数据写入。
        D("No node given!");
        new_node = quicklistCreateNode();
        // 创建ziplist，并从head加入数据。
        new_node->zl = ziplistPush(ziplistNew(), value, sz, ZIPLIST_HEAD);
        // 因为新quicklist的head和tail都为NULL，所以从head和tail插入节点一样。
        __quicklistInsertNode(quicklist, NULL, new_node, after);
        // 更新新节点的总entry数 以及 quicklist的总节点数。
        new_node->count++;
        quicklist->count++;
        return;
    }

    /* Populate accounting flags for easier boolean checks later */
    // 计算一些标识，以便后面更好的进行bool判断。
    // 到这里说明指定entry对应的节点存在，所以我们需要检查该节点的ziplist是否可以插入新元素（即写入后是否超限）。
    // 如果不能在插入新元素，则标识节点已满，full设为1。
    if (!_quicklistNodeAllowInsert(node, fill, sz)) {
        D("Current node is full with count %d with requested fill %lu",
          node->count, fill);
        full = 1;
    }

    // 如果是在entry后面插入元素，则判断当前entry是否是当前节点的tail元素，是则设置at_tail为1。
    if (after && (entry->offset == node->count)) {
        D("At Tail of current ziplist");
        at_tail = 1;
        // at_tail为1，表示在该节点ziplist最后插入元素，如果我们当前节点满了，即full为1的话，这里需要判断后一个节点是否full。
        // 如果后一个节点也full，则设置full_next为1，从而标识我们需要在这两个节点之间新创建节点来保存新元素。
        if (full && !_quicklistNodeAllowInsert(node->next, fill, sz)) {
            D("Next node is full too.");
            full_next = 1;
        }
    }

    // 如果是在entry前面插入元素，则判断当前entry是否是当前节点的head元素，是则设置at_head为1。
    if (!after && (entry->offset == 0)) {
        D("At Head");
        at_head = 1;
        // at_head为1，表示在该节点ziplist头部插入元素，如果我们当前节点满了，即full为1的话，这里需要判断前一个节点是否full。
        // 如果前一个节点也full，则设置full_prev为1，从而标识我们需要在这两个节点之间新创建节点来保存新元素。
        if (full && !_quicklistNodeAllowInsert(node->prev, fill, sz)) {
            D("Prev node is full too.");
            full_prev = 1;
        }
    }

    /* Now determine where and how to insert the new element */
    // 现在可以决定在哪里以及怎么插入新元素了。
    if (!full && after) {
        // 当前节点没满，且在entry之后插入。
        D("Not full, inserting after current position.");
        // 先解压缩节点来使用
        quicklistDecompressNodeForUse(node);
        // 定位到指定entry的next位置，如果指定entry位置或entry的下一个位置为结束符时，会返回NULL。
        unsigned char *next = ziplistNext(node->zl, entry->zi);
        if (next == NULL) {
            // next为NULL，表示我们要在ziplist的末尾插入元素。
            node->zl = ziplistPush(node->zl, value, sz, ZIPLIST_TAIL);
        } else {
            // 否则在指定的next位置插入元素。
            node->zl = ziplistInsert(node->zl, next, value, sz);
        }
        // 插入完成，更新节点的总entry数以及ziplist总字节数。
        node->count++;
        quicklistNodeUpdateSz(node);
        // 重新压缩节点
        quicklistRecompressOnly(quicklist, node);
    } else if (!full && !after) {
        // 当前节点没满，且在entry之前插入。
        D("Not full, inserting before current position.");
        // 解压缩节点
        quicklistDecompressNodeForUse(node);
        // 在entry之前插入，也就是说在指向该entry的指针位置插入。此位置我们知道，即entry->zi，所以直接插入。
        node->zl = ziplistInsert(node->zl, entry->zi, value, sz);
        // 插入完成，更新节点的总entry数以及ziplist总字节数。
        node->count++;
        quicklistNodeUpdateSz(node);
        // 重新压缩节点
        quicklistRecompressOnly(quicklist, node);
    } else if (full && at_tail && node->next && !full_next && after) {
        // 当前节点满了，并且在entry后面插入，且entry是tail元素，此时我们需要看下一个节点。
        // 如果下一个节点存在且没满（可插入当前元素），则插入下一个节点的头部。
        /* If we are: at tail, next has free space, and inserting after:
         *   - insert entry at head of next node. */
        D("Full and tail, but next isn't full; inserting next node head");
        // 获取next节点解压缩
        new_node = node->next;
        quicklistDecompressNodeForUse(new_node);
        // 在next节点的头部插入数据
        new_node->zl = ziplistPush(new_node->zl, value, sz, ZIPLIST_HEAD);
        // 更新next节点的总entry数以及ziplist总字节数。
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        // 重新压缩next节点。
        quicklistRecompressOnly(quicklist, new_node);
    } else if (full && at_head && node->prev && !full_prev && !after) {
        // 当前节点满了，并且在entry前面插入，且entry是head元素，此时我们需要看前一个节点。
        // 如果前一个节点存在且没满（可插入当前元素），则插入前一个节点的尾部。
        /* If we are: at head, previous has free space, and inserting before:
         *   - insert entry at tail of previous node. */
        D("Full and head, but prev isn't full, inserting prev node tail");
        // 获取prev节点解压缩
        new_node = node->prev;
        quicklistDecompressNodeForUse(new_node);
        // 在prev节点的尾部插入数据
        new_node->zl = ziplistPush(new_node->zl, value, sz, ZIPLIST_TAIL);
        // 更新prev节点的总entry数以及ziplist总字节数。
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        // 重新压缩prev节点。
        quicklistRecompressOnly(quicklist, new_node);
    } else if (full && ((at_tail && full_next && after) ||
                        (at_head && full_prev && !after))) {
        // 如果当前节点满了，且要么需要在前面插入，但前面节点为空或满了；要么需要在后面插入，但后面节点为空或满了。
        // 那么我们需要新创建一个节点来写入新数据，新节点根据after来判断是插入当前节点的前面或后面。
        /* If we are: full, and our prev/next is 'full' (means node is null or realy full), then:
         *   - create new node and attach to quicklist */
        D("\tprovisioning new node...");
        // 创建新节点，写入数据。并更新节点总的entry数，以及ziplist总的字节数。
        new_node = quicklistCreateNode();
        new_node->zl = ziplistPush(ziplistNew(), value, sz, ZIPLIST_HEAD);
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        // 将新节点new_node插入到原节点node的前/后（根据after参数决定）。
        __quicklistInsertNode(quicklist, node, new_node, after);
    } else if (full) {
        // 执行到这里说明我们当前节点满了，且是在中间插入数据，需要拆分当前节点。
        // 实际上原逻辑如果是在当前节点的head/tail插入数据，但是prev/next节点不存在（即当前节点是头或尾节点），也会走到这里，所以前面逻辑需要把节点非空判断去掉。
        /* else, node is full we need to split it. */
        /* covers both after and !after cases */
        D("\tsplitting node...");
        // 解压缩节点
        quicklistDecompressNodeForUse(node);
        // 根据指定的entry的offset，切分节点。注意返回的新节点不包含指定的节点。
        new_node = _quicklistSplitNode(node, entry->offset, after);
        // 如果after=1，则我们需要在新节点的head插入数据，否则在新节点的tail插入数据。
        new_node->zl = ziplistPush(new_node->zl, value, sz,
                                   after ? ZIPLIST_HEAD : ZIPLIST_TAIL);
        // 更新新节点的总entry数，以及ziplist的总字节数。
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        // 将新节点插入到指定node的前面或后面。
        __quicklistInsertNode(quicklist, node, new_node, after);
        // 因为我们对一个节点进行了切分，那么可能前后会有节点需要合并，这里对原node前后2个节点进行合并操作。
        _quicklistMergeNodes(quicklist, node);
    }

    // 更新quicklist总的entrys数量。
    quicklist->count++;
}

// 在quicklist指定的entry前面插入元素。
void quicklistInsertBefore(quicklist *quicklist, quicklistEntry *entry,
                           void *value, const size_t sz) {
    _quicklistInsert(quicklist, entry, value, sz, 0);
}

// 在quicklist指定的entry后面插入元素。
void quicklistInsertAfter(quicklist *quicklist, quicklistEntry *entry,
                          void *value, const size_t sz) {
    _quicklistInsert(quicklist, entry, value, sz, 1);
}

/* Delete a range of elements from the quicklist.
 *
 * elements may span across multiple quicklistNodes, so we
 * have to be careful about tracking where we start and end.
 *
 * Returns 1 if entries were deleted, 0 if nothing was deleted. */
// 从quicklist中删除一段连续的元素。这些元素可能跨越多个node，所以我们需要很小心的追踪start和end位置。
// 元素都删除成功则返回1，否则什么元素都没删返回0。
int quicklistDelRange(quicklist *quicklist, const long start,
                      const long count) {
    if (count <= 0)
        return 0;

    unsigned long extent = count; /* range is inclusive of start position */

    if (start >= 0 && extent > (quicklist->count - start)) {
        /* if requesting delete more elements than exist, limit to list size. */
        // 如果要求删除的元素数量 多余 从start往后的总元素数，那么我们最多从start删除到结束。
        extent = quicklist->count - start;
    } else if (start < 0 && extent > (unsigned long)(-start)) {
        /* else, if at negative offset, limit max size to rest of list. */
        // 同样，对于负的start，也计算最多删除元素数。
        extent = -start; /* c.f. LREM -29 29; just delete until end. */
    }

    quicklistEntry entry;
    // 取出start位置的entry信息，如果start为负的，则取到的entry的offset也是负的。
    if (!quicklistIndex(quicklist, start, &entry))
        return 0;

    D("Quicklist delete request for start %ld, count %ld, extent: %ld", start,
      count, extent);
    // 处理的起始节点
    quicklistNode *node = entry.node;

    /* iterate over next nodes until everything is deleted. */
    // 每次迭代处理一个节点中的entry数据删除。只要还有剩余待删除数据，则一直迭代。（extent表示剩余待删除元素个数）
    while (extent) {
        // 拿到下一个节点
        quicklistNode *next = node->next;

        unsigned long del;
        int delete_entire_node = 0;
        if (entry.offset == 0 && extent >= node->count) {
            /* If we are deleting more than the count of this node, we
             * can just delete the entire node without ziplist math. */
            // 对于当前node，如果删除起始位置是head，且剩余要删除的元素数量大于当前节点总元素数，
            // 说明我们可以直接删掉当前节点，而不需要去匹配ziplist元素。更新相关删除标识，计算删除元素数。
            delete_entire_node = 1;
            del = node->count;
        } else if (entry.offset >= 0 && extent + entry.offset >= node->count) {
            /* If deleting more nodes after this one, calculate delete based
             * on size of current node. */
            // 如果后面节点还有要删除的元素，则计算当前节点删除的元素数。
            del = node->count - entry.offset;
        } else if (entry.offset < 0) {
            /* If offset is negative, we are in the first run of this loop
             * and we are deleting the entire range
             * from this start offset to end of list.  Since the Negative
             * offset is the number of elements until the tail of the list,
             * just use it directly as the deletion count. */
            // 如果offset是负的，说明我们是首次进入这个循环，这里先假设当前节点要删除所有元素，则删除元素即为-offset。
            del = -entry.offset;

            /* If the positive offset is greater than the remaining extent,
             * we only delete the remaining extent, not the entire offset.
             */
            // 修正当前节点删除元素数。因为只有next节点上还有需要删除的元素时，我们才把当前节点offset之后的所有元素都删除。
            // 而如果删除结束位置就在当前节点上，则我们不能删除当前节点offset之后的所有元素，所以更新当前节点删除的元素数。
            if (del > extent)
                del = extent;
        } else {
            /* else, we are deleting less than the extent of this node, so
             * use extent directly. */
            // 执行到这里 entry.offset >= 0 并且结束位置也在当前节点上，所以我们只需要删除剩余待删除个数的元素即可。
            del = extent;
        }

        D("[%ld]: asking to del: %ld because offset: %d; (ENTIRE NODE: %d), "
          "node count: %u",
          extent, del, entry.offset, delete_entire_node, node->count);

        if (delete_entire_node) {
            // 如果是删除当前整个节点，则直接处理。
            __quicklistDelNode(quicklist, node);
        } else {
            // 需要删当前节点的部分元素。先解压缩当前节点。
            quicklistDecompressNodeForUse(node);
            // 从offset开始删除ziplist中后续del个元素，完成后更新节点的总entry数、ziplist总字节数、quicklist总entry数。
            node->zl = ziplistDeleteRange(node->zl, entry.offset, del);
            quicklistNodeUpdateSz(node);
            node->count -= del;
            quicklist->count -= del;
            // 如果删除操作后节点没元素了则删除该节点。如果还有元素，节点没被删除，则需要重新压缩。
            quicklistDeleteIfEmpty(quicklist, node);
            if (node)
                quicklistRecompressOnly(quicklist, node);
        }

        // 更新相关信息，下一轮继续迭代。
        // 这里更新剩余待删除元素，更新待处理节点，另外下一轮由于是从头开始删除，所以offset设置为0
        extent -= del;

        node = next;

        entry.offset = 0;
    }
    return 1;
}

/* Passthrough to ziplistCompare() */
// 对比p1指向的ziplist entry数据是否与p2内容相等。
// 先解析p1指向的entry数据，如果是字符串则与p2指向的内容挨个字符对比，如果是数字还要提取数字与p2转的数字对比。
int quicklistCompare(unsigned char *p1, unsigned char *p2, int p2_len) {
    return ziplistCompare(p1, p2, p2_len);
}

/* Returns a quicklist iterator 'iter'. After the initialization every
 * call to quicklistNext() will return the next element of the quicklist. */
// 返回一个quicklist的迭代器iter。初始化后，每次调用quicklistNext()都会返回quicklist的下一个元素。
quicklistIter *quicklistGetIterator(const quicklist *quicklist, int direction) {
    quicklistIter *iter;

    iter = zmalloc(sizeof(*iter));

    // 根据传入的迭代方向设置初始节点和offset。
    if (direction == AL_START_HEAD) {
        iter->current = quicklist->head;
        iter->offset = 0;
    } else if (direction == AL_START_TAIL) {
        iter->current = quicklist->tail;
        iter->offset = -1;
    }

    // 初始化迭代方向和quicklist
    iter->direction = direction;
    iter->quicklist = quicklist;

    iter->zi = NULL;

    return iter;
}

/* Initialize an iterator at a specific offset 'idx' and make the iterator
 * return nodes in 'direction' direction. */
// 使用指定的迭代方向和offset来初始化迭代器
quicklistIter *quicklistGetIteratorAtIdx(const quicklist *quicklist,
                                         const int direction,
                                         const long long idx) {
    quicklistEntry entry;

    // 先获取到idx处的entry。
    if (quicklistIndex(quicklist, idx, &entry)) {
        // 初始化迭代器，更新迭代器的当前offset为传入idx对应的entry。
        quicklistIter *base = quicklistGetIterator(quicklist, direction);
        base->zi = NULL;
        base->current = entry.node;
        base->offset = entry.offset;
        return base;
    } else {
        return NULL;
    }
}

/* Release iterator.
 * If we still have a valid current node, then re-encode current node. */
// 释放迭代器。如果迭代器当前的node仍然有效，则需要重新压缩节点。
void quicklistReleaseIterator(quicklistIter *iter) {
    if (iter->current)
        quicklistCompress(iter->quicklist, iter->current);

    zfree(iter);
}

/* Get next element in iterator.
 *
 * Note: You must NOT insert into the list while iterating over it.
 * You *may* delete from the list while iterating using the
 * quicklistDelEntry() function.
 * If you insert into the quicklist while iterating, you should
 * re-create the iterator after your addition.
 *
 * iter = quicklistGetIterator(quicklist,<direction>);
 * quicklistEntry entry;
 * while (quicklistNext(iter, &entry)) {
 *     if (entry.value)
 *          [[ use entry.value with entry.sz ]]
 *     else
 *          [[ use entry.longval ]]
 * }
 *
 * Populates 'entry' with values for this iteration.
 * Returns 0 when iteration is complete or if iteration not possible.
 * If return value is 0, the contents of 'entry' are not valid.
 */
// 通过迭代器获取quicklist的下一个元素。具体用法见上面注释。
// 注意在不能在迭代时插入数据，但可以使用quicklistDelEntry()来删除迭代到的数据。
// 如果想在迭代时插入数据，则需要在插入后基于该idx重新创建迭代器。
// 当迭代完成或无法再进行迭代时返回0，如果返回0，则entry是无效的，不能使用。
int quicklistNext(quicklistIter *iter, quicklistEntry *entry) {
    initEntry(entry);

    // 迭代器为NULL，直接返回
    if (!iter) {
        D("Returning because no iter!");
        return 0;
    }

    entry->quicklist = iter->quicklist;
    entry->node = iter->current;

    // 迭代到的当前节点为NULL，直接返回
    if (!iter->current) {
        D("Returning because current node is NULL")
        return 0;
    }

    unsigned char *(*nextFn)(unsigned char *, unsigned char *) = NULL;
    int offset_update = 0;

    if (!iter->zi) {
        /* If !zi, use current index. */
        // 迭代器的zi为NULL，则通过迭代器中的offset来获取填充。
        quicklistDecompressNodeForUse(iter->current);
        iter->zi = ziplistIndex(iter->current->zl, iter->offset);
    } else {
        /* else, use existing iterator offset and get prev/next as necessary. */
        // 如果zi不为空，则通过zi获取下一个entry位置。
        // 这里根据迭代方向确定获取下一个entry位置的函数，以及offset更新方向。
        if (iter->direction == AL_START_HEAD) {
            nextFn = ziplistNext;
            offset_update = 1;
        } else if (iter->direction == AL_START_TAIL) {
            nextFn = ziplistPrev;
            offset_update = -1;
        }
        // 更新zi为下一个entry位置，更新迭代的offset
        iter->zi = nextFn(iter->current->zl, iter->zi);
        iter->offset += offset_update;
    }

    entry->zi = iter->zi;
    entry->offset = iter->offset;

    if (iter->zi) {
        /* Populate value from existing ziplist position */
        // 如果前面通过迭代器获取到的下一个entry指针存在，则直接在当前节点中获取指针处的entry。
        ziplistGet(entry->zi, &entry->value, &entry->sz, &entry->longval);
        return 1;
    } else {
        /* We ran out of ziplist entries.
         * Pick next node, update offset, then re-run retrieval. */
        // 这里表示我们当前节点遍历完了，需要从下一个节点开始在进行遍历。
        // 先将当前节点压缩，然后再去迭代下一个节点。
        quicklistCompress(iter->quicklist, iter->current);
        if (iter->direction == AL_START_HEAD) {
            // 正向遍历，则跳到next节点，遍历的offset为0
            /* Forward traversal */
            D("Jumping to start of next node");
            iter->current = iter->current->next;
            iter->offset = 0;
        } else if (iter->direction == AL_START_TAIL) {
            // 逆向遍历，则跳到prev节点，遍历的offset为-1
            /* Reverse traversal */
            D("Jumping to end of previous node");
            iter->current = iter->current->prev;
            iter->offset = -1;
        }
        iter->zi = NULL;
        // 此时我们需要跳到下一个节点迭代，所以初始化完迭代器相关属性，再次调用quicklistNext进行处理。
        return quicklistNext(iter, entry);
    }
}

/* Duplicate the quicklist.
 * On success a copy of the original quicklist is returned.
 *
 * The original quicklist both on success or error is never modified.
 *
 * Returns newly allocated quicklist. */
// 复制quicklist，成功则返回新的复制的quicklist。函数无论成功或失败，原quicklist都不会有变更。
quicklist *quicklistDup(quicklist *orig) {
    quicklist *copy;

    // 使用原quicklist的fill、compress参数创建新的list。
    copy = quicklistNew(orig->fill, orig->compress);

    // 从头到尾遍历quicklist的节点进行copy处理。
    for (quicklistNode *current = orig->head; current;
         current = current->next) {
        quicklistNode *node = quicklistCreateNode();

        // 复制数据部分，根据是否压缩分开处理。因为都是连续的空间，所以都是先分配空间，然后memcpy来复制。
        if (current->encoding == QUICKLIST_NODE_ENCODING_LZF) {
            quicklistLZF *lzf = (quicklistLZF *)current->zl;
            size_t lzf_sz = sizeof(*lzf) + lzf->sz;
            node->zl = zmalloc(lzf_sz);
            memcpy(node->zl, current->zl, lzf_sz);
        } else if (current->encoding == QUICKLIST_NODE_ENCODING_RAW) {
            node->zl = zmalloc(current->sz);
            memcpy(node->zl, current->zl, current->sz);
        }

        // 复制节点的其他属性
        node->count = current->count;
        copy->count += node->count;
        node->sz = current->sz;
        node->encoding = current->encoding;

        // 将新复制的节点插入到copy的列表的尾部。
        _quicklistInsertNodeAfter(copy, copy->tail, node);
    }

    /* copy->count must equal orig->count here */
    // 返回copy的列表，注意复制列表的总元素数与原始列表的总元素数一致。
    return copy;
}

/* Populate 'entry' with the element at the specified zero-based index
 * where 0 is the head, 1 is the element next to head
 * and so on. Negative integers are used in order to count
 * from the tail, -1 is the last element, -2 the penultimate
 * and so on. If the index is out of range 0 is returned.
 *
 * Returns 1 if element found
 * Returns 0 if element not found */
// 使用指定idx的element元素数据，填充entry结构。quicklist中index从0开始处理，0表示head，-1表示tail。
// 如果找到element找到，返回1；否则index超出索引范围，没找到返回0。
int quicklistIndex(const quicklist *quicklist, const long long idx,
                   quicklistEntry *entry) {
    quicklistNode *n;
    unsigned long long accum = 0;
    unsigned long long index;
    // idx<0表示逆向，>=0表示正向
    int forward = idx < 0 ? 0 : 1; /* < 0 -> reverse, 0+ -> forward */

    // 初始化entry结构
    initEntry(entry);
    entry->quicklist = quicklist;

    if (!forward) {
        // 逆向遍历，更新index索引，设置初始节点为tail。
        index = (-idx) - 1;
        n = quicklist->tail;
    } else {
        // 正向遍历，待查询的index索引即为传入的idx，初始节点为head。
        index = idx;
        n = quicklist->head;
    }

    // 如果查询的index >= 总entry数，说明超出了范围，返回0。
    if (index >= quicklist->count)
        return 0;

    // 先遍历节点，根据每个节点的entry count数来定位index的节点位置。
    while (likely(n)) {
        if ((accum + n->count) > index) {
            // 如果遍历到当前节点时，总的entry数 > index，说明index位于该节点上。
            // 那么应该跳出循环，然后遍历该节点的ziplist获取index指定的元素。
            break;
        } else {
            D("Skipping over (%p) %u at accum %lld", (void *)n, n->count,
              accum);
            // 如果entry数 <= index，说明index不在该节点上，则跳过当前节点（按遍历顺序跳到下一个节点），并更新累计跳过节点的entry数。
            accum += n->count;
            n = forward ? n->next : n->prev;
        }
    }

    // 如果n为空，遍历到结尾了，没找到index位置，则返回0。理论上这里n不应该为空吧，因为前面我们判断了index不可能超出总entry数的。
    if (!n)
        return 0;

    D("Found node: %p at accum %llu, idx %llu, sub+ %llu, sub- %llu", (void *)n,
      accum, index, index - accum, (-index) - 1 + accum);

    // 找到了对应的节点，更新entry->node属性。
    entry->node = n;
    // 根据遍历顺序来计算元素在节点的ziplist中的offset，用于后面获取元素。
    if (forward) {
        /* forward = normal head-to-tail offset. */
        // 正常遍历，offset = index - accum，offser为当前ziplist中的entry偏移，也是从0开始的。
        entry->offset = index - accum;
    } else {
        /* reverse = need negative offset for tail-to-head, so undo
         * the result of the original if (index < 0) above. */
        // 逆向遍历，我们需要计算逆向的offset，为负数，在当前节点的ziplist从后往前（tail为-1）。
        entry->offset = (-index) - 1 + accum;
    }

    // 解压找到的节点用于查询。
    quicklistDecompressNodeForUse(entry->node);
    // 根据offset找到节点的ziplist中对应位置指针。
    entry->zi = ziplistIndex(entry->node->zl, entry->offset);
    // 获取ziplist指定指针位置处的元素数据。
    if (!ziplistGet(entry->zi, &entry->value, &entry->sz, &entry->longval))
        assert(0); /* This can happen on corrupt ziplist with fake entry count. */
    /* The caller will use our result, so we don't re-compress here.
     * The caller can recompress or delete the node as needed. */
    // 调用者将使用我们取到的entry数据，可能会使用ziplist，所以这里我们先不对节点重新压缩。后面调用方根据需要来重新压缩或者是删除节点。
    return 1;
}

/* Rotate quicklist by moving the tail element to the head. */
// 旋转quicklist，将尾部元素移到头部。
void quicklistRotate(quicklist *quicklist) {
    if (quicklist->count <= 1)
        return;

    /* First, get the tail entry */
    // 获取quicklist最后一个entry的位置。
    unsigned char *p = ziplistIndex(quicklist->tail->zl, -1);
    unsigned char *value, *tmp;
    long long longval;
    unsigned int sz;
    char longstr[32] = {0};
    // 获取最后一个元素的值
    ziplistGet(p, &tmp, &sz, &longval);

    /* If value found is NULL, then ziplistGet populated longval instead */
    // tmp非空表示值为字符串，否则值为数字
    if (!tmp) {
        /* Write the longval as a string so we can re-add it */
        // 最后一个元素值为数字longval，这里我们重新将它转为字符串格式，好后面重新加入quicklist头部。
        sz = ll2string(longstr, sizeof(longstr), longval);
        value = (unsigned char *)longstr;
    } else if (quicklist->len == 1) {
        /* Copy buffer since there could be a memory overlap when move
         * entity from tail to head in the same ziplist. */
        // 将tmp指向的数据复制出来。因为当quicklist只有一个节点时，我们是在处理同一个ziplist，会有内存重叠风险。
        // 扩容可能会产生新内存的ziplist，tmp指针可能会失效？另外如果在原地址扩容，ziplist数据统一向后移时，tmp指针可能指向错误数据？
        value = zmalloc(sz);
        memcpy(value, tmp, sz);
    } else {
        value = tmp;
    }

    /* Add tail entry to head (must happen before tail is deleted). */
    // 将获取到的tail元素插入到头部，必须要在删除tail元素之前，因为value可能还指向该元素，而这里还需要使用。
    quicklistPushHead(quicklist, value, sz);

    /* If quicklist has only one node, the head ziplist is also the
     * tail ziplist and PushHead() could have reallocated our single ziplist,
     * which would make our pre-existing 'p' unusable. */
    // head插入结束后，如果quicklist还是只有一个节点，可能前面获取到的最后一个节点的指针p已经失效了。
    // 所以我们这里要重新获取最后一个元素的位置，以便进行删除。
    if (quicklist->len == 1) {
        p = ziplistIndex(quicklist->tail->zl, -1);
    }

    /* Remove tail entry. */
    // 删除最后一个元素。
    quicklistDelIndex(quicklist, quicklist->tail, &p);
    // 释放前面分配的内存
    if (value != (unsigned char*)longstr && value != tmp)
        zfree(value);
}

/* pop from quicklist and return result in 'data' ptr.  Value of 'data'
 * is the return value of 'saver' function pointer if the data is NOT a number.
 *
 * If the quicklist element is a long long, then the return value is returned in
 * 'sval'.
 *
 * Return value of 0 means no elements available.
 * Return value of 1 means check 'data' and 'sval' for values.
 * If 'data' is set, use 'data' and 'sz'.  Otherwise, use 'sval'. */
// 从quicklist中pop元素。如果值是字符串类型，则通过saver函数封装后放到data字段返回；如果是数字类型，则直接解析成数字放到sval返回。
// 返回0表示没有元素可pop，返回1时调用方要检查data或sval来获取数据。如果data有值，则优先使用data和sz中数据；否则才使用sval。
int quicklistPopCustom(quicklist *quicklist, int where, unsigned char **data,
                       unsigned int *sz, long long *sval,
                       void *(*saver)(unsigned char *data, unsigned int sz)) {
    unsigned char *p;
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    // 根据pop位置来决定获取元素的offset
    int pos = (where == QUICKLIST_HEAD) ? 0 : -1;

    if (quicklist->count == 0)
        return 0;

    // 返回参数先设置默认值
    if (data)
        *data = NULL;
    if (sz)
        *sz = 0;
    if (sval)
        *sval = -123456789;

    quicklistNode *node;
    // 根据offset来确定查询数据的节点。
    if (where == QUICKLIST_HEAD && quicklist->head) {
        node = quicklist->head;
    } else if (where == QUICKLIST_TAIL && quicklist->tail) {
        node = quicklist->tail;
    } else {
        return 0;
    }

    // 使用offset获取指向entry的指针，并根据指针来获取数据
    p = ziplistIndex(node->zl, pos);
    if (ziplistGet(p, &vstr, &vlen, &vlong)) {
        // 成功获取到数据，根据数据是字符串还是数字来填充返回字段。
        if (vstr) {
            // 数据是字符串，调用saver对返回数据进行封装，然后填充data、sz字段返回。
            if (data)
                *data = saver(vstr, vlen);
            if (sz)
                *sz = vlen;
        } else {
            // 数据是数字，设置*data为NULL表示返回值不是字符串。
            if (data)
                *data = NULL;
            if (sval)
                *sval = vlong;
        }
        // pop元素，最后还需要删除该entry。
        quicklistDelIndex(quicklist, node, &p);
        return 1;
    }
    return 0;
}

/* Return a malloc'd copy of data passed in */
// 返回一个新的基于传入data的复制数据
REDIS_STATIC void *_quicklistSaver(unsigned char *data, unsigned int sz) {
    unsigned char *vstr;
    if (data) {
        // 如果有传入data，则分配空间，复制数据。
        vstr = zmalloc(sz);
        memcpy(vstr, data, sz);
        return vstr;
    }
    return NULL;
}

/* Default pop function
 *
 * Returns malloc'd value from quicklist */
// quicklist默认pop函数。
int quicklistPop(quicklist *quicklist, int where, unsigned char **data,
                 unsigned int *sz, long long *slong) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    if (quicklist->count == 0)
        return 0;
    // pop处理，使用_quicklistSaver对pop得到的字符串进行copy返回。
    int ret = quicklistPopCustom(quicklist, where, &vstr, &vlen, &vlong,
                                 _quicklistSaver);
    if (data)
        *data = vstr;
    if (slong)
        *slong = vlong;
    if (sz)
        *sz = vlen;
    return ret;
}

/* Wrapper to allow argument-based switching between HEAD/TAIL push */
// quicklist push函数的封装，可根据where参数选择是在head/tail操作。
void quicklistPush(quicklist *quicklist, void *value, const size_t sz,
                   int where) {
    if (where == QUICKLIST_HEAD) {
        quicklistPushHead(quicklist, value, sz);
    } else if (where == QUICKLIST_TAIL) {
        quicklistPushTail(quicklist, value, sz);
    }
}

/* Create or update a bookmark in the list which will be updated to the next node
 * automatically when the one referenced gets deleted.
 * Returns 1 on success (creation of new bookmark or override of an existing one).
 * Returns 0 on failure (reached the maximum supported number of bookmarks).
 * NOTE: use short simple names, so that string compare on find is quick.
 * NOTE: bookmark creation may re-allocate the quicklist, so the input pointer
         may change and it's the caller responsibilty to update the reference.
 */
// 创建或更新list中的bookmark。当该bookmark对应的节点被删除时，它会自动的指向下一个节点，如果原本就是最后一个节点，则该bookmark会被移除。
int quicklistBookmarkCreate(quicklist **ql_ref, const char *name, quicklistNode *node) {
    quicklist *ql = *ql_ref;
    // 如果创建的bookmark超过了最大数量限制，则不能在增加了。
    if (ql->bookmark_count >= QL_MAX_BM)
        return 0;
    // 从原有的bookmarks中根据name查询，如果有查到则表示更新，所以这里更新该bookmark对应的节点。
    quicklistBookmark *bm = _quicklistBookmarkFindByName(ql, name);
    if (bm) {
        bm->node = node;
        return 1;
    }
    // 没查到，表示需要创建，这里先realloc quicklist（对应的也就重新分配了bookmarks内存），然后在最后填入新节点的bookmark。
    ql = zrealloc(ql, sizeof(quicklist) + (ql->bookmark_count+1) * sizeof(quicklistBookmark));
    // 因为realloc可能导致quicklist的指针变了，所以这里需要再赋值给*ql_ref。
    *ql_ref = ql;
    ql->bookmarks[ql->bookmark_count].node = node;
    ql->bookmarks[ql->bookmark_count].name = zstrdup(name);
    ql->bookmark_count++;
    return 1;
}

/* Find the quicklist node referenced by a named bookmark.
 * When the bookmarked node is deleted the bookmark is updated to the next node,
 * and if that's the last node, the bookmark is deleted (so find returns NULL). */
// 通过name来查询bookmark中对应的节点。没查询到，可能是被删除了，会返回NULL。
// 当该bookmark对应的节点被删除时，它会自动的指向下一个节点，如果原本就是最后一个节点，则该bookmark会被移除。
quicklistNode *quicklistBookmarkFind(quicklist *ql, const char *name) {
    quicklistBookmark *bm = _quicklistBookmarkFindByName(ql, name);
    if (!bm) return NULL;
    return bm->node;
}

/* Delete a named bookmark.
 * returns 0 if bookmark was not found, and 1 if deleted.
 * Note that the bookmark memory is not freed yet, and is kept for future use. */
// 删除一个命名的bookmark。如果没找到对应的bookmark，则返回0；否则找到并删除了，返回1。
// 注意这里删除并没有释放内存，保留后面再使用。
int quicklistBookmarkDelete(quicklist *ql, const char *name) {
    quicklistBookmark *bm = _quicklistBookmarkFindByName(ql, name);
    if (!bm)
        return 0;
    _quicklistBookmarkDelete(ql, bm);
    return 1;
}

// 真正通过name查询bookmark的逻辑。遍历bookmarks数组，挨个对比name。
quicklistBookmark *_quicklistBookmarkFindByName(quicklist *ql, const char *name) {
    unsigned i;
    for (i=0; i<ql->bookmark_count; i++) {
        if (!strcmp(ql->bookmarks[i].name, name)) {
            return &ql->bookmarks[i];
        }
    }
    return NULL;
}

// 真正通过node来查询bookmark的逻辑。遍历bookmarks数组，挨个对比node指针。
// 这个函数目前主要用于在删除节点时，通过node查询bookmark，然后如果该node有next节点，则更新bookmark指向next节点，否则删除该bookmark。
quicklistBookmark *_quicklistBookmarkFindByNode(quicklist *ql, quicklistNode *node) {
    unsigned i;
    for (i=0; i<ql->bookmark_count; i++) {
        if (ql->bookmarks[i].node == node) {
            return &ql->bookmarks[i];
        }
    }
    return NULL;
}

// 删除指定的bookmark。
void _quicklistBookmarkDelete(quicklist *ql, quicklistBookmark *bm) {
    int index = bm - ql->bookmarks;
    zfree(bm->name);
    ql->bookmark_count--;
    // 连续内存存储的，所以直接内存复制覆盖。
    memmove(bm, bm+1, (ql->bookmark_count - index)* sizeof(*bm));
    /* NOTE: We do not shrink (realloc) the quicklist yet (to avoid resonance,
     * it may be re-used later (a call to realloc may NOP). */
    // 这里我们不进行realloc来释放空间，因为后面可以会被重新使用，避免来回进行realloc。
}

// 清空bookmarks。这里我们也不单独进行realloc释放空间，因为通常调用这个函数是在释放整个quicklist的时候，所以这里不需要单独处理。
void quicklistBookmarksClear(quicklist *ql) {
    while (ql->bookmark_count)
        zfree(ql->bookmarks[--ql->bookmark_count].name);
    /* NOTE: We do not shrink (realloc) the quick list. main use case for this
     * function is just before releasing the allocation. */
}

/* The rest of this file is test cases and test helpers. */
#ifdef REDIS_TEST
#include <stdint.h>
#include <sys/time.h>

#define yell(str, ...) printf("ERROR! " str "\n\n", __VA_ARGS__)

#define ERROR                                                                  \
    do {                                                                       \
        printf("\tERROR!\n");                                                  \
        err++;                                                                 \
    } while (0)

#define ERR(x, ...)                                                            \
    do {                                                                       \
        printf("%s:%s:%d:\t", __FILE__, __func__, __LINE__);                   \
        printf("ERROR! " x "\n", __VA_ARGS__);                                 \
        err++;                                                                 \
    } while (0)

#define TEST(name) printf("test — %s\n", name);
#define TEST_DESC(name, ...) printf("test — " name "\n", __VA_ARGS__);

#define QL_TEST_VERBOSE 0

#define UNUSED(x) (void)(x)
static void ql_info(quicklist *ql) {
#if QL_TEST_VERBOSE
    printf("Container length: %lu\n", ql->len);
    printf("Container size: %lu\n", ql->count);
    if (ql->head)
        printf("\t(zsize head: %d)\n", ziplistLen(ql->head->zl));
    if (ql->tail)
        printf("\t(zsize tail: %d)\n", ziplistLen(ql->tail->zl));
    printf("\n");
#else
    UNUSED(ql);
#endif
}

/* Return the UNIX time in microseconds */
static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec) * 1000000;
    ust += tv.tv_usec;
    return ust;
}

/* Return the UNIX time in milliseconds */
static long long mstime(void) { return ustime() / 1000; }

/* Iterate over an entire quicklist.
 * Print the list if 'print' == 1.
 *
 * Returns physical count of elements found by iterating over the list. */
static int _itrprintr(quicklist *ql, int print, int forward) {
    quicklistIter *iter =
        quicklistGetIterator(ql, forward ? AL_START_HEAD : AL_START_TAIL);
    quicklistEntry entry;
    int i = 0;
    int p = 0;
    quicklistNode *prev = NULL;
    while (quicklistNext(iter, &entry)) {
        if (entry.node != prev) {
            /* Count the number of list nodes too */
            p++;
            prev = entry.node;
        }
        if (print) {
            printf("[%3d (%2d)]: [%.*s] (%lld)\n", i, p, entry.sz,
                   (char *)entry.value, entry.longval);
        }
        i++;
    }
    quicklistReleaseIterator(iter);
    return i;
}
static int itrprintr(quicklist *ql, int print) {
    return _itrprintr(ql, print, 1);
}

static int itrprintr_rev(quicklist *ql, int print) {
    return _itrprintr(ql, print, 0);
}

#define ql_verify(a, b, c, d, e)                                               \
    do {                                                                       \
        err += _ql_verify((a), (b), (c), (d), (e));                            \
    } while (0)

/* Verify list metadata matches physical list contents. */
static int _ql_verify(quicklist *ql, uint32_t len, uint32_t count,
                      uint32_t head_count, uint32_t tail_count) {
    int errors = 0;

    ql_info(ql);
    if (len != ql->len) {
        yell("quicklist length wrong: expected %d, got %lu", len, ql->len);
        errors++;
    }

    if (count != ql->count) {
        yell("quicklist count wrong: expected %d, got %lu", count, ql->count);
        errors++;
    }

    int loopr = itrprintr(ql, 0);
    if (loopr != (int)ql->count) {
        yell("quicklist cached count not match actual count: expected %lu, got "
             "%d",
             ql->count, loopr);
        errors++;
    }

    int rloopr = itrprintr_rev(ql, 0);
    if (loopr != rloopr) {
        yell("quicklist has different forward count than reverse count!  "
             "Forward count is %d, reverse count is %d.",
             loopr, rloopr);
        errors++;
    }

    if (ql->len == 0 && !errors) {
        return errors;
    }

    if (ql->head && head_count != ql->head->count &&
        head_count != ziplistLen(ql->head->zl)) {
        yell("quicklist head count wrong: expected %d, "
             "got cached %d vs. actual %d",
             head_count, ql->head->count, ziplistLen(ql->head->zl));
        errors++;
    }

    if (ql->tail && tail_count != ql->tail->count &&
        tail_count != ziplistLen(ql->tail->zl)) {
        yell("quicklist tail count wrong: expected %d, "
             "got cached %u vs. actual %d",
             tail_count, ql->tail->count, ziplistLen(ql->tail->zl));
        errors++;
    }

    if (quicklistAllowsCompression(ql)) {
        quicklistNode *node = ql->head;
        unsigned int low_raw = ql->compress;
        unsigned int high_raw = ql->len - ql->compress;

        for (unsigned int at = 0; at < ql->len; at++, node = node->next) {
            if (node && (at < low_raw || at >= high_raw)) {
                if (node->encoding != QUICKLIST_NODE_ENCODING_RAW) {
                    yell("Incorrect compression: node %d is "
                         "compressed at depth %d ((%u, %u); total "
                         "nodes: %lu; size: %u; recompress: %d)",
                         at, ql->compress, low_raw, high_raw, ql->len, node->sz,
                         node->recompress);
                    errors++;
                }
            } else {
                if (node->encoding != QUICKLIST_NODE_ENCODING_LZF &&
                    !node->attempted_compress) {
                    yell("Incorrect non-compression: node %d is NOT "
                         "compressed at depth %d ((%u, %u); total "
                         "nodes: %lu; size: %u; recompress: %d; attempted: %d)",
                         at, ql->compress, low_raw, high_raw, ql->len, node->sz,
                         node->recompress, node->attempted_compress);
                    errors++;
                }
            }
        }
    }

    return errors;
}

/* Generate new string concatenating integer i against string 'prefix' */
static char *genstr(char *prefix, int i) {
    static char result[64] = {0};
    snprintf(result, sizeof(result), "%s%d", prefix, i);
    return result;
}

/* main test, but callable from other files */
int quicklistTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);

    unsigned int err = 0;
    int optimize_start =
        -(int)(sizeof(optimization_level) / sizeof(*optimization_level));

    printf("Starting optimization offset at: %d\n", optimize_start);

    int options[] = {0, 1, 2, 3, 4, 5, 6, 10};
    int fills[] = {-5, -4, -3, -2, -1, 0,
                   1, 2, 32, 66, 128, 999};
    size_t option_count = sizeof(options) / sizeof(*options);
    int fill_count = (int)(sizeof(fills) / sizeof(*fills));
    long long runtime[option_count];

    for (int _i = 0; _i < (int)option_count; _i++) {
        printf("Testing Compression option %d\n", options[_i]);
        long long start = mstime();

        TEST("create list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("add to tail of empty list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushTail(ql, "hello", 6);
            /* 1 for head and 1 for tail because 1 node = head = tail */
            ql_verify(ql, 1, 1, 1, 1);
            quicklistRelease(ql);
        }

        TEST("add to head of empty list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "hello", 6);
            /* 1 for head and 1 for tail because 1 node = head = tail */
            ql_verify(ql, 1, 1, 1, 1);
            quicklistRelease(ql);
        }

        TEST_DESC("add to tail 5x at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 5; i++)
                    quicklistPushTail(ql, genstr("hello", i), 32);
                if (ql->count != 5)
                    ERROR;
                if (fills[f] == 32)
                    ql_verify(ql, 1, 5, 5, 5);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("add to head 5x at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 5; i++)
                    quicklistPushHead(ql, genstr("hello", i), 32);
                if (ql->count != 5)
                    ERROR;
                if (fills[f] == 32)
                    ql_verify(ql, 1, 5, 5, 5);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("add to tail 500x at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i), 64);
                if (ql->count != 500)
                    ERROR;
                if (fills[f] == 32)
                    ql_verify(ql, 16, 500, 32, 20);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("add to head 500x at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushHead(ql, genstr("hello", i), 32);
                if (ql->count != 500)
                    ERROR;
                if (fills[f] == 32)
                    ql_verify(ql, 16, 500, 20, 32);
                quicklistRelease(ql);
            }
        }

        TEST("rotate empty") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistRotate(ql);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("rotate one val once") {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                quicklistPushHead(ql, "hello", 6);
                quicklistRotate(ql);
                /* Ignore compression verify because ziplist is
                 * too small to compress. */
                ql_verify(ql, 1, 1, 1, 1);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("rotate 500 val 5000 times at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                quicklistPushHead(ql, "900", 3);
                quicklistPushHead(ql, "7000", 4);
                quicklistPushHead(ql, "-1200", 5);
                quicklistPushHead(ql, "42", 2);
                for (int i = 0; i < 500; i++)
                    quicklistPushHead(ql, genstr("hello", i), 64);
                ql_info(ql);
                for (int i = 0; i < 5000; i++) {
                    ql_info(ql);
                    quicklistRotate(ql);
                }
                if (fills[f] == 1)
                    ql_verify(ql, 504, 504, 1, 1);
                else if (fills[f] == 2)
                    ql_verify(ql, 252, 504, 2, 2);
                else if (fills[f] == 32)
                    ql_verify(ql, 16, 504, 32, 24);
                quicklistRelease(ql);
            }
        }

        TEST("pop empty") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPop(ql, QUICKLIST_HEAD, NULL, NULL, NULL);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop 1 string from 1") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            char *populate = genstr("hello", 331);
            quicklistPushHead(ql, populate, 32);
            unsigned char *data;
            unsigned int sz;
            long long lv;
            ql_info(ql);
            quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
            assert(data != NULL);
            assert(sz == 32);
            if (strcmp(populate, (char *)data))
                ERR("Pop'd value (%.*s) didn't equal original value (%s)", sz,
                    data, populate);
            zfree(data);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop head 1 number from 1") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "55513", 5);
            unsigned char *data;
            unsigned int sz;
            long long lv;
            ql_info(ql);
            quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
            assert(data == NULL);
            assert(lv == 55513);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop head 500 from 500") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_info(ql);
            for (int i = 0; i < 500; i++) {
                unsigned char *data;
                unsigned int sz;
                long long lv;
                int ret = quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
                assert(ret == 1);
                assert(data != NULL);
                assert(sz == 32);
                if (strcmp(genstr("hello", 499 - i), (char *)data))
                    ERR("Pop'd value (%.*s) didn't equal original value (%s)",
                        sz, data, genstr("hello", 499 - i));
                zfree(data);
            }
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop head 5000 from 500") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            for (int i = 0; i < 5000; i++) {
                unsigned char *data;
                unsigned int sz;
                long long lv;
                int ret = quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
                if (i < 500) {
                    assert(ret == 1);
                    assert(data != NULL);
                    assert(sz == 32);
                    if (strcmp(genstr("hello", 499 - i), (char *)data))
                        ERR("Pop'd value (%.*s) didn't equal original value "
                            "(%s)",
                            sz, data, genstr("hello", 499 - i));
                    zfree(data);
                } else {
                    assert(ret == 0);
                }
            }
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("iterate forward over 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            quicklistIter *iter = quicklistGetIterator(ql, AL_START_HEAD);
            quicklistEntry entry;
            int i = 499, count = 0;
            while (quicklistNext(iter, &entry)) {
                char *h = genstr("hello", i);
                if (strcmp((char *)entry.value, h))
                    ERR("value [%s] didn't match [%s] at position %d",
                        entry.value, h, i);
                i--;
                count++;
            }
            if (count != 500)
                ERR("Didn't iterate over exactly 500 elements (%d)", i);
            ql_verify(ql, 16, 500, 20, 32);
            quicklistReleaseIterator(iter);
            quicklistRelease(ql);
        }

        TEST("iterate reverse over 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            quicklistIter *iter = quicklistGetIterator(ql, AL_START_TAIL);
            quicklistEntry entry;
            int i = 0;
            while (quicklistNext(iter, &entry)) {
                char *h = genstr("hello", i);
                if (strcmp((char *)entry.value, h))
                    ERR("value [%s] didn't match [%s] at position %d",
                        entry.value, h, i);
                i++;
            }
            if (i != 500)
                ERR("Didn't iterate over exactly 500 elements (%d)", i);
            ql_verify(ql, 16, 500, 20, 32);
            quicklistReleaseIterator(iter);
            quicklistRelease(ql);
        }

        TEST("insert before with 0 elements") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            quicklistInsertBefore(ql, &entry, "abc", 4);
            ql_verify(ql, 1, 1, 1, 1);
            quicklistRelease(ql);
        }

        TEST("insert after with 0 elements") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            quicklistInsertAfter(ql, &entry, "abc", 4);
            ql_verify(ql, 1, 1, 1, 1);
            quicklistRelease(ql);
        }

        TEST("insert after 1 element") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "hello", 6);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            quicklistInsertAfter(ql, &entry, "abc", 4);
            ql_verify(ql, 1, 2, 2, 2);
            quicklistRelease(ql);
        }

        TEST("insert before 1 element") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "hello", 6);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            quicklistInsertAfter(ql, &entry, "abc", 4);
            ql_verify(ql, 1, 2, 2, 2);
            quicklistRelease(ql);
        }

        TEST_DESC("insert once in elements while iterating at compress %d",
                  options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                quicklistPushTail(ql, "abc", 3);
                quicklistSetFill(ql, 1);
                quicklistPushTail(ql, "def", 3); /* force to unique node */
                quicklistSetFill(ql, f);
                quicklistPushTail(ql, "bob", 3); /* force to reset for +3 */
                quicklistPushTail(ql, "foo", 3);
                quicklistPushTail(ql, "zoo", 3);

                itrprintr(ql, 0);
                /* insert "bar" before "bob" while iterating over list. */
                quicklistIter *iter = quicklistGetIterator(ql, AL_START_HEAD);
                quicklistEntry entry;
                while (quicklistNext(iter, &entry)) {
                    if (!strncmp((char *)entry.value, "bob", 3)) {
                        /* Insert as fill = 1 so it spills into new node. */
                        quicklistInsertBefore(ql, &entry, "bar", 3);
                        break; /* didn't we fix insert-while-iterating? */
                    }
                }
                itrprintr(ql, 0);

                /* verify results */
                quicklistIndex(ql, 0, &entry);
                if (strncmp((char *)entry.value, "abc", 3))
                    ERR("Value 0 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 1, &entry);
                if (strncmp((char *)entry.value, "def", 3))
                    ERR("Value 1 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 2, &entry);
                if (strncmp((char *)entry.value, "bar", 3))
                    ERR("Value 2 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 3, &entry);
                if (strncmp((char *)entry.value, "bob", 3))
                    ERR("Value 3 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 4, &entry);
                if (strncmp((char *)entry.value, "foo", 3))
                    ERR("Value 4 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 5, &entry);
                if (strncmp((char *)entry.value, "zoo", 3))
                    ERR("Value 5 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistReleaseIterator(iter);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("insert [before] 250 new in middle of 500 elements at compress %d",
                  options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i), 32);
                for (int i = 0; i < 250; i++) {
                    quicklistEntry entry;
                    quicklistIndex(ql, 250, &entry);
                    quicklistInsertBefore(ql, &entry, genstr("abc", i), 32);
                }
                if (fills[f] == 32)
                    ql_verify(ql, 25, 750, 32, 20);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("insert [after] 250 new in middle of 500 elements at compress %d",
                  options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushHead(ql, genstr("hello", i), 32);
                for (int i = 0; i < 250; i++) {
                    quicklistEntry entry;
                    quicklistIndex(ql, 250, &entry);
                    quicklistInsertAfter(ql, &entry, genstr("abc", i), 32);
                }

                if (ql->count != 750)
                    ERR("List size not 750, but rather %ld", ql->count);

                if (fills[f] == 32)
                    ql_verify(ql, 26, 750, 20, 32);
                quicklistRelease(ql);
            }
        }

        TEST("duplicate empty list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            ql_verify(ql, 0, 0, 0, 0);
            quicklist *copy = quicklistDup(ql);
            ql_verify(copy, 0, 0, 0, 0);
            quicklistRelease(ql);
            quicklistRelease(copy);
        }

        TEST("duplicate list of 1 element") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, genstr("hello", 3), 32);
            ql_verify(ql, 1, 1, 1, 1);
            quicklist *copy = quicklistDup(ql);
            ql_verify(copy, 1, 1, 1, 1);
            quicklistRelease(ql);
            quicklistRelease(copy);
        }

        TEST("duplicate list of 500") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_verify(ql, 16, 500, 20, 32);

            quicklist *copy = quicklistDup(ql);
            ql_verify(copy, 16, 500, 20, 32);
            quicklistRelease(ql);
            quicklistRelease(copy);
        }

        for (int f = 0; f < fill_count; f++) {
            TEST_DESC("index 1,200 from 500 list at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                quicklistIndex(ql, 1, &entry);
                if (strcmp((char *)entry.value, "hello2") != 0)
                    ERR("Value: %s", entry.value);
                quicklistIndex(ql, 200, &entry);
                if (strcmp((char *)entry.value, "hello201") != 0)
                    ERR("Value: %s", entry.value);
                quicklistRelease(ql);
            }

            TEST_DESC("index -1,-2 from 500 list at fill %d at compress %d",
                      fills[f], options[_i]) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                quicklistIndex(ql, -1, &entry);
                if (strcmp((char *)entry.value, "hello500") != 0)
                    ERR("Value: %s", entry.value);
                quicklistIndex(ql, -2, &entry);
                if (strcmp((char *)entry.value, "hello499") != 0)
                    ERR("Value: %s", entry.value);
                quicklistRelease(ql);
            }

            TEST_DESC("index -100 from 500 list at fill %d at compress %d",
                      fills[f], options[_i]) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                quicklistIndex(ql, -100, &entry);
                if (strcmp((char *)entry.value, "hello401") != 0)
                    ERR("Value: %s", entry.value);
                quicklistRelease(ql);
            }

            TEST_DESC("index too big +1 from 50 list at fill %d at compress %d",
                      fills[f], options[_i]) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                for (int i = 0; i < 50; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                if (quicklistIndex(ql, 50, &entry))
                    ERR("Index found at 50 with 50 list: %.*s", entry.sz,
                        entry.value);
                quicklistRelease(ql);
            }
        }

        TEST("delete range empty list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistDelRange(ql, 5, 20);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("delete range of entire node in list of one node") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 32; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_verify(ql, 1, 32, 32, 32);
            quicklistDelRange(ql, 0, 32);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("delete range of entire node with overflow counts") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 32; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_verify(ql, 1, 32, 32, 32);
            quicklistDelRange(ql, 0, 128);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("delete middle 100 of 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 16, 500, 32, 20);
            quicklistDelRange(ql, 200, 100);
            ql_verify(ql, 14, 400, 32, 20);
            quicklistRelease(ql);
        }

        TEST("delete less than fill but across nodes") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 16, 500, 32, 20);
            quicklistDelRange(ql, 60, 10);
            ql_verify(ql, 16, 490, 32, 20);
            quicklistRelease(ql);
        }

        TEST("delete negative 1 from 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 16, 500, 32, 20);
            quicklistDelRange(ql, -1, 1);
            ql_verify(ql, 16, 499, 32, 19);
            quicklistRelease(ql);
        }

        TEST("delete negative 1 from 500 list with overflow counts") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 16, 500, 32, 20);
            quicklistDelRange(ql, -1, 128);
            ql_verify(ql, 16, 499, 32, 19);
            quicklistRelease(ql);
        }

        TEST("delete negative 100 from 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            quicklistDelRange(ql, -100, 100);
            ql_verify(ql, 13, 400, 32, 16);
            quicklistRelease(ql);
        }

        TEST("delete -10 count 5 from 50 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 50; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 2, 50, 32, 18);
            quicklistDelRange(ql, -10, 5);
            ql_verify(ql, 2, 45, 32, 13);
            quicklistRelease(ql);
        }

        TEST("numbers only list read") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushTail(ql, "1111", 4);
            quicklistPushTail(ql, "2222", 4);
            quicklistPushTail(ql, "3333", 4);
            quicklistPushTail(ql, "4444", 4);
            ql_verify(ql, 1, 4, 4, 4);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            if (entry.longval != 1111)
                ERR("Not 1111, %lld", entry.longval);
            quicklistIndex(ql, 1, &entry);
            if (entry.longval != 2222)
                ERR("Not 2222, %lld", entry.longval);
            quicklistIndex(ql, 2, &entry);
            if (entry.longval != 3333)
                ERR("Not 3333, %lld", entry.longval);
            quicklistIndex(ql, 3, &entry);
            if (entry.longval != 4444)
                ERR("Not 4444, %lld", entry.longval);
            if (quicklistIndex(ql, 4, &entry))
                ERR("Index past elements: %lld", entry.longval);
            quicklistIndex(ql, -1, &entry);
            if (entry.longval != 4444)
                ERR("Not 4444 (reverse), %lld", entry.longval);
            quicklistIndex(ql, -2, &entry);
            if (entry.longval != 3333)
                ERR("Not 3333 (reverse), %lld", entry.longval);
            quicklistIndex(ql, -3, &entry);
            if (entry.longval != 2222)
                ERR("Not 2222 (reverse), %lld", entry.longval);
            quicklistIndex(ql, -4, &entry);
            if (entry.longval != 1111)
                ERR("Not 1111 (reverse), %lld", entry.longval);
            if (quicklistIndex(ql, -5, &entry))
                ERR("Index past elements (reverse), %lld", entry.longval);
            quicklistRelease(ql);
        }

        TEST("numbers larger list read") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            char num[32];
            long long nums[5000];
            for (int i = 0; i < 5000; i++) {
                nums[i] = -5157318210846258176 + i;
                int sz = ll2string(num, sizeof(num), nums[i]);
                quicklistPushTail(ql, num, sz);
            }
            quicklistPushTail(ql, "xxxxxxxxxxxxxxxxxxxx", 20);
            quicklistEntry entry;
            for (int i = 0; i < 5000; i++) {
                quicklistIndex(ql, i, &entry);
                if (entry.longval != nums[i])
                    ERR("[%d] Not longval %lld but rather %lld", i, nums[i],
                        entry.longval);
                entry.longval = 0xdeadbeef;
            }
            quicklistIndex(ql, 5000, &entry);
            if (strncmp((char *)entry.value, "xxxxxxxxxxxxxxxxxxxx", 20))
                ERR("String val not match: %s", entry.value);
            ql_verify(ql, 157, 5001, 32, 9);
            quicklistRelease(ql);
        }

        TEST("numbers larger list read B") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushTail(ql, "99", 2);
            quicklistPushTail(ql, "98", 2);
            quicklistPushTail(ql, "xxxxxxxxxxxxxxxxxxxx", 20);
            quicklistPushTail(ql, "96", 2);
            quicklistPushTail(ql, "95", 2);
            quicklistReplaceAtIndex(ql, 1, "foo", 3);
            quicklistReplaceAtIndex(ql, -1, "bar", 3);
            quicklistRelease(ql);
        }

        TEST_DESC("lrem test at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                char *words[] = {"abc", "foo", "bar",  "foobar", "foobared",
                                 "zap", "bar", "test", "foo"};
                char *result[] = {"abc", "foo",  "foobar", "foobared",
                                  "zap", "test", "foo"};
                char *resultB[] = {"abc",      "foo", "foobar",
                                   "foobared", "zap", "test"};
                for (int i = 0; i < 9; i++)
                    quicklistPushTail(ql, words[i], strlen(words[i]));

                /* lrem 0 bar */
                quicklistIter *iter = quicklistGetIterator(ql, AL_START_HEAD);
                quicklistEntry entry;
                int i = 0;
                while (quicklistNext(iter, &entry)) {
                    if (quicklistCompare(entry.zi, (unsigned char *)"bar", 3)) {
                        quicklistDelEntry(iter, &entry);
                    }
                    i++;
                }
                quicklistReleaseIterator(iter);

                /* check result of lrem 0 bar */
                iter = quicklistGetIterator(ql, AL_START_HEAD);
                i = 0;
                while (quicklistNext(iter, &entry)) {
                    /* Result must be: abc, foo, foobar, foobared, zap, test,
                     * foo */
                    if (strncmp((char *)entry.value, result[i], entry.sz)) {
                        ERR("No match at position %d, got %.*s instead of %s",
                            i, entry.sz, entry.value, result[i]);
                    }
                    i++;
                }
                quicklistReleaseIterator(iter);

                quicklistPushTail(ql, "foo", 3);

                /* lrem -2 foo */
                iter = quicklistGetIterator(ql, AL_START_TAIL);
                i = 0;
                int del = 2;
                while (quicklistNext(iter, &entry)) {
                    if (quicklistCompare(entry.zi, (unsigned char *)"foo", 3)) {
                        quicklistDelEntry(iter, &entry);
                        del--;
                    }
                    if (!del)
                        break;
                    i++;
                }
                quicklistReleaseIterator(iter);

                /* check result of lrem -2 foo */
                /* (we're ignoring the '2' part and still deleting all foo
                 * because
                 * we only have two foo) */
                iter = quicklistGetIterator(ql, AL_START_TAIL);
                i = 0;
                size_t resB = sizeof(resultB) / sizeof(*resultB);
                while (quicklistNext(iter, &entry)) {
                    /* Result must be: abc, foo, foobar, foobared, zap, test,
                     * foo */
                    if (strncmp((char *)entry.value, resultB[resB - 1 - i],
                                entry.sz)) {
                        ERR("No match at position %d, got %.*s instead of %s",
                            i, entry.sz, entry.value, resultB[resB - 1 - i]);
                    }
                    i++;
                }

                quicklistReleaseIterator(iter);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("iterate reverse + delete at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                quicklistPushTail(ql, "abc", 3);
                quicklistPushTail(ql, "def", 3);
                quicklistPushTail(ql, "hij", 3);
                quicklistPushTail(ql, "jkl", 3);
                quicklistPushTail(ql, "oop", 3);

                quicklistEntry entry;
                quicklistIter *iter = quicklistGetIterator(ql, AL_START_TAIL);
                int i = 0;
                while (quicklistNext(iter, &entry)) {
                    if (quicklistCompare(entry.zi, (unsigned char *)"hij", 3)) {
                        quicklistDelEntry(iter, &entry);
                    }
                    i++;
                }
                quicklistReleaseIterator(iter);

                if (i != 5)
                    ERR("Didn't iterate 5 times, iterated %d times.", i);

                /* Check results after deletion of "hij" */
                iter = quicklistGetIterator(ql, AL_START_HEAD);
                i = 0;
                char *vals[] = {"abc", "def", "jkl", "oop"};
                while (quicklistNext(iter, &entry)) {
                    if (!quicklistCompare(entry.zi, (unsigned char *)vals[i],
                                          3)) {
                        ERR("Value at %d didn't match %s\n", i, vals[i]);
                    }
                    i++;
                }
                quicklistReleaseIterator(iter);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("iterator at index test at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 760; i++) {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }

                quicklistEntry entry;
                quicklistIter *iter =
                    quicklistGetIteratorAtIdx(ql, AL_START_HEAD, 437);
                int i = 437;
                while (quicklistNext(iter, &entry)) {
                    if (entry.longval != nums[i])
                        ERR("Expected %lld, but got %lld", entry.longval,
                            nums[i]);
                    i++;
                }
                quicklistReleaseIterator(iter);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("ltrim test A at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 32; i++) {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (fills[f] == 32)
                    ql_verify(ql, 1, 32, 32, 32);
                /* ltrim 25 53 (keep [25,32] inclusive = 7 remaining) */
                quicklistDelRange(ql, 0, 25);
                quicklistDelRange(ql, 0, 0);
                quicklistEntry entry;
                for (int i = 0; i < 7; i++) {
                    quicklistIndex(ql, i, &entry);
                    if (entry.longval != nums[25 + i])
                        ERR("Deleted invalid range!  Expected %lld but got "
                            "%lld",
                            entry.longval, nums[25 + i]);
                }
                if (fills[f] == 32)
                    ql_verify(ql, 1, 7, 7, 7);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("ltrim test B at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                /* Force-disable compression because our 33 sequential
                 * integers don't compress and the check always fails. */
                quicklist *ql = quicklistNew(fills[f], QUICKLIST_NOCOMPRESS);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 33; i++) {
                    nums[i] = i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (fills[f] == 32)
                    ql_verify(ql, 2, 33, 32, 1);
                /* ltrim 5 16 (keep [5,16] inclusive = 12 remaining) */
                quicklistDelRange(ql, 0, 5);
                quicklistDelRange(ql, -16, 16);
                if (fills[f] == 32)
                    ql_verify(ql, 1, 12, 12, 12);
                quicklistEntry entry;
                quicklistIndex(ql, 0, &entry);
                if (entry.longval != 5)
                    ERR("A: longval not 5, but %lld", entry.longval);
                quicklistIndex(ql, -1, &entry);
                if (entry.longval != 16)
                    ERR("B! got instead: %lld", entry.longval);
                quicklistPushTail(ql, "bobobob", 7);
                quicklistIndex(ql, -1, &entry);
                if (strncmp((char *)entry.value, "bobobob", 7))
                    ERR("Tail doesn't match bobobob, it's %.*s instead",
                        entry.sz, entry.value);
                for (int i = 0; i < 12; i++) {
                    quicklistIndex(ql, i, &entry);
                    if (entry.longval != nums[5 + i])
                        ERR("Deleted invalid range!  Expected %lld but got "
                            "%lld",
                            entry.longval, nums[5 + i]);
                }
                quicklistRelease(ql);
            }
        }

        TEST_DESC("ltrim test C at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 33; i++) {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (fills[f] == 32)
                    ql_verify(ql, 2, 33, 32, 1);
                /* ltrim 3 3 (keep [3,3] inclusive = 1 remaining) */
                quicklistDelRange(ql, 0, 3);
                quicklistDelRange(ql, -29,
                                  4000); /* make sure not loop forever */
                if (fills[f] == 32)
                    ql_verify(ql, 1, 1, 1, 1);
                quicklistEntry entry;
                quicklistIndex(ql, 0, &entry);
                if (entry.longval != -5157318210846258173)
                    ERROR;
                quicklistRelease(ql);
            }
        }

        TEST_DESC("ltrim test D at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                quicklist *ql = quicklistNew(fills[f], options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 33; i++) {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (fills[f] == 32)
                    ql_verify(ql, 2, 33, 32, 1);
                quicklistDelRange(ql, -12, 3);
                if (ql->count != 30)
                    ERR("Didn't delete exactly three elements!  Count is: %lu",
                        ql->count);
                quicklistRelease(ql);
            }
        }

        TEST_DESC("create quicklist from ziplist at compress %d", options[_i]) {
            for (int f = 0; f < fill_count; f++) {
                unsigned char *zl = ziplistNew();
                long long nums[64];
                char num[64];
                for (int i = 0; i < 33; i++) {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    zl =
                        ziplistPush(zl, (unsigned char *)num, sz, ZIPLIST_TAIL);
                }
                for (int i = 0; i < 33; i++) {
                    zl = ziplistPush(zl, (unsigned char *)genstr("hello", i),
                                     32, ZIPLIST_TAIL);
                }
                quicklist *ql = quicklistCreateFromZiplist(fills[f], options[_i], zl);
                if (fills[f] == 1)
                    ql_verify(ql, 66, 66, 1, 1);
                else if (fills[f] == 32)
                    ql_verify(ql, 3, 66, 32, 2);
                else if (fills[f] == 66)
                    ql_verify(ql, 1, 66, 66, 66);
                quicklistRelease(ql);
            }
        }

        long long stop = mstime();
        runtime[_i] = stop - start;
    }

    /* Run a longer test of compression depth outside of primary test loop. */
    int list_sizes[] = {250, 251, 500, 999, 1000};
    long long start = mstime();
    int list_count = accurate ? (int)(sizeof(list_sizes) / sizeof(*list_sizes)) : 1;
    for (int list = 0; list < list_count; list++) {
        TEST_DESC("verify specific compression of interior nodes with %d list ",
                  list_sizes[list]) {
            for (int f = 0; f < fill_count; f++) {
                for (int depth = 1; depth < 40; depth++) {
                    /* skip over many redundant test cases */
                    quicklist *ql = quicklistNew(fills[f], depth);
                    for (int i = 0; i < list_sizes[list]; i++) {
                        quicklistPushTail(ql, genstr("hello TAIL", i + 1), 64);
                        quicklistPushHead(ql, genstr("hello HEAD", i + 1), 64);
                    }

                    for (int step = 0; step < 2; step++) {
                        /* test remove node */
                        if (step == 1) {
                            for (int i = 0; i < list_sizes[list] / 2; i++) {
                                unsigned char *data;
                                quicklistPop(ql, QUICKLIST_HEAD, &data, NULL, NULL);
                                zfree(data);
                                quicklistPop(ql, QUICKLIST_TAIL, &data, NULL, NULL);
                                zfree(data);
                            }
                        }
                        quicklistNode *node = ql->head;
                        unsigned int low_raw = ql->compress;
                        unsigned int high_raw = ql->len - ql->compress;

                        for (unsigned int at = 0; at < ql->len;
                            at++, node = node->next) {
                            if (at < low_raw || at >= high_raw) {
                                if (node->encoding != QUICKLIST_NODE_ENCODING_RAW) {
                                    ERR("Incorrect compression: node %d is "
                                        "compressed at depth %d ((%u, %u); total "
                                        "nodes: %lu; size: %u)",
                                        at, depth, low_raw, high_raw, ql->len,
                                        node->sz);
                                }
                            } else {
                                if (node->encoding != QUICKLIST_NODE_ENCODING_LZF) {
                                    ERR("Incorrect non-compression: node %d is NOT "
                                        "compressed at depth %d ((%u, %u); total "
                                        "nodes: %lu; size: %u; attempted: %d)",
                                        at, depth, low_raw, high_raw, ql->len,
                                        node->sz, node->attempted_compress);
                                }
                            }
                        }
                    }

                    quicklistRelease(ql);
                }
            }
        }
    }
    long long stop = mstime();

    printf("\n");
    for (size_t i = 0; i < option_count; i++)
        printf("Test Loop %02d: %0.2f seconds.\n", options[i],
               (float)runtime[i] / 1000);
    printf("Compressions: %0.2f seconds.\n", (float)(stop - start) / 1000);
    printf("\n");

    TEST("bookmark get updated to next item") {
        quicklist *ql = quicklistNew(1, 0);
        quicklistPushTail(ql, "1", 1);
        quicklistPushTail(ql, "2", 1);
        quicklistPushTail(ql, "3", 1);
        quicklistPushTail(ql, "4", 1);
        quicklistPushTail(ql, "5", 1);
        assert(ql->len==5);
        /* add two bookmarks, one pointing to the node before the last. */
        assert(quicklistBookmarkCreate(&ql, "_dummy", ql->head->next));
        assert(quicklistBookmarkCreate(&ql, "_test", ql->tail->prev));
        /* test that the bookmark returns the right node, delete it and see that the bookmark points to the last node */
        assert(quicklistBookmarkFind(ql, "_test") == ql->tail->prev);
        assert(quicklistDelRange(ql, -2, 1));
        assert(quicklistBookmarkFind(ql, "_test") == ql->tail);
        /* delete the last node, and see that the bookmark was deleted. */
        assert(quicklistDelRange(ql, -1, 1));
        assert(quicklistBookmarkFind(ql, "_test") == NULL);
        /* test that other bookmarks aren't affected */
        assert(quicklistBookmarkFind(ql, "_dummy") == ql->head->next);
        assert(quicklistBookmarkFind(ql, "_missing") == NULL);
        assert(ql->len==3);
        quicklistBookmarksClear(ql); /* for coverage */
        assert(quicklistBookmarkFind(ql, "_dummy") == NULL);
        quicklistRelease(ql);
    }

    TEST("bookmark limit") {
        int i;
        quicklist *ql = quicklistNew(1, 0);
        quicklistPushHead(ql, "1", 1);
        for (i=0; i<QL_MAX_BM; i++)
            assert(quicklistBookmarkCreate(&ql, genstr("",i), ql->head));
        /* when all bookmarks are used, creation fails */
        assert(!quicklistBookmarkCreate(&ql, "_test", ql->head));
        /* delete one and see that we can now create another */
        assert(quicklistBookmarkDelete(ql, "0"));
        assert(quicklistBookmarkCreate(&ql, "_test", ql->head));
        /* delete one and see that the rest survive */
        assert(quicklistBookmarkDelete(ql, "_test"));
        for (i=1; i<QL_MAX_BM; i++)
            assert(quicklistBookmarkFind(ql, genstr("",i)) == ql->head);
        /* make sure the deleted ones are indeed gone */
        assert(!quicklistBookmarkFind(ql, "0"));
        assert(!quicklistBookmarkFind(ql, "_test"));
        quicklistRelease(ql);
    }

    if (!err)
        printf("ALL TESTS PASSED!\n");
    else
        ERR("Sorry, not all tests passed!  In fact, %d tests failed.", err);

    return err;
}
#endif
