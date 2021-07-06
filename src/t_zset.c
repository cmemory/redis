/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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

/*-----------------------------------------------------------------------------
 * Sorted set API
 *----------------------------------------------------------------------------*/

/* ZSETs are ordered sets using two data structures to hold the same elements
 * in order to get O(log(N)) INSERT and REMOVE operations into a sorted
 * data structure.
 *
 * The elements are added to a hash table mapping Redis objects to scores.
 * At the same time the elements are added to a skip list mapping scores
 * to Redis objects (so objects are sorted by scores in this "view").
 *
 * Note that the SDS string representing the element is the same in both
 * the hash table and skiplist in order to save memory. What we do in order
 * to manage the shared SDS string more easily is to free the SDS string
 * only in zslFreeNode(). The dictionary has no value free method set.
 * So we should always remove an element from the dictionary, and later from
 * the skiplist.
 *
 * This skiplist implementation is almost a C translation of the original
 * algorithm described by William Pugh in "Skip Lists: A Probabilistic
 * Alternative to Balanced Trees", modified in three ways:
 * a) this implementation allows for repeated scores.
 * b) the comparison is not just by key (our 'score') but by satellite data.
 * c) there is a back pointer, so it's a doubly linked list with the back
 * pointers being only at "level 1". This allows to traverse the list
 * from tail to head, useful for ZREVRANGE. */

#include "server.h"
#include <math.h>

/*-----------------------------------------------------------------------------
 * Skiplist implementation of the low level API
 *----------------------------------------------------------------------------*/

int zslLexValueGteMin(sds value, zlexrangespec *spec);
int zslLexValueLteMax(sds value, zlexrangespec *spec);

/* Create a skiplist node with the specified number of levels.
 * The SDS string 'ele' is referenced by the node after the call. */
// 使用指定level创建一个skiplist节点，ele作为该节点的元素，score为元素的评分。
zskiplistNode *zslCreateNode(int level, double score, sds ele) {
    zskiplistNode *zn =
        zmalloc(sizeof(*zn)+level*sizeof(struct zskiplistLevel));
    zn->score = score;
    zn->ele = ele;
    return zn;
}

/* Create a new skiplist. */
// 创建一个新的skiplist。
zskiplist *zslCreate(void) {
    int j;
    zskiplist *zsl;

    zsl = zmalloc(sizeof(*zsl));
    // 初始时跳表leve为1，总元素个数为0
    zsl->level = 1;
    zsl->length = 0;
    // 创建header节点（有最大层数32），不包含元素
    zsl->header = zslCreateNode(ZSKIPLIST_MAXLEVEL,0,NULL);
    // 初始化header每层的前向指针为NULL，跨度为0。
    for (j = 0; j < ZSKIPLIST_MAXLEVEL; j++) {
        zsl->header->level[j].forward = NULL;
        zsl->header->level[j].span = 0;
    }
    // header的后向指针为NULL
    zsl->header->backward = NULL;
    zsl->tail = NULL;
    return zsl;
}

/* Free the specified skiplist node. The referenced SDS string representation
 * of the element is freed too, unless node->ele is set to NULL before calling
 * this function. */
// 释放指定的跳表节点。除非调用这个函数前node->ele设置为NULL，否则也会将元素sds释放掉。
void zslFreeNode(zskiplistNode *node) {
    sdsfree(node->ele);
    zfree(node);
}

/* Free a whole skiplist. */
// 释放整个跳表。
void zslFree(zskiplist *zsl) {
    // 因为第1层会包含所有的节点，所以我们只需要遍历第1层处理节点释放就可以了。
    zskiplistNode *node = zsl->header/*->level[0].forward*/, *next;

    // 释放header节点，因为没有元素需要释放，所以可以直接调用zfree处理。其实也可以放到循环里一起处理的。
    // zfree(zsl->header);
    // 遍历第1层的节点，挨个释放。
    while(node) {
        next = node->level[0].forward;
        zslFreeNode(node);
        node = next;
    }
    // 释放跳表结构
    zfree(zsl);
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
// 为即将创建的跳表新节点返回一个随机层级。返回值在[1, ZSKIPLIST_MAXLEVEL]之间，且具有幂律分布，越大的levels有更少概率返回。
int zslRandomLevel(void) {
    int level = 1;
    // 1/4的概率为2，(1/4)^2的概率为3，...，每上升一层的概率都为1/4。结果是32层的概率为(1/4)^31
    while ((random()&0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
        level += 1;
    // 最高不会超过32层。
    return (level<ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}

/* Insert a new node in the skiplist. Assumes the element does not already
 * exist (up to the caller to enforce that). The skiplist takes ownership
 * of the passed SDS string 'ele'. */
// 插入一个新的元素到跳表中，调用方会确保该元素在跳表中不存在。这里直接使用传过来的ele字符串，从而与dict中key共享。
zskiplistNode *zslInsert(zskiplist *zsl, double score, sds ele) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned int rank[ZSKIPLIST_MAXLEVEL];
    int i, level;

    serverAssert(!isnan(score));
    x = zsl->header;
    // 跳表是socre有序的，我们需要先找到该元素插入位置。
    // 首先从当前最高level向后遍历，每当forward为NULL时，向下降一层再向后查找，直到找到第一个比当插入元素大的节点位置即为我们插入位置。
    // 如果最终遍历玩了都没找到，则我们插入位置即为跳表的末尾。
    for (i = zsl->level-1; i >= 0; i--) {
        /* store rank that is crossed to reach the insert position */
        // 存储每一层我们为了到达插入位置而跨越的节点数（即每层插入位置的backward节点的span值），用于后面更新插入节点每层指针的span。
        rank[i] = i == (zsl->level-1) ? 0 : rank[i+1];
        while (x->level[i].forward &&
                (x->level[i].forward->score < score ||
                    (x->level[i].forward->score == score &&
                    sdscmp(x->level[i].forward->ele,ele) < 0)))
        {
            // 每个节点的level结构包含forward指针链接，以及节点forward的跨度。
            // rank数组保存每层从header到待插入位置的backward节点的总跨度，不包括插入位置backward节点的forward跨度。
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
        // update数组保存了每层待插入位置的backward节点。
        // 后面更新时，需要更新每层该对应节点的forward指针为插入节点，更新每层该对应节点forward的跨度span=rank[0]-rank[i]+1
        // 并更新插入节点的forward指针为update[i]的原forward节点，更新插入节点的forward跨度为update[i]的原span+1-新span
        update[i] = x;
    }
    /* we assume the element is not already inside, since we allow duplicated
     * scores, reinserting the same element should never happen since the
     * caller of zslInsert() should test in the hash table if the element is
     * already inside or not. */
    // 我们不会插入相同的元素，因为zset还有一个dict结构保证元素不重复的。当然我们可以允许score相同。
    // 首先获取节点的level。如果新的节点level比当前跳表的live大，则我们需要更新rank和update数组。
    level = zslRandomLevel();
    if (level > zsl->level) {
        for (i = zsl->level; i < level; i++) {
            // 对于新多出来的level，我们设置rank为0，update为header节点。
            rank[i] = 0;
            update[i] = zsl->header;
            // 更新header节点新增level的每层的span都是跳表总长度，因为新增leve的每层forward指针都指向末尾NULL，所以向后的跨度为总长度。
            update[i]->level[i].span = zsl->length;
        }
        // 更新跳表的level为新值。
        zsl->level = level;
    }
    // 创建新的节点
    x = zslCreateNode(level,score,ele);
    // 遍历每层level，处理新节点的forward指针和span。
    for (i = 0; i < level; i++) {
        // 更新新节点当前level的forward指针，以及新节点当前level的backward节点update[i]的forward（指向新节点）。
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;

        /* update span covered by update[i] as x is inserted here */
        // 更新新节点当前level的span跨度，update[i]->level[i].span+1 - (rank[0]-rank[i]+1)
        // update[i]->level[i].span+1，表示我们增加了一个节点，所以update[i]节点向后的跨度+1。
        // rank[0]-rank[i]+1，表示update[i]节点到新插入节点的跨度为rank[0]-rank[i]+1。
        // 所以新插入节点向后的跨度为update[i]->level[i].span - (rank[0]-rank[i])，两个+1抵消了。
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        // 更新新节点当前level的前一个节点（即update[i]节点）向后的span跨度。
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    /* increment span for untouched levels */
    // 因为新增了一个元素，对于header中还没使用的levels，我们都需要将他们forward向前扩展指针的span+1。
    for (i = level; i < zsl->level; i++) {
        update[i]->level[i].span++;
    }

    // 更新插入节点的backward指针，指向第一层中紧挨着的前一个元素节点（header节点不是元素节点）。
    x->backward = (update[0] == zsl->header) ? NULL : update[0];
    // 更新新插入节点的后一个节点的backward指针。如果后面没有节点，则我们是在末尾插入节点，设置该节点为tail。
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else
        zsl->tail = x;
    // 跳表总元素个数+1
    zsl->length++;
    return x;
}

/* Internal function used by zslDelete, zslDeleteRangeByScore and
 * zslDeleteRangeByRank. */
// 内部函数，用于实现zslDelete、zslDeleteRangeByScore 和 zslDeleteRangeByRank。
// 参数x表示待删除节点。update表示待删除节点每层的backward节点。
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update) {
    int i;
    // 对于待删除节点的每层的backward节点，我们处理它的forward指针以及forward跨度。
    for (i = 0; i < zsl->level; i++) {
        if (update[i]->level[i].forward == x) {
            // 对于小于等于节点level的层数，删除节点在该层的链表中，
            // 我们需要处理待删除节点的前后跨度合并，以及forward指针的更新。对于第1层还需要处理backward指针。
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            // 对于大于删除节点level的层数，删除节点的backward节点的forward指针始终指向NULL，不需要变化。
            // 而backwar跨度因为我们删除了一个节点，需要-1。
            update[i]->level[i].span -= 1;
        }
    }
    // 第1层，删除节点我们需要处理backward指针。
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else {
        // 如果删除的节点是最后一个节点，则我们需要更新tail节点为update[0]
        zsl->tail = x->backward;
    }
    // 更新跳表的level。如果删除节点，更新好指针及跨度后，查看header的forward指针从上到下是否为NULL，如果为空则level-1。
    // 第一个forward不为NULL的级别，即为跳表的level。
    while(zsl->level > 1 && zsl->header->level[zsl->level-1].forward == NULL)
        zsl->level--;
    // 删除了一个删，总元素个数-1
    zsl->length--;
}

/* Delete an element with matching score/element from the skiplist.
 * The function returns 1 if the node was found and deleted, otherwise
 * 0 is returned.
 *
 * If 'node' is NULL the deleted node is freed by zslFreeNode(), otherwise
 * it is not freed (but just unlinked) and *node is set to the node pointer,
 * so that it is possible for the caller to reuse the node (including the
 * referenced SDS string at node->ele). */
// 从跳表删除匹配score/element的节点，如果节点找到且被删除则返回1，否则返回0。
// 如果node参数为NULL，则删除时调用zslFreeNode释放，否则该参数会返回从跳表移除的节点，调用者可以使用该节点并最终释放元素数据。
int zslDelete(zskiplist *zsl, double score, sds ele, zskiplistNode **node) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    x = zsl->header;
    // 遍历跳表查找指定score/element的节点。
    for (i = zsl->level-1; i >= 0; i--) {
        // 先从最高level往forward遍历，当forward为NULL时向下降一层，然后再向forward遍历，依次这样处理。
        // 直到forward找到第一个匹配score/element的节点，即算找到。
        // 否则我们遍历到第1层，forward为NULL或者forward节点的元素大于指定数据时，就算没找到。
        while (x->level[i].forward &&
                (x->level[i].forward->score < score ||
                    (x->level[i].forward->score == score &&
                     sdscmp(x->level[i].forward->ele,ele) < 0)))
        {
            // 每层遍历当前节点不匹配，则向forward遍历。
            x = x->level[i].forward;
        }
        // update[i]保存第i+1层中小于指定元素的最后一个节点。
        update[i] = x;
    }
    /* We may have multiple elements with the same score, what we need
     * is to find the element with both the right score and object. */
    // 前面遍历处理完了，update[i]保存第i+1层中小于指定元素的最后一个节点。
    // 则小于指定元素的最后一个节点即为update[i]，即x节点，也就是前面遍历到的最后一个节点。
    // 此时x的forward要么为NULL，要么就是第一个大于等于指定score/element的节点（注意比较时先score，再ele）。
    // 所以我们认为当x的forward非NULL，且x的forward节点与指定score/element匹配（score和ele都相等），才算找到匹配节点。
    x = x->level[0].forward;
    if (x && score == x->score && sdscmp(x->ele,ele) == 0) {
        // 找到了匹配的节点，这里调用zslDeleteNode处理删除。如果指定了node参数则节点填入返回，否则zslFreeNode释放该节点。
        zslDeleteNode(zsl, x, update);
        if (!node)
            zslFreeNode(x);
        else
            *node = x;
        return 1;
    }
    // x的forward为NULL，或者forward节点不匹配（即大于）指定数据，则没有找到，返回0。
    return 0; /* not found */
}

/* Update the score of an element inside the sorted set skiplist.
 * Note that the element must exist and must match 'score'.
 * This function does not update the score in the hash table side, the
 * caller should take care of it.
 *
 * Note that this function attempts to just update the node, in case after
 * the score update, the node would be exactly at the same position.
 * Otherwise the skiplist is modified by removing and re-adding a new
 * element, which is more costly.
 *
 * The function returns the updated element skiplist node pointer. */
// 更新zset跳表结构中元素节点的score。注意本函数要求指定score和ele的元素必须存在。函数最后返回我们更新的那个节点（有可能是插入的新节点）。
// 另外这里我们只更新跳表中的score，调用者需要注意更新dict中的score，从而保持一致。
// 注意我们更新score时，首先尝试在原节点上处理，即原节点前一个的score/element小于更新后节点的score/element，
// 且原节点后一个的score/element大于更新后节点的score/element，或者前后为NULL节点时，我们可以原地直接更新score。
// 否则需要修改跳表元素的顺序，我们更新的处理方式为删除指定节点，然后用新的score插入一个新节点。
zskiplistNode *zslUpdateScore(zskiplist *zsl, double curscore, sds ele, double newscore) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    /* We need to seek to element to update to start: this is useful anyway,
     * we'll have to update or remove it. */
    // 根据给定的curscore/ele查询元素节点。level从上到下，每层向forward遍历，直到找到第一个不小于指定数据的节点。
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
                (x->level[i].forward->score < curscore ||
                    (x->level[i].forward->score == curscore &&
                     sdscmp(x->level[i].forward->ele,ele) < 0)))
        {
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    /* Jump to our element: note that this function assumes that the
     * element with the matching score exists. */
    // 取到第一个不小于指定数据的节点。因为我们要确保该节点一定存在，所以该节点非空，且该节点的score/ele与指定参数一致。
    x = x->level[0].forward;
    serverAssert(x && curscore == x->score && sdscmp(x->ele,ele) == 0);

    /* If the node, after the score update, would be still exactly
     * at the same position, we can just update the score without
     * actually removing and re-inserting the element in the skiplist. */
    // 检查如果更新score后，节点前面顺序没有变，则我们可以直接在原节点上更新。否则需要删除该节点，然后用newscore插入新节点。
    // 这里检查前一个score小于newscore，或者score相等但前一个ele小于当前节点ele，则认为节点顺序没变。
    // 与forward节点判断顺序也是一样的比较，后面score大于newscore，或者score相等但后一个ele大于当前ele，则节点顺序没变。
    if ((x->backward == NULL || x->backward->score < newscore ||
        (x->backward->score == newscore && sdscmp(x->backward->->ele,ele) < 0)) &&
        (x->level[0].forward == NULL || x->level[0].forward->score > newscore ||
        (x->level[0].forward->score == newscore && sdscmp(x->level[0].forward->ele,ele) > 0)))
    {
        // 顺序没变，则直接更新score，返回该节点。
        x->score = newscore;
        return x;
    }

    /* No way to reuse the old node: we need to remove and insert a new
     * one at a different place. */
    // 顺序变了，这里我们需要移除该节点，然后用newscore插入新的节点。
    zslDeleteNode(zsl, x, update);
    zskiplistNode *newnode = zslInsert(zsl,newscore,x->ele);
    /* We reused the old node x->ele SDS string, free the node now
     * since zslInsert created a new one. */
    // 我重用了ele字符串数据，不能对它进行free，所以这里将原x->ele置为NULL并释放掉节点。
    x->ele = NULL;
    zslFreeNode(x);
    return newnode;
}

// 判断value是否大于(或等于)spec指定的最小值
int zslValueGteMin(double value, zrangespec *spec) {
    return spec->minex ? (value > spec->min) : (value >= spec->min);
}

// 判断value是否小于(或等于)spec指定的最小值
int zslValueLteMax(double value, zrangespec *spec) {
    return spec->maxex ? (value < spec->max) : (value <= spec->max);
}

/* Returns if there is a part of the zset is in range. */
// 判断zset是否有一部分数据在指定的range区间内（使用有序的跳表来进行处理）。如果有则返回1，否则返回0。
// 注意对于两个区间位置一共有6种，左/右相交2种，包含关系2种，完全没有交集2种。这里我们只能排除完全没有交集的两种。
// 而对于另外4种，左/右相交显然肯定是有元素在range中的，包含关系如果range完全包含zset的score范围也肯定有元素在range中的。
// 最后有一种zset score范围包含range，这种是有可能没有元素在范围中的，主要原因是range为连续空间，而zset score数据是离散的数据。
// 例如：zset的score为{1,2,8,9}，但我们查询的range为[4,6]。这就对应了这种情况。
int zslIsInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    // 检查range范围是否总是空。当min>max，或者min==max且有一端是开区间时，形成的范围是空的。
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;
    // 检查尾节点（即最大元素节点），为NULL则表示当前zset没有元素，显然没有元素在range内，返回0。
    // 不为NULL，但是如果最大的score都小于(或等于)range的最小值，则说明也没有元素在range内的，返回0。
    x = zsl->tail;
    if (x == NULL || !zslValueGteMin(x->score,range))
        return 0;
    // 检查从头的第一个节点（即最小元素节点），为NULL则表示当前zset没有元素，显然没有元素在range内，返回0。
    // 不为NULL，但是如果最小的score都大于(或等于)range的最大值，则说明也没有元素在range内的，返回0。
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslValueLteMax(x->score,range))
        return 0;
    // 前面检查了特殊的完全没有交集的情况，其实我们还不能确定一定就有元素在range中，例如上面最后一种情况。
    return 1;
}

/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
// 查找有序跳表中，第一个包含在指定range范围内的节点。如果没有元素包含在范围内，则返回NULL。
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    // 如果没有元素包含在指定范围内，返回NULL。
    if (!zslIsInRange(zsl,range)) return NULL;

    x = zsl->header;
    // 按level从上到下，往forward进行遍历，找到第一个score大于(或等于)range最小值的元素，即为第一个在range范围中的元素。
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward &&
            !zslValueGteMin(x->level[i].forward->score,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    // 根据zslIsInRange中的分析，针对可能有交集的集中情况，该forward节点要么是在range中的第一个的节点，要么是跳过range的第一个节点。
    // 总之该节点是一定存在的，即不为NULL。
    x = x->level[0].forward;
    serverAssert(x != NULL);

    /* Check if score <= max. */
    // 判断是否score<=max，如果score>max，表示节点为跳过range的第一个节点，那么就没有元素在range中，返回NULL。
    // 例如：zset的score为{1,2,8,9}，但我们查询的range为[4,6]，此时遍历到了8这个元素。
    if (!zslValueLteMax(x->score,range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
// 从有序的跳表中查询最后一个包含在指定范围内的节点。如果没有节点在范围内，则返回NULL。
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    // 如果没有元素包含在指定范围内，返回NULL。
    if (!zslIsInRange(zsl,range)) return NULL;

    x = zsl->header;
    // 按level从上到下，往forward进行遍历，找到第一个score大于(或等于)range最大值的元素，即为第一个超出range范围中的元素。
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward &&
            zslValueLteMax(x->level[i].forward->score,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    // 根据前面遍历可知，x节点是一定存储在的。
    serverAssert(x != NULL);

    /* Check if score >= min. */
    // 判断是否score>=min，如果score<min，表示节点为跳过range的第一个节点，那么就没有元素在range中，返回NULL。
    // 例如：zset的score为{1,2,8,9}，但我们查询的range为[4,6]。此时遍历到了2这个元素。
    if (!zslValueGteMin(x->score,range)) return NULL;
    return x;
}

/* Delete all the elements with score between min and max from the skiplist.
 * Both min and max can be inclusive or exclusive (see range->minex and
 * range->maxex). When inclusive a score >= min && score <= max is deleted.
 * Note that this function takes the reference to the hash table view of the
 * sorted set, in order to remove the elements from the hash table too. */
// 从跳表中删除range指定的score范围的元素，具体范围指定见range相关定义。
// 注意跳表中的元素ele，与dict是共享的，所以我们从跳表中移除后，也要从dict中移除。
unsigned long zslDeleteRangeByScore(zskiplist *zsl, zrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    x = zsl->header;
    // 遍历找到第一个score大于(或等于)range最小值的节点，该节点即为我们待删除的起始节点(后面还需要检查range最大值)。
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            !zslValueGteMin(x->level[i].forward->score, range))
                x = x->level[i].forward;
        // update[i]记录则我们第i+1层，遍历到的最后一个节点，该节点forward要么为NULL，要么是我们待删除的起始节点。
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    // x定位到第一个待删除节点。
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    // 向forward遍历处理删除。
    while (x && zslValueLteMax(x->score, range)) {
        // 如果x非NULL，且x的score小于(或等于)range最大值，说明x在要删除的范围内，处理删除。
        // 先获取forward节点。
        zskiplistNode *next = x->level[0].forward;
        // 处理跳表中节点删除、dict中元素删除
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->ele);
        // 释放节点，x->ele也会一起释放
        zslFreeNode(x); /* Here is where x->ele is actually released. */
        // 记录删除元素个数，用于返回
        removed++;
        // x跳到下一个节点，继续处理删除。
        x = next;
    }
    return removed;
}

// 从跳表中删除range指定的ele范围的元素(ele排序按字节顺序)，具体范围指定见range相关定义。
// 注意根据ele字典序范围删除，这个函数正常处理的前提是所有元素score一致，否则我们根据ele顺序删除结果不正确。
// 我们排序是先score再ele的，显然如果元素的score不一致的话，节点并不是按照ele升序排列的。
// 我们通过先forward找第一个在范围内的元素，后面再向forward遍历找第一个超出范围的元素，
// 这样得到的区间实际上可能只是单个score中在range范围区间内的所有元素，并没有找到zset中所有在这个range区间内的元素。
// 所以我们要使用这个方法时，必须要zset中所有的元素score都一样，这样才能保证ele是有序的，从而可以删除所有在范围区间的元素。
// 另外如果score不一样，也想用这个方法删除的话，可以循环调用这个方法，直到调用方法最终返回删除的元素为0时表示我们删除完了。
unsigned long zslDeleteRangeByLex(zskiplist *zsl, zlexrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;


    x = zsl->header;
    // level从上到下，向forward遍历找第一个ele大于(或等于)range最小值的节点（此节点可能开始进入我们的范围了，但后面还需验证ele小于最大值）。
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            !zslLexValueGteMin(x->level[i].forward->ele,range))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    // x指向第一个ele大于(或等于)range最小值的节点
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    // 从x向后遍历处理元素删除
    while (x && zslLexValueLteMax(x->ele,range)) {
        // 如果节点非空，且节点ele小于(或等于)range最大值，说明在range范围内，要处理删除。
        // 先获取forward节点。
        zskiplistNode *next = x->level[0].forward;
        // 处理跳表和dict中元素删除
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->ele);
        // 释放节点，删除节点记录+1
        zslFreeNode(x); /* Here is where x->ele is actually released. */
        removed++;
        // x跳到下一个节点，继续处理删除。
        x = next;
    }
    return removed;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
// 删除跳表中rank在[start, end]间的节点，注意包含两端，另外他们都是从1开始的（rank从1开始）。
// 虽然我们在处理命令时，用户传的参数是0开始的，但在调用这个函数时会对参数+1。
unsigned long zslDeleteRangeByRank(zskiplist *zsl, unsigned int start, unsigned int end, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;
    int i;

    x = zsl->header;
    // rank遍历我们是通过指针上的span跨度来计算定位的。不过遍历当然还是离不开level从上往下，向forward进行遍历。
    for (i = zsl->level-1; i >= 0; i--) {
        // 每一层，我们向forward遍历，当从header到forward节点的span<start，说明到forward节点的rank还不够，我们要继续向forward遍历。
        // 如果span>=start，则我们需要跳到下一层level来向后遍历，但假如我们已经到第1层不能再向下了，则说明当前节点的forward节点即start。
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    // traversed即表示我们span跨度，也即节点rank值。
    traversed++;
    // x指向start节点
    x = x->level[0].forward;
    // 从start向后遍历直到达到end 或 节点为NULL为止，挨个处理删除。
    while (x && traversed <= end) {
        // 先获取forward节点，用于下一轮循环。
        zskiplistNode *next = x->level[0].forward;
        // 处理跳表和dict数据删除
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->ele);
        // 释放节点，更新删除节点数用于返回。
        zslFreeNode(x);
        removed++;
        // 跳到下一个节点处理，并更新下一个处理的rank值。
        traversed++;
        x = next;
    }
    return removed;
}

/* Find the rank for an element by both score and key.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element. */
// 通过ele和score数据从跳表中查询该元素节点的rank值。如果没找到元素则返回0，否则返回rank值。
// 注意元素的rank值是从1开始的，因为元素的rank即是header到该节点的span跨度，跨度从1开始，从而rank也就从1开始。
unsigned long zslGetRank(zskiplist *zsl, double score, sds ele) {
    zskiplistNode *x;
    unsigned long rank = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        // 每层向forward遍历。注意这里score相等时，对比ele使用的是"<="。
        // 因为zset中ele元素唯一，所以如果是forward节点ele > 指定ele而退出循环的话，
        // 要么当前节点的ele与指定ele相等，则直接可以返回rank，要么<指定ele，则不存在对应元素。
        // 而如果其他条件跳出循环的话，我们没有找到对应元素，需要下降一层level再来遍历处理。
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                sdscmp(x->level[i].forward->ele,ele) <= 0))) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }

        /* x might be equal to zsl->header, so test if obj is non-NULL */
        // 前面分析了，这里可以直接对比元素是否相等，如果相等的话，就返回rank。
        // 其实也可以跟其他函数查询节点一样处理，上面对比ele使用"<"，然后这个if放到外层。
        if (x->ele && sdscmp(x->ele,ele) == 0) {
            return rank;
        }
    }
    return 0;
}

/* Finds an element by its rank. The rank argument needs to be 1-based. */
// 根据rank查询对应的元素，注意rank是从1开始的。
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank) {
    zskiplistNode *x;
    unsigned long traversed = 0;
    int i;

    x = zsl->header;
    // 处理方式跟zslDeleteRangeByRank中查询start差不多，这里对比rank也是使用的"<="，从而我们可以在每层遍历结束时判断rank，一致则直接返回。
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank)
        {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank) {
            return x;
        }
    }
    return NULL;
}

/* Populate the rangespec according to the objects min and max. */
// 使用min、max对象填充rangespec结构。这里处理score的range参数。
static int zslParseRange(robj *min, robj *max, zrangespec *spec) {
    char *eptr;
    spec->minex = spec->maxex = 0;

    /* Parse the min-max interval. If one of the values is prefixed
     * by the "(" character, it's considered "open". For instance
     * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */
    // 解析min-max区间。如果值是以"("开头，则表示开区间不包含该值；否则表示闭区间包含该值。
    if (min->encoding == OBJ_ENCODING_INT) {
        // INT型编码，则直接从指针中取数字。
        spec->min = (long)min->ptr;
    } else {
        // 字符串，如果第一个字符是"("，除了字符串转数字外，还需要设置minex。
        if (((char*)min->ptr)[0] == '(') {
            spec->min = strtod((char*)min->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
            spec->minex = 1;
        } else {
            spec->min = strtod((char*)min->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
        }
    }
    // max参数处理方式与前面min一致。
    if (max->encoding == OBJ_ENCODING_INT) {
        spec->max = (long)max->ptr;
    } else {
        if (((char*)max->ptr)[0] == '(') {
            spec->max = strtod((char*)max->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
            spec->maxex = 1;
        } else {
            spec->max = strtod((char*)max->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
        }
    }

    return C_OK;
}

/* ------------------------ Lexicographic ranges ---------------------------- */

/* Parse max or min argument of ZRANGEBYLEX.
  * (foo means foo (open interval)
  * [foo means foo (closed interval)
  * - means the min string possible
  * + means the max string possible
  *
  * If the string is valid the *dest pointer is set to the redis object
  * that will be used for the comparison, and ex will be set to 0 or 1
  * respectively if the item is exclusive or inclusive. C_OK will be
  * returned.
  *
  * If the string is not a valid range C_ERR is returned, and the value
  * of *dest and *ex is undefined. */
// 注意：Lexicographic ranges以及相关命令操作，都是建立在zset中所有元素的score都一致的情况下的。
// 如果score不一样的话，这些处理及命令都会产生未定义的结果！！！

// 解析ZRANGEBYLEX命令的range参数。
// 第一个字符'('表示开区间，不包含该值；第一个字符'['表示闭区间，包含该值。另外"+"、"-"分别表示最大和最小字符串。
// 如果字符串有效，则会存储在dest中，且ex设置为0或1表示开区间或闭区间，然后函数会返回ok。
// 如果字符串无效，则会返回err，且dest和ex值未定义，不可使用。
int zslParseLexRangeItem(robj *item, sds *dest, int *ex) {
    char *c = item->ptr;

    switch(c[0]) {
    case '+':
        if (c[1] != '\0') return C_ERR;
        *ex = 1;
        *dest = shared.maxstring;
        return C_OK;
    case '-':
        if (c[1] != '\0') return C_ERR;
        *ex = 1;
        *dest = shared.minstring;
        return C_OK;
    case '(':
        *ex = 1;
        *dest = sdsnewlen(c+1,sdslen(c)-1);
        return C_OK;
    case '[':
        *ex = 0;
        *dest = sdsnewlen(c+1,sdslen(c)-1);
        return C_OK;
    default:
        return C_ERR;
    }
}

/* Free a lex range structure, must be called only after zslParseLexRange()
 * populated the structure with success (C_OK returned). */
// 释放一个字符串范围的结构。只能在zslParseLexRange()填充该结构成功返回ok之后调用。
void zslFreeLexRange(zlexrangespec *spec) {
    // 如果是共享对象则不需要处理，否则都需要释放。
    if (spec->min != shared.minstring &&
        spec->min != shared.maxstring) sdsfree(spec->min);
    if (spec->max != shared.minstring &&
        spec->max != shared.maxstring) sdsfree(spec->max);
}

/* Populate the lex rangespec according to the objects min and max.
 *
 * Return C_OK on success. On error C_ERR is returned.
 * When OK is returned the structure must be freed with zslFreeLexRange(),
 * otherwise no release is needed. */
// 使用min、main对象的数据填充字符串范围结构。成功则返回OK，否则出错返回err。
// 如果填充成功，则当我们使用完毕时，需要调用zslFreeLexRange()来释放。
int zslParseLexRange(robj *min, robj *max, zlexrangespec *spec) {
    /* The range can't be valid if objects are integer encoded.
     * Every item must start with ( or [. */
    // min/max对象不可能是数字编码，因为他们都必须是"("或"["开头，
    if (min->encoding == OBJ_ENCODING_INT ||
        max->encoding == OBJ_ENCODING_INT) return C_ERR;

    spec->min = spec->max = NULL;
    // 解析min/max数据填充spec，如果失败需要调用zslFreeLexRange释放。
    if (zslParseLexRangeItem(min, &spec->min, &spec->minex) == C_ERR ||
        zslParseLexRangeItem(max, &spec->max, &spec->maxex) == C_ERR) {
        zslFreeLexRange(spec);
        return C_ERR;
    } else {
        return C_OK;
    }
}

/* This is just a wrapper to sdscmp() that is able to
 * handle shared.minstring and shared.maxstring as the equivalent of
 * -inf and +inf for strings */
// sdscmp()函数的包装，增加了一些特例处理，用于将 shared.minstring 和 shared.maxstring 当做 -inf 和 +inf 来处理
int sdscmplex(sds a, sds b) {
    if (a == b) return 0;
    if (a == shared.minstring || b == shared.maxstring) return -1;
    if (a == shared.maxstring || b == shared.minstring) return 1;
    return sdscmp(a,b);
}

// 判断字符串value是否大于(或等于)spec最小值。
int zslLexValueGteMin(sds value, zlexrangespec *spec) {
    return spec->minex ?
        (sdscmplex(value,spec->min) > 0) :
        (sdscmplex(value,spec->min) >= 0);
}

// 判断字符串value是否小于(或等于)spec最大值。
int zslLexValueLteMax(sds value, zlexrangespec *spec) {
    return spec->maxex ?
        (sdscmplex(value,spec->max) < 0) :
        (sdscmplex(value,spec->max) <= 0);
}

/* Returns if there is a part of the zset is in the lex range. */
// 判断zset中是有有一部分数据的ele在字符串范围range中。
// 前面讲过其他的inRange判断方法，range与zset集合会有6种形式，这里只能判断完全不相交的2种形式，其他4种还需要进一步检查。
int zslIsInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    // 当min>max，或者 min==max且有一端是开区间时，我们知道这一对min/max形成的范围是空的。
    int cmp = sdscmplex(range->min,range->max);
    if (cmp > 0 || (cmp == 0 && (range->minex || range->maxex)))
        return 0;
    // 当尾节点为NULL，表示zset没有元素，那么也没有元素在range范围中。
    // 当尾节点不为NULL，但是尾节点的ele（zset中最大值）小于range的最小值，则zset没有元素在range范围中。
    x = zsl->tail;
    if (x == NULL || !zslLexValueGteMin(x->ele,range))
        return 0;
    // 当头节点为NULL，表示zset没有元素，那么也没有元素在range范围中。
    // 当头节点不为NULL，但是头节点的ele（zset中最小值）大于range的最大值，则zset没有元素在range范围中。
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslLexValueLteMax(x->ele,range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
// 查找第一个ele在指定lex range中的元素节点返回，如果没有元素在range中，则返回NULL。
// 注意需要跳表中ele完全有序排列。我们知道跳表是按score-ele升序的，想要所有ele有序，则所有score必须一样。
zskiplistNode *zslFirstInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    // 首先使用zslIsInLexRange判断，如果所有元素都不在range中，则尽早返回。
    if (!zslIsInLexRange(zsl,range)) return NULL;

    x = zsl->header;
    // 遍历找到第一个大于等于range最小值的节点。
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward &&
            !zslLexValueGteMin(x->level[i].forward->ele,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    // 定位到第一个在range中的节点返回。range肯定有一部分在zset范围内部，所以得到的节点肯定是非NULL的。（前面其他类似方法有详细说明）
    x = x->level[0].forward;
    serverAssert(x != NULL);

    /* Check if ele <= max. */
    // 还有一种没有元素在range中的情况，所以这里需要判断ele<=max来判断确保节点在range内。
    if (!zslLexValueLteMax(x->ele,range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
// 查找最后ele一个在指定lex range中的元素节点返回，如果没有元素在range中，则返回NULL。
// 注意需要所有score都相同，这样ele才是有序的，才能使用这个方法。
zskiplistNode *zslLastInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    // 首先使用zslIsInLexRange判断，如果所有元素都不在range中，则尽早返回。
    if (!zslIsInLexRange(zsl,range)) return NULL;

    x = zsl->header;
    // 遍历找到第一个大于range最大值的节点。即第一个在range范围之外的节点。
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward &&
            zslLexValueLteMax(x->level[i].forward->ele,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    // 前面遍历while循环可以知道，这个节点是一定不为空的。
    serverAssert(x != NULL);

    /* Check if ele >= min. */
    // 还有一种没有元素在range中的情况，所以这里需要判断score>=min来判断确保节点在range内。
    if (!zslLexValueGteMin(x->ele,range)) return NULL;
    return x;
}

/*-----------------------------------------------------------------------------
 * Ziplist-backed sorted set API
 *----------------------------------------------------------------------------*/

// 字符串转浮点数返回。
double zzlStrtod(unsigned char *vstr, unsigned int vlen) {
    char buf[128];
    if (vlen > sizeof(buf))
        vlen = sizeof(buf);
    memcpy(buf,vstr,vlen);
    buf[vlen] = '\0';
    return strtod(buf,NULL);
 }

// 返回一个ziplist元素作为score。即从sptr指针指向的ziplist entry处提取数据，并转成浮点数score返回。
double zzlGetScore(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    double score;

    serverAssert(sptr != NULL);
    // 调用ziplistGet从sptr处解析数据
    serverAssert(ziplistGet(sptr,&vstr,&vlen,&vlong));

    // 字符串数据还需要先转成浮点数，数字则直接作为score返回。
    if (vstr) {
        score = zzlStrtod(vstr,vlen);
    } else {
        score = vlong;
    }

    return score;
}

/* Return a ziplist element as an SDS string. */
// 返回一个ziplist元素作为sds字符串。即从sptr指针指向的ziplist entry处提取数据，并转成sds字符串返回。
sds ziplistGetObject(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;

    serverAssert(sptr != NULL);
    // 调用ziplistGet从sptr处解析数据
    serverAssert(ziplistGet(sptr,&vstr,&vlen,&vlong));

    // 数据转为sds字符串返回
    if (vstr) {
        return sdsnewlen((char*)vstr,vlen);
    } else {
        return sdsfromlonglong(vlong);
    }
}

/* Compare element in sorted set with given element. */
// 对比zset中元素是否与给定元素一致。zset中元素存储在ziplist中的eptr指针处，我们需要先提取并转成字符串后再与指定元素cstr来比较。
int zzlCompareElements(unsigned char *eptr, unsigned char *cstr, unsigned int clen) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    unsigned char vbuf[32];
    int minlen, cmp;

    // 调用ziplistGet从eptr处解析数据
    serverAssert(ziplistGet(eptr,&vstr,&vlen,&vlong));
    if (vstr == NULL) {
        /* Store string representation of long long in buf. */
        // 如果zset中解析到的数据是数字，则这里转为字符串格式。
        vlen = ll2string((char*)vbuf,sizeof(vbuf),vlong);
        vstr = vbuf;
    }

    // memcmp对比两个字符串数据
    minlen = (vlen < clen) ? vlen : clen;
    cmp = memcmp(vstr,cstr,minlen);
    if (cmp == 0) return vlen-clen;
    return cmp;
}

// 获取ziplist编码的zset总元素数。
unsigned int zzlLength(unsigned char *zl) {
    return ziplistLen(zl)/2;
}

/* Move to next entry based on the values in eptr and sptr. Both are set to
 * NULL when there is no next entry. */
// 获取ziplist编码方式的zset的下一个元素。当前元素的ele和score entry的指针由*eptr、*sptr传入。（该参数即时入参也是出参）
// 获取到元素的ele和score entry的指针也都分别放在*eptr、*sptr中返回。当没有下一个元素时，两个值都设置为NULL。
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    serverAssert(*eptr != NULL && *sptr != NULL);

    // 根据ziplist的元素格式，以*sptr来调用ziplistNext获取到下一个元素的ele entry指针。
    _eptr = ziplistNext(zl,*sptr);
    if (_eptr != NULL) {
        // _eptr不为NULL，说明我们有下一个元素。这里再以_eptr调用ziplistNext，就能获取到下一个元素的score entry。
        _sptr = ziplistNext(zl,_eptr);
        serverAssert(_sptr != NULL);
    } else {
        /* No next entry. */
        // _eptr为NULL，说明没有下一个元素，这里我们将_sptr也置为NULL，后面统一处理返回。
        _sptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* Move to the previous entry based on the values in eptr and sptr. Both are
 * set to NULL when there is no next entry. */
// 获取ziplist编码方式的zset的前一个元素，当前节点的信息由*eptr、*sptr传入。（该参数即时入参也是出参）
// 获取到的元素ele和score entry的指针也都分别放在*eptr、*sptr中返回。当没有前一个元素时，两个值都设置为NULL。
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    serverAssert(*eptr != NULL && *sptr != NULL);

    // 根据ziplist的元素格式，以*eptr来调用ziplistPrev获取前一个元素的score entry指针。
    _sptr = ziplistPrev(zl,*eptr);
    if (_sptr != NULL) {
        // _sptr不为NULL，说明我们有前一个元素。这里再以_sptr调用ziplistPrev，就能获取到前一个元素的ele entry。
        _eptr = ziplistPrev(zl,_sptr);
        serverAssert(_eptr != NULL);
    } else {
        /* No previous entry. */
        // _sptr为NULL，说明没有前一个元素，这里我们将_eptr也置为NULL，后面统一处理返回。
        _eptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInRange and zzlLastInRange. */
// 判断ziplist编码的zset中是否有一部分元素在给定的range中。该方法应该只被用于zzlFirstInRange和zzlLastInRange内部。
// 注意对于两个区间位置一共有6种，左/右相交2种，包含关系2种，完全没有交集2种。这里我们只能排除完全没有交集的两种。
// 而对于另外4种，左/右相交显然肯定是有元素在range中的，包含关系如果range完全包含zset的score范围也肯定有元素在range中的。
// 最后有一种zset score范围包含range，这种是有可能没有元素在范围中的，主要原因是range为连续空间，而zset score数据是离散的数据。
// 例如：zset的score为{1,2,8,9}，但我们查询的range为[4,6]。这就对应了这种情况。
int zzlIsInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *p;
    double score;

    /* Test for ranges that will always be empty. */
    // 当min>max，或者min==max且有一端开区间时，该range范围是空的无效的。直接返回0表示没有元素在range内。
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;

    // 获取ziplist的最后一个entry，也就是zset的最后一个元素的score entry。
    p = ziplistIndex(zl,-1); /* Last score. */
    // p为NULL，说明zset是空的，直接返回0表示没有元素在range内。
    if (p == NULL) return 0; /* Empty sorted set */
    // 知道了最后一个元素的score entry位置，使用zzlGetScore来获取score值。
    score = zzlGetScore(p);
    // ziplist中我们的元素也是按score顺序存储的，如果最后一个元素的score（也就是zset中最大的score）小于(或等于)range的最小值。
    // 则zset中所有元素都小于(或等于)range的最小值。显然没有元素在range内，返回0。
    if (!zslValueGteMin(score,range))
        return 0;

    // 获取ziplist的第2个entry，也就是zset的第一个元素的score entry。
    p = ziplistIndex(zl,1); /* First score. */
    // 前面我们先处理了最后一个元素，显然如果zset为空，我们已经返回了。所以执行到这里，zset不为空，则第一个元素的score entry肯定存在。
    serverAssert(p != NULL);
    // 获取第一个元素的score。
    score = zzlGetScore(p);
    // 如果第一个元素的score，即最小的score大于range的最大值。则zset中所有元素score都大于range最大值，显然没有元素在range内，返回0。
    if (!zslValueLteMax(score,range))
        return 0;

    // 前面判断了一些特例，到这里可能有元素在range内，返回1。
    return 1;
}

/* Find pointer to the first element contained in the specified range.
 * Returns NULL when no element is contained in the range. */
// 找到ziplist编码的zset中第一个score在指定range中的元素位置指针返回。如果没有元素在range中，则返回NULL。
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range) {
    // eptr定位到第一个元素的ele entry位置。
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;
    double score;

    /* If everything is out of range, return early. */
    // 先使用zzlIsInRange判断是否有元素在range中，如果没有则直接返回NULL。
    if (!zzlIsInRange(zl,range)) return NULL;

    // 假设有元素在range中，则依次从前往后遍历ziplist，对于每个元素的score，我们都判断是否在范围内。
    // 找到第一个在范围内的，则直接返回；否则score超出了范围即大于了最大值，或者遍历到最后都没找到即所有都小于最小值，返回NULL。
    while (eptr != NULL) {
        // ziplistNext获取当前遍历元素的score entry位置。
        sptr = ziplistNext(zl,eptr);
        serverAssert(sptr != NULL);

        // 提取score entry中的score值。
        score = zzlGetScore(sptr);
        if (zslValueGteMin(score,range)) {
            // 如果score大于(或等于)range最小值，则我们还要检查score是否小于(或等于)最大值。
            // 都满足则说明该score在range范围内，我们返回当前遍历到的entry指针。
            /* Check if score <= max. */
            if (zslValueLteMax(score,range))
                return eptr;
            // 当score大于(或等于)range最小值，但也大于(或等于)最大值，说明我们超出了range范围，没有元素在range中，返回NULL。
            // 例如：zset的score为{1,2,8,9}，但我们查询的range为[4,6]。这就对应了这种情况。
            return NULL;
        }

        /* Move to next element. */
        // 如果元素score小于(或等于)range最小值，则我们移动到下一个entry检查（因为是score升序排列的，后面的score肯定不小于当前score）。
        eptr = ziplistNext(zl,sptr);
    }

    // 遍历到结束，所有的元素的score都小于(或等于)range最小值，说明没有元素在range范围中，返回NULL。
    // NOTE：这种情况在zzlIsInRange已处理，所以函数应该不会走到这里？
    return NULL;
}

/* Find pointer to the last element contained in the specified range.
 * Returns NULL when no element is contained in the range. */
// 找到ziplist编码的zset中最后一个在指定range中的元素位置指针返回。如果没有元素在range中，则返回NULL。
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec *range) {
    // eptr定位到最后一个元素的ele entry位置。
    unsigned char *eptr = ziplistIndex(zl,-2), *sptr;
    double score;

    /* If everything is out of range, return early. */
    // zzlIsInRange来判断是否有元素在range中，如果没有，则直接返回NULL。
    if (!zzlIsInRange(zl,range)) return NULL;

    // 假设有元素在range中，我们从后往前遍历，对每个元素检查是否在range中，一旦找到在range中的元素就立即返回。
    while (eptr != NULL) {
        // ziplistNext定位到当前处理原的score entry位置。
        sptr = ziplistNext(zl,eptr);
        serverAssert(sptr != NULL);

        // 提取当前处理元素的score
        score = zzlGetScore(sptr);
        if (zslValueLteMax(score,range)) {
            // 如果score小于(或等于)range的最大值，且大于(或等于)range的最小值，则在range中，直接返回当前元素位置。
            /* Check if score >= min. */
            if (zslValueGteMin(score,range))
                return eptr;
            // 如果score小于(或等于)range的最大值，同时也小于(或等于)range的最小值，
            // 则再往prev方向，所有元素都小于range最小值，我们不会再找到元素在range中了，所以直接返回NULL，表示没有元素在range中。
            // 例如：zset的score为{1,2,8,9}，但我们查询的range为[4,6]。这就对应了这种情况。
            return NULL;
        }

        /* Move to previous element by moving to the score of previous element.
         * When this returns NULL, we know there also is no element. */
        // 当前元素处理完了，跳到prev节点元素处理。
        // ziplistPrev获取前一个元素的score entry位置。如果为NULL，则说明我们没有元素了，则会跳出循环，最终返回NULL。
        // 如果不为NULL，则再使用ziplistPrev获取前一个元素的ele entry位置，用于下一轮的迭代。
        sptr = ziplistPrev(zl,eptr);
        if (sptr != NULL)
            serverAssert((eptr = ziplistPrev(zl,sptr)) != NULL);
        else
            eptr = NULL;
    }

    // 遍历到结束，所有的元素的score都大于(或等于)range最大值，说明没有元素在range范围中，返回NULL。
    // NOTE：这种情况在zzlIsInRange已处理，所以函数应该不会走到这里？
    return NULL;
}

// 从ziplist的某个entry位置解析数据为sds字符串，判断该字符串是否大于(或等于)指定range的最小值。
int zzlLexValueGteMin(unsigned char *p, zlexrangespec *spec) {
    // 解析entry数据为sds
    sds value = ziplistGetObject(p);
    // 对比该sds是否大于range的最小值
    int res = zslLexValueGteMin(value,spec);
    sdsfree(value);
    return res;
}

// 从ziplist的某个entry位置解析数据为sds字符串，判断该字符串是否小于(或等于)指定range的最大值。
int zzlLexValueLteMax(unsigned char *p, zlexrangespec *spec) {
    // 解析entry数据为sds
    sds value = ziplistGetObject(p);
    // 对比该sds是否小于range的最大值
    int res = zslLexValueLteMax(value,spec);
    sdsfree(value);
    return res;
}

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInRange and zzlLastInRange. */
// 判断zset中元素是否在lex range中。该函数只应该在zzlFirstInRange 和 zzlLastInRange内部调用。
int zzlIsInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *p;

    /* Test for ranges that will always be empty. */
    // 检查如果range为空则直接返回
    int cmp = sdscmplex(range->min,range->max);
    if (cmp > 0 || (cmp == 0 && (range->minex || range->maxex)))
        return 0;

    // 检查最后一个元素的ele值，如果小于range的最小值，则所有元素的ele都小于range最小值，没有元素在range中。
    p = ziplistIndex(zl,-2); /* Last element. */
    if (p == NULL) return 0;
    if (!zzlLexValueGteMin(p,range))
        return 0;

    // 检查第一个元素的ele值，如果大于range的最大值，则所有元素的ele都大于range最大值，没有元素在range中。
    p = ziplistIndex(zl,0); /* First element. */
    serverAssert(p != NULL);
    if (!zzlLexValueLteMax(p,range))
        return 0;

    // 最后返回1，表示特殊情况判断完了，我们需要进一步判断。
    return 1;
}

/* Find pointer to the first element contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
// 找到zipilist编码的zset中，第一个在指定lex range中的元素。没有元素在range中时，返回NULL。
// 注意需要所有score都相同，这样ele才是有序的，才能使用这个方法。
unsigned char *zzlFirstInLexRange(unsigned char *zl, zlexrangespec *range) {
    // 获取到第一个元素的ele entry位置
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;

    /* If everything is out of range, return early. */
    // zzlIsInLexRange检查是否有元素在range中，如果没有，则直接返回
    if (!zzlIsInLexRange(zl,range)) return NULL;

    // 从第一个元素依次向后遍历
    while (eptr != NULL) {
        // 判断当前遍历到的元素ele是否开始进入range范围内
        if (zzlLexValueGteMin(eptr,range)) {
            /* Check if ele <= max. */
            if (zzlLexValueLteMax(eptr,range))
                // 如果ele>=min，且ele<=max，首次到这里则当前是第一个在range中的元素，返回该元素。
                return eptr;
            // 如果ele>=min，但ele>max，时没有元素在range中，直接返回NULL。
            // 此时zset ele的最大最小值所在的区间，完全包含range区间，但由于zset ele是离散的，可能存储range区间不包含任何一个元素。
            return NULL;
        }

        /* Move to next element. */
        // 跳到下一个元素的ele。第一个ziplistNext会跳到当前元素的score entry。第二个ziplistNext才会到下一个元素的ele。
        sptr = ziplistNext(zl,eptr); /* This element score. Skip it. */
        serverAssert(sptr != NULL);
        eptr = ziplistNext(zl,sptr); /* Next element. */
    }

    // 遍历到结束，所有的元素的ele都小于(或等于)range最小值，说明没有元素在range范围中，返回NULL。
    // NOTE：这种情况在zzlIsInLexRange已处理，所以函数应该不会走到这里？
    return NULL;
}

/* Find pointer to the last element contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
// 找到zipilist编码的zset中，第一个在指定lex range中的元素。没有元素在range中时返回NULL。
// 注意需要所有score都相同，这样ele才是有序的，才能使用这个方法。
unsigned char *zzlLastInLexRange(unsigned char *zl, zlexrangespec *range) {
    // eptr初始指向最后一个元素的ele
    unsigned char *eptr = ziplistIndex(zl,-2), *sptr;

    /* If everything is out of range, return early. */
    // zzlIsInLexRange检查如果没有元素在range中，则直接返回
    if (!zzlIsInLexRange(zl,range)) return NULL;

    // 依次往prev遍历元素的ele
    while (eptr != NULL) {
        if (zzlLexValueLteMax(eptr,range)) {
            /* Check if ele >= min. */
            if (zzlLexValueGteMin(eptr,range))
                // 如果ele<=max，且ele>=min，从后往前遍历的，首次到这里则是最后一个在range中的元素，返回该元素。
                return eptr;
            // 如果ele<=max，但ele<max，时没有元素在range中，直接返回NULL。
            // 此时zset ele的最大最小值所在的区间，完全包含range区间，但由于zset ele是离散的，可能存储range区间不包含任何一个元素。
            return NULL;
        }

        /* Move to previous element by moving to the ele of previous element.
         * When this returns NULL, we know there also is no element. */
        // 跳到前一个元素的ele。先使用ziplistPrev跳到前一个元素的score entry。
        // 如果得到的sptr为NULL，则我们知道前面没有节点了，即eptr也为NULL。否则再次调用ziplistPrev获取前一个元素的ele entry。
        sptr = ziplistPrev(zl,eptr);
        if (sptr != NULL)
            serverAssert((eptr = ziplistPrev(zl,sptr)) != NULL);
        else
            eptr = NULL;
    }

    // 遍历到结束，所有的元素的ele都小于(或等于)range最小值，说明没有元素在range范围中，返回NULL。
    // NOTE：这种情况在zzlIsInLexRange已处理，所以函数应该不会走到这里？
    return NULL;
}

// 从ziplist编码的zset中找到指定 ele 的 score。
// 存在则填入*score中，并返回指向该元素的指针。不存在则参数值和返回值都为NULL。
unsigned char *zzlFind(unsigned char *zl, sds ele, double *score) {
    // eptr指向第一个元素。
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;

    // 从前往后遍历
    while (eptr != NULL) {
        // 获取当前遍历到的元素的score entry。
        sptr = ziplistNext(zl,eptr);
        serverAssert(sptr != NULL);

        // 使用ziplistCompare对比当前元素的ele字符串是否传入字符串匹配。如果匹配则填充score值并返回当前元素的指针。
        if (ziplistCompare(eptr,(unsigned char*)ele,sdslen(ele))) {
            /* Matching element, pull out score. */
            if (score != NULL) *score = zzlGetScore(sptr);
            return eptr;
        }

        /* Move to next element. */
        // 当前元素bupip，跳到下一个元素进入下一轮循环。
        eptr = ziplistNext(zl,sptr);
    }
    // 没找到指定ele的元素，返回NULL
    return NULL;
}

/* Delete (element,score) pair from ziplist. Use local copy of eptr because we
 * don't want to modify the one given as argument. */
// 从ziplist中删除ele/score对。
unsigned char *zzlDelete(unsigned char *zl, unsigned char *eptr) {
    // 是否本地副本，因为我们不想修改参数指针数据。
    unsigned char *p = eptr;

    /* TODO: add function to ziplist API to delete N elements from offset. */
    // 删除eptr指向元素的ele和score entry
    zl = ziplistDelete(zl,&p);
    zl = ziplistDelete(zl,&p);
    return zl;
}

// 在ziplist指定eptr位置处插入ele/score元素。
unsigned char *zzlInsertAt(unsigned char *zl, unsigned char *eptr, sds ele, double score) {
    unsigned char *sptr;
    char scorebuf[128];
    int scorelen;
    size_t offset;

    // score由数字转为字符串
    scorelen = d2string(scorebuf,sizeof(scorebuf),score);
    if (eptr == NULL) {
        // 没有传eptr位置，则默认在尾部插入数据。
        zl = ziplistPush(zl,(unsigned char*)ele,sdslen(ele),ZIPLIST_TAIL);
        zl = ziplistPush(zl,(unsigned char*)scorebuf,scorelen,ZIPLIST_TAIL);
    } else {
        /* Keep offset relative to zl, as it might be re-allocated. */
        // 如果指定了位置，则在指定位置插入ele和score数据。
        // 注意因为要插入两个entry，这里插入ele时需要保存当前指针的offset，因为可能重新分配内存导致zl改变。这样确保能定位到score的插入位置。
        offset = eptr-zl;
        zl = ziplistInsert(zl,eptr,(unsigned char*)ele,sdslen(ele));
        eptr = zl+offset;

        /* Insert score after the element. */
        // 获取score插入位置，写入score。这里其实可以先写入score、再写入ele，这样就不需要调用ziplistNext来获取score位置了。
        serverAssert((sptr = ziplistNext(zl,eptr)) != NULL);
        zl = ziplistInsert(zl,sptr,(unsigned char*)scorebuf,scorelen);
    }
    return zl;
}

/* Insert (element,score) pair in ziplist. This function assumes the element is
 * not yet present in the list. */
// 往ziplist中插入(element,score)对。该函数假定ele元素当前不在列表中，这点应该由调用方检查dict来确保。
unsigned char *zzlInsert(unsigned char *zl, sds ele, double score) {
    // 指针指向第一个元素。
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;
    double s;

    // 从前往后遍历，找寻插入元素位置。我们元素是按score-ele升序排序的。
    while (eptr != NULL) {
        // 获取当前原色score entry指针。
        sptr = ziplistNext(zl,eptr);
        serverAssert(sptr != NULL);
        // 解析score
        s = zzlGetScore(sptr);

        if (s > score) {
            /* First element with score larger than score for element to be
             * inserted. This means we should take its spot in the list to
             * maintain ordering. */
            // 如果待插入元素的score小于当前元素的score，则插入位置即为当前元素位置，处理元素插入
            zl = zzlInsertAt(zl,eptr,ele,score);
            break;
        } else if (s == score) {
            /* Ensure lexicographical ordering for elements. */
            // 如果待插入元素的score等于当前元素的score，且待插入元素的ele<当前元素的ele，则插入位置为当前位置，处理元素插入
            if (zzlCompareElements(eptr,(unsigned char*)ele,sdslen(ele)) > 0) {
                zl = zzlInsertAt(zl,eptr,ele,score);
                break;
            }
        }

        /* Move to next element. */
        // 当前位置不满足插入情况，跳到下一个元素继续检查。
        eptr = ziplistNext(zl,sptr);
    }

    /* Push on tail of list when it was not yet inserted. */
    // 如果遍历到最后，都还没有位置插入的话，这里处理在尾部插入。可以优化在该函数的开始就判断这种情况，对于特殊情况可以避免总是遍历。
    if (eptr == NULL)
        zl = zzlInsertAt(zl,NULL,ele,score);
    return zl;
}

// 删除score在range内的元素。
unsigned char *zzlDeleteRangeByScore(unsigned char *zl, zrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    double score;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    // 找到在range中的第一个元素位置。
    eptr = zzlFirstInRange(zl,range);
    if (eptr == NULL) return zl;

    /* When the tail of the ziplist is deleted, eptr will point to the sentinel
     * byte and ziplistNext will return NULL. */
    // 向后遍历删除。当range范围内的元素删完时，循环结束。两种请：1、ziplist遍历完了，2、遇到了第一个大于range最大值的元素。
    while ((sptr = ziplistNext(zl,eptr)) != NULL) {
        score = zzlGetScore(sptr);
        if (zslValueLteMax(score,range)) {
            /* Delete both the element and the score. */
            // score小于range最大值，说明元素在range中，处理删除。
            zl = ziplistDelete(zl,&eptr);
            zl = ziplistDelete(zl,&eptr);
            num++;
        } else {
            /* No longer in range. */
            // 元素超出了range范围，跳出循环。
            break;
        }
    }

    // deleted返回删除元素数
    if (deleted != NULL) *deleted = num;
    return zl;
}

// 删除ele在range中的元素。
unsigned char *zzlDeleteRangeByLex(unsigned char *zl, zlexrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    // 第一个ele在range中的元素
    eptr = zzlFirstInLexRange(zl,range);
    if (eptr == NULL) return zl;

    /* When the tail of the ziplist is deleted, eptr will point to the sentinel
     * byte and ziplistNext will return NULL. */
    // 向后遍历删除所有ele在range中的元素
    while ((sptr = ziplistNext(zl,eptr)) != NULL) {
        if (zzlLexValueLteMax(eptr,range)) {
            /* Delete both the element and the score. */
            // 如果当前元素的ele小于range的最大值，则在range中，处理删除。
            zl = ziplistDelete(zl,&eptr);
            zl = ziplistDelete(zl,&eptr);
            num++;
        } else {
            /* No longer in range. */
            // 当前遍历的元素超出了range范围，跳出循环。
            break;
        }
    }

    // deleted返回删除的元素数。
    if (deleted != NULL) *deleted = num;
    return zl;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
// 删除rank在[start, end]间的元素。
unsigned char *zzlDeleteRangeByRank(unsigned char *zl, unsigned int start, unsigned int end, unsigned long *deleted) {
    // 总共删除的元素数
    unsigned int num = (end-start)+1;
    if (deleted) *deleted = num;
    // 根据rank start计算第一个待删元素在ziplist中的index=2*(start-1)，总共要删除的entry为2*num。乘以2是因为我们存储都是ele/score对。
    zl = ziplistDeleteRange(zl,2*(start-1),2*num);
    return zl;
}

/*-----------------------------------------------------------------------------
 * Common sorted set API
 *----------------------------------------------------------------------------*/

// 获取zset中总的元素个数。
unsigned long zsetLength(const robj *zobj) {
    unsigned long length = 0;
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        // ziplist编码，总的entry数除以2即为元素个数。
        length = zzlLength(zobj->ptr);
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        // skiplist编码，通过跳表的length或dict size都可以获取。
        length = ((const zset*)zobj->ptr)->zsl->length;
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return length;
}

// zset底层编码变化。可以从ziplist到skiplist，也可以反向转换。
void zsetConvert(robj *zobj, int encoding) {
    zset *zs;
    zskiplistNode *node, *next;
    sds ele;
    double score;

    if (zobj->encoding == encoding) return;
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        if (encoding != OBJ_ENCODING_SKIPLIST)
            serverPanic("Unknown target encoding");

        // 处理ziplist转为skiplist。首先创建跳表编码的zset结构，一个dict + 一个skiplist。
        zs = zmalloc(sizeof(*zs));
        zs->dict = dictCreate(&zsetDictType,NULL);
        zs->zsl = zslCreate();

        // 获取ziplist中第一个zset元素的ele/score位置，即遍历的初始位置。
        eptr = ziplistIndex(zl,0);
        serverAssertWithInfo(NULL,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);
        serverAssertWithInfo(NULL,zobj,sptr != NULL);

        // 从第一个元素开始往后遍历，处理元素插入ziplist。
        while (eptr != NULL) {
            // 获取当前遍历元素的ele/score值。
            score = zzlGetScore(sptr);
            serverAssertWithInfo(NULL,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));
            if (vstr == NULL)
                ele = sdsfromlonglong(vlong);
            else
                ele = sdsnewlen((char*)vstr,vlen);

            // 往ziplist编码的zset中插入遍历的ele/score元素。跳表和dict中都要加入该元素。
            node = zslInsert(zs->zsl,score,ele);
            serverAssert(dictAdd(zs->dict,ele,&node->score) == DICT_OK);
            // 遍历指针指向下一个ziplist中zset元素。
            zzlNext(zl,&eptr,&sptr);
        }

        // 元素添加到skiplist编码的zset完成。这里更新zset对象指向的数据及编码，注意要释放原ziplist的数据。
        zfree(zobj->ptr);
        zobj->ptr = zs;
        zobj->encoding = OBJ_ENCODING_SKIPLIST;
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        // zset跳表编码转ziplist编码，这里先创建新的ziplist。
        unsigned char *zl = ziplistNew();

        if (encoding != OBJ_ENCODING_ZIPLIST)
            serverPanic("Unknown target encoding");

        /* Approach similar to zslFree(), since we want to free the skiplist at
         * the same time as creating the ziplist. */
        // 获取跳表中第1层的第一个元素的指针，然后我们只需要next遍历处理就可以了。
        // 这里处理释放掉其他不需要使用的结构，如跳表、跳表头结构，还有dict结构。
        zs = zobj->ptr;
        dictRelease(zs->dict);
        node = zs->zsl->header->level[0].forward;
        zfree(zs->zsl->header);
        zfree(zs->zsl);

        // 从前往后遍历，挨个元素加入到ziplist的末尾。
        while (node) {
            // 当前遍历到的元素加入到ziplist的末尾
            zl = zzlInsertAt(zl,NULL,node->ele,node->score);
            // 跳到下一个节点继续处理，并释放当前已处理的节点。
            next = node->level[0].forward;
            zslFreeNode(node);
            node = next;
        }

        // zset对象的底层数据替换，并更新编码。
        zfree(zs);
        zobj->ptr = zl;
        zobj->encoding = OBJ_ENCODING_ZIPLIST;
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

/* Convert the sorted set object into a ziplist if it is not already a ziplist
 * and if the number of elements and the maximum element size is within the
 * expected ranges. */
// 判断当前zset是否需要从跳表结构转为ziplist结构存储，如果需要则处理。
void zsetConvertToZiplistIfNeeded(robj *zobj, size_t maxelelen) {
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) return;
    zset *zset = zobj->ptr;

    if (zset->zsl->length <= server.zset_max_ziplist_entries &&
        maxelelen <= server.zset_max_ziplist_value)
        // 如果总的元素数和最大元素长度在ziplist编码条件内，则我们可以处理使用ziplist来编码当前zset。
            zsetConvert(zobj,OBJ_ENCODING_ZIPLIST);
}

/* Return (by reference) the score of the specified member of the sorted set
 * storing it into *score. If the element does not exist C_ERR is returned
 * otherwise C_OK is returned and *score is correctly populated.
 * If 'zobj' or 'member' is NULL, C_ERR is returned. */
// 获取zset对象中指定member的score返回。如果元素存在则返回ok，且*score填充分数返回，否则访问err。
int zsetScore(robj *zobj, sds member, double *score) {
    if (!zobj || !member) return C_ERR;

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        // ziplist编码，调用zzlFind来获取元素的score。
        if (zzlFind(zobj->ptr, member, score) == NULL) return C_ERR;
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        // skiplist编码，直接从dict中获取。
        zset *zs = zobj->ptr;
        dictEntry *de = dictFind(zs->dict, member);
        if (de == NULL) return C_ERR;
        *score = *(double*)dictGetVal(de);
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return C_OK;
}

/* Add a new element or update the score of an existing element in a sorted
 * set, regardless of its encoding.
 *
 * The set of flags change the command behavior. 
 *
 * The input flags are the following:
 *
 * ZADD_INCR: Increment the current element score by 'score' instead of updating
 *            the current element score. If the element does not exist, we
 *            assume 0 as previous score.
 * ZADD_NX:   Perform the operation only if the element does not exist.
 * ZADD_XX:   Perform the operation only if the element already exist.
 * ZADD_GT:   Perform the operation on existing elements only if the new score is 
 *            greater than the current score.
 * ZADD_LT:   Perform the operation on existing elements only if the new score is 
 *            less than the current score.
 *
 * When ZADD_INCR is used, the new score of the element is stored in
 * '*newscore' if 'newscore' is not NULL.
 *
 * The returned flags are the following:
 *
 * ZADD_NAN:     The resulting score is not a number.
 * ZADD_ADDED:   The element was added (not present before the call).
 * ZADD_UPDATED: The element score was updated.
 * ZADD_NOP:     No operation was performed because of NX or XX.
 *
 * Return value:
 *
 * The function returns 1 on success, and sets the appropriate flags
 * ADDED or UPDATED to signal what happened during the operation (note that
 * none could be set if we re-added an element using the same score it used
 * to have, or in the case a zero increment is used).
 *
 * The function returns 0 on error, currently only when the increment
 * produces a NAN condition, or when the 'score' value is NAN since the
 * start.
 *
 * The command as a side effect of adding a new element may convert the sorted
 * set internal encoding from ziplist to hashtable+skiplist.
 *
 * Memory management of 'ele':
 *
 * The function does not take ownership of the 'ele' SDS string, but copies
 * it if needed. */
// 往zset中增加一个新元素或更新已有元素的score值。函数会根据zset底层的编码来适配到不同的处理方法。
// 传入的in_flags会改变函数的行为：
//      ZADD_INCR，对指定ele元素的分数增加score，而不是直接覆盖更新。如果元素不存在，则我们假定之前分数为0。
//      ZADD_NX，仅当ele元素不存在时才执行操作，即只会add不会更新。
//      ZADD_XX，仅当ele元素存在是才执行操作，即只会更新而不会add。
//      ZADD_GT，只有当传入的score大于原分数时，才执行操作。
//      ZADD_LT，只有当传入的score小于原分数时，才执行操作。
// 当有使用ZADD_INCR时，更新后的分数会通过newscore返回（如果newscore不是NULL的话）。
// 返回的out_flags如下：
//      ZADD_NAN，返回的score不是一个数，溢出了。
//      ZADD_ADDED，元素被添加进去（该元素之前不存在）。
//      ZADD_UPDATED，元素score被更新。
//      ZADD_NOP，表示没有执行操作，用于NX、XX。
// 函数返回1表示成功，并在out_flags设置适当的ADDED或UPDATED标识指示我们执行的操作。函数失败返回0，目前只有分数变为NAN这种情况。
// 注意如果我们使用相同的score去更新或者incr 0更新，flags将不会设置，表示我们什么都没操作。
// 函数有一个副作用，当添加一个元素时，可能导致底层编码从ziplist转为hash表+skiplist形式。
// 本函数不会接管ele内存管理，需要的时候是copy使用的，所以调用者需要处理ele字符串的释放。
int zsetAdd(robj *zobj, double score, sds ele, int in_flags, int *out_flags, double *newscore) {
    /* Turn options into simple to check vars. */
    // 解析标识，转为简单变量，后面使用更方便
    int incr = (in_flags & ZADD_IN_INCR) != 0;
    int nx = (in_flags & ZADD_IN_NX) != 0;
    int xx = (in_flags & ZADD_IN_XX) != 0;
    int gt = (in_flags & ZADD_IN_GT) != 0;
    int lt = (in_flags & ZADD_IN_LT) != 0;
    *out_flags = 0; /* We'll return our response flags. */
    double curscore;

    /* NaN as input is an error regardless of all the other parameters. */
    // 确保传入的score不能是NAN。
    if (isnan(score)) {
        *out_flags = ZADD_OUT_NAN;
        return 0;
    }

    /* Update the sorted set according to its encoding. */
    // 根据底层编码，不同方式更新zset中指定元素。
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *eptr;

        // ziplist编码，首先根据ele查到当前score。
        if ((eptr = zzlFind(zobj->ptr,ele,&curscore)) != NULL) {
            /* NX? Return, same element already exists. */
            // 查到了元素，如果我们传入标识是NX，没有元素才插入，那么这里直接返回。
            if (nx) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            /* Prepare the score for the increment if needed. */
            // 如果是incr，表示增加score值。这里确保增加后的score不是NAN，如果是则返回0。
            if (incr) {
                score += curscore;
                if (isnan(score)) {
                    *out_flags |= ZADD_OUT_NAN;
                    return 0;
                }
            }

            /* GT/LT? Only update if score is greater/less than current. */
            // 如果有GT/LT标识，则我们只在条件满足时更新。这里判断条件不满足则直接返回。
            if ((lt && score >= curscore) || (gt && score <= curscore)) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            // 可以更新了，这里先填充函数返回值为新的score。
            if (newscore) *newscore = score;

            /* Remove and re-insert when score changed. */
            // 如果score有变化，则写入ziplist中。我们先delete，再插入。理论上直接更新score值更好吧。
            if (score != curscore) {
                zobj->ptr = zzlDelete(zobj->ptr,eptr);
                zobj->ptr = zzlInsert(zobj->ptr,ele,score);
                *out_flags |= ZADD_OUT_UPDATED;
            }
            return 1;
        } else if (!xx) {
            /* Optimize: check if the element is too large or the list
             * becomes too long *before* executing zzlInsert. */
            // 如果没有查到，且标识不是XX（即不是存在才处理），则我们要处理新增元素插入。
            zobj->ptr = zzlInsert(zobj->ptr,ele,score);
            // 检查如果需要将ziplist转为skiplist，则处理convert。
            if (zzlLength(zobj->ptr) > server.zset_max_ziplist_entries ||
                sdslen(ele) > server.zset_max_ziplist_value)
                zsetConvert(zobj,OBJ_ENCODING_SKIPLIST);
            // 填充数据返回
            if (newscore) *newscore = score;
            *out_flags |= ZADD_OUT_ADDED;
            return 1;
        } else {
            // 没查到，如果有xx标识，存在才处理，不存在我们直接返回。
            *out_flags |= ZADD_OUT_NOP;
            return 1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplistNode *znode;
        dictEntry *de;

        // skiplist编码，先从dict查询该元素是否存在。
        de = dictFind(zs->dict,ele);
        if (de != NULL) {
            /* NX? Return, same element already exists. */
            // 元素存在，如果是NX标识，则直接返回。
            if (nx) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            // 获取score
            curscore = *(double*)dictGetVal(de);

            /* Prepare the score for the increment if needed. */
            // 处理score incr，并确保最终值不为NAN
            if (incr) {
                score += curscore;
                if (isnan(score)) {
                    *out_flags |= ZADD_OUT_NAN;
                    return 0;
                }
            }

            /* GT/LT? Only update if score is greater/less than current. */
            // 如果有GT/LT标识，则我们只在条件满足时更新。这里判断条件不满足则直接返回。
            if ((lt && score >= curscore) || (gt && score <= curscore)) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            if (newscore) *newscore = score;

            /* Remove and re-insert when score changes. */
            // 当score改变时，我们处理更新
            if (score != curscore) {
                // 更新skiplist中元素的score
                znode = zslUpdateScore(zs->zsl,curscore,ele,score);
                /* Note that we did not removed the original element from
                 * the hash table representing the sorted set, so we just
                 * update the score. */
                // 更新dict中元素的score。
                dictGetVal(de) = &znode->score; /* Update score ptr. */
                *out_flags |= ZADD_OUT_UPDATED;
            }
            return 1;
        } else if (!xx) {
            // 不存在元素，没有XX标识，我们要处理插入该元素。跳表和hash表中都要添加。
            ele = sdsdup(ele);
            znode = zslInsert(zs->zsl,score,ele);
            serverAssert(dictAdd(zs->dict,ele,&znode->score) == DICT_OK);
            *out_flags |= ZADD_OUT_ADDED;
            if (newscore) *newscore = score;
            return 1;
        } else {
            // 元素不存在，且有XX标识，因为只有存在才操作，所以这里我们直接返回。
            *out_flags |= ZADD_OUT_NOP;
            return 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return 0; /* Never reached. */
}

/* Deletes the element 'ele' from the sorted set encoded as a skiplist+dict,
 * returning 1 if the element existed and was deleted, 0 otherwise (the
 * element was not there). It does not resize the dict after deleting the
 * element. */
// 从skiplist+dict编码的set中移除ele元素。如果元素存在且被删除了返回1，否则元素不存在返回0。删除元素后这里并不处理dict的resize。
static int zsetRemoveFromSkiplist(zset *zs, sds ele) {
    dictEntry *de;
    double score;

    // 使用unlink解除db关联，并获取到了待删除的元素，后面需要调用dictFreeUnlinkedEntry来处理真正删除。
    de = dictUnlink(zs->dict,ele);
    if (de != NULL) {
        /* Get the score in order to delete from the skiplist later. */
        // 元素存在需要处理删除。这里先获取score用于后面从跳表删除元素。
        score = *(double*)dictGetVal(de);

        /* Delete from the hash table and later from the skiplist.
         * Note that the order is important: deleting from the skiplist
         * actually releases the SDS string representing the element,
         * which is shared between the skiplist and the hash table, so
         * we need to delete from the skiplist as the final step. */
        // 先从hash表中删除元素，然后再从跳表删除元素。注意这个顺序非常重要：
        // 从跳表中删除元素实际上会释放ele数据字符串，而ele是跳表和hash表共享的，所以我们需要最后来删除跳表元素。
        // 注意zset的dict类型中key、value销毁函数都为NULL，表示不会做k、v的释放工作。
        dictFreeUnlinkedEntry(zs->dict,de);

        /* Delete from skiplist. */
        // 从跳表中删除元素。真正的释放ele。
        int retval = zslDelete(zs->zsl,score,ele,NULL);
        serverAssert(retval);

        return 1;
    }

    return 0;
}

/* Delete the element 'ele' from the sorted set, returning 1 if the element
 * existed and was deleted, 0 otherwise (the element was not there). */
// 从zset中删除ele元素。存在且删除了返回1，不存在返回0
int zsetDel(robj *zobj, sds ele) {
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *eptr;

        // ziplist编码，先使用zzlFind查询，在使用zzlDelete删除元素的ele/score entry。
        if ((eptr = zzlFind(zobj->ptr,ele,NULL)) != NULL) {
            zobj->ptr = zzlDelete(zobj->ptr,eptr);
            return 1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        // skiplist+dict编码，直接使用前面的zsetRemoveFromSkiplist移除元素。
        zset *zs = zobj->ptr;
        if (zsetRemoveFromSkiplist(zs, ele)) {
            // 这里成功移除元素检查处理resize。
            if (htNeedsResize(zs->dict)) dictResize(zs->dict);
            return 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return 0; /* No such element found. */
}

/* Given a sorted set object returns the 0-based rank of the object or
 * -1 if the object does not exist.
 *
 * For rank we mean the position of the element in the sorted collection
 * of elements. So the first element has rank 0, the second rank 1, and so
 * forth up to length-1 elements.
 *
 * If 'reverse' is false, the rank is returned considering as first element
 * the one with the lowest score. Otherwise if 'reverse' is non-zero
 * the rank is computed considering as element with rank 0 the one with
 * the highest score. */
// 查询zset中指定ele元素的rank值，注意返回给client和从client获取的rank都是从0开始的，我们服务内存要处理转换。
// 如果元素存在，则返回0开始的rank值；元素不存在则返回-1。
// 当reverse为0时，返回由小到大正向的rank值；如果reverse非0，则从大到小逆向rank值。
long zsetRank(robj *zobj, sds ele, int reverse) {
    unsigned long llen;
    unsigned long rank;

    llen = zsetLength(zobj);

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        // ziplist编码，获取第一个元素
        eptr = ziplistIndex(zl,0);
        serverAssert(eptr != NULL);
        sptr = ziplistNext(zl,eptr);
        serverAssert(sptr != NULL);

        rank = 1;
        // 从前往后遍历，直到找到元素，或者遍历到结尾没找到，则跳出循环
        while(eptr != NULL) {
            if (ziplistCompare(eptr,(unsigned char*)ele,sdslen(ele)))
                break;
            rank++;
            zzlNext(zl,&eptr,&sptr);
        }

        if (eptr != NULL) {
            // 正向是rank-1，逆向是llen-rank。
            if (reverse)
                return llen-rank;
            else
                return rank-1;
        } else {
            return -1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        dictEntry *de;
        double score;

        // skiplist+dict，直接从dict中查询元素。
        de = dictFind(zs->dict,ele);
        if (de != NULL) {
            // 元素存在，则提取score，并从skiplist中获取元素的rank。
            score = *(double*)dictGetVal(de);
            rank = zslGetRank(zsl,score,ele);
            /* Existing elements always have a rank. */
            serverAssert(rank != 0);
            // 正向是rank-1，逆向是llen-rank。
            if (reverse)
                return llen-rank;
            else
                return rank-1;
        } else {
            return -1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

/* This is a helper function for the COPY command.
 * Duplicate a sorted set object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * The resulting object always has refcount set to 1 */
// COPY命令的辅助函数。复制一个zset对象，保证底层编码与原对象一致。复制出来的对象refcount为1。
robj *zsetDup(robj *o) {
    robj *zobj;
    zset *zs;
    zset *new_zs;

    serverAssert(o->type == OBJ_ZSET);

    /* Create a new sorted set object that have the same encoding as the original object's encoding */
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        // ziplist编码，数据连续存储的，直接分配内存，使用memcpy复制数据。然后基于新的ziplist构建zset对象。
        unsigned char *zl = o->ptr;
        size_t sz = ziplistBlobLen(zl);
        unsigned char *new_zl = zmalloc(sz);
        memcpy(new_zl, zl, sz);
        zobj = createObject(OBJ_ZSET, new_zl);
        zobj->encoding = OBJ_ENCODING_ZIPLIST;
    } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
        // skiplist+dict，先创建新的zset对象，然后遍历原来的跳表，写入元素到新的zset中。
        zobj = createZsetObject();
        zs = o->ptr;
        new_zs = zobj->ptr;
        dictExpand(new_zs->dict,dictSize(zs->dict));
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        sds ele;
        long llen = zsetLength(o);

        /* We copy the skiplist elements from the greatest to the
         * smallest (that's trivial since the elements are already ordered in
         * the skiplist): this improves the load process, since the next loaded
         * element will always be the smaller, so adding to the skiplist
         * will always immediately stop at the head, making the insertion
         * O(1) instead of O(log(N)). */
        // 我们从尾部遍历跳表结构处理，因为这样我们就是从大到小插入元素的，从而插入操作总是在跳表的头部，O(1)。
        ln = zsl->tail;
        // 总共llen个元素，遍历处理llen次
        while (llen--) {
            ele = ln->ele;
            sds new_ele = sdsdup(ele);
            // ziplist插入元素，然后在写入dict。
            zskiplistNode *znode = zslInsert(new_zs->zsl,ln->score,new_ele);
            dictAdd(new_zs->dict,new_ele,&znode->score);
            // 往backward遍历。
            ln = ln->backward;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return zobj;
}

/* callback for to check the ziplist doesn't have duplicate recoreds */
// 回调函数，用于检查ziplist中没有重复的记录。
static int _zsetZiplistValidateIntegrity(unsigned char *p, void *userdata) {
    struct {
        long count;
        dict *fields;
    } *data = userdata;

    /* Even records are field names, add to dict and check that's not a dup */
    // 只检查zset的ele是否重复，所以只需要看偶数索引的entry
    if (((data->count) & 1) == 0) {
        unsigned char *str;
        unsigned int slen;
        long long vll;
        // 获取ele数据
        if (!ziplistGet(p, &str, &slen, &vll))
            return 0;
        sds field = str? sdsnewlen(str, slen): sdsfromlonglong(vll);;
        // 将ele数据加入到fields集合中，用于判重。
        if (dictAdd(data->fields, field, NULL) != DICT_OK) {
            /* Duplicate, return an error */
            sdsfree(field);
            return 0;
        }
    }

    // count返回总的entry数量，需要检查它等于2倍的zset元素。
    (data->count)++;
    return 1;
}

/* Validate the integrity of the data structure.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we scan all the entries one by one. */
// 验证zset的ziplist结构是否完成。deep为0表示只验证header，非0表示验证所有元素。
int zsetZiplistValidateIntegrity(unsigned char *zl, size_t size, int deep) {
    if (!deep)
        // 只验证header
        return ziplistValidateIntegrity(zl, size, 0, NULL, NULL);

    /* Keep track of the field names to locate duplicate ones */
    struct {
        long count;
        dict *fields;
    } data = {0, dictCreate(&hashDictType, NULL)};

    // 验证所有entry，传入_zsetZiplistValidateIntegrity回调函数，检查ele是否重复。
    int ret = ziplistValidateIntegrity(zl, size, 1, _zsetZiplistValidateIntegrity, &data);

    /* make sure we have an even number of records. */
    // 确保ziplist中的entry数量为偶数
    if (data.count & 1)
        ret = 0;

    dictRelease(data.fields);
    return ret;
}

/* Create a new sds string from the ziplist entry. */
// 使用ziplist entry中数据构建一个sds字符串。
sds zsetSdsFromZiplistEntry(ziplistEntry *e) {
    return e->sval ? sdsnewlen(e->sval, e->slen) : sdsfromlonglong(e->lval);
}

/* Reply with bulk string from the ziplist entry. */
// 根据ziplist entry数据，构建一个bulk string回复。
void zsetReplyFromZiplistEntry(client *c, ziplistEntry *e) {
    if (e->sval)
        addReplyBulkCBuffer(c, e->sval, e->slen);
    else
        addReplyBulkLongLong(c, e->lval);
}


/* Return random element from a non empty zset.
 * 'key' and 'score' will be set to hold the element.
 * The memory in `key` is not to be freed or modified by the caller.
 * 'score' can be NULL in which case it's not extracted. */
// 从一个非空的zset中返回一个随机的元素。key和score（如果非NULL）用来返回元素数据。调用者不会修改或释放key存储。
void zsetTypeRandomElement(robj *zsetobj, unsigned long zsetsize, ziplistEntry *key, double *score) {
    if (zsetobj->encoding == OBJ_ENCODING_SKIPLIST) {
        // skiplist+dict，我们可以直接从dict来随机获取元素。使用dictGetFairRandomKey。
        zset *zs = zsetobj->ptr;
        dictEntry *de = dictGetFairRandomKey(zs->dict);
        sds s = dictGetKey(de);
        key->sval = (unsigned char*)s;
        key->slen = sdslen(s);
        if (score)
            *score = *(double*)dictGetVal(de);
    } else if (zsetobj->encoding == OBJ_ENCODING_ZIPLIST) {
        // ziplist编码，使用ziplistRandomPair来获取随机元素。
        ziplistEntry val;
        ziplistRandomPair(zsetobj->ptr, zsetsize, key, &val);
        if (score) {
            if (val.sval) {
                *score = zzlStrtod(val.sval,val.slen);
            } else {
                *score = (double)val.lval;
            }
        }
    } else {
        serverPanic("Unknown zset encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Sorted set commands
 *----------------------------------------------------------------------------*/

/* This generic command implements both ZADD and ZINCRBY. */
// ZADD 和 ZINCRBY命令的通用实现方法。
void zaddGenericCommand(client *c, int flags) {
    static char *nanerr = "resulting score is not a number (NaN)";
    robj *key = c->argv[1];
    robj *zobj;
    sds ele;
    double score = 0, *scores = NULL;
    int j, elements, ch = 0;
    int scoreidx = 0;
    size_t maxelelen = 0;
    /* The following vars are used in order to track what the command actually
     * did during the execution, to reply to the client and to trigger the
     * notification of keyspace change. */
    // 下面几个变量用于最终命令实际执行情况，用于回复给client，也用于触发key空间变化的事件通知。
    int added = 0;      /* Number of new elements added. */
    int updated = 0;    /* Number of elements with updated score. */
    int processed = 0;  /* Number of elements processed, may remain zero with
                           options like XX. */

    /* Parse options. At the end 'scoreidx' is set to the argument position
     * of the score of the first score-element pair. */
    // 从参数中解析flags信息。注意这里当解析完flags信息时，scoreidx索引正好是score-element对的score参数位置。
    scoreidx = 2;
    while(scoreidx < c->argc) {
        char *opt = c->argv[scoreidx]->ptr;
        if (!strcasecmp(opt,"nx")) flags |= ZADD_IN_NX;
        else if (!strcasecmp(opt,"xx")) flags |= ZADD_IN_XX;
        else if (!strcasecmp(opt,"ch")) ch = 1; /* Return num of elements added or updated. */
        else if (!strcasecmp(opt,"incr")) flags |= ZADD_IN_INCR;
        else if (!strcasecmp(opt,"gt")) flags |= ZADD_IN_GT;
        else if (!strcasecmp(opt,"lt")) flags |= ZADD_IN_LT;
        else break;
        scoreidx++;
    }

    /* Turn options into simple to check vars. */
    // 把flags选项转为变量，后面更方便使用。
    int incr = (flags & ZADD_IN_INCR) != 0;
    int nx = (flags & ZADD_IN_NX) != 0;
    int xx = (flags & ZADD_IN_XX) != 0;
    int gt = (flags & ZADD_IN_GT) != 0;
    int lt = (flags & ZADD_IN_LT) != 0;

    /* After the options, we expect to have an even number of args, since
     * we expect any number of score-element pairs. */
    // 这里确保我们score-element参数是成对出现的，且至少有一个score-element元素。
    elements = c->argc-scoreidx;
    if (elements % 2 || !elements) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }
    elements /= 2; /* Now this holds the number of score-element pairs. */

    /* Check for incompatible options. */
    // 检查flags是否兼容
    if (nx && xx) {
        addReplyError(c,
            "XX and NX options at the same time are not compatible");
        return;
    }
    
    if ((gt && nx) || (lt && nx) || (gt && lt)) {
        addReplyError(c,
            "GT, LT, and/or NX options at the same time are not compatible");
        return;
    }
    /* Note that XX is compatible with either GT or LT */

    // incr只支持一个元素的操作
    if (incr && elements > 1) {
        addReplyError(c,
            "INCR option supports a single increment-element pair");
        return;
    }

    /* Start parsing all the scores, we need to emit any syntax error
     * before executing additions to the sorted set, as the command should
     * either execute fully or nothing at all. */
    // 检查参数中所有的scores值是否合法，这个需要在处理任何一个元素前检查，因为我们要保证所有元素执行的原子性。
    scores = zmalloc(sizeof(double)*elements);
    for (j = 0; j < elements; j++) {
        // 遍历提取score依次放到scores数组中
        if (getDoubleFromObjectOrReply(c,c->argv[scoreidx+j*2],&scores[j],NULL)
            != C_OK) goto cleanup;
        if (maxelelen < sdslen(c->argv[scoreidx+j*2+1]->ptr))
            maxelelen = sdslen(c->argv[scoreidx+j*2+1]->ptr);
    }

    /* Lookup the key and create the sorted set if does not exist. */
    // 从db获取对象，并确保对象是zset类型。
    zobj = lookupKeyWrite(c->db,key);
    if (checkType(c,zobj,OBJ_ZSET)) goto cleanup;
    if (zobj == NULL) {
        // 如果zset对象我们需要新建对象。根据我们操作的总元素数，以及元素最大长度来判断是使用ziplist还是hash+skiplist结构。
        if (xx) goto reply_to_client; /* No key + XX option: nothing to do. */
        if (server.zset_max_ziplist_entries < elements ||
            server.zset_max_ziplist_value < maxelelen)
        {
            zobj = createZsetObject();
        } else {
            zobj = createZsetZiplistObject();
        }
        dbAdd(c->db,key,zobj);
    }

    // 遍历要处理的元素加入到zset中。
    for (j = 0; j < elements; j++) {
        double newscore;
        score = scores[j];
        int retflags = 0;

        ele = c->argv[scoreidx+1+j*2]->ptr;
        // 调用zsetAdd函数处理
        int retval = zsetAdd(zobj, score, ele, flags, &retflags, &newscore);
        if (retval == 0) {
            addReplyError(c,nanerr);
            goto cleanup;
        }
        // 返回flags解析
        if (retflags & ZADD_OUT_ADDED) added++;
        if (retflags & ZADD_OUT_UPDATED) updated++;
        if (!(retflags & ZADD_OUT_NOP)) processed++;
        score = newscore;
    }
    server.dirty += (added+updated);

reply_to_client:
    if (incr) { /* ZINCRBY or INCR option. */
        // ZINCRBY 和 INCR 回复。
        if (processed)
            addReplyDouble(c,score);
        else
            addReplyNull(c);
    } else { /* ZADD. */
        // ZADD回复
        addReplyLongLong(c,ch ? added+updated : added);
    }

cleanup:
    zfree(scores);
    // 处理key变更通知
    if (added || updated) {
        signalModifiedKey(c,c->db,key);
        notifyKeyspaceEvent(NOTIFY_ZSET,
            incr ? "zincr" : "zadd", key, c->db->id);
    }
}

// ZADD key [NX|XX] [CH] [INCR] score member [score member ...]
void zaddCommand(client *c) {
    zaddGenericCommand(c,ZADD_IN_NONE);
}

// ZINCRBY key increment member
void zincrbyCommand(client *c) {
    zaddGenericCommand(c,ZADD_IN_INCR);
}

// ZREM key member [member ...]
void zremCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    int deleted = 0, keyremoved = 0, j;

    // 根据key查询对象，并确保对象是zset
    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    for (j = 2; j < c->argc; j++) {
        // 遍历删除的members，挨个处理del。如果zset中所有元素都被删除了，则从db中移除zset对象。
        if (zsetDel(zobj,c->argv[j]->ptr)) deleted++;
        if (zsetLength(zobj) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
            break;
        }
    }

    // 有删除则key有变动，处理key变更通知
    if (deleted) {
        notifyKeyspaceEvent(NOTIFY_ZSET,"zrem",key,c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
        signalModifiedKey(c,c->db,key);
        server.dirty += deleted;
    }
    // 回复删除的元素数
    addReplyLongLong(c,deleted);
}

typedef enum {
    ZRANGE_AUTO = 0,
    ZRANGE_RANK,
    ZRANGE_SCORE,
    ZRANGE_LEX,
} zrange_type;

/* Implements ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX commands. */
// 实现 ZREMRANGEBY* 命令的通用方法。
void zremrangeGenericCommand(client *c, zrange_type rangetype) {
    robj *key = c->argv[1];
    robj *zobj;
    int keyremoved = 0;
    unsigned long deleted = 0;
    zrangespec range;
    zlexrangespec lexrange;
    long start, end, llen;
    char *notify_type = NULL;

    /* Step 1: Parse the range. */
    // 解析各自命令的range参数
    if (rangetype == ZRANGE_RANK) {
        notify_type = "zremrangebyrank";
        if ((getLongFromObjectOrReply(c,c->argv[2],&start,NULL) != C_OK) ||
            (getLongFromObjectOrReply(c,c->argv[3],&end,NULL) != C_OK))
            return;
    } else if (rangetype == ZRANGE_SCORE) {
        notify_type = "zremrangebyscore";
        if (zslParseRange(c->argv[2],c->argv[3],&range) != C_OK) {
            addReplyError(c,"min or max is not a float");
            return;
        }
    } else if (rangetype == ZRANGE_LEX) {
        notify_type = "zremrangebylex";
        if (zslParseLexRange(c->argv[2],c->argv[3],&lexrange) != C_OK) {
            addReplyError(c,"min or max not valid string range item");
            return;
        }
    } else {
        serverPanic("unknown rangetype %d", (int)rangetype);
    }

    /* Step 2: Lookup & range sanity checks if needed. */
    // db中查询对象，并确保是zset。
    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) goto cleanup;

    // rank的range参数处理。负数索引换算为正数，并检查合法性。
    if (rangetype == ZRANGE_RANK) {
        /* Sanitize indexes. */
        llen = zsetLength(zobj);
        // 负数索引转换
        if (start < 0) start = llen+start;
        if (end < 0) end = llen+end;
        // start修正
        if (start < 0) start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        // 由前面处理有start>=0，所以start>end完全覆盖了end<0的情况。
        // 这里当start>end 或 start>=length时，范围是空的。
        if (start > end || start >= llen) {
            addReply(c,shared.czero);
            goto cleanup;
        }
        // end修正
        if (end >= llen) end = llen-1;
    }

    /* Step 3: Perform the range deletion operation. */
    // 执行range删除操作。
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        switch(rangetype) {
            // ziplist编码，调用不同的处理函数处理range删除。注意rank要转为从1开始。
        case ZRANGE_AUTO:
        case ZRANGE_RANK:
            zobj->ptr = zzlDeleteRangeByRank(zobj->ptr,start+1,end+1,&deleted);
            break;
        case ZRANGE_SCORE:
            zobj->ptr = zzlDeleteRangeByScore(zobj->ptr,&range,&deleted);
            break;
        case ZRANGE_LEX:
            zobj->ptr = zzlDeleteRangeByLex(zobj->ptr,&lexrange,&deleted);
            break;
        }
        // 删除元素后如果zset为空，则从db移除该对象
        if (zzlLength(zobj->ptr) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        switch(rangetype) {
            // hash+skiplist，调用不同函数处理range删除，rank要转为从1开始。
        case ZRANGE_AUTO:
        case ZRANGE_RANK:
            deleted = zslDeleteRangeByRank(zs->zsl,start+1,end+1,zs->dict);
            break;
        case ZRANGE_SCORE:
            deleted = zslDeleteRangeByScore(zs->zsl,&range,zs->dict);
            break;
        case ZRANGE_LEX:
            deleted = zslDeleteRangeByLex(zs->zsl,&lexrange,zs->dict);
            break;
        }
        // 删除元素后看是否需要处理resize。NOTE：这里需不需要转为ziplist？
        if (htNeedsResize(zs->dict)) dictResize(zs->dict);
        // 删除元素后如果zset为空，则从db移除该对象
        if (dictSize(zs->dict) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    /* Step 4: Notifications and reply. */
    // 如果删除了元素，处理key变更通知。
    if (deleted) {
        signalModifiedKey(c,c->db,key);
        notifyKeyspaceEvent(NOTIFY_ZSET,notify_type,key,c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
    }
    server.dirty += deleted;
    // 通知client删除条数。
    addReplyLongLong(c,deleted);

cleanup:
    // 释放lex range
    if (rangetype == ZRANGE_LEX) zslFreeLexRange(&lexrange);
}

// ZREMRANGEBYRANK key start stop，两端都包含，相当于自带闭区间。
void zremrangebyrankCommand(client *c) {
    zremrangeGenericCommand(c,ZRANGE_RANK);
}

// ZREMRANGEBYSCORE key min max，'('表示开区间，什么都不加表示闭区间。
void zremrangebyscoreCommand(client *c) {
    zremrangeGenericCommand(c,ZRANGE_SCORE);
}

// ZREMRANGEBYLEX key min max，'('表示开区间，'['表示闭区间。注意这个命令只能在score完全相同的zset使用，否则结果未定义。
void zremrangebylexCommand(client *c) {
    zremrangeGenericCommand(c,ZRANGE_LEX);
}

typedef struct {
    robj *subject;
    // 类型，set或zset
    int type; /* Set, sorted set */
    // 编码类型，决定后面迭代器类型
    int encoding;
    double weight;

    // 嵌套union，最终结构为4种迭代器的一个。
    union {
        /* Set iterators. */
        // set迭代器，可能是intset或者dict
        union _iterset {
            struct {
                intset *is;
                int ii;
            } is;
            struct {
                dict *dict;
                dictIterator *di;
                dictEntry *de;
            } ht;
        } set;

        /* Sorted set iterators. */
        // zset迭代器，可能是ziplist或dict
        union _iterzset {
            struct {
                unsigned char *zl;
                unsigned char *eptr, *sptr;
            } zl;
            struct {
                zset *zs;
                zskiplistNode *node;
            } sl;
        } zset;
    } iter;
} zsetopsrc;


/* Use dirty flags for pointers that need to be cleaned up in the next
 * iteration over the zsetopval. The dirty flag for the long long value is
 * special, since long long values don't need cleanup. Instead, it means that
 * we already checked that "ell" holds a long long, or tried to convert another
 * representation into a long long value. When this was successful,
 * OPVAL_VALID_LL is set as well. */
// 如果在zsetopval上的下一次迭代操作前需要清除相关指针，则要设置这些dirty标识。
// long long值的dirty标识比较特殊，因为它不需要清理。相反，它意味着我们以及检查了ell包含数字值，或者尝试转换为了数字值。
// 所以当我们知道有数字值且有效时，OPVAL_VALID_LL也会被设置。
#define OPVAL_DIRTY_SDS 1
#define OPVAL_DIRTY_LL 2
#define OPVAL_VALID_LL 4

/* Store value retrieved from the iterator. */
// 保存从迭代器获取到的数据
typedef struct {
    // dirty标识
    int flags;
    // 私有buf
    unsigned char _buf[32]; /* Private buffer. */
    // 字符串元素
    sds ele;
    // 指字符串元素的指针
    unsigned char *estr;
    // 元素长度
    unsigned int elen;
    // 元素为数字，这里为解析的值，通常我们会从ele或estr中解析数字。
    long long ell;
    // score分数
    double score;
} zsetopval;

typedef union _iterset iterset;
typedef union _iterzset iterzset;

// 初始化迭代器
void zuiInitIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == OBJ_SET) {
        // set类型
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            // 初始化intset迭代器
            it->is.is = op->subject->ptr;
            it->is.ii = 0;
        } else if (op->encoding == OBJ_ENCODING_HT) {
            // 初始化hash dict迭代器
            it->ht.dict = op->subject->ptr;
            it->ht.di = dictGetIterator(op->subject->ptr);
            it->ht.de = dictNext(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        /* Sorted sets are traversed in reverse order to optimize for
         * the insertion of the elements in a new list as in
         * ZDIFF/ZINTER/ZUNION */
        // zset从后往前遍历，用于优化ZDIFF/ZINTER/ZUNION中插入元素到新list操作。这样总是大的元素先插入，从而每次都在head操作。O(1)
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            // ziplist迭代器初始化，每次迭代需要知道元素的ele/score entry指针，这里初始化为最后一个元素。
            it->zl.zl = op->subject->ptr;
            it->zl.eptr = ziplistIndex(it->zl.zl,-2);
            if (it->zl.eptr != NULL) {
                it->zl.sptr = ziplistNext(it->zl.zl,it->zl.eptr);
                serverAssert(it->zl.sptr != NULL);
            }
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            // 跳表迭代初始化。
            it->sl.zs = op->subject->ptr;
            it->sl.node = it->sl.zs->zsl->tail;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

// 迭代器清理，目前只有set的dict迭代器需要处理。
void zuiClearIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            UNUSED(it); /* skip */
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dictReleaseIterator(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            UNUSED(it); /* skip */
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            UNUSED(it); /* skip */
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

// 获取zsetopsrc中对象的元素总个数，4种编码各自调用自己的方法获取总元素个数。
unsigned long zuiLength(zsetopsrc *op) {
    if (op->subject == NULL)
        return 0;

    if (op->type == OBJ_SET) {
        if (op->encoding == OBJ_ENCODING_INTSET) {
            return intsetLen(op->subject->ptr);
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dict *ht = op->subject->ptr;
            return dictSize(ht);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            return zzlLength(op->subject->ptr);
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            return zs->zsl->length;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

/* Check if the current value is valid. If so, store it in the passed structure
 * and move to the next element. If not valid, this means we have reached the
 * end of the structure and can abort. */
// 迭代获取next元素。即检查当前元素是否有效，有效则填充到传入的val中，并将迭代器处理指向下一个元素；无效则意味着我们遍历完了，可以结束迭代。
int zuiNext(zsetopsrc *op, zsetopval *val) {
    if (op->subject == NULL)
        return 0;

    // 有清理sds标识，则处理原值释放。
    if (val->flags & OPVAL_DIRTY_SDS)
        sdsfree(val->ele);

    // 重置val数据
    memset(val,0,sizeof(zsetopval));

    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            // set类型，intset编码
            int64_t ell;

            // 使用intsetGet获取is.ii位置的元素值，并填充val结构。set没有score，统一默认为1.0。
            if (!intsetGet(it->is.is,it->is.ii,&ell))
                return 0;
            val->ell = ell;
            val->score = 1.0;

            /* Move to next element. */
            // is.ii位置前进一个索引，供下一次迭代使用
            it->is.ii++;
        } else if (op->encoding == OBJ_ENCODING_HT) {
            // set类型，hash dict编码
            if (it->ht.de == NULL)
                return 0;
            // 直接通过dict entry提取key就可以了。
            val->ele = dictGetKey(it->ht.de);
            val->score = 1.0;

            /* Move to next element. */
            // 最后使用dictNext获取下一个遍历的元素。
            it->ht.de = dictNext(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            // zset，ziplist编码。
            /* No need to check both, but better be explicit. */
            // 并不需要两个都检查，但这里都写出来更明确
            if (it->zl.eptr == NULL || it->zl.sptr == NULL)
                return 0;
            // 分别从迭代器两个指针位置处理获取ele和score数据。
            serverAssert(ziplistGet(it->zl.eptr,&val->estr,&val->elen,&val->ell));
            val->score = zzlGetScore(it->zl.sptr);

            /* Move to next element (going backwards, see zuiInitIterator). */
            // 迭代器的两个指针处理指向前一个元素的ele/score entry。
            zzlPrev(it->zl.zl,&it->zl.eptr,&it->zl.sptr);
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            // zset，hash+skiplist编码
            if (it->sl.node == NULL)
                return 0;
            // 直接从跳表节点中获取ele/score数据。
            val->ele = it->sl.node->ele;
            val->score = it->sl.node->score;

            /* Move to next element. (going backwards, see zuiInitIterator) */
            // 迭代器指针指向backward节点。
            it->sl.node = it->sl.node->backward;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
    return 1;
}

// 从val中提取数字类型数据存储到ell中，并更新相应的dirty tags信息。返回数字是否有效。
// 如果ele或estr中有数据，则优先将字符串转为数字填充到ell中。否则数字已经在ell中了，不需要在额外提取。
int zuiLongLongFromValue(zsetopval *val) {
    if (!(val->flags & OPVAL_DIRTY_LL)) {
        val->flags |= OPVAL_DIRTY_LL;

        if (val->ele != NULL) {
            if (string2ll(val->ele,sdslen(val->ele),&val->ell))
                val->flags |= OPVAL_VALID_LL;
        } else if (val->estr != NULL) {
            if (string2ll((char*)val->estr,val->elen,&val->ell))
                val->flags |= OPVAL_VALID_LL;
        } else {
            /* The long long was already set, flag as valid. */
            val->flags |= OPVAL_VALID_LL;
        }
    }
    return val->flags & OPVAL_VALID_LL;
}

// 从val中提取sds字符串存储到ele中，并更新相应的dirty tags信息。返回sds字符串。
// 如果ele本来有数据，则直接使用，否则要将estr或ell转为sds字符串填充到ele中。
sds zuiSdsFromValue(zsetopval *val) {
    if (val->ele == NULL) {
        if (val->estr != NULL) {
            val->ele = sdsnewlen((char*)val->estr,val->elen);
        } else {
            val->ele = sdsfromlonglong(val->ell);
        }
        val->flags |= OPVAL_DIRTY_SDS;
    }
    return val->ele;
}

/* This is different from zuiSdsFromValue since returns a new SDS string
 * which is up to the caller to free. */
// 从val中提取sds字符串返回。与zuiSdsFromValue不同的是，这个函数返回一个新的sds字符串，需要调用方处理释放。
sds zuiNewSdsFromValue(zsetopval *val) {
    if (val->flags & OPVAL_DIRTY_SDS) {
        /* We have already one to return! */
        // 有sds dirty标识，即字符串准备被释放了，这里我们需要则直接重新利用该sds，不用再进行dup。
        sds ele = val->ele;
        // 相当于free了val中的字符串，并清除dirty标识。
        val->flags &= ~OPVAL_DIRTY_SDS;
        val->ele = NULL;
        return ele;
    } else if (val->ele) {
        // 这里需要dup ele字符串返回。
        return sdsdup(val->ele);
    } else if (val->estr) {
        return sdsnewlen((char*)val->estr,val->elen);
    } else {
        return sdsfromlonglong(val->ell);
    }
}

// 提取字符串数据存储到estr中。注意这4个方法中有3个数据都是解析了存储到val中对应字段的，另外一个是直接dup sds字符串返回。
int zuiBufferFromValue(zsetopval *val) {
    if (val->estr == NULL) {
        if (val->ele != NULL) {
            val->elen = sdslen(val->ele);
            val->estr = (unsigned char*)val->ele;
        } else {
            val->elen = ll2string((char*)val->_buf,sizeof(val->_buf),val->ell);
            val->estr = val->_buf;
        }
    }
    return 1;
}

/* Find value pointed to by val in the source pointer to by op. When found,
 * return 1 and store its score in target. Return 0 otherwise. */
// 在op指向的源指针中查找val指向的值。如果找到了则返回1并将它的分数存储在score参数中，否则返回0。
// 该函数肯定是在zuiNext迭代调用后，用于获取迭代到的元素数据。
int zuiFind(zsetopsrc *op, zsetopval *val, double *score) {
    if (op->subject == NULL)
        return 0;

    if (op->type == OBJ_SET) {
        if (op->encoding == OBJ_ENCODING_INTSET) {
            // set intset，先解析迭代到的数字值，并去intset中查询下该值，确保该值确实存在于set中。
            if (zuiLongLongFromValue(val) &&
                intsetFind(op->subject->ptr,val->ell))
            {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }
        } else if (op->encoding == OBJ_ENCODING_HT) {
            // set hash表，先解析迭代到字符串值，然后作为key去dict中查询，确保元素在set中。
            dict *ht = op->subject->ptr;
            zuiSdsFromValue(val);
            if (dictFind(ht,val->ele) != NULL) {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        // zset，迭代到的数据肯定是字符串，所以这里统一提取sds数据。
        zuiSdsFromValue(val);

        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            // zset ziplist，使用提取的ele数据去ziplist查询score。
            if (zzlFind(op->subject->ptr,val->ele,score) != NULL) {
                /* Score is already set by zzlFind. */
                return 1;
            } else {
                return 0;
            }
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            // zset hash+skiplist，使用提取的ele去dict查询对应的score。
            zset *zs = op->subject->ptr;
            dictEntry *de;
            if ((de = dictFind(zs->dict,val->ele)) != NULL) {
                *score = *(double*)dictGetVal(de);
                return 1;
            } else {
                return 0;
            }
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

// 按元素个数从少到多来排序zsetopsrc对象。
int zuiCompareByCardinality(const void *s1, const void *s2) {
    unsigned long first = zuiLength((zsetopsrc*)s1);
    unsigned long second = zuiLength((zsetopsrc*)s2);
    if (first > second) return 1;
    if (first < second) return -1;
    return 0;
}

// // 按元素个数从多到少来排序zsetopsrc对象。
static int zuiCompareByRevCardinality(const void *s1, const void *s2) {
    return zuiCompareByCardinality(s1, s2) * -1;
}

#define REDIS_AGGR_SUM 1
#define REDIS_AGGR_MIN 2
#define REDIS_AGGR_MAX 3
#define zunionInterDictValue(_e) (dictGetVal(_e) == NULL ? 1.0 : *(double*)dictGetVal(_e))

// score的聚合处理
inline static void zunionInterAggregate(double *target, double val, int aggregate) {
    if (aggregate == REDIS_AGGR_SUM) {
        // 如果是sum，则对加权score累加。
        *target = *target + val;
        /* The result of adding two doubles is NaN when one variable
         * is +inf and the other is -inf. When these numbers are added,
         * we maintain the convention of the result being 0.0. */
        if (isnan(*target)) *target = 0.0;
    } else if (aggregate == REDIS_AGGR_MIN) {
        // min，我们对比当前val和之前的score，并用小的那个更新score。
        *target = val < *target ? val : *target;
    } else if (aggregate == REDIS_AGGR_MAX) {
        // max，我们对比当前val和之前的score，并用大的那个更新score。
        *target = val > *target ? val : *target;
    } else {
        /* safety net */
        serverPanic("Unknown ZUNION/INTER aggregate type");
    }
}

// 获取zset中最大元素的长度，主要用于决定我们最终用什么编码来存储zset。
static int zsetDictGetMaxElementLength(dict *d) {
    dictIterator *di;
    dictEntry *de;
    size_t maxelelen = 0;

    di = dictGetIterator(d);

    // 遍历dict中每个元素，获取所有ele的最大长度。
    while((de = dictNext(di)) != NULL) {
        sds ele = dictGetKey(de);
        if (sdslen(ele) > maxelelen) maxelelen = sdslen(ele);
    }

    dictReleaseIterator(di);

    return maxelelen;
}

static void zdiffAlgorithm1(zsetopsrc *src, long setnum, zset *dstzset, size_t *maxelelen) {
    /* DIFF Algorithm 1:
     *
     * We perform the diff by iterating all the elements of the first set,
     * and only adding it to the target set if the element does not exist
     * in all the other sets.
     *
     * This way we perform at max N*M operations, where N is the size of
     * the first set, and M the number of sets.
     *
     * There is also a O(K*log(K)) cost for adding the resulting elements
     * to the target set, where K is the final size of the target set.
     *
     * The final complexity of this algorithm is O(N*M + K*log(K)). */
    // diff 算法1：
    //  遍历第一个集合的所有元素，检查如果该元素不在后面所有的其他集合中，则将该元素加入到结果集中。O(N*M)
    //  另外我们还需要额外的O(K*log(K))开销来将元素加入进结果集中。如果第一个集合是zset，因为是从后往前遍历的，加入结果集是O(1)
    int j;
    zsetopval zval;
    zskiplistNode *znode;
    sds tmp;

    /* With algorithm 1 it is better to order the sets to subtract
     * by decreasing size, so that we are more likely to find
     * duplicated elements ASAP. */
    // 对于算法1，我们将后面的集合按元素由多到少来进行排序。因为元素多更可能有相同元素，从而尽早排除，不用在检查后面的集合。
    qsort(src+1,setnum-1,sizeof(zsetopsrc),zuiCompareByRevCardinality);

    memset(&zval, 0, sizeof(zval));
    zuiInitIterator(&src[0]);
    // 遍历第一个集合的每个元素处理。
    while (zuiNext(&src[0],&zval)) {
        double value;
        int exists = 0;

        // 对于当前遍历处理的元素，我们检查每个db中是否有该元素。
        for (j = 1; j < setnum; j++) {
            /* It is not safe to access the zset we are
             * iterating, so explicitly check for equal object.
             * This check isn't really needed anymore since we already
             * check for a duplicate set in the zsetChooseDiffAlgorithm
             * function, but we're leaving it for future-proofing. */
            // 检查对应zset中是否存在该元素。只要后面有一个集合中存在该元素，我们就需要diff处理掉。
            if (src[j].subject == src[0].subject ||
                zuiFind(&src[j],&zval,&value)) {
                exists = 1;
                break;
            }
        }

        if (!exists) {
            // 如果当前元素不存在后面所有集合中，这里将该元素加入到结果zset中。
            tmp = zuiNewSdsFromValue(&zval);
            znode = zslInsert(dstzset->zsl,zval.score,tmp);
            dictAdd(dstzset->dict,tmp,&znode->score);
            // 更新结果zset中最大元素长度，用于后面决定是否转为ziplist编码。
            if (sdslen(tmp) > *maxelelen) *maxelelen = sdslen(tmp);
        }
    }
    zuiClearIterator(&src[0]);
}


static void zdiffAlgorithm2(zsetopsrc *src, long setnum, zset *dstzset, size_t *maxelelen) {
    /* DIFF Algorithm 2:
     *
     * Add all the elements of the first set to the auxiliary set.
     * Then remove all the elements of all the next sets from it.
     *

     * This is O(L + (N-K)log(N)) where L is the sum of all the elements in every
     * set, N is the size of the first set, and K is the size of the result set.
     *
     * Note that from the (L-N) dict searches, (N-K) got to the zsetRemoveFromSkiplist
     * which costs log(N)
     *
     * There is also a O(K) cost at the end for finding the largest element
     * size, but this doesn't change the algorithm complexity since K < L, and
     * O(2L) is the same as O(L). */
    // DIFF 算法2：添加第一个集合的所有的元素到辅助集合中，然后遍历后面所有集合的元素，从辅助集合中删除该元素。O(L + (N-K)log(N))
    // 注意正常第一个集合数据加入dstzset中时，写入跳表应该是Nlog(N)。
    // 但由于我们是从后往前遍历的，也就是元素插入从大到小顺序，所以每次插入总是在表头，O(1)，所以插入操作是O(N)。

    // 应该可以使用一个临时dict来装结果，最后在生成跳表时从后往前遍历第一个zset，如果在dict中则加入结果集。最终是O(L)
    // 但如果第一个是set，则我们结果是O(L + Klog(K))，因为无法保证结果有序。可以通过判断第一个集合类型来看最终是遍历结果集还是遍历第一个集合。
    int j;
    int cardinality = 0;
    zsetopval zval;
    zskiplistNode *znode;
    sds tmp;

    // 遍历所有集合
    for (j = 0; j < setnum; j++) {
        if (zuiLength(&src[j]) == 0) continue;

        memset(&zval, 0, sizeof(zval));
        zuiInitIterator(&src[j]);
        // 针对每个集合遍历所有的元素。如果有跟第一个集合一样的集合，我们在选择算法时就处理了，所以这里不用考虑这种情况。
        while (zuiNext(&src[j],&zval)) {
            if (j == 0) {
                // 对于第一个集合，我们将所有元素加入到dstzset中。
                tmp = zuiNewSdsFromValue(&zval);
                znode = zslInsert(dstzset->zsl,zval.score,tmp);
                dictAdd(dstzset->dict,tmp,&znode->score);
                cardinality++;
            } else {
                // 对于后面所有其他集合元素，我们先从dict中查看元素是否存在，存在则要删除（跳表删除存在的元素是O(logN)）。
                tmp = zuiSdsFromValue(&zval);
                if (zsetRemoveFromSkiplist(dstzset, tmp)) {
                    cardinality--;
                }
            }

            /* Exit if result set is empty as any additional removal
                * of elements will have no effect. */
            // 如果dstzset中没有元素了，则我们结果为空集，后面的元素也都不需要遍历了。
            if (cardinality == 0) break;
        }
        zuiClearIterator(&src[j]);

        // 结果为空集，后面的集合不需要遍历了。
        if (cardinality == 0) break;
    }

    /* Resize dict if needed after removing multiple elements */
    // 如果需要对dstzset中的dict进行缩容。
    if (htNeedsResize(dstzset->dict)) dictResize(dstzset->dict);

    /* Using this algorithm, we can't calculate the max element as we go,
     * we have to iterate through all elements to find the max one after. */
    // 这里还需要遍历整个结果集，来获取最大的元素长度，用于决定是否换成ziplist编码。
    *maxelelen = zsetDictGetMaxElementLength(dstzset->dict);
}

// 根据处理的数据选择合适的diff算法。
static int zsetChooseDiffAlgorithm(zsetopsrc *src, long setnum) {
    int j;

    /* Select what DIFF algorithm to use.
     *
     * Algorithm 1 is O(N*M + K*log(K)) where N is the size of the
     * first set, M the total number of sets, and K is the size of the
     * result set.
     *
     * Algorithm 2 is O(L + (N-K)log(N)) where L is the total number of elements
     * in all the sets, N is the size of the first set, and K is the size of the
     * result set.
     *
     * We compute what is the best bet with the current input here. */
    // 选择哪个DIFF差集算法使用：
    //  算法1复杂度是O(N*M + K*log(K))，N是第一个集合的元素个数，M是DIFF处理的后面所有集合数量，K是结果集的大小。
    //  算法2复杂度是O(L + (N-K)log(N))，L是所有集合总共的元素数量，N是第一个集合的大小，K睡结果集的大小。
    // 我们这里根据输入数据，来判断使用哪个算法最好。其实最终就是对比 M*N/2 与 所有集合总元素 哪个大来选择的。
    long long algo_one_work = 0;
    long long algo_two_work = 0;

    for (j = 0; j < setnum; j++) {
        /* If any other set is equal to the first set, there is nothing to be
         * done, since we would remove all elements anyway. */
        // 如果后面有集合与第一个是同一个，则diff后结果为空集，所以这里我们什么都不需要做，直接返回。
        if (j > 0 && src[0].subject == src[j].subject) {
            return 0;
        }

        algo_one_work += zuiLength(&src[0]);
        algo_two_work += zuiLength(&src[j]);
    }

    /* Algorithm 1 has better constant times and performs less operations
     * if there are elements in common. Give it some advantage. */
    // 因为算法一有更优的常数项，执行的操作更少，所以我们algo_one_work除以2后再进行比较。
    algo_one_work /= 2;
    return (algo_one_work <= algo_two_work) ? 1 : 2;
}

// 求集合的差集。
static void zdiff(zsetopsrc *src, long setnum, zset *dstzset, size_t *maxelelen) {
    /* Skip everything if the smallest input is empty. */
    // 对于diff操作，我们是没有对集合进行排序的。第一个集合为基准，如果该集合为空，则diff为空，我们什么都不用做。
    if (zuiLength(&src[0]) > 0) {
        // 这里根据元素来选择算法。返回0则结果为空集，什么都不需要处理。返回1，2对应选择使用的算法。
        int diff_algo = zsetChooseDiffAlgorithm(src, setnum);
        if (diff_algo == 1) {
            // diff算法1
            zdiffAlgorithm1(src, setnum, dstzset, maxelelen);
        } else if (diff_algo == 2) {
            // diff算法2
            zdiffAlgorithm2(src, setnum, dstzset, maxelelen);
        } else if (diff_algo != 0) {
            serverPanic("Unknown algorithm");
        }
    }
}

uint64_t dictSdsHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);

dictType setAccumulatorDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    NULL,                      /* val destructor */
    NULL                       /* allow to expand */
};

/* The zunionInterDiffGenericCommand() function is called in order to implement the
 * following commands: ZUNION, ZINTER, ZDIFF, ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE.
 *
 * 'numkeysIndex' parameter position of key number. for ZUNION/ZINTER/ZDIFF command,
 * this value is 1, for ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE command, this value is 2.
 *
 * 'op' SET_OP_INTER, SET_OP_UNION or SET_OP_DIFF.
 */
// 本函数是实现ZUNION, ZINTER, ZDIFF, ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE这些命令的通用方法。
// numkeysIndex表示指定操作集合数量的参数位置。如：ZDIFF 3 zset1 zset2 zset3，numkeysIndex为1，即参数3（代表集合个数）的索引。
// 对于ZUNION/ZINTER/ZDIFF命令，该值为1；对于ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE，该值为2。
// op参数表示集合操作类型SET_OP_INTER、SET_OP_UNION或SET_OP_DIFF。交并差集。
void zunionInterDiffGenericCommand(client *c, robj *dstkey, int numkeysIndex, int op) {
    int i, j;
    long setnum;
    int aggregate = REDIS_AGGR_SUM;
    zsetopsrc *src;
    zsetopval zval;
    sds tmp;
    size_t maxelelen = 0;
    robj *dstobj;
    zset *dstzset;
    zskiplistNode *znode;
    int withscores = 0;

    /* expect setnum input keys to be given */
    // 获取处理的集合数目。
    if ((getLongFromObjectOrReply(c, c->argv[numkeysIndex], &setnum, NULL) != C_OK))
        return;

    if (setnum < 1) {
        addReplyErrorFormat(c,
            "at least 1 input key is needed for %s", c->cmd->name);
        return;
    }

    /* test if the expected number of keys would overflow */
    // 检查给定的集合数目是否够setnum数量。
    if (setnum > (c->argc-(numkeysIndex+1))) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* read keys to be used for input */
    // 针对传入的集合keys，获取对应的集合对象。
    src = zcalloc(sizeof(zsetopsrc) * setnum);
    for (i = 0, j = numkeysIndex+1; i < setnum; i++, j++) {
        // 如果结果是store，则我们按写来读；否则返回给client，按读处理。
        robj *obj = dstkey ?
            lookupKeyWrite(c->db,c->argv[j]) :
            lookupKeyRead(c->db,c->argv[j]);
        if (obj != NULL) {
            // 查到了对象，要确保对象是集合，即类型是set或zset。
            if (obj->type != OBJ_ZSET && obj->type != OBJ_SET) {
                zfree(src);
                addReplyErrorObject(c,shared.wrongtypeerr);
                return;
            }

            // src最终是所有集合元数据构成的数组。
            src[i].subject = obj;
            src[i].type = obj->type;
            src[i].encoding = obj->encoding;
        } else {
            // 没查到，NULL标识。
            src[i].subject = NULL;
        }

        /* Default all weights to 1. */
        // 默认所有的权重都是1
        src[i].weight = 1.0;
    }

    /* parse optional extra arguments */
    // 解析可选的额外参数。
    if (j < c->argc) {
        int remaining = c->argc - j;

        while (remaining) {
            if (op != SET_OP_DIFF &&
                remaining >= (setnum + 1) &&
                !strcasecmp(c->argv[j]->ptr,"weights"))
            {
                // weights后面跟着所有参数指定的集合的权重。权重与集合是一一对应的（位置也一一对应），所以数量也是setnum。
                // 注意DIFF没有权重概念，我们会从第一个集合中减去后面所有集合中的元素，所以权重对它结果无影响，也就不需要权重参数。
                j++; remaining--;
                for (i = 0; i < setnum; i++, j++, remaining--) {
                    if (getDoubleFromObjectOrReply(c,c->argv[j],&src[i].weight,
                            "weight value is not a float") != C_OK)
                    {
                        zfree(src);
                        return;
                    }
                }
            } else if (op != SET_OP_DIFF &&
                       remaining >= 2 &&
                       !strcasecmp(c->argv[j]->ptr,"aggregate"))
            {
                // 聚合参数。DIFF也没有这个参数。可以指定sum、min、max。
                j++; remaining--;
                if (!strcasecmp(c->argv[j]->ptr,"sum")) {
                    aggregate = REDIS_AGGR_SUM;
                } else if (!strcasecmp(c->argv[j]->ptr,"min")) {
                    aggregate = REDIS_AGGR_MIN;
                } else if (!strcasecmp(c->argv[j]->ptr,"max")) {
                    aggregate = REDIS_AGGR_MAX;
                } else {
                    zfree(src);
                    addReplyErrorObject(c,shared.syntaxerr);
                    return;
                }
                j++; remaining--;
            } else if (!dstkey &&
                       !strcasecmp(c->argv[j]->ptr,"withscores"))
            {
                // 如果不是store，返回给client展示时，才有可使用withscores参数。
                j++; remaining--;
                withscores = 1;
            } else {
                zfree(src);
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
        }
    }

    if (op != SET_OP_DIFF) {
        /* sort sets from the smallest to largest, this will improve our
        * algorithm's performance */
        // 如果不是DIFF，我们这里对集合进行排序，对操作集合按元素个数从少到多进行排序，这能提高后面算法性能。
        // 如果是DIFF，后面我们有一种算法会需要对集合按元素个数从多到少排序，因更多元素说明更可能有相同元素，从而尽早移除就不用在看后面的集合了。
        qsort(src,setnum,sizeof(zsetopsrc),zuiCompareByCardinality);
    }

    // 创建一个临时的zset对象，用于保存结果。
    dstobj = createZsetObject();
    dstzset = dstobj->ptr;
    memset(&zval, 0, sizeof(zval));

    if (op == SET_OP_INTER) {
        /* Skip everything if the smallest input is empty. */
        // 求交集。我们集合是按长度从小到大排序的，这里判断最小的集合如果是空的，那么集合交集就是空，什么都不用处理。
        if (zuiLength(&src[0]) > 0) {
            /* Precondition: as src[0] is non-empty and the inputs are ordered
             * by size, all src[i > 0] are non-empty too. */
            // 因为最少元素的集合都不为空，所以后面所有集合都不为空。求交集我们以元素最少的集合为基准，遍历然后看元素是否在其他集合中。
            zuiInitIterator(&src[0]);
            // 遍历第一个集合（即最少元素集合）的元素。
            while (zuiNext(&src[0],&zval)) {
                double score, value;

                // score是记录当前计算的加权的score。如果所有集合都有当前这个元素，则该元素为交集中数据，分数即这个加权score。
                score = src[0].weight * zval.score;
                if (isnan(score)) score = 0;

                // 遍历后面的集合查找当前处理的元素。
                for (j = 1; j < setnum; j++) {
                    /* It is not safe to access the zset we are
                     * iterating, so explicitly check for equal object. */
                    // 找到了当前元素，则还需要进行聚合操作；没找到则交集中不存在该元素，后面的集合都不需要检查了，直接跳过。
                    // 如果后面的set跟第一个set是同一个，直接使用当前遍历的元素就可以了。这里再迭代访问不安全，也没必要。
                    if (src[j].subject == src[0].subject) {
                        value = zval.score*src[j].weight;
                        // 处理score的聚合，min、max、sum。
                        zunionInterAggregate(&score,value,aggregate);
                    } else if (zuiFind(&src[j],&zval,&value)) {
                        value *= src[j].weight;
                        zunionInterAggregate(&score,value,aggregate);
                    } else {
                        break;
                    }
                }

                /* Only continue when present in every input. */
                // 当j==setnum，说明我们遍历完了后面所有的集合，且都包含当前处理元素。则该元素为交集元素。
                // 处理该元素加入到我们的结果zset中。
                if (j == setnum) {
                    tmp = zuiNewSdsFromValue(&zval);
                    znode = zslInsert(dstzset->zsl,score,tmp);
                    dictAdd(dstzset->dict,tmp,&znode->score);
                    // 记录最大元素长度，用于决定后面是否转为ziplist编码。
                    if (sdslen(tmp) > maxelelen) maxelelen = sdslen(tmp);
                }
            }
            zuiClearIterator(&src[0]);
        }
    } else if (op == SET_OP_UNION) {
        // 求并集。这里先创建一个临时的dict用于存储并集元素key是元素，value是double类型的加权score值。
        dict *accumulator = dictCreate(&setAccumulatorDictType,NULL);
        dictIterator *di;
        dictEntry *de, *existing;
        double score;

        if (setnum) {
            /* Our union is at least as large as the largest set.
             * Resize the dictionary ASAP to avoid useless rehashing. */
            // 并集应该至少与我们最大的集合一样大。这里直接扩展到这个大小，避免后面加入元素时过多的rehash操作。
            dictExpand(accumulator,zuiLength(&src[setnum-1]));
        }

        /* Step 1: Create a dictionary of elements -> aggregated-scores
         * by iterating one sorted set after the other. */
        // 迭代所有的集合，构建一个 元素->聚合分数 的dict。
        for (i = 0; i < setnum; i++) {
            // 空集合，跳过
            if (zuiLength(&src[i]) == 0) continue;

            zuiInitIterator(&src[i]);
            // 遍历集合元素，将元素加入到accumulator dict中。
            while (zuiNext(&src[i],&zval)) {
                /* Initialize value */
                // 计算遍历到元素的加权score。
                score = src[i].weight * zval.score;
                if (isnan(score)) score = 0;

                /* Search for this element in the accumulating dictionary. */
                // 准备将该元素加入到accumulator中，这里先查询该元素是否已经存在。
                de = dictAddRaw(accumulator,zuiSdsFromValue(&zval),&existing);
                /* If we don't have it, we need to create a new entry. */
                if (!existing) {
                    // 如果元素不存在，则de为新创建的entry，我们填充ele和score。
                    tmp = zuiNewSdsFromValue(&zval);
                    /* Remember the longest single element encountered,
                     * to understand if it's possible to convert to ziplist
                     * at the end. */
                    // 记录最大的元素长度，用于决定是否转成ziplist存储。
                    if (sdslen(tmp) > maxelelen) maxelelen = sdslen(tmp);
                    /* Update the element with its initial score. */
                    // 更新新entry的ele/score值。
                    dictSetKey(accumulator, de, tmp);
                    dictSetDoubleVal(de,score);
                } else {
                    /* Update the score with the score of the new instance
                     * of the element found in the current sorted set.
                     *
                     * Here we access directly the dictEntry double
                     * value inside the union as it is a big speedup
                     * compared to using the getDouble/setDouble API. */
                    // 如果找到了该元素，说明之前已加入到accumulator中。这里需要处理score的聚合。
                    // 注意这里我们直接访问entry的double字段，比使用getDouble/setDouble API要快很多。
                    zunionInterAggregate(&existing->v.d,score,aggregate);
                }
            }
            zuiClearIterator(&src[i]);
        }

        /* Step 2: convert the dictionary into the final sorted set. */
        // 前面我们得到了结果的dict，这里遍历dict元素构造zset集合。
        di = dictGetIterator(accumulator);

        /* We now are aware of the final size of the resulting sorted set,
         * let's resize the dictionary embedded inside the sorted set to the
         * right size, in order to save rehashing time. */
        // 我们目前已经知道了结果的数量，所以直接对于dstzset的dict进行扩展，避免后面进行rehash。
        dictExpand(dstzset->dict,dictSize(accumulator));

        // 遍历accumulator中元素，加入到结果zset中。
        while((de = dictNext(di)) != NULL) {
            sds ele = dictGetKey(de);
            score = dictGetDoubleVal(de);
            znode = zslInsert(dstzset->zsl,score,ele);
            dictAdd(dstzset->dict,ele,&znode->score);
        }
        dictReleaseIterator(di);
        dictRelease(accumulator);
    } else if (op == SET_OP_DIFF) {
        // 求差集。
        zdiff(src, setnum, dstzset, &maxelelen);
    } else {
        serverPanic("Unknown operator");
    }

    if (dstkey) {
        // 结果store为dstkey的对象。
        if (dstzset->zsl->length) {
            // 有数据，判断是否需要转为ziplist编码。并将该对象加入到db中。
            zsetConvertToZiplistIfNeeded(dstobj, maxelelen);
            setKey(c, c->db, dstkey, dstobj);
            // 回复新集合元素个数给client。
            addReplyLongLong(c, zsetLength(dstobj));
            // 处理key变更通知
            notifyKeyspaceEvent(NOTIFY_ZSET,
                                (op == SET_OP_UNION) ? "zunionstore" :
                                    (op == SET_OP_INTER ? "zinterstore" : "zdiffstore"),
                                dstkey, c->db->id);
            server.dirty++;
        } else {
            // 没有数据，回复client 0。此时我们还需要从db中移除dstkey，并处理key变更通知。
            addReply(c, shared.czero);
            if (dbDelete(c->db, dstkey)) {
                signalModifiedKey(c, c->db, dstkey);
                notifyKeyspaceEvent(NOTIFY_GENERIC, "del", dstkey, c->db->id);
                server.dirty++;
            }
        }
    } else {
        // 结果返回给client
        unsigned long length = dstzset->zsl->length;
        zskiplist *zsl = dstzset->zsl;
        zskiplistNode *zn = zsl->header->level[0].forward;
        /* In case of WITHSCORES, respond with a single array in RESP2, and
         * nested arrays in RESP3. We can't use a map response type since the
         * client library needs to know to respect the order. */
        // 正常情况我们都是单个数组回复所有元素。
        // 但如果有WITHSCORES参数，对于RESP2，我们使用单个数组存储，元素数量为两倍；而对于RESP3，使用嵌套数组返回，元素数量还是length。
        if (withscores && c->resp == 2)
            addReplyArrayLen(c, length*2);
        else
            addReplyArrayLen(c, length);

        // 遍历结果跳表的链表，将元素添加到回复缓冲中。
        while (zn != NULL) {
            // 注意对于withscores且RESP3，我们元素是一个数组，这里写入数组长度2。
            if (withscores && c->resp > 2) addReplyArrayLen(c,2);
            // 添加元素(及score)回复给client
            addReplyBulkCBuffer(c,zn->ele,sdslen(zn->ele));
            if (withscores) addReplyDouble(c,zn->score);
            // 跳到下一个节点处理。
            zn = zn->level[0].forward;
        }
    }
    decrRefCount(dstobj);
    zfree(src);
}

// ZUNIONSTORE destination numkeys key [key ...]
void zunionstoreCommand(client *c) {
    zunionInterDiffGenericCommand(c, c->argv[1], 2, SET_OP_UNION);
}

// ZINTERSTORE destination numkeys key [key ...]
void zinterstoreCommand(client *c) {
    zunionInterDiffGenericCommand(c, c->argv[1], 2, SET_OP_INTER);
}

// ZDIFFSTORE destination numkeys key [key ...]
void zdiffstoreCommand(client *c) {
    zunionInterDiffGenericCommand(c, c->argv[1], 2, SET_OP_DIFF);
}

// ZUNION numkeys key [key ...]
void zunionCommand(client *c) {
    zunionInterDiffGenericCommand(c, NULL, 1, SET_OP_UNION);
}

// ZINTER numkeys key [key ...]
void zinterCommand(client *c) {
    zunionInterDiffGenericCommand(c, NULL, 1, SET_OP_INTER);
}

// ZDIFF numkeys key [key ...]
void zdiffCommand(client *c) {
    zunionInterDiffGenericCommand(c, NULL, 1, SET_OP_DIFF);
}

// zrange方向
typedef enum {
    ZRANGE_DIRECTION_AUTO = 0,
    ZRANGE_DIRECTION_FORWARD,
    ZRANGE_DIRECTION_REVERSE
} zrange_direction;

typedef enum {
    ZRANGE_CONSUMER_TYPE_CLIENT = 0,
    ZRANGE_CONSUMER_TYPE_INTERNAL
} zrange_consumer_type;

typedef struct zrange_result_handler zrange_result_handler;

typedef void (*zrangeResultBeginFunction)(zrange_result_handler *c);
typedef void (*zrangeResultFinalizeFunction)(
    zrange_result_handler *c, size_t result_count);
typedef void (*zrangeResultEmitCBufferFunction)(
    zrange_result_handler *c, const void *p, size_t len, double score);
typedef void (*zrangeResultEmitLongLongFunction)(
    zrange_result_handler *c, long long ll, double score);

void zrangeGenericCommand (zrange_result_handler *handler, int argc_start, int store,
                           zrange_type rangetype, zrange_direction direction);

/* Interface struct for ZRANGE/ZRANGESTORE generic implementation.
 * There is one implementation of this interface that sends a RESP reply to clients.
 * and one implementation that stores the range result into a zset object. */
// ZRANGE/ZRANGESTORE结果处理的handler接口结构。发送RESP回复给client和存储结果到新的zset对象都各自实现了这个接口。
struct zrange_result_handler {
    // 处理类型，是回复client还是存储在新对象中。
    zrange_consumer_type                 type;
    client                              *client;
    robj                                *dstkey;
    robj                                *dstobj;
    void                                *userdata;
    int                                  withscores;
    // 是否结果中每个item回复需要添加len。RESP3格式，且withscores时才需要。
    int                                  should_emit_array_length;
    zrangeResultBeginFunction            beginResultEmission;
    zrangeResultFinalizeFunction         finalizeResultEmission;
    zrangeResultEmitCBufferFunction      emitResultFromCBuffer;
    zrangeResultEmitLongLongFunction     emitResultFromLongLong;
};

/* Result handler methods for responding the ZRANGE to clients. */
// 开启client传输，需要有一个NULL占位符后面写入len。
static void zrangeResultBeginClient(zrange_result_handler *handler) {
    handler->userdata = addReplyDeferredLen(handler->client);
}

// 写入字符串item数据到buffer中。如果需要先写入长度2，再写value和score。
static void zrangeResultEmitCBufferToClient(zrange_result_handler *handler,
    const void *value, size_t value_length_in_bytes, double score)
{
    if (handler->should_emit_array_length) {
        addReplyArrayLen(handler->client, 2);
    }

    addReplyBulkCBuffer(handler->client, value, value_length_in_bytes);

    if (handler->withscores) {
        addReplyDouble(handler->client, score);
    }
}

// 写入数字item数据到buffer。如果需要先写入长度2，再写value和score。
static void zrangeResultEmitLongLongToClient(zrange_result_handler *handler,
    long long value, double score)
{
    if (handler->should_emit_array_length) {
        addReplyArrayLen(handler->client, 2);
    }

    addReplyBulkLongLong(handler->client, value);

    if (handler->withscores) {
        addReplyDouble(handler->client, score);
    }
}

// 结束client传输，处理传输长度。
static void zrangeResultFinalizeClient(zrange_result_handler *handler,
    size_t result_count)
{
    /* In case of WITHSCORES, respond with a single array in RESP2, and
     * nested arrays in RESP3. We can't use a map response type since the
     * client library needs to know to respect the order. */
    // 如果是WITHSCORES，则我们发送的数据会是item的两倍。
    // 而对于RESP2，我们是作为数组传输的，所以count为2倍。RESP3是map传输的，所以长度不变。
    if (handler->withscores && (handler->client->resp == 2)) {
        result_count *= 2;
    }

    setDeferredArrayLen(handler->client, handler->userdata, result_count);
}

/* Result handler methods for storing the ZRANGESTORE to a zset. */
// 结果存储到新对象中，开始需要创建一个ziplist对象。
static void zrangeResultBeginStore(zrange_result_handler *handler)
{
    handler->dstobj = createZsetZiplistObject();
}

// 存储到新对象，字符串item数据写入。zsetAdd处理。
static void zrangeResultEmitCBufferForStore(zrange_result_handler *handler,
    const void *value, size_t value_length_in_bytes, double score)
{
    double newscore;
    int retflags = 0;
    sds ele = sdsnewlen(value, value_length_in_bytes);
    int retval = zsetAdd(handler->dstobj, score, ele, ZADD_IN_NONE, &retflags, &newscore);
    sdsfree(ele);
    serverAssert(retval);
}

// 存储到新对象，数字item数据写入。zsetAdd处理。
static void zrangeResultEmitLongLongForStore(zrange_result_handler *handler,
    long long value, double score)
{
    double newscore;
    int retflags = 0;
    sds ele = sdsfromlonglong(value);
    int retval = zsetAdd(handler->dstobj, score, ele, ZADD_IN_NONE, &retflags, &newscore);
    sdsfree(ele);
    serverAssert(retval);
}

// 存储到新对象，写入完成处理。
static void zrangeResultFinalizeStore(zrange_result_handler *handler, size_t result_count)
{
    if (result_count) {
        // 有结果数据，则将key加入到db，回复结果count给client，并通知key变更。
        setKey(handler->client, handler->client->db, handler->dstkey, handler->dstobj);
        addReplyLongLong(handler->client, result_count);
        notifyKeyspaceEvent(NOTIFY_ZSET, "zrangestore", handler->dstkey, handler->client->db->id);
        server.dirty++;
    } else {
        // 没有结果数据，则将key从db中移除，回复结果count 0给client，并通知key变更。
        addReply(handler->client, shared.czero);
        if (dbDelete(handler->client->db, handler->dstkey)) {
            signalModifiedKey(handler->client, handler->client->db, handler->dstkey);
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", handler->dstkey, handler->client->db->id);
            server.dirty++;
        }
    }
    decrRefCount(handler->dstobj);
}

/* Initialize the consumer interface type with the requested type. */
// 根据请求的类型，初始化结果数据处理的handler
static void zrangeResultHandlerInit(zrange_result_handler *handler,
    client *client, zrange_consumer_type type)
{
    memset(handler, 0, sizeof(*handler));

    handler->client = client;

    switch (type) {
        // 根据类型，设置处理函数。
    case ZRANGE_CONSUMER_TYPE_CLIENT:
        handler->beginResultEmission = zrangeResultBeginClient;
        handler->finalizeResultEmission = zrangeResultFinalizeClient;
        handler->emitResultFromCBuffer = zrangeResultEmitCBufferToClient;
        handler->emitResultFromLongLong = zrangeResultEmitLongLongToClient;
        break;

    case ZRANGE_CONSUMER_TYPE_INTERNAL:
        handler->beginResultEmission = zrangeResultBeginStore;
        handler->finalizeResultEmission = zrangeResultFinalizeStore;
        handler->emitResultFromCBuffer = zrangeResultEmitCBufferForStore;
        handler->emitResultFromLongLong = zrangeResultEmitLongLongForStore;
        break;
    }
}

// 如果结果需要score，则调用这个方法初始化handler相关数据。
static void zrangeResultHandlerScoreEmissionEnable(zrange_result_handler *handler) {
    handler->withscores = 1;
    handler->should_emit_array_length = (handler->client->resp > 2);
}

// 如果结果需要store，则调用这个方法始化handler的dstkey。
static void zrangeResultHandlerDestinationKeySet (zrange_result_handler *handler,
    robj *dstkey)
{
    handler->dstkey = dstkey;
}

/* This command implements ZRANGE, ZREVRANGE. */
// 实现ZRANGE, ZREVRANGE的通用处理。
void genericZrangebyrankCommand(zrange_result_handler *handler,
    robj *zobj, long start, long end, int withscores, int reverse) {

    client *c = handler->client;
    long llen;
    long rangelen;
    size_t result_cardinality;

    /* Sanitize indexes. */
    // range index处理
    llen = zsetLength(zobj);
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    // begin开始处理
    handler->beginResultEmission(handler);

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    // 前面处理后始终有start>=0，当end<0时这里始终为true。所以start>end或start>=length时，range为空。
    if (start > end || start >= llen) {
        // range为空，处理结束。
        handler->finalizeResultEmission(handler, 0);
        return;
    }
    // end index处理，并计算总的range item数量
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;
    result_cardinality = rangelen;

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        double score = 0.0;

        // 计算起始处理的entry位置。eptr为ele位置。
        if (reverse)
            eptr = ziplistIndex(zl,-2-(2*start));
        else
            eptr = ziplistIndex(zl,2*start);

        serverAssertWithInfo(c,zobj,eptr != NULL);
        // sptr为score位置
        sptr = ziplistNext(zl,eptr);

        // 遍历处理所有range元素
        while (rangelen--) {
            serverAssertWithInfo(c,zobj,eptr != NULL && sptr != NULL);
            // 获取当前遍历元素的ele数据
            serverAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));

            // 如果需要score，则获取遍历元素的score数据
            if (withscores) /* don't bother to extract the score if it's gonna be ignored. */
                score = zzlGetScore(sptr);

            // 根据ele编码是数字还是字符串，使用不同的方法来处理数据。
            if (vstr == NULL) {
                handler->emitResultFromLongLong(handler, vlong, score);
            } else {
                handler->emitResultFromCBuffer(handler, vstr, vlen, score);
            }

            // 定位到下一个元素的entry位置。reverse逆向使用zzlPrev处理。
            if (reverse)
                zzlPrev(zl,&eptr,&sptr);
            else
                zzlNext(zl,&eptr,&sptr);
        }

    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* Check if starting point is trivial, before doing log(N) lookup. */
        // 定位跳表中range的起始元素位置。
        if (reverse) {
            ln = zsl->tail;
            if (start > 0)
                ln = zslGetElementByRank(zsl,llen-start);
        } else {
            ln = zsl->header->level[0].forward;
            if (start > 0)
                ln = zslGetElementByRank(zsl,start+1);
        }

        // 遍历跳表range元素
        while(rangelen--) {
            serverAssertWithInfo(c,zobj,ln != NULL);
            sds ele = ln->ele;
            // 处理遍历到的元素写入
            handler->emitResultFromCBuffer(handler, ele, sdslen(ele), ln->score);
            // 跳到下一个节点
            ln = reverse ? ln->backward : ln->level[0].forward;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    // range遍历结束处理。写入长度回复client，或者新对象加入db处理。
    handler->finalizeResultEmission(handler, result_cardinality);
}

/* ZRANGESTORE <dst> <src> <min> <max> [BYSCORE | BYLEX] [REV] [LIMIT offset count] */
void zrangestoreCommand (client *c) {
    robj *dstkey = c->argv[1];
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_INTERNAL);
    zrangeResultHandlerDestinationKeySet(&handler, dstkey);
    zrangeGenericCommand(&handler, 2, 1, ZRANGE_AUTO, ZRANGE_DIRECTION_AUTO);
}

/* ZRANGE <key> <min> <max> [BYSCORE | BYLEX] [REV] [WITHSCORES] [LIMIT offset count] */
void zrangeCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_AUTO, ZRANGE_DIRECTION_AUTO);
}

/* ZREVRANGE <key> <min> <max> [WITHSCORES] */
void zrevrangeCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_RANK, ZRANGE_DIRECTION_REVERSE);
}

/* This command implements ZRANGEBYSCORE, ZREVRANGEBYSCORE. */
// 实现ZRANGEBYSCORE, ZREVRANGEBYSCORE的通用处理。
void genericZrangebyscoreCommand(zrange_result_handler *handler,
    zrangespec *range, robj *zobj, long offset, long limit, 
    int reverse) {

    client *c = handler->client;
    unsigned long rangelen = 0;

    // begin开始处理
    handler->beginResultEmission(handler);

    /* For invalid offset, return directly. */
    // offset不合法直接结束，返回。
    if (offset > 0 && offset >= (long)zsetLength(zobj)) {
        handler->finalizeResultEmission(handler, 0);
        return;
    }

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* If reversed, get the last node in range as starting point. */
        // 定位到遍历起始位置，如果是reversed，则是最后一个在range中的元素位置。
        if (reverse) {
            eptr = zzlLastInRange(zl,range);
        } else {
            eptr = zzlFirstInRange(zl,range);
        }

        /* Get score pointer for the first element. */
        // 遍历第一个元素的score entry位置
        if (eptr)
            sptr = ziplistNext(zl,eptr);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        // 如果有offset，则根据前面得到的range起始位置，定位到offset指定的元素位置。这里我们不检查score，因为下一个循环中会处理。
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        // 遍历获取limit个元素。成功获取limit个元素或者遍历结束时，退出循环。
        while (eptr && limit--) {
            // 获取当前遍历元素的score
            double score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            // 检查当前遍历元素的score，如果不在range中了，则跳出循环。
            if (reverse) {
                if (!zslValueGteMin(score,range)) break;
            } else {
                if (!zslValueLteMax(score,range)) break;
            }

            /* We know the element exists, so ziplistGet should always
             * succeed */
            // score在范围内，这里获取当前元素的ele。
            serverAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));

            rangelen++;
            // 遍历到的元素处理加入返回结果中
            if (vstr == NULL) {
                handler->emitResultFromLongLong(handler, vlong, score);
            } else {
                handler->emitResultFromCBuffer(handler, vstr, vlen, score);
            }

            /* Move to next node */
            // 跳到下一个元素。reverse则使用zzlPrev跳到prev元素。
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        // 定位到range遍历的起始元素。reversed，则是range中的最后一个元素。
        if (reverse) {
            ln = zslLastInRange(zsl,range);
        } else {
            ln = zslFirstInRange(zsl,range);
        }

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        // 如果有offset，则定位range遍历中指定offset对应的元素。这里我们不检查score，因为下一个循环中会处理。
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        // 遍历处理元素
        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            // 检查当前遍历的元素的score，如果不在range范围内，则跳出循环。
            if (reverse) {
                if (!zslValueGteMin(ln->score,range)) break;
            } else {
                if (!zslValueLteMax(ln->score,range)) break;
            }

            rangelen++;
            // score在范围内，将元素的ele加入到返回results中。
            handler->emitResultFromCBuffer(handler, ln->ele, sdslen(ln->ele), ln->score);

            /* Move to next node */
            // 跳到下一个节点处理。
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    // range遍历结束处理。写入长度回复client，或者将新对象加入db。
    handler->finalizeResultEmission(handler, rangelen);
}

/* ZRANGEBYSCORE <key> <min> <max> [WITHSCORES] [LIMIT offset count] */
void zrangebyscoreCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_SCORE, ZRANGE_DIRECTION_FORWARD);
}

/* ZREVRANGEBYSCORE <key> <min> <max> [WITHSCORES] [LIMIT offset count] */
void zrevrangebyscoreCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_SCORE, ZRANGE_DIRECTION_REVERSE);
}

// ZCOUNT key min max，指定score范围区间元素的数量
void zcountCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zrangespec range;
    unsigned long count = 0;

    /* Parse the range arguments */
    // 解析range参数
    if (zslParseRange(c->argv[2],c->argv[3],&range) != C_OK) {
        addReplyError(c,"min or max is not a float");
        return;
    }

    /* Lookup the sorted set */
    // 获取key对象，并确保是zset类型
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET)) return;

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        double score;

        /* Use the first element in range as the starting point */
        // 获取ziplist zset中在range里的第一个元素。
        eptr = zzlFirstInRange(zl,&range);

        /* No "first" element */
        // 如果没有元素在rang中，则直接回复。
        if (eptr == NULL) {
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        // 获取在range中的第一个元素的ele/score
        sptr = ziplistNext(zl,eptr);
        score = zzlGetScore(sptr);
        serverAssertWithInfo(c,zobj,zslValueLteMax(score,&range));

        /* Iterate over elements in range */
        // 迭代在range中的元素，计数。
        while (eptr) {
            // 获取当前遍历到的元素score
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            // 如果score大于range max，则当前元素不在range中了，跳出循环。
            if (!zslValueLteMax(score,&range)) {
                break;
            } else {
                // 元素在range中，则计数，并跳到下一个元素遍历。
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        // 基于跳表的zset，定位到range中第一个元素。
        zn = zslFirstInRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        // 这里range元素计数，使用总length减去不在range的两端元素个数。
        if (zn != NULL) {
            // 减去小于range的元素后，计算总的count数。
            rank = zslGetRank(zsl, zn->score, zn->ele);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            // 找到最后一个在range中的元素位置
            zn = zslLastInRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != NULL) {
                // count减去大于range的元素数，即为在range中的元素个数。
                rank = zslGetRank(zsl, zn->score, zn->ele);
                count -= (zsl->length - rank);
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    // 回复client，range中元素总的count数。
    addReplyLongLong(c, count);
}

// ZLEXCOUNT key min max，指定字典序区间元素数量
void zlexcountCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zlexrangespec range;
    unsigned long count = 0;

    /* Parse the range arguments */
    // 解析字典序range区间
    if (zslParseLexRange(c->argv[2],c->argv[3],&range) != C_OK) {
        addReplyError(c,"min or max not valid string range item");
        return;
    }

    /* Lookup the sorted set */
    // 获取key对象，确保是zset类型
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
    {
        zslFreeLexRange(&range);
        return;
    }

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        /* Use the first element in range as the starting point */
        // 定位ziplist编码的zset的第一个在range中的元素。
        eptr = zzlFirstInLexRange(zl,&range);

        /* No "first" element */
        // 如果没有元素在zset中，直接返回。
        if (eptr == NULL) {
            zslFreeLexRange(&range);
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        // 获取第一个在zset中元素的ele/score位置。
        sptr = ziplistNext(zl,eptr);
        serverAssertWithInfo(c,zobj,zzlLexValueLteMax(eptr,&range));

        /* Iterate over elements in range */
        // 遍历在字典序range中的元素进行计数。
        while (eptr) {
            /* Abort when the node is no longer in range. */
            // 如果当前遍历到的元素的ele不在range中，则跳出循环。
            if (!zzlLexValueLteMax(eptr,&range)) {
                break;
            } else {
                // 当前元素ele在range中，则count+1，跳到下一个元素继续进行处理。
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        // ziplist编码的zset，获取第一个在range中的元素。
        zn = zslFirstInLexRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        // 这里对在range中的元素计数，使用总length减去不在range中的两端元素个数。
        if (zn != NULL) {
            // 获取range中first元素的rank值，计算去除掉小于range的元素后的总count数。
            rank = zslGetRank(zsl, zn->score, zn->ele);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            // 定位到range中last元素。
            zn = zslLastInLexRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != NULL) {
                // 获取range中last元素的rank值，让用前面count减去大于range的元素数，即为最终在range中的元素数。
                rank = zslGetRank(zsl, zn->score, zn->ele);
                count -= (zsl->length - rank);
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    // 释放range空间，回复client总的在range中count数。
    zslFreeLexRange(&range);
    addReplyLongLong(c, count);
}

/* This command implements ZRANGEBYLEX, ZREVRANGEBYLEX. */
// 该函数用于实现ZRANGEBYLEX, ZREVRANGEBYLEX命令的通用方法。
void genericZrangebylexCommand(zrange_result_handler *handler,
    zlexrangespec *range, robj *zobj, int withscores, long offset, long limit,
    int reverse)
{
    client *c = handler->client;
    unsigned long rangelen = 0;

    // 开始lex range查询处理
    handler->beginResultEmission(handler);

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* If reversed, get the last node in range as starting point. */
        // 获取range遍历的第一个元素。reversed，则是range中最后一个元素。
        if (reverse) {
            eptr = zzlLastInLexRange(zl,range);
        } else {
            eptr = zzlFirstInLexRange(zl,range);
        }

        /* Get score pointer for the first element. */
        // 获取range遍历起始元素的score
        if (eptr)
            sptr = ziplistNext(zl,eptr);

        /* If there is an offset, just traverse the number of elements without
         * checking the ele because that is done in the next loop. */
        // 如果有offset参数，则定位到range遍历的offset元素位置。这里不check元素的ele是否在range中，后面循环会处理。
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        // 遍历获取元素
        while (eptr && limit--) {
            double score = 0;
            // 如果需要查询score，则这里提取当前遍历元素的score。
            if (withscores) /* don't bother to extract the score if it's gonna be ignored. */
                score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            // 检查当前遍历的ele是否在lex range中，如果不在则跳出循环。
            if (reverse) {
                if (!zzlLexValueGteMin(eptr,range)) break;
            } else {
                if (!zzlLexValueLteMax(eptr,range)) break;
            }

            /* We know the element exists, so ziplistGet should always
             * succeed. */
            // 元素ele在lex range中，提取ele数据。
            serverAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));

            rangelen++;
            // 将当前遍历的到元素数据加入到results结果中
            if (vstr == NULL) {
                handler->emitResultFromLongLong(handler, vlong, score);
            } else {
                handler->emitResultFromCBuffer(handler, vstr, vlen, score);
            }

            /* Move to next node */
            // 跳到下一个元素继续遍历处理。reverse，则使用zzlPrev跳到prev节点。
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        // 基于跳表的zset，定位到在lex range中的第一个节点。reversed，则定位到最后一个在range中的节点。
        if (reverse) {
            ln = zslLastInLexRange(zsl,range);
        } else {
            ln = zslFirstInLexRange(zsl,range);
        }

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        // 如果有offset，则定位到lex range遍历offset位置的节点，作为起始节点。这里不check节点的ele是否在lex range中，后面循环会处理。
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        // 遍历，将在range中的节点元素加入到结果中。
        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            // 如果遍历到的当前节点ele不在lex range中，则跳出循环。
            if (reverse) {
                if (!zslLexValueGteMin(ln->ele,range)) break;
            } else {
                if (!zslLexValueLteMax(ln->ele,range)) break;
            }

            rangelen++;
            // 当前节点在range中，则加入到结果results中
            handler->emitResultFromCBuffer(handler, ln->ele, sdslen(ln->ele), ln->score);

            /* Move to next node */
            // 跳到下一个节点继续遍历处理。reverse则跳到backward节点。
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    // 结束遍历，处理results数据回复client，或者加入到db中。
    handler->finalizeResultEmission(handler, rangelen);
}

/* ZRANGEBYLEX <key> <min> <max> [LIMIT offset count] */
void zrangebylexCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_LEX, ZRANGE_DIRECTION_FORWARD);
}

/* ZREVRANGEBYLEX <key> <min> <max> [LIMIT offset count] */
void zrevrangebylexCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_LEX, ZRANGE_DIRECTION_REVERSE);
}

/**
 * This function handles ZRANGE and ZRANGESTORE, and also the deprecated
 * Z[REV]RANGE[BYPOS|BYLEX] commands.
 *
 * The simple ZRANGE and ZRANGESTORE can take _AUTO in rangetype and direction,
 * other command pass explicit value.
 *
 * The argc_start points to the src key argument, so following syntax is like:
 * <src> <min> <max> [BYSCORE | BYLEX] [REV] [WITHSCORES] [LIMIT offset count]
 */
 // 这个函数用于实现ZRANGE、ZRANGESTORE，以及已弃用的 Z[REV]RANGE[BYPOS|BYLEX] 命令。
 // 简单的 ZRANGE 和 ZRANGESTORE 可以在 rangetype 和 direction 中取 _AUTO，其他命令需显示指定值。
 // argc_start 指示 src key参数，后面的的参数形式为：
 //     <src> <min> <max> [BYSCORE | BYLEX] [REV] [WITHSCORES] [LIMIT offset count]
void zrangeGenericCommand(zrange_result_handler *handler, int argc_start, int store,
                          zrange_type rangetype, zrange_direction direction)
{
    client *c = handler->client;
    robj *key = c->argv[argc_start];
    robj *zobj;
    zrangespec range;
    zlexrangespec lexrange;
    int minidx = argc_start + 1;
    int maxidx = argc_start + 2;

    /* Options common to all */
    long opt_start = 0;
    long opt_end = 0;
    int opt_withscores = 0;
    long opt_offset = 0;
    long opt_limit = -1;

    /* Step 1: Skip the <src> <min> <max> args and parse remaining optional arguments. */
    // 首先跳过 <src> <min> <max> 参数，解析后面的选项参数。
    for (int j=argc_start + 3; j < c->argc; j++) {
        int leftargs = c->argc-j-1;
        if (!store && !strcasecmp(c->argv[j]->ptr,"withscores")) {
            // 解析 withscores 参数
            opt_withscores = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"limit") && leftargs >= 2) {
            // 解析limit、offset参数
            if ((getLongFromObjectOrReply(c, c->argv[j+1], &opt_offset, NULL) != C_OK) ||
                (getLongFromObjectOrReply(c, c->argv[j+2], &opt_limit, NULL) != C_OK))
            {
                return;
            }
            j += 2;
        } else if (direction == ZRANGE_DIRECTION_AUTO &&
                   !strcasecmp(c->argv[j]->ptr,"rev"))
        {
            // 解析range方向 rev
            direction = ZRANGE_DIRECTION_REVERSE;
        } else if (rangetype == ZRANGE_AUTO &&
                   !strcasecmp(c->argv[j]->ptr,"bylex"))
        {
            // 解析range参数 bylex
            rangetype = ZRANGE_LEX;
        } else if (rangetype == ZRANGE_AUTO &&
                   !strcasecmp(c->argv[j]->ptr,"byscore"))
        {
            // 解析range参数 byscore
            rangetype = ZRANGE_SCORE;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    /* Use defaults if not overriden by arguments. */
    // 如果没有参数设置range信息，这里给定默认值。
    if (direction == ZRANGE_DIRECTION_AUTO)
        direction = ZRANGE_DIRECTION_FORWARD;
    if (rangetype == ZRANGE_AUTO)
        rangetype = ZRANGE_RANK;

    /* Check for conflicting arguments. */
    // 检查冲突的参数。limit参数只能在 bylex 或 byscore 使用。
    if (opt_limit != -1 && rangetype == ZRANGE_RANK) {
        addReplyError(c,"syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
        return;
    }
    // withscores 不支持bylex。因为bylex要求所有的元素score都一致才能使用，否则结果未定义。
    if (opt_withscores && rangetype == ZRANGE_LEX) {
        addReplyError(c,"syntax error, WITHSCORES not supported in combination with BYLEX");
        return;
    }

    // reverse的range是以[max,min]形式给定的，这里需要调整。
    if (direction == ZRANGE_DIRECTION_REVERSE &&
        ((ZRANGE_SCORE == rangetype) || (ZRANGE_LEX == rangetype)))
    {
        /* Range is given as [max,min] */
        int tmp = maxidx;
        maxidx = minidx;
        minidx = tmp;
    }

    /* Step 2: Parse the range. */
    // 其次，解析range值。
    switch (rangetype) {
    case ZRANGE_AUTO:
    case ZRANGE_RANK:
        /* Z[REV]RANGE, ZRANGESTORE [REV]RANGE */
        // 基于rank的range，直接解析参数值。
        if ((getLongFromObjectOrReply(c, c->argv[minidx], &opt_start,NULL) != C_OK) ||
            (getLongFromObjectOrReply(c, c->argv[maxidx], &opt_end,NULL) != C_OK))
        {
            return;
        }
        break;

    case ZRANGE_SCORE:
        /* Z[REV]RANGEBYSCORE, ZRANGESTORE [REV]RANGEBYSCORE */
        // 基于score的range。调用zslParseRange来解析。
        if (zslParseRange(c->argv[minidx], c->argv[maxidx], &range) != C_OK) {
            addReplyError(c, "min or max is not a float");
            return;
        }
        break;

    case ZRANGE_LEX:
        /* Z[REV]RANGEBYLEX, ZRANGESTORE [REV]RANGEBYLEX */
        // 基于ele lex的range。调用zslParseLexRange来解析。
        if (zslParseLexRange(c->argv[minidx], c->argv[maxidx], &lexrange) != C_OK) {
            addReplyError(c, "min or max not valid string range item");
            return;
        }
        break;
    }

    // 如果withscores或者store，我们都需要查询处score，则初始化handler的score相关标识。
    if (opt_withscores || store) {
        zrangeResultHandlerScoreEmissionEnable(handler);
    }

    /* Step 3: Lookup the key and get the range. */
    // 获取key对象，确保类型是zset。
    zobj = handler->dstkey ?
        lookupKeyWrite(c->db,key) :
        lookupKeyRead(c->db,key);
    if (zobj == NULL) {
        addReply(c,shared.emptyarray);
        goto cleanup;
    }

    if (checkType(c,zobj,OBJ_ZSET)) goto cleanup;

    /* Step 4: Pass this to the command-specific handler. */
    // 根据不同的range类型，调用不同的处理方法。
    switch (rangetype) {
    case ZRANGE_AUTO:
    case ZRANGE_RANK:
        // rank range
        genericZrangebyrankCommand(handler, zobj, opt_start, opt_end,
            opt_withscores || store, direction == ZRANGE_DIRECTION_REVERSE);
        break;

    case ZRANGE_SCORE:
        // score range
        genericZrangebyscoreCommand(handler, &range, zobj, opt_offset,
            opt_limit, direction == ZRANGE_DIRECTION_REVERSE);
        break;

    case ZRANGE_LEX:
        // ele lex range
        genericZrangebylexCommand(handler, &lexrange, zobj, opt_withscores || store,
            opt_offset, opt_limit, direction == ZRANGE_DIRECTION_REVERSE);
        break;
    }

    /* Instead of returning here, we'll just fall-through the clean-up. */

cleanup:

    if (rangetype == ZRANGE_LEX) {
        zslFreeLexRange(&lexrange);
    }
}

// ZCARD key，获取zset中总的元素数。
void zcardCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;

    // 获取key对象，并确保是zset类型
    if ((zobj = lookupKeyReadOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    // zsetLength获取总元素数，并回复给client
    addReplyLongLong(c,zsetLength(zobj));
}

// ZSCORE key member，返回zset中指定元素的score。
void zscoreCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    double score;

    // 获取key对象，并确保是zset类型
    if ((zobj = lookupKeyReadOrReply(c,key,shared.null[c->resp])) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    // zsetScore来获取指定元素的score，并回复client。
    if (zsetScore(zobj,c->argv[2]->ptr,&score) == C_ERR) {
        addReplyNull(c);
    } else {
        addReplyDouble(c,score);
    }
}

// ZMSCORE key member1 [member2 ...]，返回zset中多个元素的score。
void zmscoreCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    double score;
    // 获取key对象，并确保是zset类型
    zobj = lookupKeyRead(c->db,key);
    if (checkType(c,zobj,OBJ_ZSET)) return;

    addReplyArrayLen(c,c->argc - 2);
    // 变了参数members，分别获取他们的score，并回复给client。
    for (int j = 2; j < c->argc; j++) {
        /* Treat a missing set the same way as an empty set */
        if (zobj == NULL || zsetScore(zobj,c->argv[j]->ptr,&score) == C_ERR) {
            addReplyNull(c);
        } else {
            addReplyDouble(c,score);
        }
    }
}

// zrank的通用处理方法
void zrankGenericCommand(client *c, int reverse) {
    robj *key = c->argv[1];
    robj *ele = c->argv[2];
    robj *zobj;
    long rank;

    // 获取key对象，并确保是zset类型
    if ((zobj = lookupKeyReadOrReply(c,key,shared.null[c->resp])) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    serverAssertWithInfo(c,ele,sdsEncodedObject(ele));
    // zsetRank获取指定元素的rank值，并回复给client
    rank = zsetRank(zobj,ele->ptr,reverse);
    if (rank >= 0) {
        addReplyLongLong(c,rank);
    } else {
        addReplyNull(c);
    }
}

// ZRANK key member，获取zset中指定元素的rank
void zrankCommand(client *c) {
    zrankGenericCommand(c, 0);
}

// ZREVRANK key member，获取zset中指定元素的rank，逆序遍历。
void zrevrankCommand(client *c) {
    zrankGenericCommand(c, 1);
}

// ZSCAN key cursor [MATCH pattern] [COUNT count]，扫描获取zset中的元素。
void zscanCommand(client *c) {
    robj *o;
    unsigned long cursor;

    // 解析cursor参数
    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    // // 获取key对象，并确保是zset类型
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,OBJ_ZSET)) return;
    // 调用通用的scan方法处理扫描。
    scanGenericCommand(c,o,cursor);
}

/* This command implements the generic zpop operation, used by:
 * ZPOPMIN, ZPOPMAX, BZPOPMIN and BZPOPMAX. This function is also used
 * inside blocked.c in the unblocking stage of BZPOPMIN and BZPOPMAX.
 *
 * If 'emitkey' is true also the key name is emitted, useful for the blocking
 * behavior of BZPOP[MIN|MAX], since we can block into multiple keys.
 *
 * The synchronous version instead does not need to emit the key, but may
 * use the 'count' argument to return multiple items if available. */
// zpop操作的通用处理方法，用于实现ZPOPMIN, ZPOPMAX, BZPOPMIN 和 BZPOPMAX命令。
// 该函数也被使用于blocked.c中，用于处理解除阻塞的BZPOPMIN和BZPOPMAX操作。
// 如果emitkey为true，表示BZPOP[MIN|MAX]调用，则我们也要回复key，标识从哪个zset得到的数据，因为我们参数keys可能有多个。
// pop同步版本不需要发送key回复，但是可能需要count参数，从而返回多个元素。
void genericZpopCommand(client *c, robj **keyv, int keyc, int where, int emitkey, robj *countarg) {
    int idx;
    robj *key = NULL;
    robj *zobj = NULL;
    sds ele;
    double score;
    long count = 1;

    /* If a count argument as passed, parse it or return an error. */
    // 如果有count参数，则进行解析。
    if (countarg) {
        if (getLongFromObjectOrReply(c,countarg,&count,NULL) != C_OK)
            return;
        if (count <= 0) {
            addReply(c,shared.emptyarray);
            return;
        }
    }

    /* Check type and break on the first error, otherwise identify candidate. */
    // 检查传入的多个keys，他们在db中的对象类型都应该是zset。如果发现err则返回，否则如果找到第一个有效的对象即为我们后面处理的对象。
    idx = 0;
    while (idx < keyc) {
        key = keyv[idx++];
        zobj = lookupKeyWrite(c->db,key);
        if (!zobj) continue;
        if (checkType(c,zobj,OBJ_ZSET)) return;
        break;
    }

    /* No candidate for zpopping, return empty. */
    // 如果没有有效的zset对象用于zpop操作，则直接返回。
    if (!zobj) {
        addReply(c,shared.emptyarray);
        return;
    }

    void *arraylen_ptr = addReplyDeferredLen(c);
    long arraylen = 0;

    /* We emit the key only for the blocking variant. */
    // 对于block模式，emitkey为true，我们需要回复key告诉client，从哪个zset pop的数据
    if (emitkey) addReplyBulk(c,key);

    /* Remove the element. */
    // pop元素，回复client，遍历count次。
    do {
        if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
            unsigned char *zl = zobj->ptr;
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;

            /* Get the first or last element in the sorted set. */
            // 根据pop位置，定位到头或尾部的元素ele entry。
            eptr = ziplistIndex(zl,where == ZSET_MAX ? -2 : 0);
            serverAssertWithInfo(c,zobj,eptr != NULL);
            // 取出该位置的元素ele值。
            serverAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));
            if (vstr == NULL)
                ele = sdsfromlonglong(vlong);
            else
                ele = sdsnewlen(vstr,vlen);

            /* Get the score. */
            // 定位到两头位置的score entry，并取出该位置的元素score值
            sptr = ziplistNext(zl,eptr);
            serverAssertWithInfo(c,zobj,sptr != NULL);
            score = zzlGetScore(sptr);
        } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = zobj->ptr;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *zln;

            /* Get the first or last element in the sorted set. */
            // 获取头部或尾部的跳表中节点
            zln = (where == ZSET_MAX ? zsl->tail :
                                       zsl->header->level[0].forward);

            /* There must be an element in the sorted set. */
            serverAssertWithInfo(c,zobj,zln != NULL);
            // 获取头部或尾部节点的ele/score值
            ele = sdsdup(zln->ele);
            score = zln->score;
        } else {
            serverPanic("Unknown sorted set encoding");
        }

        // 删除zset中要pop的元素
        serverAssertWithInfo(c,zobj,zsetDel(zobj,ele));
        server.dirty++;

        // 仅在第一个迭代pop出元素时，我们处理key变更通知。
        if (arraylen == 0) { /* Do this only for the first iteration. */
            char *events[2] = {"zpopmin","zpopmax"};
            notifyKeyspaceEvent(NOTIFY_ZSET,events[where],key,c->db->id);
            signalModifiedKey(c,c->db,key);
        }

        // 将pop出的元素ele/score加入回复buffer中。
        addReplyBulkCBuffer(c,ele,sdslen(ele));
        addReplyDouble(c,score);
        sdsfree(ele);
        arraylen += 2;

        /* Remove the key, if indeed needed. */
        // 如果pop后，zset为空了，则需要从db中移除该key。
        if (zsetLength(zobj) == 0) {
            dbDelete(c->db,key);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
            break;
        }
    } while(--count);

    // 添加回复数据的总长度。
    setDeferredArrayLen(c,arraylen_ptr,arraylen + (emitkey != 0));
}

/* ZPOPMIN key [<count>] */
void zpopminCommand(client *c) {
    if (c->argc > 3) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }
    genericZpopCommand(c,&c->argv[1],1,ZSET_MIN,0,
        c->argc == 3 ? c->argv[2] : NULL);
}

/* ZPOPMAX key [<count>] */
void zpopmaxCommand(client *c) {
    if (c->argc > 3) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }
    genericZpopCommand(c,&c->argv[1],1,ZSET_MAX,0,
        c->argc == 3 ? c->argv[2] : NULL);
}

/* BZPOPMIN / BZPOPMAX actual implementation. */
void blockingGenericZpopCommand(client *c, int where) {
    robj *o;
    mstime_t timeout;
    int j;

    // 解析timeout参数
    if (getTimeoutFromObjectOrReply(c,c->argv[c->argc-1],&timeout,UNIT_SECONDS)
        != C_OK) return;

    // 遍历指定的keys，检查对应的obj对象，如果有元素则处理pop，不支持count参数。
    for (j = 1; j < c->argc-1; j++) {
        o = lookupKeyWrite(c->db,c->argv[j]);
        if (checkType(c,o,OBJ_ZSET)) return;
        if (o != NULL) {
            if (zsetLength(o) != 0) {
                /* Non empty zset, this is like a normal ZPOP[MIN|MAX]. */
                // 找到一个非空的zset，则调用genericZpopCommand，来pop元素回复。
                genericZpopCommand(c,&c->argv[j],1,where,1,NULL);
                /* Replicate it as an ZPOP[MIN|MAX] instead of BZPOP[MIN|MAX]. */
                // 成功pop了元素，则我们需要把阻塞命令改写为非阻塞命令，用于传播到slaves/aof。
                rewriteClientCommandVector(c,2,
                    where == ZSET_MAX ? shared.zpopmax : shared.zpopmin,
                    c->argv[j]);
                return;
            }
        }
    }

    /* If we are not allowed to block the client and the zset is empty the only thing
     * we can do is treating it as a timeout (even with timeout 0). */
    // 这里如果client不允许阻塞，则直接返回。
    if (c->flags & CLIENT_DENY_BLOCKING) {
        addReplyNullArray(c);
        return;
    }

    /* If the keys do not exist we must block */
    // 前面所有的keys对象都没有元素，则我们需要处理进入阻塞。
    blockForKeys(c,BLOCKED_ZSET,c->argv + 1,c->argc - 2,timeout,NULL,NULL,NULL);
}

// BZPOPMIN key [key ...] timeout
void bzpopminCommand(client *c) {
    blockingGenericZpopCommand(c,ZSET_MIN);
}

// BZPOPMAX key [key ...] timeout
void bzpopmaxCommand(client *c) {
    blockingGenericZpopCommand(c,ZSET_MAX);
}

// ziplist编码的zset，根据entries构造回复。主要用zrandmember回复。
static void zrandmemberReplyWithZiplist(client *c, unsigned int count, ziplistEntry *keys, ziplistEntry *vals) {
    // 遍历处理 entries
    for (unsigned long i = 0; i < count; i++) {
        // RESP3使用嵌套的数组回复，内层是一个数组，这里先写入数组长度2。
        if (vals && c->resp > 2)
            addReplyArrayLen(c,2);
        // ele值加入回复
        if (keys[i].sval)
            addReplyBulkCBuffer(c, keys[i].sval, keys[i].slen);
        else
            addReplyBulkLongLong(c, keys[i].lval);
        // score值加入回复
        if (vals) {
            if (vals[i].sval) {
                addReplyDouble(c, zzlStrtod(vals[i].sval,vals[i].slen));
            } else
                addReplyDouble(c, vals[i].lval);
        }
    }
}

/* How many times bigger should be the zset compared to the requested size
 * for us to not use the "remove elements" strategy? Read later in the
 * implementation for more info. */
// 当zset总的元素数小于请求count的3倍时，我们采用"移除元素"的方法处理。
#define ZRANDMEMBER_SUB_STRATEGY_MUL 3

/* If client is trying to ask for a very large number of random elements,
 * queuing may consume an unlimited amount of memory, so we want to limit
 * the number of randoms per time. */
// 当client请求获取随机元素的count很大时。我们分批处理，避免一次性需要分配很多的额外内存来辅助获取随机元素。
#define ZRANDMEMBER_RANDOM_SAMPLE_LIMIT 1000

void zrandmemberWithCountCommand(client *c, long l, int withscores) {
    unsigned long count, size;
    int uniq = 1;
    robj *zsetobj;

    if ((zsetobj = lookupKeyReadOrReply(c, c->argv[1], shared.null[c->resp]))
        == NULL || checkType(c, zsetobj, OBJ_ZSET)) return;
    size = zsetLength(zsetobj);

    // 传入l为正表示uniq，为负表示返回数据可重复。
    if(l >= 0) {
        count = (unsigned long) l;
    } else {
        count = -l;
        uniq = 0;
    }

    /* If count is zero, serve it ASAP to avoid special cases later. */
    // count为0，则直接返回
    if (count == 0) {
        addReply(c,shared.emptyarray);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. This case is the only one that also needs to return the
     * elements in random order. */
    // 情形1：count为负，表示可重复，则我们直接在整个集合中取count次元素返回就可以了。
    // 这种情况处理较为简单，不需要额外的辅助结构。且这种情况返回的元素是按随机顺序返回的。
    if (!uniq || count == 1) {
        // RESP3对于返回score是采用嵌套数组返回的。RESP2是单数组返回所以数组长度为count*2
        if (withscores && c->resp == 2)
            addReplyArrayLen(c, count*2);
        else
            addReplyArrayLen(c, count);
        if (zsetobj->encoding == OBJ_ENCODING_SKIPLIST) {
            // 跳表编码的zset，count次随机调用dictGetFairRandomKey，从dict中随机获取元素，处理加入回复buffer中。
            zset *zs = zsetobj->ptr;
            while (count--) {
                // 随机获取元素
                dictEntry *de = dictGetFairRandomKey(zs->dict);
                sds key = dictGetKey(de);
                // 加入到回复buffer中
                if (withscores && c->resp > 2)
                    addReplyArrayLen(c,2);
                addReplyBulkCBuffer(c, key, sdslen(key));
                if (withscores)
                    addReplyDouble(c, dictGetDoubleVal(de));
            }
        } else if (zsetobj->encoding == OBJ_ENCODING_ZIPLIST) {
            // ziplist编码的zset，分批调用ziplistRandomPairs，从ziplist中随机获取元素，处理加入回复buffer中。
            ziplistEntry *keys, *vals = NULL;
            unsigned long limit, sample_count;
            // count很大时，分批处理。避免我们需要额外分配很大的连续内存来辅助获取随机元素。
            limit = count > ZRANDMEMBER_RANDOM_SAMPLE_LIMIT ? ZRANDMEMBER_RANDOM_SAMPLE_LIMIT : count;
            keys = zmalloc(sizeof(ziplistEntry)*limit);
            if (withscores)
                vals = zmalloc(sizeof(ziplistEntry)*limit);
            while (count) {
                // 每次最多获取limit个随机元素。
                sample_count = count > limit ? limit : count;
                count -= sample_count;
                ziplistRandomPairs(zsetobj->ptr, sample_count, keys, vals);
                // 加入回复buffer中
                zrandmemberReplyWithZiplist(c, sample_count, keys, vals);
            }
            zfree(keys);
            zfree(vals);
        }
        return;
    }

    zsetopsrc src;
    zsetopval zval;
    src.subject = zsetobj;
    src.type = zsetobj->type;
    src.encoding = zsetobj->encoding;
    zuiInitIterator(&src);
    memset(&zval, 0, sizeof(zval));

    /* Initiate reply count, RESP3 responds with nested array, RESP2 with flat one. */
    // 初始化回复count，RESP3使用嵌套数组，而RESP2使用单个平铺数组。
    long reply_size = count < size ? count : size;
    if (withscores && c->resp == 2)
        addReplyArrayLen(c, reply_size*2);
    else
        addReplyArrayLen(c, reply_size);

    /* CASE 2:
    * The number of requested elements is greater than the number of
    * elements inside the zset: simply return the whole zset. */
    // 情形2：如果count大于zset总的元素个数（返回数据不可重复），则我们遍历集合，返回所有的元素。
    if (count >= size) {
        while (zuiNext(&src, &zval)) {
            // RESP3，嵌套内层数组处理。
            if (withscores && c->resp > 2)
                addReplyArrayLen(c,2);
            // 返回元素ele/score
            addReplyBulkSds(c, zuiNewSdsFromValue(&zval));
            if (withscores)
                addReplyDouble(c, zval.score);
        }
        return;
    }

    /* CASE 3:
     * The number of elements inside the zset is not greater than
     * ZRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a dict from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requested elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 4 is highly inefficient. */
    // 情形3：zset中元素总数 < 3*count时，我们创建一个dict包含zset所有元素，然后随机从dict中移除元素。
    // 当移除元素后，dict剩余元素数量与请求的count一致时，我们返回整个dict元素即可。
    // 采用这种方式，是因为如果请求count与zset总元素数相差不大时，case4随机从原zset中获取不重复的元素效率很低。
    if (count*ZRANDMEMBER_SUB_STRATEGY_MUL > size) {
        // 创建一个临时dict
        dict *d = dictCreate(&sdsReplyDictType, NULL);
        dictExpand(d, size);
        /* Add all the elements into the temporary dictionary. */
        // 遍历zset，将所有元素加入到新的dict中。如果需要score则也加入。
        while (zuiNext(&src, &zval)) {
            sds key = zuiNewSdsFromValue(&zval);
            dictEntry *de = dictAddRaw(d, key, NULL);
            serverAssert(de);
            if (withscores)
                dictSetDoubleVal(de, zval.score);
        }
        serverAssert(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        // 随机从dict中移除元素，知道剩余元素数量等于count。
        while (size > count) {
            dictEntry *de;
            de = dictGetRandomKey(d);
            dictUnlink(d,dictGetKey(de));
            sdsfree(dictGetKey(de));
            dictFreeUnlinkedEntry(d,de);
            size--;
        }

        /* Reply with what's in the dict and release memory */
        // 迭代遍历dict中剩余的元素，添加到回复buffer中。
        dictIterator *di;
        dictEntry *de;
        di = dictGetIterator(d);
        while ((de = dictNext(di)) != NULL) {
            // RESP3嵌套数组处理。
            if (withscores && c->resp > 2)
                addReplyArrayLen(c,2);
            // 添加元素的ele/score到buffer中，等待回复给client。
            addReplyBulkSds(c, dictGetKey(de));
            if (withscores)
                addReplyDouble(c, dictGetDoubleVal(de));
        }

        dictReleaseIterator(di);
        dictRelease(d);
    }

    /* CASE 4: We have a big zset compared to the requested number of elements.
     * In this case we can simply get random elements from the zset and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. */
    // 情形4：相对于请求的count，我们有一个很大的zset。因为如果zset与count相差不大，可能最后需要随机取很多次才能取到不重复元素。
    // 这种情况我们简单的从zset中随机获取元素，然后加入到辅助dict中去重，最后获取到指定count的不重复元素返回。
    else {
        if (zsetobj->encoding == OBJ_ENCODING_ZIPLIST) {
            /* it is inefficient to repeatedly pick one random element from a
             * ziplist. so we use this instead: */
            // 每次从ziplist中获取单个元素，重复多次，这种是很低效的。所以我们这里使用算法一次获取count个随机元素。
            // 具体处理见ziplistRandomPairsUnique中使用的随机算法。
            ziplistEntry *keys, *vals = NULL;
            keys = zmalloc(sizeof(ziplistEntry)*count);
            if (withscores)
                vals = zmalloc(sizeof(ziplistEntry)*count);
            // 随机从ziplist中获取count个元素。
            serverAssert(ziplistRandomPairsUnique(zsetobj->ptr, count, keys, vals) == count);
            // 处理entries数据回复给client。
            zrandmemberReplyWithZiplist(c, count, keys, vals);
            zfree(keys);
            zfree(vals);
            return;
        }

        /* Hashtable encoding (generic implementation) */
        // hash+skiplist编码的zset。先创建临时dict，然后随机从zset中取元素加入到该dict中。
        unsigned long added = 0;
        dict *d = dictCreate(&hashDictType, NULL);
        dictExpand(d, count);

        // 遍历直到临时dict加入的不重复元素数量 == 请求count时，处理完成返回。
        while (added < count) {
            ziplistEntry key;
            double score;
            // 随机获取元素
            zsetTypeRandomElement(zsetobj, size, &key, withscores ? &score: NULL);

            /* Try to add the object to the dictionary. If it already exists
            * free it, otherwise increment the number of objects we have
            * in the result dictionary. */
            // 尝试将获取到的元素加入到临时dict中
            sds skey = zsetSdsFromZiplistEntry(&key);
            if (dictAdd(d,skey,NULL) != DICT_OK) {
                sdsfree(skey);
                continue;
            }
            // 成功加入，added计数+1。
            added++;

            // 将当前获取的元素，加入到回复buffer中。
            if (withscores && c->resp > 2)
                addReplyArrayLen(c,2);
            zsetReplyFromZiplistEntry(c, &key);
            if (withscores)
                addReplyDouble(c, score);
        }

        /* Release memory */
        // 处理完成，释放临时dict。
        dictRelease(d);
    }
}

/* ZRANDMEMBER [<count> WITHSCORES] */
// 随机从zset中获取count个元素。count为负值表示返回元素可以重复。
void zrandmemberCommand(client *c) {
    long l;
    int withscores = 0;
    robj *zset;
    ziplistEntry ele;

    if (c->argc >= 3) {
        // 获取count参数
        if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) return;
        // 获取withscores参选
        if (c->argc > 4 || (c->argc == 4 && strcasecmp(c->argv[3]->ptr,"withscores"))) {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        } else if (c->argc == 4)
            withscores = 1;
        // 处理随机获取count个元素返回
        zrandmemberWithCountCommand(c, l, withscores);
        return;
    }

    /* Handle variant without <count> argument. Reply with simple bulk string */
    // 没有count参数，则随机取一个元素。返回值是一个简单的bulk string，单独处理
    if ((zset = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]))== NULL ||
        checkType(c,zset,OBJ_ZSET)) {
        return;
    }

    // 随机获取一个元素返回。
    zsetTypeRandomElement(zset, zsetLength(zset), &ele,NULL);
    zsetReplyFromZiplistEntry(c,&ele);
}
