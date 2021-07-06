/* adlist.c - A generic doubly linked list implementation
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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


#include <stdlib.h>
#include "adlist.h"
#include "zmalloc.h"

/* Create a new list. The created list can be freed with
 * listRelease(), but private value of every node need to be freed
 * by the user before to call listRelease(), or by setting a free method using
 * listSetFreeMethod.
 *
 * On error, NULL is returned. Otherwise the pointer to the new list. */
// 创建链表
list *listCreate(void)
{
    struct list *list;

    if ((list = zmalloc(sizeof(*list))) == NULL)
        return NULL;
    list->head = list->tail = NULL;
    list->len = 0;
    list->dup = NULL;
    list->free = NULL;
    list->match = NULL;
    return list;
}

/* Remove all the elements from the list without destroying the list itself. */
// 清空链表元素，但不释放list结构。注意要先释放节点value，再free节点。
void listEmpty(list *list)
{
    unsigned long len;
    listNode *current, *next;

    current = list->head;
    len = list->len;
    // 循环处理
    while(len--) {
        next = current->next;
        // 如果value是堆上分配的内存，需要设置value释放函数，调用释放节点value
        if (list->free) list->free(current->value);
        // 释放节点
        zfree(current);
        current = next;
    }
    // 头尾指针置空，长度置为0
    list->head = list->tail = NULL;
    list->len = 0;
}

/* Free the whole list.
 *
 * This function can't fail. */
// 释放整个链表，先情况所有元素，再释放list结构。
void listRelease(list *list)
{
    listEmpty(list);
    zfree(list);
}

/* Add a new node to the list, to head, containing the specified 'value'
 * pointer as value.
 *
 * On error, NULL is returned and no operation is performed (i.e. the
 * list remains unaltered).
 * On success the 'list' pointer you pass to the function is returned. */
// 头插法添加节点
list *listAddNodeHead(list *list, void *value)
{
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    node->value = value;
    // 因为没有使用单独的头节点，所以这里单独处理空链表的情况。
    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        // 非空链表，在第一个元素前插入，更新head指针
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }
    // 更新元素数量
    list->len++;
    return list;
}

/* Add a new node to the list, to tail, containing the specified 'value'
 * pointer as value.
 *
 * On error, NULL is returned and no operation is performed (i.e. the
 * list remains unaltered).
 * On success the 'list' pointer you pass to the function is returned. */
// 在链表末尾加入节点
list *listAddNodeTail(list *list, void *value)
{
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    node->value = value;
    if (list->len == 0) {
        // 处理空链表的情况
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        // 加入节点，处理tail指针。
        node->prev = list->tail;
        node->next = NULL;
        list->tail->next = node;
        list->tail = node;
    }
    // 更新元素数量
    list->len++;
    return list;
}

// 在某个节点前/后插入节点，双向链表，好操作
list *listInsertNode(list *list, listNode *old_node, void *value, int after) {
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    node->value = value;
    // 先把节点插入，处理插入节点的前后指针。
    if (after) {
        node->prev = old_node;
        node->next = old_node->next;
        // after要处理插入末尾的情况，更新tail指针
        if (list->tail == old_node) {
            list->tail = node;
        }
    } else {
        node->next = old_node;
        node->prev = old_node->prev;
        // before要处理头部插入的情况，更新head指针
        if (list->head == old_node) {
            list->head = node;
        }
    }
    // （TODO）注意因为前面处理前后指针只处理了head或者tail指针中的一个，但对于空链表插入新节点，head、tail都要更新。
    // 处理完插入节点的前后指针后，需要判断前后指针是否为空，从而处理前节点的next和后节点的prev。
    if (node->prev != NULL) {
        node->prev->next = node;
    }
    if (node->next != NULL) {
        node->next->prev = node;
    }
    //更新链表长度
    list->len++;
    return list;
}

/* Remove the specified node from the specified list.
 * It's up to the caller to free the private value of the node.
 *
 * This function can't fail. */
// 删除指定链表的指定节点。需要调用者处理节点value的free，或者list中设置了value free函数。
void listDelNode(list *list, listNode *node)
{
    // 如果有前驱，处理前驱的next。否则删除的是头节点，需要处理head指针。
    if (node->prev)
        node->prev->next = node->next;
    else
        list->head = node->next;
    // 如果有后驱，处理后驱的prev。否则删除的是末尾节点，处理tail指针。
    if (node->next)
        node->next->prev = node->prev;
    else
        list->tail = node->prev;
    // free节点的value，free节点本身。
    if (list->free) list->free(node->value);
    zfree(node);
    // 列表节点数-1
    list->len--;
}

/* Returns a list iterator 'iter'. After the initialization every
 * call to listNext() will return the next element of the list.
 *
 * This function can't fail. */
// 初始化获取一个列表的迭代器。迭代器中总是保存了下一次访问的节点指针和迭代方向。
listIter *listGetIterator(list *list, int direction)
{
    listIter *iter;

    if ((iter = zmalloc(sizeof(*iter))) == NULL) return NULL;
    // 根据迭代方向，设置初始化的起始节点是head还是tail
    if (direction == AL_START_HEAD)
        iter->next = list->head;
    else
        iter->next = list->tail;
    iter->direction = direction;
    return iter;
}

/* Release the iterator memory */
// 是否迭代器内存
void listReleaseIterator(listIter *iter) {
    zfree(iter);
}

/* Create an iterator in the list private iterator structure */
// 构建list的正向迭代器
void listRewind(list *list, listIter *li) {
    li->next = list->head;
    li->direction = AL_START_HEAD;
}

// 构建list的反向迭代器
void listRewindTail(list *list, listIter *li) {
    li->next = list->tail;
    li->direction = AL_START_TAIL;
}

/* Return the next element of an iterator.
 * It's valid to remove the currently returned element using
 * listDelNode(), but not to remove other elements.
 *
 * The function returns a pointer to the next element of the list,
 * or NULL if there are no more elements, so the classical usage
 * pattern is:
 *
 * iter = listGetIterator(list,<direction>);
 * while ((node = listNext(iter)) != NULL) {
 *     doSomethingWith(listNodeValue(node));
 * }
 *
 * */
// 通过迭代器获取下一个要迭代处理的元素。元素处理支持删除操作，因为迭代器已经保存了再下一个元素的指针。
listNode *listNext(listIter *iter)
{
    // 拿到迭代器保存的当前将要返回的元素。
    listNode *current = iter->next;

    // 如果元素为空，则迭代结束了。反之需要更新迭代器保存的下一个元素，根据迭代方向不同，取next或prev。
    if (current != NULL) {
        if (iter->direction == AL_START_HEAD)
            iter->next = current->next;
        else
            iter->next = current->prev;
    }
    return current;
}

/* Duplicate the whole list. On out of memory NULL is returned.
 * On success a copy of the original list is returned.
 *
 * The 'Dup' method set with listSetDupMethod() function is used
 * to copy the node value. Otherwise the same pointer value of
 * the original node is used as value of the copied node.
 *
 * The original list both on success or error is never modified. */
// 链表复制
list *listDup(list *orig)
{
    list *copy;
    listIter iter;
    listNode *node;

    // 创建一个新的链表
    if ((copy = listCreate()) == NULL)
        return NULL;
    // 三个处理方法赋值
    copy->dup = orig->dup;
    copy->free = orig->free;
    copy->match = orig->match;
    // 获取原链表正向迭代器，准备迭代
    listRewind(orig, &iter);
    // 循环迭代，直到通过迭代器获取的next节点为NULL，说明迭代处理晚了。
    while((node = listNext(&iter)) != NULL) {
        void *value;

        // 如果list有dup方法，使用该方法复制节点的值。否则，直接=赋值
        if (copy->dup) {
            value = copy->dup(node->value);
            if (value == NULL) {
                // 出错了清除整个新的链表，返回NULL
                listRelease(copy);
                return NULL;
            }
        } else
            value = node->value;
        // 当拿到value后，使用listAddNodeTail方法在新链表的尾部插入节点。
        // 前面正向遍历，这里就需要在尾部插入；前面使用逆向遍历，这里就是在头部插入。
        if (listAddNodeTail(copy, value) == NULL) {
            listRelease(copy);
            return NULL;
        }
    }
    return copy;
}

/* Search the list for a node matching a given key.
 * The match is performed using the 'match' method
 * set with listSetMatchMethod(). If no 'match' method
 * is set, the 'value' pointer of every node is directly
 * compared with the 'key' pointer.
 *
 * On success the first matching node pointer is returned
 * (search starts from head). If no matching node exists
 * NULL is returned. */
// 在链表中查找指定值的节点返回。有多个匹配会返回第一个找的节点。
listNode *listSearchKey(list *list, void *key)
{
    listIter iter;
    listNode *node;

    // 正向迭代遍历，如果有match函数使用match进行匹配，没有的话直接==进行对比。
    // 最终返回第一个匹配的节点。
    listRewind(list, &iter);
    while((node = listNext(&iter)) != NULL) {
        if (list->match) {
            if (list->match(node->value, key)) {
                return node;
            }
        } else {
            if (key == node->value) {
                return node;
            }
        }
    }
    return NULL;
}

/* Return the element at the specified zero-based index
 * where 0 is the head, 1 is the element next to head
 * and so on. Negative integers are used in order to count
 * from the tail, -1 is the last element, -2 the penultimate
 * and so on. If the index is out of range NULL is returned. */
// 根据index找对应的节点，index可以为负数。0表示第一个节点，-1最后一个节点，-2倒数第二个节点。
listNode *listIndex(list *list, long index) {
    listNode *n;

    if (index < 0) {
        // 当传入index为负数时，取反并-1，表示逆向的第0个节点。
        // 然后处理流程基本跟>=0一致了。
        index = (-index)-1;
        n = list->tail;
        while(index-- && n) n = n->prev;
    } else {
        // 先指向head，如果index>0就向后遍历。当index==0时，n总是指向所找的节点。
        n = list->head;
        while(index-- && n) n = n->next;
    }
    return n;
}

/* Rotate the list removing the tail node and inserting it to the head. */
// 将尾节点移除放到list头
void listRotateTailToHead(list *list) {
    if (listLength(list) <= 1) return;

    /* Detach current tail */
    // 处理tail的相关指针
    listNode *tail = list->tail;
    list->tail = tail->prev;
    list->tail->next = NULL;
    /* Move it as head */
    // 将tail加入list头，处理相关指针
    list->head->prev = tail;
    tail->prev = NULL;
    tail->next = list->head;
    list->head = tail;
}

/* Rotate the list removing the head node and inserting it to the tail. */
// 将头节点移除放到list末尾
void listRotateHeadToTail(list *list) {
    if (listLength(list) <= 1) return;

    // 处理head的相关指针
    listNode *head = list->head;
    /* Detach current head */
    list->head = head->next;
    list->head->prev = NULL;
    /* Move it as tail */
    // 将head加入list尾，处理相关指针
    list->tail->next = head;
    head->next = NULL;
    head->prev = list->tail;
    list->tail = head;
}

/* Add all the elements of the list 'o' at the end of the
 * list 'l'. The list 'other' remains empty but otherwise valid. */
// 两个链表合并
void listJoin(list *l, list *o) {
    // 如果o是空链表，直接返回
    if (o->len == 0) return;

    // 这里o必有元素，所以o->head一定不为空。
    // 设置o头节点的前驱为l的尾节点。
    o->head->prev = l->tail;

    // 如果l尾节点存在，那么需要处理为节点的后驱（next）。
    // 如果l尾节点不存在，即l为空链表。则l的head即为o的head。
    if (l->tail)
        l->tail->next = o->head;
    else
        l->head = o->head;

    // 更新新链表的tail和len
    l->tail = o->tail;
    l->len += o->len;

    /* Setup other as an empty list. */
    // 设置o为空链表。
    o->head = o->tail = NULL;
    o->len = 0;
}
