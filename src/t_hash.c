/*
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

#include "server.h"
#include <math.h>

/*-----------------------------------------------------------------------------
 * Hash type API
 *----------------------------------------------------------------------------*/

/* Check the length of a number of objects to see if we need to convert a
 * ziplist to a real hash. Note that we only check string encoded objects
 * as their string length can be queried in constant time. */
// 检查多个对象的长度，是否需要将ziplist存储转换为真正的hash。注意这里只检查字符串对象，因为他们的长度能在常数时间获得。
void hashTypeTryConversion(robj *o, robj **argv, int start, int end) {
    int i;

    // 如果不是ziplist存储，则不需要处理。
    if (o->encoding != OBJ_ENCODING_ZIPLIST) return;

    for (i = start; i <= end; i++) {
        // 遍历[start, end]之间的字符串对象，如果有出现一个长度大于默认64的，则我们需要将ziplist存储转换为hash表。
        if (sdsEncodedObject(argv[i]) &&
            sdslen(argv[i]->ptr) > server.hash_max_ziplist_value)
        {
            hashTypeConvert(o, OBJ_ENCODING_HT);
            break;
        }
    }
}

/* Get the value from a ziplist encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
// 从ziplist编码的hash中，根据field获取value相关信息，需要从head开始依次遍历查询。
// 提供的参数用于返回value数据（字符串或数字），函数返回-1表示我们没找到该field。
int hashTypeGetFromZiplist(robj *o, sds field,
                           unsigned char **vstr,
                           unsigned int *vlen,
                           long long *vll)
{
    unsigned char *zl, *fptr = NULL, *vptr = NULL;
    int ret;

    serverAssert(o->encoding == OBJ_ENCODING_ZIPLIST);

    zl = o->ptr;
    // 获取ziplist中entry数据段的起始位置指针，我们将从该位置开始向后查询对比key。
    fptr = ziplistIndex(zl, ZIPLIST_HEAD);
    if (fptr != NULL) {
        // fptr != NULL 表示该ziplist有元素，那么我们调用ziplistFind，间隔为1，来查询key值看是否与field一致。
        fptr = ziplistFind(zl, fptr, (unsigned char*)field, sdslen(field), 1);
        if (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */
            // 如果查到了与field值一致的key，则我们取出ziplist中它的下一个entry即为value。
            vptr = ziplistNext(zl, fptr);
            serverAssert(vptr != NULL);
        }
    }

    if (vptr != NULL) {
        // 找到了value，解析数据
        ret = ziplistGet(vptr, vstr, vlen, vll);
        serverAssert(ret);
        return 0;
    }

    return -1;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns NULL when the field cannot be found, otherwise the SDS value
 * is returned. */
// 从hash表编码的hash对象中，根据field获取value。找到则返回字符串值，没找到返回NULL。
sds hashTypeGetFromHashTable(robj *o, sds field) {
    dictEntry *de;

    serverAssert(o->encoding == OBJ_ENCODING_HT);

    // dictFind根据field查询对应的entry。没找到返回NULL，找到了使用dictGetVal获取值返回。
    de = dictFind(o->ptr, field);
    if (de == NULL) return NULL;
    return dictGetVal(de);
}

/* Higher level function of hashTypeGet*() that returns the hash value
 * associated with the specified field. If the field is found C_OK
 * is returned, otherwise C_ERR. The returned object is returned by
 * reference in either *vstr and *vlen if it's returned in string form,
 * or stored in *vll if it's returned as a number.
 *
 * If *vll is populated *vstr is set to NULL, so the caller
 * can always check the function return by checking the return value
 * for C_OK and checking if vll (or vstr) is NULL. */
// 高级别的hashTypeGet*()函数，根据指定的field，从hash对象中查询value返回。具体会根据底层编码方式处理ziplist和hash的查询。
// 返回参数用于填充查询到的值，如果函数返回ok表示查询到了，进一步调用者通过*vstr是否为NULL，来判断值是字符串还是数字。
int hashTypeGetValue(robj *o, sds field, unsigned char **vstr, unsigned int *vlen, long long *vll) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        *vstr = NULL;
        // 从ziplist中查询field对应的值
        if (hashTypeGetFromZiplist(o, field, vstr, vlen, vll) == 0)
            return C_OK;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds value;
        // 从hash表中查询field对应的值
        if ((value = hashTypeGetFromHashTable(o, field)) != NULL) {
            *vstr = (unsigned char*) value;
            *vlen = sdslen(value);
            return C_OK;
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
    return C_ERR;
}

/* Like hashTypeGetValue() but returns a Redis object, which is useful for
 * interaction with the hash type outside t_hash.c.
 * The function returns NULL if the field is not found in the hash. Otherwise
 * a newly allocated string object with the value is returned. */
// 该函数跟hashTypeGetValue()类似，不过这里返回一个robj结构，这对于与t_hash.c之外的hash类型交互很有用。
// 如果没找到field，则函数返回NULL，否则返回一个基于查到的vlue新构造的robj对象。
robj *hashTypeGetValueObject(robj *o, sds field) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    // 调用hashTypeGetValue根据field查询value。
    if (hashTypeGetValue(o,field,&vstr,&vlen,&vll) == C_ERR) return NULL;
    // 根据value的类型，不同方式创建新对象返回。
    if (vstr) return createStringObject((char*)vstr,vlen);
    else return createStringObjectFromLongLong(vll);
}

/* Higher level function using hashTypeGet*() to return the length of the
 * object associated with the requested field, or 0 if the field does not
 * exist. */
// 返回field关联value的长度，具体是调用hashTypeGet*()进行处理的。如果field不存在，则返回0。
size_t hashTypeGetValueLength(robj *o, sds field) {
    size_t len = 0;
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        // 从ziplist中查询field关联value，查到了则根据value对象类型返回相应的长度。
        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0)
            len = vstr ? vlen : sdigits10(vll);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds aux;

        // 从hash表中查询field关联value，hash表中k、v都是字符串，所以直接使用sdslen获取长度即可。
        if ((aux = hashTypeGetFromHashTable(o, field)) != NULL)
            len = sdslen(aux);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return len;
}

/* Test if the specified field exists in the given hash. Returns 1 if the field
 * exists, and 0 when it doesn't. */
// 判断指定的field是否在给定的hash对象中，具体其实就是查询该field，如果查到了则存在。如果存在函数返回1，否则返回0。
int hashTypeExists(robj *o, sds field) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) return 1;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (hashTypeGetFromHashTable(o, field) != NULL) return 1;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return 0;
}

/* Add a new field, overwrite the old with the new value if it already exists.
 * Return 0 on insert and 1 on update.
 *
 * By default, the key and value SDS strings are copied if needed, so the
 * caller retains ownership of the strings passed. However this behavior
 * can be effected by passing appropriate flags (possibly bitwise OR-ed):
 *
 * HASH_SET_TAKE_FIELD -- The SDS field ownership passes to the function.
 * HASH_SET_TAKE_VALUE -- The SDS value ownership passes to the function.
 *
 * When the flags are used the caller does not need to release the passed
 * SDS string(s). It's up to the function to use the string to create a new
 * entry or to free the SDS string before returning to the caller.
 *
 * HASH_SET_COPY corresponds to no flags passed, and means the default
 * semantics of copying the values if needed.
 *
 */
// 添加一个新的field，如果field存在，更新值为新的value。如果是插入新field则返回0，如果只是更新value则返回1。
// 默认情况，如果需要我们会copy key和value来使用，所以调用者会保留传入的key、value字符串的所有权，但是我们可以通过传入flag来改变这一行为。
//  HASH_SET_COPY，表示没有设置flags，默认需要的话会copy。
//  HASH_SET_TAKE_FIELD，表示sds field的所有权转交给当前函数。
//  HASH_SET_TAKE_VALUE，表示sds value的所有权转交给当前函数。
// 当前flag设置，移交了所有权时，调用者不需要释放传入的sds字符串。而本函数需要使用该字符串创建新的entry，或者在返回前释放掉。
#define HASH_SET_TAKE_FIELD (1<<0)
#define HASH_SET_TAKE_VALUE (1<<1)
#define HASH_SET_COPY 0
int hashTypeSet(robj *o, sds field, sds value, int flags) {
    int update = 0;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        // ziplist编码处理
        unsigned char *zl, *fptr, *vptr;

        zl = o->ptr;
        // 返回ziplist entry数据部分指针（指向head entry）。
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            // 从头向后遍历，查找与field匹配的entry。
            fptr = ziplistFind(zl, fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                // 找到了对应field的entry，获取value的entry（即当前field entry的下一个entry），并设置update标识。
                vptr = ziplistNext(zl, fptr);
                serverAssert(vptr != NULL);
                update = 1;

                /* Replace value */
                // update，这里调用ziplistReplace来处理value替换（相当于删除老entry，插入新entry）。
                zl = ziplistReplace(zl, vptr, (unsigned char*)value,
                        sdslen(value));
            }
        }

        if (!update) {
            /* Push new field/value pair onto the tail of the ziplist */
            // 如果不是update，我们需要创建新的field/value entry加入到ziplist的尾部（先写入field，再写入value）。
            zl = ziplistPush(zl, (unsigned char*)field, sdslen(field),
                    ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)value, sdslen(value),
                    ZIPLIST_TAIL);
        }
        o->ptr = zl;

        /* Check if the ziplist needs to be converted to a hash table */
        // 加入了新的元素，检查是否有必要将ziplist转为hash表存储。默认大于512个field/value对时，转换。
        // 另外前面在开始执行命令时，会检查field和value的长度，默认如果超过64字节，也会转为hash表存储。
        if (hashTypeLength(o) > server.hash_max_ziplist_entries)
            hashTypeConvert(o, OBJ_ENCODING_HT);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        // hash表存储，直接通过dictFind来查询dict entry
        dictEntry *de = dictFind(o->ptr,field);
        if (de) {
            // entry存在，则释放掉原value字符串，用新的value替换。如果有传入标识要求使用传入sds则直接使用，否则copy字符串使用。
            sdsfree(dictGetVal(de));
            if (flags & HASH_SET_TAKE_VALUE) {
                dictGetVal(de) = value;
                value = NULL;
            } else {
                dictGetVal(de) = sdsdup(value);
            }
            update = 1;
        } else {
            // entry不存在，dictAdd将field/value加入。同样标识指定使用传入sds则直接使用，否则copy来使用。
            sds f,v;
            if (flags & HASH_SET_TAKE_FIELD) {
                f = field;
                field = NULL;
            } else {
                f = sdsdup(field);
            }
            if (flags & HASH_SET_TAKE_VALUE) {
                v = value;
                value = NULL;
            } else {
                v = sdsdup(value);
            }
            dictAdd(o->ptr,f,v);
        }
    } else {
        serverPanic("Unknown hash encoding");
    }

    /* Free SDS strings we did not referenced elsewhere if the flags
     * want this function to be responsible. */
    // 如果标识要当前函数接管传入的sds，则这里需要处理释放。
    // 对于hash表存储，我们使用后会置为NULL；而对于ziplist存储，我们实际上是copy的字节数据，没直接拿字符串使用，这样需要free掉。
    if (flags & HASH_SET_TAKE_FIELD && field) sdsfree(field);
    if (flags & HASH_SET_TAKE_VALUE && value) sdsfree(value);
    return update;
}

/* Delete an element from a hash.
 * Return 1 on deleted and 0 on not found. */
// 从hash对象中删除一个元素。删除成功则返回1，没找到则返回0。
int hashTypeDelete(robj *o, sds field) {
    int deleted = 0;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr;

        zl = o->ptr;
        // ziplist存储，先获取entry数据段的首地址。
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            // 从ziplist数据段首地址往后找，根据field查询对应的key位置。
            fptr = ziplistFind(zl, fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                // 找到了，则删除ziplist中连续的2个entry，即删除了k、v。
                // 可以使用ziplistDeleteRange一起删除，但这个range删除函数需要的参数是index，需要额外获取。
                zl = ziplistDelete(zl,&fptr); /* Delete the key. */
                zl = ziplistDelete(zl,&fptr); /* Delete the value. */
                o->ptr = zl;
                deleted = 1;
            }
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        // hash表存储，直接使用dictDelete删除就可以了。
        if (dictDelete((dict*)o->ptr, field) == C_OK) {
            deleted = 1;

            /* Always check if the dictionary needs a resize after a delete. */
            // hash对象中，当我们删除了一个元素时，总是检查底层使用的hash表是否需要缩容。
            if (htNeedsResize(o->ptr)) dictResize(o->ptr);
        }

    } else {
        serverPanic("Unknown hash encoding");
    }
    return deleted;
}

/* Return the number of elements in a hash. */
// 返回hash对象中的总元素数
unsigned long hashTypeLength(const robj *o) {
    unsigned long length = ULONG_MAX;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        // ziplist存储，则总元素数 = ziplist总entry数 / 2。
        length = ziplistLen(o->ptr) / 2;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        // hash表存储，则总元素数即为底层hash表的总元素数，使用dictSize获得。
        length = dictSize((const dict*)o->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return length;
}

// 初始化hash对象的迭代器，底层使用的是ziplist或dict迭代器。
hashTypeIterator *hashTypeInitIterator(robj *subject) {
    hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));
    hi->subject = subject;
    hi->encoding = subject->encoding;

    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        // ziplist编码，这里指向field/value的指针初始化为NULL。
        hi->fptr = NULL;
        hi->vptr = NULL;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        // hash表存储，这里直接初始化dict迭代器
        hi->di = dictGetIterator(subject->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return hi;
}

// 释放hash对象的迭代器
void hashTypeReleaseIterator(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(hi->di);
    zfree(hi);
}

/* Move to the next entry in the hash. Return C_OK when the next entry
 * could be found and C_ERR when the iterator reaches the end. */
// 迭代器跳到hash中下一个entry，迭代器中一些属性包含了entry的数据。
// 如果能获取下一个entry，则函数返回0；否则迭代结束，返回-1。
int hashTypeNext(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        // 如果编码是ziplist，则我们需要获取指向key/value指针，赋值给hi->fptr和hi->vptr。
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        if (fptr == NULL) {
            /* Initialize cursor */
            // 如果迭代器中hi->fptr为NULL，显然我们是首次使用跌打器，则调用ziplistIndex(zl, 0)获取ziplist中head元素的地址。
            // 获取到的元素即为本次迭代获取的key。后面我们需要调用ziplistNext来获取挨着的下一个entry作为value。
            serverAssert(vptr == NULL);
            fptr = ziplistIndex(zl, 0);
        } else {
            /* Advance cursor */
            // 如果不是第一次调用，则直接使用ziplistNext，获取下一个entry作为key。
            serverAssert(vptr != NULL);
            fptr = ziplistNext(zl, vptr);
        }
        // 如果获取下一个key时，发现迭代结束了，获取到指针为NULL，则直接返回。
        if (fptr == NULL) return C_ERR;

        /* Grab pointer to the value (fptr points to the field) */
        // 前面获取到了key，这里调用ziplistNext获取后面的entry作为value。（k-v在ziplist中是连续存储的）
        vptr = ziplistNext(zl, fptr);
        serverAssert(vptr != NULL);

        /* fptr, vptr now point to the first or next pair */
        // 将迭代器中两个指针指向真正的key、value数据，表示当前迭代到的hash元素。
        hi->fptr = fptr;
        hi->vptr = vptr;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        // hash表存储，直接调用dictNext获取下一个entry，赋值给hi->de。
        if ((hi->de = dictNext(hi->di)) == NULL) return C_ERR;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return C_OK;
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromZiplist`. */
// 从迭代器指针指向的ziplist元素中，解析出具体的数据。
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what,
                                unsigned char **vstr,
                                unsigned int *vlen,
                                long long *vll)
{
    int ret;

    serverAssert(hi->encoding == OBJ_ENCODING_ZIPLIST);

    // 使用ziplistGet获取ziplist指针位置的entry数据。fptr中获取的是key，vptr中获取的是value
    if (what & OBJ_HASH_KEY) {
        ret = ziplistGet(hi->fptr, vstr, vlen, vll);
        serverAssert(ret);
    } else {
        ret = ziplistGet(hi->vptr, vstr, vlen, vll);
        serverAssert(ret);
    }
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a hash table. Prototype is similar to
 * `hashTypeGetFromHashTable`. */
// 从迭代器中的hash entry中获取迭代到的key、value值，直接调用dictGetKey和dictGetVal获取。
sds hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what) {
    serverAssert(hi->encoding == OBJ_ENCODING_HT);

    if (what & OBJ_HASH_KEY) {
        return dictGetKey(hi->de);
    } else {
        return dictGetVal(hi->de);
    }
}

/* Higher level function of hashTypeCurrent*() that returns the hash value
 * at current iterator position.
 *
 * The returned element is returned by reference in either *vstr and *vlen if
 * it's returned in string form, or stored in *vll if it's returned as
 * a number.
 *
 * If *vll is populated *vstr is set to NULL, so the caller
 * can always check the function return by checking the return value
 * type checking if vstr == NULL. */
// 高级别的hashTypeCurrent*()函数，用于返回当前迭代位置的hash数据。
// 如果数据是字符串则在*vstr和vlen返回；如果数据是数字则在vll中返回且*vstr为NULL。调用方根据*vstr是否为空来确定是否是字符串。
void hashTypeCurrentObject(hashTypeIterator *hi, int what, unsigned char **vstr, unsigned int *vlen, long long *vll) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        *vstr = NULL;
        // 因为迭代器里只有指向key、value的指针，我们需要解析这两个指针位置的ziplist结构的数据。
        hashTypeCurrentFromZiplist(hi, what, vstr, vlen, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        // 迭代器存储的是hash entry，我们直接获取entry的key和value就可以了。
        sds ele = hashTypeCurrentFromHashTable(hi, what);
        *vstr = (unsigned char*) ele;
        *vlen = sdslen(ele);
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* Return the key or value at the current iterator position as a new
 * SDS string. */
// 使用当前hash迭代到的k-v数据，构造新的sds返回。
// 因为有可能我们只需要key或value中的一个，所以这里并不是一起都取出来，而是根据传入参数what来指定取key还是value。
sds hashTypeCurrentObjectNewSds(hashTypeIterator *hi, int what) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    // 获取当前迭代的数据，根据what参数区分是key 还是 value。
    hashTypeCurrentObject(hi,what,&vstr,&vlen,&vll);
    // 如果是字符串，则直接用它构造新的sds字符串返回
    if (vstr) return sdsnewlen(vstr,vlen);
    // 如果是数字，则转成sds字符串返回。
    return sdsfromlonglong(vll);
}

// 查询hash对象for write。如果不存在，则创建一个空的hash对象写入。
robj *hashTypeLookupWriteOrCreate(client *c, robj *key) {
    // 查询hash对象，并检查对象类型是否是HASH
    robj *o = lookupKeyWrite(c->db,key);
    if (checkType(c,o,OBJ_HASH)) return NULL;

    if (o == NULL) {
        // 如果指定key的hash对象不存在，因为我们是要往该hash对象写入数据，所以这里创建一个hash对象加入db中。
        o = createHashObject();
        dbAdd(c->db,key,o);
    }
    return o;
}

void hashTypeConvertZiplist(robj *o, int enc) {
    serverAssert(o->encoding == OBJ_ENCODING_ZIPLIST);

    if (enc == OBJ_ENCODING_ZIPLIST) {
        /* Nothing to do... */

    } else if (enc == OBJ_ENCODING_HT) {
        hashTypeIterator *hi;
        dict *dict;
        int ret;

        // 初始化hash类型的迭代器
        hi = hashTypeInitIterator(o);
        // 创建hash类型的dict
        dict = dictCreate(&hashDictType, NULL);

        // 遍历迭代hash元素处理，直到所有元素迭代完成。
        while (hashTypeNext(hi) != C_ERR) {
            sds key, value;

            // 分别取出迭代器迭代到的key和value，然后加入到前面创建的dict中。
            key = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
            value = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            ret = dictAdd(dict, key, value);
            if (ret != DICT_OK) {
                // 如果发现ziplist中存储的hash有相同的key，则panic。
                serverLogHexDump(LL_WARNING,"ziplist with dup elements dump",
                    o->ptr,ziplistBlobLen(o->ptr));
                serverPanic("Ziplist corruption detected");
            }
        }
        // 迭代元素并加入新dict完成，释放迭代器和hash对象原底层存储结构。。
        hashTypeReleaseIterator(hi);
        zfree(o->ptr);
        // 新的存储使用新构建的dict，并更新编码的hash表。
        o->encoding = OBJ_ENCODING_HT;
        o->ptr = dict;
    } else {
        serverPanic("Unknown hash encoding");
    }
}

// 用于将hash对象的ziplist存储转换成hash表存储。
void hashTypeConvert(robj *o, int enc) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        hashTypeConvertZiplist(o, enc);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        serverPanic("Not implemented");
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* This is a helper function for the COPY command.
 * Duplicate a hash object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * The resulting object always has refcount set to 1 */
// COPY命令的辅助函数，复制一个hash对象，保证复制返回的对象与原对象有相同的编码类型。复制对象的refcount总是设置为1，要注意清理。
robj *hashTypeDup(robj *o) {
    robj *hobj;
    hashTypeIterator *hi;

    serverAssert(o->type == OBJ_HASH);

    if(o->encoding == OBJ_ENCODING_ZIPLIST){
        // ziplist编码，获取原ziplist的总字节数，分配足够的空间，使用memcpy直接复制数据得到新的ziplist。
        unsigned char *zl = o->ptr;
        size_t sz = ziplistBlobLen(zl);
        unsigned char *new_zl = zmalloc(sz);
        memcpy(new_zl, zl, sz);
        // 基于新的ziplist创建新的hash对象，新hash对象编码方式为ziplist
        hobj = createObject(OBJ_HASH, new_zl);
        hobj->encoding = OBJ_ENCODING_ZIPLIST;
    } else if(o->encoding == OBJ_ENCODING_HT){
        // 底层hash表存储，这里创建新的hash类型的dict，size与原对象的hash表一致。
        dict *d = dictCreate(&hashDictType, NULL);
        dictExpand(d, dictSize((const dict*)o->ptr));

        // 使用迭代器遍历原hash对象，copy对象中的每一项加入到新的dict中。
        hi = hashTypeInitIterator(o);
        while (hashTypeNext(hi) != C_ERR) {
            sds field, value;
            sds newfield, newvalue;
            /* Extract a field-value pair from an original hash object.*/
            // 获取原hash对象的field-value对。
            field = hashTypeCurrentFromHashTable(hi, OBJ_HASH_KEY);
            value = hashTypeCurrentFromHashTable(hi, OBJ_HASH_VALUE);
            // copy构建新的field-value对。
            newfield = sdsdup(field);
            newvalue = sdsdup(value);

            /* Add a field-value pair to a new hash object. */
            // 将新的field-value加入到新的dict中。
            dictAdd(d,newfield,newvalue);
        }
        hashTypeReleaseIterator(hi);

        // 基于新的dict创建新的hash对象，底层编码方式是hash。
        hobj = createObject(OBJ_HASH, d);
        hobj->encoding = OBJ_ENCODING_HT;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return hobj;
}

/* callback for to check the ziplist doesn't have duplicate recoreds */
// 检查ziplist结构完整性时使用的回调函数，用于判断是否有重复元素（hash对象不能有重复key）。函数返回1表示结构正常且没有重复field。
static int _hashZiplistEntryValidation(unsigned char *p, void *userdata) {
    struct {
        long count;
        dict *fields;
    } *data = userdata;

    /* Odd records are field names, add to dict and check that's not a dup */
    // 偶数索引位置的元素是field名。加入到fields字典中，用于检查是否重复。
    if (((data->count) & 1) == 0) {
        unsigned char *str;
        unsigned int slen;
        long long vll;
        // 获取field字段数据
        if (!ziplistGet(p, &str, &slen, &vll))
            return 0;
        sds field = str? sdsnewlen(str, slen): sdsfromlonglong(vll);
        // 检查是否重复，即fields集合中是否有当前遍历到的field，有则重复了，返回0。
        if (dictAdd(data->fields, field, NULL) != DICT_OK) {
            /* Duplicate, return an error */
            sdsfree(field);
            return 0;
        }
    }

    // count记录总的ziplist中entry数量，我们后面会检查这个值应该为偶数。
    (data->count)++;
    return 1;
}

/* Validate the integrity of the data structure.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we scan all the entries one by one. */
// 验证ziplist编码的hash结构完整性。deep为0，表示我们值验证ziplist的header部分；为1表示遍历所有的entries检查。
int hashZiplistValidateIntegrity(unsigned char *zl, size_t size, int deep) {
    // deep为0，只验证ziplist的header
    if (!deep)
        return ziplistValidateIntegrity(zl, size, 0, NULL, NULL);

    /* Keep track of the field names to locate duplicate ones */
    // 这个构造一个结构用来追踪遍历到的hash fields，从而判断是否有重复field。
    struct {
        // 总的k-v数量，该值应该为偶数。
        long count;
        // fields的集合，用于检查是否重复
        dict *fields;
    } data = {0, dictCreate(&hashDictType, NULL)};

    // 不止验证header，还验证每个entry结构。同时遍历时会调用_hashZiplistEntryValidation来检测field是否重复，data为传入该函数的参数。
    int ret = ziplistValidateIntegrity(zl, size, 1, _hashZiplistEntryValidation, &data);

    /* make sure we have an even number of records. */
    // ziplist中我们获取的总元素数应该是偶数，因为hash是k-v结构的。
    if (data.count & 1)
        ret = 0;

    dictRelease(data.fields);
    return ret;
}

/* Create a new sds string from the ziplist entry. */
// 根据ziplist entry数据创建一个新的sds字符串。
sds hashSdsFromZiplistEntry(ziplistEntry *e) {
    return e->sval ? sdsnewlen(e->sval, e->slen) : sdsfromlonglong(e->lval);
}

/* Reply with bulk string from the ziplist entry. */
// 将ziplist entry处理成一个bulk字符串加入到client回复中。根据entry中存储的数据是字符串还是数字，有不同的处理方式。
void hashReplyFromZiplistEntry(client *c, ziplistEntry *e) {
    if (e->sval)
        addReplyBulkCBuffer(c, e->sval, e->slen);
    else
        addReplyBulkLongLong(c, e->lval);
}

/* Return random element from a non empty hash.
 * 'key' and 'val' will be set to hold the element.
 * The memory in them is not to be freed or modified by the caller.
 * 'val' can be NULL in which case it's not extracted. */
// 从一个非空的hash对象中随机返回一个元素。参数key、val（如果不需要，val可以传NULL）用存储返回的数据，调用者不会释放或修改其中的数据。
void hashTypeRandomElement(robj *hashobj, unsigned long hashsize, ziplistEntry *key, ziplistEntry *val) {
    if (hashobj->encoding == OBJ_ENCODING_HT) {
        // 底层hash表存储，使用dictGetFairRandomKey随机获取数据。
        dictEntry *de = dictGetFairRandomKey(hashobj->ptr);
        sds s = dictGetKey(de);
        // 数据填充到key、val中。
        key->sval = (unsigned char*)s;
        key->slen = sdslen(s);
        if (val) {
            sds s = dictGetVal(de);
            val->sval = (unsigned char*)s;
            val->slen = sdslen(s);
        }
    } else if (hashobj->encoding == OBJ_ENCODING_ZIPLIST) {
        // ziplist编码，直接使用ziplistRandomPair就可以获取key、val的ziplistEntry数据。
        ziplistRandomPair(hashobj->ptr, hashsize, key, val);
    } else {
        serverPanic("Unknown hash encoding");
    }
}


/*-----------------------------------------------------------------------------
 * Hash type commands
 *----------------------------------------------------------------------------*/

// hsetnx obj field value，不存在field才设置。
void hsetnxCommand(client *c) {
    robj *o;
    // 获取hash对象，如果不存在该obj则创建新的并加入到db中。
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // 如果当前是ziplist编码，且设置的field或value有超过64字节(默认大小，可自行设置)，则会转为hash表存储。
    hashTypeTryConversion(o,c->argv,2,3);

    // 检查field是否存储，存在则返回0，表示没有写入。
    if (hashTypeExists(o, c->argv[2]->ptr)) {
        addReply(c, shared.czero);
    } else {
        // 不存在我们需要将field/value加入到该hash对象中，并回复1，表示插入1条数据成功。
        hashTypeSet(o,c->argv[2]->ptr,c->argv[3]->ptr,HASH_SET_COPY);
        addReply(c, shared.cone);
        // 处理key变更通知
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
        server.dirty++;
    }
}

// hset/hmset obj field1 value1 [field2 value2]，插入或更新field/value，可以同时写入多个field值。
void hsetCommand(client *c) {
    int i, created = 0;
    robj *o;

    // 参数应该是偶数个
    if ((c->argc % 2) == 1) {
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",c->cmd->name);
        return;
    }

    // 查询或创建hash对象，如果需要则将对象的ziplist的编码变更为hash表存储。
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    hashTypeTryConversion(o,c->argv,2,c->argc-1);

    for (i = 2; i < c->argc; i += 2)
        // 遍历参数，set数据。有可能是更新field的值，也有可能是插入新field。我们根据函数返回值判断，返回0表示插入，1表示更新。
        created += !hashTypeSet(o,c->argv[i]->ptr,c->argv[i+1]->ptr,HASH_SET_COPY);

    /* HMSET (deprecated) and HSET return value is different. */
    // HMSET 和 HSET 返回值是不同的，HMSET返回ok表示成功，而HSET会返回插入的条数。HMSET已经废弃，使用HSET替代。
    char *cmdname = c->argv[0]->ptr;
    if (cmdname[1] == 's' || cmdname[1] == 'S') {
        /* HSET */
        addReplyLongLong(c, created);
    } else {
        /* HMSET */
        addReply(c, shared.ok);
    }
    // 处理key变更通知
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    server.dirty += (c->argc - 2)/2;
}

// hincrby obj field increment
void hincrbyCommand(client *c) {
    long long value, incr, oldvalue;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;

    // 获取incr参数
    if (getLongLongFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    // 获取或创建hash对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // 根据field来获取value。
    if (hashTypeGetValue(o,c->argv[2]->ptr,&vstr,&vlen,&value) == C_OK) {
        if (vstr) {
            // 如果是字符串编码，我们需要转成数字放到value中。而如果本来就是数字，那么hashTypeGetValue已经处理存储在了value中。
            if (string2ll((char*)vstr,vlen,&value) == 0) {
                addReplyError(c,"hash value is not an integer");
                return;
            }
        } /* Else hashTypeGetValue() already stored it into &value */
    } else {
        // 没有查询到field，则该value默认为0，后面我们incr后，会插入该field/value到hash对象中。
        value = 0;
    }

    oldvalue = value;
    // 判断incr后是否会溢出。
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    // 处理incr操作，并重新将数字编码，然后field/value插入或更新到hash对象中。
    value += incr;
    new = sdsfromlonglong(value);
    hashTypeSet(o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE);
    // 返回incr后的新值
    addReplyLongLong(c,value);
    // 处理key变更通知
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrby",c->argv[1],c->db->id);
    server.dirty++;
}

// hincrbyfloat obj field increment
void hincrbyfloatCommand(client *c) {
    long double value, incr;
    long long ll;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;

    // 获取浮点incr参数
    if (getLongDoubleFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    // 获取或创建hash对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    // 根据field来获取value
    if (hashTypeGetValue(o,c->argv[2]->ptr,&vstr,&vlen,&ll) == C_OK) {
        if (vstr) {
            // 如果数字编码为字符串，则转为浮点数。
            if (string2ld((char*)vstr,vlen,&value) == 0) {
                addReplyError(c,"hash value is not a float");
                return;
            }
        } else {
            // 整数需要强转为浮点型
            value = (long double)ll;
        }
    } else {
        value = 0;
    }

    // 处理incr操作，并检查是否溢出
    value += incr;
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }

    // 浮点数，我们需要转成字符串存储
    char buf[MAX_LONG_DOUBLE_CHARS];
    int len = ld2string(buf,sizeof(buf),value,LD_STR_HUMAN);
    new = sdsnewlen(buf,len);
    // 新的field/value更新或插入
    hashTypeSet(o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE);
    // 浮点数字符串格式回复。
    addReplyBulkCBuffer(c,buf,len);
    // 处理key变更通知
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrbyfloat",c->argv[1],c->db->id);
    server.dirty++;

    /* Always replicate HINCRBYFLOAT as an HSET command with the final value
     * in order to make sure that differences in float precision or formatting
     * will not create differences in replicas or after an AOF restart. */
    // 我们总是将 HINCRBYFLOAT 命令转为 HSET value 命令来进行传播，避免不同的浮点数精度和格式而导致数据不一致。
    robj *newobj;
    newobj = createRawStringObject(buf,len);
    rewriteClientCommandArgument(c,0,shared.hset);
    rewriteClientCommandArgument(c,3,newobj);
    decrRefCount(newobj);
}

// 在给定的hash对象中，根据field查询value回复给client。
static void addHashFieldToReply(client *c, robj *o, sds field) {
    int ret;

    // hash对象为NULL，则直接回复null。
    if (o == NULL) {
        addReplyNull(c);
        return;
    }

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        // ziplist编码查询field
        ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
        if (ret < 0) {
            // 返回-1，表示没查到field，回复null。
            addReplyNull(c);
        } else {
            // 查到了，需要根据vstr是否为NULL来判断value是字符串还是数字，从而不同的方式处理回复。
            if (vstr) {
                addReplyBulkCBuffer(c, vstr, vlen);
            } else {
                addReplyBulkLongLong(c, vll);
            }
        }

    } else if (o->encoding == OBJ_ENCODING_HT) {
        // hash表存储，根据field获取entry，进而获取value。没查到回复null，查到了value本来就是字符串，直接处理回复。
        sds value = hashTypeGetFromHashTable(o, field);
        if (value == NULL)
            addReplyNull(c);
        else
            addReplyBulkCBuffer(c, value, sdslen(value));
    } else {
        serverPanic("Unknown hash encoding");
    }
}

// hget obj field
void hgetCommand(client *c) {
    robj *o;

    // 查询hash对象for read，如果对象不存在则直接返回null，而不会跟set一样创建。查到了这里还需要check对象类型确保是hash。
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    // 在hash对象o中根据field查询value返回。
    addHashFieldToReply(c, o, c->argv[2]->ptr);
}

// hmget obj field1 [field2]
void hmgetCommand(client *c) {
    robj *o;
    int i;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * hashes, where HMGET should respond with a series of null bulks. */
    // hget没查到obj，我们可以直接返回null，因为只查一个field，所以obj不存在和field不存在结果是一样的。
    // 这里hmget，因为查询多个fields，我们需要返回一个列表，当obj不存在，需要返回列表中所有元素为null，而不是空列表。
    // 所以这里不直接返回而是当作空hash处理。
    o = lookupKeyRead(c->db, c->argv[1]);
    if (checkType(c,o,OBJ_HASH)) return;

    // 先回复数组长度
    addReplyArrayLen(c, c->argc-2);
    for (i = 2; i < c->argc; i++) {
        // 挨个处理查询field，回复查询到的value。
        addHashFieldToReply(c, o, c->argv[i]->ptr);
    }
}

// hdel obj field1 [field2]
void hdelCommand(client *c) {
    robj *o;
    int j, deleted = 0, keyremoved = 0;

    // 查询hash对象，不存在则返回0。检查查到对象的类型确保是hash。
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    for (j = 2; j < c->argc; j++) {
        // 遍历参数fields，挨个调用hashTypeDelete处理删除field。该函数成功删除返回1，没找到返回0。
        if (hashTypeDelete(o,c->argv[j]->ptr)) {
            // 成功删除会返回1，则进入到if中，我们统计删除的field数，并检查是否可以删除该hash对象（没有fields了就可以删除）。
            deleted++;
            if (hashTypeLength(o) == 0) {
                // 当hash对象中的field为0时，我们从db中删除该对象。
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }
    if (deleted) {
        // 如果有删除fields，改变了key（或删除了key），我们要处理key变更通知
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_HASH,"hdel",c->argv[1],c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        server.dirty += deleted;
    }
    // 返回client删除fields数量
    addReplyLongLong(c,deleted);
}

// hlen obj，获取hash对象中fields总数
void hlenCommand(client *c) {
    robj *o;

    // 查询hash对象，并检查对象类型确保是hash
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    // 返回对象总fields数，使用hashTypeLength获取。
    addReplyLongLong(c,hashTypeLength(o));
}

// hstrlen obj field，获取对象中指定field的值的长度。
void hstrlenCommand(client *c) {
    robj *o;

    // 查询hash对象，并检查对象类型确保是hash
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;
    // 使用hashTypeGetValueLength，获取hash对象中field关联的value的长度。
    addReplyLongLong(c,hashTypeGetValueLength(o,c->argv[2]->ptr));
}

// 获取hash对象当前迭代器指示的entry数据返回，根据what参数决定是返回field还是value值。
static void addHashIteratorCursorToReply(client *c, hashTypeIterator *hi, int what) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        // ziplist编码，调用hashTypeCurrentFromZiplist获取迭代器两个指针指向的ziplist中entry数据，根据返回的数据编码类型来处理回复。
        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr)
            addReplyBulkCBuffer(c, vstr, vlen);
        else
            addReplyBulkLongLong(c, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        // hash表存储，使用hashTypeCurrentFromHashTable来获取，因为key/value都是字符串，所以直接字符串类型回复。
        sds value = hashTypeCurrentFromHashTable(hi, what);
        addReplyBulkCBuffer(c, value, sdslen(value));
    } else {
        serverPanic("Unknown hash encoding");
    }
}

// hkeys、hvals、hgetall命令的统一处理方法。
void genericHgetallCommand(client *c, int flags) {
    robj *o;
    hashTypeIterator *hi;
    int length, count = 0;

    // 如果是hkeys或hvals，我们返回数据是一个数组；而hgetall会返回一个map。这里确定他们对应的空值。
    robj *emptyResp = (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) ?
        shared.emptymap[c->resp] : shared.emptyarray;
    // 查询hash对象 并 检查对象类型，如果没查到返回前面的空值（根据指令不同，可能返回数组或map）。
    if ((o = lookupKeyReadOrReply(c,c->argv[1],emptyResp))
        == NULL || checkType(c,o,OBJ_HASH)) return;

    /* We return a map if the user requested keys and values, like in the
     * HGETALL case. Otherwise to use a flat array makes more sense. */
    // 处理结果长度的返回，不同的命令也有不同的返回格式。
    // 如果是hgetall，需要返回所有的k-v，RESP3有map格式，而RESP2是数组返回的，数组包含k-v，所以应该是两倍的总元素数量。
    length = hashTypeLength(o);
    if (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) {
        addReplyMapLen(c, length);
    } else {
        addReplyArrayLen(c, length);
    }

    hi = hashTypeInitIterator(o);
    // 使用迭代器遍历hash对象。
    while (hashTypeNext(hi) != C_ERR) {
        // 每次迭代到一个元素都使用addHashIteratorCursorToReply来获取迭代器指向的key和value返回给client。
        if (flags & OBJ_HASH_KEY) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_KEY);
            count++;
        }
        if (flags & OBJ_HASH_VALUE) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_VALUE);
            count++;
        }
    }

    hashTypeReleaseIterator(hi);

    /* Make sure we returned the right number of elements. */
    // 检查确保我们返回的总元素数与对象的hash len一致。
    if (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) count /= 2;
    serverAssert(count == length);
}

void hkeysCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY);
}

void hvalsCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_VALUE);
}

void hgetallCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY|OBJ_HASH_VALUE);
}

// hexists obj field
void hexistsCommand(client *c) {
    robj *o;
    // 查询hash对象，并检查对象的类型。
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    // 使用hashTypeExists检查指定的field是否存在，结果回复给client。
    addReply(c, hashTypeExists(o,c->argv[2]->ptr) ? shared.cone : shared.czero);
}

// hscan obj cursor [MATCH pattern] [COUNT count]
void hscanCommand(client *c) {
    robj *o;
    unsigned long cursor;

    // 解析cursor参数
    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    // 获取hash对象，并检查对象类型
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;
    // 调用scanGenericCommand来处理hash对象的fields scan。
    scanGenericCommand(c,o,cursor);
}

// 处理随机获取到的多个keys/values ziplistEntry回复给client。
static void hrandfieldReplyWithZiplist(client *c, unsigned int count, ziplistEntry *keys, ziplistEntry *vals) {
    // count个entrys，挨个遍历处理
    for (unsigned long i = 0; i < count; i++) {
        // 如果是RESP3协议，且需要返回vals，每次循环处理返回的元素实际上是一个 k、v 2个元素的数组。这里先写入长度2。
        if (vals && c->resp > 2)
            addReplyArrayLen(c,2);
        // 写入key值，根据字符串或数字类型来处理。
        if (keys[i].sval)
            addReplyBulkCBuffer(c, keys[i].sval, keys[i].slen);
        else
            addReplyBulkLongLong(c, keys[i].lval);
        // 如果需要返回vals，则处理vals回复。
        if (vals) {
            if (vals[i].sval)
                addReplyBulkCBuffer(c, vals[i].sval, vals[i].slen);
            else
                addReplyBulkLongLong(c, vals[i].lval);
        }
    }
}

/* How many times bigger should be the hash compared to the requested size
 * for us to not use the "remove elements" strategy? Read later in the
 * implementation for more info. */
// 当 hash对象中fields元素数量 比 请求随机获取元素数量 大3倍以上时，我们不使用"移除元素"策略。
#define HRANDFIELD_SUB_STRATEGY_MUL 3

/* If client is trying to ask for a very large number of random elements,
 * queuing may consume an unlimited amount of memory, so we want to limit
 * the number of randoms per time. */
// 如果client尝试请求很多的随机fields元素，一次性全部处理可能会消耗很多的内存，所以我们分批处理，每次随机处理1000个元素获取。
#define HRANDFIELD_RANDOM_SAMPLE_LIMIT 1000

void hrandfieldWithCountCommand(client *c, long l, int withvalues) {
    unsigned long count, size;
    int uniq = 1;
    robj *hash;

    // 获取hash对象，并检查对象的类型确保是hash。
    if ((hash = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]))
        == NULL || checkType(c,hash,OBJ_HASH)) return;
    size = hashTypeLength(hash);

    // 处理需要查询的count数量。传入l为负时，表示随机查询数据可以重复。
    if(l >= 0) {
        count = (unsigned long) l;
    } else {
        count = -l;
        uniq = 0;
    }

    /* If count is zero, serve it ASAP to avoid special cases later. */
    // 如果count为0，尽早返回，避免继续执行后面的特殊情况。
    if (count == 0) {
        addReply(c,shared.emptyarray);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. This case is the only one that also needs to return the
     * elements in random order. */
    // 情形1：count为负数表示数据可以重复，处理方式是每次对整个集合采样返回N个随机元素。
    // 这种情况很容易处理，不需要借助其他辅助数据结构来实现。这种情况也是唯一需要以随机顺序返回元素的情况。
    if (!uniq || count == 1) {
        // 如果是RESP2协议（数组返回），且需要返回values的话，我们返回数组的长度是count*2（field、value各算一个数组元素）。
        // 如果是RESP3协议，我们返回的外层数组长度是count。但如果要返回value的话，数组元素不再是单纯字符串了，而是k、v两个值组成的数组。
        if (withvalues && c->resp == 2)
            addReplyArrayLen(c, count*2);
        else
            addReplyArrayLen(c, count);
        if (hash->encoding == OBJ_ENCODING_HT) {
            // hash表存储，因为可以重复，所以直接调用count次 dictGetFairRandomKey 来随机获取数据就可以了。
            sds key, value;
            while (count--) {
                // 遍历count次，随机获取dict中entry。
                dictEntry *de = dictGetFairRandomKey(hash->ptr);
                key = dictGetKey(de);
                value = dictGetVal(de);
                // 如果是RESP3 且 需要返回value的话，我们返回的每个数组元素也都是 k、v 2个元素的数组，这里先写入内层数组的长度2。
                if (withvalues && c->resp > 2)
                    addReplyArrayLen(c,2);
                // 添加field回复，如果需要返回value，则再添加value回复。
                addReplyBulkCBuffer(c, key, sdslen(key));
                if (withvalues)
                    addReplyBulkCBuffer(c, value, sdslen(value));
            }
        } else if (hash->encoding == OBJ_ENCODING_ZIPLIST) {
            // ziplist存储，调用ziplistRandomPairs来获取随机k、v。
            // 该底层函数一次性可以获取多个随机的k、v对，为了避免一次处理太多，额外分配存储浪费过多空间，这里分批进行处理。
            ziplistEntry *keys, *vals = NULL;
            unsigned long limit, sample_count;
            // 确定一次最多随机获取多少个元素。
            limit = count > HRANDFIELD_RANDOM_SAMPLE_LIMIT ? HRANDFIELD_RANDOM_SAMPLE_LIMIT : count;
            // 分批连续的空间，用于ziplistRandomPairs返回随机获取的数据。
            keys = zmalloc(sizeof(ziplistEntry)*limit);
            if (withvalues)
                vals = zmalloc(sizeof(ziplistEntry)*limit);
            while (count) {
                // 遍历，每次随机获取limit个元素，并处理他们回复给client
                sample_count = count > limit ? limit : count;
                count -= sample_count;
                // ziplist一次性遍历获取sample_count个随机元素，可重复。详细看ziplistRandomPairs实现，
                ziplistRandomPairs(hash->ptr, sample_count, keys, vals);
                hrandfieldReplyWithZiplist(c, sample_count, keys, vals);
            }
            zfree(keys);
            zfree(vals);
        }
        return;
    }

    /* Initiate reply count, RESP3 responds with nested array, RESP2 with flat one. */
    // 前面处理了可重复的情况，比较简单。后面需要借助额外的数据结构来获取不重复的返回。
    // 先初始化返回的总数据的count，如果count>=size，显然不重复的话，最多也只能返回size个元素。
    // 另外对于RESP2，我们返回一个大的数组，key和value都是它的元素，所以是2倍的长度；而对于RESP3，我们返回嵌套的数组，长度与返回数据一致。
    long reply_size = count < size ? count : size;
    if (withvalues && c->resp == 2)
        addReplyArrayLen(c, reply_size*2);
    else
        addReplyArrayLen(c, reply_size);

    /* CASE 2:
    * The number of requested elements is greater than the number of
    * elements inside the hash: simply return the whole hash. */
    // 情形2：请求返回的count大于等我们hash对象总的field数，则我们简单的将对象里所有的元素都返回。
    if(count >= size) {
        hashTypeIterator *hi = hashTypeInitIterator(hash);
        // 迭代遍历整个hash对象的数据，并全部返回。
        while (hashTypeNext(hi) != C_ERR) {
            // RESP3，内层也是数组，这里先写入长度2。
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c,2);
            // 回复当前遍历到的key，如果需要返回value的话，也处理回复value。
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_KEY);
            if (withvalues)
                addHashIteratorCursorToReply(c, hi, OBJ_HASH_VALUE);
        }
        hashTypeReleaseIterator(hi);
        return;
    }

    /* CASE 3:
     * The number of elements inside the hash is not greater than
     * HRANDFIELD_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a hash from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requested elements is just
     * a bit less than the number of elements in the hash, the natural approach
     * used into CASE 4 is highly inefficient. */
    // 情形3：hash对象元素数<3*count时，我们基于对象复制一个新的dict，然后随机从dict里移除元素，从而剩余count个元素时即为我们的返回数据。
    // 这样做是因为如果请求的count 比 hash中总元素个数相差不大时，使用case4中的方法效率会很低。
    if (count*HRANDFIELD_SUB_STRATEGY_MUL > size) {
        // 创建新的dict用于copy hash对象的数据。
        dict *d = dictCreate(&sdsReplyDictType, NULL);
        dictExpand(d, size);
        hashTypeIterator *hi = hashTypeInitIterator(hash);

        /* Add all the elements into the temporary dictionary. */
        // 迭代遍历对象的所有entry，并将他们都加入到我们新创建的dict中
        while ((hashTypeNext(hi)) != C_ERR) {
            int ret = DICT_ERR;
            sds key, value = NULL;

            // 基于遍历到的entry数据，构建新的k、v字符串，并添加到新dict中
            key = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
            if (withvalues)
                value = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            ret = dictAdd(d, key, value);

            serverAssert(ret == DICT_OK);
        }
        serverAssert(dictSize(d) == size);
        hashTypeReleaseIterator(hi);

        /* Remove random elements to reach the right count. */
        // 随机从新dict中移除元素，直到dict中只剩下count个元素为止，此时dict元素即为我们随机获取到的数据。
        while (size > count) {
            dictEntry *de;
            // 随机选择一个entry，从新dict中移除掉。
            de = dictGetRandomKey(d);
            dictUnlink(d,dictGetKey(de));
            // 清理释放空间
            sdsfree(dictGetKey(de));
            sdsfree(dictGetVal(de));
            dictFreeUnlinkedEntry(d,de);
            size--;
        }

        /* Reply with what's in the dict and release memory */
        dictIterator *di;
        dictEntry *de;
        di = dictGetIterator(d);
        // 遍历dict，处理数据回复给client，完成后释放dict。
        while ((de = dictNext(di)) != NULL) {
            // 获取key、value
            sds key = dictGetKey(de);
            sds value = dictGetVal(de);
            // RESP3，且需要返回value，元素是数组，这里先写入长度。
            if (withvalues && c->resp > 2)
                addReplyArrayLen(c,2);
            // 回复key、value
            addReplyBulkSds(c, key);
            if (withvalues)
                addReplyBulkSds(c, value);
        }

        dictReleaseIterator(di);
        dictRelease(d);
    }

    /* CASE 4: We have a big hash compared to the requested number of elements.
     * In this case we can simply get random elements from the hash and add
     * to the temporary hash, trying to eventually get enough unique elements
     * to reach the specified count. */
    // 情形4：相对于请求的count来说，我们的hash对象很大（包含很多fields）。
    // 此时我们可以简单的获取hash数据，添加到临时hash表中去重，当临时hash表数据量达到count时，返回。
    else {
        if (hash->encoding == OBJ_ENCODING_ZIPLIST) {
            /* it is inefficient to repeatedly pick one random element from a
             * ziplist. so we use this instead: */
            // 对于ziplsit，每次从中随机获取元素效率很低，这里我们避免多次去获取，而是一次遍历获取所有随机数据。
            ziplistEntry *keys, *vals = NULL;
            // 分配连续空间的keys、values用于返回取到的随机数据。
            keys = zmalloc(sizeof(ziplistEntry)*count);
            if (withvalues)
                vals = zmalloc(sizeof(ziplistEntry)*count);
            // ziplistRandomPairsUnique随机获取count个元素，不会重复。具体使用的基于流式的随机取数算法见函数实现。
            serverAssert(ziplistRandomPairsUnique(hash->ptr, count, keys, vals) == count);
            // 处理ziplist获取到的count个随机元素的返回。
            hrandfieldReplyWithZiplist(c, count, keys, vals);
            zfree(keys);
            zfree(vals);
            return;
        }

        /* Hashtable encoding (generic implementation) */
        // hash表存储，一般的实现，使用辅助dict去重。循环随机取hash对象的元素加入该dict，直到dict元素数量达到count为止。
        unsigned long added = 0;
        ziplistEntry key, value;
        dict *d = dictCreate(&hashDictType, NULL);
        dictExpand(d, count);
        // 当我们随机获取的不重复元素数量 < count时，就一直循环处理。
        while(added < count) {
            // 随机获取元素
            hashTypeRandomElement(hash, size, &key, withvalues? &value : NULL);

            /* Try to add the object to the dictionary. If it already exists
            * free it, otherwise increment the number of objects we have
            * in the result dictionary. */
            // 尝试将元素加入到辅助dict中，如果元素已经存在了，则说明回复过该元素了，continue继续随机取下一个处理。
            sds skey = hashSdsFromZiplistEntry(&key);
            if (dictAdd(d,skey,NULL) != DICT_OK) {
                sdsfree(skey);
                continue;
            }
            // 如果元素不在dict中，则需要处理回复，记录我们当前总的回复元素数量。当回复元素数量等于count时，我们跳出循环，处理完毕。
            added++;

            /* We can reply right away, so that we don't need to store the value in the dict. */
            // 我们这里直接回复数据，这里我们只需要将key加入到辅助dict中用于去重，而不需要管value。
            if (withvalues && c->resp > 2)
                // 需要回复value且是RESP3，嵌套数据模式，这里写入内部数组长度2。
                addReplyArrayLen(c,2);
            // 处理key、value entry回复给client。
            hashReplyFromZiplistEntry(c, &key);
            if (withvalues)
                hashReplyFromZiplistEntry(c, &value);
        }

        /* Release memory */
        dictRelease(d);
    }
}

/* HRANDFIELD obj [<count> WITHVALUES] */
// 随机获取hash对象中的field元素，可以指定获取的count数，也可以指定同时返回value。注意count为负数时，表示可以返回重复field。
void hrandfieldCommand(client *c) {
    long l;
    int withvalues = 0;
    robj *hash;
    ziplistEntry ele;

    if (c->argc >= 3) {
        // 获取count参数
        if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) return;
        // 判断第四个参数是否是 WITHVALUES，如果是则表示要同时返回value
        if (c->argc > 4 || (c->argc == 4 && strcasecmp(c->argv[3]->ptr,"withvalues"))) {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        } else if (c->argc == 4)
            withvalues = 1;
        // 处理随机获取count个field元素，根据withvalues标识确定是否同时返回value。
        hrandfieldWithCountCommand(c, l, withvalues);
        return;
    }

    /* Handle variant without <count> argument. Reply with simple bulk string */
    // 处理没有count参数的情况，此时我们返回的是string值，只返回field，不返回value。
    // 获取hash对象，并检查对象的类型确保是hash。
    if ((hash = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]))== NULL ||
        checkType(c,hash,OBJ_HASH)) {
        return;
    }

    // 在hash对象中随机获取一个field填充到ele entry中，注意这里我们没有返回value。
    hashTypeRandomElement(hash,hashTypeLength(hash),&ele,NULL);
    // 将ele entry处理返回给client
    hashReplyFromZiplistEntry(c, &ele);
}
