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
#include <math.h> /* isnan(), isinf() */

/* Forward declarations */
int getGenericCommand(client *c);

/*-----------------------------------------------------------------------------
 * String Commands
 *----------------------------------------------------------------------------*/

// 检查string值的长度，如果超过了proto_max_bulk_len则回复err。对于master client传播的数据，我们不做检查。
static int checkStringLength(client *c, long long size) {
    if (!(c->flags & CLIENT_MASTER) && size > server.proto_max_bulk_len) {
        addReplyError(c,"string exceeds maximum allowed size (proto-max-bulk-len)");
        return C_ERR;
    }
    return C_OK;
}

/* The setGenericCommand() function implements the SET operation with different
 * options and variants. This function is called in order to implement the
 * following commands: SET, SETEX, PSETEX, SETNX, GETSET.
 *
 * 'flags' changes the behavior of the command (NX, XX or GET, see below).
 *
 * 'expire' represents an expire to set in form of a Redis object as passed
 * by the user. It is interpreted according to the specified 'unit'.
 *
 * 'ok_reply' and 'abort_reply' is what the function will reply to the client
 * if the operation is performed, or when it is not because of NX or
 * XX flags.
 *
 * If ok_reply is NULL "+OK" is used.
 * If abort_reply is NULL, "$-1" is used. */
// setGenericCommand()实现了SET的各种变体操作，如SET, SETEX, PSETEX, SETNX, GETSET。
// flags参数改变命令的行为，具体见下面所有的OBJ_*标识。
// expire参数表示用户传递的该对象的过期时间，根据指定的unit单位来解释。
// ok_reply和abort_reply 是函数回复给client的信息，成功执行返回ok_reply，否则因为NX、XX等标识而未执行返回abort_reply。
// 如果传入的ok_reply和abort_reply为NULL，则对应的将返回"+OK"和"$-1"。

#define OBJ_NO_FLAGS 0
#define OBJ_SET_NX (1<<0)          /* Set if key not exists. */
#define OBJ_SET_XX (1<<1)          /* Set if key exists. */
#define OBJ_EX (1<<2)              /* Set if time in seconds is given */
#define OBJ_PX (1<<3)              /* Set if time in ms in given */
#define OBJ_KEEPTTL (1<<4)         /* Set and keep the ttl */
#define OBJ_SET_GET (1<<5)         /* Set if want to get key before set */
#define OBJ_EXAT (1<<6)            /* Set if timestamp in second is given */
#define OBJ_PXAT (1<<7)            /* Set if timestamp in ms is given */
#define OBJ_PERSIST (1<<8)         /* Set if we need to remove the ttl */

void setGenericCommand(client *c, int flags, robj *key, robj *val, robj *expire, int unit, robj *ok_reply, robj *abort_reply) {
    long long milliseconds = 0, when = 0; /* initialized to avoid any harmness warning */

    if (expire) {
        // 如果SET命令有设置过期时间，这里解析该选项值到milliseconds中。
        if (getLongLongFromObjectOrReply(c, expire, &milliseconds, NULL) != C_OK)
            return;
        if (milliseconds <= 0 || (unit == UNIT_SECONDS && milliseconds > LLONG_MAX / 1000)) {
            /* Negative value provided or multiplication is gonna overflow. */
            // 负值无效；或者单位是秒，但计算成毫秒后会溢出，则参数也是无效的。
            addReplyErrorFormat(c, "invalid expire time in %s", c->cmd->name);
            return;
        }
        // 换算为毫秒
        if (unit == UNIT_SECONDS) milliseconds *= 1000;
        when = milliseconds;
        // 如果是PXAT、EXAT，when已经是绝对时间了，不需要再加上当前时间。而PX、EX需要换算成绝对的时间点。
        if ((flags & OBJ_PX) || (flags & OBJ_EX))
            when += mstime();
        // 溢出判断，这个可以移到前一个if里面，因为前面有判断保证在加上当前时间之前，when不可能溢出。
        if (when <= 0) {
            /* Overflow detected. */
            addReplyErrorFormat(c, "invalid expire time in %s", c->cmd->name);
            return;
        }
    }

    // NX 则不存在才写，如果存在（即查询db返回非NULL）则回复abort_reply。
    // XX 存在才写。如果不存在则回复abort_reply。
    if ((flags & OBJ_SET_NX && lookupKeyWrite(c->db,key) != NULL) ||
        (flags & OBJ_SET_XX && lookupKeyWrite(c->db,key) == NULL))
    {
        addReply(c, abort_reply ? abort_reply : shared.null[c->resp]);
        return;
    }

    // 如果是set get命令，我们需要先保证查询到数据回复给client。
    if (flags & OBJ_SET_GET) {
        if (getGenericCommand(c) == C_ERR) return;
    }

    // 设置key的value，如果有KEEPTTL标识会传入，从而不删除过期时间。
    genericSetKey(c,c->db,key, val,flags & OBJ_KEEPTTL,1);
    server.dirty++;
    // key空间变更消息通知
    notifyKeyspaceEvent(NOTIFY_STRING,"set",key,c->db->id);
    if (expire) {
        // 设置新的过期时间，通知key变更消息
        setExpire(c,c->db,key,when);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"expire",key,c->db->id);

        /* Propagate as SET Key Value PXAT millisecond-timestamp if there is EXAT/PXAT or
         * propagate as SET Key Value PX millisecond if there is EX/PX flag.
         *
         * Additionally when we propagate the SET with PX (relative millisecond) we translate
         * it again to SET with PXAT for the AOF.
         *
         * Additional care is required while modifying the argument order. AOF relies on the
         * exp argument being at index 3. (see feedAppendOnlyFile)
         * */
        // 如果是EXAT/PXAT参数，这里传播都替换为PXAT；如果是EX/PX，则都替换为PX。
        // 此外在传播到AOF写入时，如果是PX，那时我们还是会转为PXAT写入。（为什么这里不一起转成PXAT绝对时间后再传播呢）
        // 这里修改参数顺序时要格外的小心，因为AOF会依赖于索引3位置处的exp参数。
        robj *exp = (flags & OBJ_PXAT) || (flags & OBJ_EXAT) ? shared.pxat : shared.px;
        robj *millisecondObj = createStringObjectFromLongLong(milliseconds);
        // 重写命令参数用于传播
        rewriteClientCommandVector(c,5,shared.set,key,val,exp,millisecondObj);
        decrRefCount(millisecondObj);
    }
    // 如果是SET GET命令，前面我们已经把查到的数据回复给client了。这里处理不是SET GET，回复ok。
    if (!(flags & OBJ_SET_GET)) {
        addReply(c, ok_reply ? ok_reply : shared.ok);
    }

    /* Propagate without the GET argument (Isn't needed if we had expire since in that case we completely re-written the command argv) */
    // 前面有过期时间时，我们重写了命令（即使有GET选项，用于传播而重写的命令也不会变）。
    // 这里对于没有过期时间，但又有GET选项的，我们要传播时要去掉GET选项，所以也要处理命令参数的重写。
    if ((flags & OBJ_SET_GET) && !expire) {
        int argc = 0;
        int j;
        // argv中存储过滤掉GET选项后的参数列表。
        robj **argv = zmalloc((c->argc-1)*sizeof(robj*));
        for (j=0; j < c->argc; j++) {
            char *a = c->argv[j]->ptr;
            /* Skip GET which may be repeated multiple times. */
            // 因为没有限制每个选项出现的次数，所以可能有多个GET，这里统统跳过。
            if (j >= 3 &&
                (a[0] == 'g' || a[0] == 'G') &&
                (a[1] == 'e' || a[1] == 'E') &&
                (a[2] == 't' || a[2] == 'T') && a[3] == '\0')
                continue;
            argv[argc++] = c->argv[j];
            incrRefCount(c->argv[j]);
        }
        // 重写命令参数后面用于传播
        replaceClientCommandVector(c, argc, argv);
    }
}

#define COMMAND_GET 0
#define COMMAND_SET 1
/*
 * The parseExtendedStringArgumentsOrReply() function performs the common validation for extended
 * string arguments used in SET and GET command.
 *
 * Get specific commands - PERSIST/DEL
 * Set specific commands - XX/NX/GET/KEEPTTL
 * Common commands - EX/EXAT/PX/PXAT
 *
 * Function takes pointers to client, flags, unit, pointer to pointer of expire obj if needed
 * to be determined and command_type which can be COMMAND_GET or COMMAND_SET.
 *
 * If there are any syntax violations C_ERR is returned else C_OK is returned.
 *
 * Input flags are updated upon parsing the arguments. Unit and expire are updated if there are any
 * EX/EXAT/PX/PXAT arguments. Unit is updated to millisecond if PX/PXAT is set.
 */
// 该函数用于GET/SET命令的公共扩展参数的验证。
// GET特定的参数指令：PERSIST/DEL。SET特定的参数指令：XX/NX/GET/KEEPTTL。
// 公共的参数指令（主要是过期的时间处理）：EX/EXAT/PX/PXAT
// 函数的前面几个参数用于解析命令的参数信息返回，最后一个参数command_type用于指定当前命令类型是SET还是GET。
// 在解析参数时会更新flags：如果有 EX/EXAT/PX/PXAT 参数，则更新Unit和expire；如果有 PX/PXAT，则单位更新为毫秒。
// 解析参数，如果格式不正确会返回err，否则解析成功返回ok。
int parseExtendedStringArgumentsOrReply(client *c, int *flags, int *unit, robj **expire, int command_type) {

    // GET命令从第三个参数开始解析；而SET命令因为多一个value参数，所以从第4个参数开始解析。
    int j = command_type == COMMAND_GET ? 2 : 3;
    for (; j < c->argc; j++) {
        // 获取当前解析参数 和 下一个参数。因为有些参数k-v形式，所以我们这里获取连续两个参数来进行处理。
        char *opt = c->argv[j]->ptr;
        robj *next = (j == c->argc-1) ? NULL : c->argv[j+1];

        if ((opt[0] == 'n' || opt[0] == 'N') &&
            (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
            !(*flags & OBJ_SET_XX) && !(*flags & OBJ_SET_GET) && (command_type == COMMAND_SET))
        {
            // SET命令专属的 NX 选项，与 XX、GET 选项互斥。
            *flags |= OBJ_SET_NX;
        } else if ((opt[0] == 'x' || opt[0] == 'X') &&
                   (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
                   !(*flags & OBJ_SET_NX) && (command_type == COMMAND_SET))
        {
            // SET命令专属的 XX 选项，与 NX 选项互斥。
            *flags |= OBJ_SET_XX;
        } else if ((opt[0] == 'g' || opt[0] == 'G') &&
                   (opt[1] == 'e' || opt[1] == 'E') &&
                   (opt[2] == 't' || opt[2] == 'T') && opt[3] == '\0' &&
                   !(*flags & OBJ_SET_NX) && (command_type == COMMAND_SET))
        {
            // SET命令专属的 GET 选项，与 NX 选项互斥。
            *flags |= OBJ_SET_GET;
        } else if (!strcasecmp(opt, "KEEPTTL") && !(*flags & OBJ_PERSIST) &&
            !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
            !(*flags & OBJ_PX) && !(*flags & OBJ_PXAT) && (command_type == COMMAND_SET))
        {
            // SET命令专属的 KEEPTTL 选项，与 PERSIST、EX、PX、EXAT、PXAT 选项互斥。（PERSIST不是GET专属么，这里条件判断可以移除吧）
            *flags |= OBJ_KEEPTTL;
        } else if (!strcasecmp(opt,"PERSIST") && (command_type == COMMAND_GET) &&
               !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
               !(*flags & OBJ_PX) && !(*flags & OBJ_PXAT) &&
               !(*flags & OBJ_KEEPTTL))
        {
            // GET命令专属 PERSIST 选项，与 EX、PX、EXAT、PXAT、KEEPTTL 选项互斥。（KEEPTTL不是SET专属么，这里条件判断可以移除吧）
            *flags |= OBJ_PERSIST;
        } else if ((opt[0] == 'e' || opt[0] == 'E') &&
                   (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EXAT) && !(*flags & OBJ_PX) &&
                   !(*flags & OBJ_PXAT) && next)
        {
            // EX选项表示过期时间，单位秒。与 KEEPTTL、PERSIST、PX、EXAT、PXAT 互斥。
            // 该选项需要后一个参数对象作为expire。我们默认单位就是秒，所以不需要再额外指定。
            *flags |= OBJ_EX;
            *expire = next;
            // 选项需要2个参数，多处理了一个，所以j++向后再跳一个参数。
            j++;
        } else if ((opt[0] == 'p' || opt[0] == 'P') &&
                   (opt[1] == 'x' || opt[1] == 'X') && opt[2] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
                   !(*flags & OBJ_PXAT) && next)
        {
            // PX选项表示过期时间，单位毫秒。与 KEEPTTL、PERSIST、EX、EXAT、PXAT 互斥。
            // 该选项需要后一个参数对象作为expire。且需要指定毫秒单位。同样需要j++再向后跳过一个参数。
            *flags |= OBJ_PX;
            *unit = UNIT_MILLISECONDS;
            *expire = next;
            j++;
        } else if ((opt[0] == 'e' || opt[0] == 'E') &&
                   (opt[1] == 'x' || opt[1] == 'X') &&
                   (opt[2] == 'a' || opt[2] == 'A') &&
                   (opt[3] == 't' || opt[3] == 'T') && opt[4] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EX) && !(*flags & OBJ_PX) &&
                   !(*flags & OBJ_PXAT) && next)
        {
            // EXAT选项表示过期时间，时间戳指定的时间，单位秒。与 KEEPTTL、PERSIST、EX、PX、PXAT 互斥。
            *flags |= OBJ_EXAT;
            *expire = next;
            j++;
        } else if ((opt[0] == 'p' || opt[0] == 'P') &&
                   (opt[1] == 'x' || opt[1] == 'X') &&
                   (opt[2] == 'a' || opt[2] == 'A') &&
                   (opt[3] == 't' || opt[3] == 'T') && opt[4] == '\0' &&
                   !(*flags & OBJ_KEEPTTL) && !(*flags & OBJ_PERSIST) &&
                   !(*flags & OBJ_EX) && !(*flags & OBJ_EXAT) &&
                   !(*flags & OBJ_PX) && next)
        {
            // PXAT选项表示过期时间，时间戳指定的时间，单位毫秒。与 KEEPTTL、PERSIST、EX、PX、PX 互斥。
            *flags |= OBJ_PXAT;
            *unit = UNIT_MILLISECONDS;
            *expire = next;
            j++;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return C_ERR;
        }
    }
    return C_OK;
}

/* SET key value [NX] [XX] [KEEPTTL] [GET] [EX <seconds>] [PX <milliseconds>]
 *     [EXAT <seconds-timestamp>][PXAT <milliseconds-timestamp>] */
// SET命令各种操作
void setCommand(client *c) {
    robj *expire = NULL;
    // 默认单位是秒，没有flags
    int unit = UNIT_SECONDS;
    int flags = OBJ_NO_FLAGS;

    // 解析SET命令的各种扩展选项
    if (parseExtendedStringArgumentsOrReply(c,&flags,&unit,&expire,COMMAND_SET) != C_OK) {
        return;
    }

    // 对于value，尝试进行编码处理
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    // 调用setGenericCommand来处理命令执行
    setGenericCommand(c,flags,c->argv[1],c->argv[2],expire,unit,NULL,NULL);
}

// setnx key value，不存在时才set，没有SET命令那么花里胡哨，算上命令总共只有3个参数。
void setnxCommand(client *c) {
    // 对参数value进行编码，然后调用setGenericCommand执行，这里传入的flag只有OBJ_SET_NX。
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,OBJ_SET_NX,c->argv[1],c->argv[2],NULL,0,shared.cone,shared.czero);
}

// setex key expire value，设置value并关联过期时间，单位秒。
void setexCommand(client *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_EX,c->argv[1],c->argv[3],c->argv[2],UNIT_SECONDS,NULL,NULL);
}

// psetex key expire value，设置value并关联过期时间，单位毫秒。
void psetexCommand(client *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_PX,c->argv[1],c->argv[3],c->argv[2],UNIT_MILLISECONDS,NULL,NULL);
}

// GET相关命令的统一处理函数。
int getGenericCommand(client *c) {
    robj *o;

    // db中获取key对应entry
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL)
        return C_OK;

    // 只有string可以使用这个命令。
    if (checkType(c,o,OBJ_STRING)) {
        return C_ERR;
    }

    // 查到数据回复给client
    addReplyBulk(c,o);
    return C_OK;
}

// GET key
void getCommand(client *c) {
    getGenericCommand(c);
}

/*
 * GETEX <key> [PERSIST][EX seconds][PX milliseconds][EXAT seconds-timestamp][PXAT milliseconds-timestamp]
 *
 * The getexCommand() function implements extended options and variants of the GET command. Unlike GET
 * command this command is not read-only.
 *
 * The default behavior when no options are specified is same as GET and does not alter any TTL.
 *
 * Only one of the below options can be used at a given time.
 *
 * 1. PERSIST removes any TTL associated with the key.
 * 2. EX Set expiry TTL in seconds.
 * 3. PX Set expiry TTL in milliseconds.
 * 4. EXAT Same like EX instead of specifying the number of seconds representing the TTL
 *      (time to live), it takes an absolute Unix timestamp
 * 5. PXAT Same like PX instead of specifying the number of milliseconds representing the TTL
 *      (time to live), it takes an absolute Unix timestamp
 *
 * Command would either return the bulk string, error or nil.
 */
// 该函数实现了扩展选项的GETEX命令，与GET命令不一样，这个命令不是只读的。默认没有任何选项时，GETEX key与GET key行为一致，不会改动TTL。
// 下面的选项，一次命令中只能使用一个：
//  1、PERSIST，移除与key相关的TTL信息。
//  2、EX，设置过期的TTL（存活时长），单位秒。
//  3、PX，设置过期的TTL，单位毫秒。
//  4、EXAT，与EX类似，但是给的是绝对时间戳，单位秒。
//  4、PXAT，与PX类似，但是给的是绝对时间戳，单位毫秒。
// 该命令会返回 bulk string、error、nil 三者中的一个。
void getexCommand(client *c) {
    robj *expire = NULL;
    int unit = UNIT_SECONDS;
    int flags = OBJ_NO_FLAGS;

    // 解析命令的选项参数
    if (parseExtendedStringArgumentsOrReply(c,&flags,&unit,&expire,COMMAND_GET) != C_OK) {
        return;
    }

    robj *o;

    // 查询key的value用于read。key不存在则直接返回。
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL)
        return;

    // 检查value的类型，非string会返回err。
    if (checkType(c,o,OBJ_STRING)) {
        return;
    }

    long long milliseconds = 0, when = 0;

    /* Validate the expiration time value first */
    // 如果有设置过期时间，这里从该参数中解析并统一转化为绝对毫秒时间戳。另外这里会检查设置的时间合法性。
    if (expire) {
        // 提取过期时间数据
        if (getLongLongFromObjectOrReply(c, expire, &milliseconds, NULL) != C_OK)
            return;
        // 合法性检查。首先不能为负，其次如果单位是秒，那么换算成毫秒不能大于LLONG_MAX。
        if (milliseconds <= 0 || (unit == UNIT_SECONDS && milliseconds > LLONG_MAX / 1000)) {
            /* Negative value provided or multiplication is gonna overflow. */
            addReplyErrorFormat(c, "invalid expire time in %s", c->cmd->name);
            return;
        }
        // 换算成毫秒
        if (unit == UNIT_SECONDS) milliseconds *= 1000;
        when = milliseconds;
        // 如果是PX/EX，还需要加上当前时间，转换成绝对时间。
        if ((flags & OBJ_PX) || (flags & OBJ_EX))
            when += mstime();
        // 判断计算的绝对时间是否发生溢出。
        if (when <= 0) {
            /* Overflow detected. */
            addReplyErrorFormat(c, "invalid expire time in %s", c->cmd->name);
            return;
        }
    }

    /* We need to do this before we expire the key or delete it */
    // 如果查询到了key，这里需要尽早回复给client。
    // 因为后面我们重新设置过期时间时，可能发现新过期时间已经到了而删除了key。所以要在过期删除之前返回给client。
    addReplyBulk(c,o);

    /* This command is never propagated as is. It is either propagated as PEXPIRE[AT],DEL,UNLINK or PERSIST.
     * This why it doesn't need special handling in feedAppendOnlyFile to convert relative expire time to absolute one. */
    // 这个命令永远不会按照原样传播，它会以PEXPIRE[AT]、DEL、UNLINK、PERSIST其中的一个命令来处理传播，具体取决于相关flags。
    // 这里在传播到AOF，feedAppendOnlyFile函数中转换相对时间为绝对时间时，不需要进行特殊处理，因为所有EXPIRE相关命令的处理方式是一致的。
    if (((flags & OBJ_PXAT) || (flags & OBJ_EXAT)) && checkAlreadyExpired(milliseconds)) {
        /* When PXAT/EXAT absolute timestamp is specified, there can be a chance that timestamp
         * has already elapsed so delete the key in that case. */
        // 当 PXAT/EXAT 标识设置绝对过期时间时，检查设置的过期时间是否已经到达，即是否已经过期。
        // checkAlreadyExpired返回1，表示已经过期，所以我们需要删除该key，根据是否允许异步来执行并传播unlink 或 del命令。
        int deleted = server.lazyfree_lazy_expire ? dbAsyncDelete(c->db, c->argv[1]) :
                      dbSyncDelete(c->db, c->argv[1]);
        serverAssert(deleted);
        // 重写传播的命令。
        robj *aux = server.lazyfree_lazy_expire ? shared.unlink : shared.del;
        rewriteClientCommandVector(c,2,aux,c->argv[1]);
        // 处理key变更通知
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        server.dirty++;
    } else if (expire) {
        // 如果有新设置过期时间，没有到期，则设置key的过期时间。
        setExpire(c,c->db,c->argv[1],when);
        /* Propagate */
        // 只设置了过期时间，所以重写为 PEXPIREAT 或 PEXPIRE命令进行传播。
        robj *exp = (flags & OBJ_PXAT) || (flags & OBJ_EXAT) ? shared.pexpireat : shared.pexpire;
        robj* millisecondObj = createStringObjectFromLongLong(milliseconds);
        rewriteClientCommandVector(c,3,exp,c->argv[1],millisecondObj);
        decrRefCount(millisecondObj);
        // 处理key变更通知
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"expire",c->argv[1],c->db->id);
        server.dirty++;
    } else if (flags & OBJ_PERSIST) {
        // 如果没有设置过期时间，但有PERSIST标识，表示设置key不过期。这里需要移除该key的过期时间。
        if (removeExpire(c->db, c->argv[1])) {
            // 如果原来有过期时间，现在移除了，则通知key变更，并重写为PERSIST命令传播给AOF。
            signalModifiedKey(c, c->db, c->argv[1]);
            rewriteClientCommandVector(c, 2, shared.persist, c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"persist",c->argv[1],c->db->id);
            server.dirty++;
        }
    }
}

// getdel key
void getdelCommand(client *c) {
    // 获取key的value回复给client
    if (getGenericCommand(c) == C_ERR) return;
    // 异步/同步删除，unlink或delete。
    int deleted = server.lazyfree_lazy_user_del ? dbAsyncDelete(c->db, c->argv[1]) :
                  dbSyncDelete(c->db, c->argv[1]);
    if (deleted) {
        /* Propagate as DEL/UNLINK command */
        // 删除成功，需要重写为DEL/UNLINK进行传播。
        robj *aux = server.lazyfree_lazy_user_del ? shared.unlink : shared.del;
        rewriteClientCommandVector(c,2,aux,c->argv[1]);
        // 处理key变更通知
        signalModifiedKey(c, c->db, c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC, "del", c->argv[1], c->db->id);
        server.dirty++;
    }
}

// getset key value命令，行为上与前面set key value get一致，只是后者扩展性更好。
void getsetCommand(client *c) {
    // 先get旧值返回
    if (getGenericCommand(c) == C_ERR) return;
    // 编码value，调用setKey来写入k-v到db中。
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setKey(c,c->db,c->argv[1],c->argv[2]);
    // keyspace变更通知（set指令）。
    notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[1],c->db->id);
    server.dirty++;

    /* Propagate as SET command */
    // 传递时命令改为SET key value。
    rewriteClientCommandArgument(c,0,shared.set);
}

// setrange key offset value，用value覆盖给定key的原值字符串offset后面的数据。最终变成 oldv[:offser] + value 形式的字符串。
void setrangeCommand(client *c) {
    robj *o;
    long offset;
    sds value = c->argv[3]->ptr;

    // 获取offset
    if (getLongFromObjectOrReply(c,c->argv[2],&offset,NULL) != C_OK)
        return;

    if (offset < 0) {
        addReplyError(c,"offset is out of range");
        return;
    }

    // 获取key的值for write
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* Return 0 when setting nothing on a non-existing string */
        // 如果db中不存在该key，且覆盖字符串为空值时，我们什么都不做。返回0，也即原值的长度。
        if (sdslen(value) == 0) {
            addReply(c,shared.czero);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        // 如果从offset覆盖追加字符串后，总长度超出最大限制，则回复err。
        if (checkStringLength(c,offset+sdslen(value)) != C_OK)
            return;

        // 创建一个空的 offset+sdslen(value) 长度的sds字符串。基于该sds字符串构建一个新的对象，设置为db中key的值。
        // 原来key在db中不存在，现在从offset处用value覆盖，所以最终我们处理后得到的值为前面全是空字符串，从offset处开始为覆盖的字符序列。
        // 这里目前key对应的新值为 offset+sdslen(value) 的空字符串，后面会将value值覆盖进去。
        o = createObject(OBJ_STRING,sdsnewlen(NULL, offset+sdslen(value)));
        dbAdd(c->db,c->argv[1],o);
    } else {
        size_t olen;

        /* Key exists, check type */
        // key存在，这里检查是否是字符串类型
        if (checkType(c,o,OBJ_STRING))
            return;

        /* Return existing string length when setting nothing */
        // 当新的value为空字符串时，我们什么都不做，返回原字符串长度。
        olen = stringObjectLen(o);
        if (sdslen(value) == 0) {
            addReplyLongLong(c,olen);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        // 如果从offset覆盖字符串后导致总长度超限，返回err
        if (checkStringLength(c,offset+sdslen(value)) != C_OK)
            return;

        /* Create a copy when the object is shared or encoded. */
        // 如果原值字符串是共享的，或者原值字符串编码不是raw，则重新复制创建新的raw字符串。
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }

    if (sdslen(value) > 0) {
        // 原o->ptr处的sds扩容为 offset+sdslen(value) 长度。如果指定的长度小于当前sds的总分配长度，则什么都不做。
        o->ptr = sdsgrowzero(o->ptr,offset+sdslen(value));
        // 将value的值复制到offset处。
        memcpy((char*)o->ptr+offset,value,sdslen(value));
        // 处理key变更通知
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_STRING,
            "setrange",c->argv[1],c->db->id);
        server.dirty++;
    }
    // 回复处理完后的总长度
    addReplyLongLong(c,sdslen(o->ptr));
}

// getrange key start end，获取[start, end]范围的字符串。
void getrangeCommand(client *c) {
    robj *o;
    long long start, end;
    char *str, llbuf[32];
    size_t strlen;

    // 解析start、end参数
    if (getLongLongFromObjectOrReply(c,c->argv[2],&start,NULL) != C_OK)
        return;
    if (getLongLongFromObjectOrReply(c,c->argv[3],&end,NULL) != C_OK)
        return;
    // 获取key的value，并检查type是否是字符串。
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptybulk)) == NULL ||
        checkType(c,o,OBJ_STRING)) return;

    // 获取value的字符串数据和数据长度
    if (o->encoding == OBJ_ENCODING_INT) {
        // 如果是INT编码则先转为字符串。
        str = llbuf;
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr);
    } else {
        str = o->ptr;
        strlen = sdslen(str);
    }

    /* Convert negative indexes */
    // 检查索引的合法性，这里先处理负索引的情况。后面统一转换成正索引后，会再次检查。
    // 不先检查的话，可能start 和 end 开始都小于-strlen，处理后start、end都为0，即使前面start > end，我们最终也会返回第一个字符数据。
    if (start < 0 && end < 0 && start > end) {
        addReply(c,shared.emptybulk);
        return;
    }
    // 负的start、end转为正索引，并且使得索引都在[0, strlen-1]范围。
    if (start < 0) start = strlen+start;
    if (end < 0) end = strlen+end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    if ((unsigned long long)end >= strlen) end = strlen-1;

    /* Precondition: end >= 0 && end < strlen, so the only condition where
     * nothing can be returned is: start > end. */
    // 前面调整后start和end都在[0, strlen-1]中，所以现在只有在 start>end 时，我们才获取不到任何数据。
    // 另外老的数据这里可能为空字符串，所以还要判断strlen==0。
    if (start > end || strlen == 0) {
        addReply(c,shared.emptybulk);
    } else {
        // 返回指定range的数据。
        addReplyBulkCBuffer(c,(char*)str+start,end-start+1);
    }
}

void mgetCommand(client *c) {
    int j;

    addReplyArrayLen(c,c->argc-1);
    for (j = 1; j < c->argc; j++) {
        // 循环查询多个key。如果key不存在则回复NULL，另外如果key存在但不是字符串也会返回NULL。
        robj *o = lookupKeyRead(c->db,c->argv[j]);
        if (o == NULL) {
            addReplyNull(c);
        } else {
            if (o->type != OBJ_STRING) {
                addReplyNull(c);
            } else {
                addReplyBulk(c,o);
            }
        }
    }
}

// mset/msetnx 命令的统一处理。
void msetGenericCommand(client *c, int nx) {
    int j;

    // mset命令总的参数应该为奇数
    if ((c->argc % 2) == 0) {
        addReplyError(c,"wrong number of arguments for MSET");
        return;
    }

    /* Handle the NX flag. The MSETNX semantic is to return zero and don't
     * set anything if at least one key already exists. */
    // 对于msetnx命令，给定的keys只要有一个存在，就返回0。
    if (nx) {
        for (j = 1; j < c->argc; j += 2) {
            if (lookupKeyWrite(c->db,c->argv[j]) != NULL) {
                addReply(c, shared.czero);
                return;
            }
        }
    }

    // 遍历挨个set k-v对。
    for (j = 1; j < c->argc; j += 2) {
        c->argv[j+1] = tryObjectEncoding(c->argv[j+1]);
        setKey(c,c->db,c->argv[j],c->argv[j+1]);
        notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[j],c->db->id);
    }
    server.dirty += (c->argc-1)/2;
    addReply(c, nx ? shared.cone : shared.ok);
}

void msetCommand(client *c) {
    msetGenericCommand(c,0);
}

void msetnxCommand(client *c) {
    msetGenericCommand(c,1);
}

// incr/decr统一处理函数，相当于转换为 incrby [incr]
void incrDecrCommand(client *c, long long incr) {
    long long value, oldvalue;
    robj *o, *new;

    // 从db查询key。这类命令操作的key对于value应该是数字，并且存储类型为字符串。
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (checkType(c,o,OBJ_STRING)) return;
    if (getLongLongFromObjectOrReply(c,o,&value,NULL) != C_OK) return;

    oldvalue = value;
    // 判断incr后是否会溢出。
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    // incr操作
    value += incr;

    // 当原value对象o非空（即key在db存在），且o不是共享对象时我们可以直接使用原value对象。
    // 一般来说数字类型的对象，编码方式只有INT或enbstr两种。对于enbstr我们没有复用原对象。
    // 因为enbstr编码，可能incr后，编码方式变为int 或者 还是enbstr但长度变了，这两种都需要我们重新创建对象，否则会有空间浪费。
    // 对于INT编码，如果新值仍然在INT编码范围内，则我们可以复用原对象；注意如果新值可以使用共享对象的话，这里我们是不会复用原对象的。
    if (o && o->refcount == 1 && o->encoding == OBJ_ENCODING_INT &&
        (value < 0 || value >= OBJ_SHARED_INTEGERS) &&
        value >= LONG_MIN && value <= LONG_MAX)
    {
        // 复用原对象，新value设置到o->ptr。
        new = o;
        o->ptr = (void*)((long)value);
    } else {
        // 创建新对象。如果value较小，且没有设置最大内存限制或没有使用LRU/LFU淘汰策略，则会使用使用共享对象。
        new = createStringObjectFromLongLongForValue(value);
        // 新值写进db中
        if (o) {
            dbOverwrite(c->db,c->argv[1],new);
        } else {
            dbAdd(c->db,c->argv[1],new);
        }
    }
    // 处理key变动通知
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"incrby",c->argv[1],c->db->id);
    server.dirty++;
    // 新值回复给client
    addReply(c,shared.colon);
    addReply(c,new);
    addReply(c,shared.crlf);
}

void incrCommand(client *c) {
    incrDecrCommand(c,1);
}

void decrCommand(client *c) {
    incrDecrCommand(c,-1);
}

void incrbyCommand(client *c) {
    long long incr;

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != C_OK) return;
    incrDecrCommand(c,incr);
}

void decrbyCommand(client *c) {
    long long incr;

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != C_OK) return;
    incrDecrCommand(c,-incr);
}

// incrbyfloat key floatvalue
void incrbyfloatCommand(client *c) {
    long double incr, value;
    robj *o, *new;

    // 获取key，确认key类型为string
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (checkType(c,o,OBJ_STRING)) return;
    // 解析value值和incr参数，如果不是浮点数则返回err。
    if (getLongDoubleFromObjectOrReply(c,o,&value,NULL) != C_OK ||
        getLongDoubleFromObjectOrReply(c,c->argv[2],&incr,NULL) != C_OK)
        return;

    value += incr;
    // math.h中的函数，判断浮点数是否溢出。
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }
    // 浮点数会统一使用字符串编码。参数1表示使用humanfriendly形式编码，可能会丢失精度。
    new = createStringObjectFromLongDouble(value,1);
    // 更新db新的value
    if (o)
        dbOverwrite(c->db,c->argv[1],new);
    else
        dbAdd(c->db,c->argv[1],new);
    // 处理key变动通知
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"incrbyfloat",c->argv[1],c->db->id);
    server.dirty++;
    // 返回client新值
    addReplyBulk(c,new);

    /* Always replicate INCRBYFLOAT as a SET command with the final value
     * in order to make sure that differences in float precision or formatting
     * will not create differences in replicas or after an AOF restart. */
    // 总是将 INCRBYFLOAT 作为 SET 命令来传播。从而确保浮点数的精度以及格式化方式不会影响最终结果的一致性。
    rewriteClientCommandArgument(c,0,shared.set);
    rewriteClientCommandArgument(c,2,new);
    rewriteClientCommandArgument(c,3,shared.keepttl);
}

// append key value，如果key不存在则创建，存在则值追加。
void appendCommand(client *c) {
    size_t totlen;
    robj *o, *append;

    // 查询key
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* Create the key */
        // key不存在，则创建
        c->argv[2] = tryObjectEncoding(c->argv[2]);
        dbAdd(c->db,c->argv[1],c->argv[2]);
        incrRefCount(c->argv[2]);
        totlen = stringObjectLen(c->argv[2]);
    } else {
        /* Key exists, check type */
        // key存在，要确保是字符串类型。
        if (checkType(c,o,OBJ_STRING))
            return;

        /* "append" is an argument, so always an sds */
        // 参数总是一个sds字符串。这里需要检查追加后的总长度是否超限。
        append = c->argv[2];
        totlen = stringObjectLen(o)+sdslen(append->ptr);
        if (checkStringLength(c,totlen) != C_OK)
            return;

        /* Append the value */
        // 追加元素。注意如果老的value字符串对象是共享的，或者不是raw编码，这里需要重新创建raw编码的字符串。
        o = dbUnshareStringValue(c->db,c->argv[1],o);
        o->ptr = sdscatlen(o->ptr,append->ptr,sdslen(append->ptr));
        totlen = sdslen(o->ptr);
    }
    // 处理key变动通知
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"append",c->argv[1],c->db->id);
    server.dirty++;
    // 回复追加后value的总长度。
    addReplyLongLong(c,totlen);
}

// STRLEN key，获取字符串对象值的长度。
void strlenCommand(client *c) {
    robj *o;
    // 查询key，并检查value的类型。
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_STRING)) return;
    // 返回字符串对象值的长度。
    addReplyLongLong(c,stringObjectLen(o));
}


/* STRALGO -- Implement complex algorithms on strings.
 *
 * STRALGO <algorithm> ... arguments ... */

// STRALGO命令，实现字符串上的复杂算法，目前只有实现了lcs算法。
void stralgoLCS(client *c);     /* This implements the LCS algorithm. */
void stralgoCommand(client *c) {
    /* Select the algorithm. */
    // 根据第一个参数选择处理的算法。
    if (!strcasecmp(c->argv[1]->ptr,"lcs")) {
        stralgoLCS(c);
    } else {
        addReplyErrorObject(c,shared.syntaxerr);
    }
}

/* STRALGO <algo> [IDX] [LEN] [MINMATCHLEN <len>] [WITHMATCHLEN]
 *     STRINGS <string> <string> | KEYS <keya> <keyb>
 */
// 求最长公共子序列的算法。
// IDX表示获取公共子序列的每个字符的index，LEN表示获取公共子序列的长度。
void stralgoLCS(client *c) {
    uint32_t i, j;
    long long minmatchlen = 0;
    sds a = NULL, b = NULL;
    int getlen = 0, getidx = 0, withmatchlen = 0;
    robj *obja = NULL, *objb = NULL;

    // 参数解析
    for (j = 2; j < (uint32_t)c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc-1) - j;

        if (!strcasecmp(opt,"IDX")) {
            // 返回最长子序列index
            getidx = 1;
        } else if (!strcasecmp(opt,"LEN")) {
            // 返回最长子序列的长度
            getlen = 1;
        } else if (!strcasecmp(opt,"WITHMATCHLEN")) {
            // 该参数表示每次返回index范围时，同时返回连续匹配的长度
            withmatchlen = 1;
        } else if (!strcasecmp(opt,"MINMATCHLEN") && moreargs) {
            // 只有达到了这个最小连续匹配长度参数值的，才返回匹配索引范围。
            if (getLongLongFromObjectOrReply(c,c->argv[j+1],&minmatchlen,NULL)
                != C_OK) goto cleanup;
            if (minmatchlen < 0) minmatchlen = 0;
            j++;
        } else if (!strcasecmp(opt,"STRINGS") && moreargs > 1) {
            // 表示两个字符串对比，直接字符串sds赋值给a、b。
            if (a != NULL) {
                addReplyError(c,"Either use STRINGS or KEYS");
                goto cleanup;
            }
            a = c->argv[j+1]->ptr;
            b = c->argv[j+2]->ptr;
            j += 2;
        } else if (!strcasecmp(opt,"KEYS") && moreargs > 1) {
            // 对比两个key对象的值
            if (a != NULL) {
                addReplyError(c,"Either use STRINGS or KEYS");
                goto cleanup;
            }
            // 取出两个key对象值，检查字符串类型。
            obja = lookupKeyRead(c->db,c->argv[j+1]);
            objb = lookupKeyRead(c->db,c->argv[j+2]);
            if ((obja && obja->type != OBJ_STRING) ||
                (objb && objb->type != OBJ_STRING))
            {
                addReplyError(c,
                    "The specified keys must contain string values");
                /* Don't cleanup the objects, we need to do that
                 * only after calling getDecodedObject(). */
                obja = NULL;
                objb = NULL;
                goto cleanup;
            }
            // 解析出来字符串，赋值给a、b。
            obja = obja ? getDecodedObject(obja) : createStringObject("",0);
            objb = objb ? getDecodedObject(objb) : createStringObject("",0);
            a = obja->ptr;
            b = objb->ptr;
            j += 2;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            goto cleanup;
        }
    }

    /* Complain if the user passed ambiguous parameters. */
    // a和b为NULL，则我们没解析到要对比的两个字符串。另外 LEN 和 IDX 选项是互斥的。
    if (a == NULL) {
        addReplyError(c,"Please specify two strings: "
                        "STRINGS or KEYS options are mandatory");
        goto cleanup;
    } else if (getlen && getidx) {
        addReplyError(c,
            "If you want both the length and indexes, please "
            "just use IDX.");
        goto cleanup;
    }

    /* Detect string truncation or later overflows. */
    // 我们是使用动态规划来计算LCS，需要一个（m+1）*（n+1）的table来记录，所以这里判断长度，防止后面table索引超限。
    if (sdslen(a) >= UINT32_MAX-1 || sdslen(b) >= UINT32_MAX-1) {
        addReplyError(c, "String too long for LCS");
        goto cleanup;
    }

    /* Compute the LCS using the vanilla dynamic programming technique of
     * building a table of LCS(x,y) substrings. */
    uint32_t alen = sdslen(a);
    uint32_t blen = sdslen(b);

    /* Setup an uint32_t array to store at LCS[i,j] the length of the
     * LCS A0..i-1, B0..j-1. Note that we have a linear array here, so
     * we index it as LCS[j+(blen+1)*j] */
    // 使用线性数组来表示，LCS所需的二维数组table。
    #define LCS(A,B) lcs[(B)+((A)*(blen+1))]

    /* Try to allocate the LCS table, and abort on overflow or insufficient memory. */
    // 尝试为LCS table分配内存，lcssize为LCS表总的元素个数（相乘不会溢出，因为前面检查过了），lcsalloc为总共需要分配的内存。
    unsigned long long lcssize = (unsigned long long)(alen+1)*(blen+1); /* Can't overflow due to the size limits above. */
    unsigned long long lcsalloc = lcssize * sizeof(uint32_t);
    uint32_t *lcs = NULL;
    if (lcsalloc < SIZE_MAX && lcsalloc / lcssize == sizeof(uint32_t))
        lcs = ztrymalloc(lcsalloc);
    if (!lcs) {
        addReplyError(c, "Insufficient memory");
        goto cleanup;
    }

    /* Start building the LCS table. */
    // 开始填充LCS表数据，两层循环遍历。
    for (uint32_t i = 0; i <= alen; i++) {
        for (uint32_t j = 0; j <= blen; j++) {
            if (i == 0 || j == 0) {
                /* If one substring has length of zero, the
                 * LCS length is zero. */
                // 如果有一个字符串长度为0，那么他们的LCS长度也为0。
                LCS(i,j) = 0;
            } else if (a[i-1] == b[j-1]) {
                /* The len LCS (and the LCS itself) of two
                 * sequences with the same final character, is the
                 * LCS of the two sequences without the last char
                 * plus that last char. */
                // 如果以index i-1和j-1结束的两个字符串的最后一个字符相同，则有 LCS(i,j) = LCS(i-1,j-1)+1
                LCS(i,j) = LCS(i-1,j-1)+1;
            } else {
                /* If the last character is different, take the longest
                 * between the LCS of the first string and the second
                 * minus the last char, and the reverse. */
                // 如果以index i-1和j-1结束的两个字符串的最后一个字符不同，则LCS(i-1,j)、LCS(i,j-1)、LCS(i-1,j-1)三者取最大值。
                // 因为 LCS(i-1,j-1) 肯定是小于前面两个的，所以这里取LCS(i-1,j)、LCS(i,j-1)二者最大值。
                uint32_t lcs1 = LCS(i-1,j);
                uint32_t lcs2 = LCS(i,j-1);
                LCS(i,j) = lcs1 > lcs2 ? lcs1 : lcs2;
            }
        }
    }

    /* Store the actual LCS string in "result" if needed. We create
     * it backward, but the length is already known, we store it into idx. */
    // 通过前面填充LCS表，我们已经知道了最长公共子序列的长度为LCS(alen,blen)。
    uint32_t idx = LCS(alen,blen);
    // result用于保存结果LCS字符串。
    sds result = NULL;        /* Resulting LCS string. */
    // 如果是getindex模式，我们不知道最终返回的bulk数组长度，所以创建新的reply节点先存数据，最后再写长度。arraylenptr指向创建的NULL节点。
    void *arraylenptr = NULL; /* Deffered length of the array for IDX. */
    uint32_t arange_start = alen, /* alen signals that values are not set. */
             arange_end = 0,
             brange_start = 0,
             brange_end = 0;

    /* Do we need to compute the actual LCS string? Allocate it in that case. */
    // 因为前面有判断 getidx 和 getlen不能同时设置，所以理论上这里只需判断 !getlen 就可以了，即只要不是只查询LCS长度，我们都要生成result。
    int computelcs = getidx || !getlen;
    if (computelcs) result = sdsnewlen(SDS_NOINIT,idx);

    /* Start with a deferred array if we have to emit the ranges. */
    // 如果我们需要返回LCS字符的索引，现在不知道bulk总长度，所以这里要提前创建reply节点存数据。arraylen用于存储最终长度，最后写入。
    uint32_t arraylen = 0;  /* Number of ranges emitted in the array. */
    if (getidx) {
        // 如果要获取index，则我们返回的数据是 matches: 匹配列表（index），len: 总长度，即2个长度的map。
        // RESP2是使用数组来返回的map，而RESP3则是更好的数据展示，直接使用map形式。
        // 这里先写入map长度，然后写入matches信息，具体匹配信息是我们要填充到arraylenptr中的。数据返回时要很小心的处理写入的顺序。
        addReplyMapLen(c,2);
        addReplyBulkCString(c,"matches");
        arraylenptr = addReplyDeferredLen(c);
    }

    i = alen, j = blen;
    // a和b字符串都从后往前遍历对比。
    while (computelcs && i > 0 && j > 0) {
        int emit_range = 0;
        if (a[i-1] == b[j-1]) {
            /* If there is a match, store the character and reduce
             * the indexes to look for a new match. */
            // 如果当前遍历到的a、b中字符相等，则需要将该字符加入result中，result从后往前填充。
            result[idx-1] = a[i-1];

            /* Track the current range. */
            // 追踪当前匹配的范围
            if (arange_start == alen) {
                // arange_start==alen表示这个范围的开始，目前只有一个连续的字符匹配，所以都设置为i-1。
                // 这里 [arange_start, arange_end] 和 [brange_start, brange_end] 这两个连续的子串是匹配的。
                arange_start = i-1;
                arange_end = i-1;
                brange_start = j-1;
                brange_end = j-1;
            } else {
                /* Let's see if we can extend the range backward since
                 * it is contiguous. */
                // 检查当前记录的连续匹配的范围是否可以扩展，即找到的两个匹配的子串的start是否可以前移。
                // 这里断上一次匹配的字符是否是i、j，如果是则连续范围可以扩展，我们更新start为i-1、j-1。
                // 反之如果不可以扩展，则标记结束，我们要发送当前index范围给client。
                if (arange_start == i && brange_start == j) {
                    arange_start--;
                    brange_start--;
                } else {
                    // 理论上这里好像不需要，因为在当前字符不匹配时，如果 arange_start != alen，表示前面有匹配的序列，已经处理发送了。
                    emit_range = 1;
                }
            }
            /* Emit the range if we matched with the first byte of
             * one of the two strings. We'll exit the loop ASAP. */
            // 如果我们已经遍历完了某个字符串，则也应该标记当前连续匹配的子串结束。
            if (arange_start == 0 || brange_start == 0) emit_range = 1;
            idx--; i--; j--;
        } else {
            /* Otherwise reduce i and j depending on the largest
             * LCS between, to understand what direction we need to go. */
            // 如果当前遍历到的a、b中的字符不相等。因为我们需要获取最长子序列，所以以对应LCS较大的那个来决定i和j哪个左进一步。
            uint32_t lcs1 = LCS(i-1,j);
            uint32_t lcs2 = LCS(i,j-1);
            if (lcs1 > lcs2)
                i--;
            else
                j--;
            // 如果arange_start不是默认值alen，而当前字符我们是不匹配的，说明前面有匹配的序列，我们需要先发送出去。
            if (arange_start != alen) emit_range = 1;
        }

        /* Emit the current range if needed. */
        // 如果需要，这里发送当前的匹配索引范围。
        uint32_t match_len = arange_end - arange_start + 1;
        if (emit_range) {
            if (minmatchlen == 0 || match_len >= minmatchlen) {
                // 没有设置minmatchlen限制，或者当前连续匹配的字符达到了minmatchlen，才发送匹配的索引范围。
                // 另外我们只有在有getidx标识时，才创建arraylenptr来存储数据，才需要发送匹配的索引范围。
                if (arraylenptr) {
                    // 返回范围时如果需返回匹配长度，则总共是3个bulk数据数组，
                    addReplyArrayLen(c,2+withmatchlen);
                    // 第一个bulk数据，是a的index范围，start、end。
                    addReplyArrayLen(c,2);
                    addReplyLongLong(c,arange_start);
                    addReplyLongLong(c,arange_end);
                    // 第二个bulk数据，是b的index范围，start、end。
                    addReplyArrayLen(c,2);
                    addReplyLongLong(c,brange_start);
                    addReplyLongLong(c,brange_end);
                    // 最后如果要返回匹配长度，第三个bulk就是长度匹配长度。
                    if (withmatchlen) addReplyLongLong(c,match_len);
                    // 一整个连续的match索引范围，作为最终返回的数组的一项。arraylen记录最终返回的数组项数，用于填充bulk长度。
                    arraylen++;
                }
            }
            // 重置匹配的范围记录
            arange_start = alen; /* Restart at the next match. */
        }
    }

    /* Signal modified key, increment dirty, ... */
    // 如果需要这里要处理key变更通知，增加db修改数量用于RDB备份使用。LCS不会变更数据，所以不需要。

    /* Reply depending on the given options. */
    if (arraylenptr) {
        // getindex，会返回匹配的索引范围，返回LCS长度。
        addReplyBulkCString(c,"len");
        addReplyLongLong(c,LCS(alen,blen));
        // 填充 匹配的索引范围数组 的总长度
        setDeferredArrayLen(c,arraylenptr,arraylen);
    } else if (getlen) {
        // getlen标识，只需要返回LCS长度
        addReplyLongLong(c,LCS(alen,blen));
    } else {
        // 两个标识都没有，则返回LCS字符串
        addReplyBulkSds(c,result);
        result = NULL;
    }

    /* Cleanup. */
    // 清理空间
    sdsfree(result);
    zfree(lcs);

cleanup:
    if (obja) decrRefCount(obja);
    if (objb) decrRefCount(objb);
    return;
}

