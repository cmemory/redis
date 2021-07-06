/*
 * Copyright (c) 2009-2016, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "monotonic.h"
#include "cluster.h"
#include "slowlog.h"
#include "bio.h"
#include "latency.h"
#include "atomicvar.h"
#include "mt19937-64.h"

#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <locale.h>
#include <sys/socket.h>
#include <sys/resource.h>

#ifdef __linux__
#include <sys/mman.h>
#endif

/* Our shared "common" objects */

struct sharedObjectsStruct shared;

/* Global vars that are actually used as constants. The following double
 * values are used for double on-disk serialization, and are initialized
 * at runtime to avoid strange compiler optimizations. */

double R_Zero, R_PosInf, R_NegInf, R_Nan;

/*================================= Globals ================================= */

/* Global vars */
struct redisServer server; /* Server global state */

/* Our command table.
 *
 * Every entry is composed of the following fields:
 *
 * name:        A string representing the command name.
 *
 * function:    Pointer to the C function implementing the command.
 *
 * arity:       Number of arguments, it is possible to use -N to say >= N
 *
 * sflags:      Command flags as string. See below for a table of flags.
 *
 * flags:       Flags as bitmask. Computed by Redis using the 'sflags' field.
 *
 * get_keys_proc: An optional function to get key arguments from a command.
 *                This is only used when the following three fields are not
 *                enough to specify what arguments are keys.
 *
 * first_key_index: First argument that is a key
 *
 * last_key_index: Last argument that is a key
 *
 * key_step:    Step to get all the keys from first to last argument.
 *              For instance in MSET the step is two since arguments
 *              are key,val,key,val,...
 *
 * microseconds: Microseconds of total execution time for this command.
 *
 * calls:       Total number of calls of this command.
 *
 * id:          Command bit identifier for ACLs or other goals.
 *
 * The flags, microseconds and calls fields are computed by Redis and should
 * always be set to zero.
 *
 * Command flags are expressed using space separated strings, that are turned
 * into actual flags by the populateCommandTable() function.
 *
 * This is the meaning of the flags:
 *
 * write:       Write command (may modify the key space).
 *
 * read-only:   Commands just reading from keys without changing the content.
 *              Note that commands that don't read from the keyspace such as
 *              TIME, SELECT, INFO, administrative commands, and connection
 *              or transaction related commands (multi, exec, discard, ...)
 *              are not flagged as read-only commands, since they affect the
 *              server or the connection in other ways.
 *
 * use-memory:  May increase memory usage once called. Don't allow if out
 *              of memory.
 *
 * admin:       Administrative command, like SAVE or SHUTDOWN.
 *
 * pub-sub:     Pub/Sub related command.
 *
 * no-script:   Command not allowed in scripts.
 *
 * random:      Random command. Command is not deterministic, that is, the same
 *              command with the same arguments, with the same key space, may
 *              have different results. For instance SPOP and RANDOMKEY are
 *              two random commands.
 *
 * to-sort:     Sort command output array if called from script, so that the
 *              output is deterministic. When this flag is used (not always
 *              possible), then the "random" flag is not needed.
 *
 * ok-loading:  Allow the command while loading the database.
 *
 * ok-stale:    Allow the command while a slave has stale data but is not
 *              allowed to serve this data. Normally no command is accepted
 *              in this condition but just a few.
 *
 * no-monitor:  Do not automatically propagate the command on MONITOR.
 *
 * no-slowlog:  Do not automatically propagate the command to the slowlog.
 *
 * cluster-asking: Perform an implicit ASKING for this command, so the
 *              command will be accepted in cluster mode if the slot is marked
 *              as 'importing'.
 *
 * fast:        Fast command: O(1) or O(log(N)) command that should never
 *              delay its execution as long as the kernel scheduler is giving
 *              us time. Note that commands that may trigger a DEL as a side
 *              effect (like SET) are not fast commands.
 * 
 * may-replicate: Command may produce replication traffic, but should be 
 *                allowed under circumstances where write commands are disallowed. 
 *                Examples include PUBLISH, which replicates pubsub messages,and 
 *                EVAL, which may execute write commands, which are replicated, 
 *                or may just execute read commands. A command can not be marked 
 *                both "write" and "may-replicate"
 *
 * The following additional flags are only used in order to put commands
 * in a specific ACL category. Commands can have multiple ACL categories.
 *
 * @keyspace, @read, @write, @set, @sortedset, @list, @hash, @string, @bitmap,
 * @hyperloglog, @stream, @admin, @fast, @slow, @pubsub, @blocking, @dangerous,
 * @connection, @transaction, @scripting, @geo.
 *
 * Note that:
 *
 * 1) The read-only flag implies the @read ACL category.
 * 2) The write flag implies the @write ACL category.
 * 3) The fast flag implies the @fast ACL category.
 * 4) The admin flag implies the @admin and @dangerous ACL category.
 * 5) The pub-sub flag implies the @pubsub ACL category.
 * 6) The lack of fast flag implies the @slow ACL category.
 * 7) The non obvious "keyspace" category includes the commands
 *    that interact with keys without having anything to do with
 *    specific data structures, such as: DEL, RENAME, MOVE, SELECT,
 *    TYPE, EXPIRE*, PEXPIRE*, TTL, PTTL, ...
 */

struct redisCommand redisCommandTable[] = {
    {"module",moduleCommand,-2,
     "admin no-script",
     0,NULL,0,0,0,0,0,0},

    {"get",getCommand,2,
     "read-only fast @string",
     0,NULL,1,1,1,0,0,0},

    {"getex",getexCommand,-2,
     "write fast @string",
     0,NULL,1,1,1,0,0,0},

    {"getdel",getdelCommand,2,
     "write fast @string",
     0,NULL,1,1,1,0,0,0},

    /* Note that we can't flag set as fast, since it may perform an
     * implicit DEL of a large key. */
    {"set",setCommand,-3,
     "write use-memory @string",
     0,NULL,1,1,1,0,0,0},

    {"setnx",setnxCommand,3,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"setex",setexCommand,4,
     "write use-memory @string",
     0,NULL,1,1,1,0,0,0},

    {"psetex",psetexCommand,4,
     "write use-memory @string",
     0,NULL,1,1,1,0,0,0},

    {"append",appendCommand,3,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"strlen",strlenCommand,2,
     "read-only fast @string",
     0,NULL,1,1,1,0,0,0},

    {"del",delCommand,-2,
     "write @keyspace",
     0,NULL,1,-1,1,0,0,0},

    {"unlink",unlinkCommand,-2,
     "write fast @keyspace",
     0,NULL,1,-1,1,0,0,0},

    {"exists",existsCommand,-2,
     "read-only fast @keyspace",
     0,NULL,1,-1,1,0,0,0},

    {"setbit",setbitCommand,4,
     "write use-memory @bitmap",
     0,NULL,1,1,1,0,0,0},

    {"getbit",getbitCommand,3,
     "read-only fast @bitmap",
     0,NULL,1,1,1,0,0,0},

    {"bitfield",bitfieldCommand,-2,
     "write use-memory @bitmap",
     0,NULL,1,1,1,0,0,0},

    {"bitfield_ro",bitfieldroCommand,-2,
     "read-only fast @bitmap",
     0,NULL,1,1,1,0,0,0},

    {"setrange",setrangeCommand,4,
     "write use-memory @string",
     0,NULL,1,1,1,0,0,0},

    {"getrange",getrangeCommand,4,
     "read-only @string",
     0,NULL,1,1,1,0,0,0},

    {"substr",getrangeCommand,4,
     "read-only @string",
     0,NULL,1,1,1,0,0,0},

    {"incr",incrCommand,2,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"decr",decrCommand,2,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"mget",mgetCommand,-2,
     "read-only fast @string",
     0,NULL,1,-1,1,0,0,0},

    {"rpush",rpushCommand,-3,
     "write use-memory fast @list",
     0,NULL,1,1,1,0,0,0},

    {"lpush",lpushCommand,-3,
     "write use-memory fast @list",
     0,NULL,1,1,1,0,0,0},

    {"rpushx",rpushxCommand,-3,
     "write use-memory fast @list",
     0,NULL,1,1,1,0,0,0},

    {"lpushx",lpushxCommand,-3,
     "write use-memory fast @list",
     0,NULL,1,1,1,0,0,0},

    {"linsert",linsertCommand,5,
     "write use-memory @list",
     0,NULL,1,1,1,0,0,0},

    {"rpop",rpopCommand,-2,
     "write fast @list",
     0,NULL,1,1,1,0,0,0},

    {"lpop",lpopCommand,-2,
     "write fast @list",
     0,NULL,1,1,1,0,0,0},

    {"brpop",brpopCommand,-3,
     "write no-script @list @blocking",
     0,NULL,1,-2,1,0,0,0},

    {"brpoplpush",brpoplpushCommand,4,
     "write use-memory no-script @list @blocking",
     0,NULL,1,2,1,0,0,0},

    {"blmove",blmoveCommand,6,
     "write use-memory no-script @list @blocking",
     0,NULL,1,2,1,0,0,0},

    {"blpop",blpopCommand,-3,
     "write no-script @list @blocking",
     0,NULL,1,-2,1,0,0,0},

    {"llen",llenCommand,2,
     "read-only fast @list",
     0,NULL,1,1,1,0,0,0},

    {"lindex",lindexCommand,3,
     "read-only @list",
     0,NULL,1,1,1,0,0,0},

    {"lset",lsetCommand,4,
     "write use-memory @list",
     0,NULL,1,1,1,0,0,0},

    {"lrange",lrangeCommand,4,
     "read-only @list",
     0,NULL,1,1,1,0,0,0},

    {"ltrim",ltrimCommand,4,
     "write @list",
     0,NULL,1,1,1,0,0,0},

    {"lpos",lposCommand,-3,
     "read-only @list",
     0,NULL,1,1,1,0,0,0},

    {"lrem",lremCommand,4,
     "write @list",
     0,NULL,1,1,1,0,0,0},

    {"rpoplpush",rpoplpushCommand,3,
     "write use-memory @list",
     0,NULL,1,2,1,0,0,0},

    {"lmove",lmoveCommand,5,
     "write use-memory @list",
     0,NULL,1,2,1,0,0,0},

    {"sadd",saddCommand,-3,
     "write use-memory fast @set",
     0,NULL,1,1,1,0,0,0},

    {"srem",sremCommand,-3,
     "write fast @set",
     0,NULL,1,1,1,0,0,0},

    {"smove",smoveCommand,4,
     "write fast @set",
     0,NULL,1,2,1,0,0,0},

    {"sismember",sismemberCommand,3,
     "read-only fast @set",
     0,NULL,1,1,1,0,0,0},

    {"smismember",smismemberCommand,-3,
     "read-only fast @set",
     0,NULL,1,1,1,0,0,0},

    {"scard",scardCommand,2,
     "read-only fast @set",
     0,NULL,1,1,1,0,0,0},

    {"spop",spopCommand,-2,
     "write random fast @set",
     0,NULL,1,1,1,0,0,0},

    {"srandmember",srandmemberCommand,-2,
     "read-only random @set",
     0,NULL,1,1,1,0,0,0},

    {"sinter",sinterCommand,-2,
     "read-only to-sort @set",
     0,NULL,1,-1,1,0,0,0},

    {"sinterstore",sinterstoreCommand,-3,
     "write use-memory @set",
     0,NULL,1,-1,1,0,0,0},

    {"sunion",sunionCommand,-2,
     "read-only to-sort @set",
     0,NULL,1,-1,1,0,0,0},

    {"sunionstore",sunionstoreCommand,-3,
     "write use-memory @set",
     0,NULL,1,-1,1,0,0,0},

    {"sdiff",sdiffCommand,-2,
     "read-only to-sort @set",
     0,NULL,1,-1,1,0,0,0},

    {"sdiffstore",sdiffstoreCommand,-3,
     "write use-memory @set",
     0,NULL,1,-1,1,0,0,0},

    {"smembers",sinterCommand,2,
     "read-only to-sort @set",
     0,NULL,1,1,1,0,0,0},

    {"sscan",sscanCommand,-3,
     "read-only random @set",
     0,NULL,1,1,1,0,0,0},

    {"zadd",zaddCommand,-4,
     "write use-memory fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zincrby",zincrbyCommand,4,
     "write use-memory fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrem",zremCommand,-3,
     "write fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zremrangebyscore",zremrangebyscoreCommand,4,
     "write @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zremrangebyrank",zremrangebyrankCommand,4,
     "write @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zremrangebylex",zremrangebylexCommand,4,
     "write @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zunionstore",zunionstoreCommand,-4,
     "write use-memory @sortedset",
     0,zunionInterDiffStoreGetKeys,1,1,1,0,0,0},

    {"zinterstore",zinterstoreCommand,-4,
     "write use-memory @sortedset",
     0,zunionInterDiffStoreGetKeys,1,1,1,0,0,0},

    {"zdiffstore",zdiffstoreCommand,-4,
     "write use-memory @sortedset",
     0,zunionInterDiffStoreGetKeys,1,1,1,0,0,0},

    {"zunion",zunionCommand,-3,
     "read-only @sortedset",
     0,zunionInterDiffGetKeys,0,0,0,0,0,0},

    {"zinter",zinterCommand,-3,
     "read-only @sortedset",
     0,zunionInterDiffGetKeys,0,0,0,0,0,0},

    {"zdiff",zdiffCommand,-3,
     "read-only @sortedset",
     0,zunionInterDiffGetKeys,0,0,0,0,0,0},

    {"zrange",zrangeCommand,-4,
     "read-only @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrangestore",zrangestoreCommand,-5,
     "write use-memory @sortedset",
     0,NULL,1,2,1,0,0,0},

    {"zrangebyscore",zrangebyscoreCommand,-4,
     "read-only @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrevrangebyscore",zrevrangebyscoreCommand,-4,
     "read-only @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrangebylex",zrangebylexCommand,-4,
     "read-only @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrevrangebylex",zrevrangebylexCommand,-4,
     "read-only @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zcount",zcountCommand,4,
     "read-only fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zlexcount",zlexcountCommand,4,
     "read-only fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrevrange",zrevrangeCommand,-4,
     "read-only @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zcard",zcardCommand,2,
     "read-only fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zscore",zscoreCommand,3,
     "read-only fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zmscore",zmscoreCommand,-3,
     "read-only fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrank",zrankCommand,3,
     "read-only fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrevrank",zrevrankCommand,3,
     "read-only fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zscan",zscanCommand,-3,
     "read-only random @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zpopmin",zpopminCommand,-2,
     "write fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zpopmax",zpopmaxCommand,-2,
     "write fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"bzpopmin",bzpopminCommand,-3,
     "write no-script fast @sortedset @blocking",
     0,NULL,1,-2,1,0,0,0},

    {"bzpopmax",bzpopmaxCommand,-3,
     "write no-script fast @sortedset @blocking",
     0,NULL,1,-2,1,0,0,0},

    {"zrandmember",zrandmemberCommand,-2,
     "read-only random @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"hset",hsetCommand,-4,
     "write use-memory fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hsetnx",hsetnxCommand,4,
     "write use-memory fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hget",hgetCommand,3,
     "read-only fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hmset",hsetCommand,-4,
     "write use-memory fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hmget",hmgetCommand,-3,
     "read-only fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hincrby",hincrbyCommand,4,
     "write use-memory fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hincrbyfloat",hincrbyfloatCommand,4,
     "write use-memory fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hdel",hdelCommand,-3,
     "write fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hlen",hlenCommand,2,
     "read-only fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hstrlen",hstrlenCommand,3,
     "read-only fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hkeys",hkeysCommand,2,
     "read-only to-sort @hash",
     0,NULL,1,1,1,0,0,0},

    {"hvals",hvalsCommand,2,
     "read-only to-sort @hash",
     0,NULL,1,1,1,0,0,0},

    {"hgetall",hgetallCommand,2,
     "read-only random @hash",
     0,NULL,1,1,1,0,0,0},

    {"hexists",hexistsCommand,3,
     "read-only fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hrandfield",hrandfieldCommand,-2,
     "read-only random @hash",
     0,NULL,1,1,1,0,0,0},

    {"hscan",hscanCommand,-3,
     "read-only random @hash",
     0,NULL,1,1,1,0,0,0},

    {"incrby",incrbyCommand,3,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"decrby",decrbyCommand,3,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"incrbyfloat",incrbyfloatCommand,3,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"getset",getsetCommand,3,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"mset",msetCommand,-3,
     "write use-memory @string",
     0,NULL,1,-1,2,0,0,0},

    {"msetnx",msetnxCommand,-3,
     "write use-memory @string",
     0,NULL,1,-1,2,0,0,0},

    {"randomkey",randomkeyCommand,1,
     "read-only random @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"select",selectCommand,2,
     "ok-loading fast ok-stale @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"swapdb",swapdbCommand,3,
     "write fast @keyspace @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"move",moveCommand,3,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"copy",copyCommand,-3,
     "write use-memory @keyspace",
     0,NULL,1,2,1,0,0,0},

    /* Like for SET, we can't mark rename as a fast command because
     * overwriting the target key may result in an implicit slow DEL. */
    {"rename",renameCommand,3,
     "write @keyspace",
     0,NULL,1,2,1,0,0,0},

    {"renamenx",renamenxCommand,3,
     "write fast @keyspace",
     0,NULL,1,2,1,0,0,0},

    {"expire",expireCommand,3,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"expireat",expireatCommand,3,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"pexpire",pexpireCommand,3,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"pexpireat",pexpireatCommand,3,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"keys",keysCommand,2,
     "read-only to-sort @keyspace @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"scan",scanCommand,-2,
     "read-only random @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"dbsize",dbsizeCommand,1,
     "read-only fast @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"auth",authCommand,-2,
     "no-auth no-script ok-loading ok-stale fast @connection",
     0,NULL,0,0,0,0,0,0},

    /* We don't allow PING during loading since in Redis PING is used as
     * failure detection, and a loading server is considered to be
     * not available. */
    {"ping",pingCommand,-1,
     "ok-stale fast @connection",
     0,NULL,0,0,0,0,0,0},

    {"echo",echoCommand,2,
     "fast @connection",
     0,NULL,0,0,0,0,0,0},

    {"save",saveCommand,1,
     "admin no-script",
     0,NULL,0,0,0,0,0,0},

    {"bgsave",bgsaveCommand,-1,
     "admin no-script",
     0,NULL,0,0,0,0,0,0},

    {"bgrewriteaof",bgrewriteaofCommand,1,
     "admin no-script",
     0,NULL,0,0,0,0,0,0},

    {"shutdown",shutdownCommand,-1,
     "admin no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"lastsave",lastsaveCommand,1,
     "random fast ok-loading ok-stale @admin @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"type",typeCommand,2,
     "read-only fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"multi",multiCommand,1,
     "no-script fast ok-loading ok-stale @transaction",
     0,NULL,0,0,0,0,0,0},

    {"exec",execCommand,1,
     "no-script no-slowlog ok-loading ok-stale @transaction",
     0,NULL,0,0,0,0,0,0},

    {"discard",discardCommand,1,
     "no-script fast ok-loading ok-stale @transaction",
     0,NULL,0,0,0,0,0,0},

    {"sync",syncCommand,1,
     "admin no-script",
     0,NULL,0,0,0,0,0,0},

    {"psync",syncCommand,-3,
     "admin no-script",
     0,NULL,0,0,0,0,0,0},

    {"replconf",replconfCommand,-1,
     "admin no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"flushdb",flushdbCommand,-1,
     "write @keyspace @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"flushall",flushallCommand,-1,
     "write @keyspace @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"sort",sortCommand,-2,
     "write use-memory @list @set @sortedset @dangerous",
     0,sortGetKeys,1,1,1,0,0,0},

    {"info",infoCommand,-1,
     "ok-loading ok-stale random @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"monitor",monitorCommand,1,
     "admin no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"ttl",ttlCommand,2,
     "read-only fast random @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"touch",touchCommand,-2,
     "read-only fast @keyspace",
     0,NULL,1,-1,1,0,0,0},

    {"pttl",pttlCommand,2,
     "read-only fast random @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"persist",persistCommand,2,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"slaveof",replicaofCommand,3,
     "admin no-script ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"replicaof",replicaofCommand,3,
     "admin no-script ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"role",roleCommand,1,
     "ok-loading ok-stale no-script fast @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"debug",debugCommand,-2,
     "admin no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"config",configCommand,-2,
     "admin ok-loading ok-stale no-script",
     0,NULL,0,0,0,0,0,0},

    {"subscribe",subscribeCommand,-2,
     "pub-sub no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"unsubscribe",unsubscribeCommand,-1,
     "pub-sub no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"psubscribe",psubscribeCommand,-2,
     "pub-sub no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"punsubscribe",punsubscribeCommand,-1,
     "pub-sub no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"publish",publishCommand,3,
     "pub-sub ok-loading ok-stale fast may-replicate",
     0,NULL,0,0,0,0,0,0},

    {"pubsub",pubsubCommand,-2,
     "pub-sub ok-loading ok-stale random",
     0,NULL,0,0,0,0,0,0},

    {"watch",watchCommand,-2,
     "no-script fast ok-loading ok-stale @transaction",
     0,NULL,1,-1,1,0,0,0},

    {"unwatch",unwatchCommand,1,
     "no-script fast ok-loading ok-stale @transaction",
     0,NULL,0,0,0,0,0,0},

    {"cluster",clusterCommand,-2,
     "admin ok-stale random",
     0,NULL,0,0,0,0,0,0},

    {"restore",restoreCommand,-4,
     "write use-memory @keyspace @dangerous",
     0,NULL,1,1,1,0,0,0},

    {"restore-asking",restoreCommand,-4,
    "write use-memory cluster-asking @keyspace @dangerous",
    0,NULL,1,1,1,0,0,0},

    {"migrate",migrateCommand,-6,
     "write random @keyspace @dangerous",
     0,migrateGetKeys,0,0,0,0,0,0},

    {"asking",askingCommand,1,
     "fast @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"readonly",readonlyCommand,1,
     "fast @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"readwrite",readwriteCommand,1,
     "fast @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"dump",dumpCommand,2,
     "read-only random @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"object",objectCommand,-2,
     "read-only random @keyspace",
     0,NULL,2,2,1,0,0,0},

    {"memory",memoryCommand,-2,
     "random read-only",
     0,memoryGetKeys,0,0,0,0,0,0},

    {"client",clientCommand,-2,
     "admin no-script random ok-loading ok-stale @connection",
     0,NULL,0,0,0,0,0,0},

    {"hello",helloCommand,-1,
     "no-auth no-script fast ok-loading ok-stale @connection",
     0,NULL,0,0,0,0,0,0},

    /* EVAL can modify the dataset, however it is not flagged as a write
     * command since we do the check while running commands from Lua.
     * 
     * EVAL and EVALSHA also feed monitors before the commands are executed,
     * as opposed to after.
      */
    {"eval",evalCommand,-3,
     "no-script no-monitor may-replicate @scripting",
     0,evalGetKeys,0,0,0,0,0,0},

    {"evalsha",evalShaCommand,-3,
     "no-script no-monitor may-replicate @scripting",
     0,evalGetKeys,0,0,0,0,0,0},

    {"slowlog",slowlogCommand,-2,
     "admin random ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"script",scriptCommand,-2,
     "no-script may-replicate @scripting",
     0,NULL,0,0,0,0,0,0},

    {"time",timeCommand,1,
     "random fast ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"bitop",bitopCommand,-4,
     "write use-memory @bitmap",
     0,NULL,2,-1,1,0,0,0},

    {"bitcount",bitcountCommand,-2,
     "read-only @bitmap",
     0,NULL,1,1,1,0,0,0},

    {"bitpos",bitposCommand,-3,
     "read-only @bitmap",
     0,NULL,1,1,1,0,0,0},

    {"wait",waitCommand,3,
     "no-script @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"command",commandCommand,-1,
     "ok-loading ok-stale random @connection",
     0,NULL,0,0,0,0,0,0},

    {"geoadd",geoaddCommand,-5,
     "write use-memory @geo",
     0,NULL,1,1,1,0,0,0},

    /* GEORADIUS has store options that may write. */
    {"georadius",georadiusCommand,-6,
     "write use-memory @geo",
     0,georadiusGetKeys,1,1,1,0,0,0},

    {"georadius_ro",georadiusroCommand,-6,
     "read-only @geo",
     0,NULL,1,1,1,0,0,0},

    {"georadiusbymember",georadiusbymemberCommand,-5,
     "write use-memory @geo",
     0,georadiusGetKeys,1,1,1,0,0,0},

    {"georadiusbymember_ro",georadiusbymemberroCommand,-5,
     "read-only @geo",
     0,NULL,1,1,1,0,0,0},

    {"geohash",geohashCommand,-2,
     "read-only @geo",
     0,NULL,1,1,1,0,0,0},

    {"geopos",geoposCommand,-2,
     "read-only @geo",
     0,NULL,1,1,1,0,0,0},

    {"geodist",geodistCommand,-4,
     "read-only @geo",
     0,NULL,1,1,1,0,0,0},

    {"geosearch",geosearchCommand,-7,
     "read-only @geo",
      0,NULL,1,1,1,0,0,0},

    {"geosearchstore",geosearchstoreCommand,-8,
     "write use-memory @geo",
      0,NULL,1,2,1,0,0,0},

    {"pfselftest",pfselftestCommand,1,
     "admin @hyperloglog",
      0,NULL,0,0,0,0,0,0},

    {"pfadd",pfaddCommand,-2,
     "write use-memory fast @hyperloglog",
     0,NULL,1,1,1,0,0,0},

    /* Technically speaking PFCOUNT may change the key since it changes the
     * final bytes in the HyperLogLog representation. However in this case
     * we claim that the representation, even if accessible, is an internal
     * affair, and the command is semantically read only. */
    {"pfcount",pfcountCommand,-2,
     "read-only may-replicate @hyperloglog",
     0,NULL,1,-1,1,0,0,0},

    {"pfmerge",pfmergeCommand,-2,
     "write use-memory @hyperloglog",
     0,NULL,1,-1,1,0,0,0},

    /* Unlike PFCOUNT that is considered as a read-only command (although
     * it changes a bit), PFDEBUG may change the entire key when converting
     * from sparse to dense representation */
    {"pfdebug",pfdebugCommand,-3,
     "admin write use-memory @hyperloglog",
     0,NULL,2,2,1,0,0,0},

    {"xadd",xaddCommand,-5,
     "write use-memory fast random @stream",
     0,NULL,1,1,1,0,0,0},

    {"xrange",xrangeCommand,-4,
     "read-only @stream",
     0,NULL,1,1,1,0,0,0},

    {"xrevrange",xrevrangeCommand,-4,
     "read-only @stream",
     0,NULL,1,1,1,0,0,0},

    {"xlen",xlenCommand,2,
     "read-only fast @stream",
     0,NULL,1,1,1,0,0,0},

    {"xread",xreadCommand,-4,
     "read-only @stream @blocking",
     0,xreadGetKeys,0,0,0,0,0,0},

    {"xreadgroup",xreadCommand,-7,
     "write @stream @blocking",
     0,xreadGetKeys,0,0,0,0,0,0},

    {"xgroup",xgroupCommand,-2,
     "write use-memory @stream",
     0,NULL,2,2,1,0,0,0},

    {"xsetid",xsetidCommand,3,
     "write use-memory fast @stream",
     0,NULL,1,1,1,0,0,0},

    {"xack",xackCommand,-4,
     "write fast random @stream",
     0,NULL,1,1,1,0,0,0},

    {"xpending",xpendingCommand,-3,
     "read-only random @stream",
     0,NULL,1,1,1,0,0,0},

    {"xclaim",xclaimCommand,-6,
     "write random fast @stream",
     0,NULL,1,1,1,0,0,0},

    {"xautoclaim",xautoclaimCommand,-6,
     "write random fast @stream",
     0,NULL,1,1,1,0,0,0},

    {"xinfo",xinfoCommand,-2,
     "read-only random @stream",
     0,NULL,2,2,1,0,0,0},

    {"xdel",xdelCommand,-3,
     "write fast @stream",
     0,NULL,1,1,1,0,0,0},

    {"xtrim",xtrimCommand,-4,
     "write random @stream",
     0,NULL,1,1,1,0,0,0},

    {"post",securityWarningCommand,-1,
     "ok-loading ok-stale read-only",
     0,NULL,0,0,0,0,0,0},

    {"host:",securityWarningCommand,-1,
     "ok-loading ok-stale read-only",
     0,NULL,0,0,0,0,0,0},

    {"latency",latencyCommand,-2,
     "admin no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"lolwut",lolwutCommand,-1,
     "read-only fast",
     0,NULL,0,0,0,0,0,0},

    {"acl",aclCommand,-2,
     "admin no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"stralgo",stralgoCommand,-2,
     "read-only @string",
     0,lcsGetKeys,0,0,0,0,0,0},

    {"reset",resetCommand,1,
     "no-script ok-stale ok-loading fast @connection",
     0,NULL,0,0,0,0,0,0},

    {"failover",failoverCommand,-1,
     "admin no-script ok-stale",
     0,NULL,0,0,0,0,0,0}
};

/*============================ Utility functions ============================ */

/* We use a private localtime implementation which is fork-safe. The logging
 * function of Redis may be called from other threads. */
void nolocks_localtime(struct tm *tmp, time_t t, time_t tz, int dst);

/* Low level logging. To use only for very big messages, otherwise
 * serverLog() is to prefer. */
void serverLogRaw(int level, const char *msg) {
    const int syslogLevelMap[] = { LOG_DEBUG, LOG_INFO, LOG_NOTICE, LOG_WARNING };
    const char *c = ".-*#";
    FILE *fp;
    char buf[64];
    int rawmode = (level & LL_RAW);
    int log_to_stdout = server.logfile[0] == '\0';

    level &= 0xff; /* clear flags */
    if (level < server.verbosity) return;

    fp = log_to_stdout ? stdout : fopen(server.logfile,"a");
    if (!fp) return;

    if (rawmode) {
        fprintf(fp,"%s",msg);
    } else {
        int off;
        struct timeval tv;
        int role_char;
        pid_t pid = getpid();

        gettimeofday(&tv,NULL);
        struct tm tm;
        nolocks_localtime(&tm,tv.tv_sec,server.timezone,server.daylight_active);
        off = strftime(buf,sizeof(buf),"%d %b %Y %H:%M:%S.",&tm);
        snprintf(buf+off,sizeof(buf)-off,"%03d",(int)tv.tv_usec/1000);
        if (server.sentinel_mode) {
            role_char = 'X'; /* Sentinel. */
        } else if (pid != server.pid) {
            role_char = 'C'; /* RDB / AOF writing child. */
        } else {
            role_char = (server.masterhost ? 'S':'M'); /* Slave or Master. */
        }
        fprintf(fp,"%d:%c %s %c %s\n",
            (int)getpid(),role_char, buf,c[level],msg);
    }
    fflush(fp);

    if (!log_to_stdout) fclose(fp);
    if (server.syslog_enabled) syslog(syslogLevelMap[level], "%s", msg);
}

/* Like serverLogRaw() but with printf-alike support. This is the function that
 * is used across the code. The raw version is only used in order to dump
 * the INFO output on crash. */
void _serverLog(int level, const char *fmt, ...) {
    va_list ap;
    char msg[LOG_MAX_LEN];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    serverLogRaw(level,msg);
}

/* Log a fixed message without printf-alike capabilities, in a way that is
 * safe to call from a signal handler.
 *
 * We actually use this only for signals that are not fatal from the point
 * of view of Redis. Signals that are going to kill the server anyway and
 * where we need printf-alike features are served by serverLog(). */
void serverLogFromHandler(int level, const char *msg) {
    int fd;
    int log_to_stdout = server.logfile[0] == '\0';
    char buf[64];

    if ((level&0xff) < server.verbosity || (log_to_stdout && server.daemonize))
        return;
    fd = log_to_stdout ? STDOUT_FILENO :
                         open(server.logfile, O_APPEND|O_CREAT|O_WRONLY, 0644);
    if (fd == -1) return;
    ll2string(buf,sizeof(buf),getpid());
    if (write(fd,buf,strlen(buf)) == -1) goto err;
    if (write(fd,":signal-handler (",17) == -1) goto err;
    ll2string(buf,sizeof(buf),time(NULL));
    if (write(fd,buf,strlen(buf)) == -1) goto err;
    if (write(fd,") ",2) == -1) goto err;
    if (write(fd,msg,strlen(msg)) == -1) goto err;
    if (write(fd,"\n",1) == -1) goto err;
err:
    if (!log_to_stdout) close(fd);
}

/* Return the UNIX time in microseconds */
long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

/* Return the UNIX time in milliseconds */
mstime_t mstime(void) {
    return ustime()/1000;
}

/* After an RDB dump or AOF rewrite we exit from children using _exit() instead of
 * exit(), because the latter may interact with the same file objects used by
 * the parent process. However if we are testing the coverage normal exit() is
 * used in order to obtain the right coverage information. */
// 在RDB转储或AOF重写之后，这里使用_exit()而不是exit()从子进程退出，因为后者可能与父进程使用的相同文件对象进行交互。
// 但是，如果我们正在覆盖率测试，则将使用正常的exit()来获取正确的覆盖率信息。
void exitFromChild(int retcode) {
#ifdef COVERAGE_TEST
    exit(retcode);
#else
    _exit(retcode);
#endif
}

/*====================== Hash table type implementation  ==================== */

/* This is a hash table type that uses the SDS dynamic strings library as
 * keys and redis objects as values (objects can hold SDS strings,
 * lists, sets). */

void dictVanillaFree(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    zfree(val);
}

void dictListDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
}

int dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

/* A case insensitive version used for the command lookup table and other
 * places where case insensitive non binary-safe comparison is needed. */
int dictSdsKeyCaseCompare(void *privdata, const void *key1,
        const void *key2)
{
    DICT_NOTUSED(privdata);

    return strcasecmp(key1, key2) == 0;
}

void dictObjectDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Lazy freeing will set value to NULL. */
    decrRefCount(val);
}

void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

int dictObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    const robj *o1 = key1, *o2 = key2;
    return dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
}

uint64_t dictObjHash(const void *key) {
    const robj *o = key;
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

uint64_t dictSdsCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictEncObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    robj *o1 = (robj*) key1, *o2 = (robj*) key2;
    int cmp;

    if (o1->encoding == OBJ_ENCODING_INT &&
        o2->encoding == OBJ_ENCODING_INT)
            return o1->ptr == o2->ptr;

    /* Due to OBJ_STATIC_REFCOUNT, we avoid calling getDecodedObject() without
     * good reasons, because it would incrRefCount() the object, which
     * is invalid. So we check to make sure dictFind() works with static
     * objects as well. */
    if (o1->refcount != OBJ_STATIC_REFCOUNT) o1 = getDecodedObject(o1);
    if (o2->refcount != OBJ_STATIC_REFCOUNT) o2 = getDecodedObject(o2);
    cmp = dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
    if (o1->refcount != OBJ_STATIC_REFCOUNT) decrRefCount(o1);
    if (o2->refcount != OBJ_STATIC_REFCOUNT) decrRefCount(o2);
    return cmp;
}

uint64_t dictEncObjHash(const void *key) {
    robj *o = (robj*) key;

    if (sdsEncodedObject(o)) {
        return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
    } else if (o->encoding == OBJ_ENCODING_INT) {
        char buf[32];
        int len;

        len = ll2string(buf,32,(long)o->ptr);
        return dictGenHashFunction((unsigned char*)buf, len);
    } else {
        serverPanic("Unknown string encoding");
    }
}

/* Return 1 if currently we allow dict to expand. Dict may allocate huge
 * memory to contain hash buckets when dict expands, that may lead redis
 * rejects user's requests or evicts some keys, we can stop dict to expand
 * provisionally if used memory will be over maxmemory after dict expands,
 * but to guarantee the performance of redis, we still allow dict to expand
 * if dict load factor exceeds HASHTABLE_MAX_LOAD_FACTOR. */
int dictExpandAllowed(size_t moreMem, double usedRatio) {
    if (usedRatio <= HASHTABLE_MAX_LOAD_FACTOR) {
        return !overMaxmemoryAfterAlloc(moreMem);
    } else {
        return 1;
    }
}

/* Generic hash table type where keys are Redis Objects, Values
 * dummy pointers. */
dictType objectKeyPointerValueDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictObjectDestructor,      /* key destructor */
    NULL,                      /* val destructor */
    NULL                       /* allow to expand */
};

/* Like objectKeyPointerValueDictType(), but values can be destroyed, if
 * not NULL, calling zfree(). */
dictType objectKeyHeapPointerValueDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictObjectDestructor,      /* key destructor */
    dictVanillaFree,           /* val destructor */
    NULL                       /* allow to expand */
};

/* Set dictionary type. Keys are SDS strings, values are not used. */
dictType setDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,         /* key destructor */
    NULL                       /* val destructor */
};

/* Sorted sets hash (note: a skiplist is used in addition to the hash table) */
dictType zsetDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* Note: SDS string shared & freed by skiplist */
    NULL,                      /* val destructor */
    NULL                       /* allow to expand */
};

/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType dbDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictObjectDestructor,       /* val destructor */
    dictExpandAllowed           /* allow to expand */
};

/* server.lua_scripts sha (as sds string) -> scripts (as robj) cache. */
dictType shaScriptObjectDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictObjectDestructor,       /* val destructor */
    NULL                        /* allow to expand */
};

/* Db->expires */
dictType dbExpiresDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL,                       /* val destructor */
    dictExpandAllowed           /* allow to expand */
};

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
};

/* Hash type hash table (note that small hashes are represented with ziplists) */
dictType hashDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictSdsDestructor,          /* val destructor */
    NULL                        /* allow to expand */
};

/* Dict type without destructor */
dictType sdsReplyDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
};

/* Keylist hash table type has unencoded redis objects as keys and
 * lists as values. It's used for blocking operations (BLPOP) and to
 * map swapped keys to a list of clients waiting for this keys to be loaded. */
dictType keylistDictType = {
    dictObjHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictObjKeyCompare,          /* key compare */
    dictObjectDestructor,       /* key destructor */
    dictListDestructor,         /* val destructor */
    NULL                        /* allow to expand */
};

/* Cluster nodes hash table, mapping nodes addresses 1.2.3.4:6379 to
 * clusterNode structures. */
dictType clusterNodesDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
};

/* Cluster re-addition blacklist. This maps node IDs to the time
 * we can re-add this node. The goal is to avoid readding a removed
 * node for some time. */
dictType clusterNodesBlackListDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
};

/* Modules system dictionary type. Keys are module name,
 * values are pointer to RedisModule struct. */
dictType modulesDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
};

/* Migrate cache dict type. */
dictType migrateCacheDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
};

/* Replication cached script dict (server.repl_scriptcache_dict).
 * Keys are sds SHA1 strings, while values are not used at all in the current
 * implementation. */
dictType replScriptCacheDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
};

int htNeedsResize(dict *dict) {
    long long size, used;

    // 获取当前dict总的slot数，总的存储元素数。
    size = dictSlots(dict);
    used = dictSize(dict);
    // 当ht表大小大于4，且存储元素数量与slot之比小于HASHTABLE_MIN_FILL，则返回true，需要进行resize。
    return (size > DICT_HT_INITIAL_SIZE &&
            (used*100/size < HASHTABLE_MIN_FILL));
}

/* If the percentage of used slots in the HT reaches HASHTABLE_MIN_FILL
 * we resize the hash table to save memory */
// 如果ht中 存储元素数量 与 slot 之比达到HASHTABLE_MIN_FILL，则需要调整哈希表的大小以节省内存。
void tryResizeHashTables(int dbid) {
    if (htNeedsResize(server.db[dbid].dict))
        dictResize(server.db[dbid].dict);
    if (htNeedsResize(server.db[dbid].expires))
        dictResize(server.db[dbid].expires);
}

/* Our hash table implementation performs rehashing incrementally while
 * we write/read from the hash table. Still if the server is idle, the hash
 * table will use two tables for a long time. So we try to use 1 millisecond
 * of CPU time at every call of this function to perform some rehashing.
 *
 * The function returns 1 if some rehashing was performed, otherwise 0
 * is returned. */
// hash表实现了在读写时渐进式rehash，但当redis数据没有访问时，2个表会同时存在很长的时间。
// 所以这里我们尝试在每次调用这个函数的时候，使用1毫秒的CPU时间来执行一些rehash操作。
// 如果有进行rehash处理，则返回1；否则rehash完成，不在rehash状态，返回0。
int incrementallyRehash(int dbid) {
    /* Keys dictionary */
    // 主要数据表的rehash操作。
    if (dictIsRehashing(server.db[dbid].dict)) {
        dictRehashMilliseconds(server.db[dbid].dict,1);
        return 1; /* already used our millisecond for this loop... */
    }
    /* Expires */
    // 过期时间表的rehash操作。
    if (dictIsRehashing(server.db[dbid].expires)) {
        dictRehashMilliseconds(server.db[dbid].expires,1);
        return 1; /* already used our millisecond for this loop... */
    }
    return 0;
}

/* This function is called once a background process of some kind terminates,
 * as we want to avoid resizing the hash tables when there is a child in order
 * to play well with copy-on-write (otherwise when a resize happens lots of
 * memory pages are copied). The goal of this function is to update the ability
 * for dict.c to resize the hash tables accordingly to the fact we have an
 * active fork child running. */
// 一旦有某种类型后台进程启动或结束时，我们会调用这个函数。因为当有一个copy-on-write的子进程在执行时，我们不希望进行hash表的resize。
// 因为父子进程共享内存页表，当发生大量内存数据修改时，需要进行拷贝修改，这是一种灾难。
// 这个函数根据我们当前是否有活跃子进程，来决定是否禁止/启用hash表的resize。
void updateDictResizePolicy(void) {
    if (!hasActiveChildProcess())
        dictEnableResize();
    else
        dictDisableResize();
}

const char *strChildType(int type) {
    switch(type) {
        case CHILD_TYPE_RDB: return "RDB";
        case CHILD_TYPE_AOF: return "AOF";
        case CHILD_TYPE_LDB: return "LDB";
        case CHILD_TYPE_MODULE: return "MODULE";
        default: return "Unknown";
    }
}

/* Return true if there are active children processes doing RDB saving,
 * AOF rewriting, or some side process spawned by a loaded module. */
// 如果当前存在活跃的子进程则返回true。子进程可以是进行RDB存储，AOF重写或加载的模块产生的某些辅助进程。
int hasActiveChildProcess() {
    return server.child_pid != -1;
}

void resetChildState() {
    server.child_type = CHILD_TYPE_NONE;
    server.child_pid = -1;
    server.stat_current_cow_bytes = 0;
    server.stat_current_cow_updated = 0;
    server.stat_current_save_keys_processed = 0;
    server.stat_module_progress = 0;
    server.stat_current_save_keys_total = 0;
    updateDictResizePolicy();
    closeChildInfoPipe();
    moduleFireServerEvent(REDISMODULE_EVENT_FORK_CHILD,
                          REDISMODULE_SUBEVENT_FORK_CHILD_DIED,
                          NULL);
}

/* Return if child type is mutual exclusive with other fork children */
// 这里这几类子进程是与其他子进程互斥的。他们执行时不能同时还有其他子进程在执行。
int isMutuallyExclusiveChildType(int type) {
    return type == CHILD_TYPE_RDB || type == CHILD_TYPE_AOF || type == CHILD_TYPE_MODULE;
}

/* Return true if this instance has persistence completely turned off:
 * both RDB and AOF are disabled. */
// 如果当前实例完全关闭了持久化，即RDB和AOF都禁用状态，函数返回true。
int allPersistenceDisabled(void) {
    return server.saveparamslen == 0 && server.aof_state == AOF_OFF;
}

/* ======================= Cron: called every 100 ms ======================== */

/* Add a sample to the operations per second array of samples. */
void trackInstantaneousMetric(int metric, long long current_reading) {
    long long now = mstime();
    long long t = now - server.inst_metric[metric].last_sample_time;
    long long ops = current_reading -
                    server.inst_metric[metric].last_sample_count;
    long long ops_sec;

    ops_sec = t > 0 ? (ops*1000/t) : 0;

    server.inst_metric[metric].samples[server.inst_metric[metric].idx] =
        ops_sec;
    server.inst_metric[metric].idx++;
    server.inst_metric[metric].idx %= STATS_METRIC_SAMPLES;
    server.inst_metric[metric].last_sample_time = now;
    server.inst_metric[metric].last_sample_count = current_reading;
}

/* Return the mean of all the samples. */
long long getInstantaneousMetric(int metric) {
    int j;
    long long sum = 0;

    for (j = 0; j < STATS_METRIC_SAMPLES; j++)
        sum += server.inst_metric[metric].samples[j];
    return sum / STATS_METRIC_SAMPLES;
}

/* The client query buffer is an sds.c string that can end with a lot of
 * free space not used, this function reclaims space if needed.
 *
 * The function always returns 0 as it never terminates the client. */
// client的查询buffer是sds字符串，因为处理查询期间可能会扩容，所以需要定时检查是否有过多的剩余空间，在这里进行回收。
// 该函数永远返回0，因为不会释放client。
int clientsCronResizeQueryBuffer(client *c) {
    // 获取当前查询buffer总共占用的空间。当前client的空闲持续时间。
    size_t querybuf_size = sdsAllocSize(c->querybuf);
    time_t idletime = server.unixtime - c->lastinteraction;

    /* There are two conditions to resize the query buffer:
     * 1) Query buffer is > BIG_ARG and too big for latest peak.
     * 2) Query buffer is > BIG_ARG and client is idle. */
    // 下面两种条件满足任意一种时，我们会resize当前client的查询buf：
    // 1、查询buf > BIG_ARG（32k），并且大概超出最近使用峰值的2倍。
    // 2、查询buf > BIG_ARG（32k），并且client空闲时间超过2s。
    if (querybuf_size > PROTO_MBULK_BIG_ARG &&
         ((querybuf_size/(c->querybuf_peak+1)) > 2 ||
          idletime > 2))
    {
        /* Only resize the query buffer if it is actually wasting
         * at least a few kbytes. */
        // 如果空闲空间过小时，不做处理。只有在空闲空间大于4K时，我们才释放掉空闲空间。
        if (sdsavail(c->querybuf) > 1024*4) {
            c->querybuf = sdsRemoveFreeSpace(c->querybuf);
        }
    }
    /* Reset the peak again to capture the peak memory usage in the next
     * cycle. */
    // 当前检查了查询buffer的容量后，置空最近使用峰值。等待下一轮的峰值记录，以及buf使用存储检测。
    c->querybuf_peak = 0;

    /* Clients representing masters also use a "pending query buffer" that
     * is the yet not applied part of the stream we are reading. Such buffer
     * also needs resizing from time to time, otherwise after a very large
     * transfer (a huge value or a big MIGRATE operation) it will keep using
     * a lot of memory. */
    // 对于master的client连接，会有一个pending_querybuf保存已接收但还未执行的复制流。
    // 这个buf也需要定期resize处理，否则在一个大传输（大的值或大的MIGRATE操作）之后，缓存将占有很大但却不使用的空间。
    if (c->flags & CLIENT_MASTER) {
        /* There are two conditions to resize the pending query buffer:
         * 1) Pending Query buffer is > LIMIT_PENDING_QUERYBUF.
         * 2) Used length is smaller than pending_querybuf_size/2 */
        // 下面两种条件同时满足时，我们才会resize pending_querybuf：
        // 1、pending_querybuf > LIMIT_PENDING_QUERYBUF（4M）。
        // 2、pending_querybuf中使用的大小 < pending_querybuf_size/2时。
        size_t pending_querybuf_size = sdsAllocSize(c->pending_querybuf);
        if(pending_querybuf_size > LIMIT_PENDING_QUERYBUF &&
           sdslen(c->pending_querybuf) < (pending_querybuf_size/2))
        {
            c->pending_querybuf = sdsRemoveFreeSpace(c->pending_querybuf);
        }
    }
    return 0;
}

/* This function is used in order to track clients using the biggest amount
 * of memory in the latest few seconds. This way we can provide such information
 * in the INFO output (clients section), without having to do an O(N) scan for
 * all the clients.
 *
 * This is how it works. We have an array of CLIENTS_PEAK_MEM_USAGE_SLOTS slots
 * where we track, for each, the biggest client output and input buffers we
 * saw in that slot. Every slot correspond to one of the latest seconds, since
 * the array is indexed by doing UNIXTIME % CLIENTS_PEAK_MEM_USAGE_SLOTS.
 *
 * When we want to know what was recently the peak memory usage, we just scan
 * such few slots searching for the maximum value. */
// 此函数用于跟踪最近几秒钟内单个client使用的最大内存量。这样我们可以在INFO（客户端部分）中直接输出此信息，无需对所有client进行O(N)扫描。
// 我们有一个CLIENTS_PEAK_MEM_USAGE_SLOTS个slot的数组，每个slot存储该时间点我们采样到的clients中，使用输入输出缓存最多是多少。
// 每个slot对应1s，因为我们是计算 UNIXTIME%CLIENTS_PEAK_MEM_USAGE_SLOTS 作为索引，这里一共会保存最近的8s中。
// 当我们想知道最近单个client使用的内存峰值，直接在数组中找最大值就好了。
#define CLIENTS_PEAK_MEM_USAGE_SLOTS 8
size_t ClientsPeakMemInput[CLIENTS_PEAK_MEM_USAGE_SLOTS] = {0};
size_t ClientsPeakMemOutput[CLIENTS_PEAK_MEM_USAGE_SLOTS] = {0};

int clientsCronTrackExpansiveClients(client *c, int time_idx) {
    // 查询缓存使用内存为：querybuf空间 + 命令解析后的参数使用空间。
    size_t in_usage = sdsZmallocSize(c->querybuf) + c->argv_len_sum +
	              (c->argv ? zmalloc_size(c->argv) : 0);
    // 回复缓存使用内存：回复buf空间 + 回复reply列表空间。
    size_t out_usage = getClientOutputBufferMemoryUsage(c);

    /* Track the biggest values observed so far in this slot. */
    // 追踪最大内存使用，比较看是否需要更新最大内存使用值。
    if (in_usage > ClientsPeakMemInput[time_idx]) ClientsPeakMemInput[time_idx] = in_usage;
    if (out_usage > ClientsPeakMemOutput[time_idx]) ClientsPeakMemOutput[time_idx] = out_usage;

    return 0; /* This function never terminates the client. */
}

/* Iterating all the clients in getMemoryOverheadData() is too slow and
 * in turn would make the INFO command too slow. So we perform this
 * computation incrementally and track the (not instantaneous but updated
 * to the second) total memory used by clients using clinetsCron() in
 * a more incremental way (depending on server.hz). */
// 在getMemoryOverheadData()中迭代所有客户端来获取总使用的存储很慢，从而会使INFO命令太慢。
// 所以在这里我们定期（取决于server.hz）使用clinetsCron()渐进式的累计计算所有clients使用的总内存（统计数据不是统一时刻的，是1s内的数据）。
int clientsCronTrackClientsMemUsage(client *c) {
    size_t mem = 0;
    // 获取client类型
    int type = getClientType(c);
    // 累计该client所使用的总的内存，包括查询buf，output buf，命令参数占用内存等。
    mem += getClientOutputBufferMemoryUsage(c);
    mem += sdsZmallocSize(c->querybuf);
    mem += zmalloc_size(c);
    mem += c->argv_len_sum;
    if (c->argv) mem += zmalloc_size(c->argv);
    /* Now that we have the memory used by the client, remove the old
     * value from the old category, and add it back. */
    // 注意这里我们计算到了该client新的内存使用，需要先减去该client老的使用空间大小，然后在加上新统计的大小。
    server.stat_clients_type_memory[c->client_cron_last_memory_type] -=
        c->client_cron_last_memory_usage;
    server.stat_clients_type_memory[type] += mem;
    /* Remember what we added and where, to remove it next time. */
    // 记录上一次统计该client时的类型以及使用空间大小，用于在client释放或者下一次更新内存时从总数据中减去该值。
    c->client_cron_last_memory_usage = mem;
    c->client_cron_last_memory_type = type;
    return 0;
}

/* Return the max samples in the memory usage of clients tracked by
 * the function clientsCronTrackExpansiveClients(). */
void getExpansiveClientsInfo(size_t *in_usage, size_t *out_usage) {
    size_t i = 0, o = 0;
    for (int j = 0; j < CLIENTS_PEAK_MEM_USAGE_SLOTS; j++) {
        if (ClientsPeakMemInput[j] > i) i = ClientsPeakMemInput[j];
        if (ClientsPeakMemOutput[j] > o) o = ClientsPeakMemOutput[j];
    }
    *in_usage = i;
    *out_usage = o;
}

/* This function is called by serverCron() and is used in order to perform
 * operations on clients that are important to perform constantly. For instance
 * we use this function in order to disconnect clients after a timeout, including
 * clients blocked in some blocking command with a non-zero timeout.
 *
 * The function makes some effort to process all the clients every second, even
 * if this cannot be strictly guaranteed, since serverCron() may be called with
 * an actual frequency lower than server.hz in case of latency events like slow
 * commands.
 *
 * It is very important for this function, and the functions it calls, to be
 * very fast: sometimes Redis has tens of hundreds of connected clients, and the
 * default server.hz value is 10, so sometimes here we need to process thousands
 * of clients per second, turning this function into a source of latency.
 */
// 这个函数由serverCron()调用，用于定期对clients做一些重要操作。例如处理client超时断开连接，包括连接超时，阻塞超时等。
// 这个函数会努力每秒处理所有的clients。但不能严格保证，因为当出现延时很大的事件如慢命令执行时，serverCron()执行频率可能低于server.hz。
// 这个函数以及它调用的函数要求执行的非常快，因为一些redis有成百上千连接的clients，而默认server.hz为10，
// 所以有时候这里我们每秒要处理几千的clients，函数执行不够快的话，就会导致延时上升。
#define CLIENTS_CRON_MIN_ITERATIONS 5
void clientsCron(void) {
    /* Try to process at least numclients/server.hz of clients
     * per call. Since normally (if there are no big latency events) this
     * function is called server.hz times per second, in the average case we
     * process all the clients in 1 second. */
    // 尝试每次处理至少numclients/server.hz个clients，因为通常处理事件没有大的延时的话，该函数每秒调用server.hz次，一秒就可以处理所有clients了。
    int numclients = listLength(server.clients);
    int iterations = numclients/server.hz;
    mstime_t now = mstime();

    /* Process at least a few clients while we are at it, even if we need
     * to process less than CLIENTS_CRON_MIN_ITERATIONS to meet our contract
     * of processing each client once per second. */
    // 处理迭代次数。
    if (iterations < CLIENTS_CRON_MIN_ITERATIONS)
        iterations = (numclients < CLIENTS_CRON_MIN_ITERATIONS) ?
                     numclients : CLIENTS_CRON_MIN_ITERATIONS;


    // client最大内存使用量统计使用，根据当前时间计算对应数组的slot。
    int curr_peak_mem_usage_slot = server.unixtime % CLIENTS_PEAK_MEM_USAGE_SLOTS;
    /* Always zero the next sample, so that when we switch to that second, we'll
     * only register samples that are greater in that second without considering
     * the history of such slot.
     *
     * Note: our index may jump to any random position if serverCron() is not
     * called for some reason with the normal frequency, for instance because
     * some slow command is called taking multiple seconds to execute. In that
     * case our array may end containing data which is potentially older
     * than CLIENTS_PEAK_MEM_USAGE_SLOTS seconds: however this is not a problem
     * since here we want just to track if "recently" there were very expansive
     * clients from the POV of memory usage. */
    // 我们总是把下一秒钟的slot数组值设置为0，这样在处理下一秒的统计时，只需直接更新为更大的值，而不用管其中的值是当前秒数据还是之前的老数据。
    // 注意：当serverCron()因为一些原因不是以正常的频率执行时，我们的index可能跳到任何位置，例如慢命令执行了好几秒，那这里就会跳过几个slot。
    // 这样我们数组里可能就包含有上一轮或更之前的老数据，不过没有问题，因为我们也只是在内存使用视角来追踪最近是否有用量很大的clients。
    int zeroidx = (curr_peak_mem_usage_slot+1) % CLIENTS_PEAK_MEM_USAGE_SLOTS;
    ClientsPeakMemInput[zeroidx] = 0;
    ClientsPeakMemOutput[zeroidx] = 0;


    // 遍历server.clients，直到列表为空，或者达到迭代次数则退出。
    while(listLength(server.clients) && iterations--) {
        client *c;
        listNode *head;

        /* Rotate the list, take the current head, process.
         * This way if the client must be removed from the list it's the
         * first element and we don't incur into O(N) computation. */
        // 实际上释放client从server.clients删除现在是这样：listDelNode(server.clients,c->client_list_node)
        // 我们有一个指针指向clients中我们自己，不再需要先listSearchKey查该client位置定位后再删除了。所以删除处理始终是O(1)。
        // 另外这里tail元素移到队首，相当于我们处理所有clients从后往前遍历，1s内执行多次时不需要额外保存处理位置供下次接着处理。
        listRotateTailToHead(server.clients);
        head = listFirst(server.clients);
        c = listNodeValue(head);
        /* The following functions do different service checks on the client.
         * The protocol is that they return non-zero if the client was
         * terminated. */
        // 依次对client进行处理。如果期间有释放client，则返回非0，从而跳过后面的处理。

        // 处理client交互超时，检测集群中blocked client的是否需要重定向。
        // blocked client的超时检测在beforeSleep中的 handleBlockedClientsTimeout() 处理。
        if (clientsCronHandleTimeout(c,now)) continue;
        // 处理client的查询buffer resize。
        if (clientsCronResizeQueryBuffer(c)) continue;
        // 追踪最近一段时间单个client使用的最多输入/输出缓存量。
        if (clientsCronTrackExpansiveClients(c, curr_peak_mem_usage_slot)) continue;
        // 追踪当前所有client使用内存的总量。
        if (clientsCronTrackClientsMemUsage(c)) continue;
        // 判断该client使用的output buf是否达到软硬性限制，如果达到了，则释放掉。
        if (closeClientOnOutputBufferLimitReached(c, 0)) continue;
    }
}

/* This function handles 'background' operations we are required to do
 * incrementally in Redis databases, such as active key expiring, resizing,
 * rehashing. */
// 这个函数处理Redis db的'后台'相关渐进式操作，如key过期处理，db的resize、rehash操作。
void databasesCron(void) {
    /* Expire keys by random sampling. Not required for slaves
     * as master will synthesize DELs for us. */
    // keys过期随机抽样进行处理。正常主要是masters处理，因为slaves会通过master传递DELs命令来删除master过期key。
    // 对于可写的slave，我们需要调用特定的方法来处理slave自有的keys过期。
    if (server.active_expire_enabled) {
        if (iAmMaster()) {
            activeExpireCycle(ACTIVE_EXPIRE_CYCLE_SLOW);
        } else {
            expireSlaveKeys();
        }
    }

    /* Defrag keys gradually. */
    // 渐进的对keys进行碎片整理。目前还没有实现。
    activeDefragCycle();

    /* Perform hash tables rehashing if needed, but only if there are no
     * other processes saving the DB on disk. Otherwise rehashing is bad
     * as will cause a lot of copy-on-write of memory pages. */
    // 处理resize、rehash操作。
    // 如果当前有fork进程在处理RDB save或AOF重写，则不做resize处理，否则会产生很多copy-on-write内存页。
    if (!hasActiveChildProcess()) {
        /* We use global counters so if we stop the computation at a given
         * DB we'll be able to start from the successive in the next
         * cron loop iteration. */
        // 使用全局计数器，这样当我们在指定的DB处停止计算，后面在下一个cron循环迭代中能接着处理。
        static unsigned int resize_db = 0;
        static unsigned int rehash_db = 0;
        int dbs_per_call = CRON_DBS_PER_CALL;
        int j;

        /* Don't test more DBs than we have. */
        // 设置一次检查的db数，如果<=16则检查所有，否则最多只会检查16个。
        if (dbs_per_call > server.dbnum) dbs_per_call = server.dbnum;

        /* Resize */
        // 尝试进行resize处理。会根据使用情况，来判断是否需要resize。
        for (j = 0; j < dbs_per_call; j++) {
            tryResizeHashTables(resize_db % server.dbnum);
            resize_db++;
        }

        /* Rehash */
        // 如果配置了server.activerehashing，表示在serverCron()中执行rehash，这里进行处理。
        if (server.activerehashing) {
            for (j = 0; j < dbs_per_call; j++) {
                // 循环检查是否有在进行rehash操作的db，有则处理rehash。
                int work_done = incrementallyRehash(rehash_db);
                if (work_done) {
                    /* If the function did some work, stop here, we'll do
                     * more at the next cron loop. */
                    // 一次定时任务处理不能执行太长时间，否则会影响redis正常缓存服务的。
                    // 这里只处理1ms的时间，如果有进行rehash操作，则当前先跳出，等下一次接着再从rehash_db继续进行rehash。
                    break;
                } else {
                    /* If this db didn't need rehash, we'll try the next one. */
                    // 如果当前db不需要进行rehas，则继续检查下一个db。
                    rehash_db++;
                    rehash_db %= server.dbnum;
                }
            }
        }
    }
}

/* We take a cached value of the unix time in the global state because with
 * virtual memory and aging there is to store the current time in objects at
 * every object access, and accuracy is not needed. To access a global var is
 * a lot faster than calling time(NULL).
 * 我们需要记录每次访问对象的时间用于虚拟内存的淘汰，记录时间不需要多么精确。
 * 所以全局状态缓存unix time。访问全局变量显然比系统调用time(NULL)快得多。
 *
 * This function should be fast because it is called at every command execution
 * in call(), so it is possible to decide if to update the daylight saving
 * info or not using the 'update_daylight_info' argument. Normally we update
 * such info only when calling this function from serverCron() but not when
 * calling it from call(). */
// 这个函数应该执行的足够快，因为每次命令执行时都会在call()中调用这个函数，所以我们使用参数update_daylight_info来决定是否更新夏令时信息。
// 通常我们仅在serverCron()中调用这个函数时，更新夏令时信息，而不是在执行命令call()中调用时来更新。
void updateCachedTime(int update_daylight_info) {
    server.ustime = ustime();
    server.mstime = server.ustime / 1000;
    time_t unixtime = server.mstime / 1000;
    atomicSet(server.unixtime, unixtime);

    /* To get information about daylight saving time, we need to call
     * localtime_r and cache the result. However calling localtime_r in this
     * context is safe since we will never fork() while here, in the main
     * thread. The logging function will call a thread safe version of
     * localtime that has no locks. */
    // 为了获取夏令时信息，我们需要调用localtime_r，并缓存结果。
    // 在这里调用localtime_r是安全的，因为主线程处理时，我们不会在这个时候fork()进程。
    if (update_daylight_info) {
        struct tm tm;
        time_t ut = server.unixtime;
        localtime_r(&ut,&tm);
        server.daylight_active = tm.tm_isdst;
    }
}

void checkChildrenDone(void) {
    int statloc = 0;
    pid_t pid;

    // 等待子进程退出。第一个参数-1表示等待任何一个子进程退出。第3个参数选项WNOHANG表示不阻塞，子进程状态没改变时立即返回0。
    if ((pid = waitpid(-1, &statloc, WNOHANG)) != 0) {
        // 结果!=0表示，要么失败为-1，要么子进程退出了返回子进程id。
        int exitcode = WIFEXITED(statloc) ? WEXITSTATUS(statloc) : -1;
        int bysignal = 0;

        if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);

        /* sigKillChildHandler catches the signal and calls exit(), but we
         * must make sure not to flag lastbgsave_status, etc incorrectly.
         * We could directly terminate the child process via SIGUSR1
         * without handling it */
        // sigKillChildHandler会捕获信号并调用exit()，但我们必须确保不会错误地标记lastbgsave_status等状态。
        // 我们可以通过SIGUSR1直接终止子进程而不用处理它。
        if (exitcode == SERVER_CHILD_NOERROR_RETVAL) {
            bysignal = SIGUSR1;
            exitcode = 1;
        }

        if (pid == -1) {
            // 返回-1表示出错了，打印错误信息
            serverLog(LL_WARNING,"waitpid() returned an error: %s. "
                "child_type: %s, child_pid = %d",
                strerror(errno),
                strChildType(server.child_type),
                (int) server.child_pid);
        } else if (pid == server.child_pid) {
            // pid是我们关注的子进程，则需要进行处理子进程结束的后续工作
            if (server.child_type == CHILD_TYPE_RDB) {
                backgroundSaveDoneHandler(exitcode, bysignal);
            } else if (server.child_type == CHILD_TYPE_AOF) {
                backgroundRewriteDoneHandler(exitcode, bysignal);
            } else if (server.child_type == CHILD_TYPE_MODULE) {
                ModuleForkDoneHandler(exitcode, bysignal);
            } else {
                serverPanic("Unknown child type %d for child pid %d", server.child_type, server.child_pid);
                exit(1);
            }
            if (!bysignal && exitcode == 0) receiveChildInfo();
            // 重置子进程状态（child_pid=-1）
            resetChildState();
        } else {
            // 结束的是用于调试的子进程
            if (!ldbRemoveChild(pid)) {
                serverLog(LL_WARNING,
                          "Warning, detected child with unmatched pid: %ld",
                          (long) pid);
            }
        }

        /* start any pending forks immediately. */
        // 检查有进程结束，这里如果有等待fork处理的任务，直接处理。
        replicationStartPendingFork();
    }
}

/* Called from serverCron and loadingCron to update cached memory metrics. */
void cronUpdateMemoryStats() {
    /* Record the max memory used since the server was started. */
    if (zmalloc_used_memory() > server.stat_peak_memory)
        server.stat_peak_memory = zmalloc_used_memory();

    run_with_period(100) {
        /* Sample the RSS and other metrics here since this is a relatively slow call.
         * We must sample the zmalloc_used at the same time we take the rss, otherwise
         * the frag ratio calculate may be off (ratio of two samples at different times) */
        server.cron_malloc_stats.process_rss = zmalloc_get_rss();
        server.cron_malloc_stats.zmalloc_used = zmalloc_used_memory();
        /* Sampling the allocator info can be slow too.
         * The fragmentation ratio it'll show is potentially more accurate
         * it excludes other RSS pages such as: shared libraries, LUA and other non-zmalloc
         * allocations, and allocator reserved pages that can be pursed (all not actual frag) */
        zmalloc_get_allocator_info(&server.cron_malloc_stats.allocator_allocated,
                                   &server.cron_malloc_stats.allocator_active,
                                   &server.cron_malloc_stats.allocator_resident);
        /* in case the allocator isn't providing these stats, fake them so that
         * fragmentation info still shows some (inaccurate metrics) */
        if (!server.cron_malloc_stats.allocator_resident) {
            /* LUA memory isn't part of zmalloc_used, but it is part of the process RSS,
             * so we must deduct it in order to be able to calculate correct
             * "allocator fragmentation" ratio */
            size_t lua_memory = lua_gc(server.lua,LUA_GCCOUNT,0)*1024LL;
            server.cron_malloc_stats.allocator_resident = server.cron_malloc_stats.process_rss - lua_memory;
        }
        if (!server.cron_malloc_stats.allocator_active)
            server.cron_malloc_stats.allocator_active = server.cron_malloc_stats.allocator_resident;
        if (!server.cron_malloc_stats.allocator_allocated)
            server.cron_malloc_stats.allocator_allocated = server.cron_malloc_stats.zmalloc_used;
    }
}

/* This is our timer interrupt, called server.hz times per second.
 * Here is where we do a number of things that need to be done asynchronously.
 * For instance:
 *
 * - Active expired keys collection (it is also performed in a lazy way on
 *   lookup).
 * - Software watchdog.
 * - Update some statistic.
 * - Incremental rehashing of the DBs hash tables.
 * - Triggering BGSAVE / AOF rewrite, and handling of terminated children.
 * - Clients timeout of different kinds.
 * - Replication reconnection.
 * - Many more...
 *
 * Everything directly called here will be called server.hz times per second,
 * so in order to throttle execution of things we want to do less frequently
 * a macro is used: run_with_period(milliseconds) { .... }
 */

// 服务event loop中的定时任务处理函数。每隔1000/server.hz ms执行一次，即每秒执行server.hz次。
// 我们在这个函数中会处理一系列的异步任务：
//  1、执行过期keys处理，SLOW模式，这里执行也是懒删除处理。databasesCron()中处理。
//  2、Software watchdog处理，接收处理信号。
//  3、更新一些统计信息。
//  4、各DBs中hash表的渐进式rehash处理。
//  5、触发BGSAVE/AOF重写，检查结束的子进程并处理。
//  6、各种client超时处理。
//  7、slaves的重新连接处理。
//  等等...
// 这里所有直接调用的都将每秒调用server.hz次。所以为了限制某些函数的执行频率，我们使用宏来处理：
//  run_with_period(milliseconds) { .... }  表示每多少毫秒执行一次。
int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    int j;
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    /* Software watchdog: deliver the SIGALRM that will reach the signal
     * handler if we don't return here fast enough. */
    if (server.watchdog_period) watchdogScheduleSignal(server.watchdog_period);

    /* Update the time cache. */
    // 更新我们服务缓存的时间
    updateCachedTime(1);

    server.hz = server.config_hz;
    /* Adapt the server.hz value to the number of configured clients. If we have
     * many clients, we want to call serverCron() with an higher frequency. */
    // 根据连接的clients数量，动态调整server.hz值。当我们有很多client连接时，我们想要执行serverCron()更频繁，这里可以进行动态调整。
    if (server.dynamic_hz) {
        while (listLength(server.clients) / server.hz >
               MAX_CLIENTS_PER_CLOCK_TICK)
        {
            server.hz *= 2;
            if (server.hz > CONFIG_MAX_HZ) {
                server.hz = CONFIG_MAX_HZ;
                break;
            }
        }
    }

    // 每100ms更新以下数据的统计信息。
    run_with_period(100) {
        long long stat_net_input_bytes, stat_net_output_bytes;
        atomicGet(server.stat_net_input_bytes, stat_net_input_bytes);
        atomicGet(server.stat_net_output_bytes, stat_net_output_bytes);

        trackInstantaneousMetric(STATS_METRIC_COMMAND,server.stat_numcommands);
        trackInstantaneousMetric(STATS_METRIC_NET_INPUT,
                stat_net_input_bytes);
        trackInstantaneousMetric(STATS_METRIC_NET_OUTPUT,
                stat_net_output_bytes);
    }

    /* We have just LRU_BITS bits per object for LRU information.
     * So we use an (eventually wrapping) LRU clock.
     *
     * Note that even if the counter wraps it's not a big problem,
     * everything will still work but some object will appear younger
     * to Redis. However for this to happen a given object should never be
     * touched for all the time needed to the counter to wrap, which is
     * not likely.
     *
     * Note that you can change the resolution altering the
     * LRU_CLOCK_RESOLUTION define. */
    // 如果使用LRU策略，每个对象都有24bit用来存储LRU信息，这里我们使用包转的LRU时钟来作为对象的LRU信息。
    // 注意这里即使使用计数器形式包装也不是什么大问题，不影响正常工作。
    // 虽然当计数转了一个轮回（超出24bit数字），可能部分更老的数据显得更新了，但这种情况需要整个轮回期间数据都没有被访问过，显然这不太可能。
    // 注意我们可以通过改变LRU_CLOCK_RESOLUTION定义，来改变resolution。

    // 这里是获取每1000/server.hz ms，获取一下LRU数字保存起来，从而不用每次操作数据对象时都通过当前时间再计算。
    unsigned int lruclock = getLRUClock();
    atomicSet(server.lruclock,lruclock);

    // 定时更新系统内存使用情况
    cronUpdateMemoryStats();

    /* We received a SIGTERM, shutting down here in a safe way, as it is
     * not ok doing so inside the signal handler. */
    // 收到了一个SIGTERM，以安全的方式在这里处理关闭，因为在信号处理程序中不安全。
    if (server.shutdown_asap) {
        if (prepareForShutdown(SHUTDOWN_NOFLAGS) == C_OK) exit(0);
        serverLog(LL_WARNING,"SIGTERM received but errors trying to shut down the server, check the logs for more information");
        server.shutdown_asap = 0;
    }

    /* Show some info about non-empty databases */
    // 打印一些非空DB的信息。VERBOSE以下的日志级别才显示，用于测试。
    if (server.verbosity <= LL_VERBOSE) {
        run_with_period(5000) {
            for (j = 0; j < server.dbnum; j++) {
                long long size, used, vkeys;

                size = dictSlots(server.db[j].dict);
                used = dictSize(server.db[j].dict);
                vkeys = dictSize(server.db[j].expires);
                // 遍历各个DB，如果db中有数据，即used或vkeys>0打印信息。
                if (used || vkeys) {
                    serverLog(LL_VERBOSE,"DB %d: %lld keys (%lld volatile) in %lld slots HT.",j,used,vkeys,size);
                }
            }
        }
    }

    /* Show information about connected clients */
    // 打印client连接数，当前使用内存数量
    if (!server.sentinel_mode) {
        run_with_period(5000) {
            serverLog(LL_DEBUG,
                "%lu clients connected (%lu replicas), %zu bytes in use",
                listLength(server.clients)-listLength(server.slaves),
                listLength(server.slaves),
                zmalloc_used_memory());
        }
    }

    /* We need to do a few operations on clients asynchronously. */
    // 定时异步处理clients相关操作。
    clientsCron();

    /* Handle background operations on Redis databases. */
    // 定期处理redis db相关操作。
    databasesCron();

    /* Start a scheduled AOF rewrite if this was requested by the user while
     * a BGSAVE was in progress. */
    // 如果当前没有后台AOF重写或RDB save在执行，则检查是否有在等待调度的aof_rewrite任务，有则启动。
    // 当系统在做AOF重写或RDB save时，可能用户发起BGSAVE操作，此时不会立即执行，而是设置调度标识，等当前任务执行完了，再在cron中调度执行。
    if (!hasActiveChildProcess() &&
        server.aof_rewrite_scheduled)
    {
        rewriteAppendOnlyFileBackground();
    }

    /* Check if a background saving or AOF rewrite in progress terminated. */
    // 如果当前有RDB save或AOF重写进程，检查是否结束。如果没有后台进程，则检查是否达到了启动RDB save或AOF重写的条件。
    if (hasActiveChildProcess() || ldbPendingChildren())
    {
        // 每秒处理子进程发送来的处理信息。
        run_with_period(1000) receiveChildInfo();
        // 每秒10次检查子进程是否结束。
        checkChildrenDone();
    } else {
        // 如果当前没有在进行后台RDB save或rewrite，则检查是否我们应该进行数据持久化了。
        /* If there is not a background saving/rewrite in progress check if
         * we have to save/rewrite now. */

        // 先检测是否达到了RDB save的条件，因为我们默认是有3个条件（1小时1变更、300秒100变更，60秒10000变更），达到任意一个则触发任务。
        for (j = 0; j < server.saveparamslen; j++) {
            struct saveparam *sp = server.saveparams+j;

            /* Save if we reached the given amount of changes,
             * the given amount of seconds, and if the latest bgsave was
             * successful or if, in case of an error, at least
             * CONFIG_BGSAVE_RETRY_DELAY seconds already elapsed. */
            // 如果我们达到了指定时间内的变更数 && （上一次是成功SAVE || 上一次SAVE已经间隔了足够时间），则开启新一个RDB save任务。
            if (server.dirty >= sp->changes &&
                server.unixtime-server.lastsave > sp->seconds &&
                (server.unixtime-server.lastbgsave_try >
                 CONFIG_BGSAVE_RETRY_DELAY ||
                 server.lastbgsave_status == C_OK))
            {
                serverLog(LL_NOTICE,"%d changes in %d seconds. Saving...",
                    sp->changes, (int)sp->seconds);
                rdbSaveInfo rsi, *rsiptr;
                // 填充复制信息，目前是选择的db
                rsiptr = rdbPopulateSaveInfo(&rsi);
                // 启动后台RDB save进程处理。
                rdbSaveBackground(server.rdb_filename,rsiptr);
                break;
            }
        }

        /* Trigger an AOF rewrite if needed. */
        // 检查是否可以触发AOF重写操作。因为前面可能开启了RDB save，所以这里还要再确保没有后台子进程在处理。
        // 另外只有在AOF文件大于64M，且增长比例大于配置的aof_rewrite_perc时才进行AOF重写。
        if (server.aof_state == AOF_ON &&
            !hasActiveChildProcess() &&
            server.aof_rewrite_perc &&
            server.aof_current_size > server.aof_rewrite_min_size)
        {
            long long base = server.aof_rewrite_base_size ?
                server.aof_rewrite_base_size : 1;
            // 增长比例为 （current-base）*100/base，如果增长比例大于server.aof_rewrite_perc，则启动AOF重写处理。
            long long growth = (server.aof_current_size*100/base) - 100;
            if (growth >= server.aof_rewrite_perc) {
                serverLog(LL_NOTICE,"Starting automatic rewriting of AOF on %lld%% growth",growth);
                rewriteAppendOnlyFileBackground();
            }
        }
    }
    /* Just for the sake of defensive programming, to avoid forgeting to
     * call this function when need. */
    // 可以不需要调用这个函数，只是为了防御性编程，避免在某些需要的地方忘了调用这个函数。
    // 理论上目前fork进程的地方我们会调用这个函数来阻止hash表的resize，然后在进程结束reset子进程状态的时候恢复，所以这里可以不需要。
    updateDictResizePolicy();


    /* AOF postponed flush: Try at every cron cycle if the slow fsync
     * completed. */
    // AOF延迟的flush处理。如果之前有一个慢的fsync而导致flush被推迟了的话，我们这里每次cron的时候检查一下是否可以进行flush。
    if (server.aof_state == AOF_ON && server.aof_flush_postponed_start)
        flushAppendOnlyFile(0);

    /* AOF write errors: in this case we have a buffer to flush as well and
     * clear the AOF error in case of success to make the DB writable again,
     * however to try every second is enough in case of 'hz' is set to
     * a higher frequency. */
    // AOF写err时处理。因为AOF flush出错时，可能会阻止redis服务的写命令，这里我们再尝试flush写文件操作，成功了就移除err，使得DB再次可写。
    // 当hz设置为很大频率时，这里err时处理，1s一次就够了。另外flush我们看过实现，尽可能的做到操作的原子性，错误回滚什么的。
    run_with_period(1000) {
        if (server.aof_state == AOF_ON && server.aof_last_write_status == C_ERR)
            flushAppendOnlyFile(0);
    }

    /* Clear the paused clients state if needed. */
    // 如果需要，清除暂停clients状态。主要就是检查是否暂停时间到了，到了则从暂停阻塞的client列表中清除。
    checkClientPauseTimeoutAndReturnIfPaused();

    /* Replication cron function -- used to reconnect to master,
     * detect transfer failures, start background RDB transfers and so forth. 
     * 
     * If Redis is trying to failover then run the replication cron faster so
     * progress on the handshake happens more quickly. */
    // 复制相关的定时处理函数，主要用于检测超时从而与master重连、检测是否传输失败、开启后台RDB传输等处理。
    // 如果Redis正在进行故障转移，则以更快的频率来执行复制cron函数，这样在选择了新的master后能更快开始重新握手连接，从而尽快开启复制。
    // 有FAILOVER时，每秒10次，否则每秒1次。
    if (server.failover_state != NO_FAILOVER) {
        run_with_period(100) replicationCron();
    } else {
        run_with_period(1000) replicationCron();
    }

    /* Run the Redis Cluster cron. */
    // 执行集群的cron处理，每秒10次。
    run_with_period(100) {
        if (server.cluster_enabled) clusterCron();
    }

    /* Run the Sentinel timer if we are in sentinel mode. */
    // 如果是sentinel模式，则执行Sentinel定时处理任务。（TODO）
    if (server.sentinel_mode) sentinelTimer();

    /* Cleanup expired MIGRATE cached sockets. */
    // 清除过期的 MIGRATE 缓存socket。
    run_with_period(1000) {
        migrateCloseTimedoutSockets();
    }

    /* Stop the I/O threads if we don't have enough pending work. */
    // 如果当前没有待处理的clients，我们可以先暂停IO线程。
    stopThreadedIOIfNeeded();

    /* Resize tracking keys table if needed. This is also done at every
     * command execution, but we want to be sure that if the last command
     * executed changes the value via CONFIG SET, the server will perform
     * the operation even if completely idle. */
    // 调整用于clients缓存而追踪记录keys的表的大小，避免超出我们设定的容量限制。必要时会清除key，并向client发送key失效的消息。
    // 这在每条命令执行时也会进行处理，但是如果命令恰好是CONFIG SET修改追踪表大小时，需要确保用量满足配置，所以这里服务空闲时也进行检查处理。
    if (server.tracking_clients) trackingLimitUsedSlots();

    /* Start a scheduled BGSAVE if the corresponding flag is set. This is
     * useful when we are forced to postpone a BGSAVE because an AOF
     * rewrite is in progress.
     *
     * Note: this code must be after the replicationCron() call above so
     * make sure when refactoring this file to keep this order. This is useful
     * because we want to give priority to RDB savings for replication. */
    // 如果有设置调度bgsave的标识，则在后台没有进程时，检查是否能启动BGSAVE进行RDB save。因为有可能有AOF重写进程，从而推迟我们BGSAVE命令。
    // 注意这个代码必须在replicationCron()后面执行，重构时要确保这个顺序。因为先执行复制cron的话，如果需要RDB传输，这里可以一个任务搞定。
    // 很好奇，如果要保证这个顺序的话，这个处理和前面复制cron处理为什么不一起放到更前面？databasesCron()后面那块？
    if (!hasActiveChildProcess() &&
        server.rdb_bgsave_scheduled &&
        (server.unixtime-server.lastbgsave_try > CONFIG_BGSAVE_RETRY_DELAY ||
         server.lastbgsave_status == C_OK))
    {
        rdbSaveInfo rsi, *rsiptr;
        rsiptr = rdbPopulateSaveInfo(&rsi);
        if (rdbSaveBackground(server.rdb_filename,rsiptr) == C_OK)
            server.rdb_bgsave_scheduled = 0;
    }

    /* Fire the cron loop modules event. */
    // 触发定时cron任务执行事件通知module。（TODO）
    RedisModuleCronLoopV1 ei = {REDISMODULE_CRON_LOOP_VERSION,server.hz};
    moduleFireServerEvent(REDISMODULE_EVENT_CRON_LOOP,
                          0,
                          &ei);

    // cron执行次数+1，返回下一次cron执行时间为 1000/server.hz 毫秒后。
    server.cronloops++;
    return 1000/server.hz;
}


void blockingOperationStarts() {
    if(!server.blocking_op_nesting++){
        updateCachedTime(0);
        server.blocked_last_cron = server.mstime;
    }
}

void blockingOperationEnds() {
    if(!(--server.blocking_op_nesting)){
        server.blocked_last_cron = 0;
    }
}

/* This function fill in the role of serverCron during RDB or AOF loading, and
 * also during blocked scripts.
 * It attempts to do its duties at a similar rate as the configured server.hz,
 * and updates cronloops variable so that similarly to serverCron, the
 * run_with_period can be used. */
// 这个函数在RDB、AOF或阻塞脚本执行期间，担任类似serverCron角色。
// 以一定的频率去执行一些操作，并更新循环记录相关变量，与serverCron一样可以使用run_with_period。
void whileBlockedCron() {
    /* Here we may want to perform some cron jobs (normally done server.hz times
     * per second). */

    /* Since this function depends on a call to blockingOperationStarts, let's
     * make sure it was done. */
    // 当前函数是在RDB、AOF或阻塞脚本执行时执行的，所以必然之前会先执行blockingOperationStarts函数，写入blocked_last_cron变量。
    serverAssert(server.blocked_last_cron);

    /* In case we where called too soon, leave right away. This way one time
     * jobs after the loop below don't need an if. and we don't bother to start
     * latency monitor if this function is called too often. */
    // 我们调用的太早了？直接返回。这样，下面循环之后的一次性任务就不再需要进行if判断了。
    // 同时当这个函数频繁调用时，我们也不用总是去做latency监控。
    if (server.blocked_last_cron >= server.mstime)
        return;

    // 监控当前BlockedCron执行的时间。
    mstime_t latency;
    latencyStartMonitor(latency);

    /* In some cases we may be called with big intervals, so we may need to do
     * extra work here. This is because some of the functions in serverCron rely
     * on the fact that it is performed every 10 ms or so. For instance, if
     * activeDefragCycle needs to utilize 25% cpu, it will utilize 2.5ms, so we
     * need to call it multiple times. */
    // 一些情况下，可能隔很长时间才调用，所以这里需要做一些额外的工作。
    // 这是因为一些函数在serverCron中每隔10ms左右就执行一次。
    // 如果activeDefragCycle需要使用25%的cpu，他将使用2.5ms，所以我们需要调用多次。
    long hz_ms = 1000/server.hz;
    while (server.blocked_last_cron < server.mstime) {

        /* Defrag keys gradually. */
        activeDefragCycle();

        server.blocked_last_cron += hz_ms;

        /* Increment cronloop so that run_with_period works. */
        server.cronloops++;
    }

    /* Other cron jobs do not need to be done in a loop. No need to check
     * server.blocked_last_cron since we have an early exit at the top. */
    // 其他的cron任务不需要循环处理，这里也不需要check blocked_last_cron，因为前面我们已经检查并退出了。

    /* Update memory stats during loading (excluding blocked scripts) */
    // 如果是在RDB或AOF加载数据，更新内存统计数据。
    if (server.loading) cronUpdateMemoryStats();

    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("while-blocked-cron",latency);
}

extern int ProcessingEventsWhileBlocked;

/* This function gets called every time Redis is entering the
 * main loop of the event driven library, that is, before to sleep
 * for ready file descriptors.
 *
 * Note: This function is (currently) called from two functions:
 * 1. aeMain - The main server loop
 * 2. processEventsWhileBlocked - Process clients during RDB/AOF load
 *
 * If it was called from processEventsWhileBlocked we don't want
 * to perform all actions (For example, we don't want to expire
 * keys), but we do need to perform some actions.
 *
 * The most important is freeClientsInAsyncFreeQueue but we also
 * call some other low-risk functions. */
// 进入select等待fd前执行一些操作。
// 注意这个函数有两处会调用：
// 1、aeMain - 主事件处理循环。
// 2、processEventsWhileBlocked - 在RDB/AOF数据加载时处理client请求，基本返回'loading'错误。
void beforeSleep(struct aeEventLoop *eventLoop) {
    UNUSED(eventLoop);

    // 检测并更新redis使用内存的最大峰值
    size_t zmalloc_used = zmalloc_used_memory();
    if (zmalloc_used > server.stat_peak_memory)
        server.stat_peak_memory = zmalloc_used;

    /* Just call a subset of vital functions in case we are re-entering
     * the event loop from processEventsWhileBlocked(). Note that in this
     * case we keep track of the number of events we are processing, since
     * processEventsWhileBlocked() wants to stop ASAP if there are no longer
     * events to handle. */
    // blocked状态时，只执行这几个处理连接数据的关键函数。只有从processEventsWhileBlocked()进入事件循环，才会执行这里。
    // 追踪有多少事件触发了正在等待处理。如果没有事件要处理，processEventsWhileBlocked()可以尽可能早的return。
    if (ProcessingEventsWhileBlocked) {
        uint64_t processed = 0;
        // 处理pending读clients
        processed += handleClientsWithPendingReadsUsingThreads();
        // 处理tls pending数据
        processed += tlsProcessPendingData();
        // 处理pending写clients
        processed += handleClientsWithPendingWrites();
        // 处理等待异步释放（通过freeClientAsync()加入到clients_to_close列表）的client
        processed += freeClientsInAsyncFreeQueue();
        // 更新blocked期间处理的任务数
        server.events_processed_while_blocked += processed;
        return;
    }

    /* Handle precise timeouts of blocked clients. */
    // 处理阻塞的client，发生阻塞超时的情况。
    handleBlockedClientsTimeout();

    /* We should handle pending reads clients ASAP after event loop. */
    // 处理pending读，开启io线程时，event loop中处理读事件时并不真正处理读操作，而是加入clients_pending_read队列。
    // 当触发的事件处理完成后，下一次检查触发事件之前，也即当前函数beforeSleep中这里真正处理读操作。
    handleClientsWithPendingReadsUsingThreads();

    /* Handle TLS pending data. (must be done before flushAppendOnlyFile) */
    // 处理TLS连接待处理的数据
    tlsProcessPendingData();

    /* If tls still has pending unread data don't sleep at all. */
    // 如果tls pending数据处理后，tls中还有待读的数据，我们设置eventLoop DONT_WAIT，从而尽快响应已有的读处理。
    aeSetDontWait(server.el, tlsHasPendingData());

    /* Call the Redis Cluster before sleep function. Note that this function
     * may change the state of Redis Cluster (from ok to fail or vice versa),
     * so it's a good idea to call it before serving the unblocked clients
     * later in this function. */
    // 调用集群的beforeSleep方法。
    // 注意这个函数可能改变集群的状态（从ok->fail，或相反），所以好的方式应该先调用这个方法，然后再处理unblocked_clients请求。
    if (server.cluster_enabled) clusterBeforeSleep();

    /* Run a fast expire cycle (the called function will return
     * ASAP if a fast cycle is not needed). */
    // 当前是master且启用了expire配置，调用一个fast expire处理。
    if (server.active_expire_enabled && server.masterhost == NULL)
        activeExpireCycle(ACTIVE_EXPIRE_CYCLE_FAST);

    /* Unblock all the clients blocked for synchronous replication
     * in WAIT. */
    // 如果有client在阻塞在同步复制WAIT上，这里根据获取到的ACKs判断是否能解除阻塞，回复client。
    if (listLength(server.clients_waiting_acks))
        processClientsWaitingReplicas();

    /* Check if there are clients unblocked by modules that implement
     * blocking commands. */
    // 检查client是否被实现了阻塞命令的modules解除了阻塞。（TODO）
    if (moduleCount()) moduleHandleBlockedClients();

    /* Try to process pending commands for clients that were just unblocked. */
    // 对于unblock的client，我们会加入到server.unblocked_clients列表中，这里尝试处理执行这些client的pending commands。
    if (listLength(server.unblocked_clients))
        processUnblockedClients();

    /* Send all the slaves an ACK request if at least one client blocked
     * during the previous event loop iteration. Note that we do this after
     * processUnblockedClients(), so if there are multiple pipelined WAITs
     * and the just unblocked WAIT gets blocked again, we don't have to wait
     * a server cron cycle in absence of other event loop events. See #6623.
     * 
     * We also don't send the ACKs while clients are paused, since it can
     * increment the replication backlog, they'll be sent after the pause
     * if we are still the master. */
    // 如果有只是一个client在前一个事件循环迭代中阻塞了，则向所有的slaves都发送一个获取ACK的请求。
    // 注意我们在processUnblockedClients()之后处理这个，因为如果pipeline中有多个WAITs，前面刚处理完上一个WAIT的回复，
    // 执行命令后，马上又进入下一个WAIT阻塞状态，我们就可以及时的发送请求出去，下一轮继续处理。

    // 我们不会在client暂停时发送获取ACK请求，因为它会增加backlog，暂停恢复后，如果我们仍然是主服务器的话，还是会发送的。
    if (server.get_ack_from_slaves && !checkClientPauseTimeoutAndReturnIfPaused()) {
        robj *argv[3];

        argv[0] = shared.replconf;
        argv[1] = shared.getack;
        argv[2] = shared.special_asterick; /* Not used argument. */
        // 向slave发送REPLCONF GETACK命令
        replicationFeedSlaves(server.slaves, server.slaveseldb, argv, 3);
        server.get_ack_from_slaves = 0;
    }

    /* We may have recieved updates from clients about their current offset. NOTE:
     * this can't be done where the ACK is recieved since failover will disconnect 
     * our clients. */
    // 故障转移的命令会设置相关故障转移的标识，这里更新状态，并检查是否达到了故障转移条件，从而进行故障转移。
    // 注意这不能在我们请求ACK的前面调用，因为故障转移会断开所有的clients连接（包括slaves连接），
    updateFailoverStatus();

    /* Send the invalidation messages to clients participating to the
     * client side caching protocol in broadcasting (BCAST) mode. */
    // 发送key无效的消息给以广播模式参与客户端缓存协议的client。（TODO）
    trackingBroadcastInvalidationMessages();

    /* Write the AOF buffer on disk */
    // 将AOF缓存追加到磁盘文件上
    if (server.aof_state == AOF_ON)
        flushAppendOnlyFile(0);

    /* Handle writes with pending output buffers. */
    // 如果有待写client，这里处理写数据。
    handleClientsWithPendingWritesUsingThreads();

    /* Close clients that need to be closed asynchronous */
    // 处理client异步释放
    freeClientsInAsyncFreeQueue();

    /* Try to process blocked clients every once in while. Example: A module
     * calls RM_SignalKeyAsReady from within a timer callback (So we don't
     * visit processCommand() at all). */
    // 尝试每隔一段时间处理下被阻塞的客户端。（TODO）
    // 例如：一个module利用定时回调调用RM_SignalKeyAsReady方法，会将key加入ready_keys中，但并不会处理命令执行，所以这里来统一处理。
    handleClientsBlockedOnKeys();

    /* Before we are going to sleep, let the threads access the dataset by
     * releasing the GIL. Redis main thread will not touch anything at this
     * time. */
    // 在我们进入sleep前，让线程通过释放GIL来访问数据集。Redis主线程此时不会操作任何数据。
    if (moduleCount()) moduleReleaseGIL();

    /* Do NOT add anything below moduleReleaseGIL !!! */
    // 注意这里不要在moduleReleaseGIL后面加任何操作。保证redis后面不会操作数据。
}

/* This function is called immediately after the event loop multiplexing
 * API returned, and the control is going to soon return to Redis by invoking
 * the different events callbacks. */
// 在事件循环epoll多路复用API返回后，立即调用这个函数。
void afterSleep(struct aeEventLoop *eventLoop) {
    UNUSED(eventLoop);

    /* Do NOT add anything above moduleAcquireGIL !!! */
    // 注意在moduleAcquireGIL前面不要添加任何操作。

    /* Aquire the modules GIL so that their threads won't touch anything. */
    // 非blocked期间执行事件循环，即正常循环，我们要获取全局锁，从而防止其他module线程变更数据。
    if (!ProcessingEventsWhileBlocked) {
        if (moduleCount()) moduleAcquireGIL();
    }
}

/* =========================== Server initialization ======================== */

void createSharedObjects(void) {
    int j;

    /* Shared command responses */
    shared.crlf = createObject(OBJ_STRING,sdsnew("\r\n"));
    shared.ok = createObject(OBJ_STRING,sdsnew("+OK\r\n"));
    shared.emptybulk = createObject(OBJ_STRING,sdsnew("$0\r\n\r\n"));
    shared.czero = createObject(OBJ_STRING,sdsnew(":0\r\n"));
    shared.cone = createObject(OBJ_STRING,sdsnew(":1\r\n"));
    shared.emptyarray = createObject(OBJ_STRING,sdsnew("*0\r\n"));
    shared.pong = createObject(OBJ_STRING,sdsnew("+PONG\r\n"));
    shared.queued = createObject(OBJ_STRING,sdsnew("+QUEUED\r\n"));
    shared.emptyscan = createObject(OBJ_STRING,sdsnew("*2\r\n$1\r\n0\r\n*0\r\n"));
    shared.space = createObject(OBJ_STRING,sdsnew(" "));
    shared.colon = createObject(OBJ_STRING,sdsnew(":"));
    shared.plus = createObject(OBJ_STRING,sdsnew("+"));

    /* Shared command error responses */
    shared.wrongtypeerr = createObject(OBJ_STRING,sdsnew(
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"));
    shared.err = createObject(OBJ_STRING,sdsnew("-ERR\r\n"));
    shared.nokeyerr = createObject(OBJ_STRING,sdsnew(
        "-ERR no such key\r\n"));
    shared.syntaxerr = createObject(OBJ_STRING,sdsnew(
        "-ERR syntax error\r\n"));
    shared.sameobjecterr = createObject(OBJ_STRING,sdsnew(
        "-ERR source and destination objects are the same\r\n"));
    shared.outofrangeerr = createObject(OBJ_STRING,sdsnew(
        "-ERR index out of range\r\n"));
    shared.noscripterr = createObject(OBJ_STRING,sdsnew(
        "-NOSCRIPT No matching script. Please use EVAL.\r\n"));
    shared.loadingerr = createObject(OBJ_STRING,sdsnew(
        "-LOADING Redis is loading the dataset in memory\r\n"));
    shared.slowscripterr = createObject(OBJ_STRING,sdsnew(
        "-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"));
    shared.masterdownerr = createObject(OBJ_STRING,sdsnew(
        "-MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.\r\n"));
    shared.bgsaveerr = createObject(OBJ_STRING,sdsnew(
        "-MISCONF Redis is configured to save RDB snapshots, but it is currently not able to persist on disk. Commands that may modify the data set are disabled, because this instance is configured to report errors during writes if RDB snapshotting fails (stop-writes-on-bgsave-error option). Please check the Redis logs for details about the RDB error.\r\n"));
    shared.roslaveerr = createObject(OBJ_STRING,sdsnew(
        "-READONLY You can't write against a read only replica.\r\n"));
    shared.noautherr = createObject(OBJ_STRING,sdsnew(
        "-NOAUTH Authentication required.\r\n"));
    shared.oomerr = createObject(OBJ_STRING,sdsnew(
        "-OOM command not allowed when used memory > 'maxmemory'.\r\n"));
    shared.execaborterr = createObject(OBJ_STRING,sdsnew(
        "-EXECABORT Transaction discarded because of previous errors.\r\n"));
    shared.noreplicaserr = createObject(OBJ_STRING,sdsnew(
        "-NOREPLICAS Not enough good replicas to write.\r\n"));
    shared.busykeyerr = createObject(OBJ_STRING,sdsnew(
        "-BUSYKEY Target key name already exists.\r\n"));

    /* The shared NULL depends on the protocol version. */
    shared.null[0] = NULL;
    shared.null[1] = NULL;
    shared.null[2] = createObject(OBJ_STRING,sdsnew("$-1\r\n"));
    shared.null[3] = createObject(OBJ_STRING,sdsnew("_\r\n"));

    shared.nullarray[0] = NULL;
    shared.nullarray[1] = NULL;
    shared.nullarray[2] = createObject(OBJ_STRING,sdsnew("*-1\r\n"));
    shared.nullarray[3] = createObject(OBJ_STRING,sdsnew("_\r\n"));

    shared.emptymap[0] = NULL;
    shared.emptymap[1] = NULL;
    shared.emptymap[2] = createObject(OBJ_STRING,sdsnew("*0\r\n"));
    shared.emptymap[3] = createObject(OBJ_STRING,sdsnew("%0\r\n"));

    shared.emptyset[0] = NULL;
    shared.emptyset[1] = NULL;
    shared.emptyset[2] = createObject(OBJ_STRING,sdsnew("*0\r\n"));
    shared.emptyset[3] = createObject(OBJ_STRING,sdsnew("~0\r\n"));

    for (j = 0; j < PROTO_SHARED_SELECT_CMDS; j++) {
        char dictid_str[64];
        int dictid_len;

        dictid_len = ll2string(dictid_str,sizeof(dictid_str),j);
        shared.select[j] = createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, dictid_str));
    }
    shared.messagebulk = createStringObject("$7\r\nmessage\r\n",13);
    shared.pmessagebulk = createStringObject("$8\r\npmessage\r\n",14);
    shared.subscribebulk = createStringObject("$9\r\nsubscribe\r\n",15);
    shared.unsubscribebulk = createStringObject("$11\r\nunsubscribe\r\n",18);
    shared.psubscribebulk = createStringObject("$10\r\npsubscribe\r\n",17);
    shared.punsubscribebulk = createStringObject("$12\r\npunsubscribe\r\n",19);

    /* Shared command names */
    shared.del = createStringObject("DEL",3);
    shared.unlink = createStringObject("UNLINK",6);
    shared.rpop = createStringObject("RPOP",4);
    shared.lpop = createStringObject("LPOP",4);
    shared.lpush = createStringObject("LPUSH",5);
    shared.rpoplpush = createStringObject("RPOPLPUSH",9);
    shared.lmove = createStringObject("LMOVE",5);
    shared.blmove = createStringObject("BLMOVE",6);
    shared.zpopmin = createStringObject("ZPOPMIN",7);
    shared.zpopmax = createStringObject("ZPOPMAX",7);
    shared.multi = createStringObject("MULTI",5);
    shared.exec = createStringObject("EXEC",4);
    shared.hset = createStringObject("HSET",4);
    shared.srem = createStringObject("SREM",4);
    shared.xgroup = createStringObject("XGROUP",6);
    shared.xclaim = createStringObject("XCLAIM",6);
    shared.script = createStringObject("SCRIPT",6);
    shared.replconf = createStringObject("REPLCONF",8);
    shared.pexpireat = createStringObject("PEXPIREAT",9);
    shared.pexpire = createStringObject("PEXPIRE",7);
    shared.persist = createStringObject("PERSIST",7);
    shared.set = createStringObject("SET",3);
    shared.eval = createStringObject("EVAL",4);

    /* Shared command argument */
    shared.left = createStringObject("left",4);
    shared.right = createStringObject("right",5);
    shared.pxat = createStringObject("PXAT", 4);
    shared.px = createStringObject("PX",2);
    shared.time = createStringObject("TIME",4);
    shared.retrycount = createStringObject("RETRYCOUNT",10);
    shared.force = createStringObject("FORCE",5);
    shared.justid = createStringObject("JUSTID",6);
    shared.lastid = createStringObject("LASTID",6);
    shared.default_username = createStringObject("default",7);
    shared.ping = createStringObject("ping",4);
    shared.setid = createStringObject("SETID",5);
    shared.keepttl = createStringObject("KEEPTTL",7);
    shared.load = createStringObject("LOAD",4);
    shared.createconsumer = createStringObject("CREATECONSUMER",14);
    shared.getack = createStringObject("GETACK",6);
    shared.special_asterick = createStringObject("*",1);
    shared.special_equals = createStringObject("=",1);
    shared.redacted = makeObjectShared(createStringObject("(redacted)",10));

    for (j = 0; j < OBJ_SHARED_INTEGERS; j++) {
        shared.integers[j] =
            makeObjectShared(createObject(OBJ_STRING,(void*)(long)j));
        shared.integers[j]->encoding = OBJ_ENCODING_INT;
    }
    for (j = 0; j < OBJ_SHARED_BULKHDR_LEN; j++) {
        shared.mbulkhdr[j] = createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),"*%d\r\n",j));
        shared.bulkhdr[j] = createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),"$%d\r\n",j));
    }
    /* The following two shared objects, minstring and maxstrings, are not
     * actually used for their value but as a special object meaning
     * respectively the minimum possible string and the maximum possible
     * string in string comparisons for the ZRANGEBYLEX command. */
    shared.minstring = sdsnew("minstring");
    shared.maxstring = sdsnew("maxstring");
}

void initServerConfig(void) {
    int j;

    updateCachedTime(1);
    getRandomHexChars(server.runid,CONFIG_RUN_ID_SIZE);
    server.runid[CONFIG_RUN_ID_SIZE] = '\0';
    changeReplicationId();
    clearReplicationId2();
    server.hz = CONFIG_DEFAULT_HZ; /* Initialize it ASAP, even if it may get
                                      updated later after loading the config.
                                      This value may be used before the server
                                      is initialized. */
    server.timezone = getTimeZone(); /* Initialized by tzset(). */
    server.configfile = NULL;
    server.executable = NULL;
    server.arch_bits = (sizeof(long) == 8) ? 64 : 32;
    server.bindaddr_count = 0;
    server.unixsocketperm = CONFIG_DEFAULT_UNIX_SOCKET_PERM;
    server.ipfd.count = 0;
    server.tlsfd.count = 0;
    server.sofd = -1;
    server.active_expire_enabled = 1;
    server.skip_checksum_validation = 0;
    server.saveparams = NULL;
    server.loading = 0;
    server.loading_rdb_used_mem = 0;
    server.logfile = zstrdup(CONFIG_DEFAULT_LOGFILE);
    server.aof_state = AOF_OFF;
    server.aof_rewrite_base_size = 0;
    server.aof_rewrite_scheduled = 0;
    server.aof_flush_sleep = 0;
    server.aof_last_fsync = time(NULL);
    atomicSet(server.aof_bio_fsync_status,C_OK);
    server.aof_rewrite_time_last = -1;
    server.aof_rewrite_time_start = -1;
    server.aof_lastbgrewrite_status = C_OK;
    server.aof_delayed_fsync = 0;
    server.aof_fd = -1;
    server.aof_selected_db = -1; /* Make sure the first time will not match */
    server.aof_flush_postponed_start = 0;
    server.pidfile = NULL;
    server.active_defrag_running = 0;
    server.notify_keyspace_events = 0;
    server.blocked_clients = 0;
    memset(server.blocked_clients_by_type,0,
           sizeof(server.blocked_clients_by_type));
    server.shutdown_asap = 0;
    server.cluster_configfile = zstrdup(CONFIG_DEFAULT_CLUSTER_CONFIG_FILE);
    server.cluster_module_flags = CLUSTER_MODULE_FLAG_NONE;
    server.migrate_cached_sockets = dictCreate(&migrateCacheDictType,NULL);
    server.next_client_id = 1; /* Client IDs, start from 1 .*/
    server.loading_process_events_interval_bytes = (1024*1024*2);

    unsigned int lruclock = getLRUClock();
    atomicSet(server.lruclock,lruclock);
    resetServerSaveParams();

    // 初始化触发RDB save的条件。1小时内有1个改变，或者300秒内有100个变更，或者60秒内有10000个变更
    appendServerSaveParams(60*60,1);  /* save after 1 hour and 1 change */
    appendServerSaveParams(300,100);  /* save after 5 minutes and 100 changes */
    appendServerSaveParams(60,10000); /* save after 1 minute and 10000 changes */

    /* Replication related */
    // 初始化复制传播的相关字段
    server.masterauth = NULL;
    server.masterhost = NULL;
    server.masterport = 6379;
    server.master = NULL;
    server.cached_master = NULL;
    server.master_initial_offset = -1;
    server.repl_state = REPL_STATE_NONE;
    server.repl_transfer_tmpfile = NULL;
    server.repl_transfer_fd = -1;
    server.repl_transfer_s = NULL;
    server.repl_syncio_timeout = CONFIG_REPL_SYNCIO_TIMEOUT;
    server.repl_down_since = 0; /* Never connected, repl is down since EVER. */
    server.master_repl_offset = 0;

    /* Replication partial resync backlog */
    // 初始化用于部分同步复制的backlog属性
    server.repl_backlog = NULL;
    server.repl_backlog_histlen = 0;
    server.repl_backlog_idx = 0;
    server.repl_backlog_off = 0;
    server.repl_no_slaves_since = time(NULL);

    /* Failover related */
    // 初始化故障转移相关属性
    server.failover_end_time = 0;
    server.force_failover = 0;
    server.target_replica_host = NULL;
    server.target_replica_port = 0;
    server.failover_state = NO_FAILOVER;

    /* Client output buffer limits */
    // 初始化输入缓存区的限制，达到了限制会close连接。
    for (j = 0; j < CLIENT_TYPE_OBUF_COUNT; j++)
        server.client_obuf_limits[j] = clientBufferLimitsDefaults[j];

    /* Linux OOM Score config */
    for (j = 0; j < CONFIG_OOM_COUNT; j++)
        server.oom_score_adj_values[j] = configOOMScoreAdjValuesDefaults[j];

    /* Double constants initialization */
    R_Zero = 0.0;
    R_PosInf = 1.0/R_Zero;
    R_NegInf = -1.0/R_Zero;
    R_Nan = R_Zero/R_Zero;

    /* Command table -- we initialize it here as it is part of the
     * initial configuration, since command names may be changed via
     * redis.conf using the rename-command directive. */
    // 初始化命令表以及常见命令。
    server.commands = dictCreate(&commandTableDictType,NULL);
    server.orig_commands = dictCreate(&commandTableDictType,NULL);
    populateCommandTable();
    server.delCommand = lookupCommandByCString("del");
    server.multiCommand = lookupCommandByCString("multi");
    server.lpushCommand = lookupCommandByCString("lpush");
    server.lpopCommand = lookupCommandByCString("lpop");
    server.rpopCommand = lookupCommandByCString("rpop");
    server.zpopminCommand = lookupCommandByCString("zpopmin");
    server.zpopmaxCommand = lookupCommandByCString("zpopmax");
    server.sremCommand = lookupCommandByCString("srem");
    server.execCommand = lookupCommandByCString("exec");
    server.expireCommand = lookupCommandByCString("expire");
    server.pexpireCommand = lookupCommandByCString("pexpire");
    server.xclaimCommand = lookupCommandByCString("xclaim");
    server.xgroupCommand = lookupCommandByCString("xgroup");
    server.rpoplpushCommand = lookupCommandByCString("rpoplpush");
    server.lmoveCommand = lookupCommandByCString("lmove");

    /* Debugging */
    server.watchdog_period = 0;

    /* By default we want scripts to be always replicated by effects
     * (single commands executed by the script), and not by sending the
     * script to the slave / AOF. This is the new way starting from
     * Redis 5. However it is possible to revert it via redis.conf. */
    server.lua_always_replicate_commands = 1;

    initConfigValues();
}

extern char **environ;

/* Restart the server, executing the same executable that started this
 * instance, with the same arguments and configuration file.
 *
 * The function is designed to directly call execve() so that the new
 * server instance will retain the PID of the previous one.
 *
 * The list of flags, that may be bitwise ORed together, alter the
 * behavior of this function:
 *
 * RESTART_SERVER_NONE              No flags.
 * RESTART_SERVER_GRACEFULLY        Do a proper shutdown before restarting.
 * RESTART_SERVER_CONFIG_REWRITE    Rewrite the config file before restarting.
 *
 * On success the function does not return, because the process turns into
 * a different process. On error C_ERR is returned. */
int restartServer(int flags, mstime_t delay) {
    int j;

    /* Check if we still have accesses to the executable that started this
     * server instance. */
    if (access(server.executable,X_OK) == -1) {
        serverLog(LL_WARNING,"Can't restart: this process has no "
                             "permissions to execute %s", server.executable);
        return C_ERR;
    }

    /* Config rewriting. */
    if (flags & RESTART_SERVER_CONFIG_REWRITE &&
        server.configfile &&
        rewriteConfig(server.configfile, 0) == -1)
    {
        serverLog(LL_WARNING,"Can't restart: configuration rewrite process "
                             "failed");
        return C_ERR;
    }

    /* Perform a proper shutdown. */
    if (flags & RESTART_SERVER_GRACEFULLY &&
        prepareForShutdown(SHUTDOWN_NOFLAGS) != C_OK)
    {
        serverLog(LL_WARNING,"Can't restart: error preparing for shutdown");
        return C_ERR;
    }

    /* Close all file descriptors, with the exception of stdin, stdout, strerr
     * which are useful if we restart a Redis server which is not daemonized. */
    for (j = 3; j < (int)server.maxclients + 1024; j++) {
        /* Test the descriptor validity before closing it, otherwise
         * Valgrind issues a warning on close(). */
        if (fcntl(j,F_GETFD) != -1) close(j);
    }

    /* Execute the server with the original command line. */
    if (delay) usleep(delay*1000);
    zfree(server.exec_argv[0]);
    server.exec_argv[0] = zstrdup(server.executable);
    execve(server.executable,server.exec_argv,environ);

    /* If an error occurred here, there is nothing we can do, but exit. */
    _exit(1);

    return C_ERR; /* Never reached. */
}

static void readOOMScoreAdj(void) {
#ifdef HAVE_PROC_OOM_SCORE_ADJ
    char buf[64];
    int fd = open("/proc/self/oom_score_adj", O_RDONLY);

    if (fd < 0) return;
    if (read(fd, buf, sizeof(buf)) > 0)
        server.oom_score_adj_base = atoi(buf);
    close(fd);
#endif
}

/* This function will configure the current process's oom_score_adj according
 * to user specified configuration. This is currently implemented on Linux
 * only.
 *
 * A process_class value of -1 implies OOM_CONFIG_MASTER or OOM_CONFIG_REPLICA,
 * depending on current role.
 */
int setOOMScoreAdj(int process_class) {

    if (server.oom_score_adj == OOM_SCORE_ADJ_NO) return C_OK;
    if (process_class == -1)
        process_class = (server.masterhost ? CONFIG_OOM_REPLICA : CONFIG_OOM_MASTER);

    serverAssert(process_class >= 0 && process_class < CONFIG_OOM_COUNT);

#ifdef HAVE_PROC_OOM_SCORE_ADJ
    int fd;
    int val;
    char buf[64];

    val = server.oom_score_adj_values[process_class];
    if (server.oom_score_adj == OOM_SCORE_RELATIVE)
        val += server.oom_score_adj_base;
    if (val > 1000) val = 1000;
    if (val < -1000) val = -1000;

    snprintf(buf, sizeof(buf) - 1, "%d\n", val);

    fd = open("/proc/self/oom_score_adj", O_WRONLY);
    if (fd < 0 || write(fd, buf, strlen(buf)) < 0) {
        serverLog(LOG_WARNING, "Unable to write oom_score_adj: %s", strerror(errno));
        if (fd != -1) close(fd);
        return C_ERR;
    }

    close(fd);
    return C_OK;
#else
    /* Unsupported */
    return C_ERR;
#endif
}

/* This function will try to raise the max number of open files accordingly to
 * the configured max number of clients. It also reserves a number of file
 * descriptors (CONFIG_MIN_RESERVED_FDS) for extra operations of
 * persistence, listening sockets, log files and so forth.
 *
 * If it will not be possible to set the limit accordingly to the configured
 * max number of clients, the function will do the reverse setting
 * server.maxclients to the value that we can actually handle. */
void adjustOpenFilesLimit(void) {
    rlim_t maxfiles = server.maxclients+CONFIG_MIN_RESERVED_FDS;
    struct rlimit limit;

    if (getrlimit(RLIMIT_NOFILE,&limit) == -1) {
        serverLog(LL_WARNING,"Unable to obtain the current NOFILE limit (%s), assuming 1024 and setting the max clients configuration accordingly.",
            strerror(errno));
        server.maxclients = 1024-CONFIG_MIN_RESERVED_FDS;
    } else {
        rlim_t oldlimit = limit.rlim_cur;

        /* Set the max number of files if the current limit is not enough
         * for our needs. */
        if (oldlimit < maxfiles) {
            rlim_t bestlimit;
            int setrlimit_error = 0;

            /* Try to set the file limit to match 'maxfiles' or at least
             * to the higher value supported less than maxfiles. */
            bestlimit = maxfiles;
            while(bestlimit > oldlimit) {
                rlim_t decr_step = 16;

                limit.rlim_cur = bestlimit;
                limit.rlim_max = bestlimit;
                if (setrlimit(RLIMIT_NOFILE,&limit) != -1) break;
                setrlimit_error = errno;

                /* We failed to set file limit to 'bestlimit'. Try with a
                 * smaller limit decrementing by a few FDs per iteration. */
                if (bestlimit < decr_step) break;
                bestlimit -= decr_step;
            }

            /* Assume that the limit we get initially is still valid if
             * our last try was even lower. */
            if (bestlimit < oldlimit) bestlimit = oldlimit;

            if (bestlimit < maxfiles) {
                unsigned int old_maxclients = server.maxclients;
                server.maxclients = bestlimit-CONFIG_MIN_RESERVED_FDS;
                /* maxclients is unsigned so may overflow: in order
                 * to check if maxclients is now logically less than 1
                 * we test indirectly via bestlimit. */
                if (bestlimit <= CONFIG_MIN_RESERVED_FDS) {
                    serverLog(LL_WARNING,"Your current 'ulimit -n' "
                        "of %llu is not enough for the server to start. "
                        "Please increase your open file limit to at least "
                        "%llu. Exiting.",
                        (unsigned long long) oldlimit,
                        (unsigned long long) maxfiles);
                    exit(1);
                }
                serverLog(LL_WARNING,"You requested maxclients of %d "
                    "requiring at least %llu max file descriptors.",
                    old_maxclients,
                    (unsigned long long) maxfiles);
                serverLog(LL_WARNING,"Server can't set maximum open files "
                    "to %llu because of OS error: %s.",
                    (unsigned long long) maxfiles, strerror(setrlimit_error));
                serverLog(LL_WARNING,"Current maximum open files is %llu. "
                    "maxclients has been reduced to %d to compensate for "
                    "low ulimit. "
                    "If you need higher maxclients increase 'ulimit -n'.",
                    (unsigned long long) bestlimit, server.maxclients);
            } else {
                serverLog(LL_NOTICE,"Increased maximum number of open files "
                    "to %llu (it was originally set to %llu).",
                    (unsigned long long) maxfiles,
                    (unsigned long long) oldlimit);
            }
        }
    }
}

/* Check that server.tcp_backlog can be actually enforced in Linux according
 * to the value of /proc/sys/net/core/somaxconn, or warn about it. */
void checkTcpBacklogSettings(void) {
#ifdef HAVE_PROC_SOMAXCONN
    FILE *fp = fopen("/proc/sys/net/core/somaxconn","r");
    char buf[1024];
    if (!fp) return;
    if (fgets(buf,sizeof(buf),fp) != NULL) {
        int somaxconn = atoi(buf);
        if (somaxconn > 0 && somaxconn < server.tcp_backlog) {
            serverLog(LL_WARNING,"WARNING: The TCP backlog setting of %d cannot be enforced because /proc/sys/net/core/somaxconn is set to the lower value of %d.", server.tcp_backlog, somaxconn);
        }
    }
    fclose(fp);
#endif
}

void closeSocketListeners(socketFds *sfd) {
    int j;

    for (j = 0; j < sfd->count; j++) {
        if (sfd->fd[j] == -1) continue;

        aeDeleteFileEvent(server.el, sfd->fd[j], AE_READABLE);
        close(sfd->fd[j]);
    }

    sfd->count = 0;
}

/* Create an event handler for accepting new connections in TCP or TLS domain sockets.
 * This works atomically for all socket fds */
// 创建事件处理handler，用于处理新的TCP/TLS连接。
int createSocketAcceptHandler(socketFds *sfd, aeFileProc *accept_handler) {
    int j;

    for (j = 0; j < sfd->count; j++) {
        if (aeCreateFileEvent(server.el, sfd->fd[j], AE_READABLE, accept_handler,NULL) == AE_ERR) {
            /* Rollback */
            for (j = j-1; j >= 0; j--) aeDeleteFileEvent(server.el, sfd->fd[j], AE_READABLE);
            return C_ERR;
        }
    }
    return C_OK;
}

/* Initialize a set of file descriptors to listen to the specified 'port'
 * binding the addresses specified in the Redis server configuration.
 *
 * The listening file descriptors are stored in the integer array 'fds'
 * and their number is set in '*count'.
 *
 * The addresses to bind are specified in the global server.bindaddr array
 * and their number is server.bindaddr_count. If the server configuration
 * contains no specific addresses to bind, this function will try to
 * bind * (all addresses) for both the IPv4 and IPv6 protocols.
 *
 * On success the function returns C_OK.
 *
 * On error the function returns C_ERR. For the function to be on
 * error, at least one of the server.bindaddr addresses was
 * impossible to bind, or no bind addresses were specified in the server
 * configuration but the function is not able to bind * for at least
 * one of the IPv4 or IPv6 protocols. */
// 根据配置，初始化一系列fd绑定到指定地址端口，并进行监听。监听的fd都存储在socketFds的fd数组中。
// 绑定地址和数量存储在全局变量server.bindaddr数组和server.bindaddr_count中，如果没有设置，会默认绑定到"*"，即所有地址。
int listenToPort(int port, socketFds *sfd) {
    int j;
    char **bindaddr = server.bindaddr;
    int bindaddr_count = server.bindaddr_count;
    char *default_bindaddr[2] = {"*", "-::*"};

    /* Force binding of 0.0.0.0 if no bind address is specified. */
    // 如果没有指定绑定地址，强制绑定到0.0.0.0
    if (server.bindaddr_count == 0) {
        bindaddr_count = 2;
        bindaddr = default_bindaddr;
    }

    for (j = 0; j < bindaddr_count; j++) {
        char* addr = bindaddr[j];
        int optional = *addr == '-';
        if (optional) addr++;
        if (strchr(addr,':')) {
            /* Bind IPv6 address. */
            // 处理IPv6地址绑定
            sfd->fd[sfd->count] = anetTcp6Server(server.neterr,port,addr,server.tcp_backlog);
        } else {
            /* Bind IPv4 address. */
            // 处理IPv4地址绑定
            sfd->fd[sfd->count] = anetTcpServer(server.neterr,port,addr,server.tcp_backlog);
        }
        // 绑定出错了需要处理
        if (sfd->fd[sfd->count] == ANET_ERR) {
            int net_errno = errno;
            serverLog(LL_WARNING,
                "Warning: Could not create server TCP listening socket %s:%d: %s",
                addr, port, server.neterr);
            if (net_errno == EADDRNOTAVAIL && optional)
                continue;
            if (net_errno == ENOPROTOOPT     || net_errno == EPROTONOSUPPORT ||
                net_errno == ESOCKTNOSUPPORT || net_errno == EPFNOSUPPORT ||
                net_errno == EAFNOSUPPORT)
                continue;

            /* Rollback successful listens before exiting */
            // 如果遇到未知错误，回滚已经成功listen的fd
            closeSocketListeners(sfd);
            return C_ERR;
        }
        // 监听的fd设置为非阻塞
        anetNonBlock(NULL,sfd->fd[sfd->count]);
        // 设置fd的FD_CLOEXEC标识，用于fork的子进程将自己空间的fd close掉。
        anetCloexec(sfd->fd[sfd->count]);
        // 成功bind的fd数+1
        sfd->count++;
    }
    return C_OK;
}

/* Resets the stats that we expose via INFO or other means that we want
 * to reset via CONFIG RESETSTAT. The function is also used in order to
 * initialize these fields in initServer() at server startup. */
void resetServerStats(void) {
    int j;

    server.stat_numcommands = 0;
    server.stat_numconnections = 0;
    server.stat_expiredkeys = 0;
    server.stat_expired_stale_perc = 0;
    server.stat_expired_time_cap_reached_count = 0;
    server.stat_expire_cycle_time_used = 0;
    server.stat_evictedkeys = 0;
    server.stat_keyspace_misses = 0;
    server.stat_keyspace_hits = 0;
    server.stat_active_defrag_hits = 0;
    server.stat_active_defrag_misses = 0;
    server.stat_active_defrag_key_hits = 0;
    server.stat_active_defrag_key_misses = 0;
    server.stat_active_defrag_scanned = 0;
    server.stat_fork_time = 0;
    server.stat_fork_rate = 0;
    server.stat_total_forks = 0;
    server.stat_rejected_conn = 0;
    server.stat_sync_full = 0;
    server.stat_sync_partial_ok = 0;
    server.stat_sync_partial_err = 0;
    server.stat_io_reads_processed = 0;
    atomicSet(server.stat_total_reads_processed, 0);
    server.stat_io_writes_processed = 0;
    atomicSet(server.stat_total_writes_processed, 0);
    for (j = 0; j < STATS_METRIC_COUNT; j++) {
        server.inst_metric[j].idx = 0;
        server.inst_metric[j].last_sample_time = mstime();
        server.inst_metric[j].last_sample_count = 0;
        memset(server.inst_metric[j].samples,0,
            sizeof(server.inst_metric[j].samples));
    }
    atomicSet(server.stat_net_input_bytes, 0);
    atomicSet(server.stat_net_output_bytes, 0);
    server.stat_unexpected_error_replies = 0;
    server.stat_total_error_replies = 0;
    server.stat_dump_payload_sanitizations = 0;
    server.aof_delayed_fsync = 0;
}

/* Make the thread killable at any time, so that kill threads functions
 * can work reliably (default cancelability type is PTHREAD_CANCEL_DEFERRED).
 * Needed for pthread_cancel used by the fast memory test used by the crash report. */
void makeThreadKillable(void) {
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
}

void initServer(void) {
    int j;

    // 设置服务信号处理handler
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    setupSignalHandlers();
    // 设置线程可被kill
    makeThreadKillable();

    if (server.syslog_enabled) {
        openlog(server.syslog_ident, LOG_PID | LOG_NDELAY | LOG_NOWAIT,
            server.syslog_facility);
    }

    /* Initialization after setting defaults from the config system. */
    // 初始化服务的一些数据结构
    server.aof_state = server.aof_enabled ? AOF_ON : AOF_OFF;
    server.hz = server.config_hz;
    server.pid = getpid();
    server.in_fork_child = CHILD_TYPE_NONE;
    server.main_thread_id = pthread_self();
    server.current_client = NULL;
    server.errors = raxNew();
    server.fixed_time_expire = 0;
    server.clients = listCreate();
    server.clients_index = raxNew();
    server.clients_to_close = listCreate();
    server.slaves = listCreate();
    server.monitors = listCreate();
    server.clients_pending_write = listCreate();
    server.clients_pending_read = listCreate();
    server.clients_timeout_table = raxNew();
    server.replication_allowed = 1;
    server.slaveseldb = -1; /* Force to emit the first SELECT command. */
    server.unblocked_clients = listCreate();
    server.ready_keys = listCreate();
    server.clients_waiting_acks = listCreate();
    server.get_ack_from_slaves = 0;
    server.client_pause_type = 0;
    server.paused_clients = listCreate();
    server.events_processed_while_blocked = 0;
    server.system_memory_size = zmalloc_get_memory_size();
    server.blocked_last_cron = 0;
    server.blocking_op_nesting = 0;

    if ((server.tls_port || server.tls_replication || server.tls_cluster)
                && tlsConfigure(&server.tls_ctx_config) == C_ERR) {
        serverLog(LL_WARNING, "Failed to configure TLS. Check logs for more info.");
        exit(1);
    }

    // 创建全局共享的对象
    createSharedObjects();
    // 调整打开的fd限制，会修改server.maxclients
    adjustOpenFilesLimit();
    const char *clk_msg = monotonicInit();
    serverLog(LL_NOTICE, "monotonic clock: %s", clk_msg);
    // 创建EventLoop结构
    server.el = aeCreateEventLoop(server.maxclients+CONFIG_FDSET_INCR);
    if (server.el == NULL) {
        serverLog(LL_WARNING,
            "Failed creating the event loop. Error message: '%s'",
            strerror(errno));
        exit(1);
    }
    // 为redis db分配内存
    server.db = zmalloc(sizeof(redisDb)*server.dbnum);

    /* Open the TCP listening socket for the user commands. */
    // 创建TCP连接监听的socket，用于处理TCP连接
    // 传入端口为server.port，默认6379，绑定成功的fd位于server.ipfd中。
    if (server.port != 0 &&
        listenToPort(server.port,&server.ipfd) == C_ERR) {
        serverLog(LL_WARNING, "Failed listening on port %u (TCP), aborting.", server.port);
        exit(1);
    }
    // 创建TLS端口监听的TCP连接
    if (server.tls_port != 0 &&
        listenToPort(server.tls_port,&server.tlsfd) == C_ERR) {
        serverLog(LL_WARNING, "Failed listening on port %u (TLS), aborting.", server.tls_port);
        exit(1);
    }

    /* Open the listening Unix domain socket. */
    // 打开Unix域套接字UDS
    if (server.unixsocket != NULL) {
        // unlink，删除指定的文件
        unlink(server.unixsocket); /* don't care if this fails */
        // .sock 文件很可能是UNIX域套接字，即通过文件系统（而非网络地址）进行寻址和访问的套接字。
        // 启动Unix域套接字监听
        server.sofd = anetUnixServer(server.neterr,server.unixsocket,
            server.unixsocketperm, server.tcp_backlog);
        if (server.sofd == ANET_ERR) {
            serverLog(LL_WARNING, "Opening Unix socket: %s", server.neterr);
            exit(1);
        }
        // 设置非阻塞
        anetNonBlock(NULL,server.sofd);
        // 设置fd的FD_CLOEXEC标识，用于fork的子进程将fd close掉。
        anetCloexec(server.sofd);
    }

    /* Abort if there are no listening sockets at all. */
    // 如果没有正在监听的socket，退出
    if (server.ipfd.count == 0 && server.tlsfd.count == 0 && server.sofd < 0) {
        serverLog(LL_WARNING, "Configured to not listen anywhere, exiting.");
        exit(1);
    }

    /* Create the Redis databases, and initialize other internal state. */
    // 创建redis DB，并初始化其他的服务内部状态
    for (j = 0; j < server.dbnum; j++) {
        // 初始化DB
        server.db[j].dict = dictCreate(&dbDictType,NULL);
        server.db[j].expires = dictCreate(&dbExpiresDictType,NULL);
        server.db[j].expires_cursor = 0;
        server.db[j].blocking_keys = dictCreate(&keylistDictType,NULL);
        server.db[j].ready_keys = dictCreate(&objectKeyPointerValueDictType,NULL);
        server.db[j].watched_keys = dictCreate(&keylistDictType,NULL);
        server.db[j].id = j;
        server.db[j].avg_ttl = 0;
        server.db[j].defrag_later = listCreate();
        listSetFreeMethod(server.db[j].defrag_later,(void (*)(void*))sdsfree);
    }
    // 初始化驱逐池
    evictionPoolAlloc(); /* Initialize the LRU keys pool. */
    // 初始化pubsub字典，channel/pattern -> clients
    server.pubsub_channels = dictCreate(&keylistDictType,NULL);
    server.pubsub_patterns = dictCreate(&keylistDictType,NULL);
    server.cronloops = 0;
    // 是否在执行脚本，或执行事务期间
    server.in_eval = 0;
    server.in_exec = 0;
    server.propagate_in_transaction = 0;
    server.client_pause_in_transaction = 0;
    server.child_pid = -1;
    server.child_type = CHILD_TYPE_NONE;
    server.rdb_child_type = RDB_CHILD_TYPE_NONE;
    server.rdb_pipe_conns = NULL;
    server.rdb_pipe_numconns = 0;
    server.rdb_pipe_numconns_writing = 0;
    server.rdb_pipe_buff = NULL;
    server.rdb_pipe_bufflen = 0;
    server.rdb_bgsave_scheduled = 0;
    server.child_info_pipe[0] = -1;
    server.child_info_pipe[1] = -1;
    server.child_info_nread = 0;
    aofRewriteBufferReset();
    server.aof_buf = sdsempty();
    server.lastsave = time(NULL); /* At startup we consider the DB saved. */
    server.lastbgsave_try = 0;    /* At startup we never tried to BGSAVE. */
    server.rdb_save_time_last = -1;
    server.rdb_save_time_start = -1;
    server.dirty = 0;
    resetServerStats();
    /* A few stats we don't want to reset: server startup time, and peak mem. */
    server.stat_starttime = time(NULL);
    server.stat_peak_memory = 0;
    server.stat_current_cow_bytes = 0;
    server.stat_current_cow_updated = 0;
    server.stat_current_save_keys_processed = 0;
    server.stat_current_save_keys_total = 0;
    server.stat_rdb_cow_bytes = 0;
    server.stat_aof_cow_bytes = 0;
    server.stat_module_cow_bytes = 0;
    server.stat_module_progress = 0;
    for (int j = 0; j < CLIENT_TYPE_COUNT; j++)
        server.stat_clients_type_memory[j] = 0;
    server.cron_malloc_stats.zmalloc_used = 0;
    server.cron_malloc_stats.process_rss = 0;
    server.cron_malloc_stats.allocator_allocated = 0;
    server.cron_malloc_stats.allocator_active = 0;
    server.cron_malloc_stats.allocator_resident = 0;
    server.lastbgsave_status = C_OK;
    server.aof_last_write_status = C_OK;
    server.aof_last_write_errno = 0;
    server.repl_good_slaves_count = 0;

    /* Create the timer callback, this is our way to process many background
     * operations incrementally, like clients timeout, eviction of unaccessed
     * expired keys and so forth. */
    // 创建定时任务serverCron，用于执行许多渐进处理的后台操作，如处理client超时，过期key清理等
    if (aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL) == AE_ERR) {
        serverPanic("Can't create event loop timers.");
        exit(1);
    }

    /* Create an event handler for accepting new connections in TCP and Unix
     * domain sockets. */
    // 创建event handler处理TCP连接事件，accept处理函数为acceptTcpHandler
    // 因为前面创建的这些socket都是非阻塞的，所以这里要利用多路复用来处理连接请求，否则直接调用accept会立即返回，没有连接时形成死循环调用。
    // 当有连接进来时会先利用系统调用accept建立底层连接conn，然后调用acceptCommonHandler来创建socket及client，处理业务层的连接操作。
    if (createSocketAcceptHandler(&server.ipfd, acceptTcpHandler) != C_OK) {
        serverPanic("Unrecoverable error creating TCP socket accept handler.");
    }
    if (createSocketAcceptHandler(&server.tlsfd, acceptTLSHandler) != C_OK) {
        serverPanic("Unrecoverable error creating TLS socket accept handler.");
    }
    if (server.sofd > 0 && aeCreateFileEvent(server.el,server.sofd,AE_READABLE,
        acceptUnixHandler,NULL) == AE_ERR) serverPanic("Unrecoverable error creating server.sofd file event.");


    /* Register a readable event for the pipe used to awake the event loop
     * when a blocked client in a module needs attention. */
    // 当module中一个阻塞的client需要处理时，为管道注册一个可读事件用于唤醒event loop。
    if (aeCreateFileEvent(server.el, server.module_blocked_pipe[0], AE_READABLE,
        moduleBlockedClientPipeReadable,NULL) == AE_ERR) {
            serverPanic(
                "Error registering the readable event for the module "
                "blocked clients subsystem.");
    }

    /* Register before and after sleep handlers (note this needs to be done
     * before loading persistence since it is used by processEventsWhileBlocked. */
    // 注册before and after sleep处理函数。
    // 注意这需要在加载持久化数据之前完成，因为加载数据时，如果超时会调用processEventsWhileBlocked函数进行事件处理逻辑。
    // 进一步select事件前后会调用beforeSleep和afterSleep。所以需要先注册。
    aeSetBeforeSleepProc(server.el,beforeSleep);
    aeSetAfterSleepProc(server.el,afterSleep);

    /* Open the AOF file if needed. */
    // 如果AOF是开启的，这里打开aof文件。
    if (server.aof_state == AOF_ON) {
        server.aof_fd = open(server.aof_filename,
                               O_WRONLY|O_APPEND|O_CREAT,0644);
        if (server.aof_fd == -1) {
            serverLog(LL_WARNING, "Can't open the append-only file: %s",
                strerror(errno));
            exit(1);
        }
    }

    /* 32 bit instances are limited to 4GB of address space, so if there is
     * no explicit limit in the user provided configuration we set a limit
     * at 3 GB using maxmemory with 'noeviction' policy'. This avoids
     * useless crashes of the Redis instance for out of memory. */
    // 32bit的系统架构限制4GB的寻址空间，所以如果没有限制最大内存使用的话，我们这里限制3GB内存并使用noeviction策略。
    // 避免因为redis实例内存使用超出而服务挂掉。
    if (server.arch_bits == 32 && server.maxmemory == 0) {
        serverLog(LL_WARNING,"Warning: 32 bit instance detected but no memory limit set. Setting 3 GB maxmemory limit with 'noeviction' policy now.");
        server.maxmemory = 3072LL*(1024*1024); /* 3 GB */
        server.maxmemory_policy = MAXMEMORY_NO_EVICTION;
    }

    // 如果是集群模式，这里做集群初始化操作
    if (server.cluster_enabled) clusterInit();
    // 初始化副本脚本缓存
    replicationScriptCacheInit();
    // 初始化脚本执行环境
    scriptingInit(1);
    // 初始化慢查询日志
    slowlogInit();
    // 初始化延时监控系统
    latencyMonitorInit();
    
    /* Initialize ACL default password if it exists */
    // 初始化ACL默认的password
    ACLUpdateDefaultUserPassword(server.requirepass);
}

/* Some steps in server initialization need to be done last (after modules
 * are loaded).
 * Specifically, creation of threads due to a race bug in ld.so, in which
 * Thread Local Storage initialization collides with dlopen call.
 * see: https://sourceware.org/bugzilla/show_bug.cgi?id=19329 */
void InitServerLast() {
    bioInit();
    initThreadedIO();
    set_jemalloc_bg_thread(server.jemalloc_bg_thread);
    server.initial_memory_usage = zmalloc_used_memory();
}

/* Parse the flags string description 'strflags' and set them to the
 * command 'c'. If the flags are all valid C_OK is returned, otherwise
 * C_ERR is returned (yet the recognized flags are set in the command). */
int populateCommandTableParseFlags(struct redisCommand *c, char *strflags) {
    int argc;
    sds *argv;

    /* Split the line into arguments for processing. */
    // 空格切分sflags，生成argv列表
    argv = sdssplitargs(strflags,&argc);
    if (argv == NULL) return C_ERR;

    // 对每个flag参数处理，设置命令的标识
    for (int j = 0; j < argc; j++) {
        char *flag = argv[j];
        if (!strcasecmp(flag,"write")) {
            c->flags |= CMD_WRITE|CMD_CATEGORY_WRITE;
        } else if (!strcasecmp(flag,"read-only")) {
            c->flags |= CMD_READONLY|CMD_CATEGORY_READ;
        } else if (!strcasecmp(flag,"use-memory")) {
            c->flags |= CMD_DENYOOM;
        } else if (!strcasecmp(flag,"admin")) {
            c->flags |= CMD_ADMIN|CMD_CATEGORY_ADMIN|CMD_CATEGORY_DANGEROUS;
        } else if (!strcasecmp(flag,"pub-sub")) {
            c->flags |= CMD_PUBSUB|CMD_CATEGORY_PUBSUB;
        } else if (!strcasecmp(flag,"no-script")) {
            c->flags |= CMD_NOSCRIPT;
        } else if (!strcasecmp(flag,"random")) {
            c->flags |= CMD_RANDOM;
        } else if (!strcasecmp(flag,"to-sort")) {
            c->flags |= CMD_SORT_FOR_SCRIPT;
        } else if (!strcasecmp(flag,"ok-loading")) {
            c->flags |= CMD_LOADING;
        } else if (!strcasecmp(flag,"ok-stale")) {
            c->flags |= CMD_STALE;
        } else if (!strcasecmp(flag,"no-monitor")) {
            c->flags |= CMD_SKIP_MONITOR;
        } else if (!strcasecmp(flag,"no-slowlog")) {
            c->flags |= CMD_SKIP_SLOWLOG;
        } else if (!strcasecmp(flag,"cluster-asking")) {
            c->flags |= CMD_ASKING;
        } else if (!strcasecmp(flag,"fast")) {
            c->flags |= CMD_FAST | CMD_CATEGORY_FAST;
        } else if (!strcasecmp(flag,"no-auth")) {
            c->flags |= CMD_NO_AUTH;
        } else if (!strcasecmp(flag,"may-replicate")) {
            c->flags |= CMD_MAY_REPLICATE;
        } else {
            /* Parse ACL categories here if the flag name starts with @. */
            uint64_t catflag;
            if (flag[0] == '@' &&
                (catflag = ACLGetCommandCategoryFlagByName(flag+1)) != 0)
            {
                c->flags |= catflag;
            } else {
                sdsfreesplitres(argv,argc);
                return C_ERR;
            }
        }
    }
    /* If it's not @fast is @slow in this binary world. */
    // 如果不是fast命令则就是slow命令
    if (!(c->flags & CMD_CATEGORY_FAST)) c->flags |= CMD_CATEGORY_SLOW;

    sdsfreesplitres(argv,argc);
    return C_OK;
}

/* Populates the Redis Command Table starting from the hard coded list
 * we have on top of server.c file. */
// 通过代码中的redisCommandTable，来生成Redis命令表
void populateCommandTable(void) {
    int j;
    // 计算redisCommandTable中总的命令数
    int numcommands = sizeof(redisCommandTable)/sizeof(struct redisCommand);

    for (j = 0; j < numcommands; j++) {
        // 循环处理每一个命令
        struct redisCommand *c = redisCommandTable+j;
        int retval1, retval2;

        /* Translate the command string flags description into an actual
         * set of flags. */
        // 将单个命令的字符串sflags，解析成真实代码中使用的flags
        if (populateCommandTableParseFlags(c,c->sflags) == C_ERR)
            serverPanic("Unsupported command flag");

        // 根据name获取命令的id
        c->id = ACLGetCommandID(c->name); /* Assign the ID used for ACL. */
        // 命令加入commands表中，这个表后面是可以通过重命名来修改某些命令name的。
        retval1 = dictAdd(server.commands, sdsnew(c->name), c);
        /* Populate an additional dictionary that will be unaffected
         * by rename-command statements in redis.conf. */
        // 同时也加入orig_commands表中，这个表中的命令后面不会被rename-command来进行name修改。
        retval2 = dictAdd(server.orig_commands, sdsnew(c->name), c);
        serverAssert(retval1 == DICT_OK && retval2 == DICT_OK);
    }
}

void resetCommandTableStats(void) {
    struct redisCommand *c;
    dictEntry *de;
    dictIterator *di;

    di = dictGetSafeIterator(server.commands);
    while((de = dictNext(di)) != NULL) {
        c = (struct redisCommand *) dictGetVal(de);
        c->microseconds = 0;
        c->calls = 0;
        c->rejected_calls = 0;
        c->failed_calls = 0;
    }
    dictReleaseIterator(di);

}

void resetErrorTableStats(void) {
    raxFreeWithCallback(server.errors, zfree);
    server.errors = raxNew();
}

/* ========================== Redis OP Array API ============================ */

// 初始化redis操作数组
void redisOpArrayInit(redisOpArray *oa) {
    oa->ops = NULL;
    oa->numops = 0;
}

// 向redis操作数组中加入命令
int redisOpArrayAppend(redisOpArray *oa, struct redisCommand *cmd, int dbid,
                       robj **argv, int argc, int target)
{
    redisOp *op;

    // 重新分配存储
    oa->ops = zrealloc(oa->ops,sizeof(redisOp)*(oa->numops+1));
    // 找到新增加的操作位置，首地址+便宜
    op = oa->ops+oa->numops;
    // 设置新增加操作的属性
    op->cmd = cmd;
    op->dbid = dbid;
    op->argv = argv;
    op->argc = argc;
    op->target = target;
    // 更新数组中操作的数量
    oa->numops++;
    return oa->numops;
}

// 释放redis操作数组中命令的参数。
void redisOpArrayFree(redisOpArray *oa) {
    // 遍历挨个释放
    while(oa->numops) {
        int j;
        redisOp *op;

        oa->numops--;
        // 这里是从后往前处理的
        // 一般只有redis obj需要释放，所以只处理了参数对象
        op = oa->ops+oa->numops;
        for (j = 0; j < op->argc; j++)
            decrRefCount(op->argv[j]);
        zfree(op->argv);
    }
    // 释放redisOp整个结构
    zfree(oa->ops);
    oa->ops = NULL;
}

/* ====================== Commands lookup and execution ===================== */

struct redisCommand *lookupCommand(sds name) {
    return dictFetchValue(server.commands, name);
}

struct redisCommand *lookupCommandByCString(const char *s) {
    struct redisCommand *cmd;
    sds name = sdsnew(s);

    cmd = dictFetchValue(server.commands, name);
    sdsfree(name);
    return cmd;
}

/* Lookup the command in the current table, if not found also check in
 * the original table containing the original command names unaffected by
 * redis.conf rename-command statement.
 *
 * This is used by functions rewriting the argument vector such as
 * rewriteClientCommandVector() in order to set client->cmd pointer
 * correctly even if the command was renamed. */
// 根据name查询命令（先查询当前命令表，再查询原始命令表）。
// 主要在重写命令参数向量时查找命令。
struct redisCommand *lookupCommandOrOriginal(sds name) {
    // 先从当前command表查询
    struct redisCommand *cmd = dictFetchValue(server.commands, name);

    // 如果没查到，再从原始没使用rename-command语句改名的command列表查询。
    if (!cmd) cmd = dictFetchValue(server.orig_commands,name);
    return cmd;
}

/* Propagate the specified command (in the context of the specified database id)
 * to AOF and Slaves.
 *
 * flags are an xor between:
 * + PROPAGATE_NONE (no propagation of command at all)
 * + PROPAGATE_AOF (propagate into the AOF file if is enabled)
 * + PROPAGATE_REPL (propagate into the replication link)
 *
 * This should not be used inside commands implementation since it will not
 * wrap the resulting commands in MULTI/EXEC. Use instead alsoPropagate(),
 * preventCommandPropagation(), forceCommandPropagation().
 *
 * However for functions that need to (also) propagate out of the context of a
 * command execution, for example when serving a blocked client, you
 * want to use propagate().
 */
// 传播指定DB的指定命令到AOF和slaves。flags是PROPAGATE_NONE、PROPAGATE_AOF、PROPAGATE_REPL这三个的异或。
// 这个方法不应在命令实现中使用，因为它不会将结果命令包装在MULTI/EXEC中。
// 应该使用alsoPropagate、preventCommandPropagation、forceCommandPropagation来替代，打上传播标识，然后最终在这里处理。
// 但是对于在命令执行以外的context（如处理blocked的client时）中也需要执行传播的函数来说，也可以调用这个函数处理。
void propagate(struct redisCommand *cmd, int dbid, robj **argv, int argc,
               int flags)
{
    // 如果server不允许执行复制，即不允许传播，直接返回。
    if (!server.replication_allowed)
        return;

    /* Propagate a MULTI request once we encounter the first command which
     * is a write command.
     * This way we'll deliver the MULTI/..../EXEC block as a whole and
     * both the AOF and the replication link will have the same consistency
     * and atomicity guarantees. */
    // 事务EXEC命令执行时，一旦我们遇到第一个写命令时，才传播MULTI请求，并且期间MULTI只传播一次。
    // 这个样我们传播MULTI/..../EXEC作为一个整体，保证AOF和slaves传播数据有相同的一致性和原子性。
    // in_exec标识执行事务EXEC执行期间，propagate_in_transaction标识是否有开始传播事务。
    if (server.in_exec && !server.propagate_in_transaction)
        // 事务执行期间第一次传播，需要加上MULTI命令
        execCommandPropagateMulti(dbid);

    /* This needs to be unreachable since the dataset should be fixed during 
     * client pause, otherwise data may be lossed during a failover. */
    // 在client暂停期间，数据应该是固定不变的，否则故障转移的时候可能引起数据丢失。
    // 也就是说当服务处于client暂停状态并且不是在执行EXEC事务命令期间暂停的话，是不可能执行到这里的。
    // 即不是client暂停状态，或者暂停状态发生于事务执行命令期间，这样才可能执行到这里。
    serverAssert(!(areClientsPaused() && !server.client_pause_in_transaction));

    // 如果AOF是开启的，且设置传播AOF，处理命令写入AOF
    if (server.aof_state != AOF_OFF && flags & PROPAGATE_AOF)
        feedAppendOnlyFile(cmd,dbid,argv,argc);
    // 如果设置传播slaves，处理数据发送到slaves
    if (flags & PROPAGATE_REPL)
        replicationFeedSlaves(server.slaves,dbid,argv,argc);
}

/* Used inside commands to schedule the propagation of additional commands
 * after the current command is propagated to AOF / Replication.
 *
 * 'cmd' must be a pointer to the Redis command to replicate, dbid is the
 * database ID the command should be propagated into.
 * Arguments of the command to propagate are passed as an array of redis
 * objects pointers of len 'argc', using the 'argv' vector.
 *
 * The function does not take a reference to the passed 'argv' vector,
 * so it is up to the caller to release the passed argv (but it is usually
 * stack allocated).  The function automatically increments ref count of
 * passed objects, so the caller does not need to. */
void alsoPropagate(struct redisCommand *cmd, int dbid, robj **argv, int argc,
                   int target)
{
    robj **argvcopy;
    int j;

    if (server.loading) return; /* No propagation during loading. */

    argvcopy = zmalloc(sizeof(robj*)*argc);
    for (j = 0; j < argc; j++) {
        argvcopy[j] = argv[j];
        incrRefCount(argv[j]);
    }
    redisOpArrayAppend(&server.also_propagate,cmd,dbid,argvcopy,argc,target);
}

/* It is possible to call the function forceCommandPropagation() inside a
 * Redis command implementation in order to to force the propagation of a
 * specific command execution into AOF / Replication. */
void forceCommandPropagation(client *c, int flags) {
    serverAssert(c->cmd->flags & (CMD_WRITE | CMD_MAY_REPLICATE));
    if (flags & PROPAGATE_REPL) c->flags |= CLIENT_FORCE_REPL;
    if (flags & PROPAGATE_AOF) c->flags |= CLIENT_FORCE_AOF;
}

/* Avoid that the executed command is propagated at all. This way we
 * are free to just propagate what we want using the alsoPropagate()
 * API. */
void preventCommandPropagation(client *c) {
    c->flags |= CLIENT_PREVENT_PROP;
}

/* AOF specific version of preventCommandPropagation(). */
void preventCommandAOF(client *c) {
    c->flags |= CLIENT_PREVENT_AOF_PROP;
}

/* Replication specific version of preventCommandPropagation(). */
void preventCommandReplication(client *c) {
    c->flags |= CLIENT_PREVENT_REPL_PROP;
}

/* Log the last command a client executed into the slowlog. */
// 记录client最后执行的命令到slow log
void slowlogPushCurrentCommand(client *c, struct redisCommand *cmd, ustime_t duration) {
    /* Some commands may contain sensitive data that should not be available in the slowlog. */
    // 一些命令可能包含敏感数据不应该被slowlog记录，如AUTH命令等。
    // 如果有skip log标识，不记录log，直接返回。
    if (cmd->flags & CMD_SKIP_SLOWLOG)
        return;

    /* If command argument vector was rewritten, use the original
     * arguments. */
    // 如果命令的参数向量重写过，这里使用原来的参数来进行log。
    robj **argv = c->original_argv ? c->original_argv : c->argv;
    int argc = c->original_argv ? c->original_argc : c->argc;
    // 如果需要，构建一个新的Entry加入slow log列表
    slowlogPushEntryIfNeeded(c,argv,argc,duration);
}

/* Call() is the core of Redis execution of a command.
 *
 * The following flags can be passed:
 * CMD_CALL_NONE        No flags.
 * CMD_CALL_SLOWLOG     Check command speed and log in the slow log if needed.
 * CMD_CALL_STATS       Populate command stats.
 * CMD_CALL_PROPAGATE_AOF   Append command to AOF if it modified the dataset
 *                          or if the client flags are forcing propagation.
 * CMD_CALL_PROPAGATE_REPL  Send command to slaves if it modified the dataset
 *                          or if the client flags are forcing propagation.
 * CMD_CALL_PROPAGATE   Alias for PROPAGATE_AOF|PROPAGATE_REPL.
 * CMD_CALL_FULL        Alias for SLOWLOG|STATS|PROPAGATE.
 *
 * The exact propagation behavior depends on the client flags.
 * Specifically:
 *
 * 1. If the client flags CLIENT_FORCE_AOF or CLIENT_FORCE_REPL are set
 *    and assuming the corresponding CMD_CALL_PROPAGATE_AOF/REPL is set
 *    in the call flags, then the command is propagated even if the
 *    dataset was not affected by the command.
 * 2. If the client flags CLIENT_PREVENT_REPL_PROP or CLIENT_PREVENT_AOF_PROP
 *    are set, the propagation into AOF or to slaves is not performed even
 *    if the command modified the dataset.
 *
 * Note that regardless of the client flags, if CMD_CALL_PROPAGATE_AOF
 * or CMD_CALL_PROPAGATE_REPL are not set, then respectively AOF or
 * slaves propagation will never occur.
 *
 * Client flags are modified by the implementation of a given command
 * using the following API:
 *
 * forceCommandPropagation(client *c, int flags);
 * preventCommandPropagation(client *c);
 * preventCommandAOF(client *c);
 * preventCommandReplication(client *c);
 *
 */
// core_call 这个函数是redis执行命令的核心方法。函数可以传递如下的flag：
//   CMD_CALL_NONE，没有标识。
//   CMD_CALL_SLOWLOG，检查命令执行耗时，需要的话写slow log。
//   CMD_CALL_STATS，生成命令统计。
//   CMD_CALL_PROPAGATE_AOF，如果修改了数据，命令加入AOF中；或者client flags强制写入AOF。
//   CMD_CALL_PROPAGATE_REPL，如果修改了数据，命令发送到slaves；或者client flags强制发送。
//   CMD_CALL_PROPAGATE，代表PROPAGATE_AOF|PROPAGATE_REPL，同时发到AOF和slaves。
//   CMD_CALL_FULL，代表SLOWLOG|STATS|PROPAGATE，前面几标识全有。
// 注意：准确的数据传播行为是由client flags决定。特别的（假设CMD都有PROPAGATE的两个标识）：
// 1、如果client标识有CLIENT_FORCE_AOF或CLIENT_FORCE_REPL，那么无论命令是否有修改数据，都会传播命令。
// 2、如果client标识有CLIENT_PREVENT_REPL_PROP或CLIENT_PREVENT_AOF_PROP，那么无论命令是否有修改数据，都不会传播命令。
// 另外注意：不管client flags怎么设置，如果CMD的PROPAGATE两个标识没有设置，那么向AOF或slavers的传播也都不会发生。

// client flags的修改是通过给定的命令执行如下API来处理的：
// forceCommandPropagation(client *c, int flags);
// preventCommandPropagation(client *c);
// preventCommandAOF(client *c);
// preventCommandReplication(client *c);
void call(client *c, int flags) {
    long long dirty;
    monotime call_timer;
    int client_old_flags = c->flags;
    struct redisCommand *real_cmd = c->cmd;
    static long long prev_err_count;

    // 当fixed_time_expire>0时，使用缓存的server.mstime来设置过期。反之调用mstime()获取最新时间来设置。
    // 这里没有那么精确要求，调用缓存处理即可。
    server.fixed_time_expire++;

    /* Initialization: clear the flags that must be set by the command on
     * demand, and initialize the array for additional commands propagation. */
    // 清除client的这些flags，因为命令执行时可能会根据需要再进行设置，初始化附加的命令传播数组。
    c->flags &= ~(CLIENT_FORCE_AOF|CLIENT_FORCE_REPL|CLIENT_PREVENT_PROP);
    redisOpArray prev_also_propagate = server.also_propagate;
    redisOpArrayInit(&server.also_propagate);

    /* Call the command. */
    dirty = server.dirty;
    prev_err_count = server.stat_total_error_replies;
    updateCachedTime(0);
    elapsedStart(&call_timer);
    // 执行命令
    c->cmd->proc(c);
    // 命令处理时间
    const long duration = elapsedUs(call_timer);
    c->duration = duration;
    // 检查是否DB数据有修改
    dirty = server.dirty-dirty;
    if (dirty < 0) dirty = 0;

    /* Update failed command calls if required.
     * We leverage a static variable (prev_err_count) to retain
     * the counter across nested function calls and avoid logging
     * the same error twice. */
    // 更新命令失败调用次数。即使命令有嵌套函数调用出错多次，这里也只记录失败命令调用+1，避免记录相同的error多次
    if ((server.stat_total_error_replies - prev_err_count) > 0) {
        real_cmd->failed_calls++;
    }

    /* After executing command, we will close the client after writing entire
     * reply if it is set 'CLIENT_CLOSE_AFTER_COMMAND' flag. */
    // 如果client设置了CLIENT_CLOSE_AFTER_COMMAND标识，在执行完命令后，我们将在写完所有的回复后关闭client
    if (c->flags & CLIENT_CLOSE_AFTER_COMMAND) {
        c->flags &= ~CLIENT_CLOSE_AFTER_COMMAND;
        c->flags |= CLIENT_CLOSE_AFTER_REPLY;
    }

    /* When EVAL is called loading the AOF we don't want commands called
     * from Lua to go into the slowlog or to populate statistics. */
    // 如果服务正在执行加载AOF，我们不想通过LUA脚本执行的当前命令写入slow log或加入统计。
    if (server.loading && c->flags & CLIENT_LUA)
        flags &= ~(CMD_CALL_SLOWLOG | CMD_CALL_STATS);

    /* If the caller is Lua, we want to force the EVAL caller to propagate
     * the script if the command flag or client flag are forcing the
     * propagation. */
    // 如果当前命令调用方是Lua脚本，且client或命令的flag是强制传播，那么我们强制EVAL执行者传播这个脚本。
    // client的传播标识前面有清空处理的，这里是执行命令的时候因为需要设置的。
    if (c->flags & CLIENT_LUA && server.lua_caller) {
        if (c->flags & CLIENT_FORCE_REPL)
            server.lua_caller->flags |= CLIENT_FORCE_REPL;
        if (c->flags & CLIENT_FORCE_AOF)
            server.lua_caller->flags |= CLIENT_FORCE_AOF;
    }

    /* Note: the code below uses the real command that was executed
     * c->cmd and c->lastcmd may be different, in case of MULTI-EXEC or
     * re-written commands such as EXPIRE, GEOADD, etc. */
    // 注意：后面代码使用的真实执行的命令cmd和lastcmd可能不一致，例如MULTI-EXEC或re-write（expire，geoadd等）命令。

    /* Record the latency this command induced on the main thread.
     * unless instructed by the caller not to log. (happens when processing
     * a MULTI-EXEC from inside an AOF). */
    // 除非调用者显式指示不记录log（一般在重放AOF执行MULTI-EXEC时不记录），否则这里要记录当前命令占有主线程执行的时间延迟。
    if (flags & CMD_CALL_SLOWLOG) {
        char *latency_event = (real_cmd->flags & CMD_FAST) ?
                               "fast-command" : "command";
        latencyAddSampleIfNeeded(latency_event,duration/1000);
    }

    /* Log the command into the Slow log if needed.
     * If the client is blocked we will handle slowlog when it is unblocked. */
    // 如果需要的话，将当前执行的命令写入slow log。
    // 当client处于blocked状态时，我们等它unblocked的时候在处理slowlog。
    if ((flags & CMD_CALL_SLOWLOG) && !(c->flags & CLIENT_BLOCKED))
        slowlogPushCurrentCommand(c, real_cmd, duration);

    /* Send the command to clients in MONITOR mode if applicable.
     * Administrative commands are considered too dangerous to be shown. */
    if (!(c->cmd->flags & (CMD_SKIP_MONITOR|CMD_ADMIN))) {
        robj **argv = c->original_argv ? c->original_argv : c->argv;
        int argc = c->original_argv ? c->original_argc : c->argc;
        replicationFeedMonitors(c,server.monitors,c->db->id,argv,argc);
    }

    /* Clear the original argv.
     * If the client is blocked we will handle slowlog when it is unblocked. */
    // 清除client原来的参数argv。如果client处于blocked状态，等待解除后处理。
    if (!(c->flags & CLIENT_BLOCKED))
        freeClientOriginalArgv(c);

    /* populate the per-command statistics that we show in INFO commandstats. */
    // 增加每个命令的统计，这些统计将在INFO commandstats展示。
    if (flags & CMD_CALL_STATS) {
        real_cmd->microseconds += duration;
        real_cmd->calls++;
    }

    /* Propagate the command into the AOF and replication link */
    // 传播当前命令到AOF和slave link。
    if (flags & CMD_CALL_PROPAGATE &&
        (c->flags & CLIENT_PREVENT_PROP) != CLIENT_PREVENT_PROP)
    {
        int propagate_flags = PROPAGATE_NONE;

        /* Check if the command operated changes in the data set. If so
         * set for replication / AOF propagation. */
        // 检查命令执行后数据如果有修改，默认需要传播到AOF和slave，设置传播标识
        if (dirty) propagate_flags |= (PROPAGATE_AOF|PROPAGATE_REPL);

        /* If the client forced AOF / replication of the command, set
         * the flags regardless of the command effects on the data set. */
        // 如果client强制传播AOF或slave，设置传播标识。
        if (c->flags & CLIENT_FORCE_REPL) propagate_flags |= PROPAGATE_REPL;
        if (c->flags & CLIENT_FORCE_AOF) propagate_flags |= PROPAGATE_AOF;

        /* However prevent AOF / replication propagation if the command
         * implementation called preventCommandPropagation() or similar,
         * or if we don't have the call() flags to do so. */
        // 但是如果命令调用函数设置了client不传播AOF或slave，或者命令没有设置call的传播标识，那么我们将不传播数据。
        // 也就是说如果要传播，首先需要设置call传播标识，其次需要client不阻止传播。
        // 这样才会在数据修改时默认传播。另外如果要数据没修改时也传播，那么需要设置强制传播标识。
        if (c->flags & CLIENT_PREVENT_REPL_PROP ||
            !(flags & CMD_CALL_PROPAGATE_REPL))
                propagate_flags &= ~PROPAGATE_REPL;
        if (c->flags & CLIENT_PREVENT_AOF_PROP ||
            !(flags & CMD_CALL_PROPAGATE_AOF))
                propagate_flags &= ~PROPAGATE_AOF;

        /* Call propagate() only if at least one of AOF / replication
         * propagation is needed. Note that modules commands handle replication
         * in an explicit way, so we never replicate them automatically. */
        // 经过前面判断，需要传播时，这里调用propagate进行处理。
        // 至少AOF或slave有一个需要传播时才进行处理，另外module命令会显式的处理副本传播，所以这里我们不再自动处理。
        if (propagate_flags != PROPAGATE_NONE && !(c->cmd->flags & CMD_MODULE))
            propagate(c->cmd,c->db->id,c->argv,c->argc,propagate_flags);
    }

    /* Restore the old replication flags, since call() can be executed
     * recursively. */
    // 将前面涉及传播的client flags还原。因为call可能递归调用，不能让内层执行影响外层。
    c->flags &= ~(CLIENT_FORCE_AOF|CLIENT_FORCE_REPL|CLIENT_PREVENT_PROP);
    c->flags |= client_old_flags &
        (CLIENT_FORCE_AOF|CLIENT_FORCE_REPL|CLIENT_PREVENT_PROP);

    /* Handle the alsoPropagate() API to handle commands that want to propagate
     * multiple separated commands. Note that alsoPropagate() is not affected
     * by CLIENT_PREVENT_PROP flag. */
    // 处理alsoPropagate接口来进行传播多个单独的命令。
    // 注意 alsoPropagate不受 CLIENT_PREVENT_PROP 标识影响。
    if (server.also_propagate.numops) {
        int j;
        redisOp *rop;

        if (flags & CMD_CALL_PROPAGATE) {
            int multi_emitted = 0;
            /* Wrap the commands in server.also_propagate array,
             * but don't wrap it if we are already in MULTI context,
             * in case the nested MULTI/EXEC.
             *
             * And if the array contains only one command, no need to
             * wrap it, since the single command is atomic. */
            // 针对also_propagate中的多个命令进行MULTI/EXEC包装处理。
            // 如果目前正在处理一个事务，那么我们不再进行包装了，避免嵌套的MULTI/EXEC。
            // 另外如果只有一个命令，本身就是原子操作，不需要进行包装。
            // CMD_MODULE 如果正在执行的是模块导出命令，也不包装also_propagate数组
            // CMD_CALL_NOWRAP 表示不包装also_propagate数组，调用方自己处理。
            if (server.also_propagate.numops > 1 &&
                !(c->cmd->flags & CMD_MODULE) &&
                !(c->flags & CLIENT_MULTI) &&
                !(flags & CMD_CALL_NOWRAP))
            {
                // 传播MULTI命令
                execCommandPropagateMulti(c->db->id);
                multi_emitted = 1;
            }

            for (j = 0; j < server.also_propagate.numops; j++) {
                // 按索引获取数组中的命令
                rop = &server.also_propagate.ops[j];
                int target = rop->target;
                /* Whatever the command wish is, we honor the call() flags. */
                // 处理传播标识，即使当前命令中有标识，但调用call时的flag中没设置，我们也不进行传播。
                if (!(flags&CMD_CALL_PROPAGATE_AOF)) target &= ~PROPAGATE_AOF;
                if (!(flags&CMD_CALL_PROPAGATE_REPL)) target &= ~PROPAGATE_REPL;
                if (target)
                    // 如果命令需要传播，这里处理传播逻辑
                    propagate(rop->cmd,rop->dbid,rop->argv,rop->argc,target);
            }

            if (multi_emitted) {
                // 如果是有做MULTI/EXEC包装，即前面发送了MULTI命令，那么这里需要发送EXEC命令。
                execCommandPropagateExec(c->db->id);
            }
        }
        // 释放also_propagate数组
        redisOpArrayFree(&server.also_propagate);
    }
    // 还原执行前的also_propagate，避免递归调用call引起内层影响外层。
    server.also_propagate = prev_also_propagate;

    /* Client pause takes effect after a transaction has finished. This needs
     * to be located after everything is propagated. */
    // 因为在Client pause时，我们做了判断设置了client_pause_in_transaction标识。
    // 从而在暂停期间允许整个事务里的命令（正常命令加了MULTI/EXEC包装）执行。
    // 现在事务执行完了，我们可以移除client_pause_in_transaction标识了，从而Client pause真正起作用。
    // 这个操作必须在所有的数据命令传播完成之后处理。
    if (!server.in_exec && server.client_pause_in_transaction) {
        server.client_pause_in_transaction = 0;
    }

    /* If the client has keys tracking enabled for client side caching,
     * make sure to remember the keys it fetched via this command. */
    // 如果client有开启keys track来实现client侧的缓存，这里要记录通过当前命令获取的keys。
    // 注意只有只读的client才支持。
    if (c->cmd->flags & CMD_READONLY) {
        client *caller = (c->flags & CLIENT_LUA && server.lua_caller) ?
                            server.lua_caller : c;
        if (caller->flags & CLIENT_TRACKING &&
            !(caller->flags & CLIENT_TRACKING_BCAST))
        {
            // BCAST模式不使用这个表记录，不进行这个处理。
            trackingRememberKeys(caller);
        }
    }

    // 还原fixed_time_expire
    server.fixed_time_expire--;
    // 更新一些统计信息
    server.stat_numcommands++;
    prev_err_count = server.stat_total_error_replies;

    /* Record peak memory after each command and before the eviction that runs
     * before the next command. */
    // 在每次命令执行后 并且 下一次命令执行时eviction操作之前，记录使用的内存峰值。
    size_t zmalloc_used = zmalloc_used_memory();
    if (zmalloc_used > server.stat_peak_memory)
        server.stat_peak_memory = zmalloc_used;
}

/* Used when a command that is ready for execution needs to be rejected, due to
 * varios pre-execution checks. it returns the appropriate error to the client.
 * If there's a transaction is flags it as dirty, and if the command is EXEC,
 * it aborts the transaction.
 * Note: 'reply' is expected to end with \r\n */
// 拒绝执行命令，返回对应而信息。
void rejectCommand(client *c, robj *reply) {
    flagTransaction(c);
    if (c->cmd) c->cmd->rejected_calls++;
    if (c->cmd && c->cmd->proc == execCommand) {
        execCommandAbort(c, reply->ptr);
    } else {
        /* using addReplyError* rather than addReply so that the error can be logged. */
        addReplyErrorObject(c, reply);
    }
}

void rejectCommandFormat(client *c, const char *fmt, ...) {
    if (c->cmd) c->cmd->rejected_calls++;
    flagTransaction(c);
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    /* Make sure there are no newlines in the string, otherwise invalid protocol
     * is emitted (The args come from the user, they may contain any character). */
    sdsmapchars(s, "\r\n", "  ",  2);
    if (c->cmd && c->cmd->proc == execCommand) {
        // 事务执行reject处理
        execCommandAbort(c, s);
        sdsfree(s);
    } else {
        /* The following frees 's'. */
        // 正常命令，直接回复err
        addReplyErrorSds(c, s);
    }
}

/* Returns 1 for commands that may have key names in their arguments, but have
 * no pre-determined key positions. */
// 如果命令可能有key，但key位置没定。这种是通过getkets函数来获取key的。
// 也就是有getkeys函数，则返回1。
static int cmdHasMovableKeys(struct redisCommand *cmd) {
    return (cmd->getkeys_proc && !(cmd->flags & CMD_MODULE)) ||
            cmd->flags & CMD_MODULE_GETKEYS;
}

/* If this function gets called we already read a whole
 * command, arguments are in the client argv/argc fields.
 * processCommand() execute the command or prepare the
 * server for a bulk read from the client.
 *
 * If C_OK is returned the client is still alive and valid and
 * other operations can be performed by the caller. Otherwise
 * if C_ERR is returned the client was destroyed (i.e. after QUIT). */
// 这个函数执行的时候，我们已经获取到了完整的命令及参数，并存储在client的argv/argc中。
// processCommand 执行命令 或 服务端准备好进行下一轮的从client批量读取数据数据。
// 返回ok表示client仍然存活、有效，调用者可以进行其他的操作。返回err，表示client已经被销毁，不可用了。
int processCommand(client *c) {
    if (!server.lua_timedout) {
        /* Both EXEC and EVAL call call() directly so there should be
         * no way in_exec or in_eval or propagate_in_transaction is 1.
         * That is unless lua_timedout, in which case client may run
         * some commands. */
        // EXEC和EVAL都是直接调用call执行。所以这里server的 in_exec、in_eval都不应为1
        // transaction执行也是使用的EXEC命令，事务执行期间向slave传递执行的命令。所以propagate_in_transaction也不应为1？
        // 除非在执行脚本的时候超时，因为每隔一定时间需要处理事件循环，不能让脚本执行太久，所以设置超时检查。
        // 脚本执行时设置这些标识，超时进入事件循环执行命令到这里，所以这里断言合理。
        serverAssert(!server.propagate_in_transaction);
        serverAssert(!server.in_exec);
        serverAssert(!server.in_eval);
    }

    // 使用module中的command filter对执行命令的的参数进行filter处理。
    moduleCallCommandFilters(c);

    /* The QUIT command is handled separately. Normal command procs will
     * go through checking for replication and QUIT will cause trouble
     * when FORCE_REPLICATION is enabled and would be implemented in
     * a regular command proc. */
    // 正常命令执行会经历复制同步check，当FORCE_REPLICATION开启时，都使用正常命令流程，QUIT的处理会有点麻烦。
    // 所以这里QUIT命令单独处理。QUIT表示当所有回复处理完后关闭client。
    if (!strcasecmp(c->argv[0]->ptr,"quit")) {
        // 回复ok，并设置client标识为CLIENT_CLOSE_AFTER_REPLY
        addReply(c,shared.ok);
        c->flags |= CLIENT_CLOSE_AFTER_REPLY;
        return C_ERR;
    }

    /* Now lookup the command and check ASAP about trivial error conditions
     * such as wrong arity, bad command name and so forth. */
    // 检查命令，尽早处理不重要的error，如错的命令name，参数个数等。
    // 从server.commands中找命令。
    c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr);
    if (!c->cmd) {
        // 如果没找到，返回"unknown command"信息给客户端
        sds args = sdsempty();
        int i;
        for (i=1; i < c->argc && sdslen(args) < 128; i++)
            args = sdscatprintf(args, "`%.*s`, ", 128-(int)sdslen(args), (char*)c->argv[i]->ptr);
        // reject命令。回复err信息给客户端。对于事务执行，还需要abort，并通知slave exec。
        rejectCommandFormat(c,"unknown command `%s`, with args beginning with: %s",
            (char*)c->argv[0]->ptr, args);
        sdsfree(args);
        return C_OK;
    } else if ((c->cmd->arity > 0 && c->cmd->arity != c->argc) ||
               (c->argc < -c->cmd->arity)) {
        // 校验参数个数，不对也reject
        rejectCommandFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        return C_OK;
    }

    // 判断命令具有的哪些属性类别。如只读、写、使用内存等。
    int is_read_command = (c->cmd->flags & CMD_READONLY) ||
                           (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_READONLY));
    int is_write_command = (c->cmd->flags & CMD_WRITE) ||
                           (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_WRITE));
    int is_denyoom_command = (c->cmd->flags & CMD_DENYOOM) ||
                             (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_DENYOOM));
    int is_denystale_command = !(c->cmd->flags & CMD_STALE) ||
                               (c->cmd->proc == execCommand && (c->mstate.cmd_inv_flags & CMD_STALE));
    int is_denyloading_command = !(c->cmd->flags & CMD_LOADING) ||
                                 (c->cmd->proc == execCommand && (c->mstate.cmd_inv_flags & CMD_LOADING));
    int is_may_replicate_command = (c->cmd->flags & (CMD_WRITE | CMD_MAY_REPLICATE)) ||
                                   (c->cmd->proc == execCommand && (c->mstate.cmd_flags & (CMD_WRITE | CMD_MAY_REPLICATE)));

    /* Check if the user is authenticated. This check is skipped in case
     * the default user is flagged as "nopass" and is active. */
    // 检测user是否需要认证。
    // 如果默认user不需要password，且没有被禁止，则不需要认证。
    // 默认用户被禁了，显然需要认证；没被禁且需要密码，则所有用户都需要密码，显然就需要认证了。
    // 如果需要认证，进一步要检查当前client是否已认证，从而判断是否该用户应该auth_required。
    int auth_required = (!(DefaultUser->flags & USER_FLAG_NOPASS) ||
                          (DefaultUser->flags & USER_FLAG_DISABLED)) &&
                        !c->authenticated;
    if (auth_required) {
        /* AUTH and HELLO and no auth modules are valid even in
         * non-authenticated state. */
        // 如果该client用户需要认证，并且执行的命令没有"no-auth" flag，则拒绝命令执行。
        if (!(c->cmd->flags & CMD_NO_AUTH)) {
            rejectCommand(c,shared.noautherr);
            return C_OK;
        }
    }

    /* Check if the user can run this command according to the current
     * ACLs. */
    // 通过ACL，检查用户是否能够执行这个命令。
    int acl_errpos;
    int acl_retval = ACLCheckAllPerm(c,&acl_errpos);
    // 如果check后不允许执行，则reject并回复对应的信息。
    if (acl_retval != ACL_OK) {
        // 对于具体那个cmd或key不允许，这里写入ACL日志。
        addACLLogEntry(c,acl_retval,acl_errpos,NULL);
        switch (acl_retval) {
        case ACL_DENIED_CMD:
            rejectCommandFormat(c,
                "-NOPERM this user has no permissions to run "
                "the '%s' command or its subcommand", c->cmd->name);
            break;
        case ACL_DENIED_KEY:
            rejectCommandFormat(c,
                "-NOPERM this user has no permissions to access "
                "one of the keys used as arguments");
            break;
        case ACL_DENIED_CHANNEL:
            rejectCommandFormat(c,
                "-NOPERM this user has no permissions to access "
                "one of the channels used as arguments");
            break;
        default:
            rejectCommandFormat(c, "no permission");
            break;
        }
        return C_OK;
    }

    /* If cluster is enabled perform the cluster redirection here.
     * However we don't perform the redirection if:
     * 1) The sender of this command is our master.
     * 2) The command has no key arguments. */
    // 如果是集群模式，这里集群重定向处理。
    // 有两种情况我们不需要处理：
    // 1、命令发送方是master，或者当前是在执行lua脚本命令，而脚本的执行者是master。
    // 2、命令没有key参数（即没有getkeys函数，同时firstkey==0。），但不是exec命令。
    if (server.cluster_enabled &&
        !(c->flags & CLIENT_MASTER) &&
        !(c->flags & CLIENT_LUA &&
          server.lua_caller->flags & CLIENT_MASTER) &&
        !(!cmdHasMovableKeys(c->cmd) && c->cmd->firstkey == 0 &&
          c->cmd->proc != execCommand))
    {
        int hashslot;
        int error_code;
        // 根据query，获取命令执行的节点
        clusterNode *n = getNodeByQuery(c,c->cmd,c->argv,c->argc,
                                        &hashslot,&error_code);
        // 如果节点为NULL 或者 节点不是本机的话，需要重定向处理
        if (n == NULL || n != server.cluster->myself) {
            if (c->cmd->proc == execCommand) {
                // 如果是exec命令，事务不支持垮节点，这里discard掉
                discardTransaction(c);
            } else {
                // 重定向了，不支持事务，如果是CLIENT_MULTI状态，需要标识事务dirty exec状态。
                // 当后续要执行exec时，发现dirty就执行失败了。
                flagTransaction(c);
            }
            // 返回重定向回复，rejected_calls统计+1
            clusterRedirectClient(c,n,hashslot,error_code);
            c->cmd->rejected_calls++;
            return C_OK;
        }
    }

    /* Handle the maxmemory directive.
     *
     * Note that we do not want to reclaim memory if we are here re-entering
     * the event loop since there is a busy Lua script running in timeout
     * condition, to avoid mixing the propagation of scripts with the
     * propagation of DELs due to eviction. */
    // 处理最大内存限制。
    // 注意假设我们由于忙的Lua脚本执行超时而重新进入event loop，然后处理命令到这里。
    // 如果我们此时由于达到maxmemory而回收内存，那么会在数据传播到slave时，混淆搞不清是脚本执行的命令还是因驱逐而DEL操作删除的命令。
    // 所以这种情况，我们不做内存回收。也就是说lua脚本执行期间，不处理数据驱逐。
    if (server.maxmemory && !server.lua_timedout) {
        // 设置了最大内存限制，且服务没有在执行lua脚本时，检查是否还有内存来处理命令。
        // 如果内存状态正常，或者可淘汰驱逐一部分数据后，内存状态正常，则可以处理。
        int out_of_memory = (performEvictions() == EVICT_FAIL);
        /* performEvictions may flush slave output buffers. This may result
         * in a slave, that may be the active client, to be freed. */
        // 执行数据驱逐时，可能会刷新slave output buffers，即将当前server的slave client的optput buffer数据发送出去。
        // 这可能导致该活跃的的slave client被释放掉，写数据失败而释放slave client，然后slave client刚好时current_client？
        if (server.current_client == NULL) return C_ERR;

        // 默认根据该执行命令的属性看是否在oom时reject
        int reject_cmd_on_oom = is_denyoom_command;
        /* If client is in MULTI/EXEC context, queuing may consume an unlimited
         * amount of memory, so we want to stop that.
         * However, we never want to reject DISCARD, or even EXEC (unless it
         * contains denied commands, in which case is_denyoom_command is already
         * set. */
        // 如果client正在处理一个事务，且当前命令不是exec、discard和reset，也就是在命令入队阶段。
        // 此时即使当前入队命令是读命令或允许oom的命令，因为命令入队数量不可控，需要的内存也可能无限制，所以我们这里统一OOM拒绝处理。
        // 但是我们对于DISCARD和EXEC命令不拒绝，除非EXEC的命令中包含OOM拒绝执行的命令，这里我们通过is_denyoom_command判断处理了。
        if (c->flags & CLIENT_MULTI &&
            c->cmd->proc != execCommand &&
            c->cmd->proc != discardCommand &&
            c->cmd->proc != resetCommand) {
            reject_cmd_on_oom = 1;
        }

        // OOM发送，且命令在OOM时拒绝执行。这里直接reject。
        if (out_of_memory && reject_cmd_on_oom) {
            rejectCommand(c, shared.oomerr);
            return C_OK;
        }

        /* Save out_of_memory result at script start, otherwise if we check OOM
         * until first write within script, memory used by lua stack and
         * arguments might interfere. */
        // 如果当前命令是执行脚本eval相关命令。这里在执行前默认设置out_of_memory作为lua_oom结果。
        // 否则当我们在执行脚本首次写数据时，可能lua执行栈和参数会影响当前内存的使用。
        if (c->cmd->proc == evalCommand || c->cmd->proc == evalShaCommand) {
            server.lua_oom = out_of_memory;
        }
    }

    /* Make sure to use a reasonable amount of memory for client side
     * caching metadata. */
    // 这里确保使用合理的内存来作为client侧的metadata缓存。
    if (server.tracking_clients) trackingLimitUsedSlots();

    /* Don't accept write commands if there are problems persisting on disk
     * and if this is a master instance. */
    // 如果当前是master节点，且在持久化数据到磁盘除了err，则不允许写命令执行。
    // 这里server.masterhost == NULL 标识当前是master节点，因为只有slave节点才有这信息。
    // 另外写命令和ping命令都会拒绝执行，返回RDB或AOF错误信息。
    int deny_write_type = writeCommandsDeniedByDiskError();
    if (deny_write_type != DISK_ERROR_TYPE_NONE &&
        server.masterhost == NULL &&
        (is_write_command ||c->cmd->proc == pingCommand))
    {
        if (deny_write_type == DISK_ERROR_TYPE_RDB)
            rejectCommand(c, shared.bgsaveerr);
        else
            rejectCommandFormat(c,
                "-MISCONF Errors writing to the AOF file: %s",
                strerror(server.aof_last_write_errno));
        return C_OK;
    }

    /* Don't accept write commands if there are not enough good slaves and
     * user configured the min-slaves-to-write option. */
    // 如果设置最小slaves写同步配置，而并没有足够多的可用的slaves，这里我们也禁止写入。
    // good slaves判定：lag<=max lag
    if (server.masterhost == NULL &&
        server.repl_min_slaves_to_write &&
        server.repl_min_slaves_max_lag &&
        is_write_command &&
        server.repl_good_slaves_count < server.repl_min_slaves_to_write)
    {
        rejectCommand(c, shared.noreplicaserr);
        return C_OK;
    }

    /* Don't accept write commands if this is a read only slave. But
     * accept write commands if this is our master. */
    // 如果当前是slave，且slave是只读的，并且执行命令的client不是master，那么拒绝写命令执行。
    if (server.masterhost && server.repl_slave_ro &&
        !(c->flags & CLIENT_MASTER) &&
        is_write_command)
    {
        rejectCommand(c, shared.roslaveerr);
        return C_OK;
    }

    /* Only allow a subset of commands in the context of Pub/Sub if the
     * connection is in RESP2 mode. With RESP3 there are no limits. */
    // 如果conn是RESP2协议模式，在Pub/Sub中只支持部分的命令。不是这些命令的拒绝执行。
    // RESP3协议模式没有这些限制。
    if ((c->flags & CLIENT_PUBSUB && c->resp == 2) &&
        c->cmd->proc != pingCommand &&
        c->cmd->proc != subscribeCommand &&
        c->cmd->proc != unsubscribeCommand &&
        c->cmd->proc != psubscribeCommand &&
        c->cmd->proc != punsubscribeCommand &&
        c->cmd->proc != resetCommand) {
        rejectCommandFormat(c,
            "Can't execute '%s': only (P)SUBSCRIBE / "
            "(P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
            c->cmd->name);
        return C_OK;
    }

    /* Only allow commands with flag "t", such as INFO, SLAVEOF and so on,
     * when slave-serve-stale-data is no and we are a slave with a broken
     * link with master. */
    // 当slave-serve-stale-data是no，即在当前节点是slave，但与master连接断开，slave是旧数据不能接受服务时。
    // 我们只允许flag中有ok-stale标识的命令执行。没有该标识就是is_denystale_command，需要拒绝。
    if (server.masterhost && server.repl_state != REPL_STATE_CONNECTED &&
        server.repl_serve_stale_data == 0 &&
        is_denystale_command)
    {
        rejectCommand(c, shared.masterdownerr);
        return C_OK;
    }

    /* Loading DB? Return an error if the command has not the
     * CMD_LOADING flag. */
    // 如果服务正在loading数据，并且命令不允许loading时执行（没有ok-loading标识），那么就拒绝执行返回"-LOADING" err
    if (server.loading && is_denyloading_command) {
        rejectCommand(c, shared.loadingerr);
        return C_OK;
    }

    /* Lua script too slow? Only allow a limited number of commands.
     * Note that we need to allow the transactions commands, otherwise clients
     * sending a transaction with pipelining without error checking, may have
     * the MULTI plus a few initial commands refused, then the timeout
     * condition resolves, and the bottom-half of the transaction gets
     * executed, see Github PR #7022. */
    // 如果有一个执行时间很长的lua脚本，每次执行超时期间进入event loop执行命令到这里时，我们仅允许有限的命令执行。
    // 注意这里我们需要允许事务命令执行，因为client可能通过pipeline形式发送事务命令而不进行错误检查，
    // 实际上事务的启动以及初始化的一些命令是可以处理的，命令都是存起来并不执行，这里只是不允许EXEC命令操作就好了。
    // 这样事务开启、初始化以及命令入队不影响，后续等lua脚本执行完了，没有超时了，再通过EXEC执行事务就可以了。
    // 所以这里是允许auth、hello、replconf以及部分事务命令可执行，另外可以执行的是"shotdown nosave"和"script kill"执行。
    if (server.lua_timedout &&
          c->cmd->proc != authCommand &&
          c->cmd->proc != helloCommand &&
          c->cmd->proc != replconfCommand &&
          c->cmd->proc != multiCommand &&
          c->cmd->proc != discardCommand &&
          c->cmd->proc != watchCommand &&
          c->cmd->proc != unwatchCommand &&
          c->cmd->proc != resetCommand &&
        !(c->cmd->proc == shutdownCommand &&
          c->argc == 2 &&
          tolower(((char*)c->argv[1]->ptr)[0]) == 'n') &&
        !(c->cmd->proc == scriptCommand &&
          c->argc == 2 &&
          tolower(((char*)c->argv[1]->ptr)[0]) == 'k'))
    {
        rejectCommand(c, shared.slowscripterr);
        return C_OK;
    }

    /* Prevent a replica from sending commands that access the keyspace.
     * The main objective here is to prevent abuse of client pause check
     * from which replicas are exempt. */
    // 阻止slave发送命令来访问键空间，即不允许slave client执行读、写或replicate相关命令
    // 主要目的是防止滥用client暂停检查，从而slave client不被阻止而执行命令。
    // 因为后面处理可以看到，暂停时除slave client外都会进入阻塞，所以这里拒绝slave client操作数据。
    if ((c->flags & CLIENT_SLAVE) && (is_may_replicate_command || is_write_command || is_read_command)) {
        rejectCommandFormat(c, "Replica can't interract with the keyspace");
        return C_OK;
    }

    /* If the server is paused, block the client until
     * the pause has ended. Replicas are never paused. */
    // 如果服务处于暂停状态，阻止client执行命令直到暂停解除。
    // slave client不会被暂停阻塞。
    if (!(c->flags & CLIENT_SLAVE) && 
        ((server.client_pause_type == CLIENT_PAUSE_ALL) ||
        (server.client_pause_type == CLIENT_PAUSE_WRITE && is_may_replicate_command)))
    {
        c->bpop.timeout = 0;
        blockClient(c,BLOCKED_PAUSE);
        return C_OK;       
    }

    /* Exec the command */
    // 命令检查操作完成，执行命令
    if (c->flags & CLIENT_MULTI &&
        c->cmd->proc != execCommand && c->cmd->proc != discardCommand &&
        c->cmd->proc != multiCommand && c->cmd->proc != watchCommand &&
        c->cmd->proc != resetCommand)
    {
        // MULTI状态，不是上面几个事务命令，则加入mstate命令队列，返回入队成功消息。
        // 事务期间，命令并不真正去执行，所以这里不调用call()函数执行，而是将命令入队。
        // 真正执行是在处理EXEC命令时，从队列中挨个取出命令来执行并传播。
        queueMultiCommand(c);
        addReply(c,shared.queued);
    } else {
        // 不是事务状态，正常命令执行，调用call函数处理。
        call(c,CMD_CALL_FULL);
        // 处理写操作需要同步给指定数量的slave，这里同步完后更新最新的全局写同步offset。
        c->woff = server.master_repl_offset;
        // 执行完命令后，如果当前有ready keys，需要处理在这个key上block的client
        if (listLength(server.ready_keys))
            handleClientsBlockedOnKeys();
    }

    return C_OK;
}

/* ====================== Error lookup and execution ===================== */

void incrementErrorCount(const char *fullerr, size_t namelen) {
    struct redisError *error = raxFind(server.errors,(unsigned char*)fullerr,namelen);
    if (error == raxNotFound) {
        error = zmalloc(sizeof(*error));
        error->count = 0;
        raxInsert(server.errors,(unsigned char*)fullerr,namelen,error,NULL);
    }
    error->count++;
}

/*================================== Shutdown =============================== */

/* Close listening sockets. Also unlink the unix domain socket if
 * unlink_unix_socket is non-zero. */
void closeListeningSockets(int unlink_unix_socket) {
    int j;

    for (j = 0; j < server.ipfd.count; j++) close(server.ipfd.fd[j]);
    for (j = 0; j < server.tlsfd.count; j++) close(server.tlsfd.fd[j]);
    if (server.sofd != -1) close(server.sofd);
    if (server.cluster_enabled)
        for (j = 0; j < server.cfd.count; j++) close(server.cfd.fd[j]);
    if (unlink_unix_socket && server.unixsocket) {
        serverLog(LL_NOTICE,"Removing the unix socket file.");
        unlink(server.unixsocket); /* don't care if this fails */
    }
}

int prepareForShutdown(int flags) {
    /* When SHUTDOWN is called while the server is loading a dataset in
     * memory we need to make sure no attempt is performed to save
     * the dataset on shutdown (otherwise it could overwrite the current DB
     * with half-read data).
     *
     * Also when in Sentinel mode clear the SAVE flag and force NOSAVE. */
    if (server.loading || server.sentinel_mode)
        flags = (flags & ~SHUTDOWN_SAVE) | SHUTDOWN_NOSAVE;

    int save = flags & SHUTDOWN_SAVE;
    int nosave = flags & SHUTDOWN_NOSAVE;

    serverLog(LL_WARNING,"User requested shutdown...");
    if (server.supervised_mode == SUPERVISED_SYSTEMD)
        redisCommunicateSystemd("STOPPING=1\n");

    /* Kill all the Lua debugger forked sessions. */
    ldbKillForkedSessions();

    /* Kill the saving child if there is a background saving in progress.
       We want to avoid race conditions, for instance our saving child may
       overwrite the synchronous saving did by SHUTDOWN. */
    if (server.child_type == CHILD_TYPE_RDB) {
        serverLog(LL_WARNING,"There is a child saving an .rdb. Killing it!");
        killRDBChild();
        /* Note that, in killRDBChild normally has backgroundSaveDoneHandler
         * doing it's cleanup, but in this case this code will not be reached,
         * so we need to call rdbRemoveTempFile which will close fd(in order
         * to unlink file actully) in background thread.
         * The temp rdb file fd may won't be closed when redis exits quickly,
         * but OS will close this fd when process exits. */
        rdbRemoveTempFile(server.child_pid, 0);
    }

    /* Kill module child if there is one. */
    if (server.child_type == CHILD_TYPE_MODULE) {
        serverLog(LL_WARNING,"There is a module fork child. Killing it!");
        TerminateModuleForkChild(server.child_pid,0);
    }

    if (server.aof_state != AOF_OFF) {
        /* Kill the AOF saving child as the AOF we already have may be longer
         * but contains the full dataset anyway. */
        if (server.child_type == CHILD_TYPE_AOF) {
            /* If we have AOF enabled but haven't written the AOF yet, don't
             * shutdown or else the dataset will be lost. */
            if (server.aof_state == AOF_WAIT_REWRITE) {
                serverLog(LL_WARNING, "Writing initial AOF, can't exit.");
                return C_ERR;
            }
            serverLog(LL_WARNING,
                "There is a child rewriting the AOF. Killing it!");
            killAppendOnlyChild();
        }
        /* Append only file: flush buffers and fsync() the AOF at exit */
        serverLog(LL_NOTICE,"Calling fsync() on the AOF file.");
        flushAppendOnlyFile(1);
        if (redis_fsync(server.aof_fd) == -1) {
            serverLog(LL_WARNING,"Fail to fsync the AOF file: %s.",
                                 strerror(errno));
        }
    }

    /* Create a new RDB file before exiting. */
    if ((server.saveparamslen > 0 && !nosave) || save) {
        serverLog(LL_NOTICE,"Saving the final RDB snapshot before exiting.");
        if (server.supervised_mode == SUPERVISED_SYSTEMD)
            redisCommunicateSystemd("STATUS=Saving the final RDB snapshot\n");
        /* Snapshotting. Perform a SYNC SAVE and exit */
        rdbSaveInfo rsi, *rsiptr;
        rsiptr = rdbPopulateSaveInfo(&rsi);
        if (rdbSave(server.rdb_filename,rsiptr) != C_OK) {
            /* Ooops.. error saving! The best we can do is to continue
             * operating. Note that if there was a background saving process,
             * in the next cron() Redis will be notified that the background
             * saving aborted, handling special stuff like slaves pending for
             * synchronization... */
            serverLog(LL_WARNING,"Error trying to save the DB, can't exit.");
            if (server.supervised_mode == SUPERVISED_SYSTEMD)
                redisCommunicateSystemd("STATUS=Error trying to save the DB, can't exit.\n");
            return C_ERR;
        }
    }

    /* Fire the shutdown modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_SHUTDOWN,0,NULL);

    /* Remove the pid file if possible and needed. */
    if (server.daemonize || server.pidfile) {
        serverLog(LL_NOTICE,"Removing the pid file.");
        unlink(server.pidfile);
    }

    /* Best effort flush of slave output buffers, so that we hopefully
     * send them pending writes. */
    flushSlavesOutputBuffers();

    /* Close the listening sockets. Apparently this allows faster restarts. */
    closeListeningSockets(1);
    serverLog(LL_WARNING,"%s is now ready to exit, bye bye...",
        server.sentinel_mode ? "Sentinel" : "Redis");
    return C_OK;
}

/*================================== Commands =============================== */

/* Sometimes Redis cannot accept write commands because there is a persistence
 * error with the RDB or AOF file, and Redis is configured in order to stop
 * accepting writes in such situation. This function returns if such a
 * condition is active, and the type of the condition.
 *
 * Function return values:
 *
 * DISK_ERROR_TYPE_NONE:    No problems, we can accept writes.
 * DISK_ERROR_TYPE_AOF:     Don't accept writes: AOF errors.
 * DISK_ERROR_TYPE_RDB:     Don't accept writes: RDB errors.
 */
// 如果RDB或AOF持久化时出了err，而配置中设置在这种情况不接受写入操作，这种情况下redis不能处理写命令。
// 这个函数就是做这个判断，返回如下结果：
//   DISK_ERROR_TYPE_NONE，服务没问题，可以进行写。
//   DISK_ERROR_TYPE_AOF，AOF err，不接受写。
//   DISK_ERROR_TYPE_RDB，RDB err，不接受写。
int writeCommandsDeniedByDiskError(void) {
    if (server.stop_writes_on_bgsave_err &&
        server.saveparamslen > 0 &&
        server.lastbgsave_status == C_ERR)
    {
        // 配置了bgsave err时禁止写入 && 有save点，说明需要做save操作 && 上一次save操作err了
        // RDB save有失败，返回RDB err，禁止写操作
        return DISK_ERROR_TYPE_RDB;
    } else if (server.aof_state != AOF_OFF) {
        // 开启了AOF，如果AOF上一次写失败，返回AOF err，禁止写
        if (server.aof_last_write_status == C_ERR) {
            return DISK_ERROR_TYPE_AOF;
        }
        /* AOF fsync error. */
        int aof_bio_fsync_status;
        atomicGet(server.aof_bio_fsync_status,aof_bio_fsync_status);
        // 判断AOF异步fsync是否有失败，有则返回AOF err，禁止写
        if (aof_bio_fsync_status == C_ERR) {
            atomicGet(server.aof_bio_fsync_errno,server.aof_last_write_errno);
            return DISK_ERROR_TYPE_AOF;
        }
    }

    // 最后都没有失败，正常，可以处理写
    return DISK_ERROR_TYPE_NONE;
}

/* The PING command. It works in a different way if the client is in
 * in Pub/Sub mode. */
void pingCommand(client *c) {
    /* The command takes zero or one arguments. */
    if (c->argc > 2) {
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        return;
    }

    if (c->flags & CLIENT_PUBSUB && c->resp == 2) {
        addReply(c,shared.mbulkhdr[2]);
        addReplyBulkCBuffer(c,"pong",4);
        if (c->argc == 1)
            addReplyBulkCBuffer(c,"",0);
        else
            addReplyBulk(c,c->argv[1]);
    } else {
        if (c->argc == 1)
            addReply(c,shared.pong);
        else
            addReplyBulk(c,c->argv[1]);
    }
}

void echoCommand(client *c) {
    addReplyBulk(c,c->argv[1]);
}

void timeCommand(client *c) {
    struct timeval tv;

    /* gettimeofday() can only fail if &tv is a bad address so we
     * don't check for errors. */
    gettimeofday(&tv,NULL);
    addReplyArrayLen(c,2);
    addReplyBulkLongLong(c,tv.tv_sec);
    addReplyBulkLongLong(c,tv.tv_usec);
}

/* Helper function for addReplyCommand() to output flags. */
int addReplyCommandFlag(client *c, struct redisCommand *cmd, int f, char *reply) {
    if (cmd->flags & f) {
        addReplyStatus(c, reply);
        return 1;
    }
    return 0;
}

/* Output the representation of a Redis command. Used by the COMMAND command. */
void addReplyCommand(client *c, struct redisCommand *cmd) {
    if (!cmd) {
        addReplyNull(c);
    } else {
        /* We are adding: command name, arg count, flags, first, last, offset, categories */
        addReplyArrayLen(c, 7);
        addReplyBulkCString(c, cmd->name);
        addReplyLongLong(c, cmd->arity);

        int flagcount = 0;
        void *flaglen = addReplyDeferredLen(c);
        flagcount += addReplyCommandFlag(c,cmd,CMD_WRITE, "write");
        flagcount += addReplyCommandFlag(c,cmd,CMD_READONLY, "readonly");
        flagcount += addReplyCommandFlag(c,cmd,CMD_DENYOOM, "denyoom");
        flagcount += addReplyCommandFlag(c,cmd,CMD_ADMIN, "admin");
        flagcount += addReplyCommandFlag(c,cmd,CMD_PUBSUB, "pubsub");
        flagcount += addReplyCommandFlag(c,cmd,CMD_NOSCRIPT, "noscript");
        flagcount += addReplyCommandFlag(c,cmd,CMD_RANDOM, "random");
        flagcount += addReplyCommandFlag(c,cmd,CMD_SORT_FOR_SCRIPT,"sort_for_script");
        flagcount += addReplyCommandFlag(c,cmd,CMD_LOADING, "loading");
        flagcount += addReplyCommandFlag(c,cmd,CMD_STALE, "stale");
        flagcount += addReplyCommandFlag(c,cmd,CMD_SKIP_MONITOR, "skip_monitor");
        flagcount += addReplyCommandFlag(c,cmd,CMD_SKIP_SLOWLOG, "skip_slowlog");
        flagcount += addReplyCommandFlag(c,cmd,CMD_ASKING, "asking");
        flagcount += addReplyCommandFlag(c,cmd,CMD_FAST, "fast");
        flagcount += addReplyCommandFlag(c,cmd,CMD_NO_AUTH, "no_auth");
        flagcount += addReplyCommandFlag(c,cmd,CMD_MAY_REPLICATE, "may_replicate");
        if (cmdHasMovableKeys(cmd)) {
            addReplyStatus(c, "movablekeys");
            flagcount += 1;
        }
        setDeferredSetLen(c, flaglen, flagcount);

        addReplyLongLong(c, cmd->firstkey);
        addReplyLongLong(c, cmd->lastkey);
        addReplyLongLong(c, cmd->keystep);

        addReplyCommandCategories(c,cmd);
    }
}

/* COMMAND <subcommand> <args> */
void commandCommand(client *c) {
    dictIterator *di;
    dictEntry *de;

    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"(no subcommand)",
"    Return details about all Redis commands.",
"COUNT",
"    Return the total number of commands in this Redis server.",
"GETKEYS <full-command>",
"    Return the keys from a full Redis command.",
"INFO [<command-name> ...]",
"    Return details about multiple Redis commands.",
NULL
        };
        addReplyHelp(c, help);
    } else if (c->argc == 1) {
        addReplyArrayLen(c, dictSize(server.commands));
        di = dictGetIterator(server.commands);
        while ((de = dictNext(di)) != NULL) {
            addReplyCommand(c, dictGetVal(de));
        }
        dictReleaseIterator(di);
    } else if (!strcasecmp(c->argv[1]->ptr, "info")) {
        int i;
        addReplyArrayLen(c, c->argc-2);
        for (i = 2; i < c->argc; i++) {
            addReplyCommand(c, dictFetchValue(server.commands, c->argv[i]->ptr));
        }
    } else if (!strcasecmp(c->argv[1]->ptr, "count") && c->argc == 2) {
        addReplyLongLong(c, dictSize(server.commands));
    } else if (!strcasecmp(c->argv[1]->ptr,"getkeys") && c->argc >= 3) {
        struct redisCommand *cmd = lookupCommand(c->argv[2]->ptr);
        getKeysResult result = GETKEYS_RESULT_INIT;
        int j;

        if (!cmd) {
            addReplyError(c,"Invalid command specified");
            return;
        } else if (cmd->getkeys_proc == NULL && cmd->firstkey == 0) {
            addReplyError(c,"The command has no key arguments");
            return;
        } else if ((cmd->arity > 0 && cmd->arity != c->argc-2) ||
                   ((c->argc-2) < -cmd->arity))
        {
            addReplyError(c,"Invalid number of arguments specified for command");
            return;
        }

        if (!getKeysFromCommand(cmd,c->argv+2,c->argc-2,&result)) {
            addReplyError(c,"Invalid arguments specified for command");
        } else {
            addReplyArrayLen(c,result.numkeys);
            for (j = 0; j < result.numkeys; j++) addReplyBulk(c,c->argv[result.keys[j]+2]);
        }
        getKeysFreeResult(&result);
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* Convert an amount of bytes into a human readable string in the form
 * of 100B, 2G, 100M, 4K, and so forth. */
void bytesToHuman(char *s, unsigned long long n) {
    double d;

    if (n < 1024) {
        /* Bytes */
        sprintf(s,"%lluB",n);
    } else if (n < (1024*1024)) {
        d = (double)n/(1024);
        sprintf(s,"%.2fK",d);
    } else if (n < (1024LL*1024*1024)) {
        d = (double)n/(1024*1024);
        sprintf(s,"%.2fM",d);
    } else if (n < (1024LL*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024);
        sprintf(s,"%.2fG",d);
    } else if (n < (1024LL*1024*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024*1024);
        sprintf(s,"%.2fT",d);
    } else if (n < (1024LL*1024*1024*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024*1024*1024);
        sprintf(s,"%.2fP",d);
    } else {
        /* Let's hope we never need this */
        sprintf(s,"%lluB",n);
    }
}

/* Characters we sanitize on INFO output to maintain expected format. */
static char unsafe_info_chars[] = "#:\n\r";
static char unsafe_info_chars_substs[] = "____";   /* Must be same length as above */

/* Returns a sanitized version of s that contains no unsafe info string chars.
 * If no unsafe characters are found, simply returns s. Caller needs to
 * free tmp if it is non-null on return.
 */
const char *getSafeInfoString(const char *s, size_t len, char **tmp) {
    *tmp = NULL;
    if (mempbrk(s, len, unsafe_info_chars,sizeof(unsafe_info_chars)-1)
        == NULL) return s;
    char *new = *tmp = zmalloc(len + 1);
    memcpy(new, s, len);
    new[len] = '\0';
    return memmapchars(new, len, unsafe_info_chars, unsafe_info_chars_substs,
                       sizeof(unsafe_info_chars)-1);
}

/* Create the string returned by the INFO command. This is decoupled
 * by the INFO command itself as we need to report the same information
 * on memory corruption problems. */
sds genRedisInfoString(const char *section) {
    sds info = sdsempty();
    time_t uptime = server.unixtime-server.stat_starttime;
    int j;
    int allsections = 0, defsections = 0, everything = 0, modules = 0;
    int sections = 0;

    if (section == NULL) section = "default";
    allsections = strcasecmp(section,"all") == 0;
    defsections = strcasecmp(section,"default") == 0;
    everything = strcasecmp(section,"everything") == 0;
    modules = strcasecmp(section,"modules") == 0;
    if (everything) allsections = 1;

    /* Server */
    if (allsections || defsections || !strcasecmp(section,"server")) {
        static int call_uname = 1;
        static struct utsname name;
        char *mode;
        char *supervised;

        if (server.cluster_enabled) mode = "cluster";
        else if (server.sentinel_mode) mode = "sentinel";
        else mode = "standalone";

        if (server.supervised) {
            if (server.supervised_mode == SUPERVISED_UPSTART) supervised = "upstart";
            else if (server.supervised_mode == SUPERVISED_SYSTEMD) supervised = "systemd";
            else supervised = "unknown";
        } else {
            supervised = "no";
        }

        if (sections++) info = sdscat(info,"\r\n");

        if (call_uname) {
            /* Uname can be slow and is always the same output. Cache it. */
            uname(&name);
            call_uname = 0;
        }

        unsigned int lruclock;
        atomicGet(server.lruclock,lruclock);
        info = sdscatfmt(info,
            "# Server\r\n"
            "redis_version:%s\r\n"
            "redis_git_sha1:%s\r\n"
            "redis_git_dirty:%i\r\n"
            "redis_build_id:%s\r\n"
            "redis_mode:%s\r\n"
            "os:%s %s %s\r\n"
            "arch_bits:%i\r\n"
            "multiplexing_api:%s\r\n"
            "atomicvar_api:%s\r\n"
            "gcc_version:%i.%i.%i\r\n"
            "process_id:%I\r\n"
            "process_supervised:%s\r\n"
            "run_id:%s\r\n"
            "tcp_port:%i\r\n"
            "server_time_usec:%I\r\n"
            "uptime_in_seconds:%I\r\n"
            "uptime_in_days:%I\r\n"
            "hz:%i\r\n"
            "configured_hz:%i\r\n"
            "lru_clock:%u\r\n"
            "executable:%s\r\n"
            "config_file:%s\r\n"
            "io_threads_active:%i\r\n",
            REDIS_VERSION,
            redisGitSHA1(),
            strtol(redisGitDirty(),NULL,10) > 0,
            redisBuildIdString(),
            mode,
            name.sysname, name.release, name.machine,
            server.arch_bits,
            aeGetApiName(),
            REDIS_ATOMIC_API,
#ifdef __GNUC__
            __GNUC__,__GNUC_MINOR__,__GNUC_PATCHLEVEL__,
#else
            0,0,0,
#endif
            (int64_t) getpid(),
            supervised,
            server.runid,
            server.port ? server.port : server.tls_port,
            (int64_t)server.ustime,
            (int64_t)uptime,
            (int64_t)(uptime/(3600*24)),
            server.hz,
            server.config_hz,
            lruclock,
            server.executable ? server.executable : "",
            server.configfile ? server.configfile : "",
            server.io_threads_active);
    }

    /* Clients */
    if (allsections || defsections || !strcasecmp(section,"clients")) {
        size_t maxin, maxout;
        getExpansiveClientsInfo(&maxin,&maxout);
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Clients\r\n"
            "connected_clients:%lu\r\n"
            "cluster_connections:%lu\r\n"
            "maxclients:%u\r\n"
            "client_recent_max_input_buffer:%zu\r\n"
            "client_recent_max_output_buffer:%zu\r\n"
            "blocked_clients:%d\r\n"
            "tracking_clients:%d\r\n"
            "clients_in_timeout_table:%llu\r\n",
            listLength(server.clients)-listLength(server.slaves),
            getClusterConnectionsCount(),
            server.maxclients,
            maxin, maxout,
            server.blocked_clients,
            server.tracking_clients,
            (unsigned long long) raxSize(server.clients_timeout_table));
    }

    /* Memory */
    if (allsections || defsections || !strcasecmp(section,"memory")) {
        char hmem[64];
        char peak_hmem[64];
        char total_system_hmem[64];
        char used_memory_lua_hmem[64];
        char used_memory_scripts_hmem[64];
        char used_memory_rss_hmem[64];
        char maxmemory_hmem[64];
        size_t zmalloc_used = zmalloc_used_memory();
        size_t total_system_mem = server.system_memory_size;
        const char *evict_policy = evictPolicyToString();
        long long memory_lua = server.lua ? (long long)lua_gc(server.lua,LUA_GCCOUNT,0)*1024 : 0;
        struct redisMemOverhead *mh = getMemoryOverheadData();

        /* Peak memory is updated from time to time by serverCron() so it
         * may happen that the instantaneous value is slightly bigger than
         * the peak value. This may confuse users, so we update the peak
         * if found smaller than the current memory usage. */
        if (zmalloc_used > server.stat_peak_memory)
            server.stat_peak_memory = zmalloc_used;

        bytesToHuman(hmem,zmalloc_used);
        bytesToHuman(peak_hmem,server.stat_peak_memory);
        bytesToHuman(total_system_hmem,total_system_mem);
        bytesToHuman(used_memory_lua_hmem,memory_lua);
        bytesToHuman(used_memory_scripts_hmem,mh->lua_caches);
        bytesToHuman(used_memory_rss_hmem,server.cron_malloc_stats.process_rss);
        bytesToHuman(maxmemory_hmem,server.maxmemory);

        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Memory\r\n"
            "used_memory:%zu\r\n"
            "used_memory_human:%s\r\n"
            "used_memory_rss:%zu\r\n"
            "used_memory_rss_human:%s\r\n"
            "used_memory_peak:%zu\r\n"
            "used_memory_peak_human:%s\r\n"
            "used_memory_peak_perc:%.2f%%\r\n"
            "used_memory_overhead:%zu\r\n"
            "used_memory_startup:%zu\r\n"
            "used_memory_dataset:%zu\r\n"
            "used_memory_dataset_perc:%.2f%%\r\n"
            "allocator_allocated:%zu\r\n"
            "allocator_active:%zu\r\n"
            "allocator_resident:%zu\r\n"
            "total_system_memory:%lu\r\n"
            "total_system_memory_human:%s\r\n"
            "used_memory_lua:%lld\r\n"
            "used_memory_lua_human:%s\r\n"
            "used_memory_scripts:%lld\r\n"
            "used_memory_scripts_human:%s\r\n"
            "number_of_cached_scripts:%lu\r\n"
            "maxmemory:%lld\r\n"
            "maxmemory_human:%s\r\n"
            "maxmemory_policy:%s\r\n"
            "allocator_frag_ratio:%.2f\r\n"
            "allocator_frag_bytes:%zu\r\n"
            "allocator_rss_ratio:%.2f\r\n"
            "allocator_rss_bytes:%zd\r\n"
            "rss_overhead_ratio:%.2f\r\n"
            "rss_overhead_bytes:%zd\r\n"
            "mem_fragmentation_ratio:%.2f\r\n"
            "mem_fragmentation_bytes:%zd\r\n"
            "mem_not_counted_for_evict:%zu\r\n"
            "mem_replication_backlog:%zu\r\n"
            "mem_clients_slaves:%zu\r\n"
            "mem_clients_normal:%zu\r\n"
            "mem_aof_buffer:%zu\r\n"
            "mem_allocator:%s\r\n"
            "active_defrag_running:%d\r\n"
            "lazyfree_pending_objects:%zu\r\n"
            "lazyfreed_objects:%zu\r\n",
            zmalloc_used,
            hmem,
            server.cron_malloc_stats.process_rss,
            used_memory_rss_hmem,
            server.stat_peak_memory,
            peak_hmem,
            mh->peak_perc,
            mh->overhead_total,
            mh->startup_allocated,
            mh->dataset,
            mh->dataset_perc,
            server.cron_malloc_stats.allocator_allocated,
            server.cron_malloc_stats.allocator_active,
            server.cron_malloc_stats.allocator_resident,
            (unsigned long)total_system_mem,
            total_system_hmem,
            memory_lua,
            used_memory_lua_hmem,
            (long long) mh->lua_caches,
            used_memory_scripts_hmem,
            dictSize(server.lua_scripts),
            server.maxmemory,
            maxmemory_hmem,
            evict_policy,
            mh->allocator_frag,
            mh->allocator_frag_bytes,
            mh->allocator_rss,
            mh->allocator_rss_bytes,
            mh->rss_extra,
            mh->rss_extra_bytes,
            mh->total_frag,       /* This is the total RSS overhead, including
                                     fragmentation, but not just it. This field
                                     (and the next one) is named like that just
                                     for backward compatibility. */
            mh->total_frag_bytes,
            freeMemoryGetNotCountedMemory(),
            mh->repl_backlog,
            mh->clients_slaves,
            mh->clients_normal,
            mh->aof_buffer,
            ZMALLOC_LIB,
            server.active_defrag_running,
            lazyfreeGetPendingObjectsCount(),
            lazyfreeGetFreedObjectsCount()
        );
        freeMemoryOverheadData(mh);
    }

    /* Persistence */
    if (allsections || defsections || !strcasecmp(section,"persistence")) {
        if (sections++) info = sdscat(info,"\r\n");
        double fork_perc = 0;
        if (server.stat_module_progress) {
            fork_perc = server.stat_module_progress * 100;
        } else if (server.stat_current_save_keys_total) {
            fork_perc = ((double)server.stat_current_save_keys_processed / server.stat_current_save_keys_total) * 100;
        }
        int aof_bio_fsync_status;
        atomicGet(server.aof_bio_fsync_status,aof_bio_fsync_status);

        info = sdscatprintf(info,
            "# Persistence\r\n"
            "loading:%d\r\n"
            "current_cow_size:%zu\r\n"
            "current_cow_size_age:%lu\r\n"
            "current_fork_perc:%.2f\r\n"
            "current_save_keys_processed:%zu\r\n"
            "current_save_keys_total:%zu\r\n"
            "rdb_changes_since_last_save:%lld\r\n"
            "rdb_bgsave_in_progress:%d\r\n"
            "rdb_last_save_time:%jd\r\n"
            "rdb_last_bgsave_status:%s\r\n"
            "rdb_last_bgsave_time_sec:%jd\r\n"
            "rdb_current_bgsave_time_sec:%jd\r\n"
            "rdb_last_cow_size:%zu\r\n"
            "aof_enabled:%d\r\n"
            "aof_rewrite_in_progress:%d\r\n"
            "aof_rewrite_scheduled:%d\r\n"
            "aof_last_rewrite_time_sec:%jd\r\n"
            "aof_current_rewrite_time_sec:%jd\r\n"
            "aof_last_bgrewrite_status:%s\r\n"
            "aof_last_write_status:%s\r\n"
            "aof_last_cow_size:%zu\r\n"
            "module_fork_in_progress:%d\r\n"
            "module_fork_last_cow_size:%zu\r\n",
            (int)server.loading,
            server.stat_current_cow_bytes,
            server.stat_current_cow_updated ? (unsigned long) elapsedMs(server.stat_current_cow_updated) / 1000 : 0,
            fork_perc,
            server.stat_current_save_keys_processed,
            server.stat_current_save_keys_total,
            server.dirty,
            server.child_type == CHILD_TYPE_RDB,
            (intmax_t)server.lastsave,
            (server.lastbgsave_status == C_OK) ? "ok" : "err",
            (intmax_t)server.rdb_save_time_last,
            (intmax_t)((server.child_type != CHILD_TYPE_RDB) ?
                -1 : time(NULL)-server.rdb_save_time_start),
            server.stat_rdb_cow_bytes,
            server.aof_state != AOF_OFF,
            server.child_type == CHILD_TYPE_AOF,
            server.aof_rewrite_scheduled,
            (intmax_t)server.aof_rewrite_time_last,
            (intmax_t)((server.child_type != CHILD_TYPE_AOF) ?
                -1 : time(NULL)-server.aof_rewrite_time_start),
            (server.aof_lastbgrewrite_status == C_OK) ? "ok" : "err",
            (server.aof_last_write_status == C_OK &&
                aof_bio_fsync_status == C_OK) ? "ok" : "err",
            server.stat_aof_cow_bytes,
            server.child_type == CHILD_TYPE_MODULE,
            server.stat_module_cow_bytes);

        if (server.aof_enabled) {
            info = sdscatprintf(info,
                "aof_current_size:%lld\r\n"
                "aof_base_size:%lld\r\n"
                "aof_pending_rewrite:%d\r\n"
                "aof_buffer_length:%zu\r\n"
                "aof_rewrite_buffer_length:%lu\r\n"
                "aof_pending_bio_fsync:%llu\r\n"
                "aof_delayed_fsync:%lu\r\n",
                (long long) server.aof_current_size,
                (long long) server.aof_rewrite_base_size,
                server.aof_rewrite_scheduled,
                sdslen(server.aof_buf),
                aofRewriteBufferSize(),
                bioPendingJobsOfType(BIO_AOF_FSYNC),
                server.aof_delayed_fsync);
        }

        if (server.loading) {
            double perc = 0;
            time_t eta, elapsed;
            off_t remaining_bytes = 1;

            if (server.loading_total_bytes) {
                perc = ((double)server.loading_loaded_bytes / server.loading_total_bytes) * 100;
                remaining_bytes = server.loading_total_bytes - server.loading_loaded_bytes;
            } else if(server.loading_rdb_used_mem) {
                perc = ((double)server.loading_loaded_bytes / server.loading_rdb_used_mem) * 100;
                remaining_bytes = server.loading_rdb_used_mem - server.loading_loaded_bytes;
                /* used mem is only a (bad) estimation of the rdb file size, avoid going over 100% */
                if (perc > 99.99) perc = 99.99;
                if (remaining_bytes < 1) remaining_bytes = 1;
            }

            elapsed = time(NULL)-server.loading_start_time;
            if (elapsed == 0) {
                eta = 1; /* A fake 1 second figure if we don't have
                            enough info */
            } else {
                eta = (elapsed*remaining_bytes)/(server.loading_loaded_bytes+1);
            }

            info = sdscatprintf(info,
                "loading_start_time:%jd\r\n"
                "loading_total_bytes:%llu\r\n"
                "loading_rdb_used_mem:%llu\r\n"
                "loading_loaded_bytes:%llu\r\n"
                "loading_loaded_perc:%.2f\r\n"
                "loading_eta_seconds:%jd\r\n",
                (intmax_t) server.loading_start_time,
                (unsigned long long) server.loading_total_bytes,
                (unsigned long long) server.loading_rdb_used_mem,
                (unsigned long long) server.loading_loaded_bytes,
                perc,
                (intmax_t)eta
            );
        }
    }

    /* Stats */
    if (allsections || defsections || !strcasecmp(section,"stats")) {
        long long stat_total_reads_processed, stat_total_writes_processed;
        long long stat_net_input_bytes, stat_net_output_bytes;
        atomicGet(server.stat_total_reads_processed, stat_total_reads_processed);
        atomicGet(server.stat_total_writes_processed, stat_total_writes_processed);
        atomicGet(server.stat_net_input_bytes, stat_net_input_bytes);
        atomicGet(server.stat_net_output_bytes, stat_net_output_bytes);

        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Stats\r\n"
            "total_connections_received:%lld\r\n"
            "total_commands_processed:%lld\r\n"
            "instantaneous_ops_per_sec:%lld\r\n"
            "total_net_input_bytes:%lld\r\n"
            "total_net_output_bytes:%lld\r\n"
            "instantaneous_input_kbps:%.2f\r\n"
            "instantaneous_output_kbps:%.2f\r\n"
            "rejected_connections:%lld\r\n"
            "sync_full:%lld\r\n"
            "sync_partial_ok:%lld\r\n"
            "sync_partial_err:%lld\r\n"
            "expired_keys:%lld\r\n"
            "expired_stale_perc:%.2f\r\n"
            "expired_time_cap_reached_count:%lld\r\n"
            "expire_cycle_cpu_milliseconds:%lld\r\n"
            "evicted_keys:%lld\r\n"
            "keyspace_hits:%lld\r\n"
            "keyspace_misses:%lld\r\n"
            "pubsub_channels:%ld\r\n"
            "pubsub_patterns:%lu\r\n"
            "latest_fork_usec:%lld\r\n"
            "total_forks:%lld\r\n"
            "migrate_cached_sockets:%ld\r\n"
            "slave_expires_tracked_keys:%zu\r\n"
            "active_defrag_hits:%lld\r\n"
            "active_defrag_misses:%lld\r\n"
            "active_defrag_key_hits:%lld\r\n"
            "active_defrag_key_misses:%lld\r\n"
            "tracking_total_keys:%lld\r\n"
            "tracking_total_items:%lld\r\n"
            "tracking_total_prefixes:%lld\r\n"
            "unexpected_error_replies:%lld\r\n"
            "total_error_replies:%lld\r\n"
            "dump_payload_sanitizations:%lld\r\n"
            "total_reads_processed:%lld\r\n"
            "total_writes_processed:%lld\r\n"
            "io_threaded_reads_processed:%lld\r\n"
            "io_threaded_writes_processed:%lld\r\n",
            server.stat_numconnections,
            server.stat_numcommands,
            getInstantaneousMetric(STATS_METRIC_COMMAND),
            stat_net_input_bytes,
            stat_net_output_bytes,
            (float)getInstantaneousMetric(STATS_METRIC_NET_INPUT)/1024,
            (float)getInstantaneousMetric(STATS_METRIC_NET_OUTPUT)/1024,
            server.stat_rejected_conn,
            server.stat_sync_full,
            server.stat_sync_partial_ok,
            server.stat_sync_partial_err,
            server.stat_expiredkeys,
            server.stat_expired_stale_perc*100,
            server.stat_expired_time_cap_reached_count,
            server.stat_expire_cycle_time_used/1000,
            server.stat_evictedkeys,
            server.stat_keyspace_hits,
            server.stat_keyspace_misses,
            dictSize(server.pubsub_channels),
            dictSize(server.pubsub_patterns),
            server.stat_fork_time,
            server.stat_total_forks,
            dictSize(server.migrate_cached_sockets),
            getSlaveKeyWithExpireCount(),
            server.stat_active_defrag_hits,
            server.stat_active_defrag_misses,
            server.stat_active_defrag_key_hits,
            server.stat_active_defrag_key_misses,
            (unsigned long long) trackingGetTotalKeys(),
            (unsigned long long) trackingGetTotalItems(),
            (unsigned long long) trackingGetTotalPrefixes(),
            server.stat_unexpected_error_replies,
            server.stat_total_error_replies,
            server.stat_dump_payload_sanitizations,
            stat_total_reads_processed,
            stat_total_writes_processed,
            server.stat_io_reads_processed,
            server.stat_io_writes_processed);
    }

    /* Replication */
    if (allsections || defsections || !strcasecmp(section,"replication")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Replication\r\n"
            "role:%s\r\n",
            server.masterhost == NULL ? "master" : "slave");
        if (server.masterhost) {
            long long slave_repl_offset = 1;

            if (server.master)
                slave_repl_offset = server.master->reploff;
            else if (server.cached_master)
                slave_repl_offset = server.cached_master->reploff;

            info = sdscatprintf(info,
                "master_host:%s\r\n"
                "master_port:%d\r\n"
                "master_link_status:%s\r\n"
                "master_last_io_seconds_ago:%d\r\n"
                "master_sync_in_progress:%d\r\n"
                "slave_repl_offset:%lld\r\n"
                ,server.masterhost,
                server.masterport,
                (server.repl_state == REPL_STATE_CONNECTED) ?
                    "up" : "down",
                server.master ?
                ((int)(server.unixtime-server.master->lastinteraction)) : -1,
                server.repl_state == REPL_STATE_TRANSFER,
                slave_repl_offset
            );

            if (server.repl_state == REPL_STATE_TRANSFER) {
                double perc = 0;
                if (server.repl_transfer_size) {
                    perc = ((double)server.repl_transfer_read / server.repl_transfer_size) * 100;
                }
                info = sdscatprintf(info,
                    "master_sync_total_bytes:%lld\r\n"
                    "master_sync_read_bytes:%lld\r\n"
                    "master_sync_left_bytes:%lld\r\n"
                    "master_sync_perc:%.2f\r\n"
                    "master_sync_last_io_seconds_ago:%d\r\n",
                    (long long) server.repl_transfer_size,
                    (long long) server.repl_transfer_read,
                    (long long) (server.repl_transfer_size - server.repl_transfer_read),
                    perc,
                    (int)(server.unixtime-server.repl_transfer_lastio)
                );
            }

            if (server.repl_state != REPL_STATE_CONNECTED) {
                info = sdscatprintf(info,
                    "master_link_down_since_seconds:%jd\r\n",
                    server.repl_down_since ?
                    (intmax_t)(server.unixtime-server.repl_down_since) : -1);
            }
            info = sdscatprintf(info,
                "slave_priority:%d\r\n"
                "slave_read_only:%d\r\n"
                "replica_announced:%d\r\n",
                server.slave_priority,
                server.repl_slave_ro,
                server.replica_announced);
        }

        info = sdscatprintf(info,
            "connected_slaves:%lu\r\n",
            listLength(server.slaves));

        /* If min-slaves-to-write is active, write the number of slaves
         * currently considered 'good'. */
        if (server.repl_min_slaves_to_write &&
            server.repl_min_slaves_max_lag) {
            info = sdscatprintf(info,
                "min_slaves_good_slaves:%d\r\n",
                server.repl_good_slaves_count);
        }

        if (listLength(server.slaves)) {
            int slaveid = 0;
            listNode *ln;
            listIter li;

            listRewind(server.slaves,&li);
            while((ln = listNext(&li))) {
                client *slave = listNodeValue(ln);
                char *state = NULL;
                char ip[NET_IP_STR_LEN], *slaveip = slave->slave_addr;
                int port;
                long lag = 0;

                if (!slaveip) {
                    if (connPeerToString(slave->conn,ip,sizeof(ip),&port) == -1)
                        continue;
                    slaveip = ip;
                }
                switch(slave->replstate) {
                case SLAVE_STATE_WAIT_BGSAVE_START:
                case SLAVE_STATE_WAIT_BGSAVE_END:
                    state = "wait_bgsave";
                    break;
                case SLAVE_STATE_SEND_BULK:
                    state = "send_bulk";
                    break;
                case SLAVE_STATE_ONLINE:
                    state = "online";
                    break;
                }
                if (state == NULL) continue;
                if (slave->replstate == SLAVE_STATE_ONLINE)
                    lag = time(NULL) - slave->repl_ack_time;

                info = sdscatprintf(info,
                    "slave%d:ip=%s,port=%d,state=%s,"
                    "offset=%lld,lag=%ld\r\n",
                    slaveid,slaveip,slave->slave_listening_port,state,
                    slave->repl_ack_off, lag);
                slaveid++;
            }
        }
        info = sdscatprintf(info,
            "master_failover_state:%s\r\n"
            "master_replid:%s\r\n"
            "master_replid2:%s\r\n"
            "master_repl_offset:%lld\r\n"
            "second_repl_offset:%lld\r\n"
            "repl_backlog_active:%d\r\n"
            "repl_backlog_size:%lld\r\n"
            "repl_backlog_first_byte_offset:%lld\r\n"
            "repl_backlog_histlen:%lld\r\n",
            getFailoverStateString(),
            server.replid,
            server.replid2,
            server.master_repl_offset,
            server.second_replid_offset,
            server.repl_backlog != NULL,
            server.repl_backlog_size,
            server.repl_backlog_off,
            server.repl_backlog_histlen);
    }

    /* CPU */
    if (allsections || defsections || !strcasecmp(section,"cpu")) {
        if (sections++) info = sdscat(info,"\r\n");

        struct rusage self_ru, c_ru;
        getrusage(RUSAGE_SELF, &self_ru);
        getrusage(RUSAGE_CHILDREN, &c_ru);
        info = sdscatprintf(info,
        "# CPU\r\n"
        "used_cpu_sys:%ld.%06ld\r\n"
        "used_cpu_user:%ld.%06ld\r\n"
        "used_cpu_sys_children:%ld.%06ld\r\n"
        "used_cpu_user_children:%ld.%06ld\r\n",
        (long)self_ru.ru_stime.tv_sec, (long)self_ru.ru_stime.tv_usec,
        (long)self_ru.ru_utime.tv_sec, (long)self_ru.ru_utime.tv_usec,
        (long)c_ru.ru_stime.tv_sec, (long)c_ru.ru_stime.tv_usec,
        (long)c_ru.ru_utime.tv_sec, (long)c_ru.ru_utime.tv_usec);
#ifdef RUSAGE_THREAD
        struct rusage m_ru;
        getrusage(RUSAGE_THREAD, &m_ru);
        info = sdscatprintf(info,
            "used_cpu_sys_main_thread:%ld.%06ld\r\n"
            "used_cpu_user_main_thread:%ld.%06ld\r\n",
            (long)m_ru.ru_stime.tv_sec, (long)m_ru.ru_stime.tv_usec,
            (long)m_ru.ru_utime.tv_sec, (long)m_ru.ru_utime.tv_usec);
#endif  /* RUSAGE_THREAD */
    }

    /* Modules */
    if (allsections || defsections || !strcasecmp(section,"modules")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,"# Modules\r\n");
        info = genModulesInfoString(info);
    }

    /* Command statistics */
    if (allsections || !strcasecmp(section,"commandstats")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info, "# Commandstats\r\n");

        struct redisCommand *c;
        dictEntry *de;
        dictIterator *di;
        di = dictGetSafeIterator(server.commands);
        while((de = dictNext(di)) != NULL) {
            char *tmpsafe;
            c = (struct redisCommand *) dictGetVal(de);
            if (!c->calls && !c->failed_calls && !c->rejected_calls)
                continue;
            info = sdscatprintf(info,
                "cmdstat_%s:calls=%lld,usec=%lld,usec_per_call=%.2f"
                ",rejected_calls=%lld,failed_calls=%lld\r\n",
                getSafeInfoString(c->name, strlen(c->name), &tmpsafe), c->calls, c->microseconds,
                (c->calls == 0) ? 0 : ((float)c->microseconds/c->calls),
                c->rejected_calls, c->failed_calls);
            if (tmpsafe != NULL) zfree(tmpsafe);
        }
        dictReleaseIterator(di);
    }
    /* Error statistics */
    if (allsections || defsections || !strcasecmp(section,"errorstats")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscat(info, "# Errorstats\r\n");
        raxIterator ri;
        raxStart(&ri,server.errors);
        raxSeek(&ri,"^",NULL,0);
        struct redisError *e;
        while(raxNext(&ri)) {
            char *tmpsafe;
            e = (struct redisError *) ri.data;
            info = sdscatprintf(info,
                "errorstat_%.*s:count=%lld\r\n",
                (int)ri.key_len, getSafeInfoString((char *) ri.key, ri.key_len, &tmpsafe), e->count);
            if (tmpsafe != NULL) zfree(tmpsafe);
        }
        raxStop(&ri);
    }

    /* Cluster */
    if (allsections || defsections || !strcasecmp(section,"cluster")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
        "# Cluster\r\n"
        "cluster_enabled:%d\r\n",
        server.cluster_enabled);
    }

    /* Key space */
    if (allsections || defsections || !strcasecmp(section,"keyspace")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info, "# Keyspace\r\n");
        for (j = 0; j < server.dbnum; j++) {
            long long keys, vkeys;

            keys = dictSize(server.db[j].dict);
            vkeys = dictSize(server.db[j].expires);
            if (keys || vkeys) {
                info = sdscatprintf(info,
                    "db%d:keys=%lld,expires=%lld,avg_ttl=%lld\r\n",
                    j, keys, vkeys, server.db[j].avg_ttl);
            }
        }
    }

    /* Get info from modules.
     * if user asked for "everything" or "modules", or a specific section
     * that's not found yet. */
    if (everything || modules ||
        (!allsections && !defsections && sections==0)) {
        info = modulesCollectInfo(info,
                                  everything || modules ? NULL: section,
                                  0, /* not a crash report */
                                  sections);
    }
    return info;
}

void infoCommand(client *c) {
    char *section = c->argc == 2 ? c->argv[1]->ptr : "default";

    if (c->argc > 2) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }
    sds info = genRedisInfoString(section);
    addReplyVerbatim(c,info,sdslen(info),"txt");
    sdsfree(info);
}

void monitorCommand(client *c) {
    if (c->flags & CLIENT_DENY_BLOCKING) {
        /**
         * A client that has CLIENT_DENY_BLOCKING flag on
         * expects a reply per command and so can't execute MONITOR. */
        addReplyError(c, "MONITOR isn't allowed for DENY BLOCKING client");
        return;
    }

    /* ignore MONITOR if already slave or in monitor mode */
    if (c->flags & CLIENT_SLAVE) return;

    c->flags |= (CLIENT_SLAVE|CLIENT_MONITOR);
    listAddNodeTail(server.monitors,c);
    addReply(c,shared.ok);
}

/* =================================== Main! ================================ */

int checkIgnoreWarning(const char *warning) {
    int argc, j;
    sds *argv = sdssplitargs(server.ignore_warnings, &argc);
    if (argv == NULL)
        return 0;

    for (j = 0; j < argc; j++) {
        char *flag = argv[j];
        if (!strcasecmp(flag, warning))
            break;
    }
    sdsfreesplitres(argv,argc);
    return j < argc;
}

#ifdef __linux__
int linuxOvercommitMemoryValue(void) {
    FILE *fp = fopen("/proc/sys/vm/overcommit_memory","r");
    char buf[64];

    if (!fp) return -1;
    if (fgets(buf,64,fp) == NULL) {
        fclose(fp);
        return -1;
    }
    fclose(fp);

    return atoi(buf);
}

void linuxMemoryWarnings(void) {
    if (linuxOvercommitMemoryValue() == 0) {
        serverLog(LL_WARNING,"WARNING overcommit_memory is set to 0! Background save may fail under low memory condition. To fix this issue add 'vm.overcommit_memory = 1' to /etc/sysctl.conf and then reboot or run the command 'sysctl vm.overcommit_memory=1' for this to take effect.");
    }
    if (THPIsEnabled() && THPDisable()) {
        serverLog(LL_WARNING,"WARNING you have Transparent Huge Pages (THP) support enabled in your kernel. This will create latency and memory usage issues with Redis. To fix this issue run the command 'echo madvise > /sys/kernel/mm/transparent_hugepage/enabled' as root, and add it to your /etc/rc.local in order to retain the setting after a reboot. Redis must be restarted after THP is disabled (set to 'madvise' or 'never').");
    }
}

#ifdef __arm64__

/* Get size in kilobytes of the Shared_Dirty pages of the calling process for the
 * memory map corresponding to the provided address, or -1 on error. */
static int smapsGetSharedDirty(unsigned long addr) {
    int ret, in_mapping = 0, val = -1;
    unsigned long from, to;
    char buf[64];
    FILE *f;

    f = fopen("/proc/self/smaps", "r");
    if (!f) return -1;

    while (1) {
        if (!fgets(buf, sizeof(buf), f))
            break;

        ret = sscanf(buf, "%lx-%lx", &from, &to);
        if (ret == 2)
            in_mapping = from <= addr && addr < to;

        if (in_mapping && !memcmp(buf, "Shared_Dirty:", 13)) {
            sscanf(buf, "%*s %d", &val);
            /* If parsing fails, we remain with val == -1 */
            break;
        }
    }

    fclose(f);
    return val;
}

/* Older arm64 Linux kernels have a bug that could lead to data corruption
 * during background save in certain scenarios. This function checks if the
 * kernel is affected.
 * The bug was fixed in commit ff1712f953e27f0b0718762ec17d0adb15c9fd0b
 * titled: "arm64: pgtable: Ensure dirty bit is preserved across pte_wrprotect()"
 * Return -1 on unexpected test failure, 1 if the kernel seems to be affected,
 * and 0 otherwise. */
int linuxMadvFreeForkBugCheck(void) {
    int ret, pipefd[2] = { -1, -1 };
    pid_t pid;
    char *p = NULL, *q;
    int bug_found = 0;
    long page_size = sysconf(_SC_PAGESIZE);
    long map_size = 3 * page_size;

    /* Create a memory map that's in our full control (not one used by the allocator). */
    p = mmap(NULL, map_size, PROT_READ, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (p == MAP_FAILED) {
        serverLog(LL_WARNING, "Failed to mmap(): %s", strerror(errno));
        return -1;
    }

    q = p + page_size;

    /* Split the memory map in 3 pages by setting their protection as RO|RW|RO to prevent
     * Linux from merging this memory map with adjacent VMAs. */
    ret = mprotect(q, page_size, PROT_READ | PROT_WRITE);
    if (ret < 0) {
        serverLog(LL_WARNING, "Failed to mprotect(): %s", strerror(errno));
        bug_found = -1;
        goto exit;
    }

    /* Write to the page once to make it resident */
    *(volatile char*)q = 0;

    /* Tell the kernel that this page is free to be reclaimed. */
#ifndef MADV_FREE
#define MADV_FREE 8
#endif
    ret = madvise(q, page_size, MADV_FREE);
    if (ret < 0) {
        /* MADV_FREE is not available on older kernels that are presumably
         * not affected. */
        if (errno == EINVAL) goto exit;

        serverLog(LL_WARNING, "Failed to madvise(): %s", strerror(errno));
        bug_found = -1;
        goto exit;
    }

    /* Write to the page after being marked for freeing, this is supposed to take
     * ownership of that page again. */
    *(volatile char*)q = 0;

    /* Create a pipe for the child to return the info to the parent. */
    ret = pipe(pipefd);
    if (ret < 0) {
        serverLog(LL_WARNING, "Failed to create pipe: %s", strerror(errno));
        bug_found = -1;
        goto exit;
    }

    /* Fork the process. */
    pid = fork();
    if (pid < 0) {
        serverLog(LL_WARNING, "Failed to fork: %s", strerror(errno));
        bug_found = -1;
        goto exit;
    } else if (!pid) {
        /* Child: check if the page is marked as dirty, page_size in kb.
         * A value of 0 means the kernel is affected by the bug. */
        ret = smapsGetSharedDirty((unsigned long) q);
        if (!ret)
            bug_found = 1;
        else if (ret == -1)     /* Failed to read */
            bug_found = -1;

        if (write(pipefd[1], &bug_found, sizeof(bug_found)) < 0)
            serverLog(LL_WARNING, "Failed to write to parent: %s", strerror(errno));
        exit(0);
    } else {
        /* Read the result from the child. */
        ret = read(pipefd[0], &bug_found, sizeof(bug_found));
        if (ret < 0) {
            serverLog(LL_WARNING, "Failed to read from child: %s", strerror(errno));
            bug_found = -1;
        }

        /* Reap the child pid. */
        waitpid(pid, NULL, 0);
    }

exit:
    /* Cleanup */
    if (pipefd[0] != -1) close(pipefd[0]);
    if (pipefd[1] != -1) close(pipefd[1]);
    if (p != NULL) munmap(p, map_size);

    return bug_found;
}
#endif /* __arm64__ */
#endif /* __linux__ */

void createPidFile(void) {
    /* If pidfile requested, but no pidfile defined, use
     * default pidfile path */
    if (!server.pidfile) server.pidfile = zstrdup(CONFIG_DEFAULT_PID_FILE);

    /* Try to write the pid file in a best-effort way. */
    FILE *fp = fopen(server.pidfile,"w");
    if (fp) {
        fprintf(fp,"%d\n",(int)getpid());
        fclose(fp);
    }
}

void daemonize(void) {
    int fd;

    if (fork() != 0) exit(0); /* parent exits */
    setsid(); /* create a new session */

    /* Every output goes to /dev/null. If Redis is daemonized but
     * the 'logfile' is set to 'stdout' in the configuration file
     * it will not log at all. */
    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
}

void version(void) {
    printf("Redis server v=%s sha=%s:%d malloc=%s bits=%d build=%llx\n",
        REDIS_VERSION,
        redisGitSHA1(),
        atoi(redisGitDirty()) > 0,
        ZMALLOC_LIB,
        sizeof(long) == 4 ? 32 : 64,
        (unsigned long long) redisBuildId());
    exit(0);
}

void usage(void) {
    fprintf(stderr,"Usage: ./redis-server [/path/to/redis.conf] [options] [-]\n");
    fprintf(stderr,"       ./redis-server - (read config from stdin)\n");
    fprintf(stderr,"       ./redis-server -v or --version\n");
    fprintf(stderr,"       ./redis-server -h or --help\n");
    fprintf(stderr,"       ./redis-server --test-memory <megabytes>\n\n");
    fprintf(stderr,"Examples:\n");
    fprintf(stderr,"       ./redis-server (run the server with default conf)\n");
    fprintf(stderr,"       ./redis-server /etc/redis/6379.conf\n");
    fprintf(stderr,"       ./redis-server --port 7777\n");
    fprintf(stderr,"       ./redis-server --port 7777 --replicaof 127.0.0.1 8888\n");
    fprintf(stderr,"       ./redis-server /etc/myredis.conf --loglevel verbose -\n");
    fprintf(stderr,"       ./redis-server /etc/myredis.conf --loglevel verbose\n\n");
    fprintf(stderr,"Sentinel mode:\n");
    fprintf(stderr,"       ./redis-server /etc/sentinel.conf --sentinel\n");
    exit(1);
}

void redisAsciiArt(void) {
#include "asciilogo.h"
    char *buf = zmalloc(1024*16);
    char *mode;

    if (server.cluster_enabled) mode = "cluster";
    else if (server.sentinel_mode) mode = "sentinel";
    else mode = "standalone";

    /* Show the ASCII logo if: log file is stdout AND stdout is a
     * tty AND syslog logging is disabled. Also show logo if the user
     * forced us to do so via redis.conf. */
    int show_logo = ((!server.syslog_enabled &&
                      server.logfile[0] == '\0' &&
                      isatty(fileno(stdout))) ||
                     server.always_show_logo);

    if (!show_logo) {
        serverLog(LL_NOTICE,
            "Running mode=%s, port=%d.",
            mode, server.port ? server.port : server.tls_port
        );
    } else {
        snprintf(buf,1024*16,ascii_logo,
            REDIS_VERSION,
            redisGitSHA1(),
            strtol(redisGitDirty(),NULL,10) > 0,
            (sizeof(long) == 8) ? "64" : "32",
            mode, server.port ? server.port : server.tls_port,
            (long) getpid()
        );
        serverLogRaw(LL_NOTICE|LL_RAW,buf);
    }
    zfree(buf);
}

int changeBindAddr(sds *addrlist, int addrlist_len) {
    int i;
    int result = C_OK;

    char *prev_bindaddr[CONFIG_BINDADDR_MAX];
    int prev_bindaddr_count;

    /* Close old TCP and TLS servers */
    closeSocketListeners(&server.ipfd);
    closeSocketListeners(&server.tlsfd);

    /* Keep previous settings */
    prev_bindaddr_count = server.bindaddr_count;
    memcpy(prev_bindaddr, server.bindaddr, sizeof(server.bindaddr));

    /* Copy new settings */
    memset(server.bindaddr, 0, sizeof(server.bindaddr));
    for (i = 0; i < addrlist_len; i++) {
        server.bindaddr[i] = zstrdup(addrlist[i]);
    }
    server.bindaddr_count = addrlist_len;

    /* Bind to the new port */
    if ((server.port != 0 && listenToPort(server.port, &server.ipfd) != C_OK) ||
        (server.tls_port != 0 && listenToPort(server.tls_port, &server.tlsfd) != C_OK)) {
        serverLog(LL_WARNING, "Failed to bind, trying to restore old listening sockets.");

        /* Restore old bind addresses */
        for (i = 0; i < addrlist_len; i++) {
            zfree(server.bindaddr[i]);
        }
        memcpy(server.bindaddr, prev_bindaddr, sizeof(server.bindaddr));
        server.bindaddr_count = prev_bindaddr_count;

        /* Re-Listen TCP and TLS */
        server.ipfd.count = 0;
        if (server.port != 0 && listenToPort(server.port, &server.ipfd) != C_OK) {
            serverPanic("Failed to restore old listening sockets.");
        }

        server.tlsfd.count = 0;
        if (server.tls_port != 0 && listenToPort(server.tls_port, &server.tlsfd) != C_OK) {
            serverPanic("Failed to restore old listening sockets.");
        }

        result = C_ERR;
    } else {
        /* Free old bind addresses */
        for (i = 0; i < prev_bindaddr_count; i++) {
            zfree(prev_bindaddr[i]);
        }
    }

    /* Create TCP and TLS event handlers */
    if (createSocketAcceptHandler(&server.ipfd, acceptTcpHandler) != C_OK) {
        serverPanic("Unrecoverable error creating TCP socket accept handler.");
    }
    if (createSocketAcceptHandler(&server.tlsfd, acceptTLSHandler) != C_OK) {
        serverPanic("Unrecoverable error creating TLS socket accept handler.");
    }

    if (server.set_proc_title) redisSetProcTitle(NULL);

    return result;
}

int changeListenPort(int port, socketFds *sfd, aeFileProc *accept_handler) {
    socketFds new_sfd = {{0}};

    /* Just close the server if port disabled */
    if (port == 0) {
        closeSocketListeners(sfd);
        if (server.set_proc_title) redisSetProcTitle(NULL);
        return C_OK;
    }

    /* Bind to the new port */
    if (listenToPort(port, &new_sfd) != C_OK) {
        return C_ERR;
    }

    /* Create event handlers */
    if (createSocketAcceptHandler(&new_sfd, accept_handler) != C_OK) {
        closeSocketListeners(&new_sfd);
        return C_ERR;
    }

    /* Close old servers */
    closeSocketListeners(sfd);

    /* Copy new descriptors */
    sfd->count = new_sfd.count;
    memcpy(sfd->fd, new_sfd.fd, sizeof(new_sfd.fd));

    if (server.set_proc_title) redisSetProcTitle(NULL);

    return C_OK;
}

static void sigShutdownHandler(int sig) {
    char *msg;

    switch (sig) {
    case SIGINT:
        msg = "Received SIGINT scheduling shutdown...";
        break;
    case SIGTERM:
        msg = "Received SIGTERM scheduling shutdown...";
        break;
    default:
        msg = "Received shutdown signal, scheduling shutdown...";
    };

    /* SIGINT is often delivered via Ctrl+C in an interactive session.
     * If we receive the signal the second time, we interpret this as
     * the user really wanting to quit ASAP without waiting to persist
     * on disk. */
    if (server.shutdown_asap && sig == SIGINT) {
        serverLogFromHandler(LL_WARNING, "You insist... exiting now.");
        rdbRemoveTempFile(getpid(), 1);
        exit(1); /* Exit with an error since this was not a clean shutdown. */
    } else if (server.loading) {
        serverLogFromHandler(LL_WARNING, "Received shutdown signal during loading, exiting now.");
        exit(0);
    }

    serverLogFromHandler(LL_WARNING, msg);
    server.shutdown_asap = 1;
}

void setupSignalHandlers(void) {
    struct sigaction act;

    /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction is used.
     * Otherwise, sa_handler is used. */
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = sigShutdownHandler;
    sigaction(SIGTERM, &act, NULL);
    sigaction(SIGINT, &act, NULL);

    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = sigsegvHandler;
    if(server.crashlog_enabled) {
        sigaction(SIGSEGV, &act, NULL);
        sigaction(SIGBUS, &act, NULL);
        sigaction(SIGFPE, &act, NULL);
        sigaction(SIGILL, &act, NULL);
        sigaction(SIGABRT, &act, NULL);
    }
    return;
}

void removeSignalHandlers(void) {
    struct sigaction act;
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_RESETHAND;
    act.sa_handler = SIG_DFL;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGILL, &act, NULL);
    sigaction(SIGABRT, &act, NULL);
}

/* This is the signal handler for children process. It is currently useful
 * in order to track the SIGUSR1, that we send to a child in order to terminate
 * it in a clean way, without the parent detecting an error and stop
 * accepting writes because of a write error condition. */
static void sigKillChildHandler(int sig) {
    UNUSED(sig);
    int level = server.in_fork_child == CHILD_TYPE_MODULE? LL_VERBOSE: LL_WARNING;
    serverLogFromHandler(level, "Received SIGUSR1 in child, exiting now.");
    exitFromChild(SERVER_CHILD_NOERROR_RETVAL);
}

void setupChildSignalHandlers(void) {
    struct sigaction act;

    /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction is used.
     * Otherwise, sa_handler is used. */
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = sigKillChildHandler;
    sigaction(SIGUSR1, &act, NULL);
    return;
}

/* After fork, the child process will inherit the resources
 * of the parent process, e.g. fd(socket or flock) etc.
 * should close the resources not used by the child process, so that if the
 * parent restarts it can bind/lock despite the child possibly still running. */
void closeChildUnusedResourceAfterFork() {
    closeListeningSockets(0);
    if (server.cluster_enabled && server.cluster_config_file_lock_fd != -1)
        close(server.cluster_config_file_lock_fd);  /* don't care if this fails */

    /* Clear server.pidfile, this is the parent pidfile which should not
     * be touched (or deleted) by the child (on exit / crash) */
    zfree(server.pidfile);
    server.pidfile = NULL;
}

/* purpose is one of CHILD_TYPE_ types */
int redisFork(int purpose) {
    if (isMutuallyExclusiveChildType(purpose)) {
        // 如果是与其他子进程互斥的，且当前有互斥子进程在执行，则返回-1
        if (hasActiveChildProcess())
            return -1;

        // 如果可以fork，这里创建父子进程通信的管道。
        openChildInfoPipe();
    }

    int childpid;
    long long start = ustime();
    if ((childpid = fork()) == 0) {
        /* Child */
        server.in_fork_child = purpose;
        setOOMScoreAdj(CONFIG_OOM_BGCHILD);
        setupChildSignalHandlers();
        closeChildUnusedResourceAfterFork();
    } else {
        /* Parent */
        server.stat_total_forks++;
        server.stat_fork_time = ustime()-start;
        server.stat_fork_rate = (double) zmalloc_used_memory() * 1000000 / server.stat_fork_time / (1024*1024*1024); /* GB per second. */
        latencyAddSampleIfNeeded("fork",server.stat_fork_time/1000);
        if (childpid == -1) {
            if (isMutuallyExclusiveChildType(purpose)) closeChildInfoPipe();
            return -1;
        }

        /* The child_pid and child_type are only for mutual exclusive children.
         * other child types should handle and store their pid's in dedicated variables.
         *
         * Today, we allows CHILD_TYPE_LDB to run in parallel with the other fork types:
         * - it isn't used for production, so it will not make the server be less efficient
         * - used for debugging, and we don't want to block it from running while other
         *   forks are running (like RDB and AOF) */
        // child_pid 和 child_type 仅用于标识互斥的子进程。其他类型的子进程应该处理并存储他们的pid在专用变量中。
        // 目前，我们允许CHILD_TYPE_LDB跟其他子类型进程并行执行。
        // LDB不用于生产环境，所以不会降低服务器效率。另外它是用于调试的，所以在有其他子进程执行时我们也不想阻塞它。
        if (isMutuallyExclusiveChildType(purpose)) {
            server.child_pid = childpid;
            server.child_type = purpose;
            server.stat_current_cow_bytes = 0;
            server.stat_current_cow_updated = 0;
            server.stat_current_save_keys_processed = 0;
            server.stat_module_progress = 0;
            server.stat_current_save_keys_total = dbTotalServerKeyCount();
        }

        updateDictResizePolicy();
        moduleFireServerEvent(REDISMODULE_EVENT_FORK_CHILD,
                              REDISMODULE_SUBEVENT_FORK_CHILD_BORN,
                              NULL);
    }
    return childpid;
}

void sendChildCowInfo(childInfoType info_type, char *pname) {
    sendChildInfoGeneric(info_type, 0, -1, pname);
}

void sendChildInfo(childInfoType info_type, size_t keys, char *pname) {
    sendChildInfoGeneric(info_type, keys, -1, pname);
}

void memtest(size_t megabytes, int passes);

/* Returns 1 if there is --sentinel among the arguments or if
 * argv[0] contains "redis-sentinel". */
// 检查当前服务是否sentinel启动
int checkForSentinelMode(int argc, char **argv) {
    int j;

    if (strstr(argv[0],"redis-sentinel") != NULL) return 1;
    for (j = 1; j < argc; j++)
        if (!strcmp(argv[j],"--sentinel")) return 1;
    return 0;
}

/* Function called at startup to load RDB or AOF file in memory. */
void loadDataFromDisk(void) {
    long long start = ustime();
    if (server.aof_state == AOF_ON) {
        if (loadAppendOnlyFile(server.aof_filename) == C_OK)
            serverLog(LL_NOTICE,"DB loaded from append only file: %.3f seconds",(float)(ustime()-start)/1000000);
    } else {
        rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
        errno = 0; /* Prevent a stale value from affecting error checking */
        if (rdbLoad(server.rdb_filename,&rsi,RDBFLAGS_NONE) == C_OK) {
            serverLog(LL_NOTICE,"DB loaded from disk: %.3f seconds",
                (float)(ustime()-start)/1000000);

            /* Restore the replication ID / offset from the RDB file. */
            if ((server.masterhost ||
                (server.cluster_enabled &&
                nodeIsSlave(server.cluster->myself))) &&
                rsi.repl_id_is_set &&
                rsi.repl_offset != -1 &&
                /* Note that older implementations may save a repl_stream_db
                 * of -1 inside the RDB file in a wrong way, see more
                 * information in function rdbPopulateSaveInfo. */
                rsi.repl_stream_db != -1)
            {
                memcpy(server.replid,rsi.repl_id,sizeof(server.replid));
                server.master_repl_offset = rsi.repl_offset;
                /* If we are a slave, create a cached master from this
                 * information, in order to allow partial resynchronizations
                 * with masters. */
                replicationCacheMasterUsingMyself();
                selectDb(server.cached_master,rsi.repl_stream_db);
            }
        } else if (errno != ENOENT) {
            serverLog(LL_WARNING,"Fatal error loading the DB: %s. Exiting.",strerror(errno));
            exit(1);
        }
    }
}

void redisOutOfMemoryHandler(size_t allocation_size) {
    serverLog(LL_WARNING,"Out Of Memory allocating %zu bytes!",
        allocation_size);
    serverPanic("Redis aborting for OUT OF MEMORY. Allocating %zu bytes!",
        allocation_size);
}

/* Callback for sdstemplate on proc-title-template. See redis.conf for
 * supported variables.
 */
static sds redisProcTitleGetVariable(const sds varname, void *arg)
{
    if (!strcmp(varname, "title")) {
        return sdsnew(arg);
    } else if (!strcmp(varname, "listen-addr")) {
        if (server.port || server.tls_port)
            return sdscatprintf(sdsempty(), "%s:%u",
                                server.bindaddr_count ? server.bindaddr[0] : "*",
                                server.port ? server.port : server.tls_port);
        else
            return sdscatprintf(sdsempty(), "unixsocket:%s", server.unixsocket);
    } else if (!strcmp(varname, "server-mode")) {
        if (server.cluster_enabled) return sdsnew("[cluster]");
        else if (server.sentinel_mode) return sdsnew("[sentinel]");
        else return sdsempty();
    } else if (!strcmp(varname, "config-file")) {
        return sdsnew(server.configfile ? server.configfile : "-");
    } else if (!strcmp(varname, "port")) {
        return sdscatprintf(sdsempty(), "%u", server.port);
    } else if (!strcmp(varname, "tls-port")) {
        return sdscatprintf(sdsempty(), "%u", server.tls_port);
    } else if (!strcmp(varname, "unixsocket")) {
        return sdsnew(server.unixsocket);
    } else
        return NULL;    /* Unknown variable name */
}

/* Expand the specified proc-title-template string and return a newly
 * allocated sds, or NULL. */
static sds expandProcTitleTemplate(const char *template, const char *title) {
    sds res = sdstemplate(template, redisProcTitleGetVariable, (void *) title);
    if (!res)
        return NULL;
    return sdstrim(res, " ");
}
/* Validate the specified template, returns 1 if valid or 0 otherwise. */
int validateProcTitleTemplate(const char *template) {
    int ok = 1;
    sds res = expandProcTitleTemplate(template, "");
    if (!res)
        return 0;
    if (sdslen(res) == 0) ok = 0;
    sdsfree(res);
    return ok;
}

int redisSetProcTitle(char *title) {
#ifdef USE_SETPROCTITLE
    if (!title) title = server.exec_argv[0];
    sds proc_title = expandProcTitleTemplate(server.proc_title_template, title);
    if (!proc_title) return C_ERR;  /* Not likely, proc_title_template is validated */

    setproctitle("%s", proc_title);
    sdsfree(proc_title);
#else
    UNUSED(title);
#endif

    return C_OK;
}

void redisSetCpuAffinity(const char *cpulist) {
#ifdef USE_SETCPUAFFINITY
    setcpuaffinity(cpulist);
#else
    UNUSED(cpulist);
#endif
}

/* Send a notify message to systemd. Returns sd_notify return code which is
 * a positive number on success. */
int redisCommunicateSystemd(const char *sd_notify_msg) {
#ifdef HAVE_LIBSYSTEMD
    int ret = sd_notify(0, sd_notify_msg);

    if (ret == 0)
        serverLog(LL_WARNING, "systemd supervision error: NOTIFY_SOCKET not found!");
    else if (ret < 0)
        serverLog(LL_WARNING, "systemd supervision error: sd_notify: %d", ret);
    return ret;
#else
    UNUSED(sd_notify_msg);
    return 0;
#endif
}

/* Attempt to set up upstart supervision. Returns 1 if successful. */
static int redisSupervisedUpstart(void) {
    const char *upstart_job = getenv("UPSTART_JOB");

    if (!upstart_job) {
        serverLog(LL_WARNING,
                "upstart supervision requested, but UPSTART_JOB not found!");
        return 0;
    }

    serverLog(LL_NOTICE, "supervised by upstart, will stop to signal readiness.");
    raise(SIGSTOP);
    unsetenv("UPSTART_JOB");
    return 1;
}

/* Attempt to set up systemd supervision. Returns 1 if successful. */
static int redisSupervisedSystemd(void) {
#ifndef HAVE_LIBSYSTEMD
    serverLog(LL_WARNING,
            "systemd supervision requested or auto-detected, but Redis is compiled without libsystemd support!");
    return 0;
#else
    if (redisCommunicateSystemd("STATUS=Redis is loading...\n") <= 0)
        return 0;
    serverLog(LL_NOTICE,
        "Supervised by systemd. Please make sure you set appropriate values for TimeoutStartSec and TimeoutStopSec in your service unit.");
    return 1;
#endif
}

int redisIsSupervised(int mode) {
    int ret = 0;

    if (mode == SUPERVISED_AUTODETECT) {
        if (getenv("UPSTART_JOB")) {
            serverLog(LL_VERBOSE, "Upstart supervision detected.");
            mode = SUPERVISED_UPSTART;
        } else if (getenv("NOTIFY_SOCKET")) {
            serverLog(LL_VERBOSE, "Systemd supervision detected.");
            mode = SUPERVISED_SYSTEMD;
        }
    }

    switch (mode) {
        case SUPERVISED_UPSTART:
            ret = redisSupervisedUpstart();
            break;
        case SUPERVISED_SYSTEMD:
            ret = redisSupervisedSystemd();
            break;
        default:
            break;
    }

    if (ret)
        server.supervised_mode = mode;

    return ret;
}

int iAmMaster(void) {
    return ((!server.cluster_enabled && server.masterhost == NULL) ||
            (server.cluster_enabled && nodeIsMaster(server.cluster->myself)));
}

#ifdef REDIS_TEST
typedef int redisTestProc(int argc, char **argv, int accurate);
struct redisTest {
    char *name;
    redisTestProc *proc;
    int failed;
} redisTests[] = {
    {"ziplist", ziplistTest},
    {"quicklist", quicklistTest},
    {"intset", intsetTest},
    {"zipmap", zipmapTest},
    {"sha1test", sha1Test},
    {"util", utilTest},
    {"endianconv", endianconvTest},
    {"crc64", crc64Test},
    {"zmalloc", zmalloc_test},
    {"sds", sdsTest},
    {"dict", dictTest}
};
redisTestProc *getTestProcByName(const char *name) {
    int numtests = sizeof(redisTests)/sizeof(struct redisTest);
    for (int j = 0; j < numtests; j++) {
        if (!strcasecmp(name,redisTests[j].name)) {
            return redisTests[j].proc;
        }
    }
    return NULL;
}
#endif

int main(int argc, char **argv) {
    struct timeval tv;
    int j;
    char config_from_stdin = 0;

#ifdef REDIS_TEST
    if (argc >= 3 && !strcasecmp(argv[1], "test")) {
        int accurate = 0;
        for (j = 3; j < argc; j++) {
            if (!strcasecmp(argv[j], "--accurate")) {
                accurate = 1;
            }
        }

        if (!strcasecmp(argv[2], "all")) {
            int numtests = sizeof(redisTests)/sizeof(struct redisTest);
            for (j = 0; j < numtests; j++) {
                redisTests[j].failed = (redisTests[j].proc(argc,argv,accurate) != 0);
            }

            /* Report tests result */
            int failed_num = 0;
            for (j = 0; j < numtests; j++) {
                if (redisTests[j].failed) {
                    failed_num++;
                    printf("[failed] Test - %s\n", redisTests[j].name);
                } else {
                    printf("[ok] Test - %s\n", redisTests[j].name);
                }
            }

            printf("%d tests, %d passed, %d failed\n", numtests,
                   numtests-failed_num, failed_num);

            return failed_num == 0 ? 0 : 1;
        } else {
            redisTestProc *proc = getTestProcByName(argv[2]);
            if (!proc) return -1; /* test not found */
            return proc(argc,argv,accurate);
        }

        return 0;
    }
#endif

    /* We need to initialize our libraries, and the server configuration. */
#ifdef INIT_SETPROCTITLE_REPLACEMENT
    spt_init(argc, argv);
#endif
    setlocale(LC_COLLATE,"");
    tzset(); /* Populates 'timezone' global. */
    zmalloc_set_oom_handler(redisOutOfMemoryHandler);
    srand(time(NULL)^getpid());
    srandom(time(NULL)^getpid());
    gettimeofday(&tv,NULL);
    init_genrand64(((long long) tv.tv_sec * 1000000 + tv.tv_usec) ^ getpid());
    crc64_init();

    /* Store umask value. Because umask(2) only offers a set-and-get API we have
     * to reset it and restore it back. We do this early to avoid a potential
     * race condition with threads that could be creating files or directories.
     */
    umask(server.umask = umask(0777));

    // 设置hash种子
    uint8_t hashseed[16];
    getRandomBytes(hashseed,sizeof(hashseed));
    dictSetHashFunctionSeed(hashseed);
    // 检查是否是sentinel模式
    server.sentinel_mode = checkForSentinelMode(argc,argv);
    // 初始化server 配置参数。
    initServerConfig();
    // 初始化ACL子系统，基础网络连接以及创建client都需要使用ACL，所以尽可能早的初始化
    ACLInit(); /* The ACL subsystem must be initialized ASAP because the
                  basic networking code and client creation depends on it. */
    // module系统初始化
    moduleInitModulesSystem();
    // tls初始化
    tlsInit();

    /* Store the executable path and arguments in a safe place in order
     * to be able to restart the server later. */
    // 存储服务的执行命令及参数，以便后续能够restart服务
    server.executable = getAbsolutePath(argv[0]);
    server.exec_argv = zmalloc(sizeof(char*)*(argc+1));
    server.exec_argv[argc] = NULL;
    for (j = 0; j < argc; j++) server.exec_argv[j] = zstrdup(argv[j]);

    /* We need to init sentinel right now as parsing the configuration file
     * in sentinel mode will have the effect of populating the sentinel
     * data structures with master nodes to monitor. */
    // 如果是哨兵模式，这里我们要做哨兵初始化处理了。
    // 因为哨兵模式下解析配置文件会生成哨兵数据结构用于监控master节点。
    if (server.sentinel_mode) {
        // 初始化哨兵配置，主要使用哨兵指定的默认配置来覆盖redis正常服务配置。如端口、保护模式配置等。
        initSentinelConfig();
        // 哨兵模式初始化
        initSentinel();
    }

    /* Check if we need to start in redis-check-rdb/aof mode. We just execute
     * the program main. However the program is part of the Redis executable
     * so that we can easily execute an RDB check on loading errors. */
    // 如果是redis-check-rdb/aof模式，这里处理check逻辑。
    if (strstr(argv[0],"redis-check-rdb") != NULL)
        redis_check_rdb_main(argc,argv,NULL);
    else if (strstr(argv[0],"redis-check-aof") != NULL)
        redis_check_aof_main(argc,argv);

    if (argc >= 2) {
        j = 1; /* First option to parse in argv[] */
        sds options = sdsempty();

        /* Handle special options --help and --version */
        // 处理特殊的运行参数
        if (strcmp(argv[1], "-v") == 0 ||
            strcmp(argv[1], "--version") == 0) version();
        if (strcmp(argv[1], "--help") == 0 ||
            strcmp(argv[1], "-h") == 0) usage();
        if (strcmp(argv[1], "--test-memory") == 0) {
            if (argc == 3) {
                memtest(atoi(argv[2]),50);
                exit(0);
            } else {
                fprintf(stderr,"Please specify the amount of memory to test in megabytes.\n");
                fprintf(stderr,"Example: ./redis-server --test-memory 4096\n\n");
                exit(1);
            }
        }
        /* Parse command line options
         * Precedence wise, File, stdin, explicit options -- last config is the one that matters.
         *
         * First argument is the config file name? */
        // 处理命令行参数，文件、标准输入、指定选项优先级依次更高。
        if (argv[1][0] != '-') {
            /* Replace the config file in server.exec_argv with its absolute path. */
            // 如果第一个参数是配置文件，这里使用该文件的绝对路径进行替换。
            server.configfile = getAbsolutePath(argv[1]);
            zfree(server.exec_argv[1]);
            server.exec_argv[1] = zstrdup(server.configfile);
            j = 2; // Skip this arg when parsing options
        }
        while(j < argc) {
            /* Either first or last argument - Should we read config from stdin? */
            if (argv[j][0] == '-' && argv[j][1] == '\0' && (j == 1 || j == argc-1)) {
                config_from_stdin = 1;
            }
            /* All the other options are parsed and conceptually appended to the
             * configuration file. For instance --port 6380 will generate the
             * string "port 6380\n" to be parsed after the actual config file
             * and stdin input are parsed (if they exist). */
            else if (argv[j][0] == '-' && argv[j][1] == '-') {
                /* Option name */
                if (sdslen(options)) options = sdscat(options,"\n");
                options = sdscat(options,argv[j]+2);
                options = sdscat(options," ");
            } else {
                /* Option argument */
                options = sdscatrepr(options,argv[j],strlen(argv[j]));
                options = sdscat(options," ");
            }
            j++;
        }

        // 从多个来源解析服务的配置。
        loadServerConfig(server.configfile, config_from_stdin, options);
        // 如果服务是sentinel模式，Sentinel配置处理。
        // 配置参数都是在loadServerConfig -> loadServerConfigFromString中解析好了的。
        if (server.sentinel_mode) loadSentinelConfigFromQueue();
        sdsfree(options);
    }
    // sentinel模式检查sentinel配置文件是否设置且可写。
    if (server.sentinel_mode) sentinelCheckConfigFile();
    // 判断redis是否使用其他服务管理，是否后台守护进程执行。
    server.supervised = redisIsSupervised(server.supervised_mode);
    int background = server.daemonize && !server.supervised;
    if (background) daemonize();

    serverLog(LL_WARNING, "oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo");
    serverLog(LL_WARNING,
        "Redis version=%s, bits=%d, commit=%s, modified=%d, pid=%d, just started",
            REDIS_VERSION,
            (sizeof(long) == 8) ? 64 : 32,
            redisGitSHA1(),
            strtol(redisGitDirty(),NULL,10) > 0,
            (int)getpid());

    if (argc == 1) {
        serverLog(LL_WARNING, "Warning: no config file specified, using the default config. In order to specify a config file use %s /path/to/redis.conf", argv[0]);
    } else {
        serverLog(LL_WARNING, "Configuration loaded");
    }

    readOOMScoreAdj();
    // 初始化server
    initServer();
    // 创建pid文件
    if (background || server.pidfile) createPidFile();
    // 设置进程名
    if (server.set_proc_title) redisSetProcTitle(NULL);
    // redis ascii log及相关信息打印
    redisAsciiArt();
    // 检查server.tcp_backlog是否超出了系统的backlog（accept队列大小限制）限制。超过了则不能设置，并打印log信息。
    checkTcpBacklogSettings();

    // 非sentinel模式需要的剩余初始化操作
    if (!server.sentinel_mode) {
        /* Things not needed when running in Sentinel mode. */
        serverLog(LL_WARNING,"Server initialized");
    #ifdef __linux__
        linuxMemoryWarnings();
    #if defined (__arm64__)
        int ret;
        if ((ret = linuxMadvFreeForkBugCheck())) {
            if (ret == 1)
                serverLog(LL_WARNING,"WARNING Your kernel has a bug that could lead to data corruption during background save. "
                                     "Please upgrade to the latest stable kernel.");
            else
                serverLog(LL_WARNING, "Failed to test the kernel for a bug that could lead to data corruption during background save. "
                                      "Your system could be affected, please report this error.");
            if (!checkIgnoreWarning("ARM64-COW-BUG")) {
                serverLog(LL_WARNING,"Redis will now exit to prevent data corruption. "
                                     "Note that it is possible to suppress this warning by setting the following config: ignore-warnings ARM64-COW-BUG");
                exit(1);
            }
        }
    #endif /* __arm64__ */
    #endif /* __linux__ */
        // 完善module的初始化，做一些需要在server初始化完后才能处理的操作，如创建module client。
        moduleInitModulesSystemLast();
        // 根据之前config解析的module列表，挨个进行加载。
        moduleLoadFromQueue();
        // ACL系统加载users。从配置或文件加载处理。
        ACLLoadUsersAtStartup();
        // 做一些服务最后的初始化操作，如初始化后台io线程，初始化读写io线程，初始化jemalloc后台线程。最后统计初始化后服务内存使用。
        InitServerLast();
        // 服务初始化完成后，需要从磁盘加载db数据。
        loadDataFromDisk();
        // 如果开启集群模式，需要检查集群加载的数据与集群配置是否一致
        if (server.cluster_enabled) {
            if (verifyClusterConfigWithData() == C_ERR) {
                serverLog(LL_WARNING,
                    "You can't have keys in a DB different than DB 0 when in "
                    "Cluster mode. Exiting.");
                exit(1);
            }
        }
        // 有socket监听端口，打印日志，服务启动了。
        if (server.ipfd.count > 0 || server.tlsfd.count > 0)
            serverLog(LL_NOTICE,"Ready to accept connections");
        if (server.sofd > 0)
            serverLog(LL_NOTICE,"The server is now ready to accept connections at %s", server.unixsocket);
        // SYSTEMD管理模式处理
        if (server.supervised_mode == SUPERVISED_SYSTEMD) {
            if (!server.masterhost) {
                redisCommunicateSystemd("STATUS=Ready to accept connections\n");
            } else {
                redisCommunicateSystemd("STATUS=Ready to accept connections in read-only mode. Waiting for MASTER <-> REPLICA sync\n");
            }
            redisCommunicateSystemd("READY=1\n");
        }
    } else {
        // sentinel模式处理。ACL、server初始化都跟正常是一样的。
        ACLLoadUsersAtStartup();
        InitServerLast();
        // sentinel启动、加载配置，等待正常的操作处理。
        sentinelIsRunning();
        if (server.supervised_mode == SUPERVISED_SYSTEMD) {
            redisCommunicateSystemd("STATUS=Ready to accept connections\n");
            redisCommunicateSystemd("READY=1\n");
        }
    }

    /* Warning the user about suspicious maxmemory setting. */
    // 提示内存限制设置太小
    if (server.maxmemory > 0 && server.maxmemory < 1024*1024) {
        serverLog(LL_WARNING,"WARNING: You specified a maxmemory value that is less than 1MB (current value is %llu bytes). Are you sure this is what you really want?", server.maxmemory);
    }

    // 设置CPU亲和性
    redisSetCpuAffinity(server.server_cpulist);
    setOOMScoreAdj(-1);

    // 启动事件处理主循环
    aeMain(server.el);
    // 事件循环退出，清除event loop结构
    aeDeleteEventLoop(server.el);
    return 0;
}

/* The End */
