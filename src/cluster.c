/* Redis Cluster implementation.
 *
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
#include "cluster.h"
#include "endianconv.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <math.h>

/* A global reference to myself is handy to make code more clear.
 * Myself always points to server.cluster->myself, that is, the clusterNode
 * that represents this node. */
clusterNode *myself = NULL;

clusterNode *createClusterNode(char *nodename, int flags);
void clusterAddNode(clusterNode *node);
void clusterAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void clusterReadHandler(connection *conn);
void clusterSendPing(clusterLink *link, int type);
void clusterSendFail(char *nodename);
void clusterSendFailoverAuthIfNeeded(clusterNode *node, clusterMsg *request);
void clusterUpdateState(void);
int clusterNodeGetSlotBit(clusterNode *n, int slot);
sds clusterGenNodesDescription(int filter, int use_pport);
clusterNode *clusterLookupNode(const char *name);
int clusterNodeAddSlave(clusterNode *master, clusterNode *slave);
int clusterAddSlot(clusterNode *n, int slot);
int clusterDelSlot(int slot);
int clusterDelNodeSlots(clusterNode *node);
int clusterNodeSetSlotBit(clusterNode *n, int slot);
void clusterSetMaster(clusterNode *n);
void clusterHandleSlaveFailover(void);
void clusterHandleSlaveMigration(int max_slaves);
int bitmapTestBit(unsigned char *bitmap, int pos);
void clusterDoBeforeSleep(int flags);
void clusterSendUpdate(clusterLink *link, clusterNode *node);
void resetManualFailover(void);
void clusterCloseAllSlots(void);
void clusterSetNodeAsMaster(clusterNode *n);
void clusterDelNode(clusterNode *delnode);
sds representClusterNodeFlags(sds ci, uint16_t flags);
uint64_t clusterGetMaxEpoch(void);
int clusterBumpConfigEpochWithoutConsensus(void);
void moduleCallClusterReceivers(const char *sender_id, uint64_t module_id, uint8_t type, const unsigned char *payload, uint32_t len);

#define RCVBUF_INIT_LEN 1024
#define RCVBUF_MAX_PREALLOC (1<<20) /* 1MB */

/* -----------------------------------------------------------------------------
 * Initialization
 * -------------------------------------------------------------------------- */

/* Load the cluster config from 'filename'.
 *
 * If the file does not exist or is zero-length (this may happen because
 * when we lock the nodes.conf file, we create a zero-length one for the
 * sake of locking if it does not already exist), C_ERR is returned.
 * If the configuration was loaded from the file, C_OK is returned. */
int clusterLoadConfig(char *filename) {
    FILE *fp = fopen(filename,"r");
    struct stat sb;
    char *line;
    int maxline, j;

    if (fp == NULL) {
        if (errno == ENOENT) {
            return C_ERR;
        } else {
            serverLog(LL_WARNING,
                "Loading the cluster node config from %s: %s",
                filename, strerror(errno));
            exit(1);
        }
    }

    /* Check if the file is zero-length: if so return C_ERR to signal
     * we have to write the config. */
    if (fstat(fileno(fp),&sb) != -1 && sb.st_size == 0) {
        fclose(fp);
        return C_ERR;
    }

    /* Parse the file. Note that single lines of the cluster config file can
     * be really long as they include all the hash slots of the node.
     * This means in the worst possible case, half of the Redis slots will be
     * present in a single line, possibly in importing or migrating state, so
     * together with the node ID of the sender/receiver.
     *
     * To simplify we allocate 1024+CLUSTER_SLOTS*128 bytes per line. */
    maxline = 1024+CLUSTER_SLOTS*128;
    line = zmalloc(maxline);
    while(fgets(line,maxline,fp) != NULL) {
        int argc;
        sds *argv;
        clusterNode *n, *master;
        char *p, *s;

        /* Skip blank lines, they can be created either by users manually
         * editing nodes.conf or by the config writing process if stopped
         * before the truncate() call. */
        if (line[0] == '\n' || line[0] == '\0') continue;

        /* Split the line into arguments for processing. */
        argv = sdssplitargs(line,&argc);
        if (argv == NULL) goto fmterr;

        /* Handle the special "vars" line. Don't pretend it is the last
         * line even if it actually is when generated by Redis. */
        if (strcasecmp(argv[0],"vars") == 0) {
            if (!(argc % 2)) goto fmterr;
            for (j = 1; j < argc; j += 2) {
                if (strcasecmp(argv[j],"currentEpoch") == 0) {
                    server.cluster->currentEpoch =
                            strtoull(argv[j+1],NULL,10);
                } else if (strcasecmp(argv[j],"lastVoteEpoch") == 0) {
                    server.cluster->lastVoteEpoch =
                            strtoull(argv[j+1],NULL,10);
                } else {
                    serverLog(LL_WARNING,
                        "Skipping unknown cluster config variable '%s'",
                        argv[j]);
                }
            }
            sdsfreesplitres(argv,argc);
            continue;
        }

        /* Regular config lines have at least eight fields */
        if (argc < 8) {
            sdsfreesplitres(argv,argc);
            goto fmterr;
        }

        /* Create this node if it does not exist */
        n = clusterLookupNode(argv[0]);
        if (!n) {
            n = createClusterNode(argv[0],0);
            clusterAddNode(n);
        }
        /* Address and port */
        if ((p = strrchr(argv[1],':')) == NULL) {
            sdsfreesplitres(argv,argc);
            goto fmterr;
        }
        *p = '\0';
        memcpy(n->ip,argv[1],strlen(argv[1])+1);
        char *port = p+1;
        char *busp = strchr(port,'@');
        if (busp) {
            *busp = '\0';
            busp++;
        }
        n->port = atoi(port);
        /* In older versions of nodes.conf the "@busport" part is missing.
         * In this case we set it to the default offset of 10000 from the
         * base port. */
        n->cport = busp ? atoi(busp) : n->port + CLUSTER_PORT_INCR;

        /* The plaintext port for client in a TLS cluster (n->pport) is not
         * stored in nodes.conf. It is received later over the bus protocol. */

        /* Parse flags */
        p = s = argv[2];
        while(p) {
            p = strchr(s,',');
            if (p) *p = '\0';
            if (!strcasecmp(s,"myself")) {
                serverAssert(server.cluster->myself == NULL);
                myself = server.cluster->myself = n;
                n->flags |= CLUSTER_NODE_MYSELF;
            } else if (!strcasecmp(s,"master")) {
                n->flags |= CLUSTER_NODE_MASTER;
            } else if (!strcasecmp(s,"slave")) {
                n->flags |= CLUSTER_NODE_SLAVE;
            } else if (!strcasecmp(s,"fail?")) {
                n->flags |= CLUSTER_NODE_PFAIL;
            } else if (!strcasecmp(s,"fail")) {
                n->flags |= CLUSTER_NODE_FAIL;
                n->fail_time = mstime();
            } else if (!strcasecmp(s,"handshake")) {
                n->flags |= CLUSTER_NODE_HANDSHAKE;
            } else if (!strcasecmp(s,"noaddr")) {
                n->flags |= CLUSTER_NODE_NOADDR;
            } else if (!strcasecmp(s,"nofailover")) {
                n->flags |= CLUSTER_NODE_NOFAILOVER;
            } else if (!strcasecmp(s,"noflags")) {
                /* nothing to do */
            } else {
                serverPanic("Unknown flag in redis cluster config file");
            }
            if (p) s = p+1;
        }

        /* Get master if any. Set the master and populate master's
         * slave list. */
        if (argv[3][0] != '-') {
            master = clusterLookupNode(argv[3]);
            if (!master) {
                master = createClusterNode(argv[3],0);
                clusterAddNode(master);
            }
            n->slaveof = master;
            clusterNodeAddSlave(master,n);
        }

        /* Set ping sent / pong received timestamps */
        if (atoi(argv[4])) n->ping_sent = mstime();
        if (atoi(argv[5])) n->pong_received = mstime();

        /* Set configEpoch for this node. */
        n->configEpoch = strtoull(argv[6],NULL,10);

        /* Populate hash slots served by this instance. */
        for (j = 8; j < argc; j++) {
            int start, stop;

            if (argv[j][0] == '[') {
                /* Here we handle migrating / importing slots */
                int slot;
                char direction;
                clusterNode *cn;

                p = strchr(argv[j],'-');
                serverAssert(p != NULL);
                *p = '\0';
                direction = p[1]; /* Either '>' or '<' */
                slot = atoi(argv[j]+1);
                if (slot < 0 || slot >= CLUSTER_SLOTS) {
                    sdsfreesplitres(argv,argc);
                    goto fmterr;
                }
                p += 3;
                cn = clusterLookupNode(p);
                if (!cn) {
                    cn = createClusterNode(p,0);
                    clusterAddNode(cn);
                }
                if (direction == '>') {
                    server.cluster->migrating_slots_to[slot] = cn;
                } else {
                    server.cluster->importing_slots_from[slot] = cn;
                }
                continue;
            } else if ((p = strchr(argv[j],'-')) != NULL) {
                *p = '\0';
                start = atoi(argv[j]);
                stop = atoi(p+1);
            } else {
                start = stop = atoi(argv[j]);
            }
            if (start < 0 || start >= CLUSTER_SLOTS ||
                stop < 0 || stop >= CLUSTER_SLOTS)
            {
                sdsfreesplitres(argv,argc);
                goto fmterr;
            }
            while(start <= stop) clusterAddSlot(n, start++);
        }

        sdsfreesplitres(argv,argc);
    }
    /* Config sanity check */
    if (server.cluster->myself == NULL) goto fmterr;

    zfree(line);
    fclose(fp);

    serverLog(LL_NOTICE,"Node configuration loaded, I'm %.40s", myself->name);

    /* Something that should never happen: currentEpoch smaller than
     * the max epoch found in the nodes configuration. However we handle this
     * as some form of protection against manual editing of critical files. */
    if (clusterGetMaxEpoch() > server.cluster->currentEpoch) {
        server.cluster->currentEpoch = clusterGetMaxEpoch();
    }
    return C_OK;

fmterr:
    serverLog(LL_WARNING,
        "Unrecoverable error: corrupted cluster config file.");
    zfree(line);
    if (fp) fclose(fp);
    exit(1);
}

/* Cluster node configuration is exactly the same as CLUSTER NODES output.
 *
 * This function writes the node config and returns 0, on error -1
 * is returned.
 *
 * Note: we need to write the file in an atomic way from the point of view
 * of the POSIX filesystem semantics, so that if the server is stopped
 * or crashes during the write, we'll end with either the old file or the
 * new one. Since we have the full payload to write available we can use
 * a single write to write the whole file. If the pre-existing file was
 * bigger we pad our payload with newlines that are anyway ignored and truncate
 * the file afterward. */
int clusterSaveConfig(int do_fsync) {
    sds ci;
    size_t content_size;
    struct stat sb;
    int fd;

    server.cluster->todo_before_sleep &= ~CLUSTER_TODO_SAVE_CONFIG;

    /* Get the nodes description and concatenate our "vars" directive to
     * save currentEpoch and lastVoteEpoch. */
    ci = clusterGenNodesDescription(CLUSTER_NODE_HANDSHAKE, 0);
    ci = sdscatprintf(ci,"vars currentEpoch %llu lastVoteEpoch %llu\n",
        (unsigned long long) server.cluster->currentEpoch,
        (unsigned long long) server.cluster->lastVoteEpoch);
    content_size = sdslen(ci);

    if ((fd = open(server.cluster_configfile,O_WRONLY|O_CREAT,0644))
        == -1) goto err;

    /* Pad the new payload if the existing file length is greater. */
    if (fstat(fd,&sb) != -1) {
        if (sb.st_size > (off_t)content_size) {
            ci = sdsgrowzero(ci,sb.st_size);
            memset(ci+content_size,'\n',sb.st_size-content_size);
        }
    }
    if (write(fd,ci,sdslen(ci)) != (ssize_t)sdslen(ci)) goto err;
    if (do_fsync) {
        server.cluster->todo_before_sleep &= ~CLUSTER_TODO_FSYNC_CONFIG;
        if (fsync(fd) == -1) goto err;
    }

    /* Truncate the file if needed to remove the final \n padding that
     * is just garbage. */
    if (content_size != sdslen(ci) && ftruncate(fd,content_size) == -1) {
        /* ftruncate() failing is not a critical error. */
    }
    close(fd);
    sdsfree(ci);
    return 0;

err:
    if (fd != -1) close(fd);
    sdsfree(ci);
    return -1;
}

void clusterSaveConfigOrDie(int do_fsync) {
    if (clusterSaveConfig(do_fsync) == -1) {
        serverLog(LL_WARNING,"Fatal: can't update cluster config file.");
        exit(1);
    }
}

/* Lock the cluster config using flock(), and leaks the file descriptor used to
 * acquire the lock so that the file will be locked forever.
 *
 * This works because we always update nodes.conf with a new version
 * in-place, reopening the file, and writing to it in place (later adjusting
 * the length with ftruncate()).
 *
 * On success C_OK is returned, otherwise an error is logged and
 * the function returns C_ERR to signal a lock was not acquired. */
int clusterLockConfig(char *filename) {
/* flock() does not exist on Solaris
 * and a fcntl-based solution won't help, as we constantly re-open that file,
 * which will release _all_ locks anyway
 */
#if !defined(__sun)
    /* To lock it, we need to open the file in a way it is created if
     * it does not exist, otherwise there is a race condition with other
     * processes. */
    int fd = open(filename,O_WRONLY|O_CREAT|O_CLOEXEC,0644);
    if (fd == -1) {
        serverLog(LL_WARNING,
            "Can't open %s in order to acquire a lock: %s",
            filename, strerror(errno));
        return C_ERR;
    }

    if (flock(fd,LOCK_EX|LOCK_NB) == -1) {
        if (errno == EWOULDBLOCK) {
            serverLog(LL_WARNING,
                 "Sorry, the cluster configuration file %s is already used "
                 "by a different Redis Cluster node. Please make sure that "
                 "different nodes use different cluster configuration "
                 "files.", filename);
        } else {
            serverLog(LL_WARNING,
                "Impossible to lock %s: %s", filename, strerror(errno));
        }
        close(fd);
        return C_ERR;
    }
    /* Lock acquired: leak the 'fd' by not closing it, so that we'll retain the
     * lock to the file as long as the process exists.
     *
     * After fork, the child process will get the fd opened by the parent process,
     * we need save `fd` to `cluster_config_file_lock_fd`, so that in redisFork(),
     * it will be closed in the child process.
     * If it is not closed, when the main process is killed -9, but the child process
     * (redis-aof-rewrite) is still alive, the fd(lock) will still be held by the
     * child process, and the main process will fail to get lock, means fail to start. */
    server.cluster_config_file_lock_fd = fd;
#else
    UNUSED(filename);
#endif /* __sun */

    return C_OK;
}

/* Derives our ports to be announced in the cluster bus. */
// 生成我们对外宣布的集群间通信的端口信息。
void deriveAnnouncedPorts(int *announced_port, int *announced_pport,
                          int *announced_cport) {
    int port = server.tls_cluster ? server.tls_port : server.port;
    /* Default announced ports. */
    *announced_port = port;
    *announced_pport = server.tls_cluster ? server.port : 0;
    *announced_cport = port + CLUSTER_PORT_INCR;
    /* Config overriding announced ports. */
    if (server.tls_cluster && server.cluster_announce_tls_port) {
        *announced_port = server.cluster_announce_tls_port;
        *announced_pport = server.cluster_announce_port;
    } else if (server.cluster_announce_port) {
        *announced_port = server.cluster_announce_port;
    }
    if (server.cluster_announce_bus_port) {
        *announced_cport = server.cluster_announce_bus_port;
    }
}

/* Some flags (currently just the NOFAILOVER flag) may need to be updated
 * in the "myself" node based on the current configuration of the node,
 * that may change at runtime via CONFIG SET. This function changes the
 * set of flags in myself->flags accordingly. */
// 我们可能通过CONFIG SET来在运行中改变当前节点的配置。
// 所以基于当前最新配置的一些flags（目前只有NOFAILOVER标识）变更需要被更新进"myself"节点中。
// 这个函数主要就是根据新配置去修改myself->flags中的标识集。
void clusterUpdateMyselfFlags(void) {
    int oldflags = myself->flags;
    int nofailover = server.cluster_slave_no_failover ?
                     CLUSTER_NODE_NOFAILOVER : 0;
    // 先把原标识位置空归0，然后设置最新的标识（不管0还是1）。
    myself->flags &= ~CLUSTER_NODE_NOFAILOVER;
    myself->flags |= nofailover;
    if (myself->flags != oldflags) {
        //如果myself->flags有变动，在事件循环sleep之前需要保存集群配置并更新集群状态。
        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                             CLUSTER_TODO_UPDATE_STATE);
    }
}

void clusterInit(void) {
    int saveconf = 0;

    // 初始化集群相关状态信息
    server.cluster = zmalloc(sizeof(clusterState));
    server.cluster->myself = NULL;
    server.cluster->currentEpoch = 0;
    server.cluster->state = CLUSTER_FAIL;
    server.cluster->size = 1;
    server.cluster->todo_before_sleep = 0;
    server.cluster->nodes = dictCreate(&clusterNodesDictType,NULL);
    server.cluster->nodes_black_list =
        dictCreate(&clusterNodesBlackListDictType,NULL);
    server.cluster->failover_auth_time = 0;
    server.cluster->failover_auth_count = 0;
    server.cluster->failover_auth_rank = 0;
    server.cluster->failover_auth_epoch = 0;
    server.cluster->cant_failover_reason = CLUSTER_CANT_FAILOVER_NONE;
    server.cluster->lastVoteEpoch = 0;
    for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
        server.cluster->stats_bus_messages_sent[i] = 0;
        server.cluster->stats_bus_messages_received[i] = 0;
    }
    server.cluster->stats_pfail_nodes = 0;
    memset(server.cluster->slots,0, sizeof(server.cluster->slots));
    // 清除节点当前slot迁移相关信息，不再继续迁移
    clusterCloseAllSlots();

    /* Lock the cluster config file to make sure every node uses
     * its own nodes.conf. */
    server.cluster_config_file_lock_fd = -1;
    // 集群配置处理
    if (clusterLockConfig(server.cluster_configfile) == C_ERR)
        exit(1);

    /* Load or create a new nodes configuration. */
    if (clusterLoadConfig(server.cluster_configfile) == C_ERR) {
        /* No configuration found. We will just use the random name provided
         * by the createClusterNode() function. */
        // 使用默认配置创建集群节点，默认节点是master，默认带有的标识是mysql、master。
        myself = server.cluster->myself =
            createClusterNode(NULL,CLUSTER_NODE_MYSELF|CLUSTER_NODE_MASTER);
        serverLog(LL_NOTICE,"No cluster configuration found, I'm %.40s",
            myself->name);
        // 集群加入当前节点
        clusterAddNode(myself);
        saveconf = 1;
    }
    if (saveconf) clusterSaveConfigOrDie(1);

    /* We need a listening TCP port for our cluster messaging needs. */
    // 监听TCP连接，构造file event加入事件监听列表
    server.cfd.count = 0;

    /* Port sanity check II
     * The other handshake port check is triggered too late to stop
     * us from trying to use a too-high cluster port number. */
    // 因为默认使用正常port+10000来作为集群通信端口，所以这里我们设置的port不能超过65535-10000，否则cport会超出范围。
    int port = server.tls_cluster ? server.tls_port : server.port;
    if (port > (65535-CLUSTER_PORT_INCR)) {
        serverLog(LL_WARNING, "Redis port number too high. "
                   "Cluster communication port is 10,000 port "
                   "numbers higher than your Redis port. "
                   "Your Redis port number must be 55535 or less.");
        exit(1);
    }
    // 创建集群通信socket，绑定到地址，并进行listen。这里注意socket是非阻塞的，所有后续对于该socket的调用都是立即返回。
    if (listenToPort(port+CLUSTER_PORT_INCR, &server.cfd) == C_ERR) {
        exit(1);
    }
    // 创建事件监听，有集群连接进来时调用clusterAcceptHandler进行处理。
    if (createSocketAcceptHandler(&server.cfd, clusterAcceptHandler) != C_OK) {
        serverPanic("Unrecoverable error creating Redis Cluster socket accept handler.");
    }

    /* The slots -> keys map is a radix tree. Initialize it here. */
    // 初始化slots -> keys map，基数树结构存储
    server.cluster->slots_to_keys = raxNew();
    memset(server.cluster->slots_keys_count,0,
           sizeof(server.cluster->slots_keys_count));

    /* Set myself->port/cport/pport to my listening ports, we'll just need to
     * discover the IP address via MEET messages. */
    // 设置当前集群节点Announced的clients port和cluster port，ip信息通过MEET消息来自动发现
    deriveAnnouncedPorts(&myself->port, &myself->pport, &myself->cport);

    // 初始化集群的手动故障转移状态
    server.cluster->mf_end = 0;
    resetManualFailover();
    clusterUpdateMyselfFlags();
}

/* Reset a node performing a soft or hard reset:
 *
 * 1) All other nodes are forgotten.
 * 2) All the assigned / open slots are released.
 * 3) If the node is a slave, it turns into a master.
 * 4) Only for hard reset: a new Node ID is generated.
 * 5) Only for hard reset: currentEpoch and configEpoch are set to 0.
 * 6) The new configuration is saved and the cluster state updated.
 * 7) If the node was a slave, the whole data set is flushed away. */
// soft/hard方式重置当前集群节点。
// 1、Forget当前所有的已知节点。
// 2、释放掉所有已指定、开启状态的slots。
// 3、如果节点是slave，则将该节点转为master。
// 4、hard模式专属：生成一个新的Node ID。
// 5、hard模式专属：节点的配置纪元和集群当前纪元都置为0。
// 6、保存新的配置，并更新集群状态。
// 7、如果节点是slave，所有的数据将被清空。
void clusterReset(int hard) {
    dictIterator *di;
    dictEntry *de;
    int j;

    /* Turn into master. */
    // 如果当前节点是slave，则将之转为master。
    if (nodeIsSlave(myself)) {
        // 设置当前节点为master
        clusterSetNodeAsMaster(myself);
        // 处理复制信息脱离master
        replicationUnsetMaster();
        // 清空整个DB
        emptyDb(-1,EMPTYDB_NO_FLAGS,NULL);
    }

    /* Close slots, reset manual failover state. */
    // 关闭所有slots，即清除所有slots的importing/migrating状态。
    clusterCloseAllSlots();
    // 重置手动故障转移状态。
    resetManualFailover();

    /* Unassign all the slots. */
    // 移除所有slots的指定状态，都变为未分配状态。
    for (j = 0; j < CLUSTER_SLOTS; j++) clusterDelSlot(j);

    /* Forget all the nodes, but myself. */
    // Forget除自己外的所有节点。
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        // 遍历集群nodes，只要不是自己，调用clusterDelNode移除节点。
        if (node == myself) continue;
        clusterDelNode(node);
    }
    dictReleaseIterator(di);

    /* Hard reset only: set epochs to 0, change node ID. */
    // Hard模式需要做的额外操作：设置配置纪元和集群当前纪元为0，重新生成当前节点的ID。
    if (hard) {
        sds oldname;

        // 重置纪元
        server.cluster->currentEpoch = 0;
        server.cluster->lastVoteEpoch = 0;
        myself->configEpoch = 0;
        serverLog(LL_WARNING, "configEpoch set to 0 via CLUSTER RESET HARD");

        /* To change the Node ID we need to remove the old name from the
         * nodes table, change the ID, and re-add back with new name. */
        // 修改当前节点ID（name）。我们需要从节点nodes表中，先将老name的节点删除；然后设置新name，再将新name该节点加入nodes列表。
        oldname = sdsnewlen(myself->name, CLUSTER_NAMELEN);
        dictDelete(server.cluster->nodes,oldname);
        sdsfree(oldname);
        getRandomHexChars(myself->name, CLUSTER_NAMELEN);
        clusterAddNode(myself);
        serverLog(LL_NOTICE,"Node hard reset, now I'm %.40s", myself->name);
    }

    /* Make sure to persist the new config and update the state. */
    // 确保进行持久化新的配置，并更新集群状态。
    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                         CLUSTER_TODO_UPDATE_STATE|
                         CLUSTER_TODO_FSYNC_CONFIG);
}

/* -----------------------------------------------------------------------------
 * CLUSTER communication link
 * -------------------------------------------------------------------------- */
// 为指定节点创建一个新的link，link->node指向传入的节点。
clusterLink *createClusterLink(clusterNode *node) {
    clusterLink *link = zmalloc(sizeof(*link));
    link->ctime = mstime();
    link->sndbuf = sdsempty();
    link->rcvbuf = zmalloc(link->rcvbuf_alloc = RCVBUF_INIT_LEN);
    link->rcvbuf_len = 0;
    link->node = node;
    link->conn = NULL;
    return link;
}

/* Free a cluster link, but does not free the associated node of course.
 * This function will just make sure that the original node associated
 * with this link will have the 'link' field set to NULL. */
// 释放一个cluster link，但不会释放节点，会将关联节点的link属性置NULL
void freeClusterLink(clusterLink *link) {
    // 释放link的conn
    if (link->conn) {
        connClose(link->conn);
        link->conn = NULL;
    }
    // 释放发送和接收缓冲区
    sdsfree(link->sndbuf);
    zfree(link->rcvbuf);
    // link关联的节点的link属性设为NULL
    if (link->node)
        link->node->link = NULL;
    // 释放link
    zfree(link);
}

// 处理集群连接，目前主要是创建集群link，并监听事件。
static void clusterConnAcceptHandler(connection *conn) {
    clusterLink *link;

    // 这里conn应该是CONNECTED状态。
    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_VERBOSE,
                "Error accepting cluster node connection: %s", connGetLastError(conn));
        connClose(conn);
        return;
    }

    /* Create a link object we use to handle the connection.
     * It gets passed to the readable handler when data is available.
     * Initially the link->node pointer is set to NULL as we don't know
     * which node is, but the right node is references once we know the
     * node identity. */
    // 创建一个link结构来处理集群连接。
    // 初始时link->node为空，因为我们还没有为对端创建节点，等到我们收到一些message，确定对端节点时，link->node就会指向该节点。
    link = createClusterLink(NULL);
    link->conn = conn;
    connSetPrivateData(conn, link);

    /* Register read handler */
    // 目前link->conn注册可读事件，当有数据可读时会调用clusterReadHandler来进行处理。
    connSetReadHandler(conn, clusterReadHandler);
}

#define MAX_CLUSTER_ACCEPTS_PER_CALL 1000
// 当有连接进来时，触发可读事件会调用这个方法来处理请求。该handler是逻辑层面整体的Accept处理函数，
// 这函数里面会调用底层accept进行连接建立，然后构建conn连接，最后基于conn建立集群业务连接（clusterConnAcceptHandler）。
void clusterAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd;
    int max = MAX_CLUSTER_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    /* If the server is starting up, don't accept cluster connections:
     * UPDATE messages may interact with the database content. */
    // 如果当前服务正在启动，加载数据期间不接受集群连接请求。因为UPDATE相关的消息可能会改变数据中内容。
    if (server.masterhost == NULL && server.loading) return;

    // 一次调用可处理1000个连接。
    // 这对集群模式很重要，因为当一个节点重新加入集群时(重启或恢复)，它会被所有其他节点尝试连接。
    while(max--) {
        // 调用底层的accept建立连接，获取对端ip和port信息，并创建新的fd用于与对端进行通信。
        // 如果处理完了没有新连接时，会返回EWOULDBLOCK错误，从而直接返回，不再进行循环。
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_VERBOSE,
                    "Error accepting cluster node: %s", server.neterr);
            return;
        }

        // 利用accept返回的新的fd，创建conn（tls或socket）。
        connection *conn = server.tls_cluster ?
            connCreateAcceptedTLS(cfd, TLS_CLIENT_AUTH_YES) : connCreateAcceptedSocket(cfd);

        /* Make sure connection is not in an error state */
        // 确保conn是正确的状态。目前执行到这里，conn一定是ACCEPTING，正在建立连接。
        if (connGetState(conn) != CONN_STATE_ACCEPTING) {
            serverLog(LL_VERBOSE,
                "Error creating an accepting connection for cluster node: %s",
                    connGetLastError(conn));
            connClose(conn);
            return;
        }
        // 设置conn为非阻塞
        connNonBlock(conn);
        // 禁用Nagle，实时发送，允许小包传输。
        connEnableTcpNoDelay(conn);

        /* Use non-blocking I/O for cluster messages. */
        serverLog(LL_VERBOSE,"Accepting cluster node connection from %s:%d", cip, cport);

        /* Accept the connection now.  connAccept() may call our handler directly
         * or schedule it for later depending on connection implementation.
         */
        // connAccept()会变更conn状态为CONN_STATE_CONNECTED，从而完成conn连接。
        // 然后还会调用（直接或调度调用取决于conn实现）clusterConnAcceptHandler来实现集群业务连接。
        if (connAccept(conn, clusterConnAcceptHandler) == C_ERR) {
            if (connGetState(conn) == CONN_STATE_ERROR)
                serverLog(LL_VERBOSE,
                        "Error accepting cluster node connection: %s",
                        connGetLastError(conn));
            connClose(conn);
            return;
        }
    }
}

/* Return the approximated number of sockets we are using in order to
 * take the cluster bus connections. */
unsigned long getClusterConnectionsCount(void) {
    /* We decrement the number of nodes by one, since there is the
     * "myself" node too in the list. Each node uses two file descriptors,
     * one incoming and one outgoing, thus the multiplication by 2. */
    return server.cluster_enabled ?
           ((dictSize(server.cluster->nodes)-1)*2) : 0;
}

/* -----------------------------------------------------------------------------
 * Key space handling
 * -------------------------------------------------------------------------- */

/* We have 16384 hash slots. The hash slot of a given key is obtained
 * as the least significant 14 bits of the crc16 of the key.
 *
 * However if the key contains the {...} pattern, only the part between
 * { and } is hashed. This may be useful in the future to force certain
 * keys to be in the same node (assuming no resharding is in progress). */
// 我们有16384个hash slotslot。给定key的hash slot是通过crc16的低14位获得的。
// 如果key包含{...}模式，则将使用第一个大括号包含的部分来进行hash处理。
// 这种tag设计，可以用于将某一类业务key hash到相同的节点上。
unsigned int keyHashSlot(char *key, int keylen) {
    int s, e; /* start-end indexes of { and } */

    // 先找左括号'{'
    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    // 如果没有左括号'{'，则hash整个key。
    if (s == keylen) return crc16(key,keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    // 执行到这里左括号肯定找到了，这里找对应的右括号。
    for (e = s+1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing between {} ? Hash the whole key. */
    // 没找到右括号，hash整个key
    if (e == keylen || e == s+1) return crc16(key,keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
     * what is in the middle between { and }. */
    // 有左右括号，使用括号里的数据进行hash处理。
    return crc16(key+s+1,e-s-1) & 0x3FFF;
}

/* -----------------------------------------------------------------------------
 * CLUSTER node API
 * -------------------------------------------------------------------------- */

/* Create a new cluster node, with the specified flags.
 * If "nodename" is NULL this is considered a first handshake and a random
 * node name is assigned to this node (it will be fixed later when we'll
 * receive the first pong).
 *
 * The node is created and returned to the user, but it is not automatically
 * added to the nodes hash table. */
// 使用指定的flags创建一个新的集群节点。
// 如果nodename为NULL，则被认为是首次握手，默认会指定一个随机字符串作为name，后面握手会根据收到的第一个PONG消息来修复。
// 该节点被创建返回给用户，但它并不自动加入到集群节点hash表中。
clusterNode *createClusterNode(char *nodename, int flags) {
    clusterNode *node = zmalloc(sizeof(*node));

    if (nodename)
        memcpy(node->name, nodename, CLUSTER_NAMELEN);
    else
        // 不指定name，随机生成
        getRandomHexChars(node->name, CLUSTER_NAMELEN);
    node->ctime = mstime();
    node->configEpoch = 0;
    node->flags = flags;
    memset(node->slots,0,sizeof(node->slots));
    node->slots_info = NULL;
    node->numslots = 0;
    node->numslaves = 0;
    node->slaves = NULL;
    node->slaveof = NULL;
    node->ping_sent = node->pong_received = 0;
    node->data_received = 0;
    node->fail_time = 0;
    node->link = NULL;
    memset(node->ip,0,sizeof(node->ip));
    node->port = 0;
    node->cport = 0;
    node->pport = 0;
    node->fail_reports = listCreate();
    node->voted_time = 0;
    node->orphaned_time = 0;
    node->repl_offset_time = 0;
    node->repl_offset = 0;
    listSetFreeMethod(node->fail_reports,zfree);
    return node;
}

/* This function is called every time we get a failure report from a node.
 * The side effect is to populate the fail_reports list (or to update
 * the timestamp of an existing report).
 *
 * 'failing' is the node that is in failure state according to the
 * 'sender' node.
 *
 * The function returns 0 if it just updates a timestamp of an existing
 * failure report from the same sender. 1 is returned if a new failure
 * report is created. */
// 每次我们收到一个节点发送来的故障报告，都会调用这个方法。副作用是填充加入fail_reports列表，或者更新已存在的report的时间戳为当前时间。
// sender是发送方节点，failing是被指定节点，sender认为failing节点处于PFAIL或FAIL状态。
// 如果是已存在sender对failing节点的故障报告，则只更新时间戳，返回0。否则不存在需要创建加入列表，返回1。
int clusterNodeAddFailureReport(clusterNode *failing, clusterNode *sender) {
    // 取到failing节点的故障报告列表
    list *l = failing->fail_reports;
    listNode *ln;
    listIter li;
    clusterNodeFailReport *fr;

    /* If a failure report from the same sender already exists, just update
     * the timestamp. */
    // 遍历failing节点的故障报告列表，如果sender节点已存在，则更新时间戳，返回0。
    listRewind(l,&li);
    while ((ln = listNext(&li)) != NULL) {
        fr = ln->value;
        if (fr->node == sender) {
            fr->time = mstime();
            return 0;
        }
    }

    /* Otherwise create a new report. */
    // failing节点的故障报告列表中没有sender节点，则创建新的节点加入。返回1。
    fr = zmalloc(sizeof(*fr));
    fr->node = sender;
    fr->time = mstime();
    listAddNodeTail(l,fr);
    return 1;
}

/* Remove failure reports that are too old, where too old means reasonably
 * older than the global node timeout. Note that anyway for a node to be
 * flagged as FAIL we need to have a local PFAIL state that is at least
 * older than the global node timeout, so we don't just trust the number
 * of failure reports from other nodes. */
// 对于给定节点，删除太老的对该节点的故障报告，太老意味着报告时间到现在的时间间隔大于全局节点超时时间。
// 注意任何时候要将一个节点标记为FAIL状态，我们需要先有一个早于全局节点超时时间的本地PFAIL状态。
// 所以我们不仅仅只是相信其他节点对该节点进行故障报告总数。
void clusterNodeCleanupFailureReports(clusterNode *node) {
    list *l = node->fail_reports;
    listNode *ln;
    listIter li;
    clusterNodeFailReport *fr;
    // 这里设置最大超时时间为2倍的节点超时时间。
    mstime_t maxtime = server.cluster_node_timeout *
                     CLUSTER_FAIL_REPORT_VALIDITY_MULT;
    mstime_t now = mstime();

    listRewind(l,&li);
    // 遍历节点的fail_reports，对于太老的报告，移除掉。
    while ((ln = listNext(&li)) != NULL) {
        fr = ln->value;
        if (now - fr->time > maxtime) listDelNode(l,ln);
    }
}

/* Remove the failing report for 'node' if it was previously considered
 * failing by 'sender'. This function is called when a node informs us via
 * gossip that a node is OK from its point of view (no FAIL or PFAIL flags).
 *
 * Note that this function is called relatively often as it gets called even
 * when there are no nodes failing, and is O(N), however when the cluster is
 * fine the failure reports list is empty so the function runs in constant
 * time.
 *
 * The function returns 1 if the failure report was found and removed.
 * Otherwise 0 is returned. */
// 当某节点使用gossip通知我们在它的视角'node'是ok（没有FAIL或PFAIL标识）时，调用这个函数来处理，移除'sender'对'node'之前的故障报告。
// 注意即使没有节点发送故障，这个函数也会很频繁的被调用，并且是O(n)的复杂度。
// 但是当集群正常的时候fail_reports列表是空的，所以该函数是常数时间运行。
// 如果有找到想要的故障报告并删除了，则函数返回1；其他情况（没找到）返回0。
int clusterNodeDelFailureReport(clusterNode *node, clusterNode *sender) {
    list *l = node->fail_reports;
    listNode *ln;
    listIter li;
    clusterNodeFailReport *fr;

    /* Search for a failure report from this sender. */
    // 遍历集群中每个节点的故障报告列表，查看报告发送方是否是指定的sender节点，是则跳出遍历，后面删除掉该报告。
    listRewind(l,&li);
    while ((ln = listNext(&li)) != NULL) {
        fr = ln->value;
        if (fr->node == sender) break;
    }
    // 没有这个sender发送的故障报告，返回0
    if (!ln) return 0; /* No failure report from this sender. */

    /* Remove the failure report. */
    // 删除这个sender发送的故障报告，返回1
    listDelNode(l,ln);
    // 这里清理当前节点的较老的故障报告数据
    clusterNodeCleanupFailureReports(node);
    return 1;
}

/* Return the number of external nodes that believe 'node' is failing,
 * not including this node, that may have a PFAIL or FAIL state for this
 * node as well. */
// 返回认为该节点FAIL的外部节点的数量，不包括该节点本身，该节点也可能具有PFAIL或FAIL状态的。
int clusterNodeFailureReportsCount(clusterNode *node) {
    // 清除太老的报告。
    clusterNodeCleanupFailureReports(node);
    // 返回FAIL报告的节点数量。
    return listLength(node->fail_reports);
}

// 移除指定master节点的指定slave。
int clusterNodeRemoveSlave(clusterNode *master, clusterNode *slave) {
    int j;

    // 遍历master节点的slaves列表，一一比对
    for (j = 0; j < master->numslaves; j++) {
        if (master->slaves[j] == slave) {
            // 找到了对应的slave，进行处理。
            if ((j+1) < master->numslaves) {
                // 要删除的slave不是最后一个，则将数组后面的元素前移。
                int remaining_slaves = (master->numslaves - j) - 1;
                memmove(master->slaves+j,master->slaves+(j+1),
                        (sizeof(*master->slaves) * remaining_slaves));
            }
            // mas减为0时，mater的slaves数-1，当ster节点就不能migrate to了
            master->numslaves--;
            if (master->numslaves == 0)
                master->flags &= ~CLUSTER_NODE_MIGRATE_TO;
            return C_OK;
        }
    }
    return C_ERR;
}

int clusterNodeAddSlave(clusterNode *master, clusterNode *slave) {
    int j;

    /* If it's already a slave, don't add it again. */
    for (j = 0; j < master->numslaves; j++)
        if (master->slaves[j] == slave) return C_ERR;
    master->slaves = zrealloc(master->slaves,
        sizeof(clusterNode*)*(master->numslaves+1));
    master->slaves[master->numslaves] = slave;
    master->numslaves++;
    master->flags |= CLUSTER_NODE_MIGRATE_TO;
    return C_OK;
}

// 获取指定节点的非故障的slave数量
int clusterCountNonFailingSlaves(clusterNode *n) {
    int j, okslaves = 0;

    for (j = 0; j < n->numslaves; j++)
        if (!nodeFailed(n->slaves[j])) okslaves++;
    return okslaves;
}

/* Low level cleanup of the node structure. Only called by clusterDelNode(). */
// 对于集群节点结构的Low level清理工作，只用于clusterDelNode()调用
void freeClusterNode(clusterNode *n) {
    sds nodename;
    int j;

    /* If the node has associated slaves, we have to set
     * all the slaves->slaveof fields to NULL (unknown). */
    // 如果该节点有关联的slaves，移除所有关联。即对于所有的slave->slaveof设置为NULL。
    for (j = 0; j < n->numslaves; j++)
        n->slaves[j]->slaveof = NULL;

    /* Remove this node from the list of slaves of its master. */
    // 如果当前节点是slave，且n->slaveof（即master）不为NULL，则从节点的master的slaves列表中移除该节点。
    if (nodeIsSlave(n) && n->slaveof) clusterNodeRemoveSlave(n->slaveof,n);

    /* Unlink from the set of nodes. */
    // 从集群的节点集合server.cluster->nodes中移除该节点。
    nodename = sdsnewlen(n->name, CLUSTER_NAMELEN);
    serverAssert(dictDelete(server.cluster->nodes,nodename) == DICT_OK);
    sdsfree(nodename);

    /* Release link and associated data structures. */
    // 释放link以及关联的数据结构。
    if (n->link) freeClusterLink(n->link);
    listRelease(n->fail_reports);
    zfree(n->slaves);
    zfree(n);
}

/* Add a node to the nodes hash table */
// 将一个节点加入到当前节点的server.cluster->nodes中
void clusterAddNode(clusterNode *node) {
    int retval;

    retval = dictAdd(server.cluster->nodes,
            sdsnewlen(node->name,CLUSTER_NAMELEN), node);
    serverAssert(retval == DICT_OK);
}

/* Remove a node from the cluster. The function performs the high level
 * cleanup, calling freeClusterNode() for the low level cleanup.
 * Here we do the following:
 *
 * 1) Mark all the slots handled by it as unassigned.
 * 2) Remove all the failure reports sent by this node and referenced by
 *    other nodes.
 * 3) Free the node with freeClusterNode() that will in turn remove it
 *    from the hash table and from the list of slaves of its master, if
 *    it is a slave node.
 */
// 从集群中删除一个节点。这个函数处理high level清理，会调用freeClusterNode来进行low level清理。
// 这里我们主要做3件事：
// 1、标记所有这个待删除节点负责的slots为未分配状态。
// 2、移除所有这个节点报告的所有其他节点的故障报告信息。
// 3、调用freeClusterNode()来释放一个节点。将从节点hash表中移除该节点，如果节点是slave，还将从master的slaves列表中移除。
void clusterDelNode(clusterNode *delnode) {
    int j;
    dictIterator *di;
    dictEntry *de;

    /* 1) Mark slots as unassigned. */
    // 1) 标记所有负责的slots为未分配状态
    for (j = 0; j < CLUSTER_SLOTS; j++) {
        // 对于从该节点迁移的slots，移除迁入标识。
        if (server.cluster->importing_slots_from[j] == delnode)
            server.cluster->importing_slots_from[j] = NULL;
        // 对于迁出到该节点的slots，移除迁出标识。
        if (server.cluster->migrating_slots_to[j] == delnode)
            server.cluster->migrating_slots_to[j] = NULL;
        // 对于该节点负责的slots，设置为未分配状态。（先清除节点内部slot bitmap标识，在将集群该slot指向的节点置为NULL）
        if (server.cluster->slots[j] == delnode)
            clusterDelSlot(j);
    }

    /* 2) Remove failure reports. */
    // 2) 移除故障报告。遍历集群中的节点处理
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (node == delnode) continue;
        // 对于每个待删除节点之外的节点，查看待删除节点有没有对该节点做故障报告，有则删除。
        clusterNodeDelFailureReport(node,delnode);
    }
    dictReleaseIterator(di);

    /* 3) Free the node, unlinking it from the cluster. */
    // 3) 释放节点，移除于集群的连接
    freeClusterNode(delnode);
}

/* Node lookup by name */
// 根据name从集群nodes中找到对应节点
clusterNode *clusterLookupNode(const char *name) {
    sds s = sdsnewlen(name, CLUSTER_NAMELEN);
    dictEntry *de;

    de = dictFind(server.cluster->nodes,s);
    sdsfree(s);
    if (de == NULL) return NULL;
    return dictGetVal(de);
}

/* This is only used after the handshake. When we connect a given IP/PORT
 * as a result of CLUSTER MEET we don't have the node name yet, so we
 * pick a random one, and will fix it when we receive the PONG request using
 * this function. */
// 该函数仅在握手后调用。当我们通过CLUSTER MEET连接给定的IP/PORT时，没有节点名称，所以随机生成了一个，后面收到PONG消息时在这里修复它。
void clusterRenameNode(clusterNode *node, char *newname) {
    int retval;
    sds s = sdsnewlen(node->name, CLUSTER_NAMELEN);

    serverLog(LL_DEBUG,"Renaming node %.40s into %.40s",
        node->name, newname);
    // 根据老得name移除节点。
    retval = dictDelete(server.cluster->nodes, s);
    sdsfree(s);
    serverAssert(retval == DICT_OK);
    // 更新node name为新值。并重新将该节点加入集群。
    memcpy(node->name, newname, CLUSTER_NAMELEN);
    clusterAddNode(node);
}

/* -----------------------------------------------------------------------------
 * CLUSTER config epoch handling
 * -------------------------------------------------------------------------- */

/* Return the greatest configEpoch found in the cluster, or the current
 * epoch if greater than any node configEpoch. */
// 返回集群 所有节点的配置纪元 和 集群当前纪元 的最大值。
uint64_t clusterGetMaxEpoch(void) {
    uint64_t max = 0;
    dictIterator *di;
    dictEntry *de;

    // 遍历集群nodes，找到所有节点中最大的配置纪元。
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        if (node->configEpoch > max) max = node->configEpoch;
    }
    dictReleaseIterator(di);
    // 对比最大的配置纪元 和 集群的当前纪元，谁大返回谁。
    if (max < server.cluster->currentEpoch) max = server.cluster->currentEpoch;
    return max;
}

/* If this node epoch is zero or is not already the greatest across the
 * cluster (from the POV of the local configuration), this function will:
 *
 * 1) Generate a new config epoch, incrementing the current epoch.
 * 2) Assign the new epoch to this node, WITHOUT any consensus.
 * 3) Persist the configuration on disk before sending packets with the
 *    new configuration.
 *
 * If the new config epoch is generated and assigned, C_OK is returned,
 * otherwise C_ERR is returned (since the node has already the greatest
 * configuration around) and no operation is performed.
 *
 * Important note: this function violates the principle that config epochs
 * should be generated with consensus and should be unique across the cluster.
 * However Redis Cluster uses this auto-generated new config epochs in two
 * cases:
 *
 * 1) When slots are closed after importing. Otherwise resharding would be
 *    too expensive.
 * 2) When CLUSTER FAILOVER is called with options that force a slave to
 *    failover its master even if there is not master majority able to
 *    create a new configuration epoch.
 *
 * Redis Cluster will not explode using this function, even in the case of
 * a collision between this node and another node, generating the same
 * configuration epoch unilaterally, because the config epoch conflict
 * resolution algorithm will eventually move colliding nodes to different
 * config epochs. However using this function may violate the "last failover
 * wins" rule, so should only be used with care. */
// 如果当前节点的纪元是0，或者在本地配置的视角，节点纪元不是集群中的最大纪元，这个函数将执行如下操作：
//  1、集群当前纪元自增，生成一个新配置纪元。
//  2、指定生成的新配置纪元给当前节点，无需达成任何共识。
//  3、在BeforeSleep中，持久化配置到磁盘中。
// 如果新的配置纪元成功生成且指定，返回ok；否则当前节点的配置纪元为最大纪元，不会执行任何操作，返回err。
// 重要说明：这个函数违反了配置纪元应该在达成共识的情况下生成，且全集群唯一的准则。
// 但是redis集群会在如下两种情况使用这个自动生成的新配置纪元：
//  1、当前节点slots importing配置变更完成后，需要设置节点新的配置纪元，从而其他节点会以这个节点slot配置为准，避免进行耗时的resharding。
//  2、当CLUSTER FAILOVER TAKEOVER执行时，不用管大多数master同意，直接设置新的配置纪元，广播消息通知接管master的slots。
// redis使用这个功能不会爆炸，即使当前节点和其他节点有纪元冲突，配置纪元冲突解决算法也会最终会为冲突节点指定不同的配置纪元。
// 但是使用这个方法会违反"last failover wins（最新纪元节点故障转移会获胜？）"，所以应该谨慎使用。
int clusterBumpConfigEpochWithoutConsensus(void) {
    uint64_t maxEpoch = clusterGetMaxEpoch();

    if (myself->configEpoch == 0 ||
        myself->configEpoch != maxEpoch)
    {
        // 如果当前节点的配置纪元为0，或者不是最大纪元，则新建一个最大的纪元作为当前节点的配置纪元。
        server.cluster->currentEpoch++;
        myself->configEpoch = server.cluster->currentEpoch;
        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                             CLUSTER_TODO_FSYNC_CONFIG);
        serverLog(LL_WARNING,
            "New configEpoch set to %llu",
            (unsigned long long) myself->configEpoch);
        return C_OK;
    } else {
        return C_ERR;
    }
}

/* This function is called when this node is a master, and we receive from
 * another master a configuration epoch that is equal to our configuration
 * epoch.
 *
 * BACKGROUND
 *
 * It is not possible that different slaves get the same config
 * epoch during a failover election, because the slaves need to get voted
 * by a majority. However when we perform a manual resharding of the cluster
 * the node will assign a configuration epoch to itself without to ask
 * for agreement. Usually resharding happens when the cluster is working well
 * and is supervised by the sysadmin, however it is possible for a failover
 * to happen exactly while the node we are resharding a slot to assigns itself
 * a new configuration epoch, but before it is able to propagate it.
 *
 * So technically it is possible in this condition that two nodes end with
 * the same configuration epoch.
 *
 * Another possibility is that there are bugs in the implementation causing
 * this to happen.
 *
 * Moreover when a new cluster is created, all the nodes start with the same
 * configEpoch. This collision resolution code allows nodes to automatically
 * end with a different configEpoch at startup automatically.
 *
 * In all the cases, we want a mechanism that resolves this issue automatically
 * as a safeguard. The same configuration epoch for masters serving different
 * set of slots is not harmful, but it is if the nodes end serving the same
 * slots for some reason (manual errors or software bugs) without a proper
 * failover procedure.
 *
 * In general we want a system that eventually always ends with different
 * masters having different configuration epochs whatever happened, since
 * nothing is worse than a split-brain condition in a distributed system.
 *
 * BEHAVIOR
 *
 * When this function gets called, what happens is that if this node
 * has the lexicographically smaller Node ID compared to the other node
 * with the conflicting epoch (the 'sender' node), it will assign itself
 * the greatest configuration epoch currently detected among nodes plus 1.
 *
 * This means that even if there are multiple nodes colliding, the node
 * with the greatest Node ID never moves forward, so eventually all the nodes
 * end with a different configuration epoch.
 */
// 我们是master，当我们收到另外一个master的配置纪元与我们相同时，有冲突，需要调用这个函数来解决。
// 背景：
//  不可能不同的slaves在故障转移选举期间获得相同的配置纪元，因为slaves需要获得大多数的投票。
//  但是当我们对集群进行手动resharding时，节点会在未经同意的情况下为自己分配一个配置纪元。
//  通常集群运行良好，且在管理员监督下处理集群resharding，但当节点正在重分片slot并指给自己新配置纪元时，如果还没传播出去就发生了故障转移。
//  这种情况下，从技术上讲，是有可能两个节点具有相同的配置纪元的。另外一种情况是实现上存在bugs可能导致配置纪元相同。
//  此外，当创建新的集群时，所有节点都以相同的configEpoch开始。这里冲突解决代码允许节点在启动时，自动获得不同的configEpoch。

// 上面所有的情况，我们需要一个自动解决机制作为保护措施。
// 相同配置纪元的master负责不同的slots并没有什么害处，但是当节点因为err导致不再负责这些slots，却没有进行适当的故障转移处理时就会有问题了。
// 一般来说，我们想要系统在无论发生什么时，最终都是不同的主节点具有不同的配置纪元，因为没有什么比分布式系统中的裂脑情况更糟糕的了。

// 当这个函数被调用时，如果这个节点比sender有更小的Node ID，它将指定自己为可探测到的所有节点中最大的配置纪元+1（即cluster->currentEpoch+1）。
// 这意味着，即使有多个节点冲突，他们之中具有最大Node ID的节点将永远不会前移（最坏情况其他冲突节点还是冲突，但总冲突节点数量-1了），
// 所以经过多次迭代处理，最终所有的节点的配置纪元都会不同。
void clusterHandleConfigEpochCollision(clusterNode *sender) {
    /* Prerequisites: nodes have the same configEpoch and are both masters. */
    // 只有两个节点都是master，且配置纪元相等才处理。不是这情况，不需要处理，直接返回。
    if (sender->configEpoch != myself->configEpoch ||
        !nodeIsMaster(sender) || !nodeIsMaster(myself)) return;
    /* Don't act if the colliding node has a smaller Node ID. */
    // 因为我们只能修改自己的配置纪元，所有当sender ID小于我们时，我们可能是冲突里面最大的ID，赞不处理，返回。
    if (memcmp(sender->name,myself->name,CLUSTER_NAMELEN) <= 0) return;
    /* Get the next ID available at the best of this node knowledge. */
    // 获取我们知道的下一个可用的最大纪元，作为当前节点的配置纪元。
    server.cluster->currentEpoch++;
    myself->configEpoch = server.cluster->currentEpoch;
    // 同步更新配置
    clusterSaveConfigOrDie(1);
    serverLog(LL_VERBOSE,
        "WARNING: configEpoch collision with node %.40s."
        " configEpoch set to %llu",
        sender->name,
        (unsigned long long) myself->configEpoch);
}

/* -----------------------------------------------------------------------------
 * CLUSTER nodes blacklist
 *
 * The nodes blacklist is just a way to ensure that a given node with a given
 * Node ID is not readded before some time elapsed (this time is specified
 * in seconds in CLUSTER_BLACKLIST_TTL).
 *
 * This is useful when we want to remove a node from the cluster completely:
 * when CLUSTER FORGET is called, it also puts the node into the blacklist so
 * that even if we receive gossip messages from other nodes that still remember
 * about the node we want to remove, we don't re-add it before some time.
 *
 * Currently the CLUSTER_BLACKLIST_TTL is set to 1 minute, this means
 * that redis-trib has 60 seconds to send CLUSTER FORGET messages to nodes
 * in the cluster without dealing with the problem of other nodes re-adding
 * back the node to nodes we already sent the FORGET command to.
 *
 * The data structure used is a hash table with an sds string representing
 * the node ID as key, and the time when it is ok to re-add the node as
 * value.
 * -------------------------------------------------------------------------- */
// 节点黑名单只是一种方式，确保给定ID的节点在某时间内（CLUSTER_BLACKLIST_TTL指定）不允许被重新加入到集群中。
// 当我们想从集群中完全删除一个节点是，这种方式很有用。当调用CLUSTER FORGET时，会同时将节点加入到黑名单中。
// 这样即使我们收到了其他节点的gossip消息，我们始终记得要移除该节点，所以我们在一段时间内不会将该节点重新加入到集群中。
// 注意如果执行CLUSTER FORGET时，需要向所有节点发送该命令，否则等一分钟过后，如果消息中有该节点，则该节点又会重新加入。
// 当前黑名单TTL设置为1分钟，这意味着redis-trib有60s时间来发送CLUSTER FORGET给集群其他节点，而无需处理forget的节点重新加入集群的问题。
// 黑名单数据结构使用的是hash表，key为sds类型的ID，可以重新加入的时间（即1分钟之后的时间）为value。

#define CLUSTER_BLACKLIST_TTL 60      /* 1 minute. */


/* Before of the addNode() or Exists() operations we always remove expired
 * entries from the black list. This is an O(N) operation but it is not a
 * problem since add / exists operations are called very infrequently and
 * the hash table is supposed to contain very little elements at max.
 * However without the cleanup during long uptime and with some automated
 * node add/removal procedures, entries could accumulate. */
// 在addNode() or Exists()操作之前，我们总是先从black list移除过期的nodeids。
// 移除操作是O(n)，但因为add / exists操作很少，且dict中元素最大也是很少的，所以这里没有问题。
// 但是，如果长时间运行期间没有进行清理，或者使用一些自动添加/删除节点程序，dict元素可能会增加很多。
void clusterBlacklistCleanup(void) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(server.cluster->nodes_black_list);
    while((de = dictNext(di)) != NULL) {
        // 迭代遍历，查看是否达到了过期时间，达到则从nodes_black_list中删除。
        int64_t expire = dictGetUnsignedIntegerVal(de);

        if (expire < server.unixtime)
            dictDelete(server.cluster->nodes_black_list,dictGetKey(de));
    }
    dictReleaseIterator(di);
}

/* Cleanup the blacklist and add a new node ID to the black list. */
// 先调用clusterBlacklistCleanup()，清理blacklist。然后将新的节点ID加入黑名单列表。
void clusterBlacklistAddNode(clusterNode *node) {
    dictEntry *de;
    sds id = sdsnewlen(node->name,CLUSTER_NAMELEN);

    // 清理过期节点ID。
    clusterBlacklistCleanup();
    // 添加新节点ID到黑名单列表
    if (dictAdd(server.cluster->nodes_black_list,id,NULL) == DICT_OK) {
        /* If the key was added, duplicate the sds string representation of
         * the key for the next lookup. We'll free it at the end. */
        // 如果key被成功加入，key的释放会在我们执行dictDelete时，随着删除元素一起释放。而如果key加入失败，我们需要在当前函数中释放id的。
        // 所以这里复制ID字符串用于后面释放。
        id = sdsdup(id);
    }
    // 查询出id对应的entry，设置过期的时间为value。
    de = dictFind(server.cluster->nodes_black_list,id);
    dictSetUnsignedIntegerVal(de,time(NULL)+CLUSTER_BLACKLIST_TTL);
    // 释放id
    sdsfree(id);
}

/* Return non-zero if the specified node ID exists in the blacklist.
 * You don't need to pass an sds string here, any pointer to 40 bytes
 * will work. */
// 如果指定的node ID在黑名单中，则返回非0。
// 这里nodeid并不一样要传sds字符串，任何指向40字节数据的指针都可以。
int clusterBlacklistExists(char *nodeid) {
    // 使用传入的数据构建sds字符串
    sds id = sdsnewlen(nodeid,CLUSTER_NAMELEN);
    int retval;

    // 清除过期的黑名单节点id
    clusterBlacklistCleanup();
    // 查看当前是否有现限制该nodeid节点
    retval = dictFind(server.cluster->nodes_black_list,id) != NULL;
    sdsfree(id);
    return retval;
}

/* -----------------------------------------------------------------------------
 * CLUSTER messages exchange - PING/PONG and gossip
 * -------------------------------------------------------------------------- */

/* This function checks if a given node should be marked as FAIL.
 * It happens if the following conditions are met:
 *
 * 1) We received enough failure reports from other master nodes via gossip.
 *    Enough means that the majority of the masters signaled the node is
 *    down recently.
 * 2) We believe this node is in PFAIL state.
 *
 * If a failure is detected we also inform the whole cluster about this
 * event trying to force every other node to set the FAIL flag for the node.
 *
 * Note that the form of agreement used here is weak, as we collect the majority
 * of masters state during some time, and even if we force agreement by
 * propagating the FAIL message, because of partitions we may not reach every
 * node. However:
 *
 * 1) Either we reach the majority and eventually the FAIL state will propagate
 *    to all the cluster.
 * 2) Or there is no majority so no slave promotion will be authorized and the
 *    FAIL flag will be cleared after some time.
 */
// 这个函数检查给定的节点是否应该标记为FAIL。满足下面条件则为FAIL：
//  1、我们收到足够多的其他master节点通过gossip发送过来的该节点故障的报告。足够意味着最近大多数master通知该节点down了。
//  2、我们相信该节点是PFAIL状态。即我们认为该节点可能下线了，且大多数master也认为该节点下线了，此时我们将该节点标识为FAIL。
// 如果我们这里判断该节点确实FAIL了，我们会通知集群中的所有节点，让他们也将该节点设置为FAIL状态。
// 注意这里所用的协议形式很弱，因为我们需要一段时间来收集大多数的master状态，即使我们传播FAIL消息来强制处理，也会有分区导致不可达情况。
// 所以最终会出现以下结果：
//  1）我们达到多数同意FAIL，并最终FAIL状态传播到集群所有节点。
//  2) 没有多数同意FAIL，所以不会授权salve升级为master，一段时间后将清除FAIL标志。
void markNodeAsFailingIfNeeded(clusterNode *node) {
    int failures;
    // 需要达到一半以上
    int needed_quorum = (server.cluster->size / 2) + 1;

    // 判断该节点是否PFAIL状态，当节点timeout时，我们就认为PFAIL。不是PFAIL，显然不应该标记为FAIL，返回。
    if (!nodeTimedOut(node)) return; /* We can reach it. */
    // 已经是FAIL状态了，返回。
    if (nodeFailed(node)) return; /* Already FAILing. */

    // 检查最近对该节点FAIL报告的外部master节点数量。
    failures = clusterNodeFailureReportsCount(node);
    /* Also count myself as a voter if I'm a master. */
    // 如果我们当前节点是master，这里也加上我们自己的一票。
    // 因为前面如果我们认为该节点不是PFAIL的话，不会执行到这里，所以我们实际上也是认为该节点可能FAIL的。
    if (nodeIsMaster(myself)) failures++;
    // 没达到quorum，不应该FAIL，返回。
    if (failures < needed_quorum) return; /* No weak agreement from masters. */

    serverLog(LL_NOTICE,
        "Marking node %.40s as failing (quorum reached).", node->name);

    /* Mark the node as failing. */
    // 到这里，达到了大多数master同意FAIL。设置FAIL标识。
    node->flags &= ~CLUSTER_NODE_PFAIL;
    node->flags |= CLUSTER_NODE_FAIL;
    node->fail_time = mstime();

    /* Broadcast the failing node name to everybody, forcing all the other
     * reachable nodes to flag the node as FAIL.
     * We do that even if this node is a replica and not a master: anyway
     * the failing state is triggered collecting failure reports from masters,
     * so here the replica is only helping propagating this status. */
    // 向集群所有节点广播该节点FAIL的消息，强制其他可达的节点将该节点标识为FAIL。
    // 即使该节点是slave，我们也这样处理。虽然FAIL状态只会触发master来收集故障报告，但我们可以通过slave来传播该节点FAIL信息。
    clusterSendFail(node->name);
    clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
}

/* This function is called only if a node is marked as FAIL, but we are able
 * to reach it again. It checks if there are the conditions to undo the FAIL
 * state. */
// 这个函数仅在一个节点被标识为FAIL，但后面又接收到该节点的消息时调用。这里会检查是否该节点达到了移除FAIL状态的条件，从而进行处理。
void clearNodeFailureIfNeeded(clusterNode *node) {
    mstime_t now = mstime();

    serverAssert(nodeFailed(node));

    /* For slaves we always clear the FAIL flag if we can contact the
     * node again. */
    // 对于slaves节点或者不负责slot的master节点，如果重新联系上了，则我们总是可以直接清除FAIL标识。
    if (nodeIsSlave(node) || node->numslots == 0) {
        serverLog(LL_NOTICE,
            "Clear FAIL state for node %.40s: %s is reachable again.",
                node->name,
                nodeIsSlave(node) ? "replica" : "master without slots");
        node->flags &= ~CLUSTER_NODE_FAIL;
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
    }

    /* If it is a master and...
     * 1) The FAIL state is old enough.
     * 2) It is yet serving slots from our point of view (not failed over).
     * Apparently no one is going to fix these slots, clear the FAIL flag. */
    // 如果节点是master，当它处于FAIL状态达到一定时间时，在我们视角如果还在负责slots（即没有进行故障转移）。
    // 显然没有其他节点来接管修复这些slats，没办法，我们也只有直接移除FAIL标识了。
    if (nodeIsMaster(node) && node->numslots > 0 &&
        (now - node->fail_time) >
        (server.cluster_node_timeout * CLUSTER_FAIL_UNDO_TIME_MULT))
    {
        serverLog(LL_NOTICE,
            "Clear FAIL state for node %.40s: is reachable again and nobody is serving its slots after some time.",
                node->name);
        node->flags &= ~CLUSTER_NODE_FAIL;
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
    }
}

/* Return true if we already have a node in HANDSHAKE state matching the
 * specified ip address and port number. This function is used in order to
 * avoid adding a new handshake node for the same address multiple times. */
// 如果我们已经有相同的ip和端口的节点正处于HANDSHAKE状态，则返回true。
// 这个函数用于避免对于相同的地址重复多次开启handshake。
int clusterHandshakeInProgress(char *ip, int port, int cport) {
    dictIterator *di;
    dictEntry *de;

    // 遍历当前cluster的所有节点列表。
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        // 如果遍历到的节点不是处于HANDSHAKE状态，跳过
        if (!nodeInHandshake(node)) continue;
        // 对于处于HANDSHAKE状态的节点，对比ip、port、cport，如果都相等，则找到了，跳出循环。
        if (!strcasecmp(node->ip,ip) &&
            node->port == port &&
            node->cport == cport) break;
    }
    dictReleaseIterator(di);
    // 找到了节点，显然de不为NULL，返回true
    return de != NULL;
}

/* Start a handshake with the specified address if there is not one
 * already in progress. Returns non-zero if the handshake was actually
 * started. On error zero is returned and errno is set to one of the
 * following values:
 *
 * EAGAIN - There is already a handshake in progress for this address.
 * EINVAL - IP or port are not valid. */
// 开始一次与指定地址节点的握手操作（如果之前没有的话）。
// 如果成功开启握手，则返回非0；否则返回0并在errno中可能返回如下err：
//      EAGAIN，指定地址已经有一个正在进行中的握手。
//      EINVAL，IP或端口无效
int clusterStartHandshake(char *ip, int port, int cport) {
    clusterNode *n;
    char norm_ip[NET_IP_STR_LEN];
    struct sockaddr_storage sa;

    /* IP sanity check */
    // ip检查，并确认是ipv4还是ipv6
    if (inet_pton(AF_INET,ip,
            &(((struct sockaddr_in *)&sa)->sin_addr)))
    {
        sa.ss_family = AF_INET;
    } else if (inet_pton(AF_INET6,ip,
            &(((struct sockaddr_in6 *)&sa)->sin6_addr)))
    {
        sa.ss_family = AF_INET6;
    } else {
        errno = EINVAL;
        return 0;
    }

    /* Port sanity check */
    // 检查port是否超出范围
    if (port <= 0 || port > 65535 || cport <= 0 || cport > 65535) {
        errno = EINVAL;
        return 0;
    }

    /* Set norm_ip as the normalized string representation of the node
     * IP address. */
    // 设置对方节点的标准化ip字符串
    memset(norm_ip,0,NET_IP_STR_LEN);
    if (sa.ss_family == AF_INET)
        inet_ntop(AF_INET,
            (void*)&(((struct sockaddr_in *)&sa)->sin_addr),
            norm_ip,NET_IP_STR_LEN);
    else
        inet_ntop(AF_INET6,
            (void*)&(((struct sockaddr_in6 *)&sa)->sin6_addr),
            norm_ip,NET_IP_STR_LEN);

    // 检查对应的地址是否处于握手中，只会对处于handshake状态的节点对比ip、port、cport。
    if (clusterHandshakeInProgress(norm_ip,port,cport)) {
        errno = EAGAIN;
        return 0;
    }

    /* Add the node with a random address (NULL as first argument to
     * createClusterNode()). Everything will be fixed during the
     * handshake. */
    // createClusterNode第一个参数为NULL时，表示使用随机的地址来创建一个集群节点。
    // 我们所有的关于集群节点的信息都可以在握手中修复，所以创建集群节点时可能都是默认值。
    n = createClusterNode(NULL,CLUSTER_NODE_HANDSHAKE|CLUSTER_NODE_MEET);
    memcpy(n->ip,norm_ip,sizeof(n->ip));
    n->port = port;
    n->cport = cport;
    // 将该新创建的节点加入当前节点的server.cluster->nodes中
    clusterAddNode(n);
    return 1;
}

/* Process the gossip section of PING or PONG packets.
 * Note that this function assumes that the packet is already sanity-checked
 * by the caller, not in the content of the gossip section, but in the
 * length. */
// 处理PING或PONG消息包的gossip部分。
// 注意这里我们假定消息包已经被调用者做了完整性检查，基于长度检查，而不是gossip内容。
void clusterProcessGossipSection(clusterMsg *hdr, clusterLink *link) {
    // 获取到gossip消息数量，以及数组首指针。
    uint16_t count = ntohs(hdr->count);
    clusterMsgDataGossip *g = (clusterMsgDataGossip*) hdr->data.ping.gossip;
    // 获取sender节点。
    clusterNode *sender = link->node ? link->node : clusterLookupNode(hdr->sender);

    // 挨个解析gossip消息
    while(count--) {
        // 获取传过来描述对应节点的flags
        uint16_t flags = ntohs(g->flags);
        clusterNode *node;
        sds ci;

        // 打印debug日志
        if (server.verbosity == LL_DEBUG) {
            ci = representClusterNodeFlags(sdsempty(), flags);
            serverLog(LL_DEBUG,"GOSSIP %.40s %s:%d@%d %s",
                g->nodename,
                g->ip,
                ntohs(g->port),
                ntohs(g->cport),
                ci);
            sdsfree(ci);
        }

        /* Update our state accordingly to the gossip sections */
        // 通过sender传递过来的gossip信息，更新我们当前所知的集群状态。
        // 根据当前gossip消息里的nodename，在我们cluster nodes中查询该节点。
        node = clusterLookupNode(g->nodename);
        if (node) {
            /* We already know this node.
               Handle failure reports, only when the sender is a master. */
            // 找到了，说明我们知道该节点，处理相关信息的更新。
            // 当sender是master，且指定的该节点不是我们自己时，我们需要根据flags看是否要处理故障报告。
            if (sender && nodeIsMaster(sender) && node != myself) {
                if (flags & (CLUSTER_NODE_FAIL|CLUSTER_NODE_PFAIL)) {
                    // 如果flags标识该节点是FAIL或PFAIL，我们需要将信息加入到集群故障报告列表中，指定sender认为node下线。
                    if (clusterNodeAddFailureReport(node,sender)) {
                        serverLog(LL_VERBOSE,
                            "Node %.40s reported node %.40s as not reachable.",
                            sender->name, node->name);
                    }
                    // 基于当前我们得到的信息，判断node节点是否FAIL。如果是，我们会广播发送FAIL给其他所有节点。
                    markNodeAsFailingIfNeeded(node);
                } else {
                    // 如果收到关于指定的节点没有FAIL或PFAIL标识，则我们需要遍历该节点故障报告列表，如果有的话需要移除sender的报告。
                    if (clusterNodeDelFailureReport(node,sender)) {
                        serverLog(LL_VERBOSE,
                            "Node %.40s reported node %.40s is back online.",
                            sender->name, node->name);
                    }
                }
            }

            /* If from our POV the node is up (no failure flags are set),
             * we have no pending ping for the node, nor we have failure
             * reports for this node, update the last pong time with the
             * one we see from the other nodes. */
            // 如果在我们的视角，该节点是活跃的（没有FAIL/PFAIL标识）。
            // 我们没有正在发送到该节点的PING，也没有收到任何其他节点关于该节点的故障报告，则使用sender关于该节点的PONG时间来更新我们数据。
            if (!(flags & (CLUSTER_NODE_FAIL|CLUSTER_NODE_PFAIL)) &&
                node->ping_sent == 0 &&
                clusterNodeFailureReportsCount(node) == 0)
            {
                // gossip中sender节点收到指定node的PONG时间。
                mstime_t pongtime = ntohl(g->pong_received);
                pongtime *= 1000; /* Convert back to milliseconds. */

                /* Replace the pong time with the received one only if
                 * it's greater than our view but is not in the future
                 * (with 500 milliseconds tolerance) from the POV of our
                 * clock. */
                // 当sender最近收到的PONG时间比我们最近收到PONG的时间要大，且在超出当前节点时钟500ms以内时，
                // 利用sender的收到PONG时间来更新我们的数据。
                if (pongtime <= (server.mstime+500) &&
                    pongtime > node->pong_received)
                {
                    node->pong_received = pongtime;
                }
            }

            /* If we already know this node, but it is not reachable, and
             * we see a different address in the gossip section of a node that
             * can talk with this other node, update the address, disconnect
             * the old link if any, so that we'll attempt to connect with the
             * new address. */
            // 如果我们知道该节点，并且当前视角已经将该节点标识为FAIL或PFAIL（即认为该节点不可达），
            // 但是我们收到gossip消息显示，其他节点能与该节点正常通信，则我们更新该节点的地址，释放link，后面尝试使用新地址重新进行连接。
            if (node->flags & (CLUSTER_NODE_FAIL|CLUSTER_NODE_PFAIL) &&
                !(flags & CLUSTER_NODE_NOADDR) &&
                !(flags & (CLUSTER_NODE_FAIL|CLUSTER_NODE_PFAIL)) &&
                (strcasecmp(node->ip,g->ip) ||
                 node->port != ntohs(g->port) ||
                 node->cport != ntohs(g->cport)))
            {
                // ip或端口发生了变化。释放link，更新地址信息。
                if (node->link) freeClusterLink(node->link);
                memcpy(node->ip,g->ip,NET_IP_STR_LEN);
                node->port = ntohs(g->port);
                node->pport = ntohs(g->pport);
                node->cport = ntohs(g->cport);
                node->flags &= ~CLUSTER_NODE_NOADDR;
            }
        } else {
            /* If it's not in NOADDR state and we don't have it, we
             * add it to our trusted dict with exact nodeid and flag.
             * Note that we cannot simply start a handshake against
             * this IP/PORT pairs, since IP/PORT can be reused already,
             * otherwise we risk joining another cluster.
             *
             * Note that we require that the sender of this gossip message
             * is a well known node in our cluster, otherwise we risk
             * joining another cluster. */
            // 如果该节点不是NOADDR状态，但我们不知道该节点，则需要将它加入到我们集群状态nodes中，使用sender发送过来的name和flags。
            // 注意不能简单就使用sender发来的ip/port开启handshake，因为ip/port可能已经被重新使用，直接连接可能有加入其他集群的风险。
            if (sender &&
                !(flags & CLUSTER_NODE_NOADDR) &&
                !clusterBlacklistExists(g->nodename))
            {
                clusterNode *node;
                // 创建节点加入集群nodes。更新该节点的ip、port，供后面重新连接。
                node = createClusterNode(g->nodename, flags);
                memcpy(node->ip,g->ip,NET_IP_STR_LEN);
                node->port = ntohs(g->port);
                node->pport = ntohs(g->pport);
                node->cport = ntohs(g->cport);
                clusterAddNode(node);
            }
        }

        /* Next node */
        // 处理下一条消息内容
        g++;
    }
}

/* IP -> string conversion. 'buf' is supposed to at least be 46 bytes.
 * If 'announced_ip' length is non-zero, it is used instead of extracting
 * the IP from the socket peer address. */
// IP转换为string，存储到buf中，buf需要至少46字节长度。
// 如果announced_ip长度是非0，则直接使用announced_ip填充buf，否则需要从conn中来获取对端ip。
void nodeIp2String(char *buf, clusterLink *link, char *announced_ip) {
    if (announced_ip[0] != '\0') {
        memcpy(buf,announced_ip,NET_IP_STR_LEN);
        buf[NET_IP_STR_LEN-1] = '\0'; /* We are not sure the input is sane. */
    } else {
        connPeerToString(link->conn, buf, NET_IP_STR_LEN, NULL);
    }
}

/* Update the node address to the IP address that can be extracted
 * from link->fd, or if hdr->myip is non empty, to the address the node
 * is announcing us. The port is taken from the packet header as well.
 *
 * If the address or port changed, disconnect the node link so that we'll
 * connect again to the new address.
 *
 * If the ip/port pair are already correct no operation is performed at
 * all.
 *
 * The function returns 0 if the node address is still the same,
 * otherwise 1 is returned. */
// 更新节点的地址，优先从集群消息hdr中取得ip，如果没有则从link的conn连接中获取对端地址。port相关信息从hdr中获取。
// 如果节点地址和port发生了变化，这里会断开节点link连接，后面cron中会重新使用新的地址建立连接。
// 如果节点ip/port没有变化，都已经是对的了，不会执行任何操作，函数会返回0。否则会返回1。
int nodeUpdateAddressIfNeeded(clusterNode *node, clusterLink *link,
                              clusterMsg *hdr)
{
    char ip[NET_IP_STR_LEN] = {0};
    // 从hdr获取port信息
    int port = ntohs(hdr->port);
    int pport = ntohs(hdr->pport);
    int cport = ntohs(hdr->cport);

    /* We don't proceed if the link is the same as the sender link, as this
     * function is designed to see if the node link is consistent with the
     * symmetric link that is used to receive PINGs from the node.
     *
     * As a side effect this function never frees the passed 'link', so
     * it is safe to call during packet processing. */
    // 如果link与节点link相同，则直接返回0。因为这个函数主要用于判断节点link是否与当前接收PINGs消息的link一致。
    // 副作用是，这个函数永远不会释放传递消息的link，因此在数据包处理期间调用是安全的。
    if (link == node->link) return 0;

    // 从link的conn或者hdr->myip中获取节点的ip
    nodeIp2String(ip,link,hdr->myip);
    // 如果ip和port都没变，直接返回0。
    if (node->port == port && node->cport == cport && node->pport == pport &&
        strcmp(ip,node->ip) == 0) return 0;

    /* IP / port is different, update it. */
    // ip/port变了，更新
    memcpy(node->ip,ip,sizeof(ip));
    node->port = port;
    node->pport = pport;
    node->cport = cport;
    // 释放节点的link
    if (node->link) freeClusterLink(node->link);
    node->flags &= ~CLUSTER_NODE_NOADDR;
    serverLog(LL_WARNING,"Address updated for node %.40s, now %s:%d",
        node->name, node->ip, node->port);

    /* Check if this is our master and we have to change the
     * replication target as well. */
    // 如果当前是slave，则检查该node是否是我们的master节点。如果是，这里需要改变复制目标，关联到新的ip/port节点上。
    if (nodeIsSlave(myself) && myself->slaveof == node)
        replicationSetMaster(node->ip, node->port);
    return 1;
}

/* Reconfigure the specified node 'n' as a master. This function is called when
 * a node that we believed to be a slave is now acting as master in order to
 * update the state of the node. */
// 重新设置指定节点为master。当我们认为之前是slave的节点现在变成了master，就需要调用这个函数来更新节点状态。
void clusterSetNodeAsMaster(clusterNode *n) {
    // 节点本身是master，不需要处理，返回。
    if (nodeIsMaster(n)) return;

    if (n->slaveof) {
        // 如果当前节点之前是某个master的slave，从master节点的slaves列表中移除该slave
        clusterNodeRemoveSlave(n->slaveof,n);
        // 如果这里升级为master的节点不是mysql，则要将该节点设置为能MIGRATE_TO
        if (n != myself) n->flags |= CLUSTER_NODE_MIGRATE_TO;
    }
    // 取消该节点的slave属性，增加master属性
    n->flags &= ~CLUSTER_NODE_SLAVE;
    n->flags |= CLUSTER_NODE_MASTER;
    n->slaveof = NULL;

    /* Update config and state. */
    // 需要before sleep更新配置和集群状态
    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                         CLUSTER_TODO_UPDATE_STATE);
}

/* This function is called when we receive a master configuration via a
 * PING, PONG or UPDATE packet. What we receive is a node, a configEpoch of the
 * node, and the set of slots claimed under this configEpoch.
 *
 * What we do is to rebind the slots with newer configuration compared to our
 * local configuration, and if needed, we turn ourself into a replica of the
 * node (see the function comments for more info).
 *
 * The 'sender' is the node for which we received a configuration update.
 * Sometimes it is not actually the "Sender" of the information, like in the
 * case we receive the info via an UPDATE packet. */
// 当我们通过PING/PONG/UPDATE packet收到了master的配置信息时，会调用这个函数来处理。
// 函数参数为节点sender、节点的配置纪元senderConfigEpoch、以及节点该配置纪元下负责的slots信息。
// 我们这里会对比本地保存的该节点的slots，使用新的配置，来重新绑定（更新）slots信息。并且在需要的时候会将我们自己转为该节点的slave。
void clusterUpdateSlotsConfigWith(clusterNode *sender, uint64_t senderConfigEpoch, unsigned char *slots) {
    int j;
    clusterNode *curmaster = NULL, *newmaster = NULL;
    /* The dirty slots list is a list of slots for which we lose the ownership
     * while having still keys inside. This usually happens after a failover
     * or after a manual cluster reconfiguration operated by the admin.
     *
     * If the update message is not able to demote a master to slave (in this
     * case we'll resync with the master updating the whole key space), we
     * need to delete all the keys in the slots we lost ownership. */
    // dirty_slots列表是我们本地丢失了负责人，且仍然有keys在里面的slots。这情况通常发生于故障转移后，或者管理者手动重新配置集群后。
    // 如果update消息不能将master降级为slave（这种情况我们需要重新同步master的所有数据），我们需要删除当前丢失负责人的slots里所有keys。
    uint16_t dirty_slots[CLUSTER_SLOTS];
    int dirty_slots_count = 0;

    /* We should detect if sender is new master of our shard.
     * We will know it if all our slots were migrated to sender, and sender
     * has no slots except ours */
    // 我们需要检查sender是否是我们节点的新master。
    // 如果我们所有slots都迁移到sender，且sender除了我们的slots外没有其他slots，则sender是我们的新master。
    int sender_slots = 0;
    int migrated_our_slots = 0;

    /* Here we set curmaster to this node or the node this node
     * replicates to if it's a slave. In the for loop we are
     * interested to check if slots are taken away from curmaster. */
    // 这里我们设置curmaster为当前节点（如果myself是master），或者当前节点的master（myself是slave）。
    // 在for循环中，我们检查slots是否从curmaster移除。
    curmaster = nodeIsMaster(myself) ? myself : myself->slaveof;

    // 如果sender就是我们自己，这在UPDATE消息中可能出现，此时我们不需要处理，直接返回就好。
    if (sender == myself) {
        serverLog(LL_WARNING,"Discarding UPDATE message about myself.");
        return;
    }

    for (j = 0; j < CLUSTER_SLOTS; j++) {
        if (bitmapTestBit(slots,j)) {
            // 如果当前检查的slot j在sender声明的它自己的slots中。
            sender_slots++;

            /* The slot is already bound to the sender of this message. */
            // 如果在我们视角的集群状态中，该slot确实由sender负责，不用处理，跳过。
            if (server.cluster->slots[j] == sender) continue;

            /* The slot is in importing state, it should be modified only
             * manually via redis-trib (example: a resharding is in progress
             * and the migrating side slot was already closed and is advertising
             * a new config. We still want the slot to be closed manually). */
            // 如果该slot正在importing到当前节点，它只能通过redis-trib手动进行修改。我们这里也不用处理，跳过。
            if (server.cluster->importing_slots_from[j]) continue;

            /* We rebind the slot to the new node claiming it if:
             * 1) The slot was unassigned or the new node claims it with a
             *    greater configEpoch.
             * 2) We are not currently importing the slot. */
            // 如果出现以下情况，我们重新绑定该slot给新的声明负责该slot的节点。
            // 1、该slot为指定，或者新节点声明负责该slot的配置纪元更大，即该声明的状态更新。
            // 2、我们当前没有在importing该slot（这个判断逻辑最新代码移到了前面，处于importing状态的直接continue了）。
            if (server.cluster->slots[j] == NULL ||
                server.cluster->slots[j]->configEpoch < senderConfigEpoch)
            {
                /* Was this slot mine, and still contains keys? Mark it as
                 * a dirty slot. */
                // 如果该slot属于我们，且有key在该slot中，而该slot不是importing。
                // 说明我们通过UPDATE变更了该slot的负责人，但是keys没有迁移，那么我们要标记该slot为dirty状态，后面清除这类keys。
                if (server.cluster->slots[j] == myself &&
                    countKeysInSlot(j) &&
                    sender != myself)
                {
                    dirty_slots[dirty_slots_count] = j;
                    dirty_slots_count++;
                }

                // 如果该slot属于curmaster，则我们设置newmaster为sender。
                if (server.cluster->slots[j] == curmaster) {
                    newmaster = sender;
                    migrated_our_slots++;
                }
                // 先清除该slot负责人信息，然后将该slot交给sender节点负责。
                clusterDelSlot(j);
                clusterAddSlot(sender,j);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                     CLUSTER_TODO_UPDATE_STATE|
                                     CLUSTER_TODO_FSYNC_CONFIG);
            }
        }
    }

    /* After updating the slots configuration, don't do any actual change
     * in the state of the server if a module disabled Redis Cluster
     * keys redirections. */
    // 更新slots配置后，如果模块禁用了Redis集群keys重定向，则不要对服务器的状态进行任何实际的更改，直接返回。
    if (server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_REDIRECTION)
        return;

    /* If at least one slot was reassigned from a node to another node
     * with a greater configEpoch, it is possible that:
     * 1) We are a master left without slots. This means that we were
     *    failed over and we should turn into a replica of the new
     *    master.
     * 2) We are a slave and our master is left without slots. We need
     *    to replicate to the new slots owner. */
    // 如果至少一个slot被重新指定给了另外一个有更大配置纪元的节点，可能存在的情况是：
    // 1、我们是master，且不再负责任何slots了。这意味着我们被执行故障转移了，我们需要转变为新master的slave。
    // 2、我们是slave，且我们的master不再负责任何slots了，我们需要从新的master中进行复制数据。
    if (newmaster && curmaster->numslots == 0 &&
            (server.cluster_allow_replica_migration ||
             sender_slots == migrated_our_slots)) {
        serverLog(LL_WARNING,
            "Configuration change detected. Reconfiguring myself "
            "as a replica of %.40s", sender->name);
        // 如果允许slave从空的masters自动迁移到孤立的masters，或者新master与我们或我们旧的master负责的slots一致，则迁移基本无代价。
        // 则我们可以设置sender节点为我们新的master，即将slave迁移到新的master下。
        clusterSetMaster(sender);
        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                             CLUSTER_TODO_UPDATE_STATE|
                             CLUSTER_TODO_FSYNC_CONFIG);
    } else if (dirty_slots_count) {
        /* If we are here, we received an update message which removed
         * ownership for certain slots we still have keys about, but still
         * we are serving some slots, so this master node was not demoted to
         * a slave.
         *
         * In order to maintain a consistent state between keys and slots
         * we need to remove all the keys from the slots we lost. */
        // 如果执行到这里，说明我们收到的是UPDATE消息，它移除了我们对部分slots的负责，但我们仍然有keys在这些slots中，
        // 另外我们也还在负责一些其他的slots，所以当前节点无法降级为slave。
        // 为了保持keys和slots状态的一致性，我们需要移除所失去的slots里面的所有keys。
        for (j = 0; j < dirty_slots_count; j++)
            // 因为该slot已经不属于我们负责了，我们存储的该slot的keys没有用了。所以这里移除该指定hash slot中的所有keys。
            delKeysInSlot(dirty_slots[j]);
    }
}

/* When this function is called, there is a packet to process starting
 * at node->rcvbuf. Releasing the buffer is up to the caller, so this
 * function should just handle the higher level stuff of processing the
 * packet, modifying the cluster state if needed.
 *
 * The function returns 1 if the link is still valid after the packet
 * was processed, otherwise 0 if the link was freed since the packet
 * processing lead to some inconsistency error (for instance a PONG
 * received from the wrong sender ID). */
// 从头解析node->rcvbuf中的数据，buffer的释放由调用者来处理，这个函数只处理高级别的集群包消息，并在需要的时候修改集群状态。
// 如果解析完，link依然有效则返回1。否则如果处理中出现err（如从错误的sender收到PONG，集群不一致），则释放link，返回0。
int clusterProcessPacket(clusterLink *link) {
    clusterMsg *hdr = (clusterMsg*) link->rcvbuf;
    uint32_t totlen = ntohl(hdr->totlen);
    uint16_t type = ntohs(hdr->type);
    mstime_t now = mstime();

    // 根据消息类型统计消息数量+1。
    if (type < CLUSTERMSG_TYPE_COUNT)
        server.cluster->stats_bus_messages_received[type]++;
    serverLog(LL_DEBUG,"--- Processing packet of type %d, %lu bytes",
        type, (unsigned long) totlen);

    /* Perform sanity checks */
    // 做一些校验。如消息必须包含签名、版本、总长度、端口、消息类型、count，一共16字节，所以消息总长要不小于16。
    // 另外消息总长度也不应该大于rcvbuf_len，否则与我们前面数据读取处理那不一致了。
    if (totlen < 16) return 1; /* At least signature, version, totlen, count. */
    if (totlen > link->rcvbuf_len) return 1;

    if (ntohs(hdr->ver) != CLUSTER_PROTO_VER) {
        /* Can't handle messages of different versions. */
        // 消息版本不一致
        return 1;
    }

    uint16_t flags = ntohs(hdr->flags);
    uint64_t senderCurrentEpoch = 0, senderConfigEpoch = 0;
    clusterNode *sender;

    // 各种类型消息的长度校验。clusterMsgData是一个联合体，sizeof计算时按照占用空间最大的成员计算。
    // 所以如果我们要精确计算最终消息大小，需要使用sizeof(clusterMsg)-sizeof(union clusterMsgData)，然后再加上真正不同消息内容的大小。
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_PONG ||
        type == CLUSTERMSG_TYPE_MEET)
    {
        // ping、pong、meet类型消息，使用clusterMsgDataGossip结构，这几种类型的消息会一次发送多个，计算时要注意。
        uint16_t count = ntohs(hdr->count);
        uint32_t explen; /* expected length of this packet */

        explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
        // count个gossip消息，一起加上再对比消息长度。
        explen += (sizeof(clusterMsgDataGossip)*count);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_FAIL) {
        // 标记节点FAIL的消息，长度正常计算
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataFail);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_PUBLISH) {
        // pub/sub publish消息，真实消息长度需要减去占位符，然后再加上channel和message数据长度。
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataPublish) -
                8 +
                ntohl(hdr->data.publish.msg.channel_len) +
                ntohl(hdr->data.publish.msg.message_len);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST ||
               type == CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK ||
               type == CLUSTERMSG_TYPE_MFSTART)
    {
        // 故障转移相关消息，这种消息没有union类型数据。
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_UPDATE) {
        // 信息更新的消息，主要是通知节点slot信息。
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataUpdate);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_MODULE) {
        // module API消息，计算时需要注意消息体大小，同样有占位符处理。
        uint32_t explen = sizeof(clusterMsg)-sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgModule) -
                3 + ntohl(hdr->data.module.msg.len);
        if (totlen != explen) return 1;
    }

    /* Check if the sender is a known node. Note that for incoming connections
     * we don't store link->node information, but resolve the node by the
     * ID in the header each time in the current implementation. */
    // 检查sender是否是一个我们已知的node。
    // 注意在目前实现中，对于主动向我们建立连接的节点，并没在link->node中存储。而是从消息header中拿到节点name，然后通过name查找节点。
    sender = clusterLookupNode(hdr->sender);

    /* Update the last time we saw any data from this node. We
     * use this in order to avoid detecting a timeout from a node that
     * is just sending a lot of data in the cluster bus, for instance
     * because of Pub/Sub. */
    // 更新从对应节点收到数据的时间。知道了对应节点是通的，后续一段时间内不再探测该节点，避免在集群bus上发送大量消息数据。
    if (sender) sender->data_received = now;

    // 如果sender不处在handshake阶段，我们需要更新对应节点带来的相关信息。
    if (sender && !nodeInHandshake(sender)) {
        /* Update our currentEpoch if we see a newer epoch in the cluster. */
        // 如果sender传过来的纪元信息更新，则我们需要使用该纪元来更新当前节点看到的集群纪元信息。
        senderCurrentEpoch = ntohu64(hdr->currentEpoch);
        senderConfigEpoch = ntohu64(hdr->configEpoch);
        if (senderCurrentEpoch > server.cluster->currentEpoch)
            server.cluster->currentEpoch = senderCurrentEpoch;
        /* Update the sender configEpoch if it is publishing a newer one. */
        // 更新我们知道的sender节点的configEpoch
        if (senderConfigEpoch > sender->configEpoch) {
            sender->configEpoch = senderConfigEpoch;
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                 CLUSTER_TODO_FSYNC_CONFIG);
        }
        /* Update the replication offset info for this node. */
        // 更新我们知道的sender节点的replication offset信息
        sender->repl_offset = ntohu64(hdr->offset);
        sender->repl_offset_time = now;
        /* If we are a slave performing a manual failover and our master
         * sent its offset while already paused, populate the MF state. */
        // 如果当前节点是slave，且在进行手动故障转移。发送方是我们的master，在暂停client时发送了它的复制offser。
        // 这里我们需要填充manual failover状态信息。
        if (server.cluster->mf_end &&
            nodeIsSlave(myself) &&
            myself->slaveof == sender &&
            hdr->mflags[0] & CLUSTERMSG_FLAG0_PAUSED &&
            server.cluster->mf_master_offset == -1)
        {
            server.cluster->mf_master_offset = sender->repl_offset;
            clusterDoBeforeSleep(CLUSTER_TODO_HANDLE_MANUALFAILOVER);
            serverLog(LL_WARNING,
                "Received replication offset for paused "
                "master manual failover: %lld",
                server.cluster->mf_master_offset);
        }
    }

    /* Initial processing of PING and MEET requests replying with a PONG. */
    // 开始处理PING、MEET请求，更新集群状态信息，处理gossip消息，最后回复PONG
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_MEET) {
        serverLog(LL_DEBUG,"Ping packet received: %p", (void*)link->node);

        /* We use incoming MEET messages in order to set the address
         * for 'myself', since only other cluster nodes will send us
         * MEET messages on handshakes, when the cluster joins, or
         * later if we changed address, and those nodes will use our
         * official address to connect to us. So by obtaining this address
         * from the socket is a simple way to discover / update our own
         * address in the cluster without it being hardcoded in the config.
         *
         * However if we don't have an address at all, we update the address
         * even with a normal PING packet. If it's wrong it will be fixed
         * by MEET later. */
        // 通过收到的MEET消息更新自己的ip地址，因为只有集群的其他节点在handshakes阶段才会发送MEET消息。
        // 在加入新集群时，或者后面我们修改了地址时，这些集群节点会使用我们官方地址来连接我们。
        // socket中可以获取到双方的地址信息，所以在集群中以这种方式获取更新我们的地址是很简单的，而不用硬编码到config中去。
        // 但是如果我们当前就没有地址，那么我们会使用PING消息中的地址来填充。如果不对，后面会根据MEET消息来修复。
        if ((type == CLUSTERMSG_TYPE_MEET || myself->ip[0] == '\0') &&
            server.cluster_announce_ip == NULL)
        {
            // 如果是MEET消息 或者 我们本来就没有地址，需要更新地址信息。
            char ip[NET_IP_STR_LEN];

            if (connSockName(link->conn,ip,sizeof(ip),NULL) != -1 &&
                strcmp(ip,myself->ip))
            {
                // 成功从conn取到本地地址，且新地址与原地址不一致，则更新。
                memcpy(myself->ip,ip,NET_IP_STR_LEN);
                serverLog(LL_WARNING,"IP address for this node updated to %s",
                    myself->ip);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            }
        }

        /* Add this node if it is new for us and the msg type is MEET.
         * In this stage we don't try to add the node with the right
         * flags, slaveof pointer, and so forth, as this details will be
         * resolved when we'll receive PONGs from the node. */
        // 如果是一个新的节点发送的MEET消息，需要把该节点加入到当前节点集群状态nodes中。
        // 在这个阶段，我们不会尝试设置正确的flags、slaveof指针等节点信息，这些详细信息将在我们后续收到该节点PONG消息时处理。
        if (!sender && type == CLUSTERMSG_TYPE_MEET) {
            clusterNode *node;

            node = createClusterNode(NULL,CLUSTER_NODE_HANDSHAKE);
            // 填充sender节点ip信息，如果MEET消息头有带过来ip，则直接使用，否则需要从conn去取对端ip。
            nodeIp2String(node->ip,link,hdr->myip);
            node->port = ntohs(hdr->port);
            node->pport = ntohs(hdr->pport);
            node->cport = ntohs(hdr->cport);
            // 加入节点
            clusterAddNode(node);
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
        }

        /* If this is a MEET packet from an unknown node, we still process
         * the gossip section here since we have to trust the sender because
         * of the message type. */
        // 对于新节点的MEET消息，这里也要处理gossip信息，因为基于这种消息类型，我们必须相信发送方。
        if (!sender && type == CLUSTERMSG_TYPE_MEET)
            clusterProcessGossipSection(hdr,link);

        /* Anyway reply with a PONG */
        // 回复PONG消息
        clusterSendPing(link,CLUSTERMSG_TYPE_PONG);
    }

    /* PING, PONG, MEET: process config information. */
    // 对于PING, PONG, MEET消息，我们需要处理配置信息。
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_PONG ||
        type == CLUSTERMSG_TYPE_MEET)
    {
        serverLog(LL_DEBUG,"%s packet received: %p",
            type == CLUSTERMSG_TYPE_PING ? "ping" : "pong",
            (void*)link->node);
        // 如果link关联的对端节点存在，表示是我们主动向对端建立连接的（前面说过，对端主动向我们建立连接，link->node是为NULL的）。
        if (link->node) {
            if (nodeInHandshake(link->node)) {
                /* If we already have this node, try to change the
                 * IP/port of the node with the new one. */
                // 如果sender是我们已经有的节点，尝试用sender传来的ip/port更新已有的信息。
                if (sender) {
                    serverLog(LL_VERBOSE,
                        "Handshake: we already know node %.40s, "
                        "updating the address if needed.", sender->name);
                    // 更新sender地址信息
                    if (nodeUpdateAddressIfNeeded(sender,link,hdr))
                    {
                        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                             CLUSTER_TODO_UPDATE_STATE);
                    }
                    /* Free this node as we already have it. This will
                     * cause the link to be freed as well. */
                    // 1、link->node非空，表示我们主动向对端建立连接。
                    // 2、处于handshake阶段的node的name是随机的。
                    // 而我们根据sender的name可以在集群nodes中找到节点，说明对端节点存在，不需要再建立连接，这里移除该节点并释放link。
                    clusterDelNode(link->node);
                    return 0;
                }

                /* First thing to do is replacing the random name with the
                 * right node name if this was a handshake stage. */
                // 当sender不存在时，第一件事情就是使用正确的节点name替换正处理handshake阶段这个节点的随机name。
                clusterRenameNode(link->node, hdr->sender);
                serverLog(LL_DEBUG,"Handshake with node %.40s completed.",
                    link->node->name);
                // 移除handshake标识，添加master/slave标识。
                link->node->flags &= ~CLUSTER_NODE_HANDSHAKE;
                link->node->flags |= flags&(CLUSTER_NODE_MASTER|CLUSTER_NODE_SLAVE);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            } else if (memcmp(link->node->name,hdr->sender,
                        CLUSTER_NAMELEN) != 0)
            {
                /* If the reply has a non matching node ID we
                 * disconnect this node and set it as not having an associated
                 * address. */
                // 如果节点不处于handshake阶段，而link对端节点name与sender传递的name不一致时，我们断开连接，并将节点的地址信息清空。
                serverLog(LL_DEBUG,"PONG contains mismatching sender ID. About node %.40s added %d ms ago, having flags %d",
                    link->node->name,
                    (int)(now-(link->node->ctime)),
                    link->node->flags);
                link->node->flags |= CLUSTER_NODE_NOADDR;
                link->node->ip[0] = '\0';
                link->node->port = 0;
                link->node->pport = 0;
                link->node->cport = 0;
                freeClusterLink(link);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                return 0;
            }
        }

        /* Copy the CLUSTER_NODE_NOFAILOVER flag from what the sender
         * announced. This is a dynamic flag that we receive from the
         * sender, and the latest status must be trusted. We need it to
         * be propagated because the slave ranking used to understand the
         * delay of each slave in the voting process, needs to know
         * what are the instances really competing. */
        // 如果sender存在，则提取传送的CLUSTER_NODE_NOFAILOVER标识，设置给sender。
        // 这是我们从sender收到的动态标识信息，必须要信任最新的状态。
        // 我们需要传播该信息，从而更多slaves节点知道故障转移信息，进行发起投票。
        if (sender) {
            int nofailover = flags & CLUSTER_NODE_NOFAILOVER;
            sender->flags &= ~CLUSTER_NODE_NOFAILOVER;
            sender->flags |= nofailover;
        }

        /* Update the node address if it changed. */
        // 如果ip/port发送变化，则更新。
        // 显然sender肯定需要存在，且我们只在PING消息中更新地址。
        // 另外如果sender处于handshake阶段，说明我们是被动建立连接的，否则前面link->node会非NULL，处理了直接返回了。
        // 被动建立连接是在当前函数前面处理MEET消息创建的新节点。需要接收PONG消息来更新信息，而不会在这个PING消息里更新。
        if (sender && type == CLUSTERMSG_TYPE_PING &&
            !nodeInHandshake(sender) &&
            nodeUpdateAddressIfNeeded(sender,link,hdr))
        {
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                 CLUSTER_TODO_UPDATE_STATE);
        }

        /* Update our info about the node */
        // link->node非NULL，我们是主动建立连接，且收到PONG消息。这里要更新节点相关信息。
        if (link->node && type == CLUSTERMSG_TYPE_PONG) {
            // 更新收到对端节点PONG消息的时间。
            link->node->pong_received = now;
            link->node->ping_sent = 0;

            /* The PFAIL condition can be reversed without external
             * help if it is momentary (that is, if it does not
             * turn into a FAIL state).
             *
             * The FAIL condition is also reversible under specific
             * conditions detected by clearNodeFailureIfNeeded(). */
            // 如果当前对端节点是PFAIL（不是FAIL），我们收到了回复则可以直接移除PFAIL标识。
            // 而如果对端节点是FAIL，则再指定条件下，我们也可以移除FAIL标识，具体详情见clearNodeFailureIfNeeded()。
            if (nodeTimedOut(link->node)) {
                link->node->flags &= ~CLUSTER_NODE_PFAIL;
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                     CLUSTER_TODO_UPDATE_STATE);
            } else if (nodeFailed(link->node)) {
                clearNodeFailureIfNeeded(link->node);
            }
        }

        /* Check for role switch: slave -> master or master -> slave. */
        // 检查角色的转换，如slave -> master 或 master -> slave。
        if (sender) {
            if (!memcmp(hdr->slaveof,CLUSTER_NODE_NULL_NAME,
                sizeof(hdr->slaveof)))
            {
                /* Node is a master. */
                // 如果sender存在，且sender发送消息中slaveof为空，则我们将sender设为master（如果本身就是master，则该函数什么也不做）。
                clusterSetNodeAsMaster(sender);
            } else {
                /* Node is a slave. */
                // 否则slaveof有值，则sender为slave节点，需要进行处理。这里先找到sender节点消息指定的master节点。
                clusterNode *master = clusterLookupNode(hdr->slaveof);

                if (nodeIsMaster(sender)) {
                    /* Master turned into a slave! Reconfigure the node. */
                    // 如果sender原来是master节点，我们现在需要将它变为slave，重新配置该节点。
                    clusterDelNodeSlots(sender);
                    sender->flags &= ~(CLUSTER_NODE_MASTER|
                                       CLUSTER_NODE_MIGRATE_TO);
                    sender->flags |= CLUSTER_NODE_SLAVE;

                    /* Update config and state. */
                    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                         CLUSTER_TODO_UPDATE_STATE);
                }

                /* Master node changed for this slave? */
                // 如果master节点存在，且sender之前不是该master的slave。
                // 这里需要先从sender原master中移除sender，然后在加入新master的slaves列表中。
                if (master && sender->slaveof != master) {
                    if (sender->slaveof)
                        clusterNodeRemoveSlave(sender->slaveof,sender);
                    clusterNodeAddSlave(master,sender);
                    sender->slaveof = master;

                    /* Update config. */
                    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                }
            }
        }

        /* Update our info about served slots.
         *
         * Note: this MUST happen after we update the master/slave state
         * so that CLUSTER_NODE_MASTER flag will be set. */

        /* Many checks are only needed if the set of served slots this
         * instance claims is different compared to the set of slots we have
         * for it. Check this ASAP to avoid other computational expansive
         * checks later. */
        // 更新我们当我负责的slots信息。
        // 注意：这必须在我们更新主/从状态之后处理，以便先设置好CLUSTER_NODE_MASTER标志。
        // 仅当该节点声明负责的slots信息，跟我们已知的该节点负责的slots数据不一致时，才需要进行许多的检查。
        // 所以这里尽早处理这个对比，避免后面进行很多的其他无用检查操作。
        clusterNode *sender_master = NULL; /* Sender or its master if slave. */
        int dirty_slots = 0; /* Sender claimed slots don't match my view? */

        if (sender) {
            // 当sender是master时，sender_master即为sender，否则sender_master为sender的master。
            sender_master = nodeIsMaster(sender) ? sender : sender->slaveof;
            if (sender_master) {
                // 比对我们知道的sender_master负责的slots是否与gossip消息中sender声明的一致。
                dirty_slots = memcmp(sender_master->slots,
                        hdr->myslots,sizeof(hdr->myslots)) != 0;
            }
        }

        /* 1) If the sender of the message is a master, and we detected that
         *    the set of slots it claims changed, scan the slots to see if we
         *    need to update our configuration. */
        // 1、如果sender是master，且slots不一致。则我们扫描gossip消息声明的slots，看我们是否需要更新我们的集群状态信息。
        if (sender && nodeIsMaster(sender) && dirty_slots)
            clusterUpdateSlotsConfigWith(sender,senderConfigEpoch,hdr->myslots);

        /* 2) We also check for the reverse condition, that is, the sender
         *    claims to serve slots we know are served by a master with a
         *    greater configEpoch. If this happens we inform the sender.
         *
         * This is useful because sometimes after a partition heals, a
         * reappearing master may be the last one to claim a given set of
         * hash slots, but with a configuration that other instances know to
         * be deprecated. Example:
         *
         * A and B are master and slave for slots 1,2,3.
         * A is partitioned away, B gets promoted.
         * B is partitioned away, and A returns available.
         *
         * Usually B would PING A publishing its set of served slots and its
         * configEpoch, but because of the partition B can't inform A of the
         * new configuration, so other nodes that have an updated table must
         * do it. In this way A will stop to act as a master (or can try to
         * failover if there are the conditions to win the election). */
        // 2、我们同时检查相反的情况，即sender声明它的master负责slots，且拥有更大的配置纪元，出现这种情况，我们要通知sender。
        // 这很有用，因为通常在分区修复后，重新出现的master很可能是最后一个声称给定slots的节点，但是持有的配置却是其他节点知道且已弃用的。
        // 例如：A、B是master和slave负责slots 1、2、3；A先分区了，B升级为master；然后B分区了，A返回正常。
        // 通常B会PING A发布它服务的slots和它的配置纪元，但是因为分区原因B无法通知A新的配置，所以拥有更新表的其他节点必须要能够进行通知。
        // 这样A将停止作为master服务，或者在有赢得选举得条件下，能够尝试进行故障转移。

        // sender是slave，且sender请求中声明master负责的slots信息与当前节点所知的对应master负责的slots信息不一致。
        if (sender && dirty_slots) {
            int j;

            for (j = 0; j < CLUSTER_SLOTS; j++) {
                // 遍历集群slots，看对应slot是否在sender声明的slots中，如果在则需要进行处理。
                if (bitmapTestBit(hdr->myslots,j)) {
                    // 如果该slot是由sender负责，或者无人负责，跳过。
                    if (server.cluster->slots[j] == sender ||
                        server.cluster->slots[j] == NULL) continue;
                    // 如果该slot当前负责人的配置纪元>sender声明负责该slot的配置纪元。
                    // 则显然该slot此时不能由sender负责，那么我们需要发送UPDATE消息通知sender更新该slot。
                    if (server.cluster->slots[j]->configEpoch >
                        senderConfigEpoch)
                    {
                        serverLog(LL_VERBOSE,
                            "Node %.40s has old slots configuration, sending "
                            "an UPDATE message about %.40s",
                                sender->name, server.cluster->slots[j]->name);
                        // 发送update消息给sender，拥有更新slots信息。
                        clusterSendUpdate(sender->link,
                            server.cluster->slots[j]);

                        /* TODO: instead of exiting the loop send every other
                         * UPDATE packet for other nodes that are the new owner
                         * of sender's slots. */
                        // TODO：不直接退出循环，而是继续检查，看对于sender宣称负责的slot是否有新的owner，从而发送其他的update包。
                        break;
                    }
                }
            }
        }

        /* If our config epoch collides with the sender's try to fix
         * the problem. */
        // 如果我们的配置纪元与sender的有冲突，尝试修复它。
        // redis slave的选举保证了，新选的master会有唯一的配置纪元，但是手动重新分片、bug等可能导致有相同的配置纪元，这里进行处理。
        if (sender &&
            nodeIsMaster(myself) && nodeIsMaster(sender) &&
            senderConfigEpoch == myself->configEpoch)
        {
            clusterHandleConfigEpochCollision(sender);
        }

        /* Get info from the gossip section */
        // 前面处理了sender不存在的情况，这里sender存在，我们也要处理gossip消息来更新故障报告数据。
        if (sender) clusterProcessGossipSection(hdr,link);
    } else if (type == CLUSTERMSG_TYPE_FAIL) {
        clusterNode *failing;

        // 处理FAIL消息，如果sender不在我们当前集群nodes中，不管该消息。
        if (sender) {
            // 查询fail的节点。显然如果集群nodes中没查到该节点，或者该节点是我们自己，或者在我们看了已经是FAIL状态了，则什么都不需要处理。
            failing = clusterLookupNode(hdr->data.fail.about.nodename);
            if (failing &&
                !(failing->flags & (CLUSTER_NODE_FAIL|CLUSTER_NODE_MYSELF)))
            {
                serverLog(LL_NOTICE,
                    "FAIL message received from %.40s about %.40s",
                    hdr->sender, hdr->data.fail.about.nodename);
                // 处理该节点为FAIL状态，更新flag标识及fail时间。
                failing->flags |= CLUSTER_NODE_FAIL;
                failing->fail_time = now;
                failing->flags &= ~CLUSTER_NODE_PFAIL;
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                                     CLUSTER_TODO_UPDATE_STATE);
            }
        } else {
            serverLog(LL_NOTICE,
                "Ignoring FAIL message from unknown node %.40s about %.40s",
                hdr->sender, hdr->data.fail.about.nodename);
        }
    } else if (type == CLUSTERMSG_TYPE_PUBLISH) {
        robj *channel, *message;
        uint32_t channel_len, message_len;

        /* Don't bother creating useless objects if there are no
         * Pub/Sub subscribers. */
        // 如果没有Pub/Sub订阅者，那么就不要创建无用的对象了。
        if (dictSize(server.pubsub_channels) ||
           dictSize(server.pubsub_patterns))
        {
            channel_len = ntohl(hdr->data.publish.msg.channel_len);
            message_len = ntohl(hdr->data.publish.msg.message_len);
            // 根据消息长度取出数据构建channel和message字符串对象。
            channel = createStringObject(
                        (char*)hdr->data.publish.msg.bulk_data,channel_len);
            message = createStringObject(
                        (char*)hdr->data.publish.msg.bulk_data+channel_len,
                        message_len);
            // 向该channel publish 消息。
            pubsubPublishMessage(channel,message);
            decrRefCount(channel);
            decrRefCount(message);
        }
    } else if (type == CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST) {
        // sender请求进行故障转移。如果我们集群nodes中没有该节点，不处理，直接返回。
        if (!sender) return 1;  /* We don't know that node. */
        // 处理，如果对端可以进行故障转移，而我们又还没有投票的话，则回复支持。
        clusterSendFailoverAuthIfNeeded(sender,hdr);
    } else if (type == CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK) {
        // 故障转移投票确认消息。如果我们集群nodes中没有该节点，不处理，直接返回。
        if (!sender) return 1;  /* We don't know that node. */
        /* We consider this vote only if the sender is a master serving
         * a non zero number of slots, and its currentEpoch is greater or
         * equal to epoch where this node started the election. */
        // 仅在sender是master，且有负责slots，且sender的currentEpoch>=我们开始选举时的纪元时，我们才认为该投票有效。
        if (nodeIsMaster(sender) && sender->numslots > 0 &&
            senderCurrentEpoch >= server.cluster->failover_auth_epoch)
        {
            // 收到的投票有效数+1，
            server.cluster->failover_auth_count++;
            /* Maybe we reached a quorum here, set a flag to make sure
             * we check ASAP. */
            // 因为收到了有效的投票，可能我们达到了quorum数，所以我们这里设置FAILOVER标识，从而尽快进行故障转移，不用等到cron中。
            clusterDoBeforeSleep(CLUSTER_TODO_HANDLE_FAILOVER);
        }
    } else if (type == CLUSTERMSG_TYPE_MFSTART) {
        /* This message is acceptable only if I'm a master and the sender
         * is one of my slaves. */
        // 收到手动故障转移的消息。仅当我们是master，且sender是我们的slave时，我们才接受处理该消息。
        if (!sender || sender->slaveof != myself) return 1;
        /* Manual failover requested from slaves. Initialize the state
         * accordingly. */
        // slave请求进行手动故障转移，这里初始化手动故障转移状态。
        resetManualFailover();
        server.cluster->mf_end = now + CLUSTER_MF_TIMEOUT;
        server.cluster->mf_slave = sender;
        // 手动故障转移，暂停当前master节点的所有client写操作。
        pauseClients(now+(CLUSTER_MF_TIMEOUT*CLUSTER_MF_PAUSE_MULT),CLIENT_PAUSE_WRITE);
        serverLog(LL_WARNING,"Manual failover requested by replica %.40s.",
            sender->name);
        /* We need to send a ping message to the replica, as it would carry
         * `server.cluster->mf_master_offset`, which means the master paused clients
         * at offset `server.cluster->mf_master_offset`, so that the replica would
         * know that it is safe to set its `server.cluster->mf_can_start` to 1 so as
         * to complete failover as quickly as possible. */
        // 我们需要发送PING消息给该slave，因为我们需要同步过去当前的master_repl_offset。
        // 这样进行故障转移的salve收到PING消息，会将该值作为mf_master_offset，当该slave处理的复制数据达到该offset时，
        // 就可以设置mf_can_start为1，从而安全的开启故障转移了。
        clusterSendPing(link, CLUSTERMSG_TYPE_PING);
    } else if (type == CLUSTERMSG_TYPE_UPDATE) {
        // 收到了让我们更新slots数据的消息。
        clusterNode *n; /* The node the update is about. */
        // 获取请求中报告节点的配置纪元
        uint64_t reportedConfigEpoch =
                    ntohu64(hdr->data.update.nodecfg.configEpoch);

        // 如果我们集群nodes中没有sender节点，不处理，直接返回。
        if (!sender) return 1;  /* We don't know the sender. */
        // 查找请求中的报告节点，如果节点不存在，则直接返回。
        n = clusterLookupNode(hdr->data.update.nodecfg.nodename);
        if (!n) return 1;   /* We don't know the reported node. */
        // 如果当前我们知道的指定节点的配置纪元 不小于 请求中报告的对应节点的配置纪元，那么我们数据是最新的，什么都不需要处理。
        if (n->configEpoch >= reportedConfigEpoch) return 1; /* Nothing new. */

        // 执行到这里说明，请求中报告的节点配置是更新的，我们需要以它为准，需要更新我们自己的配置信息。
        /* If in our current config the node is a slave, set it as a master. */
        // 如果在我们本地配置中，报告节点是slave，我们将它设置为master。（因为对端发送的报告节点都是master）
        if (nodeIsSlave(n)) clusterSetNodeAsMaster(n);

        /* Update the node's configEpoch. */
        // 更新当前节点视角下，报告节点的配置纪元。
        n->configEpoch = reportedConfigEpoch;
        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                             CLUSTER_TODO_FSYNC_CONFIG);

        /* Check the bitmap of served slots and update our
         * config accordingly. */
        // 使用请求中的slots信息，来更新我们视角下报告节点的slots表。
        clusterUpdateSlotsConfigWith(n,reportedConfigEpoch,
            hdr->data.update.nodecfg.slots);
    } else if (type == CLUSTERMSG_TYPE_MODULE) {
        // 未知发送者，消息一律不处理
        if (!sender) return 1;  /* Protect the module from unknown nodes. */
        /* We need to route this message back to the right module subscribed
         * for the right message type. */
        // 我们根据module消息类型，将消息路由到正确的module去执行处理。
        uint64_t module_id = hdr->data.module.msg.module_id; /* Endian-safe ID */
        uint32_t len = ntohl(hdr->data.module.msg.len);
        uint8_t type = hdr->data.module.msg.type;
        unsigned char *payload = hdr->data.module.msg.bulk_data;
        moduleCallClusterReceivers(sender->name,module_id,type,payload,len);
    } else {
        serverLog(LL_WARNING,"Received unknown packet type: %d", type);
    }
    return 1;
}

/* This function is called when we detect the link with this node is lost.
   We set the node as no longer connected. The Cluster Cron will detect
   this connection and will try to get it connected again.

   Instead if the node is a temporary node used to accept a query, we
   completely free the node on error. */
// 当我们探测到link断开时，我们调用这个函数来释放连接，置空link。这里不会释放节点数据，Cluster Cron后面会重新进行连接处理。
// 但是如果节点是一个用于接收请求临时节点，我们在遇到error时会完全的释放节点。
void handleLinkIOError(clusterLink *link) {
    freeClusterLink(link);
}

/* Send data. This is handled using a trivial send buffer that gets
 * consumed by write(). We don't try to optimize this for speed too much
 * as this is a very low traffic channel. */
// 发送数据handler。connWrite最终会调用write()方法来发送link->sndbuf数据到conn中。
void clusterWriteHandler(connection *conn) {
    clusterLink *link = connGetPrivateData(conn);
    ssize_t nwritten;

    // 写数据到conn中
    nwritten = connWrite(conn, link->sndbuf, sdslen(link->sndbuf));
    if (nwritten <= 0) {
        serverLog(LL_DEBUG,"I/O error writing to node link: %s",
            (nwritten == -1) ? connGetLastError(conn) : "short write");
        handleLinkIOError(link);
        return;
    }
    // 移除已经发送的数据。
    sdsrange(link->sndbuf,nwritten,-1);
    if (sdslen(link->sndbuf) == 0)
        // 当剩余数据为0时，这里移除写处理handler。
        connSetWriteHandler(link->conn, NULL);
}

/* A connect handler that gets called when a connection to another node
 * gets established.
 */
// 当conn与其他节点连接建立时，socket可写，执行socket的ae_handler处理连接，其中会调用这个函数做一些集群相关操作。
void clusterLinkConnectHandler(connection *conn) {
    // 从conn获取link，从link拿到连接的节点。
    clusterLink *link = connGetPrivateData(conn);
    clusterNode *node = link->node;

    /* Check if connection succeeded */
    // 检查底层conn是否真的连接成功
    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_VERBOSE, "Connection with Node %.40s at %s:%d failed: %s",
                node->name, node->ip, node->cport,
                connGetLastError(conn));
        freeClusterLink(link);
        return;
    }

    /* Register a read handler from now on */
    // 给conn注册读事件处理函数
    connSetReadHandler(conn, clusterReadHandler);

    /* Queue a PING in the new connection ASAP: this is crucial
     * to avoid false positives in failure detection.
     *
     * If the node is flagged as MEET, we send a MEET message instead
     * of a PING one, to force the receiver to add us in its node
     * table. */
    // 尽早发送PING给conn对端节点：这对于避免故障探测时误报很重要。
    // 如果节点当前是MEET状态，我们发送MEET消息而不是PING，来强制接收方将我们加入它自己的cluster节点表。
    mstime_t old_ping_sent = node->ping_sent;
    clusterSendPing(link, node->flags & CLUSTER_NODE_MEET ?
            CLUSTERMSG_TYPE_MEET : CLUSTERMSG_TYPE_PING);
    if (old_ping_sent) {
        /* If there was an active ping before the link was
         * disconnected, we want to restore the ping time, otherwise
         * replaced by the clusterSendPing() call. */
        // 如果在link断开之前有发送ping，我们这里需要刷新ping时间，避免因为时间到了而执行clusterSendPing()操作。
        node->ping_sent = old_ping_sent;
    }
    /* We can clear the flag after the first packet is sent.
     * If we'll never receive a PONG, we'll never send new packets
     * to this node. Instead after the PONG is received and we
     * are no longer in meet/handshake status, we want to send
     * normal PING packets. */
    // 发送第一个数据包后，我们可以清除该标志。如果我们后面没有收到PONG，那么我们将不会将新数据包发送到该节点。
    // 相反如果收到PONG，此时我们也不处于MEET/handshake状态，直接就发送普通的PING数据包了。
    node->flags &= ~CLUSTER_NODE_MEET;

    serverLog(LL_DEBUG,"Connecting with Node %.40s at %s:%d",
            node->name, node->ip, node->cport);
}

/* Read data. Try to read the first field of the header first to check the
 * full length of the packet. When a whole packet is in memory this function
 * will call the function to process the packet. And so forth. */
// 读取数据。尝试先读取header的第一个字段以检查数据包的全长。当整个数据包在内存中时，此函数将调用clusterProcessPacket()来处理数据包。
void clusterReadHandler(connection *conn) {
    clusterMsg buf[1];
    ssize_t nread;
    clusterMsg *hdr;
    clusterLink *link = connGetPrivateData(conn);
    unsigned int readlen, rcvbuflen;

    // 只要有数据，就一直循环读取。
    while(1) { /* Read as long as there is data to read. */
        // 当前link接收数据到buf的长度
        rcvbuflen = link->rcvbuf_len;
        if (rcvbuflen < 8) {
            /* First, obtain the first 8 bytes to get the full message
             * length. */
            // 如果已读取的长度小于8字节，则继续读满8字节来获取消息的总长度。
            // clusterMsg的头4个字节是消息签名特殊字符"RCmb"，然后紧接着4个字节表示消息总长度。
            readlen = 8 - rcvbuflen;
        } else {
            /* Finally read the full message. */
            // 已读取长度大于8字节。我们需要解析消息总长度，然后读取全部消息内容到link buf中。
            hdr = (clusterMsg*) link->rcvbuf;
            if (rcvbuflen == 8) {
                /* Perform some sanity check on the message signature
                 * and length. */
                // 从前面if中可以看到，我们会先去读取8字节数据，从而在这里检查下消息签名是否正确以及长度是否足够。
                if (memcmp(hdr->sig,"RCmb",4) != 0 ||
                    ntohl(hdr->totlen) < CLUSTERMSG_MIN_LEN)
                {
                    // 签名或消息长度不对，打印日志并释放连接。连接释放后会在定时任务中重新来建立连接。
                    serverLog(LL_WARNING,
                        "Bad message length or signature received "
                        "from Cluster bus.");
                    handleLinkIOError(link);
                    return;
                }
            }
            // 计算还应读取数据的总长度。消息总长度-已读取长度。
            readlen = ntohl(hdr->totlen) - rcvbuflen;
            // 什么情况会readlen>sizeof(buf)，溢出？buf大小即一个clusterMsg结构大小，一次消息读取数据理应最大为buf大小。
            if (readlen > sizeof(buf)) readlen = sizeof(buf);
        }

        // 从conn读取数据到buf中。
        nread = connRead(conn,buf,readlen);
        // 没读取到，但conn连接正常。返回等待下次可读触发再处理。
        if (nread == -1 && (connGetState(conn) == CONN_STATE_CONNECTED)) return; /* No more data ready. */

        if (nread <= 0) {
            /* I/O error... */
            // 出现其他读err，释放连接返回。
            serverLog(LL_DEBUG,"I/O error reading from node link: %s",
                (nread == 0) ? "connection closed" : connGetLastError(conn));
            handleLinkIOError(link);
            return;
        } else {
            /* Read data and recast the pointer to the new buffer. */
            // 读到了数据到临时buf中，但我们需要将数据放入link的buf中，这里看是否能放下，放不下需要扩容。
            // 计算link的buf中剩余未使用空间。
            size_t unused = link->rcvbuf_alloc - link->rcvbuf_len;
            if ((size_t)nread > unused) {
                // 如果当前读取的数据大小大于了link中buf的未使用空间，则需要扩容。
                size_t required = link->rcvbuf_len + nread;
                /* If less than 1mb, grow to twice the needed size, if larger grow by 1mb. */
                // 如果required小于1MB，则按两倍大小扩容，否则每次增加1MB。
                link->rcvbuf_alloc = required < RCVBUF_MAX_PREALLOC ? required * 2: required + RCVBUF_MAX_PREALLOC;
                // 调用zrealloc扩容
                link->rcvbuf = zrealloc(link->rcvbuf, link->rcvbuf_alloc);
            }
            // 当前读取到的数据从buf中追加到link->rcvbuf中。
            memcpy(link->rcvbuf + link->rcvbuf_len, buf, nread);
            link->rcvbuf_len += nread;
            // 可能进行了扩容分配内存，所以这里要将hdr指针重新指向读取数据的位置。
            hdr = (clusterMsg*) link->rcvbuf;
            rcvbuflen += nread;
        }

        /* Total length obtained? Process this packet. */
        // 这里需要判断clusterMsg是否全部读取完毕，如果读取长度与消息头指示的总长度一致则全读取完，需要进一步处理该消息包。
        if (rcvbuflen >= 8 && rcvbuflen == ntohl(hdr->totlen)) {
            // 处理消息packet
            if (clusterProcessPacket(link)) {
                // 处理完返回1，表示link依然有效，我们这里重置rcvbuf。有zrealloc重分配的我们需要释放掉，还原为初始1kb大小。
                if (link->rcvbuf_alloc > RCVBUF_INIT_LEN) {
                    zfree(link->rcvbuf);
                    link->rcvbuf = zmalloc(link->rcvbuf_alloc = RCVBUF_INIT_LEN);
                }
                link->rcvbuf_len = 0;
            } else {
                return; /* Link no longer valid. */
            }
        }
    }
}

/* Put stuff into the send buffer.
 *
 * It is guaranteed that this function will never have as a side effect
 * the link to be invalidated, so it is safe to call this function
 * from event handlers that will do stuff with the same link later. */
// 将消息放置到link的send buf中。如果没有安装写hander，这里还会处理设置。
// 该函数保证不会产生link失效的副作用，所以在event handlers中调用是安全的。
void clusterSendMessage(clusterLink *link, unsigned char *msg, size_t msglen) {
    if (sdslen(link->sndbuf) == 0 && msglen != 0)
        // 如果原link->sndbuf是空的，要发送数据，这里需要安装写handler。
        connSetWriteHandlerWithBarrier(link->conn, clusterWriteHandler, 1);

    // 将msg数据追加到link->sndbuf中。
    link->sndbuf = sdscatlen(link->sndbuf, msg, msglen);

    /* Populate sent messages stats. */
    // 数据发送次数按类别统计+1。
    clusterMsg *hdr = (clusterMsg*) msg;
    uint16_t type = ntohs(hdr->type);
    if (type < CLUSTERMSG_TYPE_COUNT)
        server.cluster->stats_bus_messages_sent[type]++;
}

/* Send a message to all the nodes that are part of the cluster having
 * a connected link.
 *
 * It is guaranteed that this function will never have as a side effect
 * some node->link to be invalidated, so it is safe to call this function
 * from event handlers that will do stuff with node links later. */
// 向集群中具有连接的link的节点发送消息。
// 该函数保证不会产生node->link失效的副作用，所以在连接的event handlers中调用这个函数是安全的。
void clusterBroadcastMessage(void *buf, size_t len) {
    dictIterator *di;
    dictEntry *de;

    // 迭代遍历集群节点。
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        // 如果节点没有link，跳过。
        if (!node->link) continue;
        // 如果节点是我们自己，或者节点正在handshake，也跳过
        if (node->flags & (CLUSTER_NODE_MYSELF|CLUSTER_NODE_HANDSHAKE))
            continue;
        // 发送消息给对应节点。
        clusterSendMessage(node->link,buf,len);
    }
    dictReleaseIterator(di);
}

/* Build the message header. hdr must point to a buffer at least
 * sizeof(clusterMsg) in bytes. */
// 构建message header。hdr指针必须指向sizeof(clusterMsg)字节大小的buf空间。
void clusterBuildMessageHdr(clusterMsg *hdr, int type) {
    int totlen = 0;
    uint64_t offset;
    clusterNode *master;

    /* If this node is a master, we send its slots bitmap and configEpoch.
     * If this node is a slave we send the master's information instead (the
     * node is flagged as slave so the receiver knows that it is NOT really
     * in charge for this slots. */
    // 如果当前节点是master，我们还会发送所负责的slots bitmap 和 configEpoch。
    // 如果节点是slave，我们会发送我们master的这些信息。
    // 因为接收方根据节点的flag可以知道是slave还是master，所以可以判断区分。
    master = (nodeIsSlave(myself) && myself->slaveof) ?
              myself->slaveof : myself;

    // 置空hdr
    memset(hdr,0,sizeof(*hdr));
    // 写入版本、签名、type、sender name等信息
    hdr->ver = htons(CLUSTER_PROTO_VER);
    hdr->sig[0] = 'R';
    hdr->sig[1] = 'C';
    hdr->sig[2] = 'm';
    hdr->sig[3] = 'b';
    hdr->type = htons(type);
    memcpy(hdr->sender,myself->name,CLUSTER_NAMELEN);

    /* If cluster-announce-ip option is enabled, force the receivers of our
     * packets to use the specified address for this node. Otherwise if the
     * first byte is zero, they'll do auto discovery. */
    // 如果cluster-announce-ip选项开启，强制接收方使用该cluster_announce_ip来作为我们节点的地址。
    // 否则如果hdr->myip首字符是'\0'的话，接收方将从conn中自己获取。
    memset(hdr->myip,0,NET_IP_STR_LEN);
    if (server.cluster_announce_ip) {
        strncpy(hdr->myip,server.cluster_announce_ip,NET_IP_STR_LEN);
        hdr->myip[NET_IP_STR_LEN-1] = '\0';
    }

    /* Handle cluster-announce-[tls-|bus-]port. */
    // 解析cluster announced相关port。
    int announced_port, announced_pport, announced_cport;
    deriveAnnouncedPorts(&announced_port, &announced_pport, &announced_cport);

    // 发送master的slots信息
    memcpy(hdr->myslots,master->slots,sizeof(hdr->myslots));
    // 如果当前是slave，则填充master的name
    memset(hdr->slaveof,0,CLUSTER_NAMELEN);
    if (myself->slaveof != NULL)
        memcpy(hdr->slaveof,myself->slaveof->name, CLUSTER_NAMELEN);
    // 填充port信息
    hdr->port = htons(announced_port);
    hdr->pport = htons(announced_pport);
    hdr->cport = htons(announced_cport);
    // 当前节点的flags，以及当前节点视角集群的状态。
    hdr->flags = htons(myself->flags);
    hdr->state = server.cluster->state;

    /* Set the currentEpoch and configEpochs. */
    // 填充集群的当前纪元，以及当前节点（或master）的配置纪元。
    hdr->currentEpoch = htonu64(server.cluster->currentEpoch);
    hdr->configEpoch = htonu64(master->configEpoch);

    /* Set the replication offset. */
    // 填充复制offset。如果是master，则是数据传播的offset；如果是slave，则是已处理的复制数据offset。
    if (nodeIsSlave(myself))
        offset = replicationGetSlaveOffset();
    else
        offset = server.master_repl_offset;
    hdr->offset = htonu64(offset);

    /* Set the message flags. */
    // 设置message flag。如果当前是master，且接收到了手动故障转移，则设置PAUSED标识。
    if (nodeIsMaster(myself) && server.cluster->mf_end)
        hdr->mflags[0] |= CLUSTERMSG_FLAG0_PAUSED;

    /* Compute the message length for certain messages. For other messages
     * this is up to the caller. */
    // 对于指定的message（固定长度，没有变长消息体），这里计算消息长度。其他类别的消息，调用者需要自己处理修复总长度数据。
    if (type == CLUSTERMSG_TYPE_FAIL) {
        totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
        totlen += sizeof(clusterMsgDataFail);
    } else if (type == CLUSTERMSG_TYPE_UPDATE) {
        totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
        totlen += sizeof(clusterMsgDataUpdate);
    }
    // 填充消息总长度
    hdr->totlen = htonl(totlen);
    /* For PING, PONG, and MEET, fixing the totlen field is up to the caller. */
}

/* Return non zero if the node is already present in the gossip section of the
 * message pointed by 'hdr' and having 'count' gossip entries. Otherwise
 * zero is returned. Helper for clusterSendPing(). */
// clusterSendPing()的帮助函数，判断节点是否已经加入gossip中。如果节点已经存在于'hdr'指向集群消息的gossip中返回非零，否则返回零。
int clusterNodeIsInGossipSection(clusterMsg *hdr, int count, clusterNode *n) {
    int j;
    for (j = 0; j < count; j++) {
        // 遍历gossip中已经加入的节点，对比节点的name，如果一致，则说明已经存在，则跳出循环。
        if (memcmp(hdr->data.ping.gossip[j].nodename,n->name,
                CLUSTER_NAMELEN) == 0) break;
    }
    // j==count，则说明对比完了，但没找到，即节点不在gossip中。
    return j != count;
}

/* Set the i-th entry of the gossip section in the message pointed by 'hdr'
 * to the info of the specified node 'n'. */
// 将节点加入集群消息的gossip中，加入位置为第i个节点处。
void clusterSetGossipEntry(clusterMsg *hdr, int i, clusterNode *n) {
    clusterMsgDataGossip *gossip;
    // 取到应该加入的位置指针。
    gossip = &(hdr->data.ping.gossip[i]);
    // 按gossip data结构，依次填充进去数据。
    memcpy(gossip->nodename,n->name,CLUSTER_NAMELEN);
    gossip->ping_sent = htonl(n->ping_sent/1000);
    gossip->pong_received = htonl(n->pong_received/1000);
    memcpy(gossip->ip,n->ip,sizeof(n->ip));
    gossip->port = htons(n->port);
    gossip->cport = htons(n->cport);
    gossip->flags = htons(n->flags);
    gossip->pport = htons(n->pport);
    gossip->notused1 = 0;
}

/* Send a PING or PONG packet to the specified node, making sure to add enough
 * gossip information. */
// 发送PING或PONG消息给指定节点，确保添加足够的gossip信息
void clusterSendPing(clusterLink *link, int type) {
    unsigned char *buf;
    clusterMsg *hdr;
    // 已经添加的gossip消息数
    int gossipcount = 0; /* Number of gossip sections added so far. */
    // 希望添加的gossip消息数
    int wanted; /* Number of gossip sections we want to append if possible. */
    // 总的packet长度
    int totlen; /* Total packet length. */
    /* freshnodes is the max number of nodes we can hope to append at all:
     * nodes available minus two (ourself and the node we are sending the
     * message to). However practically there may be less valid nodes since
     * nodes in handshake state, disconnected, are not considered. */
    // freshnodes是我们希望附加gossip信息的最大节点数：所有可用节点减去两个（自己和发送消息的节点）。
    // 但是实际上可能有效节点数较少，因为我们不考虑处于握手状态、断开连接的节点。
    int freshnodes = dictSize(server.cluster->nodes)-2;

    /* How many gossip sections we want to add? 1/10 of the number of nodes
     * and anyway at least 3. Why 1/10?
     *
     * If we have N masters, with N/10 entries, and we consider that in
     * node_timeout we exchange with each other node at least 4 packets
     * (we ping in the worst case in node_timeout/2 time, and we also
     * receive two pings from the host), we have a total of 8 packets
     * in the node_timeout*2 failure reports validity time. So we have
     * that, for a single PFAIL node, we can expect to receive the following
     * number of failure reports (in the specified window of time):
     *
     * PROB * GOSSIP_ENTRIES_PER_PACKET * TOTAL_PACKETS:
     *
     * PROB = probability of being featured in a single gossip entry,
     *        which is 1 / NUM_OF_NODES.
     * ENTRIES = 10.
     * TOTAL_PACKETS = 2 * 4 * NUM_OF_MASTERS.
     *
     * If we assume we have just masters (so num of nodes and num of masters
     * is the same), with 1/10 we always get over the majority, and specifically
     * 80% of the number of nodes, to account for many masters failing at the
     * same time.
     *
     * Since we have non-voting slaves that lower the probability of an entry
     * to feature our node, we set the number of entries per packet as
     * 10% of the total nodes we have. */
    // 这里我们附加所有节点个数的1/10数量的gossip消息，但最少也不少于3个。为什么是1/10？理由如下：
    // 如果我们有N个master，则附加N/10个条目，
    // 我们考虑到一个node_timeout期间会交换至少4个packets（最坏情况node_timeout/2发送一个ping，也就是会发2次，同时也会收到2次ping），
    // 所以在故障报告超时时间node_timeout*2的时间内，我们会交换8个packets。
    // 基于上面考虑，对于单个PFAIL节点，在指定的时间窗口中，期望收到 PROB * GOSSIP_ENTRIES_PER_PACKET * TOTAL_PACKETS 数量的故障报告。
    //  PROB是指node在出现在单个gossip entry中的概率，即1/NUM_OF_NODES，均等。
    //  ENTRIES = NUM_OF_NODES/10
    //  TOTAL_PACKETS = 2 * 4 * NUM_OF_MASTERS
    //  计算期望收到的故障报告数量为 PROB * GOSSIP_ENTRIES_PER_PACKET * TOTAL_PACKETS = 80% * NUM_OF_MASTERS
    // 假设我们只有master节点（即总节点数与master节点数一致），当ENTRIES为N/10时，我们能覆盖到80%的节点，能同时处理更多master节点出现故障情况。
    // 因为我们slave节点是没有投票权的，会降低entry发送到我们节点的概率，所以我们设置每个packet携带的entries数量为我们有的总节点数的1/10。
    wanted = floor(dictSize(server.cluster->nodes)/10);
    // 最少3个，最多不不多余上限freshnodes。
    if (wanted < 3) wanted = 3;
    if (wanted > freshnodes) wanted = freshnodes;

    /* Include all the nodes in PFAIL state, so that failure reports are
     * faster to propagate to go from PFAIL to FAIL state. */
    // 发送时我们这里设置包含所有的PFAIL状态的节点，这样对于PFAIL节点failure reports能尽快的传播，从而快速变更为FAIL状态。
    int pfail_wanted = server.cluster->stats_pfail_nodes;

    /* Compute the maximum totlen to allocate our buffer. We'll fix the totlen
     * later according to the number of gossip sections we really were able
     * to put inside the packet. */
    // 计算最大totlen，来分配我们的buffer空间。后面我们会根据实际添加到packet中的gossip消息数来重新计算totlen。
    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += (sizeof(clusterMsgDataGossip)*(wanted+pfail_wanted));
    /* Note: clusterBuildMessageHdr() expects the buffer to be always at least
     * sizeof(clusterMsg) or more. */
    // 注意：clusterBuildMessageHdr()期望的buffer大小总是至少sizeof(clusterMsg)，或者更多。
    if (totlen < (int)sizeof(clusterMsg)) totlen = sizeof(clusterMsg);
    // 分配空间
    buf = zcalloc(totlen);
    hdr = (clusterMsg*) buf;

    /* Populate the header. */
    if (link->node && type == CLUSTERMSG_TYPE_PING)
        // 如果是发送PING消息，更新发送时间，用于超时检测。
        link->node->ping_sent = mstime();
    // 填充header信息
    clusterBuildMessageHdr(hdr,type);

    /* Populate the gossip fields */
    // 填充gossip消息数据，这里最大迭代次数为3倍的wanted。
    int maxiterations = wanted*3;
    while(freshnodes > 0 && gossipcount < wanted && maxiterations--) {
        // 随机选择一个节点
        dictEntry *de = dictGetRandomKey(server.cluster->nodes);
        clusterNode *this = dictGetVal(de);

        /* Don't include this node: the whole packet header is about us
         * already, so we just gossip about other nodes. */
        // 如果拿到的是自己，则跳过。因为packet header已经包含我们自己的信息了，所以gossip部分发送其他节点的信息就好。
        if (this == myself) continue;

        /* PFAIL nodes will be added later. */
        // PFAIL类型的节点后面会另外加上，这里也跳过处理。
        if (this->flags & CLUSTER_NODE_PFAIL) continue;

        /* In the gossip section don't include:
         * 1) Nodes in HANDSHAKE state.
         * 3) Nodes with the NOADDR flag set.
         * 4) Disconnected nodes if they don't have configured slots.
         */
        // gossip传递的节点不包含如下节点：
        // 1、正在进行handshake的节点。
        // 2、没有地址信息的节点，即带有NOADDR标识的节点。
        // 3、如果节点断开了连接，但没有配置slots。
        if (this->flags & (CLUSTER_NODE_HANDSHAKE|CLUSTER_NODE_NOADDR) ||
            (this->link == NULL && this->numslots == 0))
        {
            // 满足这些条的节点，除了跳过外，freshnodes还自减1。技术上可能不对，但可以提早跳出循环，避免过多的CPU处理。
            freshnodes--; /* Technically not correct, but saves CPU. */
            continue;
        }

        /* Do not add a node we already have. */
        // 如果该节点我们已经加入到了gossip中，跳过重复的节点。
        if (clusterNodeIsInGossipSection(hdr,gossipcount,this)) continue;

        /* Add it */
        // 执行到这里，说明我们需要将节点加入到gossip中。
        clusterSetGossipEntry(hdr,gossipcount,this);
        freshnodes--;
        gossipcount++;
    }

    /* If there are PFAIL nodes, add them at the end. */
    // 如果有PFAIL节点，这里我们单独将所有这类节点加入。
    if (pfail_wanted) {
        dictIterator *di;
        dictEntry *de;

        di = dictGetSafeIterator(server.cluster->nodes);
        // 遍历所有的集群nodes，判断如果是PFAIL，则加入到gossip中
        while((de = dictNext(di)) != NULL && pfail_wanted > 0) {
            clusterNode *node = dictGetVal(de);
            if (node->flags & CLUSTER_NODE_HANDSHAKE) continue;
            if (node->flags & CLUSTER_NODE_NOADDR) continue;
            if (!(node->flags & CLUSTER_NODE_PFAIL)) continue;
            clusterSetGossipEntry(hdr,gossipcount,node);
            // 这里freshnodes自减可以移除。
            freshnodes--;
            gossipcount++;
            /* We take the count of the slots we allocated, since the
             * PFAIL stats may not match perfectly with the current number
             * of PFAIL nodes. */
            // 因为前面PFAIL统计数据可能与当前的PFAIL节点数不一致。
            // 有可能有更多PFAIL，而hdr是预分配的空间，避免超出空间，所以这里将pfail_wanted>0也加入了循环判断。
            pfail_wanted--;
        }
        dictReleaseIterator(di);
    }

    /* Ready to send... fix the totlen fiend and queue the message in the
     * output buffer. */
    // 准备发送消息，这里先修改totlen值，然后将消息加入output缓冲中，等待事件可写时写入conn发送到对方。
    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += (sizeof(clusterMsgDataGossip)*gossipcount);
    hdr->count = htons(gossipcount);
    hdr->totlen = htonl(totlen);
    // 将消息放入link->sndbuf中，等待发送。
    clusterSendMessage(link,buf,totlen);
    zfree(buf);
}

/* Send a PONG packet to every connected node that's not in handshake state
 * and for which we have a valid link.
 *
 * In Redis Cluster pongs are not used just for failure detection, but also
 * to carry important configuration information. So broadcasting a pong is
 * useful when something changes in the configuration and we want to make
 * the cluster aware ASAP (for instance after a slave promotion).
 *
 * The 'target' argument specifies the receiving instances using the
 * defines below:
 *
 * CLUSTER_BROADCAST_ALL -> All known instances.
 * CLUSTER_BROADCAST_LOCAL_SLAVES -> All slaves in my master-slaves ring.
 */
// 广播PONG消息给所有连接的节点（节点不是handshake状态，且有一个有效的link）。
// 在redis集群中，PONG消息不仅拥有故障检测，同时也会携带许多重要的配置信息。
// 当我们改变了集群配置（如在将slave提升为master后），并且想让集群尽快感知时，广播PONG消息是非常有用的。
// target参数指定了接收节点，可传入如下参数：
//  CLUSTER_BROADCAST_ALL，表示发送到所有已知的节点。
//  CLUSTER_BROADCAST_LOCAL_SLAVES，表示发送到我们master-slaves链上的所有slaves节点（包括我们自己的slaves，以及我们master的slaves）。
#define CLUSTER_BROADCAST_ALL 0
#define CLUSTER_BROADCAST_LOCAL_SLAVES 1
void clusterBroadcastPong(int target) {
    dictIterator *di;
    dictEntry *de;

    // 遍历集群nodes列表中的所有节点处理
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        // 如果节点没有有效link，或节点处于handshake，或节点是我们自己时，跳过。
        if (!node->link) continue;
        if (node == myself || nodeInHandshake(node)) continue;
        if (target == CLUSTER_BROADCAST_LOCAL_SLAVES) {
            // 如果目标是所有slave，则判断该节点是否是我们的slave或是我们master的slave，如果不是则跳过。
            int local_slave =
                nodeIsSlave(node) && node->slaveof &&
                (node->slaveof == myself || node->slaveof == myself->slaveof);
            if (!local_slave) continue;
        }
        // 发送PONG消息通过对应节点
        clusterSendPing(node->link,CLUSTERMSG_TYPE_PONG);
    }
    dictReleaseIterator(di);
}

/* Send a PUBLISH message.
 *
 * If link is NULL, then the message is broadcasted to the whole cluster. */
// 发送一个集群PUBLISH消息。如果link非空，则publish消息发送到指定节点；否则link为NULL，publish消息广播到集群所有节点。
void clusterSendPublish(clusterLink *link, robj *channel, robj *message) {
    unsigned char *payload;
    clusterMsg buf[1];
    clusterMsg *hdr = (clusterMsg*) buf;
    uint32_t totlen;
    uint32_t channel_len, message_len;

    // 获取channel、message对象，及数据长度。
    channel = getDecodedObject(channel);
    message = getDecodedObject(message);
    channel_len = sdslen(channel->ptr);
    message_len = sdslen(message->ptr);

    // 构建消息header，并更新消息总长度
    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_PUBLISH);
    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += sizeof(clusterMsgDataPublish) - 8 + channel_len + message_len;

    // 填充message中长度信息
    hdr->data.publish.msg.channel_len = htonl(channel_len);
    hdr->data.publish.msg.message_len = htonl(message_len);
    hdr->totlen = htonl(totlen);

    /* Try to use the local buffer if possible */
    // 尝试使用本地缓存，如果空间不够则需要从新分配空间，并将hdr数据复制到新分配的空间。
    if (totlen < sizeof(buf)) {
        payload = (unsigned char*)buf;
    } else {
        payload = zmalloc(totlen);
        memcpy(payload,hdr,sizeof(*hdr));
        hdr = (clusterMsg*) payload;
    }
    // 填充集群PUBLISH消息的channel、message数据
    memcpy(hdr->data.publish.msg.bulk_data,channel->ptr,sdslen(channel->ptr));
    memcpy(hdr->data.publish.msg.bulk_data+sdslen(channel->ptr),
        message->ptr,sdslen(message->ptr));

    // 如果有指定link，则发送给指定节点，否则广播发送整个集群。
    if (link)
        clusterSendMessage(link,payload,totlen);
    else
        clusterBroadcastMessage(payload,totlen);

    // 释放分配的空间
    decrRefCount(channel);
    decrRefCount(message);
    if (payload != (unsigned char*)buf) zfree(payload);
}

/* Send a FAIL message to all the nodes we are able to contact.
 * The FAIL message is sent when we detect that a node is failing
 * (CLUSTER_NODE_PFAIL) and we also receive a gossip confirmation of this:
 * we switch the node state to CLUSTER_NODE_FAIL and ask all the other
 * nodes to do the same ASAP. */
// 向所有我们能通信的节点发送指定节点的FAIL消息。
// 当我们检测到一个节点故障了(CLUSTER_NODE_PFAIL)，并收到大部分master节点gossip消息确认时，
// 我们会将该节点设为CLUSTER_NODE_FAIL状态，并发送FAIL消息给其他所有节点，让他们也尽快按规则标记该节点为FAIL状态。
void clusterSendFail(char *nodename) {
    clusterMsg buf[1];
    clusterMsg *hdr = (clusterMsg*) buf;

    // 根据type构建message header。
    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_FAIL);
    // 填充FAIL消息的节点name信息
    memcpy(hdr->data.fail.about.nodename,nodename,CLUSTER_NAMELEN);
    // 广播消息发送
    clusterBroadcastMessage(buf,ntohl(hdr->totlen));
}

/* Send an UPDATE message to the specified link carrying the specified 'node'
 * slots configuration. The node name, slots bitmap, and configEpoch info
 * are included. */
// 发送UPDATE消息，携带指定节点的slots配置（包含节点name，slots位图，配置纪元）给指定的link。
void clusterSendUpdate(clusterLink *link, clusterNode *node) {
    clusterMsg buf[1];
    clusterMsg *hdr = (clusterMsg*) buf;

    if (link == NULL) return;
    // 构造消息header
    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_UPDATE);
    // 填充节点的name、configEpoch、slots信息。
    memcpy(hdr->data.update.nodecfg.nodename,node->name,CLUSTER_NAMELEN);
    hdr->data.update.nodecfg.configEpoch = htonu64(node->configEpoch);
    memcpy(hdr->data.update.nodecfg.slots,node->slots,sizeof(node->slots));
    // 发送消息给对应link节点
    clusterSendMessage(link,(unsigned char*)buf,ntohl(hdr->totlen));
}

/* Send a MODULE message.
 *
 * If link is NULL, then the message is broadcasted to the whole cluster. */
// 发送module消息。如果link为NULL，消息将广播发送到整个集群。
void clusterSendModule(clusterLink *link, uint64_t module_id, uint8_t type,
                       unsigned char *payload, uint32_t len) {
    unsigned char *heapbuf;
    clusterMsg buf[1];
    clusterMsg *hdr = (clusterMsg*) buf;
    uint32_t totlen;

    // 构建消息header，更新消息总长度
    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_MODULE);
    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    totlen += sizeof(clusterMsgModule) - 3 + len;

    // 填充module信息
    hdr->data.module.msg.module_id = module_id; /* Already endian adjusted. */
    hdr->data.module.msg.type = type;
    hdr->data.module.msg.len = htonl(len);
    hdr->totlen = htonl(totlen);

    /* Try to use the local buffer if possible */
    // 尝试使用本地buf，空间不足则再分配。
    if (totlen < sizeof(buf)) {
        heapbuf = (unsigned char*)buf;
    } else {
        heapbuf = zmalloc(totlen);
        memcpy(heapbuf,hdr,sizeof(*hdr));
        hdr = (clusterMsg*) heapbuf;
    }
    // 填充module数据。
    memcpy(hdr->data.module.msg.bulk_data,payload,len);

    // link不为NULL，则发送到指定节点，否则广播发送到整个集群。
    if (link)
        clusterSendMessage(link,heapbuf,totlen);
    else
        clusterBroadcastMessage(heapbuf,totlen);

    // 释放分配的空间
    if (heapbuf != (unsigned char*)buf) zfree(heapbuf);
}

/* This function gets a cluster node ID string as target, the same way the nodes
 * addresses are represented in the modules side, resolves the node, and sends
 * the message. If the target is NULL the message is broadcasted.
 *
 * The function returns C_OK if the target is valid, otherwise C_ERR is
 * returned. */
// 函数根据target表示的ID获取节点。如果有指定该target，则发送module消息到指定节点，否则广播发送到整个集群。
int clusterSendModuleMessageToTarget(const char *target, uint64_t module_id, uint8_t type, unsigned char *payload, uint32_t len) {
    clusterNode *node = NULL;

    if (target != NULL) {
        node = clusterLookupNode(target);
        if (node == NULL || node->link == NULL) return C_ERR;
    }

    clusterSendModule(target ? node->link : NULL,
                      module_id, type, payload, len);
    return C_OK;
}

/* -----------------------------------------------------------------------------
 * CLUSTER Pub/Sub support
 *
 * For now we do very little, just propagating PUBLISH messages across the whole
 * cluster. In the future we'll try to get smarter and avoiding propagating those
 * messages to hosts without receives for a given channel.
 * -------------------------------------------------------------------------- */
// 集群 Pub/Sub 支持
// 现在做的事情很少，只是传播PUBLISH消息给整个集群。未来可能尝试更智能并避免传播该消息给那些不监听对应channel的节点。
void clusterPropagatePublish(robj *channel, robj *message) {
    clusterSendPublish(NULL, channel, message);
}

/* -----------------------------------------------------------------------------
 * SLAVE node specific functions
 * -------------------------------------------------------------------------- */

/* This function sends a FAILOVER_AUTH_REQUEST message to every node in order to
 * see if there is the quorum for this slave instance to failover its failing
 * master.
 *
 * Note that we send the failover request to everybody, master and slave nodes,
 * but only the masters are supposed to reply to our query. */
// 这个函数想所有的节点发送一个FAILOVER_AUTH_REQUEST消息，最终看是否有quorum数的master节点同意当前节点进行故障转移。
// 注意我们将failover请求发送给了所有的节点（master和slave都发），但是只有master才应该回复请求。
void clusterRequestFailoverAuth(void) {
    clusterMsg buf[1];
    clusterMsg *hdr = (clusterMsg*) buf;
    uint32_t totlen;

    // 构建FAILOVER投票请求消息的header。
    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST);
    /* If this is a manual failover, set the CLUSTERMSG_FLAG0_FORCEACK bit
     * in the header to communicate the nodes receiving the message that
     * they should authorized the failover even if the master is working. */
    // 如果这是手动故障转移，设置FORCEACK标识，从而强制要求收到消息的masters回复我们，即使我们的master是正常的。
    if (server.cluster->mf_end) hdr->mflags[0] |= CLUSTERMSG_FLAG0_FORCEACK;
    // 更新消息总长
    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    hdr->totlen = htonl(totlen);
    // 广播消息
    clusterBroadcastMessage(buf,totlen);
}

/* Send a FAILOVER_AUTH_ACK message to the specified node. */
// 发送故障转移 FAILOVER_AUTH_ACK 投票消息给指定节点
void clusterSendFailoverAuth(clusterNode *node) {
    clusterMsg buf[1];
    clusterMsg *hdr = (clusterMsg*) buf;
    uint32_t totlen;

    if (!node->link) return;
    // 构造消息header，并更新消息长度。
    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK);
    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    hdr->totlen = htonl(totlen);
    // 发送消息
    clusterSendMessage(node->link,(unsigned char*)buf,totlen);
}

/* Send a MFSTART message to the specified node. */
// 发送MFSTART消息到指定的节点
void clusterSendMFStart(clusterNode *node) {
    clusterMsg buf[1];
    clusterMsg *hdr = (clusterMsg*) buf;
    uint32_t totlen;

    if (!node->link) return;
    // 构建message header，更新消息长度。
    clusterBuildMessageHdr(hdr,CLUSTERMSG_TYPE_MFSTART);
    totlen = sizeof(clusterMsg)-sizeof(union clusterMsgData);
    hdr->totlen = htonl(totlen);
    // MFSTART消息，没有额外的消息数据。直接发送。
    clusterSendMessage(node->link,(unsigned char*)buf,totlen);
}

/* Vote for the node asking for our vote if there are the conditions. */
// 如果条件允许，投票支持请求故障转移的节点。
void clusterSendFailoverAuthIfNeeded(clusterNode *node, clusterMsg *request) {
    // 取到请求故障转移节点的master节点
    clusterNode *master = node->slaveof;
    // 取到请求故障转移节点发送过来的集群当前纪元和该节点的配置纪元
    uint64_t requestCurrentEpoch = ntohu64(request->currentEpoch);
    uint64_t requestConfigEpoch = ntohu64(request->configEpoch);
    // 取到请求故障转移节点（或该master）声称所负责的slots信息
    unsigned char *claimed_slots = request->myslots;
    // 判断是否强制当前节点回复（即使master是正常的），主要用于手动故障转移。
    int force_ack = request->mflags[0] & CLUSTERMSG_FLAG0_FORCEACK;
    int j;

    /* IF we are not a master serving at least 1 slot, we don't have the
     * right to vote, as the cluster size in Redis Cluster is the number
     * of masters serving at least one slot, and quorum is the cluster
     * size + 1 */
    // 如果我们是slave，或者是master但不负责slots，那么我们没权进行投票。
    // 因为集群size是负责至少1个slot的masters数量，而quorum数是集群size/2+1。
    if (nodeIsSlave(myself) || myself->numslots == 0) return;

    /* Request epoch must be >= our currentEpoch.
     * Note that it is impossible for it to actually be greater since
     * our currentEpoch was updated as a side effect of receiving this
     * request, if the request epoch was greater. */
    // 请求节点的集群当前纪元应该>=我们知道集群的当前纪元。
    // 注意这里不可能请求纪元>节点集群当前纪元的。因为如果请求纪元更大的话，我们收到消息处理时，会更新我们的集群当前纪元的（此时也只是相等）。
    if (requestCurrentEpoch < server.cluster->currentEpoch) {
        serverLog(LL_WARNING,
            "Failover auth denied to %.40s: reqEpoch (%llu) < curEpoch(%llu)",
            node->name,
            (unsigned long long) requestCurrentEpoch,
            (unsigned long long) server.cluster->currentEpoch);
        return;
    }

    /* I already voted for this epoch? Return ASAP. */
    // 如果当前纪元我们已经投过票了，则尽快返回。
    if (server.cluster->lastVoteEpoch == server.cluster->currentEpoch) {
        serverLog(LL_WARNING,
                "Failover auth denied to %.40s: already voted for epoch %llu",
                node->name,
                (unsigned long long) server.cluster->currentEpoch);
        return;
    }

    /* Node must be a slave and its master down.
     * The master can be non failing if the request is flagged
     * with CLUSTERMSG_FLAG0_FORCEACK (manual failover). */
    // 请求节点必须是slave，且它的master已经down了。
    // 但如果请求被标记为CLUSTERMSG_FLAG0_FORCEACK（处于手动故障转移状态会带这个标识），则主节点可能处于正常状态。
    if (nodeIsMaster(node) || master == NULL ||
        (!nodeFailed(master) && !force_ack))
    {
        // 请求节点不是slave，或请求节点没有master，或者请求节点的master没有down且不是强制要求回复（手动故障转移）。则我们不处理。
        if (nodeIsMaster(node)) {
            serverLog(LL_WARNING,
                    "Failover auth denied to %.40s: it is a master node",
                    node->name);
        } else if (master == NULL) {
            serverLog(LL_WARNING,
                    "Failover auth denied to %.40s: I don't know its master",
                    node->name);
        } else if (!nodeFailed(master)) {
            serverLog(LL_WARNING,
                    "Failover auth denied to %.40s: its master is up",
                    node->name);
        }
        return;
    }

    /* We did not voted for a slave about this master for two
     * times the node timeout. This is not strictly needed for correctness
     * of the algorithm but makes the base case more linear. */
    // 如果在2倍的节点超时时间内，我们为该master节点的slave投过票了，则不再进行投票了。
    // 这对于算法的正确性来说不是严格要求的，但会使基本情况更加线性。
    if (mstime() - node->slaveof->voted_time < server.cluster_node_timeout * 2)
    {
        serverLog(LL_WARNING,
                "Failover auth denied to %.40s: "
                "can't vote about this master before %lld milliseconds",
                node->name,
                (long long) ((server.cluster_node_timeout*2)-
                             (mstime() - node->slaveof->voted_time)));
        return;
    }

    /* The slave requesting the vote must have a configEpoch for the claimed
     * slots that is >= the one of the masters currently serving the same
     * slots in the current configuration. */
    // 请求投票的slave必须拥有声明负责slots的配置纪元，且该配置纪元应该>=当前节点知道的负责相同slots的master节点的配置纪元。
    for (j = 0; j < CLUSTER_SLOTS; j++) {
        // 遍历集群所有slots，如果不在请求节点声明的slots中，则跳过。否则需要进行检测。
        if (bitmapTestBit(claimed_slots, j) == 0) continue;
        // 如果请求节点声明的slot目前没有节点负责，或者负责的节点的配置纪元小于请求节点配置纪元，则请求节点可以接管负责，跳过继续检测下一个slot。
        if (server.cluster->slots[j] == NULL ||
            server.cluster->slots[j]->configEpoch <= requestConfigEpoch)
        {
            continue;
        }
        /* If we reached this point we found a slot that in our current slots
         * is served by a master with a greater configEpoch than the one claimed
         * by the slave requesting our vote. Refuse to vote for this slave. */
        // 如果执行到这里，表示我们找到了一个slot，负责它的master节点的配置纪元>请求节点的配置纪元，则该slave配置不是最新的，拒绝为它投票。
        serverLog(LL_WARNING,
                "Failover auth denied to %.40s: "
                "slot %d epoch (%llu) > reqEpoch (%llu)",
                node->name, j,
                (unsigned long long) server.cluster->slots[j]->configEpoch,
                (unsigned long long) requestConfigEpoch);
        return;
    }

    /* We can vote for this slave. */
    // 到这里我们可以对请求的slave节点进行投票了。
    // 更新当前节点投票的纪元、对该master的slave节点投票时间。并发送投票消息给对应节点。
    server.cluster->lastVoteEpoch = server.cluster->currentEpoch;
    node->slaveof->voted_time = mstime();
    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|CLUSTER_TODO_FSYNC_CONFIG);
    // 发送故障转移投票消息
    clusterSendFailoverAuth(node);
    serverLog(LL_WARNING, "Failover auth granted to %.40s for epoch %llu",
        node->name, (unsigned long long) server.cluster->currentEpoch);
}

/* This function returns the "rank" of this instance, a slave, in the context
 * of its master-slaves ring. The rank of the slave is given by the number of
 * other slaves for the same master that have a better replication offset
 * compared to the local one (better means, greater, so they claim more data).
 *
 * A slave with rank 0 is the one with the greatest (most up to date)
 * replication offset, and so forth. Note that because how the rank is computed
 * multiple slaves may have the same rank, in case they have the same offset.
 *
 * The slave rank is used to add a delay to start an election in order to
 * get voted and replace a failing master. Slaves with better replication
 * offsets are more likely to win. */
// 这个函数返回当前节点作为slave，在自己master的所有slave中的排名。
// 通过对比其他slave宣称的它们复制master的offset来进行排序，offset更大说明拥有更多master复制的数据，更优。
// 注意slave的rank为0，表示它是最好的，具有最新的数据。当有多个slave的offset一致时，它们的排名可能一样。
// slave的rank主要用于在开始选举添加一个相应的延时，排名考前的，加的延时更小，更早发送出去，从而更有机会获胜。
int clusterGetSlaveRank(void) {
    long long myoffset;
    int j, rank = 0;
    clusterNode *master;

    serverAssert(nodeIsSlave(myself));
    master = myself->slaveof;
    if (master == NULL) return 0; /* Never called by slaves without master. */

    // 从当前slave的master连接中获取本节点的复制offset
    myoffset = replicationGetSlaveOffset();
    // 遍历master的所有slave，找出能进行故障转移的节点中，有多少个的复制offset > 本节点的offset，从而得出本节点的排名。
    for (j = 0; j < master->numslaves; j++)
        if (master->slaves[j] != myself &&
            !nodeCantFailover(master->slaves[j]) &&
            master->slaves[j]->repl_offset > myoffset) rank++;
    return rank;
}

/* This function is called by clusterHandleSlaveFailover() in order to
 * let the slave log why it is not able to failover. Sometimes there are
 * not the conditions, but since the failover function is called again and
 * again, we can't log the same things continuously.
 *
 * This function works by logging only if a given set of conditions are
 * true:
 *
 * 1) The reason for which the failover can't be initiated changed.
 *    The reasons also include a NONE reason we reset the state to
 *    when the slave finds that its master is fine (no FAIL flag).
 * 2) Also, the log is emitted again if the master is still down and
 *    the reason for not failing over is still the same, but more than
 *    CLUSTER_CANT_FAILOVER_RELOG_PERIOD seconds elapsed.
 * 3) Finally, the function only logs if the slave is down for more than
 *    five seconds + NODE_TIMEOUT. This way nothing is logged when a
 *    failover starts in a reasonable time.
 *
 * The function is called with the reason why the slave can't failover
 * which is one of the integer macros CLUSTER_CANT_FAILOVER_*.
 *
 * The function is guaranteed to be called only if 'myself' is a slave. */
void clusterLogCantFailover(int reason) {
    char *msg;
    static time_t lastlog_time = 0;
    mstime_t nolog_fail_time = server.cluster_node_timeout + 5000;

    /* Don't log if we have the same reason for some time. */
    if (reason == server.cluster->cant_failover_reason &&
        time(NULL)-lastlog_time < CLUSTER_CANT_FAILOVER_RELOG_PERIOD)
        return;

    server.cluster->cant_failover_reason = reason;

    /* We also don't emit any log if the master failed no long ago, the
     * goal of this function is to log slaves in a stalled condition for
     * a long time. */
    if (myself->slaveof &&
        nodeFailed(myself->slaveof) &&
        (mstime() - myself->slaveof->fail_time) < nolog_fail_time) return;

    switch(reason) {
    case CLUSTER_CANT_FAILOVER_DATA_AGE:
        msg = "Disconnected from master for longer than allowed. "
              "Please check the 'cluster-replica-validity-factor' configuration "
              "option.";
        break;
    case CLUSTER_CANT_FAILOVER_WAITING_DELAY:
        msg = "Waiting the delay before I can start a new failover.";
        break;
    case CLUSTER_CANT_FAILOVER_EXPIRED:
        msg = "Failover attempt expired.";
        break;
    case CLUSTER_CANT_FAILOVER_WAITING_VOTES:
        msg = "Waiting for votes, but majority still not reached.";
        break;
    default:
        msg = "Unknown reason code.";
        break;
    }
    lastlog_time = time(NULL);
    serverLog(LL_WARNING,"Currently unable to failover: %s", msg);
}

/* This function implements the final part of automatic and manual failovers,
 * where the slave grabs its master's hash slots, and propagates the new
 * configuration.
 *
 * Note that it's up to the caller to be sure that the node got a new
 * configuration epoch already. */
// 这个函数实现了自动和手动故障转移的最后处理部分。包括获取master的slots负责处理，传递新的配置。
void clusterFailoverReplaceYourMaster(void) {
    int j;
    clusterNode *oldmaster = myself->slaveof;

    // 如果当前节点已经是master了，或者当前节点不从属于任何master，直接返回。
    if (nodeIsMaster(myself) || oldmaster == NULL) return;

    /* 1) Turn this node into a master. */
    // 处理1：将当前节点设为master。
    clusterSetNodeAsMaster(myself);
    replicationUnsetMaster();

    /* 2) Claim all the slots assigned to our master. */
    // 处理2：对于之前master负责的所有slot，当前节点先对该位重置，然后将slot加入自己负责bitmap中。
    for (j = 0; j < CLUSTER_SLOTS; j++) {
        if (clusterNodeGetSlotBit(oldmaster,j)) {
            clusterDelSlot(j);
            clusterAddSlot(myself,j);
        }
    }

    /* 3) Update state and save config. */
    // 处理3：及时的更新状态并保存配置，不等到before sleep中处理了。
    clusterUpdateState();
    clusterSaveConfigOrDie(1);

    /* 4) Pong all the other nodes so that they can update the state
     *    accordingly and detect that we switched to master role. */
    // 处理4：PONG通知所有其他节点，以便它们可以相应地更新状态，并检测到我们已切换为master。
    clusterBroadcastPong(CLUSTER_BROADCAST_ALL);

    /* 5) If there was a manual failover in progress, clear the state. */
    // 如果当前有一个进行中的手动故障转移，清除相应的状态。
    resetManualFailover();
}

/* This function is called if we are a slave node and our master serving
 * a non-zero amount of hash slots is in FAIL state.
 *
 * The goal of this function is:
 * 1) To check if we are able to perform a failover, is our data updated?
 * 2) Try to get elected by masters.
 * 3) Perform the failover informing all the other nodes.
 */
// 当我们是slave节点，且我们的master有负责slots 而进入FAIL状态时，我们会调用这个函数来进行故障转移操作。
// 这个函数的目标：
// 1、检查是否我们能够执行一次故障转移，是否我们数据是最新的？
// 2、尝试去获得masters的选举投票。
// 3、执行故障转移，并通知所有节点。
void clusterHandleSlaveFailover(void) {
    mstime_t data_age;
    mstime_t auth_age = mstime() - server.cluster->failover_auth_time;
    int needed_quorum = (server.cluster->size / 2) + 1;
    int manual_failover = server.cluster->mf_end != 0 &&
                          server.cluster->mf_can_start;
    mstime_t auth_timeout, auth_retry_time;

    server.cluster->todo_before_sleep &= ~CLUSTER_TODO_HANDLE_FAILOVER;

    /* Compute the failover timeout (the max time we have to send votes
     * and wait for replies), and the failover retry time (the time to wait
     * before trying to get voted again).
     *
     * Timeout is MAX(NODE_TIMEOUT*2,2000) milliseconds.
     * Retry is two times the Timeout.
     */
    // 计算故障转移timeout（我们发送请求投票并等待回复的最大来回时间），该timeout最小值为2s
    // 计算故障转移重试timeout时间（需要等待第一次超时，然后在试一次，所以是2倍的auth_timeout）
    auth_timeout = server.cluster_node_timeout*2;
    if (auth_timeout < 2000) auth_timeout = 2000;
    auth_retry_time = auth_timeout*2;

    /* Pre conditions to run the function, that must be met both in case
     * of an automatic or manual failover:
     * 1) We are a slave.
     * 2) Our master is flagged as FAIL, or this is a manual failover.
     * 3) We don't have the no failover configuration set, and this is
     *    not a manual failover.
     * 4) It is serving slots. */
    // 执行的前提条件，在自动或手动故障转移的情况下都必须满足：
    // 1、当前节点是slave。
    // 2、当前节点的master被标识为FAIL，或者当前是进行手动故障转移。
    // 3、当前节点的配置中没有禁止failover，并且不是在进行手动故障转移。
    // 4、master节点有服务于slots。
    if (nodeIsMaster(myself) ||
        myself->slaveof == NULL ||
        (!nodeFailed(myself->slaveof) && !manual_failover) ||
        (server.cluster_slave_no_failover && !manual_failover) ||
        myself->slaveof->numslots == 0)
    {
        /* There are no reasons to failover, so we set the reason why we
         * are returning without failing over to NONE. */
        // 这些条件下，我们将不会进行故障转移。所以设置reason返回。
        server.cluster->cant_failover_reason = CLUSTER_CANT_FAILOVER_NONE;
        return;
    }

    /* Set data_age to the number of milliseconds we are disconnected from
     * the master. */
    // 设置data_age为我们与master断开连接的时长。
    // 如果目前是Connected状态，则计算的时间为当前时间与上一次交互时间的差值。
    // 如果不是Connected状态，则计算的时间为当前时间与repl_down的时间差值。
    if (server.repl_state == REPL_STATE_CONNECTED) {
        data_age = (mstime_t)(server.unixtime - server.master->lastinteraction)
                   * 1000;
    } else {
        data_age = (mstime_t)(server.unixtime - server.repl_down_since) * 1000;
    }

    /* Remove the node timeout from the data age as it is fine that we are
     * disconnected from our master at least for the time it was down to be
     * flagged as FAIL, that's the baseline. */
    // data_age减去超时时间，因为断开连接后至少经过timeout，才会将master标记为FAIL
    if (data_age > server.cluster_node_timeout)
        data_age -= server.cluster_node_timeout;

    /* Check if our data is recent enough according to the slave validity
     * factor configured by the user.
     *
     * Check bypassed for manual failovers. */
    // 根据用户配置的slave有效性因子，检查当前节点的数据是否足够新。如果是手动故障转移，则绕过这个检查。
    // 如果 data_age > ping周期时间 + 允许超时的最大时间，则数据不够新，不允许进行故障转移。
    if (server.cluster_slave_validity_factor &&
        data_age >
        (((mstime_t)server.repl_ping_slave_period * 1000) +
         (server.cluster_node_timeout * server.cluster_slave_validity_factor)))
    {
        if (!manual_failover) {
            clusterLogCantFailover(CLUSTER_CANT_FAILOVER_DATA_AGE);
            return;
        }
    }

    /* If the previous failover attempt timeout and the retry time has
     * elapsed, we can setup a new one. */
    // 如果上一次尝试故障转移处理超时了，且重试也超时了。我们这里开启一个新的故障转移处理。
    // auth_age = mstime() - server.cluster->failover_auth_time，上一次开启故障转移到现在的时间。
    // auth_retry_time = server.cluster_node_timeout*4，加上了重试处理，相当于2个来回，4次msg超时时间。
    if (auth_age > auth_retry_time) {
        // 新的开启时间，我们留下了500-1000ms的时间，让FAIL消息传递。
        server.cluster->failover_auth_time = mstime() +
            500 + /* Fixed delay of 500 milliseconds, let FAIL msg propagate. */
            random() % 500; /* Random delay between 0 and 500 milliseconds. */
        // 重置请求投票的回复数为0
        server.cluster->failover_auth_count = 0;
        // 当前还未发送请求投票，所以为false
        server.cluster->failover_auth_sent = 0;
        // 获取当前节点在slave的排名
        server.cluster->failover_auth_rank = clusterGetSlaveRank();
        /* We add another delay that is proportional to the slave rank.
         * Specifically 1 second * rank. This way slaves that have a probably
         * less updated replication offset, are penalized. */
        // 这里添加一个与slave排名成比例的延时（rank*1s）。这样offset更大的节点更容易胜选。
        server.cluster->failover_auth_time +=
            server.cluster->failover_auth_rank * 1000;
        /* However if this is a manual failover, no delay is needed. */
        // 这里如果是一个手动的故障转移，我们不添加delay处理。
        if (server.cluster->mf_end) {
            server.cluster->failover_auth_time = mstime();
            server.cluster->failover_auth_rank = 0;
	    clusterDoBeforeSleep(CLUSTER_TODO_HANDLE_FAILOVER);
        }
        serverLog(LL_WARNING,
            "Start of election delayed for %lld milliseconds "
            "(rank #%d, offset %lld).",
            server.cluster->failover_auth_time - mstime(),
            server.cluster->failover_auth_rank,
            replicationGetSlaveOffset());
        /* Now that we have a scheduled election, broadcast our offset
         * to all the other slaves so that they'll updated their offsets
         * if our offset is better. */
        // 由于我们有一个预计划的选举，广播我们的offset给所有的其他slaves，这样它们好更新我们的offset。
        clusterBroadcastPong(CLUSTER_BROADCAST_LOCAL_SLAVES);
        return;
    }

    /* It is possible that we received more updated offsets from other
     * slaves for the same master since we computed our election delay.
     * Update the delay if our rank changed.
     *
     * Not performed if this is a manual failover. */
    // 因为我们前面计算了我们的选举延时，到这里可能有其他的slaves同样发送消息让我们更新他们的offset。
    // 所以这里我们需要更新rank，重新计算delay。注意对于手动故障转移，我们不增加延时，所以不需要这里进行处理。
    if (server.cluster->failover_auth_sent == 0 &&
        server.cluster->mf_end == 0)
    {
        // 还没有发送投票请求，且不是手动故障转移。重新获取newrank，当newrank更大时，我们要增加更多的延时。
        int newrank = clusterGetSlaveRank();
        if (newrank > server.cluster->failover_auth_rank) {
            long long added_delay =
                (newrank - server.cluster->failover_auth_rank) * 1000;
            server.cluster->failover_auth_time += added_delay;
            server.cluster->failover_auth_rank = newrank;
            serverLog(LL_WARNING,
                "Replica rank updated to #%d, added %lld milliseconds of delay.",
                newrank, added_delay);
        }
    }

    /* Return ASAP if we can't still start the election. */
    // 到这里如果本节点开始选举投票请求的时间没有到的话，尽早返回。
    if (mstime() < server.cluster->failover_auth_time) {
        clusterLogCantFailover(CLUSTER_CANT_FAILOVER_WAITING_DELAY);
        return;
    }

    /* Return ASAP if the election is too old to be valid. */
    // 如果选举已经超时了，那么尽早返回。
    if (auth_age > auth_timeout) {
        clusterLogCantFailover(CLUSTER_CANT_FAILOVER_EXPIRED);
        return;
    }

    /* Ask for votes if needed. */
    // 故障转移处理，当前slave节点请求其他master节点投票自己。
    if (server.cluster->failover_auth_sent == 0) {
        // 故障转移请求投票时，总是将集群的当前纪元+1。使用新的纪元来开启故障转移。
        server.cluster->currentEpoch++;
        server.cluster->failover_auth_epoch = server.cluster->currentEpoch;
        serverLog(LL_WARNING,"Starting a failover election for epoch %llu.",
            (unsigned long long) server.cluster->currentEpoch);
        // 向所有节点发送FAILOVE_AUTH_REQUEST消息，但只有master节点会回复。
        clusterRequestFailoverAuth();
        // failover_auth_sent置为1表示我们已经发送了投票请求。
        server.cluster->failover_auth_sent = 1;
        // 需要在下一轮before sleep中更新集群状态，保存及持久化配置。
        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|
                             CLUSTER_TODO_UPDATE_STATE|
                             CLUSTER_TODO_FSYNC_CONFIG);
        // 发送投票请求后，直接返回，等待master节点回复后，下一轮再处理后面票数判断。
        return; /* Wait for replies. */
    }

    /* Check if we reached the quorum. */
    // 检查我们收到的票数是否达到了quorum数
    if (server.cluster->failover_auth_count >= needed_quorum) {
        /* We have the quorum, we can finally failover the master. */
        // 如果我们达到了quorum数支持，我们能够进行故障转移了。

        serverLog(LL_WARNING,
            "Failover election won: I'm the new master.");

        /* Update my configEpoch to the epoch of the election. */
        // 如果configEpoch小于本次选举的纪元，则更新当前节点的configEpoch为这个选举的epoch纪元。
        if (myself->configEpoch < server.cluster->failover_auth_epoch) {
            myself->configEpoch = server.cluster->failover_auth_epoch;
            serverLog(LL_WARNING,
                "configEpoch set to %llu after successful failover",
                (unsigned long long) myself->configEpoch);
        }

        /* Take responsibility for the cluster slots. */
        // 替换master来对slots进行负责
        clusterFailoverReplaceYourMaster();
    } else {
        // 如果没有达到quorum数，记录日志，下一轮再看。
        clusterLogCantFailover(CLUSTER_CANT_FAILOVER_WAITING_VOTES);
    }
}

/* -----------------------------------------------------------------------------
 * CLUSTER slave migration
 *
 * Slave migration is the process that allows a slave of a master that is
 * already covered by at least another slave, to "migrate" to a master that
 * is orphaned, that is, left with no working slaves.
 * ------------------------------------------------------------------------- */

/* This function is responsible to decide if this replica should be migrated
 * to a different (orphaned) master. It is called by the clusterCron() function
 * only if:
 *
 * 1) We are a slave node.
 * 2) It was detected that there is at least one orphaned master in
 *    the cluster.
 * 3) We are a slave of one of the masters with the greatest number of
 *    slaves.
 *
 * This checks are performed by the caller since it requires to iterate
 * the nodes anyway, so we spend time into clusterHandleSlaveMigration()
 * if definitely needed.
 *
 * The function is called with a pre-computed max_slaves, that is the max
 * number of working (not in FAIL state) slaves for a single master.
 *
 * Additional conditions for migration are examined inside the function.
 */
// slave迁移，允许某个master（还有其他的slave）的某个slave迁移到孤立的master（没有有效的slaves）。
// 这个函数负责决定是否当前slave应该被迁移到其他的(孤立)master。仅在如下条件时由clusterCron()调用：
//  1、我们是slave节点。
//  2、检测到集群中至少有一个孤立的master节点。
//  3、我们是slaves数量最多的master的slave。
// 上面这些条件检查是由调用者处理的，因为当前函数无论如何都需要迭代所有节点，所有我们只有在需要的时候才会执行这个函数。
// 另外函数内部也会进一步检查其他迁移条件。函数参数max_slaves，是预先计算的单个master当前最大可用slaves数量。
void clusterHandleSlaveMigration(int max_slaves) {
    int j, okslaves = 0;
    clusterNode *mymaster = myself->slaveof, *target = NULL, *candidate = NULL;
    dictIterator *di;
    dictEntry *de;

    /* Step 1: Don't migrate if the cluster state is not ok. */
    // 1、如果集群当前状态不是OK，则不做迁移。
    if (server.cluster->state != CLUSTER_OK) return;

    /* Step 2: Don't migrate if my master will not be left with at least
     *         'migration-barrier' slaves after my migration. */
    // 2、如果我们迁移后，我们的master节点剩余的可用slaves数量小于配置的migration-barrier时，则不能进行迁移。
    if (mymaster == NULL) return;
    for (j = 0; j < mymaster->numslaves; j++)
        // 遍历统计我们master的可以slaves数量，与配置的migration_barrier数比对。
        if (!nodeFailed(mymaster->slaves[j]) &&
            !nodeTimedOut(mymaster->slaves[j])) okslaves++;
    if (okslaves <= server.cluster_migration_barrier) return;

    /* Step 3: Identify a candidate for migration, and check if among the
     * masters with the greatest number of ok slaves, I'm the one with the
     * smallest node ID (the "candidate slave").
     *
     * Note: this means that eventually a replica migration will occur
     * since slaves that are reachable again always have their FAIL flag
     * cleared, so eventually there must be a candidate. At the same time
     * this does not mean that there are no race conditions possible (two
     * slaves migrating at the same time), but this is unlikely to
     * happen, and harmless when happens. */
    // 3、确定迁移的候选者，检查是否在具有最大数量的可用slaves的masters中，我们是所有masters的slaves中ID最小的那个。
    // 注意：这意味着最终会发生slave迁移，因为salves再次可访问时总会清除它的FAIL标志，所以最终必须会有一个候选者。
    //  同时，这并不意味着没有可能的争用情形（两个slave同时迁移），但这不太可能发生，并且发生时也是无害的。
    candidate = myself;
    // 默认candidate为当前节点，然后遍历所有master节点，找到孤立节点即为我们的target。
    // 找到slaves总数与max_slaves相等的masters节点，然后遍历这些masters节点的所有slaves，找出最小ID的slave作为candidate。
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        int okslaves = 0, is_orphaned = 1;

        /* We want to migrate only if this master is working, orphaned, and
         * used to have slaves or if failed over a master that had slaves
         * (MIGRATE_TO flag). This way we only migrate to instances that were
         * supposed to have replicas. */
        // 只有该节点是master，且是正常的（不是FAIL），孤立的（当前没有可用slaves），
        // 并且之前有过可用slaves或者故障转移对应的master有过slaves时（即具有MIGRATE_TO标识），它才是我们迁移的目标。
        // 这样我们只会迁移到期望有slaves的master去。
        if (nodeIsSlave(node) || nodeFailed(node)) is_orphaned = 0;
        if (!(node->flags & CLUSTER_NODE_MIGRATE_TO)) is_orphaned = 0;

        /* Check number of working slaves. */
        // 检查该节点的可用slaves数量。如果大于0，则该节点不是孤立master，不需要迁移过去。
        if (nodeIsMaster(node)) okslaves = clusterCountNonFailingSlaves(node);
        if (okslaves > 0) is_orphaned = 0;

        if (is_orphaned) {
            // 该节点是孤立的master。如果我们当前还没有迁移目标，且该节点有负责slots，则这个节点即为我们迁移目标。
            if (!target && node->numslots > 0) target = node;

            /* Track the starting time of the orphaned condition for this
             * master. */
            // 如果该节点之前没有设置孤立时间，则这里设置孤立开始时间为当前时间。
            if (!node->orphaned_time) node->orphaned_time = mstime();
        } else {
            // 不是孤立master，置空该节点的孤立时间。
            node->orphaned_time = 0;
        }

        /* Check if I'm the slave candidate for the migration: attached
         * to a master with the maximum number of slaves and with the smallest
         * node ID. */
        // 检查是否我们应该作为迁移的候选者：
        // 候选者的master应该是有最大数量的slaves，且候选者的ID应该是 具有最大可以slaves数量的masters 的所有slaves中最小的。
        if (okslaves == max_slaves) {
            // 如果当前节点具有最大数量的slaves数，那么遍历节点的slaves，看ID是否小于当前candidate的ID，小于则替换candidate。
            for (j = 0; j < node->numslaves; j++) {
                if (memcmp(node->slaves[j]->name,
                           candidate->name,
                           CLUSTER_NAMELEN) < 0)
                {
                    candidate = node->slaves[j];
                }
            }
        }
    }
    dictReleaseIterator(di);

    /* Step 4: perform the migration if there is a target, and if I'm the
     * candidate, but only if the master is continuously orphaned for a
     * couple of seconds, so that during failovers, we give some time to
     * the natural slaves of this instance to advertise their switch from
     * the old master to the new one. */
    // 4、如果有迁移目标master，并且我们是候选者，满足了条件，但是我们还是仅在target成为孤立master一段时间后才执行迁移。
    // 因为在故障转移期间，我们需要给target节点的原自然slaves一定时间来通告他们从老的master切换到了新的master了。
    // 另外如果有module禁止了故障转移操作，我们也是不会进行迁移到新的master的。
    if (target && candidate == myself &&
        (mstime()-target->orphaned_time) > CLUSTER_SLAVE_MIGRATION_DELAY &&
       !(server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_FAILOVER))
    {
        serverLog(LL_WARNING,"Migrating to orphaned master %.40s",
            target->name);
        // 将target设置为我们的master。
        clusterSetMaster(target);
    }
}

/* -----------------------------------------------------------------------------
 * CLUSTER manual failover
 *
 * This are the important steps performed by slaves during a manual failover:
 * 1) User send CLUSTER FAILOVER command. The failover state is initialized
 *    setting mf_end to the millisecond unix time at which we'll abort the
 *    attempt.
 * 2) Slave sends a MFSTART message to the master requesting to pause clients
 *    for two times the manual failover timeout CLUSTER_MF_TIMEOUT.
 *    When master is paused for manual failover, it also starts to flag
 *    packets with CLUSTERMSG_FLAG0_PAUSED.
 * 3) Slave waits for master to send its replication offset flagged as PAUSED.
 * 4) If slave received the offset from the master, and its offset matches,
 *    mf_can_start is set to 1, and clusterHandleSlaveFailover() will perform
 *    the failover as usually, with the difference that the vote request
 *    will be modified to force masters to vote for a slave that has a
 *    working master.
 *
 * From the point of view of the master things are simpler: when a
 * PAUSE_CLIENTS packet is received the master sets mf_end as well and
 * the sender in mf_slave. During the time limit for the manual failover
 * the master will just send PINGs more often to this slave, flagged with
 * the PAUSED flag, so that the slave will set mf_master_offset when receiving
 * a packet from the master with this flag set.
 *
 * The gaol of the manual failover is to perform a fast failover without
 * data loss due to the asynchronous master-slave replication.
 * -------------------------------------------------------------------------- */
// 手动故障转移的总体目标是执行快速故障转移，而不会由于异步主从复制丢失数据。
// slaves处理集群手动故障转移的重要步骤：
// 1、用户发送CLUSTER FAILOVER命令。故障转移状态初始化，设置mf_end为处理截止时间，达到该时间会abort。
// 2、slave发送 MFSTART 消息给master，请求master暂停所有client写 2*CLUSTER_MF_TIMEOUT 时间。
//  如果master因为手动故障转移而暂停，它也会发送ping消息告知该slave当前复制offset，同时也会传递CLUSTERMSG_FLAG0_PAUSED标识。
// 3、slave等待master发送复制offset。如果有带CLUSTERMSG_FLAG0_PAUSED标识，则说明是故障转移发送的包。
// 4、如果slave收到了master的offset，并且slave处理到的offset与它一致，则将mf_can_start设为1。
//  从而clusterHandleSlaveFailover()就可以跟正常故障转移一样处理了。
//  与正常故障转移不同的是，slave发送投票请求时，会强制要求其他master进行投票，即使slave的master处于正常状态。

// 在master视角，事情处理很简单：当收到slave的MFSTART（PAUSE_CLIENTS）消息时，设置mf_end时间，并将mf_slave设置为sender。
// 在手动故障转移期间（mf_end前），master将更频繁发送PINGs消息（附带FLAG0_PAUSED标识）给该slave，从而该slave能设置mf_master_offset。

/* Reset the manual failover state. This works for both masters and slaves
 * as all the state about manual failover is cleared.
 *
 * The function can be used both to initialize the manual failover state at
 * startup or to abort a manual failover in progress. */
// 重置手动故障转移状态。这对master和slave都有效，因为所有关于手动故障转移的状态都被清除了。
// 这个函数可以在启动故障转移时初始化故障转移的状态；也可以用于中止进行中的故障转移操作，恢复状态。
void resetManualFailover(void) {
    if (server.cluster->mf_end) {
        checkClientPauseTimeoutAndReturnIfPaused();
    }
    server.cluster->mf_end = 0; /* No manual failover in progress. */
    server.cluster->mf_can_start = 0;
    server.cluster->mf_slave = NULL;
    server.cluster->mf_master_offset = -1;
}

/* If a manual failover timed out, abort it. */
// 检查手动故障转移是否超时，超时则reset。
void manualFailoverCheckTimeout(void) {
    if (server.cluster->mf_end && server.cluster->mf_end < mstime()) {
        serverLog(LL_WARNING,"Manual failover timed out.");
        resetManualFailover();
    }
}

/* This function is called from the cluster cron function in order to go
 * forward with a manual failover state machine. */
// 判断是否需要触发手动故障转移，并更新相关状态。主要就是等待slave处理到复制offset与master传递的一致，才能开始进行故障转移处理。
void clusterHandleManualFailover(void) {
    /* Return ASAP if no manual failover is in progress. */
    // 如果不在处理手动故障转移，尽早返回。
    if (server.cluster->mf_end == 0) return;

    /* If mf_can_start is non-zero, the failover was already triggered so the
     * next steps are performed by clusterHandleSlaveFailover(). */
    // 如果mf_can_start!=0，表示故障转移已经触发了，下一步就是执行clusterHandleSlaveFailover()处理故障转移操作了。
    if (server.cluster->mf_can_start) return;

    // 当前slave如果没有收到故障转移master offset，需要等待offset才能继续故障转移。
    if (server.cluster->mf_master_offset == -1) return; /* Wait for offset... */

    // 如果收到的故障转移master offset与当前节点处理的offset一致，则当前slave能够开启故障转移操作。
    if (server.cluster->mf_master_offset == replicationGetSlaveOffset()) {
        /* Our replication offset matches the master replication offset
         * announced after clients were paused. We can start the failover. */
        // 我们的复制偏移量与客户端暂停后宣布的复制偏移量一致。 则可以开始故障转移。
        server.cluster->mf_can_start = 1;
        serverLog(LL_WARNING,
            "All master replication stream processed, "
            "manual failover can start.");
        // before sleep中处理故障转移
        clusterDoBeforeSleep(CLUSTER_TODO_HANDLE_FAILOVER);
        return;
    }
    // offset不一致，加上标识，等待再次进入这个函数。其实就是等待本slave节点处理数据到offset与master一致时，才能开始进行故障转移处理。
    clusterDoBeforeSleep(CLUSTER_TODO_HANDLE_MANUALFAILOVER);
}

/* -----------------------------------------------------------------------------
 * CLUSTER cron job
 * -------------------------------------------------------------------------- */

/* This is executed 10 times every second */
// 集群的异步任务，每秒执行10次
void clusterCron(void) {
    dictIterator *di;
    dictEntry *de;
    // 标识是否需要更新集群状态
    int update_state = 0;
    // 哪些masters没有slaves
    int orphaned_masters; /* How many masters there are without ok slaves. */
    // 单个master拥有的good slaves的最大数量
    int max_slaves; /* Max number of ok slaves for a single master. */
    // 我们的master所拥有的good slaves数量
    int this_slaves; /* Number of ok slaves for our master (if we are slave). */
    // min_pong，min_pong_node 用于抽样选择最久没有收到回复的节点发送ping
    mstime_t min_pong = 0, now = mstime();
    clusterNode *min_pong_node = NULL;
    static unsigned long long iteration = 0;
    mstime_t handshake_timeout;

    // 统计当前这个函数执行的总次数
    iteration++; /* Number of times this function was called so far. */

    /* We want to take myself->ip in sync with the cluster-announce-ip option.
     * The option can be set at runtime via CONFIG SET, so we periodically check
     * if the option changed to reflect this into myself->ip. */
    // 我们想要使我们的 myself->ip 与 cluster-announce-ip 选项同步。
    // 这个选项能在运行期间通过CONFIG SET来设置，所以我们要定期的检查这个选项是否改变了，从而更新进myself->ip。
    {
        // 更新mysql->ip
        static char *prev_ip = NULL;
        char *curr_ip = server.cluster_announce_ip;
        int changed = 0;

        // cluster_announce_ip改变有以下几种情况：
        // 1、如果之前ip为NULL，且现在cluster_announce_ip不为NULL
        // 2、如果之前ip不为NULL，且现在cluster_announce_ip为NULL。
        // 3、两个都不为NULL，且两个不一样。
        if (prev_ip == NULL && curr_ip != NULL) changed = 1;
        else if (prev_ip != NULL && curr_ip == NULL) changed = 1;
        else if (prev_ip && curr_ip && strcmp(prev_ip,curr_ip)) changed = 1;

        // 如果cluster_announce_ip有变化，这里需要更新mysql->ip
        if (changed) {
            // 先更新prev_ip
            if (prev_ip) zfree(prev_ip);
            prev_ip = curr_ip;

            // 更新mysql->ip
            if (curr_ip) {
                /* We always take a copy of the previous IP address, by
                 * duplicating the string. This way later we can check if
                 * the address really changed. */
                // 我们总是对previous IP进行copy操作来更新mysql->ip，这样我们后面就能够检查address是否真的改变了。
                prev_ip = zstrdup(prev_ip);
                strncpy(myself->ip,server.cluster_announce_ip,NET_IP_STR_LEN);
                myself->ip[NET_IP_STR_LEN-1] = '\0';
            } else {
                // 新的ip为NULL，这里设置为空字符串，强制自动检测。
                myself->ip[0] = '\0'; /* Force autodetection. */
            }
        }
    }

    /* The handshake timeout is the time after which a handshake node that was
     * not turned into a normal node is removed from the nodes. Usually it is
     * just the NODE_TIMEOUT value, but when NODE_TIMEOUT is too small we use
     * the value of 1 second. */
    // 当一个handshake状态的节点没有在超时时间内转变为一个正常节点，将被从集群节点列表中移除。
    // 超时时间通常设置为与NODE_TIMEOUT一致，但是当NODE_TIMEOUT太小（小于1s）时，我们会设置handshake timeout为1s
    handshake_timeout = server.cluster_node_timeout;
    if (handshake_timeout < 1000) handshake_timeout = 1000;

    /* Update myself flags. */
    // 更新集群状态中myself的标识，主要是nofailover标识。
    clusterUpdateMyselfFlags();

    /* Check if we have disconnected nodes and re-establish the connection.
     * Also update a few stats while we are here, that can be used to make
     * better decisions in other part of the code. */
    // 检查是否有断开连接的节点，并重新建立连接。因为读写出错或其他原因，我们经常会调用freeClusterLink释放link，这里要重新连接。
    // 同时也更新一些统计信息，主要用于在其他地方的代码中更好的做决定。
    di = dictGetSafeIterator(server.cluster->nodes);
    server.cluster->stats_pfail_nodes = 0;
    // 遍历server.cluster->nodes，即集群中的所有节点，进行检查
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        /* Not interested in reconnecting the link with myself or nodes
         * for which we have no address. */
        // 如果当前遍历的节点是myself，或者我们没有地址信息。则跳过
        if (node->flags & (CLUSTER_NODE_MYSELF|CLUSTER_NODE_NOADDR)) continue;

        // 如果节点当前遍历节点是CLUSTER_NODE_PFAIL状态，则记录cluster中pfail节点的数+1
        if (node->flags & CLUSTER_NODE_PFAIL)
            server.cluster->stats_pfail_nodes++;

        /* A Node in HANDSHAKE state has a limited lifespan equal to the
         * configured node timeout. */
        // 如果节点在HANDSHAKE状态，且处于该状态的时间超过了timeout时间，则将该节点从集群节点集合中删除。然后再处理下一个节点。
        if (nodeInHandshake(node) && now - node->ctime > handshake_timeout) {
            clusterDelNode(node);
            continue;
        }

        // 如果节点的link为NULL，说明本实例与对应节点断开了连接，需要重新连接
        if (node->link == NULL) {
            // 创建link指向该node
            clusterLink *link = createClusterLink(node);
            // 为link创建底层conn
            link->conn = server.tls_cluster ? connCreateTLS() : connCreateSocket();
            // link作为conn私有数据，从而可以直接从conn拿到link。
            connSetPrivateData(link->conn, link);
            // 建立conn跟对应node进行连接。
            // 非阻塞方式，监听socket的可写事件，可写时执行socket的ae_handler，最终调用clusterLinkConnectHandler来处理连接。
            if (connConnect(link->conn, node->ip, node->cport, NET_FIRST_BIND_ADDR,
                        clusterLinkConnectHandler) == -1) {
                /* We got a synchronous error from connect before
                 * clusterSendPing() had a chance to be called.
                 * If node->ping_sent is zero, failure detection can't work,
                 * so we claim we actually sent a ping now (that will
                 * be really sent as soon as the link is obtained). */
                // 在clusterSendPing()有机会被调用之前，我们这里收到了一个同步的error。
                // 如果node->ping_sent为0的话，故障检测将无法工作。
                // 所以这里我们声明当前已经发送了ping，即设置ping_sent为now（实际上只要我们后面获得了link，就会尽快的真的发送ping）。
                if (node->ping_sent == 0) node->ping_sent = mstime();
                serverLog(LL_DEBUG, "Unable to connect to "
                    "Cluster Node [%s]:%d -> %s", node->ip,
                    node->cport, server.neterr);

                freeClusterLink(link);
                continue;
            }
            // 设置对应节点的link为新建立的连接
            node->link = link;
        }
    }
    dictReleaseIterator(di);

    /* Ping some random node 1 time every 10 iterations, so that we usually ping
     * one random node every second. */
    // 每10次迭代，即每1s，随机挑5个节点，向其中通信时间间隔最久的节点发送ping信息。
    if (!(iteration % 10)) {
        int j;

        /* Check a few random nodes and ping the one with the oldest
         * pong_received time. */
        // 随机取5个节点，选择pong_received最早的那个节点发送ping
        for (j = 0; j < 5; j++) {
            de = dictGetRandomKey(server.cluster->nodes);
            clusterNode *this = dictGetVal(de);

            /* Don't ping nodes disconnected or with a ping currently active. */
            // 如果连接不存在，或者正在向这个节点发送ping，则跳过
            if (this->link == NULL || this->ping_sent != 0) continue;
            // 如果节点是自己，或者节点刚加入在等待第一次的握手，跳过
            if (this->flags & (CLUSTER_NODE_MYSELF|CLUSTER_NODE_HANDSHAKE))
                continue;
            // 更新最早未收到回复的节点及对应时间
            if (min_pong_node == NULL || min_pong > this->pong_received) {
                min_pong_node = this;
                min_pong = this->pong_received;
            }
        }
        // 找到最早未收到回复的节点，发送ping
        if (min_pong_node) {
            serverLog(LL_DEBUG,"Pinging node %.40s", min_pong_node->name);
            clusterSendPing(min_pong_node->link, CLUSTERMSG_TYPE_PING);
        }
    }

    /* Iterate nodes to check if we need to flag something as failing.
     * This loop is also responsible to:
     * 1) Check if there are orphaned masters (masters without non failing
     *    slaves).
     * 2) Count the max number of non failing slaves for a single master.
     * 3) Count the number of slaves for our master, if we are a slave. */
    // 迭代所有节点，检查节点是否需要被标识为failing。在循环期间同样会进行以下处理：
    // 1、检查是否有孤点master，即没有可用的slaves
    // 2、计算单个master的最大可用slaves数
    // 3、计算当前自己master节点的slaves数
    orphaned_masters = 0;
    max_slaves = 0;
    this_slaves = 0;
    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        // 遍历节点
        clusterNode *node = dictGetVal(de);
        now = mstime(); /* Use an updated time at every iteration. */

        // 如果节点是自己，或没有地址，或正在进行第一次握手。跳过
        if (node->flags &
            (CLUSTER_NODE_MYSELF|CLUSTER_NODE_NOADDR|CLUSTER_NODE_HANDSHAKE))
                continue;

        /* Orphaned master check, useful only if the current instance
         * is a slave that may migrate to another master. */
        // 检查节点是否是孤立的master，仅用于当前节点是slave且可以迁移到其他的master节点。
        // 因为如果当前节点是master，则master是不能迁移作为孤立节点的slave的，所以如果mysql是master则不需要检查孤立master。
        if (nodeIsSlave(myself) && nodeIsMaster(node) && !nodeFailed(node)) {
            // 获取指定节点正常可用的slave数量
            int okslaves = clusterCountNonFailingSlaves(node);

            /* A master is orphaned if it is serving a non-zero number of
             * slots, have no working slaves, but used to have at least one
             * slave, or failed over a master that used to have slaves. */
            // 一个master变成孤立需要满足以下条件：
            // 1、服务的slots数量必须大于0。
            // 2、目前没有可用的slaves，但是过去有至少一个slave；或者对过去拥有slaves的master进行故障转移。
            if (okslaves == 0 && node->numslots > 0 &&
                node->flags & CLUSTER_NODE_MIGRATE_TO)
            {
                // 如果节点有负责slots，没有可用slave但有资格进行slave迁入，则该节点是孤立master。
                orphaned_masters++;
            }
            // 更新单个节点的最大可用slaves数
            if (okslaves > max_slaves) max_slaves = okslaves;
            // 如果当前本机节点是slave，而遍历到的节点是master，则更新当前自己master节点的可用slaves数
            if (nodeIsSlave(myself) && myself->slaveof == node)
                this_slaves = okslaves;
        }

        /* If we are not receiving any data for more than half the cluster
         * timeout, reconnect the link: maybe there is a connection
         * issue even if the node is alive. */
        // 对于遍历到的节点，如果我们在大于集群timeout/2期间都没有收到该节点的任何数据，则需要重新建立连接。
        // 因为即使节点是活跃状态，但连接可能有问题。
        // 所以这里释放连接置空，等待下一轮定时任务执行时，在这个循环的开始检查link为NULL再重连。
        mstime_t ping_delay = now - node->ping_sent;
        mstime_t data_delay = now - node->data_received;
        // 节点是连接状态 && 没有在进行重连 && 我们发送了PING && PING回复延时超过了timeout/2 && timeout/2时间内也没收到任何数据
        // 这种情况是需要释放连接，等待自动重连
        if (node->link && /* is connected */
            now - node->link->ctime >
            server.cluster_node_timeout && /* was not already reconnected */
            node->ping_sent && /* we already sent a ping */
            /* and we are waiting for the pong more than timeout/2 */
            ping_delay > server.cluster_node_timeout/2 &&
            /* and in such interval we are not seeing any traffic at all. */
            data_delay > server.cluster_node_timeout/2)
        {
            /* Disconnect the link, it will be reconnected automatically. */
            // 释放连接，等待下一轮定时任务，来处理重连
            freeClusterLink(node->link);
        }

        /* If we have currently no active ping in this instance, and the
         * received PONG is older than half the cluster timeout, send
         * a new ping now, to ensure all the nodes are pinged without
         * a too big delay. */
        // 如果我们当前没有正在ping这个节点，而最近收到PONG的时间到现在已经超过的timeout/2，
        // 则主动发送PING，即主动进行通信，确保所有节点之间通信时间间隔不会过长。
        if (node->link &&
            node->ping_sent == 0 &&
            (now - node->pong_received) > server.cluster_node_timeout/2)
        {
            clusterSendPing(node->link, CLUSTERMSG_TYPE_PING);
            continue;
        }

        /* If we are a master and one of the slaves requested a manual
         * failover, ping it continuously. */
        // 如果集群正处于故障转移期间，且当前处理的节点正是在进行手动故障的slave节点，而如果我们是master，则这里ping一下该slave。
        // 只有集群中的master才会进行手动故障转移的投票处理工作。
        if (server.cluster->mf_end &&
            nodeIsMaster(myself) &&
            server.cluster->mf_slave == node &&
            node->link)
        {
            clusterSendPing(node->link, CLUSTERMSG_TYPE_PING);
            continue;
        }

        /* Check only if we have an active ping for this instance. */
        // 仅在我们有正在发送ping，且没有收到回复时才做后面的检查。根据上一次交互时间判断是否将该节点加入PFAIL状态。
        if (node->ping_sent == 0) continue;

        /* Check if this node looks unreachable.
         * Note that if we already received the PONG, then node->ping_sent
         * is zero, so can't reach this code at all, so we don't risk of
         * checking for a PONG delay if we didn't sent the PING.
         *
         * We also consider every incoming data as proof of liveness, since
         * our cluster bus link is also used for data: under heavy data
         * load pong delays are possible. */
        // 检查节点是否看起来不可达。
        // 注意如果我们收到了PONG，则node->ping_sent为0，此时没有正在发送的ping，我们的代码是到不了这里的。
        // 所以我们不用管没有发送PING的情况，这里这样检查PONG延时是没有风险的。
        // 使用ping或data最近的一次交互到现在的时间间隔作为计算的延时。
        mstime_t node_delay = (ping_delay < data_delay) ? ping_delay :
                                                          data_delay;

        // 如果delay时间大于cluster_node_timeout，认为possibly failing
        if (node_delay > server.cluster_node_timeout) {
            /* Timeout reached. Set the node as possibly failing if it is
             * not already in this state. */
            // 达到超时时间，且节点不是PFAIL或FAIL状态，则置该节点为PFAIL，并且需要更新集群状态
            if (!(node->flags & (CLUSTER_NODE_PFAIL|CLUSTER_NODE_FAIL))) {
                serverLog(LL_DEBUG,"*** NODE %.40s possibly failing",
                    node->name);
                node->flags |= CLUSTER_NODE_PFAIL;
                update_state = 1;
            }
        }
    }
    dictReleaseIterator(di);

    /* If we are a slave node but the replication is still turned off,
     * enable it if we know the address of our master and it appears to
     * be up. */
    // 如果当前节点是slave，且数据复制处于关闭状态。则我们需要根据地址关联到master并启动复制。
    if (nodeIsSlave(myself) &&
        server.masterhost == NULL &&
        myself->slaveof &&
        nodeHasAddr(myself->slaveof))
    {
        replicationSetMaster(myself->slaveof->ip, myself->slaveof->port);
    }

    /* Abort a manual failover if the timeout is reached. */
    // 检查手动故障转移是否超时，超时就abort
    manualFailoverCheckTimeout();

    // 如果当前是slave，查看当前集群故障转移状态，并进行相应处理。
    if (nodeIsSlave(myself)) {
        // 设置手动故障转移相关状态，从而在before sleep中做一些处理
        clusterHandleManualFailover();
        // 如果module没有禁止故障转移，这里执行slave故障转移操作
        if (!(server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_FAILOVER))
            clusterHandleSlaveFailover();
        /* If there are orphaned slaves, and we are a slave among the masters
         * with the max number of non-failing slaves, consider migrating to
         * the orphaned masters. Note that it does not make sense to try
         * a migration if there is no master with at least *two* working
         * slaves. */
        // 如果有孤立的master，并且在所有masters中，我们master拥有最大数量的ok slaves。考虑把自己迁移到孤立的master去。
        // 注意如果所有master的ok slaves的数量小于2时，将不会做任何迁移操作。
        if (orphaned_masters && max_slaves >= 2 && this_slaves == max_slaves &&
		server.cluster_allow_replica_migration)
            clusterHandleSlaveMigration(max_slaves);
    }

    // 处理集群状态的更新
    if (update_state || server.cluster->state == CLUSTER_FAIL)
        clusterUpdateState();
}

/* This function is called before the event handler returns to sleep for
 * events. It is useful to perform operations that must be done ASAP in
 * reaction to events fired but that are not safe to perform inside event
 * handlers, or to perform potentially expansive tasks that we need to do
 * a single time before replying to clients. */
// 该函数在事件循环的before sleep中执行。这在执行一些操作时非常有用。
// 如需要尽早响应处理触发的事件，但这些任务在事件处理函数内部执行并不安全，此时就可以使用放到这个函数中来处理。
// 另外如果在回复client之前需要执行一次潜在的扩展任务，也可能放到这里处理。
void clusterBeforeSleep(void) {
    int flags = server.cluster->todo_before_sleep;

    /* Reset our flags (not strictly needed since every single function
     * called for flags set should be able to clear its flag). */
    // 前面取出了flags，这里重置集群状态中的需要这里处理的flags。
    // 因为每个单独需要调用的函数都应该清除它自己处理的flag，所以这里可以不用整个reset。
    server.cluster->todo_before_sleep = 0;

    // 当本slave节点正在进行手动故障转移时，收到master的packet消息时，会更新复制offset信息，并加上MANUALFAILOVER标识，以便这里处理。
    if (flags & CLUSTER_TODO_HANDLE_MANUALFAILOVER) {
        /* Handle manual failover as soon as possible so that won't have a 100ms
         * as it was handled only in clusterCron */
        // 尽早处理手动故障转移，这样就不需要每100ms再处理了，因为我们会每100ms在clusterCron中处理。
        if(nodeIsSlave(myself)) {
            clusterHandleManualFailover();
            if (!(server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_FAILOVER))
                // 如果module配置flag中，没有禁止故障转移。则执行slave故障转移。
                clusterHandleSlaveFailover();
        }
    } else if (flags & CLUSTER_TODO_HANDLE_FAILOVER) {
        /* Handle failover, this is needed when it is likely that there is already
         * the quorum from masters in order to react fast. */
        // 处理故障转移。这是很有必要的，因为当已经有quorum数量的master同意了，我们需要尽快的响应。
        clusterHandleSlaveFailover();
    }

    /* Update the cluster state. */
    // 更新集群的状态
    if (flags & CLUSTER_TODO_UPDATE_STATE)
        clusterUpdateState();

    /* Save the config, possibly using fsync. */
    // 保存配置变更到磁盘。
    if (flags & CLUSTER_TODO_SAVE_CONFIG) {
        int fsync = flags & CLUSTER_TODO_FSYNC_CONFIG;
        clusterSaveConfigOrDie(fsync);
    }
}

void clusterDoBeforeSleep(int flags) {
    server.cluster->todo_before_sleep |= flags;
}

/* -----------------------------------------------------------------------------
 * Slots management
 * -------------------------------------------------------------------------- */

/* Test bit 'pos' in a generic bitmap. Return 1 if the bit is set,
 * otherwise 0. */
// 测试对应slot是否为1，是则表示已设置返回1，否则返回0。
int bitmapTestBit(unsigned char *bitmap, int pos) {
    // 计算该位对应的字节位置。
    off_t byte = pos/8;
    // 计算该位对应字节内的bit位置
    int bit = pos&7;
    // 判断对应bit是否为1
    return (bitmap[byte] & (1<<bit)) != 0;
}

/* Set the bit at position 'pos' in a bitmap. */
// 设置bitmap中pos位置为1
void bitmapSetBit(unsigned char *bitmap, int pos) {
    off_t byte = pos/8;
    int bit = pos&7;
    // 找到位置，对应位置设置为1
    bitmap[byte] |= 1<<bit;
}

/* Clear the bit at position 'pos' in a bitmap. */
// 清除bitmap中'pos'位置的bit值
void bitmapClearBit(unsigned char *bitmap, int pos) {
    off_t byte = pos/8;
    int bit = pos&7;
    // 找到位置，对应位置设置为0
    bitmap[byte] &= ~(1<<bit);
}

/* Return non-zero if there is at least one master with slaves in the cluster.
 * Otherwise zero is returned. Used by clusterNodeSetSlotBit() to set the
 * MIGRATE_TO flag the when a master gets the first slot. */
// 如果集群中至少有一个master有slaves，该函数返回非0；否则集群中所有master都没有slaves的话，返回0。
// 用于clusterNodeSetSlotBit()中在master获得第一个slot时判断是否设置MIGRATE_TO标识。
int clusterMastersHaveSlaves(void) {
    dictIterator *di = dictGetSafeIterator(server.cluster->nodes);
    dictEntry *de;
    int slaves = 0;
    // 遍历集群所有节点，计算出master节点的slave节点总数。
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        // 节点是slave时跳过。
        if (nodeIsSlave(node)) continue;
        // slaves数累加
        slaves += node->numslaves;
    }
    dictReleaseIterator(di);
    // slaves != 0 表示当前集群中至少有一个master有slaves，返回true
    return slaves != 0;
}

/* Set the slot bit and return the old value. */
// 设置slot bit为1并返回原来该bit值。
int clusterNodeSetSlotBit(clusterNode *n, int slot) {
    // 取到slot对应bitmap位置当前值
    int old = bitmapTestBit(n->slots,slot);
    // 设置slot对应bitmap位置值为1
    bitmapSetBit(n->slots,slot);
    if (!old) {
        // 如果原来该位置本来就没有设置，则需要更新该节点负责的slots总数+1
        n->numslots++;
        /* When a master gets its first slot, even if it has no slaves,
         * it gets flagged with MIGRATE_TO, that is, the master is a valid
         * target for replicas migration, if and only if at least one of
         * the other masters has slaves right now.
         *
         * Normally masters are valid targets of replica migration if:
         * 1. The used to have slaves (but no longer have).
         * 2. They are slaves failing over a master that used to have slaves.
         *
         * However new masters with slots assigned are considered valid
         * migration targets if the rest of the cluster is not a slave-less.
         *
         * See https://github.com/redis/redis/issues/3043 for more info. */
        // 当一个master获得了它的第一个slot分配，即使它自己没有slaves，它也会标识为MIGRATE_TO。
        // 也就是说，当且仅当其他master中至少有一个现在拥有slaves时，该主服务器才是副本迁移的有效目标。
        if (n->numslots == 1 && clusterMastersHaveSlaves())
            n->flags |= CLUSTER_NODE_MIGRATE_TO;
    }
    return old;
}

/* Clear the slot bit and return the old value. */
// 清除节点内slot bitmap中对应的slot
int clusterNodeClearSlotBit(clusterNode *n, int slot) {
    // 取到该slot的bit位置上原来的值
    int old = bitmapTestBit(n->slots,slot);
    // 清理该slot的bit位置上的值
    bitmapClearBit(n->slots,slot);
    // 如果对应位置原来有设置，置空后更新当前节点目前负责的slots数-1。
    if (old) n->numslots--;
    return old;
}

/* Return the slot bit from the cluster node structure. */
// 返回指定节点对指定slot的负责情况，主要查询节点的slots bitmap，看对应slot位是否为1
int clusterNodeGetSlotBit(clusterNode *n, int slot) {
    return bitmapTestBit(n->slots,slot);
}

/* Add the specified slot to the list of slots that node 'n' will
 * serve. Return C_OK if the operation ended with success.
 * If the slot is already assigned to another instance this is considered
 * an error and C_ERR is returned. */
// 将slot指定给节点n负责。操作成功返回ok，如果该slot已经有节点负责了则返回err。
int clusterAddSlot(clusterNode *n, int slot) {
    // slot有节点负责，返回err
    if (server.cluster->slots[slot]) return C_ERR;
    // 更新节点中的属性，设置对应位bit值为1，更新节点负责的slots总数。
    clusterNodeSetSlotBit(n,slot);
    // 更新集群中该slot所指定的节点
    server.cluster->slots[slot] = n;
    return C_OK;
}

/* Delete the specified slot marking it as unassigned.
 * Returns C_OK if the slot was assigned, otherwise if the slot was
 * already unassigned C_ERR is returned. */
// 删除节点内slot的指定，并标记集群该slot为未分配状态。
int clusterDelSlot(int slot) {
    clusterNode *n = server.cluster->slots[slot];

    if (!n) return C_ERR;
    // 清除节点内部关于该slot的信息，bit位置空，总的负责slots数-1
    serverAssert(clusterNodeClearSlotBit(n,slot) == 1);
    // 集群中该slot关联的节点置空，即设置位未分配状态。
    server.cluster->slots[slot] = NULL;
    return C_OK;
}

/* Delete all the slots associated with the specified node.
 * The number of deleted slots is returned. */
// 给定节点，清除分配给该节点的所有slots。返回清除的slots数量
int clusterDelNodeSlots(clusterNode *node) {
    int deleted = 0, j;

    for (j = 0; j < CLUSTER_SLOTS; j++) {
        if (clusterNodeGetSlotBit(node,j)) {
            clusterDelSlot(j);
            deleted++;
        }
    }
    return deleted;
}

/* Clear the migrating / importing state for all the slots.
 * This is useful at initialization and when turning a master into slave. */
// 清除所有slots的 migrating/importing 状态。
void clusterCloseAllSlots(void) {
    memset(server.cluster->migrating_slots_to,0,
        sizeof(server.cluster->migrating_slots_to));
    memset(server.cluster->importing_slots_from,0,
        sizeof(server.cluster->importing_slots_from));
}

/* -----------------------------------------------------------------------------
 * Cluster state evaluation function
 * -------------------------------------------------------------------------- */

/* The following are defines that are only used in the evaluation function
 * and are based on heuristics. Actually the main point about the rejoin and
 * writable delay is that they should be a few orders of magnitude larger
 * than the network latency. */
// 下面的定义仅用于集群状态评估函数，定义值是基于启发式设置的。实际上，关于重新加入和写延迟数据定义为应该比网络延迟大几个数量级。
#define CLUSTER_MAX_REJOIN_DELAY 5000
#define CLUSTER_MIN_REJOIN_DELAY 500
#define CLUSTER_WRITABLE_DELAY 2000

void clusterUpdateState(void) {
    int j, new_state;
    int reachable_masters = 0;
    static mstime_t among_minority_time;
    static mstime_t first_call_time = 0;

    // 当前已在处理集群状态更新，可以移除对应标识。
    server.cluster->todo_before_sleep &= ~CLUSTER_TODO_UPDATE_STATE;

    /* If this is a master node, wait some time before turning the state
     * into OK, since it is not a good idea to rejoin the cluster as a writable
     * master, after a reboot, without giving the cluster a chance to
     * reconfigure this node. Note that the delay is calculated starting from
     * the first call to this function and not since the server start, in order
     * to don't count the DB loading time. */
    // 如果当前是master节点，初始化时是FAIL状态，在将状态更改为ok之前先等待一段时间。
    // 因为当节点重启后，还没有给集群机会来重新配置该节点，就重新加入到集群中，不是一个好的主意。
    // 注意延迟是从第一次调用此函数开始计算的，而不是从服务器启动开始计算，以免将数据库加载时间也计算在内。
    if (first_call_time == 0) first_call_time = mstime();
    if (nodeIsMaster(myself) &&
        server.cluster->state == CLUSTER_FAIL &&
        mstime() - first_call_time < CLUSTER_WRITABLE_DELAY) return;

    /* Start assuming the state is OK. We'll turn it into FAIL if there
     * are the right conditions. */
    // 处理状态更新，开始先假定新的状态时OK。后面如果达到一些条件时，我们会将状态再变更为FAIL。
    new_state = CLUSTER_OK;

    /* Check if all the slots are covered. */
    // 检查是否所有的slots都有节点负责。
    if (server.cluster_require_full_coverage) {
        // 如果配置是要求所有slots都需要有节点负责，但是出现slot没指定给节点，或者指定的节点是FAIL状态，则整个集群就是FAIL的。
        for (j = 0; j < CLUSTER_SLOTS; j++) {
            if (server.cluster->slots[j] == NULL ||
                server.cluster->slots[j]->flags & (CLUSTER_NODE_FAIL))
            {
                new_state = CLUSTER_FAIL;
                break;
            }
        }
    }

    /* Compute the cluster size, that is the number of master nodes
     * serving at least a single slot.
     *
     * At the same time count the number of reachable masters having
     * at least one slot. */
    // 计算集群的size，即有负责slots的masters数量。我们同时也计算不是FAIL状态的且有负责slots的masters数量。
    {
        dictIterator *di;
        dictEntry *de;

        server.cluster->size = 0;
        di = dictGetSafeIterator(server.cluster->nodes);
        while((de = dictNext(di)) != NULL) {
            clusterNode *node = dictGetVal(de);

            // 节点是master，且有负责slots，则size+1；进一步如果不是处于FAIL/PFAIL状态，reachable_masters+1。
            if (nodeIsMaster(node) && node->numslots) {
                server.cluster->size++;
                if ((node->flags & (CLUSTER_NODE_FAIL|CLUSTER_NODE_PFAIL)) == 0)
                    reachable_masters++;
            }
        }
        dictReleaseIterator(di);
    }

    /* If we are in a minority partition, change the cluster state
     * to FAIL. */
    // 如果我们当前集群中，有效master少于quorum数，说明分区集群不可用，是FAIL的。
    {
        int needed_quorum = (server.cluster->size / 2) + 1;

        if (reachable_masters < needed_quorum) {
            new_state = CLUSTER_FAIL;
            among_minority_time = mstime();
        }
    }

    /* Log a state change */
    // 日志记录集群状态的变更。
    if (new_state != server.cluster->state) {
        mstime_t rejoin_delay = server.cluster_node_timeout;

        /* If the instance is a master and was partitioned away with the
         * minority, don't let it accept queries for some time after the
         * partition heals, to make sure there is enough time to receive
         * a configuration update. */
        // 如果当前节点是master，且与少部分节点组成一个分区，显然目前该分区是不可用的。
        // 当该分区恢复可用时，我们也不能让该节点立即加入集群接收并处理请求，因为配置可能比较老，需要确保有足够时间收到新的配置。
        if (rejoin_delay > CLUSTER_MAX_REJOIN_DELAY)
            rejoin_delay = CLUSTER_MAX_REJOIN_DELAY;
        if (rejoin_delay < CLUSTER_MIN_REJOIN_DELAY)
            rejoin_delay = CLUSTER_MIN_REJOIN_DELAY;

        // 如果当前节点是由FAIL转为OK。当一切准备就绪时，我们也需要等待时间达到rejoin_delay，才更新节点的集群状态，使当前节点可用。
        if (new_state == CLUSTER_OK &&
            nodeIsMaster(myself) &&
            mstime() - among_minority_time < rejoin_delay)
        {
            return;
        }

        /* Change the state and log the event. */
        // 更改集群状态，并记录日志。
        serverLog(LL_WARNING,"Cluster state changed: %s",
            new_state == CLUSTER_OK ? "ok" : "fail");
        server.cluster->state = new_state;
    }
}

/* This function is called after the node startup in order to verify that data
 * loaded from disk is in agreement with the cluster configuration:
 *
 * 1) If we find keys about hash slots we have no responsibility for, the
 *    following happens:
 *    A) If no other node is in charge according to the current cluster
 *       configuration, we add these slots to our node.
 *    B) If according to our config other nodes are already in charge for
 *       this slots, we set the slots as IMPORTING from our point of view
 *       in order to justify we have those slots, and in order to make
 *       redis-trib aware of the issue, so that it can try to fix it.
 * 2) If we find data in a DB different than DB0 we return C_ERR to
 *    signal the caller it should quit the server with an error message
 *    or take other actions.
 *
 * The function always returns C_OK even if it will try to correct
 * the error described in "1". However if data is found in DB different
 * from DB0, C_ERR is returned.
 *
 * The function also uses the logging facility in order to warn the user
 * about desynchronizations between the data we have in memory and the
 * cluster configuration. */
// 这个函数在节点启动后调用，用来验证从磁盘加载的数据是否于集群的配置一致：
// 1）如果我们发现了不归我们负责的hash slot的key，则会进行如下处理：
//    A) 如果根据集群配置，没有其他节点负责这些slots，则将这些slots加入我们节点中。
//    B) 如果根据我们配置，其他节点已经负责了这些slots，我们从自己角度出发将这些slots设置为IMPORTING，证明我们拥有这些slots，
//      从而让redis-trib意识到这个问题，然后尝试去修复。
// 2) 如果我们在DB0之外的DB中找到了数据，将返回err告诉调用者应该退出服务或者采取其他措施。
// 这个函数始终返回ok，即使出现了1中的数据问题，也是尝试去处理。但是当在DB0之外发现数据时，返回err。
// 这个函数也会使用日志功能来警告用户内存数据与集群配置不一致。
int verifyClusterConfigWithData(void) {
    int j;
    int update_config = 0;

    /* Return ASAP if a module disabled cluster redirections. In that case
     * every master can store keys about every possible hash slot. */
    // 如果一个module禁用了集群的重定向功能，任何master都可以存储任何的key，我们不需要做数据检查，尽早返回。
    if (server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_REDIRECTION)
        return C_OK;

    /* If this node is a slave, don't perform the check at all as we
     * completely depend on the replication stream. */
    // 如果节点是slave，不处理check，因为我们数据完全依赖于复制流。
    if (nodeIsSlave(myself)) return C_OK;

    /* Make sure we only have keys in DB0. */
    // 确保只有DB0中有数据，否则返回err
    for (j = 1; j < server.dbnum; j++) {
        if (dictSize(server.db[j].dict)) return C_ERR;
    }

    /* Check that all the slots we see populated memory have a corresponding
     * entry in the cluster table. Otherwise fix the table. */
    // 检测内存数据生成的所有slots是否都在cluster表中有相应entry，没有则加入表中。
    for (j = 0; j < CLUSTER_SLOTS; j++) {
        // 如果该slot没有key，跳过
        if (!countKeysInSlot(j)) continue; /* No keys in this slot. */
        /* Check if we are assigned to this slot or if we are importing it.
         * In both cases check the next slot as the configuration makes
         * sense. */
        // 检测该slot是否属于自己，或者这个slot正在迁移到当前节点，不需要处理，跳过。
        if (server.cluster->slots[j] == myself ||
            server.cluster->importing_slots_from[j] != NULL) continue;

        /* If we are here data and cluster config don't agree, and we have
         * slot 'j' populated even if we are not importing it, nor we are
         * assigned to this slot. Fix this condition. */

        // 如果执行到这里，说明内存数据与集群配置不一致，需要处理。
        update_config++;
        /* Case A: slot is unassigned. Take responsibility for it. */
        if (server.cluster->slots[j] == NULL) {
            serverLog(LL_WARNING, "I have keys for unassigned slot %d. "
                                    "Taking responsibility for it.",j);
            // 该slot没有分配，将该slot加入当当前节点
            clusterAddSlot(myself,j);
        } else {
            serverLog(LL_WARNING, "I have keys for slot %d, but the slot is "
                                    "assigned to another node. "
                                    "Setting it to importing state.",j);
            // 该slot有分配，设置当前节点该slot为importing状态。
            server.cluster->importing_slots_from[j] = server.cluster->slots[j];
        }
    }
    if (update_config) clusterSaveConfigOrDie(1);
    return C_OK;
}

/* -----------------------------------------------------------------------------
 * SLAVE nodes handling
 * -------------------------------------------------------------------------- */

/* Set the specified node 'n' as master for this node.
 * If this node is currently a master, it is turned into a slave. */
// 设置指定节点n为我们的master。如果当前节点是master，则它将转变为slave
void clusterSetMaster(clusterNode *n) {
    // 这里要确保n不是当前节点，且当前节点复制的slots数为0
    serverAssert(n != myself);
    serverAssert(myself->numslots == 0);

    if (nodeIsMaster(myself)) {
        // 如果当前节点是master，则节点转为slave，清除相关标识，清除所有的slots
        myself->flags &= ~(CLUSTER_NODE_MASTER|CLUSTER_NODE_MIGRATE_TO);
        myself->flags |= CLUSTER_NODE_SLAVE;
        clusterCloseAllSlots();
    } else {
        if (myself->slaveof)
            // 如果当前节点是slave，且有master，则需要从master的slaves中移除当前节点。
            clusterNodeRemoveSlave(myself->slaveof,myself);
    }
    // 将当前节点设置为n的slave。并重置手动故障转移状态。
    myself->slaveof = n;
    clusterNodeAddSlave(n,myself);
    replicationSetMaster(n->ip, n->port);
    resetManualFailover();
}

/* -----------------------------------------------------------------------------
 * Nodes to string representation functions.
 * -------------------------------------------------------------------------- */

struct redisNodeFlags {
    uint16_t flag;
    char *name;
};

static struct redisNodeFlags redisNodeFlagsTable[] = {
    {CLUSTER_NODE_MYSELF,       "myself,"},
    {CLUSTER_NODE_MASTER,       "master,"},
    {CLUSTER_NODE_SLAVE,        "slave,"},
    {CLUSTER_NODE_PFAIL,        "fail?,"},
    {CLUSTER_NODE_FAIL,         "fail,"},
    {CLUSTER_NODE_HANDSHAKE,    "handshake,"},
    {CLUSTER_NODE_NOADDR,       "noaddr,"},
    {CLUSTER_NODE_NOFAILOVER,   "nofailover,"}
};

/* Concatenate the comma separated list of node flags to the given SDS
 * string 'ci'. */
sds representClusterNodeFlags(sds ci, uint16_t flags) {
    size_t orig_len = sdslen(ci);
    int i, size = sizeof(redisNodeFlagsTable)/sizeof(struct redisNodeFlags);
    for (i = 0; i < size; i++) {
        struct redisNodeFlags *nodeflag = redisNodeFlagsTable + i;
        if (flags & nodeflag->flag) ci = sdscat(ci, nodeflag->name);
    }
    /* If no flag was added, add the "noflags" special flag. */
    if (sdslen(ci) == orig_len) ci = sdscat(ci,"noflags,");
    sdsIncrLen(ci,-1); /* Remove trailing comma. */
    return ci;
}

/* Generate a csv-alike representation of the specified cluster node.
 * See clusterGenNodesDescription() top comment for more information.
 *
 * The function returns the string representation as an SDS string. */
sds clusterGenNodeDescription(clusterNode *node, int use_pport) {
    int j, start;
    sds ci;
    int port = use_pport && node->pport ? node->pport : node->port;

    /* Node coordinates */
    ci = sdscatlen(sdsempty(),node->name,CLUSTER_NAMELEN);
    ci = sdscatfmt(ci," %s:%i@%i ",
        node->ip,
        port,
        node->cport);

    /* Flags */
    ci = representClusterNodeFlags(ci, node->flags);

    /* Slave of... or just "-" */
    ci = sdscatlen(ci," ",1);
    if (node->slaveof)
        ci = sdscatlen(ci,node->slaveof->name,CLUSTER_NAMELEN);
    else
        ci = sdscatlen(ci,"-",1);

    unsigned long long nodeEpoch = node->configEpoch;
    if (nodeIsSlave(node) && node->slaveof) {
        nodeEpoch = node->slaveof->configEpoch;
    }
    /* Latency from the POV of this node, config epoch, link status */
    ci = sdscatfmt(ci," %I %I %U %s",
        (long long) node->ping_sent,
        (long long) node->pong_received,
        nodeEpoch,
        (node->link || node->flags & CLUSTER_NODE_MYSELF) ?
                    "connected" : "disconnected");

    /* Slots served by this instance. If we already have slots info,
     * append it diretly, otherwise, generate slots only if it has. */
    if (node->slots_info) {
        ci = sdscatsds(ci, node->slots_info);
    } else if (node->numslots > 0) {
        start = -1;
        for (j = 0; j < CLUSTER_SLOTS; j++) {
            int bit;

            if ((bit = clusterNodeGetSlotBit(node,j)) != 0) {
                if (start == -1) start = j;
            }
            if (start != -1 && (!bit || j == CLUSTER_SLOTS-1)) {
                if (bit && j == CLUSTER_SLOTS-1) j++;

                if (start == j-1) {
                    ci = sdscatfmt(ci," %i",start);
                } else {
                    ci = sdscatfmt(ci," %i-%i",start,j-1);
                }
                start = -1;
            }
        }
    }

    /* Just for MYSELF node we also dump info about slots that
     * we are migrating to other instances or importing from other
     * instances. */
    if (node->flags & CLUSTER_NODE_MYSELF) {
        for (j = 0; j < CLUSTER_SLOTS; j++) {
            if (server.cluster->migrating_slots_to[j]) {
                ci = sdscatprintf(ci," [%d->-%.40s]",j,
                    server.cluster->migrating_slots_to[j]->name);
            } else if (server.cluster->importing_slots_from[j]) {
                ci = sdscatprintf(ci," [%d-<-%.40s]",j,
                    server.cluster->importing_slots_from[j]->name);
            }
        }
    }
    return ci;
}

/* Generate the slot topology for all nodes and store the string representation
 * in the slots_info struct on the node. This is used to improve the efficiency
 * of clusterGenNodesDescription() because it removes looping of the slot space
 * for generating the slot info for each node individually. */
void clusterGenNodesSlotsInfo(int filter) {
    clusterNode *n = NULL;
    int start = -1;

    for (int i = 0; i <= CLUSTER_SLOTS; i++) {
        /* Find start node and slot id. */
        if (n == NULL) {
            if (i == CLUSTER_SLOTS) break;
            n = server.cluster->slots[i];
            start = i;
            continue;
        }

        /* Generate slots info when occur different node with start
         * or end of slot. */
        if (i == CLUSTER_SLOTS || n != server.cluster->slots[i]) {
            if (!(n->flags & filter)) {
                if (n->slots_info == NULL) n->slots_info = sdsempty();
                if (start == i-1) {
                    n->slots_info = sdscatfmt(n->slots_info," %i",start);
                } else {
                    n->slots_info = sdscatfmt(n->slots_info," %i-%i",start,i-1);
                }
            }
            if (i == CLUSTER_SLOTS) break;
            n = server.cluster->slots[i];
            start = i;
        }
    }
}

/* Generate a csv-alike representation of the nodes we are aware of,
 * including the "myself" node, and return an SDS string containing the
 * representation (it is up to the caller to free it).
 *
 * All the nodes matching at least one of the node flags specified in
 * "filter" are excluded from the output, so using zero as a filter will
 * include all the known nodes in the representation, including nodes in
 * the HANDSHAKE state.
 *
 * Setting use_pport to 1 in a TLS cluster makes the result contain the
 * plaintext client port rather then the TLS client port of each node.
 *
 * The representation obtained using this function is used for the output
 * of the CLUSTER NODES function, and as format for the cluster
 * configuration file (nodes.conf) for a given node. */
sds clusterGenNodesDescription(int filter, int use_pport) {
    sds ci = sdsempty(), ni;
    dictIterator *di;
    dictEntry *de;

    /* Generate all nodes slots info firstly. */
    clusterGenNodesSlotsInfo(filter);

    di = dictGetSafeIterator(server.cluster->nodes);
    while((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (node->flags & filter) continue;
        ni = clusterGenNodeDescription(node, use_pport);
        ci = sdscatsds(ci,ni);
        sdsfree(ni);
        ci = sdscatlen(ci,"\n",1);

        /* Release slots info. */
        if (node->slots_info) {
            sdsfree(node->slots_info);
            node->slots_info = NULL;
        }
    }
    dictReleaseIterator(di);
    return ci;
}

/* -----------------------------------------------------------------------------
 * CLUSTER command
 * -------------------------------------------------------------------------- */

// 根据消息类型返回对应类型的name字符串
const char *clusterGetMessageTypeString(int type) {
    switch(type) {
    case CLUSTERMSG_TYPE_PING: return "ping";
    case CLUSTERMSG_TYPE_PONG: return "pong";
    case CLUSTERMSG_TYPE_MEET: return "meet";
    case CLUSTERMSG_TYPE_FAIL: return "fail";
    case CLUSTERMSG_TYPE_PUBLISH: return "publish";
    case CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST: return "auth-req";
    case CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK: return "auth-ack";
    case CLUSTERMSG_TYPE_UPDATE: return "update";
    case CLUSTERMSG_TYPE_MFSTART: return "mfstart";
    case CLUSTERMSG_TYPE_MODULE: return "module";
    }
    return "unknown";
}

// 从对象中解析slot值，解析出错或slot值不对则回复err
int getSlotOrReply(client *c, robj *o) {
    long long slot;

    if (getLongLongFromObject(o,&slot) != C_OK ||
        slot < 0 || slot >= CLUSTER_SLOTS)
    {
        addReplyError(c,"Invalid or out of range slot");
        return -1;
    }
    return (int) slot;
}

void addNodeReplyForClusterSlot(client *c, clusterNode *node, int start_slot, int end_slot) {
    int i, nested_elements = 3; /* slots (2) + master addr (1) */
    void *nested_replylen = addReplyDeferredLen(c);
    addReplyLongLong(c, start_slot);
    addReplyLongLong(c, end_slot);
    addReplyArrayLen(c, 3);
    addReplyBulkCString(c, node->ip);
    /* Report non-TLS ports to non-TLS client in TLS cluster if available. */
    int use_pport = (server.tls_cluster &&
                     c->conn && connGetType(c->conn) != CONN_TYPE_TLS);
    addReplyLongLong(c, use_pport && node->pport ? node->pport : node->port);
    addReplyBulkCBuffer(c, node->name, CLUSTER_NAMELEN);

    /* Remaining nodes in reply are replicas for slot range */
    for (i = 0; i < node->numslaves; i++) {
        /* This loop is copy/pasted from clusterGenNodeDescription()
         * with modifications for per-slot node aggregation. */
        if (nodeFailed(node->slaves[i])) continue;
        addReplyArrayLen(c, 3);
        addReplyBulkCString(c, node->slaves[i]->ip);
        /* Report slave's non-TLS port to non-TLS client in TLS cluster */
        addReplyLongLong(c, (use_pport && node->slaves[i]->pport ?
                             node->slaves[i]->pport :
                             node->slaves[i]->port));
        addReplyBulkCBuffer(c, node->slaves[i]->name, CLUSTER_NAMELEN);
        nested_elements++;
    }
    setDeferredArrayLen(c, nested_replylen, nested_elements);
}

void clusterReplyMultiBulkSlots(client * c) {
    /* Format: 1) 1) start slot
     *            2) end slot
     *            3) 1) master IP
     *               2) master port
     *               3) node ID
     *            4) 1) replica IP
     *               2) replica port
     *               3) node ID
     *           ... continued until done
     */
    clusterNode *n = NULL;
    int num_masters = 0, start = -1;
    void *slot_replylen = addReplyDeferredLen(c);

    for (int i = 0; i <= CLUSTER_SLOTS; i++) {
        /* Find start node and slot id. */
        if (n == NULL) {
            if (i == CLUSTER_SLOTS) break;
            n = server.cluster->slots[i];
            start = i;
            continue;
        }

        /* Add cluster slots info when occur different node with start
         * or end of slot. */
        if (i == CLUSTER_SLOTS || n != server.cluster->slots[i]) {
            addNodeReplyForClusterSlot(c, n, start, i-1);
            num_masters++;
            if (i == CLUSTER_SLOTS) break;
            n = server.cluster->slots[i];
            start = i;
        }
    }
    setDeferredArrayLen(c, slot_replylen, num_masters);
}

void clusterCommand(client *c) {
    // 当前节点不是集群模式
    if (server.cluster_enabled == 0) {
        addReplyError(c,"This instance has cluster support disabled");
        return;
    }

    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        // cluster help
        const char *help[] = {
"ADDSLOTS <slot> [<slot> ...]",
"    Assign slots to current node.",
"BUMPEPOCH",
"    Advance the cluster config epoch.",
"COUNT-FAILURE-REPORTS <node-id>",
"    Return number of failure reports for <node-id>.",
"COUNTKEYSINSLOT <slot>",
"    Return the number of keys in <slot>.",
"DELSLOTS <slot> [<slot> ...]",
"    Delete slots information from current node.",
"FAILOVER [FORCE|TAKEOVER]",
"    Promote current replica node to being a master.",
"FORGET <node-id>",
"    Remove a node from the cluster.",
"GETKEYSINSLOT <slot> <count>",
"    Return key names stored by current node in a slot.",
"FLUSHSLOTS",
"    Delete current node own slots information.",
"INFO",
"    Return information about the cluster.",
"KEYSLOT <key>",
"    Return the hash slot for <key>.",
"MEET <ip> <port> [<bus-port>]",
"    Connect nodes into a working cluster.",
"MYID",
"    Return the node id.",
"NODES",
"    Return cluster configuration seen by node. Output format:",
"    <id> <ip:port> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ...",
"REPLICATE <node-id>",
"    Configure current node as replica to <node-id>.",
"RESET [HARD|SOFT]",
"    Reset current node (default: soft).",
"SET-CONFIG-EPOCH <epoch>",
"    Set config epoch of current node.",
"SETSLOT <slot> (IMPORTING|MIGRATING|STABLE|NODE <node-id>)",
"    Set slot state.",
"REPLICAS <node-id>",
"    Return <node-id> replicas.",
"SAVECONFIG",
"    Force saving cluster configuration on disk.",
"SLOTS",
"    Return information about slots range mappings. Each range is made of:",
"    start, end, master and replicas IP addresses, ports and ids",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"meet") && (c->argc == 4 || c->argc == 5)) {
        /* CLUSTER MEET <ip> <port> [cport] */
        long long port, cport;

        // 获取port
        if (getLongLongFromObject(c->argv[3], &port) != C_OK) {
            addReplyErrorFormat(c,"Invalid TCP base port specified: %s",
                                (char*)c->argv[3]->ptr);
            return;
        }

        if (c->argc == 5) {
            // 如果参数为5个说明指定了cluster port，解析
            if (getLongLongFromObject(c->argv[4], &cport) != C_OK) {
                addReplyErrorFormat(c,"Invalid TCP bus port specified: %s",
                                    (char*)c->argv[4]->ptr);
                return;
            }
        } else {
            // 否则没有设置的话，给个默认的
            cport = port + CLUSTER_PORT_INCR;
        }

        // 当前节点与命令中指定的节点进行握手
        if (clusterStartHandshake(c->argv[2]->ptr,port,cport) == 0 &&
            errno == EINVAL)
        {
            addReplyErrorFormat(c,"Invalid node address specified: %s:%s",
                            (char*)c->argv[2]->ptr, (char*)c->argv[3]->ptr);
        } else {
            addReply(c,shared.ok);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"nodes") && c->argc == 2) {
        /* CLUSTER NODES */
        /* Report plaintext ports, only if cluster is TLS but client is known to
         * be non-TLS). */
        // 查询集群节点信息
        int use_pport = (server.tls_cluster &&
                         c->conn && connGetType(c->conn) != CONN_TYPE_TLS);
        sds nodes = clusterGenNodesDescription(0, use_pport);
        addReplyVerbatim(c,nodes,sdslen(nodes),"txt");
        sdsfree(nodes);
    } else if (!strcasecmp(c->argv[1]->ptr,"myid") && c->argc == 2) {
        /* CLUSTER MYID */
        // 查询集群中我们当前节点的id
        addReplyBulkCBuffer(c,myself->name, CLUSTER_NAMELEN);
    } else if (!strcasecmp(c->argv[1]->ptr,"slots") && c->argc == 2) {
        /* CLUSTER SLOTS */
        // 查询集群slots信息
        clusterReplyMultiBulkSlots(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"flushslots") && c->argc == 2) {
        /* CLUSTER FLUSHSLOTS */
        // 清空集群当前节点的slots信息，如果db中有数据，则不能处理，返回err。
        if (dictSize(server.db[0].dict) != 0) {
            addReplyError(c,"DB must be empty to perform CLUSTER FLUSHSLOTS.");
            return;
        }
        // 清除myself节点负责的slots信息
        clusterDelNodeSlots(myself);
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
        addReply(c,shared.ok);
    } else if ((!strcasecmp(c->argv[1]->ptr,"addslots") ||
               !strcasecmp(c->argv[1]->ptr,"delslots")) && c->argc >= 3)
    {
        /* CLUSTER ADDSLOTS <slot> [slot] ... */
        /* CLUSTER DELSLOTS <slot> [slot] ... */
        int j, slot;
        unsigned char *slots = zmalloc(CLUSTER_SLOTS);
        int del = !strcasecmp(c->argv[1]->ptr,"delslots");

        memset(slots,0,CLUSTER_SLOTS);
        /* Check that all the arguments are parseable and that all the
         * slots are not already busy. */
        // 检查所有的参数是否都可解析，并确保参数中所有slots没被分配。
        for (j = 2; j < c->argc; j++) {
            // 解析slot，解析失败则回复err并返回
            if ((slot = getSlotOrReply(c,c->argv[j])) == -1) {
                zfree(slots);
                return;
            }
            // 如果是del操作，且当前处理的slot还没有被分配，则回复err并返回。
            // 而如果是add操作，且处理的slot已经分配了，而也回复err并返回。
            if (del && server.cluster->slots[slot] == NULL) {
                addReplyErrorFormat(c,"Slot %d is already unassigned", slot);
                zfree(slots);
                return;
            } else if (!del && server.cluster->slots[slot]) {
                addReplyErrorFormat(c,"Slot %d is already busy", slot);
                zfree(slots);
                return;
            }
            // 如果slots[slot]==1，则说明该slot前面指定过了，回复err并返回。否则设置slots[slot]++，后面根据该数组来设置slot信息。
            if (slots[slot]++ == 1) {
                addReplyErrorFormat(c,"Slot %d specified multiple times",
                    (int)slot);
                zfree(slots);
                return;
            }
        }
        // 遍历slots数组，处理add/del slot操作。
        for (j = 0; j < CLUSTER_SLOTS; j++) {
            if (slots[j]) {
                // slots[j]为1，表示我们需要处理slot j。
                int retval;

                /* If this slot was set as importing we can clear this
                 * state as now we are the real owner of the slot. */
                // 如果这个slot当前是正在importing，我们可以清除掉该状态，因为我们现在是该slot真正的owner了。
                // 只有del才会有这里这个置importing为NULL的处理，因为add时前面判断过该slot肯定是没有分配的。
                if (server.cluster->importing_slots_from[j])
                    server.cluster->importing_slots_from[j] = NULL;

                // 如果是del操作，则清除当前处理的slot（不管slot之前由谁负责）。
                // 如果是add操作，则将当前处理的slot添加给我们自己负责。
                retval = del ? clusterDelSlot(j) :
                               clusterAddSlot(myself,j);
                serverAssertWithInfo(c,NULL,retval == C_OK);
            }
        }
        zfree(slots);
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"setslot") && c->argc >= 4) {
        /* SETSLOT 10 MIGRATING <node ID> */
        /* SETSLOT 10 IMPORTING <node ID> */
        /* SETSLOT 10 STABLE */
        /* SETSLOT 10 NODE <node ID> */
        int slot;
        clusterNode *n;

        // slave节点不能使用SETSLOT命令
        if (nodeIsSlave(myself)) {
            addReplyError(c,"Please use SETSLOT only with masters.");
            return;
        }

        // 获取指定的slot值
        if ((slot = getSlotOrReply(c,c->argv[2])) == -1) return;

        if (!strcasecmp(c->argv[3]->ptr,"migrating") && c->argc == 5) {
            // 如果是migrating，判断该slot当前是否由我们负责。
            if (server.cluster->slots[slot] != myself) {
                addReplyErrorFormat(c,"I'm not the owner of hash slot %u",slot);
                return;
            }
            // 检查指定节点是否存在
            if ((n = clusterLookupNode(c->argv[4]->ptr)) == NULL) {
                addReplyErrorFormat(c,"I don't know about node %s",
                    (char*)c->argv[4]->ptr);
                return;
            }
            // 设置该slot迁移到指定节点
            server.cluster->migrating_slots_to[slot] = n;
        } else if (!strcasecmp(c->argv[3]->ptr,"importing") && c->argc == 5) {
            // 如果是importing，检查该slot是否已经由我们自己负责了。不需要检查该节点是否正负责该slot吗？
            if (server.cluster->slots[slot] == myself) {
                addReplyErrorFormat(c,
                    "I'm already the owner of hash slot %u",slot);
                return;
            }
            // 检查指定节点是否存在
            if ((n = clusterLookupNode(c->argv[4]->ptr)) == NULL) {
                addReplyErrorFormat(c,"I don't know about node %s",
                    (char*)c->argv[4]->ptr);
                return;
            }
            // 设置该slot从指定节点迁入
            server.cluster->importing_slots_from[slot] = n;
        } else if (!strcasecmp(c->argv[3]->ptr,"stable") && c->argc == 4) {
            /* CLUSTER SETSLOT <SLOT> STABLE */
            // 设置该slot为stable，将对应importing和migrating置NULL。
            server.cluster->importing_slots_from[slot] = NULL;
            server.cluster->migrating_slots_to[slot] = NULL;
        } else if (!strcasecmp(c->argv[3]->ptr,"node") && c->argc == 5) {
            /* CLUSTER SETSLOT <SLOT> NODE <NODE ID> */
            // 将该slot给指定node负责。
            clusterNode *n = clusterLookupNode(c->argv[4]->ptr);

            if (!n) {
                addReplyErrorFormat(c,"Unknown node %s",
                    (char*)c->argv[4]->ptr);
                return;
            }
            /* If this hash slot was served by 'myself' before to switch
             * make sure there are no longer local keys for this hash slot. */
            // 如果该slot当前由我们自己负责，且指定的节点不是我们自己，则检查该slot中是否还有keys存在。
            if (server.cluster->slots[slot] == myself && n != myself) {
                if (countKeysInSlot(slot) != 0) {
                    addReplyErrorFormat(c,
                        "Can't assign hashslot %d to a different node "
                        "while I still hold keys for this hash slot.", slot);
                    return;
                }
            }
            /* If this slot is in migrating status but we have no keys
             * for it assigning the slot to another node will clear
             * the migrating status. */
            // 如果该slot处于migrating状态，但是该slot中没有keys，则清除该slot的migrating状态。
            if (countKeysInSlot(slot) == 0 &&
                server.cluster->migrating_slots_to[slot])
                server.cluster->migrating_slots_to[slot] = NULL;

            // 移除我们节点的负责的该slot，并将slot加入指定节点负责。
            // 注意如果我们直接修改slots表，而不设置importing属性的话，需要生成新的配置纪元，并通知其他节点，否则slot信息会错乱。
            // https://blog.csdn.net/u011535541/article/details/78834565
            clusterDelSlot(slot);
            clusterAddSlot(n,slot);

            /* If this node was importing this slot, assigning the slot to
             * itself also clears the importing status. */
            // 如果当前节点正在importing该slot，指定该slot给我们自己，也会清除掉importing状态。
            if (n == myself &&
                server.cluster->importing_slots_from[slot])
            {
                /* This slot was manually migrated, set this node configEpoch
                 * to a new epoch so that the new version can be propagated
                 * by the cluster.
                 *
                 * Note that if this ever results in a collision with another
                 * node getting the same configEpoch, for example because a
                 * failover happens at the same time we close the slot, the
                 * configEpoch collision resolution will fix it assigning
                 * a different epoch to each node. */
                // 有importing标识，该slot被手动迁移到本节点，需要设置当前节点新的配置纪元，从而当前节点的配置以最新版本传播到整个集群。
                // 注意如果这导致当前节点与其他节点获得了相同的配置纪元，从而产生冲突（如同时发送故障转移我们关闭slot时）。
                // 后面配置纪元冲突解决算法将修复并指定不同的配置纪元给这冲突的节点。
                if (clusterBumpConfigEpochWithoutConsensus() == C_OK) {
                    serverLog(LL_WARNING,
                        "configEpoch updated after importing slot %d", slot);
                }
                server.cluster->importing_slots_from[slot] = NULL;
                /* After importing this slot, let the other nodes know as
                 * soon as possible. */
                // importing slot后，设置了新的配置纪元，我们尽快广播消息给集群所有节点。
                clusterBroadcastPong(CLUSTER_BROADCAST_ALL);
            }
        } else {
            addReplyError(c,
                "Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP");
            return;
        }
        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG|CLUSTER_TODO_UPDATE_STATE);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"bumpepoch") && c->argc == 2) {
        /* CLUSTER BUMPEPOCH */
        // 强制设置当前节点配置纪元为最新纪元。
        int retval = clusterBumpConfigEpochWithoutConsensus();
        sds reply = sdscatprintf(sdsempty(),"+%s %llu\r\n",
                (retval == C_OK) ? "BUMPED" : "STILL",
                (unsigned long long) myself->configEpoch);
        addReplySds(c,reply);
    } else if (!strcasecmp(c->argv[1]->ptr,"info") && c->argc == 2) {
        /* CLUSTER INFO */
        // 查询集群信息
        char *statestr[] = {"ok","fail","needhelp"};
        int slots_assigned = 0, slots_ok = 0, slots_pfail = 0, slots_fail = 0;
        uint64_t myepoch;
        int j;

        for (j = 0; j < CLUSTER_SLOTS; j++) {
            clusterNode *n = server.cluster->slots[j];

            if (n == NULL) continue;
            slots_assigned++;
            if (nodeFailed(n)) {
                slots_fail++;
            } else if (nodeTimedOut(n)) {
                slots_pfail++;
            } else {
                slots_ok++;
            }
        }

        myepoch = (nodeIsSlave(myself) && myself->slaveof) ?
                  myself->slaveof->configEpoch : myself->configEpoch;

        sds info = sdscatprintf(sdsempty(),
            "cluster_state:%s\r\n"
            "cluster_slots_assigned:%d\r\n"
            "cluster_slots_ok:%d\r\n"
            "cluster_slots_pfail:%d\r\n"
            "cluster_slots_fail:%d\r\n"
            "cluster_known_nodes:%lu\r\n"
            "cluster_size:%d\r\n"
            "cluster_current_epoch:%llu\r\n"
            "cluster_my_epoch:%llu\r\n"
            , statestr[server.cluster->state],
            slots_assigned,
            slots_ok,
            slots_pfail,
            slots_fail,
            dictSize(server.cluster->nodes),
            server.cluster->size,
            (unsigned long long) server.cluster->currentEpoch,
            (unsigned long long) myepoch
        );

        /* Show stats about messages sent and received. */
        long long tot_msg_sent = 0;
        long long tot_msg_received = 0;

        for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
            if (server.cluster->stats_bus_messages_sent[i] == 0) continue;
            tot_msg_sent += server.cluster->stats_bus_messages_sent[i];
            info = sdscatprintf(info,
                "cluster_stats_messages_%s_sent:%lld\r\n",
                clusterGetMessageTypeString(i),
                server.cluster->stats_bus_messages_sent[i]);
        }
        info = sdscatprintf(info,
            "cluster_stats_messages_sent:%lld\r\n", tot_msg_sent);

        for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
            if (server.cluster->stats_bus_messages_received[i] == 0) continue;
            tot_msg_received += server.cluster->stats_bus_messages_received[i];
            info = sdscatprintf(info,
                "cluster_stats_messages_%s_received:%lld\r\n",
                clusterGetMessageTypeString(i),
                server.cluster->stats_bus_messages_received[i]);
        }
        info = sdscatprintf(info,
            "cluster_stats_messages_received:%lld\r\n", tot_msg_received);

        /* Produce the reply protocol. */
        addReplyVerbatim(c,info,sdslen(info),"txt");
        sdsfree(info);
    } else if (!strcasecmp(c->argv[1]->ptr,"saveconfig") && c->argc == 2) {
        // 保存集群配置到磁盘
        int retval = clusterSaveConfig(1);

        if (retval == 0)
            addReply(c,shared.ok);
        else
            addReplyErrorFormat(c,"error saving the cluster node config: %s",
                strerror(errno));
    } else if (!strcasecmp(c->argv[1]->ptr,"keyslot") && c->argc == 3) {
        /* CLUSTER KEYSLOT <key> */
        // 查询指定key应归属那个slot
        sds key = c->argv[2]->ptr;

        // 计算hash，找到对应slot返回
        addReplyLongLong(c,keyHashSlot(key,sdslen(key)));
    } else if (!strcasecmp(c->argv[1]->ptr,"countkeysinslot") && c->argc == 3) {
        /* CLUSTER COUNTKEYSINSLOT <slot> */
        // 查询指定slot拥有的keys的数量
        long long slot;

        // 获取指定slot，并检查是否合法
        if (getLongLongFromObjectOrReply(c,c->argv[2],&slot,NULL) != C_OK)
            return;
        if (slot < 0 || slot >= CLUSTER_SLOTS) {
            addReplyError(c,"Invalid slot");
            return;
        }
        // 获取slot中keys数据，构造结果返回
        addReplyLongLong(c,countKeysInSlot(slot));
    } else if (!strcasecmp(c->argv[1]->ptr,"getkeysinslot") && c->argc == 4) {
        /* CLUSTER GETKEYSINSLOT <slot> <count> */
        // 获取指定slot下的keys，可以指定最大返回keys的数量
        long long maxkeys, slot;
        unsigned int numkeys, j;
        robj **keys;

        // 获取slots和count参数，并检查是否合法
        if (getLongLongFromObjectOrReply(c,c->argv[2],&slot,NULL) != C_OK)
            return;
        if (getLongLongFromObjectOrReply(c,c->argv[3],&maxkeys,NULL)
            != C_OK)
            return;
        if (slot < 0 || slot >= CLUSTER_SLOTS || maxkeys < 0) {
            addReplyError(c,"Invalid slot or number of keys");
            return;
        }

        /* Avoid allocating more than needed in case of large COUNT argument
         * and smaller actual number of keys. */
        // 计算好需要返回的keys数量，提前分配需要的存储空间，避免浪费。
        unsigned int keys_in_slot = countKeysInSlot(slot);
        if (maxkeys > keys_in_slot) maxkeys = keys_in_slot;

        keys = zmalloc(sizeof(robj*)*maxkeys);
        // 从指定 slot 中查询 maxkeys数量 的key放入 keys 中
        numkeys = getKeysInSlot(slot, keys, maxkeys);
        // 构造结果返回
        addReplyArrayLen(c,numkeys);
        for (j = 0; j < numkeys; j++) {
            addReplyBulk(c,keys[j]);
            decrRefCount(keys[j]);
        }
        zfree(keys);
    } else if (!strcasecmp(c->argv[1]->ptr,"forget") && c->argc == 3) {
        /* CLUSTER FORGET <NODE ID> */
        // 移除某个节点，FORGET操作。注意如果要完全移除，则需要所有节点都执行这个命令，否则一定时间后，节点又会加入集群中。
        clusterNode *n = clusterLookupNode(c->argv[2]->ptr);

        // 不能移除不存在的节点，不能移除自己以及自己的master节点。
        if (!n) {
            addReplyErrorFormat(c,"Unknown node %s", (char*)c->argv[2]->ptr);
            return;
        } else if (n == myself) {
            addReplyError(c,"I tried hard but I can't forget myself...");
            return;
        } else if (nodeIsSlave(myself) && myself->slaveof == n) {
            addReplyError(c,"Can't forget my master!");
            return;
        }
        // 先加入节点黑名单，1分钟内防止再加入集群。然后加从集群中删除该节点。
        clusterBlacklistAddNode(n);
        clusterDelNode(n);
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|
                             CLUSTER_TODO_SAVE_CONFIG);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"replicate") && c->argc == 3) {
        /* CLUSTER REPLICATE <NODE ID> */
        // 设置当前节点为指定节点的slave
        clusterNode *n = clusterLookupNode(c->argv[2]->ptr);

        /* Lookup the specified node in our table. */
        // 检查指定节点是否存在
        if (!n) {
            addReplyErrorFormat(c,"Unknown node %s", (char*)c->argv[2]->ptr);
            return;
        }

        /* I can't replicate myself. */
        // 不能自己作为自己的slave
        if (n == myself) {
            addReplyError(c,"Can't replicate myself");
            return;
        }

        /* Can't replicate a slave. */
        // 指定节点不能为slave。我们只能作为master的slave。
        if (nodeIsSlave(n)) {
            addReplyError(c,"I can only replicate a master, not a replica.");
            return;
        }

        /* If the instance is currently a master, it should have no assigned
         * slots nor keys to accept to replicate some other node.
         * Slaves can switch to another master without issues. */
        // 如果当前节点是master，要把master指定为其他节点的slave，则我们不能负责任何slots，且我们db中不能有任何keys。否则数据会丢失。
        // 如果当前节点是slave，则slave可以自由切换为其他master的slave，不会产生任何问题。
        if (nodeIsMaster(myself) &&
            (myself->numslots != 0 || dictSize(server.db[0].dict) != 0)) {
            addReplyError(c,
                "To set a master the node must be empty and "
                "without assigned slots.");
            return;
        }

        /* Set the master. */
        // 设置当前节点的master为指定节点n
        clusterSetMaster(n);
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|CLUSTER_TODO_SAVE_CONFIG);
        addReply(c,shared.ok);
    } else if ((!strcasecmp(c->argv[1]->ptr,"slaves") ||
                !strcasecmp(c->argv[1]->ptr,"replicas")) && c->argc == 3) {
        /* CLUSTER SLAVES <NODE ID> */
        // 列出指定节点的所有slaves节点的信息。
        clusterNode *n = clusterLookupNode(c->argv[2]->ptr);
        int j;

        /* Lookup the specified node in our table. */
        // 如果指定节点不存在，或者指定节点不是master，回复err。
        if (!n) {
            addReplyErrorFormat(c,"Unknown node %s", (char*)c->argv[2]->ptr);
            return;
        }

        if (nodeIsSlave(n)) {
            addReplyError(c,"The specified node is not a master");
            return;
        }

        /* Use plaintext port if cluster is TLS but client is non-TLS. */
        // 如果集群使用TLS，但是client使用的不是TLS，则使用plaintext port。
        int use_pport = (server.tls_cluster &&
                         c->conn && connGetType(c->conn) != CONN_TYPE_TLS);
        // 构造slaves节点信息返回。
        addReplyArrayLen(c,n->numslaves);
        for (j = 0; j < n->numslaves; j++) {
            sds ni = clusterGenNodeDescription(n->slaves[j], use_pport);
            addReplyBulkCString(c,ni);
            sdsfree(ni);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"count-failure-reports") &&
               c->argc == 3)
    {
        /* CLUSTER COUNT-FAILURE-REPORTS <NODE ID> */
        // 计算指定节点的故障报告数量。
        clusterNode *n = clusterLookupNode(c->argv[2]->ptr);

        // 节点不存在则回复err。否则查询该节点的fail_reports列表元素数量，并回复。
        if (!n) {
            addReplyErrorFormat(c,"Unknown node %s", (char*)c->argv[2]->ptr);
            return;
        } else {
            addReplyLongLong(c,clusterNodeFailureReportsCount(n));
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"failover") &&
               (c->argc == 2 || c->argc == 3))
    {
        /* CLUSTER FAILOVER [FORCE|TAKEOVER] */
        // 集群故障转移命令
        int force = 0, takeover = 0;

        if (c->argc == 3) {
            if (!strcasecmp(c->argv[2]->ptr,"force")) {
                // force
                force = 1;
            } else if (!strcasecmp(c->argv[2]->ptr,"takeover")) {
                // takeover，同时也是force
                takeover = 1;
                force = 1; /* Takeover also implies force. */
            } else {
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
        }

        /* Check preconditions. */
        // 检查先决条件，执行手动故障转移命令的节点只能是slave，并且有自己的master。
        // 另外如果不是force强制执行，只有在master是FAIL状态或者我们与master连接断开时，才能执行手动故障转移。
        if (nodeIsMaster(myself)) {
            addReplyError(c,"You should send CLUSTER FAILOVER to a replica");
            return;
        } else if (myself->slaveof == NULL) {
            addReplyError(c,"I'm a replica but my master is unknown to me");
            return;
        } else if (!force &&
                   (nodeFailed(myself->slaveof) ||
                    myself->slaveof->link == NULL))
        {
            addReplyError(c,"Master is down or failed, "
                            "please use CLUSTER FAILOVER FORCE");
            return;
        }
        // 设置手动故障转移状态，结束时间。
        resetManualFailover();
        server.cluster->mf_end = mstime() + CLUSTER_MF_TIMEOUT;

        if (takeover) {
            /* A takeover does not perform any initial check. It just
             * generates a new configuration epoch for this node without
             * consensus, claims the master's slots, and broadcast the new
             * configuration. */
            // takeover，接管前不进行任何检查，只是无脑将当前节点的配置纪元更新为最新纪元，并声明接管主节点的slots，广播新配置。
            serverLog(LL_WARNING,"Taking over the master (user request).");
            // 在没有达成共识的情况下，更新当前节点配置纪元为最新纪元。
            clusterBumpConfigEpochWithoutConsensus();
            // 当前节点转为msater，接管master的slots，更新状态并保存配置，最后广播传输我们的新配置。
            clusterFailoverReplaceYourMaster();
        } else if (force) {
            /* If this is a forced failover, we don't need to talk with our
             * master to agree about the offset. We just failover taking over
             * it without coordination. */
            // 如果是强制的故障转移，不需要与master就offset达成一致，不进行协调的情况下直接准备接管master。
            // 设置mf_can_start为1，下次before sleep中执行故障转移。
            serverLog(LL_WARNING,"Forced failover user request accepted.");
            server.cluster->mf_can_start = 1;
        } else {
            // 手动故障转移，不是force，发送MFSTART消息到我们的master
            serverLog(LL_WARNING,"Manual failover user request accepted.");
            clusterSendMFStart(myself->slaveof);
        }
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"set-config-epoch") && c->argc == 3)
    {
        /* CLUSTER SET-CONFIG-EPOCH <epoch>
         *
         * The user is allowed to set the config epoch only when a node is
         * totally fresh: no config epoch, no other known node, and so forth.
         * This happens at cluster creation time to start with a cluster where
         * every node has a different node ID, without to rely on the conflicts
         * resolution system which is too slow when a big cluster is created. */
        // 仅当节点是全新的：没有配置纪元，没有其他节点知道这个节点等时，用户才允许设置节点的配置纪元。
        // 通常在一个大的集群创建的时候，集群中所有节点都有不同的ID，我们为了不完全依赖于冲突解决算法设置不同的配置纪元，而使用这个命令自己操作。
        // 因为在大的集群刚创建时，所有配置纪元都为0，以来冲突解决算法的话需要很长时间才能使得所有节点都有不同的配置纪元。
        long long epoch;

        // 获取设置的纪元参数
        if (getLongLongFromObjectOrReply(c,c->argv[2],&epoch,NULL) != C_OK)
            return;

        // 校验处理，只有以下条件完全满足才能设置当前节点的配置纪元。
        // 1、纪元应该>=0。2、当前节点集群状态nodes中应该只有自己。3、自己的配置纪元应该为0。
        if (epoch < 0) {
            addReplyErrorFormat(c,"Invalid config epoch specified: %lld",epoch);
        } else if (dictSize(server.cluster->nodes) > 1) {
            addReplyError(c,"The user can assign a config epoch only when the "
                            "node does not know any other node.");
        } else if (myself->configEpoch != 0) {
            addReplyError(c,"Node config epoch is already non-zero");
        } else {
            // 设置自己的配置纪元
            myself->configEpoch = epoch;
            serverLog(LL_WARNING,
                "configEpoch set to %llu via CLUSTER SET-CONFIG-EPOCH",
                (unsigned long long) myself->configEpoch);

            // 如果集群的当前纪元，小于设置的配置纪元，则更新集群当前纪元。集群的当前纪元永远是已知节点中的最大配置纪元。
            if (server.cluster->currentEpoch < (uint64_t)epoch)
                server.cluster->currentEpoch = epoch;
            /* No need to fsync the config here since in the unlucky event
             * of a failure to persist the config, the conflict resolution code
             * will assign a unique config to this node. */
            // 这里不需要fsync该配置变更，因为即使不幸持久化配置失败了。纪元冲突解决算法最终也会为所有节点都指定唯一的配置纪元。
            clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE|
                                 CLUSTER_TODO_SAVE_CONFIG);
            addReply(c,shared.ok);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"reset") &&
               (c->argc == 2 || c->argc == 3))
    {
        /* CLUSTER RESET [SOFT|HARD] */
        // 集群reset操作
        int hard = 0;

        /* Parse soft/hard argument. Default is soft. */
        // 解析 soft/hard 参数，默认是soft。
        if (c->argc == 3) {
            if (!strcasecmp(c->argv[2]->ptr,"hard")) {
                hard = 1;
            } else if (!strcasecmp(c->argv[2]->ptr,"soft")) {
                hard = 0;
            } else {
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
        }

        /* Slaves can be reset while containing data, but not master nodes
         * that must be empty. */
        // slaves能在数据不为空的时候reset，但是master必须要数据为空时才能重置。
        if (nodeIsMaster(myself) && dictSize(c->db->dict) != 0) {
            addReplyError(c,"CLUSTER RESET can't be called with "
                            "master nodes containing keys");
            return;
        }
        // 处理集群节点重置
        clusterReset(hard);
        addReply(c,shared.ok);
    } else {
        // 返回子命令错误
        addReplySubcommandSyntaxError(c);
        return;
    }
}

/* -----------------------------------------------------------------------------
 * DUMP, RESTORE and MIGRATE commands
 * -------------------------------------------------------------------------- */
// redis键迁移：https://blog.csdn.net/sinat_36171694/article/details/100567527

/* Generates a DUMP-format representation of the object 'o', adding it to the
 * io stream pointed by 'rio'. This function can't fail. */
// 生成DUMP格式形式的o对象，并将其加入到rio流中。函数不会失败。
// RDB payload（1字节type + obj） + RDB version（2字节） + CRC64（8字节）
void createDumpPayload(rio *payload, robj *o, robj *key) {
    unsigned char buf[2];
    uint64_t crc;

    /* Serialize the object in an RDB-like format. It consist of an object type
     * byte followed by the serialized object. This is understood by RESTORE. */
    // 以类似RDB的格式来序列化对象，它由 一个表示对象类型的字节 + 序列化后的对象 组成。RESTORE指令能识别这种格式。
    // 先初始化基于buffer的rio，然后依次以RDB形式写入对象类型，对象数据。
    rioInitWithBuffer(payload,sdsempty());
    serverAssert(rdbSaveObjectType(payload,o));
    serverAssert(rdbSaveObject(payload,o,key));

    /* Write the footer, this is how it looks like:
     * ----------------+---------------------+---------------+
     * ... RDB payload | 2 bytes RDB version | 8 bytes CRC64 |
     * ----------------+---------------------+---------------+
     * RDB version and CRC are both in little endian.
     */

    /* RDB version */
    // 填充RDB版本信息
    buf[0] = RDB_VERSION & 0xff;
    buf[1] = (RDB_VERSION >> 8) & 0xff;
    payload->io.buffer.ptr = sdscatlen(payload->io.buffer.ptr,buf,2);

    /* CRC64 */
    // 填充CRC64校验数据
    crc = crc64(0,(unsigned char*)payload->io.buffer.ptr,
                sdslen(payload->io.buffer.ptr));
    memrev64ifbe(&crc);
    payload->io.buffer.ptr = sdscatlen(payload->io.buffer.ptr,&crc,8);
}

/* Verify that the RDB version of the dump payload matches the one of this Redis
 * instance and that the checksum is ok.
 * If the DUMP payload looks valid C_OK is returned, otherwise C_ERR
 * is returned. */
// 验证DUMP数据的RDB版本是否与当前redis实例匹配，验证checksum是否正确。如果DUMP数据有效，返回ok，否则返回err。
int verifyDumpPayload(unsigned char *p, size_t len) {
    unsigned char *footer;
    uint16_t rdbver;
    uint64_t crc;

    /* At least 2 bytes of RDB version and 8 of CRC64 should be present. */
    // dump数据至少10字节，即2字节RDB版本+8字节CRC64校验和。
    if (len < 10) return C_ERR;
    footer = p+(len-10);

    /* Verify RDB version */
    // 验证RDB版本，不能超过当前最大版本。
    rdbver = (footer[1] << 8) | footer[0];
    if (rdbver > RDB_VERSION) return C_ERR;

    if (server.skip_checksum_validation)
        return C_OK;

    /* Verify CRC64 */
    // 如果没有配置跳过校验和验证，这里还要计算校验和，然后与数据中的比对。
    crc = crc64(0,p,len-8);
    memrev64ifbe(&crc);
    return (memcmp(&crc,footer+2,8) == 0) ? C_OK : C_ERR;
}

/* DUMP keyname
 * DUMP is actually not used by Redis Cluster but it is the obvious
 * complement of RESTORE and can be useful for different applications. */
// DUMP命令，实际上在redis里并没有使用，但它可作为RESTORE命令的补充，在不同应用中使用。
void dumpCommand(client *c) {
    robj *o;
    rio payload;

    /* Check if the key is here. */
    // 从DB中查询key
    if ((o = lookupKeyRead(c->db,c->argv[1])) == NULL) {
        addReplyNull(c);
        return;
    }

    /* Create the DUMP encoded representation. */
    // DUMP格式编码对象o
    createDumpPayload(&payload,o,c->argv[1]);

    /* Transfer to the client */
    // 编码后的数据回复client
    addReplyBulkSds(c,payload.io.buffer.ptr);
    return;
}

/* RESTORE key ttl serialized-value [REPLACE] */
// RESTORE命令，还原DUMP格式数据
void restoreCommand(client *c) {
    long long ttl, lfu_freq = -1, lru_idle = -1, lru_clock = -1;
    rio payload;
    int j, type, replace = 0, absttl = 0;
    robj *obj;

    /* Parse additional options */
    // 解析附加选项
    for (j = 4; j < c->argc; j++) {
        int additional = c->argc-j-1;
        if (!strcasecmp(c->argv[j]->ptr,"replace")) {
            // replace标识key存在则替换。如果没有这个标识，但key存在，会回复err。
            replace = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"absttl")) {
            // absttl标识绝对ttl，指定的是某个具体时间。如果没有这个标识，则是相对ttl，设置时使用ttl+当前时间。
            absttl = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"idletime") && additional >= 1 &&
                   lfu_freq == -1)
        {
            // 如果lfu_freq没设置，则紧跟idletime后面的参数表示lru_idle，用于后面设置key的lru时钟。
            // 显然idletime和freq参数从前往后，谁先设置谁优先。
            if (getLongLongFromObjectOrReply(c,c->argv[j+1],&lru_idle,NULL)
                    != C_OK) return;
            if (lru_idle < 0) {
                addReplyError(c,"Invalid IDLETIME value, must be >= 0");
                return;
            }
            // 获取当前lru时钟
            lru_clock = LRU_CLOCK();
            j++; /* Consume additional arg. */
        } else if (!strcasecmp(c->argv[j]->ptr,"freq") && additional >= 1 &&
                   lru_idle == -1)
        {
            // 如果lru_idle没设置，则紧跟freq后面的参数表示lfu_freq，用于后面设置key的lfu频率。
            if (getLongLongFromObjectOrReply(c,c->argv[j+1],&lfu_freq,NULL)
                    != C_OK) return;
            if (lfu_freq < 0 || lfu_freq > 255) {
                addReplyError(c,"Invalid FREQ value, must be >= 0 and <= 255");
                return;
            }
            j++; /* Consume additional arg. */
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    /* Make sure this key does not already exist here... */
    // 如果不是replace，我们需要检查当前db中是否key已存在。
    robj *key = c->argv[1];
    if (!replace && lookupKeyWrite(c->db,key) != NULL) {
        addReplyErrorObject(c,shared.busykeyerr);
        return;
    }

    /* Check if the TTL value makes sense */
    // 获取检查TTL参数值。
    if (getLongLongFromObjectOrReply(c,c->argv[2],&ttl,NULL) != C_OK) {
        return;
    } else if (ttl < 0) {
        addReplyError(c,"Invalid TTL value, must be >= 0");
        return;
    }

    /* Verify RDB version and data checksum. */
    // 校验DUMP数据的RDB版本和数据校验和
    if (verifyDumpPayload(c->argv[3]->ptr,sdslen(c->argv[3]->ptr)) == C_ERR)
    {
        addReplyError(c,"DUMP payload version or checksum are wrong");
        return;
    }

    // 初始化基于buf的rio
    rioInitWithBuffer(&payload,c->argv[3]->ptr);
    // 解析数据type 和 数据对象
    if (((type = rdbLoadObjectType(&payload)) == -1) ||
        ((obj = rdbLoadObject(type,&payload,key->ptr)) == NULL))
    {
        addReplyError(c,"Bad data format");
        return;
    }

    /* Remove the old key if needed. */
    // 如果是replace，则需要先从db中删除原key。
    int deleted = 0;
    if (replace)
        deleted = dbDelete(c->db,key);

    // 如果不是绝对ttl时间，则ttl+当前时间，计算出过期的绝对时间值。
    if (ttl && !absttl) ttl+=mstime();
    if (ttl && checkAlreadyExpired(ttl)) {
        // 如果要设置的ttl已经过期了，那么我们不会再将该数据写入db。
        // 另外如果前面有老数据被我们删除了，那么我们需要触发key变更的通知，基于keyspace del命令事件通知。
        if (deleted) {
            rewriteClientCommandVector(c,2,shared.del,key);
            signalModifiedKey(c,c->db,key);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
            server.dirty++;
        }
        decrRefCount(obj);
        addReply(c, shared.ok);
        return;
    }

    /* Create the key and set the TTL if any */
    // 将key加入db中，如果有ttl则设置过期时间。
    dbAdd(c->db,key,obj);
    if (ttl) {
        setExpire(c,c->db,key,ttl);
    }
    // 设置key的lru/lfu。
    objectSetLRUOrLFU(obj,lfu_freq,lru_idle,lru_clock,1000);
    // 通知key变更事件。
    signalModifiedKey(c,c->db,key);
    notifyKeyspaceEvent(NOTIFY_GENERIC,"restore",key,c->db->id);
    addReply(c,shared.ok);
    server.dirty++;
}

/* MIGRATE socket cache implementation.
 *
 * We take a map between host:ip and a TCP socket that we used to connect
 * to this instance in recent time.
 * This sockets are closed when the max number we cache is reached, and also
 * in serverCron() when they are around for more than a few seconds. */
// MIGRATE socket 缓存实现
// 我们使用一个map来缓存最近使用的 host:ip 和 对应的TCP socket连接 的映射关系。
// 当缓存的socket连接数量达到64时，我们会随机选一个释放掉，或者在serverCron()中我们会隔一段时间检查过期的连接释放掉。
#define MIGRATE_SOCKET_CACHE_ITEMS 64 /* max num of items in the cache. */
#define MIGRATE_SOCKET_CACHE_TTL 10 /* close cached sockets after 10 sec. */

typedef struct migrateCachedSocket {
    connection *conn;
    long last_dbid;
    time_t last_use_time;
} migrateCachedSocket;

/* Return a migrateCachedSocket containing a TCP socket connected with the
 * target instance, possibly returning a cached one.
 *
 * This function is responsible of sending errors to the client if a
 * connection can't be established. In this case -1 is returned.
 * Otherwise on success the socket is returned, and the caller should not
 * attempt to free it after usage.
 *
 * If the caller detects an error while using the socket, migrateCloseSocket()
 * should be called so that the connection will be created from scratch
 * the next time. */
// 返回一个migrateCachedSocket对象，该对象包含一个与目前实例建好连接的TCP socket conn，可能是之前缓存的，也可能是新建的。
// 如果连接无法建立，则该函数负责发送err给client，并且返回NULL。否则成功会对应的migrateCachedSocket对象。
// 如果调用者在使用socket conn时发现错误，则应该调用migrateCloseSocket()来释放连接，下一次再重新建立连接。
migrateCachedSocket* migrateGetSocket(client *c, robj *host, robj *port, long timeout) {
    connection *conn;
    sds name = sdsempty();
    migrateCachedSocket *cs;

    /* Check if we have an already cached socket for this ip:port pair. */
    // 使用host、port构建key。
    name = sdscatlen(name,host->ptr,sdslen(host->ptr));
    name = sdscatlen(name,":",1);
    name = sdscatlen(name,port->ptr,sdslen(port->ptr));
    // 检查是否已经有缓存该key对应的socket，如果有则取出直接返回使用，同时更新last_use_time为当前时间。
    cs = dictFetchValue(server.migrate_cached_sockets,name);
    if (cs) {
        sdsfree(name);
        cs->last_use_time = server.unixtime;
        return cs;
    }

    /* No cached socket, create one. */
    // 如果key对应的socket没有被缓存，则我们需要创建一个，并将它加入缓存map。
    // 这里先检查缓存的sockets是否达到限制，如果达到了，因为我们要再创建加入一个，所以这里处理随机选一个淘汰删除掉。
    if (dictSize(server.migrate_cached_sockets) == MIGRATE_SOCKET_CACHE_ITEMS) {
        /* Too many items, drop one at random. */
        // 随机从dict中取一个entry，关闭连接，释放空间，并从缓存map中移除。
        dictEntry *de = dictGetRandomKey(server.migrate_cached_sockets);
        cs = dictGetVal(de);
        connClose(cs->conn);
        zfree(cs);
        dictDelete(server.migrate_cached_sockets,dictGetKey(de));
    }

    /* Create the socket */
    // 创建一个新的socket，与指定的ip、port阻塞方式建立连接。
    conn = server.tls_cluster ? connCreateTLS() : connCreateSocket();
    if (connBlockingConnect(conn, c->argv[1]->ptr, atoi(c->argv[2]->ptr), timeout)
            != C_OK) {
        addReplyError(c,"-IOERR error or timeout connecting to the client");
        connClose(conn);
        sdsfree(name);
        return NULL;
    }
    // 禁用Nagle，实时数据传输，允许小包
    connEnableTcpNoDelay(conn);

    /* Add to the cache and return it to the caller. */
    // 构建 migrateCachedSocket 对象，并加入到缓存map中。
    cs = zmalloc(sizeof(*cs));
    cs->conn = conn;

    cs->last_dbid = -1;
    cs->last_use_time = server.unixtime;
    dictAdd(server.migrate_cached_sockets,name,cs);
    return cs;
}

/* Free a migrate cached connection. */
// 释放一个缓存的socket conn
void migrateCloseSocket(robj *host, robj *port) {
    sds name = sdsempty();
    migrateCachedSocket *cs;

    // 根据host、port构造key
    name = sdscatlen(name,host->ptr,sdslen(host->ptr));
    name = sdscatlen(name,":",1);
    name = sdscatlen(name,port->ptr,sdslen(port->ptr));
    // 获取缓存的socket conn
    cs = dictFetchValue(server.migrate_cached_sockets,name);
    if (!cs) {
        sdsfree(name);
        return;
    }

    // 关闭连接，释放相关内存，并从缓存map中移除该socket conn。
    connClose(cs->conn);
    zfree(cs);
    dictDelete(server.migrate_cached_sockets,name);
    sdsfree(name);
}

// 关闭timeout的socket，并从缓存map中移除
void migrateCloseTimedoutSockets(void) {
    dictIterator *di = dictGetSafeIterator(server.migrate_cached_sockets);
    dictEntry *de;

    while((de = dictNext(di)) != NULL) {
        migrateCachedSocket *cs = dictGetVal(de);

        // 迭代遍历server.migrate_cached_sockets缓存map，检查每个socket conn的上次使用时间到现在是否达到了过期时间。
        // 如果达到了过期时间，则关闭conn，释放缓存的migrateCachedSocket对象，然后从缓存map中移除该entry。
        if ((server.unixtime - cs->last_use_time) > MIGRATE_SOCKET_CACHE_TTL) {
            connClose(cs->conn);
            zfree(cs);
            dictDelete(server.migrate_cached_sockets,dictGetKey(de));
        }
    }
    dictReleaseIterator(di);
}

/* MIGRATE host port key dbid timeout [COPY | REPLACE | AUTH password |
 *         AUTH2 username password]
 *
 * On in the multiple keys form:
 *
 * MIGRATE host port "" dbid timeout [COPY | REPLACE | AUTH password |
 *         AUTH2 username password] KEYS key1 key2 ... keyN */
// migrate 命令，先
void migrateCommand(client *c) {
    migrateCachedSocket *cs;
    int copy = 0, replace = 0, j;
    char *username = NULL;
    char *password = NULL;
    long timeout;
    long dbid;
    // 将要migrate的对象列表
    robj **ov = NULL; /* Objects to migrate. */
    // 将要migrate的key names列表
    robj **kv = NULL; /* Key names. */
    // 用于重写命令为 DEL ... keys ...
    robj **newargv = NULL; /* Used to rewrite the command as DEL ... keys ... */
    rio cmd, payload;
    int may_retry = 1;
    int write_error = 0;
    int argv_rewritten = 0;

    /* To support the KEYS option we need the following additional state. */
    // 为了支持keys选项，我们需要使用下面的状态字段。默认表示只迁移'key'参数。
    // first_key表示，参数中第一个key的index位置。num_keys表示迁移的key的数量。
    int first_key = 3; /* Argument index of the first key. */
    int num_keys = 1;  /* By default only migrate the 'key' argument. */

    /* Parse additional options */
    // 解析附加的选项。
    for (j = 6; j < c->argc; j++) {
        int moreargs = (c->argc-1) - j;
        if (!strcasecmp(c->argv[j]->ptr,"copy")) {
            // copy标识复制，我们不需要删除原db中的key，从而不用生成DEL命令并传播。
            copy = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"replace")) {
            // replace标识迁移过去，如果有相同的key是否进行替换。
            replace = 1;
        } else if (!strcasecmp(c->argv[j]->ptr,"auth")) {
            // auth，后面需要有password参数用于验证。
            if (!moreargs) {
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
            j++;
            password = c->argv[j]->ptr;
            // original_argv中设置password参数为redacted，从而密码不会出现在慢日志中。
            redactClientCommandArgument(c,j);
        } else if (!strcasecmp(c->argv[j]->ptr,"auth2")) {
            // auth2，后面需要username、password两个参数进行验证。
            if (moreargs < 2) {
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
            // 获取username、password，并将两个参数在original_argv设置为redacted，从而不再慢日志中展示。
            username = c->argv[++j]->ptr;
            redactClientCommandArgument(c,j);
            password = c->argv[++j]->ptr;
            redactClientCommandArgument(c,j);
        } else if (!strcasecmp(c->argv[j]->ptr,"keys")) {
            // keys参数，处理多个keys的迁移。当有keys参数时第三个参数key值应该为空字符串。
            if (sdslen(c->argv[3]->ptr) != 0) {
                addReplyError(c,
                    "When using MIGRATE KEYS option, the key argument"
                    " must be set to the empty string");
                return;
            }
            // 更新第一个key的index，以及总共keys的数量
            first_key = j+1;
            num_keys = c->argc - j - 1;
            break; /* All the remaining args are keys. */
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    /* Sanity check */
    // 获取timeout、dbid参数，并进行相关检测
    if (getLongFromObjectOrReply(c,c->argv[5],&timeout,NULL) != C_OK ||
        getLongFromObjectOrReply(c,c->argv[4],&dbid,NULL) != C_OK)
    {
        return;
    }
    if (timeout <= 0) timeout = 1000;

    /* Check if the keys are here. If at least one key is to migrate, do it
     * otherwise if all the keys are missing reply with "NOKEY" to signal
     * the caller there was nothing to migrate. We don't return an error in
     * this case, since often this is due to a normal condition like the key
     * expiring in the meantime. */
    // 检查是否所有keys都在当前节点。如果至少有一个key需要进行迁移，则进行操作；否则所有的keys都找不到，则回复"NOKEY"通知调用者没做任何迁移。
    // 在这种情况下，我们不会返回错误，因为通常这是由于key过期导致的正常现象。
    ov = zrealloc(ov,sizeof(robj*)*num_keys);
    kv = zrealloc(kv,sizeof(robj*)*num_keys);
    int oi = 0;

    for (j = 0; j < num_keys; j++) {
        // 遍历参数中的keys，在当前db中查找。如果找到了则将key和value分别加入到kv和ov列表中。
        if ((ov[oi] = lookupKeyRead(c->db,c->argv[first_key+j])) != NULL) {
            kv[oi] = c->argv[first_key+j];
            oi++;
        }
    }
    // 判断最终需要迁移的keys数量，如果为0，则返回+NOKEY。
    num_keys = oi;
    if (num_keys == 0) {
        zfree(ov); zfree(kv);
        addReplySds(c,sdsnew("+NOKEY\r\n"));
        return;
    }

try_again:
    write_error = 0;

    /* Connect */
    // 拿到与参数指定的ip、port的连接，可能是新建的，也可能是从缓存中获取的。
    cs = migrateGetSocket(c,c->argv[1],c->argv[2],timeout);
    if (cs == NULL) {
        // 获取连接失败，释放前面分配的ov、kv空间，因为err信息已经在migrateGetSocket()发送了，这里直接返回。
        zfree(ov); zfree(kv);
        return; /* error sent to the client by migrateGetSocket() */
    }

    // 初始化基于buffer的rio
    rioInitWithBuffer(&cmd,sdsempty());

    /* Authentication */
    // 如果有password，传输AUTH命令。AUTH [username] password
    if (password) {
        int arity = username ? 3 : 2;
        serverAssertWithInfo(c,NULL,rioWriteBulkCount(&cmd,'*',arity));
        serverAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,"AUTH",4));
        if (username) {
            serverAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,username,
                                 sdslen(username)));
        }
        serverAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,password,
            sdslen(password)));
    }

    /* Send the SELECT command if the current DB is not already selected. */
    // 如果还没有选择当前db，则发送select db命令。
    int select = cs->last_dbid != dbid; /* Should we emit SELECT? */
    if (select) {
        serverAssertWithInfo(c,NULL,rioWriteBulkCount(&cmd,'*',2));
        serverAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,"SELECT",6));
        serverAssertWithInfo(c,NULL,rioWriteBulkLongLong(&cmd,dbid));
    }

    // 我们发现的没有过期的keys数量。
    // 注意因为序列号大keys可能需要一些时间，所以某些keys可能在lookupKey()查询时没有过期，但后面到这里过期了。
    int non_expired = 0; /* Number of keys that we'll find non expired.
                            Note that serializing large keys may take some time
                            so certain keys that were found non expired by the
                            lookupKey() function, may be expired later. */

    /* Create RESTORE payload and generate the protocol to call the command. */
    // 构建RESTORE命令，并生成协议格式，从而可以直接调用执行。
    for (j = 0; j < num_keys; j++) {
        long long ttl = 0;
        long long expireat = getExpire(c->db,kv[j]);

        // 根据查到的过期时间判断key是否已经过期，过期则跳过。
        if (expireat != -1) {
            ttl = expireat-mstime();
            if (ttl < 0) {
                continue;
            }
            if (ttl < 1) ttl = 1;
        }

        /* Relocate valid (non expired) keys and values into the array in successive
         * positions to remove holes created by the keys that were present
         * in the first lookup but are now expired after the second lookup. */
        // 将有效（没过期）的keys和values重新放到数组中从头开始的联系位置。从而删除第一次查看有效，但第二次查询过期的数据。
        ov[non_expired] = ov[j];
        kv[non_expired++] = kv[j];

        serverAssertWithInfo(c,NULL,
            rioWriteBulkCount(&cmd,'*',replace ? 5 : 4));

        // 集群模式使用RESTORE-ASKING，否则使用RESTORE命令
        // RESTORE key ttl serialized-value [REPLACE]
        if (server.cluster_enabled)
            serverAssertWithInfo(c,NULL,
                rioWriteBulkString(&cmd,"RESTORE-ASKING",14));
        else
            serverAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,"RESTORE",7));
        // rio写入key
        serverAssertWithInfo(c,NULL,sdsEncodedObject(kv[j]));
        serverAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,kv[j]->ptr,
                sdslen(kv[j]->ptr)));
        // rio写入过期时间
        serverAssertWithInfo(c,NULL,rioWriteBulkLongLong(&cmd,ttl));

        /* Emit the payload argument, that is the serialized object using
         * the DUMP format. */
        // 生成DUMP格式的value对象。并写入rio。
        createDumpPayload(&payload,ov[j],kv[j]);
        serverAssertWithInfo(c,NULL,
            rioWriteBulkString(&cmd,payload.io.buffer.ptr,
                               sdslen(payload.io.buffer.ptr)));
        sdsfree(payload.io.buffer.ptr);

        /* Add the REPLACE option to the RESTORE command if it was specified
         * as a MIGRATE option. */
        // 如果指定了replace，这里为RESTORE命令加上REPLACE选项。
        if (replace)
            serverAssertWithInfo(c,NULL,rioWriteBulkString(&cmd,"REPLACE",7));
    }

    /* Fix the actual number of keys we are migrating. */
    // 修复正在需要迁移的keys数量
    num_keys = non_expired;

    /* Transfer the query to the other node in 64K chunks. */
    // 传输迁移RESTORE请求到其他节点。一次最大传输64K。
    errno = 0;
    {
        sds buf = cmd.io.buffer.ptr;
        size_t pos = 0, towrite;
        int nwritten = 0;

        // 只要没传输完就一直循环处理
        while ((towrite = sdslen(buf)-pos) > 0) {
            towrite = (towrite > (64*1024) ? (64*1024) : towrite);
            // 同步传输。一次最多64K
            nwritten = connSyncWrite(cs->conn,buf+pos,towrite,timeout);
            if (nwritten != (signed)towrite) {
                // 如果出现部分写，或写报错，则处理socket err。
                write_error = 1;
                goto socket_err;
            }
            pos += nwritten;
        }
    }

    char buf0[1024]; /* Auth reply. */
    char buf1[1024]; /* Select reply. */
    char buf2[1024]; /* Restore reply. */

    /* Read the AUTH reply if needed. */
    // 同步读取AUTH命令的回复。
    if (password && connSyncReadLine(cs->conn, buf0, sizeof(buf0), timeout) <= 0)
        goto socket_err;

    /* Read the SELECT reply if needed. */
    // 同步读取SELECT命令回复。
    if (select && connSyncReadLine(cs->conn, buf1, sizeof(buf1), timeout) <= 0)
        goto socket_err;

    /* Read the RESTORE replies. */
    // 读取RESTORE命令的回复并处理。
    int error_from_target = 0;
    int socket_error = 0;
    // 当前加入newargv的用于复制的DEL命令的keys参数的数量（包含DEL指令）。
    int del_idx = 1; /* Index of the key argument for the replicated DEL op. */

    /* Allocate the new argument vector that will replace the current command,
     * to propagate the MIGRATE as a DEL command (if no COPY option was given).
     * We allocate num_keys+1 because the additional argument is for "DEL"
     * command name itself. */
    // 如果没有COPY选项的话，我们分配新的参数向量，替换当前的命令，MIGRATE实际上对于当前节点算DEL。
    // 分配num_keys+1个对象空间，因为除了迁移的keys，还有DEL命令占一个对象。
    if (!copy) newargv = zmalloc(sizeof(robj*)*(num_keys+1));

    for (j = 0; j < num_keys; j++) {
        // 每个迁移的key，同步读取回复。
        if (connSyncReadLine(cs->conn, buf2, sizeof(buf2), timeout) <= 0) {
            socket_error = 1;
            break;
        }
        if ((password && buf0[0] == '-') ||
            (select && buf1[0] == '-') ||
            buf2[0] == '-')
        {
            /* On error assume that last_dbid is no longer valid. */
            // 对于任意一个key，如果AUTH、SELECT或RESTORE有一个失败，则回复err给client。同时设置last_dbid无效。
            if (!error_from_target) {
                cs->last_dbid = -1;
                char *errbuf;
                if (password && buf0[0] == '-') errbuf = buf0;
                else if (select && buf1[0] == '-') errbuf = buf1;
                else errbuf = buf2;

                error_from_target = 1;
                addReplyErrorFormat(c,"Target instance replied with error: %s",
                    errbuf+1);
            }
        } else {
            // 没有失败，成功传输。
            if (!copy) {
                /* No COPY option: remove the local key, signal the change. */
                // 如果不是COPY，则我们需要在当前节点移除key，并通知key变更。
                dbDelete(c->db,kv[j]);
                signalModifiedKey(c,c->db,kv[j]);
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",kv[j],c->db->id);
                server.dirty++;

                /* Populate the argument vector to replace the old one. */
                // 移除key，需要传播到slaves或AOF，所以这里收集迁移成功的keys，后面重写为DEL命令，用于传播。
                newargv[del_idx++] = kv[j];
                incrRefCount(kv[j]);
            }
        }
    }

    /* On socket error, if we want to retry, do it now before rewriting the
     * command vector. We only retry if we are sure nothing was processed
     * and we failed to read the first reply (j == 0 test). */
    // 出现了socket err，如果我们想重试，则现在处理，在重写命令行参数向量前进行重试。
    // 我们仅在什么key都没有迁移的情况下，即处理第一个key（j==0）就出错时才进行重试。
    if (!error_from_target && socket_error && j == 0 && may_retry &&
        errno != ETIMEDOUT)
    {
        // 条件满足，跳转到socket_err，进行重试。
        goto socket_err; /* A retry is guaranteed because of tested conditions.*/
    }

    /* On socket errors, close the migration socket now that we still have
     * the original host/port in the ARGV. Later the original command may be
     * rewritten to DEL and will be too later. */
    // 有socket err，不重试，这里需要close迁移的socket。
    // 因为现在我们还有迁移的host/port在命令参数中，等后面重写成DEL命令，就找不到地址信息，就没法移除缓存的socket了。
    if (socket_error) migrateCloseSocket(c->argv[1],c->argv[2]);

    if (!copy) {
        /* Translate MIGRATE as DEL for replication/AOF. Note that we do
         * this only for the keys for which we received an acknowledgement
         * from the receiving Redis server, by using the del_idx index. */
        // 不是COPY，这里需要将MIGRATE命令重写为DEL用于传播到slaves/AOF。
        // 注意我们仅对传输成功，收到回复确认的keys进行改写为DEL，keys总数为del_idx-1。
        if (del_idx > 1) {
            // 如果有keys需要删除，构造DEL命令。
            newargv[0] = createStringObject("DEL",3);
            /* Note that the following call takes ownership of newargv. */
            // 使用newargv来完全替换原来的MIGRATE命令。
            replaceClientCommandVector(c,del_idx,newargv);
            argv_rewritten = 1;
        } else {
            /* No key transfer acknowledged, no need to rewrite as DEL. */
            // 没有keys传输成功，不需要重写为DEL命令，释放分配的空间。
            zfree(newargv);
        }
        // newargv置空，确保后面再对它进行zfree时不报错。
        newargv = NULL; /* Make it safe to call zfree() on it in the future. */
    }

    /* If we are here and a socket error happened, we don't want to retry.
     * Just signal the problem to the client, but only do it if we did not
     * already queue a different error reported by the destination server. */
    // 如果我们遇到socket err，且不想重试，则向client发送socket err信息。
    // 注意只有在我们没有从目标节点收到其他错误回复时才回复client socket err。
    // 因为如果目标节点执行命令错误时，前面循环处理命令的时候就回复了，不再重复回复。
    if (!error_from_target && socket_error) {
        may_retry = 0;
        goto socket_err;
    }

    if (!error_from_target) {
        /* Success! Update the last_dbid in migrateCachedSocket, so that we can
         * avoid SELECT the next time if the target DB is the same. Reply +OK.
         *
         * Note: If we reached this point, even if socket_error is true
         * still the SELECT command succeeded (otherwise the code jumps to
         * socket_err label. */
        // 如果目标节点没有回复错误。则迁移成功，回复+OK。
        // 更新last_dbid，下一次迁移相同的DB时，就不需要再发送SELECT命令了。
        // 注意：如果我们执行到这里，即使有socket err，SELECT命令仍然执行成功了，否则代码将跳转到socket_err执行。
        cs->last_dbid = dbid;
        addReply(c,shared.ok);
    } else {
        /* On error we already sent it in the for loop above, and set
         * the currently selected socket to -1 to force SELECT the next time. */
        // 如果目标节点回复命令执行出错，我们这里什么都不需要处理。因为前面循环处理命令的时候就回复了err，且设置了last_dbid=-1。
    }

    // 释放分配的空间
    sdsfree(cmd.io.buffer.ptr);
    zfree(ov); zfree(kv); zfree(newargv);
    return;

/* On socket errors we try to close the cached socket and try again.
 * It is very common for the cached socket to get closed, if just reopening
 * it works it's a shame to notify the error to the caller. */
// 出现了socket err，我们尝试close 缓存的socket，并再次尝试处理。
// 缓存的socket关闭是很常见的，如果重试重新建立socket就可以继续，那么直接告诉调用者出错了是一种耻辱。
socket_err:
    /* Cleanup we want to perform in both the retry and no retry case.
     * Note: Closing the migrate socket will also force SELECT next time. */
    // 处理清理工作。我们希望对于重试和不重试两种情况统一处理。
    // 注意：关闭migrate socket也会导致下一次处理时，先执行SELECT命令，因为新建的socket，last_dbid默认为-1。
    sdsfree(cmd.io.buffer.ptr);

    /* If the command was rewritten as DEL and there was a socket error,
     * we already closed the socket earlier. While migrateCloseSocket()
     * is idempotent, the host/port arguments are now gone, so don't do it
     * again. */
    // 如果命令已经重写为DEL了，因为移除缓存需要目标host/port，所以我们在重写前就处理了缓存清理。
    // 这里针对没有进行命令重写的情况，关闭移除缓存socket。
    if (!argv_rewritten) migrateCloseSocket(c->argv[1],c->argv[2]);
    // 释放重写命令分配的空间。如果后面有重试，会重新再分配内存。
    zfree(newargv);
    newargv = NULL; /* This will get reallocated on retry. */

    /* Retry only if it's not a timeout and we never attempted a retry
     * (or the code jumping here did not set may_retry to zero). */
    // 如果不是timeout，并且我们想要重试，则进行重试。只会重试一次，所以这里我们会将may_retry设置为0，下次就不再重试处理了。
    if (errno != ETIMEDOUT && may_retry) {
        may_retry = 0;
        goto try_again;
    }

    /* Cleanup we want to do if no retry is attempted. */
    // 如果没有重试的话，清理并回复。
    zfree(ov); zfree(kv);
    addReplySds(c,
        sdscatprintf(sdsempty(),
            "-IOERR error or timeout %s to target instance\r\n",
            write_error ? "writing" : "reading"));
    return;
}

/* -----------------------------------------------------------------------------
 * Cluster functions related to serving / redirecting clients
 * -------------------------------------------------------------------------- */

/* The ASKING command is required after a -ASK redirection.
 * The client should issue ASKING before to actually send the command to
 * the target instance. See the Redis Cluster specification for more
 * information. */
// 当收到-ASK重定向回复时，我们转到重定向节点重新执行命令前，需要先执行ASKING指令。ASKING命令只在集群模式下可用。
// 服务端收到ASKING指令，会将该client设置CLIENT_ASKING标识，表示下一个命令必须要由当前节点处理。
void askingCommand(client *c) {
    // 只有集群模式才可以使用该命令
    if (server.cluster_enabled == 0) {
        addReplyError(c,"This instance has cluster support disabled");
        return;
    }
    // 设置CLIENT_ASKING标识
    c->flags |= CLIENT_ASKING;
    addReply(c,shared.ok);
}

/* The READONLY command is used by clients to enter the read-only mode.
 * In this mode slaves will not redirect clients as long as clients access
 * with read-only commands to keys that are served by the slave's master. */
// client使用这个命令是自己进入read-only模式。这个模式下，如果client是向slaves发送指令，client将不会被重定向到对应的master。
void readonlyCommand(client *c) {
    // 集群模式下才可用
    if (server.cluster_enabled == 0) {
        addReplyError(c,"This instance has cluster support disabled");
        return;
    }
    // 设置CLIENT_READONLY标识
    c->flags |= CLIENT_READONLY;
    addReply(c,shared.ok);
}

/* The READWRITE command just clears the READONLY command state. */
// READWRITE命令仅用于清除READONLY状态。
void readwriteCommand(client *c) {
    c->flags &= ~CLIENT_READONLY;
    addReply(c,shared.ok);
}

/* Return the pointer to the cluster node that is able to serve the command.
 * For the function to succeed the command should only target either:
 *
 * 1) A single key (even multiple times like LPOPRPUSH mylist mylist).
 * 2) Multiple keys in the same hash slot, while the slot is stable (no
 *    resharding in progress).
 *
 * On success the function returns the node that is able to serve the request.
 * If the node is not 'myself' a redirection must be performed. The kind of
 * redirection is specified setting the integer passed by reference
 * 'error_code', which will be set to CLUSTER_REDIR_ASK or
 * CLUSTER_REDIR_MOVED.
 *
 * When the node is 'myself' 'error_code' is set to CLUSTER_REDIR_NONE.
 *
 * If the command fails NULL is returned, and the reason of the failure is
 * provided via 'error_code', which will be set to:
 *
 * CLUSTER_REDIR_CROSS_SLOT if the request contains multiple keys that
 * don't belong to the same hash slot.
 *
 * CLUSTER_REDIR_UNSTABLE if the request contains multiple keys
 * belonging to the same slot, but the slot is not stable (in migration or
 * importing state, likely because a resharding is in progress).
 *
 * CLUSTER_REDIR_DOWN_UNBOUND if the request addresses a slot which is
 * not bound to any node. In this case the cluster global state should be
 * already "down" but it is fragile to rely on the update of the global state,
 * so we also handle it here.
 *
 * CLUSTER_REDIR_DOWN_STATE and CLUSTER_REDIR_DOWN_RO_STATE if the cluster is
 * down but the user attempts to execute a command that addresses one or more keys. */
// 函数返回能够执行该命令的集群节点。当前函数能够成功选择节点，执行的命令应该满足如下任一条件：
//   1、仅仅只单个key，即使对同一个key操作多次。如LPOPRPUSH mylist mylist。
//   2、处理多个key，这些key都在相同的hsah slot，且hash slot是稳定的，没有在resharding。
// 函数执行成功将返回能够服务当前请求的节点。如果节点不是"myself"，则要处理重定向操作。
// 重定向类型由参数error_code指定返回，CLUSTER_REDIR_MOVED正常重定向，CLUSTER_REDIR_ASK表示hash slot在迁移。
// 当返回节点是"myself"时，error_code返回 CLUSTER_REDIR_NONE。
//
// 当命令执行选择节点有异常时，函数会返回NULL，此时失败信息通过error_code返回。主要有如下err：
//   1、CLUSTER_REDIR_CROSS_SLOT，请求包含多个keys，且keys不属于相同的slot
//   2、CLUSTER_REDIR_UNSTABLE，请求包含多个keys属于相同slot，但slot不稳定，处于migration或importing状态（可能在resharding）。
//   3、CLUSTER_REDIR_DOWN_UNBOUND，key对应的slot不在任何可用节点。这种情况时，集群的全局状态实际上时down的。
//      但是依赖全局状态的更新是很脆弱的，所以仍然在这里通过slot来判断处理。
//   4、CLUSTER_REDIR_DOWN_STATE 和 CLUSTER_REDIR_DOWN_RO_STATE，集群是down状态，但用户尝试执行针对一个或多个key的命令。
clusterNode *getNodeByQuery(client *c, struct redisCommand *cmd, robj **argv, int argc, int *hashslot, int *error_code) {
    clusterNode *n = NULL;
    robj *firstkey = NULL;
    int multiple_keys = 0;
    multiState *ms, _ms;
    multiCmd mc;
    int i, slot = 0, migrating_slot = 0, importing_slot = 0, missing_keys = 0;

    /* Allow any key to be set if a module disabled cluster redirections. */
    // 如果module禁止了集群重定向，允许任何key在本节点执行。
    // module禁止一些集群操作，主要用于通过module实现基于redis的上层分布式系统。
    if (server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_REDIRECTION)
        return myself;

    /* Set error code optimistically for the base case. */
    // error_code设置默认值
    if (error_code) *error_code = CLUSTER_REDIR_NONE;

    /* Modules can turn off Redis Cluster redirection: this is useful
     * when writing a module that implements a completely different
     * distributed system. */

    /* We handle all the cases as if they were EXEC commands, so we have
     * a common code path for everything */
    // 我们将所有情况都按照EXEC命令方式来处理，这样我们可以有统一的代码逻辑。
    if (cmd->proc == execCommand) {
        /* If CLIENT_MULTI flag is not set EXEC is just going to return an
         * error. */
        // 如果命令是exec，而client的flag不是CLIENT_MULTI，直接返回myself，后续exec执行时会返回err。
        if (!(c->flags & CLIENT_MULTI)) return myself;
        // 如果是正常CLIENT_MULTI，则获取MULTI/EXEC state，后面来检查队列中的命令处理。
        ms = &c->mstate;
    } else {
        /* In order to have a single codepath create a fake Multi State
         * structure if the client is not in MULTI/EXEC state, this way
         * we have a single codepath below. */
        // 不是exec命令，这里构造一个Multi state结构，这样后面处理逻辑就一致了。
        // 像极了mysql中单个语句也是一个事务处理形式。
        ms = &_ms;
        _ms.commands = &mc;
        _ms.count = 1;
        mc.argv = argv;
        mc.argc = argc;
        mc.cmd = cmd;
    }

    /* Check that all the keys are in the same hash slot, and obtain this
     * slot and the node associated. */
    // 检查所有的key是否都在相同的hash slot上，并获取这个slot和关联的节点。
    // 遍历所有的multiCmd，每个命令都检查所有key。
    for (i = 0; i < ms->count; i++) {
        struct redisCommand *mcmd;
        robj **margv;
        int margc, *keyindex, numkeys, j;

        mcmd = ms->commands[i].cmd;
        margc = ms->commands[i].argc;
        margv = ms->commands[i].argv;

        // 拿到当前命令的所有key index列表。
        getKeysResult result = GETKEYS_RESULT_INIT;
        numkeys = getKeysFromCommand(mcmd,margv,margc,&result);
        keyindex = result.keys;

        // 挨个key进行处理
        for (j = 0; j < numkeys; j++) {
            // 获取索引指向的key
            robj *thiskey = margv[keyindex[j]];
            // 获取key的hash slot
            int thisslot = keyHashSlot((char*)thiskey->ptr,
                                       sdslen(thiskey->ptr));

            // firstkey表示我们处理的全局第一个key，所有cmd的所有keys中的第一个，用于确定唯一slot和node。
            // 这里为NULL，说明我们当前处理的key是第一个。
            if (firstkey == NULL) {
                /* This is the first key we see. Check what is the slot
                 * and node. */
                // 这里处理的是第一个key，检查slot和node。
                firstkey = thiskey;
                slot = thisslot;
                n = server.cluster->slots[slot];

                /* Error: If a slot is not served, we are in "cluster down"
                 * state. However the state is yet to be updated, so this was
                 * not trapped earlier in processCommand(). Report the same
                 * error to the client. */
                // 如果slot目前没有关联到可用节点，当前处理"cluster down"状态，但没有及时更新，所以没有及早发现。
                // 这里返回相同的err CLUSTER_REDIR_DOWN_UNBOUND给client
                if (n == NULL) {
                    getKeysFreeResult(&result);
                    if (error_code)
                        *error_code = CLUSTER_REDIR_DOWN_UNBOUND;
                    return NULL;
                }

                /* If we are migrating or importing this slot, we need to check
                 * if we have all the keys in the request (the only way we
                 * can safely serve the request, otherwise we return a TRYAGAIN
                 * error). To do so we set the importing/migrating state and
                 * increment a counter for every missing key. */
                // 如果slot指向的节点是"myself"，但是我们正在对这个slot做迁出；或者我们正在从某节点迁入该slot。
                // 这两个操作都与当前节点有关，可能有部分数据在当前节点，有部分在其他节点。
                // 所以我们需要check请求中的所有key，保证安全的服务请求，否则返回TRYAGAIN err。
                // 这里我们对这两种情况设置importing/migrating状态，并在每次当前节点missing key时对应+1。
                if (n == myself &&
                    server.cluster->migrating_slots_to[slot] != NULL)
                {
                    // slot迁出，设置migrating_slot
                    migrating_slot = 1;
                } else if (server.cluster->importing_slots_from[slot] != NULL) {
                    // slot迁入，设置importing_slot
                    importing_slot = 1;
                }
            } else {
                /* If it is not the first key, make sure it is exactly
                 * the same key as the first we saw. */
                // 不是第一个key，需要检查是否跟第一个key是同一个。
                // 如果是，则不需要处理；如果不是，则需要判断slot是否一致。
                // 理论上不是在同一个节点就可以了，为什么要限制slot完全一致？
                // 实现起来很复杂吧，这样要考虑各个slot的missing key。
                if (!equalStringObjects(firstkey,thiskey)) {
                    if (slot != thisslot) {
                        /* Error: multiple keys from different slots. */
                        // slot不一致，返回CLUSTER_REDIR_CROSS_SLOT err
                        getKeysFreeResult(&result);
                        if (error_code)
                            *error_code = CLUSTER_REDIR_CROSS_SLOT;
                        return NULL;
                    } else {
                        /* Flag this request as one with multiple different
                         * keys. */
                        // slot一致，但key不一样，这里标记当前请求是有多个不同key的请求。
                        multiple_keys = 1;
                    }
                }
            }

            /* Migrating / Importing slot? Count keys we don't have. */
            // 如果是Migrating/Importing的slot，需要检查当前节点key是否存在，不存在则missing_keys+1。
            if ((migrating_slot || importing_slot) &&
                lookupKeyRead(&server.db[0],thiskey) == NULL)
            {
                missing_keys++;
            }
        }
        getKeysFreeResult(&result);
    }

    /* No key at all in command? then we can serve the request
     * without redirections or errors in all the cases. */
    // 如果有key的话，前面n为NULL会直接返回。
    // 能到这里，说明当前执行的命令没有key，当前节点可以直接执行，返回myself。
    if (n == NULL) return myself;

    /* Cluster is globally down but we got keys? We only serve the request
     * if it is a read command and when allow_reads_when_down is enabled. */
    // 走到这里n!=NULL且有key。
    if (server.cluster->state != CLUSTER_OK) {
        // 如果集群不是ok状态，需要判断是否允许allow_reads_when_down。
        // 如果集群fail时不允许读，则返回CLUSTER_REDIR_DOWN_STATE err
        // 如果fail时允许读，但该命令是写命令，返回CLUSTER_REDIR_DOWN_RO_STATE err。
        // 如果fail时允许读，且命令不是写命令，则该命令可以执行。
        if (!server.cluster_allow_reads_when_down) {
            /* The cluster is configured to block commands when the
             * cluster is down. */
            if (error_code) *error_code = CLUSTER_REDIR_DOWN_STATE;
            return NULL;
        } else if (cmd->flags & CMD_WRITE) {
            /* The cluster is configured to allow read only commands */
            if (error_code) *error_code = CLUSTER_REDIR_DOWN_RO_STATE;
            return NULL;
        } else {
            /* Fall through and allow the command to be executed:
             * this happens when server.cluster_allow_reads_when_down is
             * true and the command is not a write command */
        }
    }

    /* Return the hashslot by reference. */
    // 设置获取到的命令执行的slot
    if (hashslot) *hashslot = slot;

    /* MIGRATE always works in the context of the local node if the slot
     * is open (migrating or importing state). We need to be able to freely
     * move keys among instances in this case. */
    // 如果当前执行的是migrate命令，而当前节点的migrating或importing状态是开启的，总是可以执行。
    // 这种情况，我们需要能够自由的在目前节点间移动key。
    if ((migrating_slot || importing_slot) && cmd->proc == migrateCommand)
        return myself;

    /* If we don't have all the keys and we are migrating the slot, send
     * an ASK redirection. */
    // 如果我们正在做对应slot迁出，且在当前节点没有获取到所有key，则发送ASK重定向。
    // 返回CLUSTER_REDIR_ASK err，同时返回重定向的目标节点。
    if (migrating_slot && missing_keys) {
        if (error_code) *error_code = CLUSTER_REDIR_ASK;
        return server.cluster->migrating_slots_to[slot];
    }

    /* If we are receiving the slot, and the client correctly flagged the
     * request as "ASKING", we can serve the request. However if the request
     * involves multiple keys and we don't have them all, the only option is
     * to send a TRYAGAIN error. */
    // 如果我们正在迁入slot，并且client或请求cmd有ASKING标识，则我们需要处理该请求。
    // 但是我们并没有请求涉及到的所有key（有key miss），那么我们唯一的操作只有返回TRYAGAIN err了。
    if (importing_slot &&
        (c->flags & CLIENT_ASKING || cmd->flags & CMD_ASKING))
    {
        // slot迁入，但有ASKING标识，判断是否是多key操作 且 有key miss。
        if (multiple_keys && missing_keys) {
            // 多key操作，且有key miss了，返回CLUSTER_REDIR_UNSTABLE err
            if (error_code) *error_code = CLUSTER_REDIR_UNSTABLE;
            return NULL;
        } else {
            // 不是多key操作，或者所有key都在当前节点，返回myself
            return myself;
        }
    }

    /* Handle the read-only client case reading from a slave: if this
     * node is a slave and the request is about a hash slot our master
     * is serving, we can reply without redirection. */
    int is_write_command = (c->cmd->flags & CMD_WRITE) ||
                           (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_WRITE));
    // 如果当前client是只读，且执行的命令不是写命令。
    // 而当前节点是前面查到节点n的slave节点，则myself就可以直接处理请求了，不用再重定向到master处理。
    if (c->flags & CLIENT_READONLY &&
        !is_write_command &&
        nodeIsSlave(myself) &&
        myself->slaveof == n)
    {
        return myself;
    }

    /* Base case: just return the right node. However if this node is not
     * myself, set error_code to MOVED since we need to issue a redirection. */
    // 基本情况，前面找到的节点n不是当前节点，则返回CLUSTER_REDIR_MOVED err。
    if (n != myself && error_code) *error_code = CLUSTER_REDIR_MOVED;
    // 最终，不管我们找到的是否是当前节点，我们始终返回key对应slot关联的节点n来执行命令。
    return n;
}

/* Send the client the right redirection code, according to error_code
 * that should be set to one of CLUSTER_REDIR_* macros.
 *
 * If CLUSTER_REDIR_ASK or CLUSTER_REDIR_MOVED error codes
 * are used, then the node 'n' should not be NULL, but should be the
 * node we want to mention in the redirection. Moreover hashslot should
 * be set to the hash slot that caused the redirection. */
// 根据之前查询节点返回的error_code，构造正确的 CLUSTER_REDIR_* 重定向code发送给client。
// 如果返回的是CLUSTER_REDIR_ASK 或 CLUSTER_REDIR_MOVED，n不应为NULL，而应为重定向目标节点。
// 而且hashslot应该设置为引起重定向的hash slot。
void clusterRedirectClient(client *c, clusterNode *n, int hashslot, int error_code) {
    if (error_code == CLUSTER_REDIR_CROSS_SLOT) {
        addReplyError(c,"-CROSSSLOT Keys in request don't hash to the same slot");
    } else if (error_code == CLUSTER_REDIR_UNSTABLE) {
        /* The request spawns multiple keys in the same slot,
         * but the slot is not "stable" currently as there is
         * a migration or import in progress. */
        // 当slot正处理迁入中，请求是多keys操作，且当前节点有key miss。只有这种情况才会返回-TRYAGAIN
        addReplyError(c,"-TRYAGAIN Multiple keys request during rehashing of slot");
    } else if (error_code == CLUSTER_REDIR_DOWN_STATE) {
        addReplyError(c,"-CLUSTERDOWN The cluster is down");
    } else if (error_code == CLUSTER_REDIR_DOWN_RO_STATE) {
        addReplyError(c,"-CLUSTERDOWN The cluster is down and only accepts read commands");
    } else if (error_code == CLUSTER_REDIR_DOWN_UNBOUND) {
        addReplyError(c,"-CLUSTERDOWN Hash slot not served");
    } else if (error_code == CLUSTER_REDIR_MOVED ||
               error_code == CLUSTER_REDIR_ASK)
    {
        /* Redirect to IP:port. Include plaintext port if cluster is TLS but
         * client is non-TLS. */
        // 处理重定向的节点地址。如果是tls cluster但client不是tls，使用plaintext port。
        int use_pport = (server.tls_cluster &&
                         c->conn && connGetType(c->conn) != CONN_TYPE_TLS);
        int port = use_pport && n->pport ? n->pport : n->port;
        addReplyErrorSds(c,sdscatprintf(sdsempty(),
            "-%s %d %s:%d",
            (error_code == CLUSTER_REDIR_ASK) ? "ASK" : "MOVED",
            hashslot, n->ip, port));
    } else {
        serverPanic("getNodeByQuery() unknown error.");
    }
}

/* This function is called by the function processing clients incrementally
 * to detect timeouts, in order to handle the following case:
 *
 * 1) A client blocks with BLPOP or similar blocking operation.
 * 2) The master migrates the hash slot elsewhere or turns into a slave.
 * 3) The client may remain blocked forever (or up to the max timeout time)
 *    waiting for a key change that will never happen.
 *
 * If the client is found to be blocked into a hash slot this node no
 * longer handles, the client is sent a redirection error, and the function
 * returns 1. Otherwise 0 is returned and no operation is performed. */
// 这个函数在clientCron中调用来做阻塞client的重定向处理，避免key被迁移后一直阻塞，主要处理下面的情况：
//  1、一个client阻塞在BLPOP或相似的阻塞操作上。
//  2、master把slots迁移到其他地方，或者master变为了slave。
//  3、client可能会永远保持阻塞状态（或直到超时），等待永远不会变更的key。
// 如果client阻塞于当前节点中某个slot下，而该slot已不被我们自己负责了，则将发送redirection错误给client，函数返回1。否则不做任何操作，返回0.
int clusterRedirectBlockedClientIfNeeded(client *c) {
    // 只有client处于阻塞状态，且阻塞与某些key，我们才进一步判断。因为与slot相关的只有key的阻塞。
    if (c->flags & CLIENT_BLOCKED &&
        (c->btype == BLOCKED_LIST ||
         c->btype == BLOCKED_ZSET ||
         c->btype == BLOCKED_STREAM))
    {
        dictEntry *de;
        dictIterator *di;

        /* If the cluster is down, unblock the client with the right error.
         * If the cluster is configured to allow reads on cluster down, we
         * still want to emit this error since a write will be required
         * to unblock them which may never come.  */
        // 如果当前节点的视角，集群是FAIL状态，则返回正常集群DOWN的err。
        // 如果集群配置为DOWN状态也允许读，我们也返回DOWN err，因为阻塞的client只有写入操作才能唤醒，而写操作现在是不允许的。
        if (server.cluster->state == CLUSTER_FAIL) {
            clusterRedirectClient(c,NULL,0,CLUSTER_REDIR_DOWN_STATE);
            return 1;
        }

        /* All keys must belong to the same slot, so check first key only. */
        // client是因为某个阻塞命令而进入阻塞的，如果命令有多个keys，对于集群操作，我们会在执行命令的时候校验这些keys都处于相同的slot中。
        // 所以阻塞时，加入bpop.keys中的这些keys都一定属于同一个slot，这里我们只用检查第一个key就好了。
        di = dictGetIterator(c->bpop.keys);
        if ((de = dictNext(di)) != NULL) {
            // 先取到entry，然后取出key，再根据key计算对应的slot，最后由slot找到负责它的节点。
            // 如果取到的node是我们自己，则不需要进行调整，可以正常服务该key。
            robj *key = dictGetKey(de);
            int slot = keyHashSlot((char*)key->ptr, sdslen(key->ptr));
            clusterNode *node = server.cluster->slots[slot];

            /* if the client is read-only and attempting to access key that our
             * replica can handle, allow it. */
            // client处于read-only模式，阻塞命令是尝试访问key（非写）。
            // 此时如果我们是slave，但是我们的master负责可以对应的slot，那么我们是可以直接服务的，node设置为我们自己。
            if ((c->flags & CLIENT_READONLY) &&
                !(c->lastcmd->flags & CMD_WRITE) &&
                nodeIsSlave(myself) && myself->slaveof == node)
            {
                node = myself;
            }

            /* We send an error and unblock the client if:
             * 1) The slot is unassigned, emitting a cluster down error.
             * 2) The slot is not handled by this node, nor being imported. */
            // 如果node不是我们自己，且该slot不是处于导入状态。则我们不能再服务该阻塞的client了，需要回复相应的err信息并解除阻塞。
            // 1、当node为NULL，即slot未分配时，返回-CLUSTERDOWN err。
            // 2、当slot不能由当前节点处理，且不是导入状态，则返回-MOVED redirection err
            if (node != myself &&
                server.cluster->importing_slots_from[slot] == NULL)
            {
                if (node == NULL) {
                    clusterRedirectClient(c,NULL,0,
                        CLUSTER_REDIR_DOWN_UNBOUND);
                } else {
                    clusterRedirectClient(c,node,slot,
                        CLUSTER_REDIR_MOVED);
                }
                dictReleaseIterator(di);
                return 1;
            }
        }
        dictReleaseIterator(di);
    }
    return 0;
}
