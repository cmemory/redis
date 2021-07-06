#ifndef __CLUSTER_H
#define __CLUSTER_H

/*-----------------------------------------------------------------------------
 * Redis cluster data structures, defines, exported API.
 *----------------------------------------------------------------------------*/

#define CLUSTER_SLOTS 16384
#define CLUSTER_OK 0          /* Everything looks ok */
#define CLUSTER_FAIL 1        /* The cluster can't work */
#define CLUSTER_NAMELEN 40    /* sha1 hex length */
#define CLUSTER_PORT_INCR 10000 /* Cluster port = baseport + PORT_INCR */

/* The following defines are amount of time, sometimes expressed as
 * multiplicators of the node timeout value (when ending with MULT). */
#define CLUSTER_FAIL_REPORT_VALIDITY_MULT 2 /* Fail report validity. */
#define CLUSTER_FAIL_UNDO_TIME_MULT 2 /* Undo fail if master is back. */
#define CLUSTER_FAIL_UNDO_TIME_ADD 10 /* Some additional time. */
#define CLUSTER_FAILOVER_DELAY 5 /* Seconds */
#define CLUSTER_MF_TIMEOUT 5000 /* Milliseconds to do a manual failover. */
#define CLUSTER_MF_PAUSE_MULT 2 /* Master pause manual failover mult. */
#define CLUSTER_SLAVE_MIGRATION_DELAY 5000 /* Delay for slave migration. */

/* Redirection errors returned by getNodeByQuery(). */
#define CLUSTER_REDIR_NONE 0          /* Node can serve the request. */
#define CLUSTER_REDIR_CROSS_SLOT 1    /* -CROSSSLOT request. */
#define CLUSTER_REDIR_UNSTABLE 2      /* -TRYAGAIN redirection required */
#define CLUSTER_REDIR_ASK 3           /* -ASK redirection required. */
#define CLUSTER_REDIR_MOVED 4         /* -MOVED redirection required. */
#define CLUSTER_REDIR_DOWN_STATE 5    /* -CLUSTERDOWN, global state. */
#define CLUSTER_REDIR_DOWN_UNBOUND 6  /* -CLUSTERDOWN, unbound slot. */
#define CLUSTER_REDIR_DOWN_RO_STATE 7 /* -CLUSTERDOWN, allow reads. */

struct clusterNode;

/* clusterLink encapsulates everything needed to talk with a remote node. */
// clusterLink 概括了跟一个远程节点通信需要的东西。
typedef struct clusterLink {
    // 连接创建的时间
    mstime_t ctime;             /* Link creation time */
    // 与远端节点的连接conn
    connection *conn;           /* Connection to remote node */
    // 数据包发送缓冲区
    sds sndbuf;                 /* Packet send buffer */
    // 数据包接收缓存区（非sds字符串，我们自己控制扩容）
    char *rcvbuf;               /* Packet reception buffer */
    // rcvbuf分配的内存以及使用的内存
    size_t rcvbuf_len;          /* Used size of rcvbuf */
    size_t rcvbuf_alloc;        /* Allocated size of rcvbuf */
    // 关联到该link的节点。
    struct clusterNode *node;   /* Node related to this link if any, or NULL */
} clusterLink;

/* Cluster node flags and macros. */
// 这些宏定义表示集群节点的状态标识
#define CLUSTER_NODE_MASTER 1     /* The node is a master */
#define CLUSTER_NODE_SLAVE 2      /* The node is a slave */
// 集群节点处于可能下线状态（主观Fail）
#define CLUSTER_NODE_PFAIL 4      /* Failure? Need acknowledge */
// 节点已下线（超半数已确认，客观下线）
#define CLUSTER_NODE_FAIL 8       /* The node is believed to be malfunctioning */
// 节点是自己
#define CLUSTER_NODE_MYSELF 16    /* This node is myself */
// 节点正在handshake，即在交换第一个ping
#define CLUSTER_NODE_HANDSHAKE 32 /* We have still to exchange the first ping */
#define CLUSTER_NODE_NOADDR   64  /* We don't know the address of this node */
#define CLUSTER_NODE_MEET 128     /* Send a MEET message to this node */
// 标识该节点能够进行副本迁移
#define CLUSTER_NODE_MIGRATE_TO 256 /* Master eligible for replica migration. */
#define CLUSTER_NODE_NOFAILOVER 512 /* Slave will not try to failover. */
#define CLUSTER_NODE_NULL_NAME "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"

#define nodeIsMaster(n) ((n)->flags & CLUSTER_NODE_MASTER)
#define nodeIsSlave(n) ((n)->flags & CLUSTER_NODE_SLAVE)
#define nodeInHandshake(n) ((n)->flags & CLUSTER_NODE_HANDSHAKE)
#define nodeHasAddr(n) (!((n)->flags & CLUSTER_NODE_NOADDR))
#define nodeWithoutAddr(n) ((n)->flags & CLUSTER_NODE_NOADDR)
#define nodeTimedOut(n) ((n)->flags & CLUSTER_NODE_PFAIL)
#define nodeFailed(n) ((n)->flags & CLUSTER_NODE_FAIL)
#define nodeCantFailover(n) ((n)->flags & CLUSTER_NODE_NOFAILOVER)

/* Reasons why a slave is not able to failover. */
#define CLUSTER_CANT_FAILOVER_NONE 0
#define CLUSTER_CANT_FAILOVER_DATA_AGE 1
#define CLUSTER_CANT_FAILOVER_WAITING_DELAY 2
#define CLUSTER_CANT_FAILOVER_EXPIRED 3
#define CLUSTER_CANT_FAILOVER_WAITING_VOTES 4
#define CLUSTER_CANT_FAILOVER_RELOG_PERIOD (60*5) /* seconds. */

/* clusterState todo_before_sleep flags. */
#define CLUSTER_TODO_HANDLE_FAILOVER (1<<0)
#define CLUSTER_TODO_UPDATE_STATE (1<<1)
#define CLUSTER_TODO_SAVE_CONFIG (1<<2)
#define CLUSTER_TODO_FSYNC_CONFIG (1<<3)
#define CLUSTER_TODO_HANDLE_MANUALFAILOVER (1<<4)

/* Message types.
 *
 * Note that the PING, PONG and MEET messages are actually the same exact
 * kind of packet. PONG is the reply to ping, in the exact format as a PING,
 * while MEET is a special PING that forces the receiver to add the sender
 * as a node (if it is not already in the list). */
// 消息类型。
// 注意PING、PONG和MEET消息实际上是完全相同的packet类型。
// PONG是ping的回复，与PONG格式完全相同。MEET是一种特殊的PING，如果sender不在接收者集群nodes列表中时，强制要求接收者将该sender添加进去。
#define CLUSTERMSG_TYPE_PING 0          /* Ping */
#define CLUSTERMSG_TYPE_PONG 1          /* Pong (reply to Ping) */
#define CLUSTERMSG_TYPE_MEET 2          /* Meet "let's join" message */
#define CLUSTERMSG_TYPE_FAIL 3          /* Mark node xxx as failing */
// Pub/Sub publish命令传播到集群其他节点
#define CLUSTERMSG_TYPE_PUBLISH 4       /* Pub/Sub Publish propagation */
// 发送消息询问是否可以进行故障转移，请求投票
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST 5 /* May I failover? */
// 回复故障转移消息，ACK投票支持
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK 6     /* Yes, you have my vote */
#define CLUSTERMSG_TYPE_UPDATE 7        /* Another node slots configuration */
#define CLUSTERMSG_TYPE_MFSTART 8       /* Pause clients for manual failover */
#define CLUSTERMSG_TYPE_MODULE 9        /* Module cluster API message. */
#define CLUSTERMSG_TYPE_COUNT 10        /* Total number of message types. */

/* Flags that a module can set in order to prevent certain Redis Cluster
 * features to be enabled. Useful when implementing a different distributed
 * system on top of Redis Cluster message bus, using modules. */
// module可以设置这些flag来避免特定的redis集群操作。
// 通常用于使用module，基于redis集群的消息通信，实现不同的分布式系统。
#define CLUSTER_MODULE_FLAG_NONE 0
#define CLUSTER_MODULE_FLAG_NO_FAILOVER (1<<1)
#define CLUSTER_MODULE_FLAG_NO_REDIRECTION (1<<2)

/* This structure represent elements of node->fail_reports. */
// node->fail_reports列表的节点元素
typedef struct clusterNodeFailReport {
    // 通报某节点故障的发起者
    struct clusterNode *node;  /* Node reporting the failure condition. */
    // 通报时间
    mstime_t time;             /* Time of the last report from this node. */
} clusterNodeFailReport;

typedef struct clusterNode {
    // 集群节点创建时间
    mstime_t ctime; /* Node object creation time. */
    // 集群节点name
    char name[CLUSTER_NAMELEN]; /* Node name, hex string, sha1-size */
    // 集群节点的标识
    int flags;      /* CLUSTER_NODE_... */
    // 当前节点被观测到的最近的配置纪元
    uint64_t configEpoch; /* Last configEpoch observed for this node */
    // bitmap标识当前节点负责的slot，对应位为1表示对应slot归当前节点负责。
    unsigned char slots[CLUSTER_SLOTS/8]; /* slots handled by this node */
    // 字符串形式的当前节点负责的slots信息
    sds slots_info; /* Slots info represented by string. */
    // 当前节点负责的slots总数
    int numslots;   /* Number of slots handled by this node */
    // 如果当前节点是master，该字段表示它的slaves节点数。
    int numslaves;  /* Number of slave nodes, if this is a master */
    // 当前节点的slaves节点列表
    struct clusterNode **slaves; /* pointers to slave nodes */
    // 如果当前节点是slave节点，该字段指向当前节点的master节点
    struct clusterNode *slaveof; /* pointer to the master node. Note that it
                                    may be NULL even if the node is a slave
                                    if we don't have the master node in our
                                    tables. */
    // 最近一次发送ping的时间
    mstime_t ping_sent;      /* Unix time we sent latest ping */
    // 最近一次收到pong的时间
    mstime_t pong_received;  /* Unix time we received the pong */
    // 最近一次收到任何数据的时间
    mstime_t data_received;  /* Unix time we received any data */
    // 设置当前节点FAIL标识的时间
    mstime_t fail_time;      /* Unix time when FAIL flag was set */
    // 上一次我们给这个master节点的slave投票的时间（如果当前节点是master）。
    mstime_t voted_time;     /* Last time we voted for a slave of this master */
    // 对于当前节点，我们收到offset的时间。
    mstime_t repl_offset_time;  /* Unix time we received offset for this node */
    // 独立master情况开始时间
    mstime_t orphaned_time;     /* Starting time of orphaned master condition */
    // 这个节点的repl offset
    long long repl_offset;      /* Last known repl offset for this node. */
    // 我们所知道的这个节点的最新ip
    char ip[NET_IP_STR_LEN];  /* Latest known IP address of this node */
    // 我们所知道的这个节点的最新port
    int port;                   /* Latest known clients port (TLS or plain). */
    int pport;                  /* Latest known clients plaintext port. Only used
                                   if the main clients port is for TLS. */
    // 我们所知道的这个节点的最新集群端口
    int cport;                  /* Latest known cluster port of this node. */
    // 与这个节点的TCP/IP连接
    clusterLink *link;          /* TCP/IP link with this node */
    // 发送信号标识这个节点fail的其他节点列表。
    list *fail_reports;         /* List of nodes signaling this as failing */
} clusterNode;

typedef struct clusterState {
    // 总是指向当前节点
    clusterNode *myself;  /* This node */
    // 当前时间纪元
    uint64_t currentEpoch;
    // 集群状态
    int state;            /* CLUSTER_OK, CLUSTER_FAIL, ... */
    // 至少有一个slot的master节点数量
    int size;             /* Num of master nodes with at least one slot */
    // name->clusterNode 的节点集合
    dict *nodes;          /* Hash table of name -> clusterNode structures */
    // 我们最近不想重新加入集群的节点。节点id的集合
    dict *nodes_black_list; /* Nodes we don't re-add for a few seconds. */
    // 该数组记录了16384个槽位中，当前节点所负责的某个槽位正在迁出到哪个节点
    clusterNode *migrating_slots_to[CLUSTER_SLOTS];
    // 该数组记录了16384个槽位中，当前节点正在从哪个节点将某个槽位迁入到本节点中
    clusterNode *importing_slots_from[CLUSTER_SLOTS];
    // slots数组记录了16384个槽位，分别由哪个集群节点负责
    clusterNode *slots[CLUSTER_SLOTS];
    // slots_keys_count数组记录16384个槽位中，每个槽位对应的keys的数量
    uint64_t slots_keys_count[CLUSTER_SLOTS];
    // slots_to_keys，每个slot中拥有哪些key。可快速通过slot，获取到该slot下的所有的keys。
    rax *slots_to_keys;
    /* The following fields are used to take the slave state on elections. */
    // 下面的字段都用于获取slave选举时的状态
    // 上一次或下一次选举时间
    mstime_t failover_auth_time; /* Time of previous or next election. */
    // 请求投票的回复数
    int failover_auth_count;    /* Number of votes received so far. */
    // 如果已经发送了投票请求，则置为true
    int failover_auth_sent;     /* True if we already asked for votes. */
    // 当前slave在本次投票中的排名
    int failover_auth_rank;     /* This slave rank for current auth request. */
    // 本次选举的时间纪元
    uint64_t failover_auth_epoch; /* Epoch of the current election. */
    // 当前slave不能进行故障转移的原因
    int cant_failover_reason;   /* Why a slave is currently not able to
                                   failover. See the CANT_FAILOVER_* macros. */
    /* Manual failover state in common. */
    // 手动故障转移的通用状态
    // 手动故障转移的时间限制，如果当前没有处于手动故障转移期间，该字段值为0
    mstime_t mf_end;            /* Manual failover time limit (ms unixtime).
                                   It is zero if there is no MF in progress. */
    /* Manual failover state of master. */
    // 手动故障转移时master的状态
    // 执行故障转移的slave节点
    clusterNode *mf_slave;      /* Slave performing the manual failover. */
    /* Manual failover state of slave. */
    // 手动故障转移时slave的状态
    // slave需要进行故障转移时，master的offset。如果没有收到的话，该值为-1。
    long long mf_master_offset; /* Master offset the slave needs to start MF
                                   or -1 if still not received. */
    // 非0则表示能启动手动故障转移请求masters投票
    int mf_can_start;           /* If non-zero signal that the manual failover
                                   can start requesting masters vote. */
    /* The following fields are used by masters to take state on elections. */
    // 下面的字段用于masters获取投票状态
    // 上一次投票的时间纪元
    uint64_t lastVoteEpoch;     /* Epoch of the last vote granted. */
    // 表示需要在clusterBeforeSleep()中做一些事情
    int todo_before_sleep; /* Things to do in clusterBeforeSleep(). */
    /* Messages received and sent by type. */
    // 根据类型对发送/接收消息的统计
    long long stats_bus_messages_sent[CLUSTERMSG_TYPE_COUNT];
    long long stats_bus_messages_received[CLUSTERMSG_TYPE_COUNT];
    // 在PFAIL状态的节点数，不包含没有地址的节点。
    long long stats_pfail_nodes;    /* Number of nodes in PFAIL status,
                                       excluding nodes without address. */
} clusterState;

/* Redis cluster messages header */

/* Initially we don't know our "name", but we'll find it once we connect
 * to the first node, using the getsockname() function. Then we'll use this
 * address for all the next messages. */
// 初始我们不知道自己的"name"，但一旦有一个节点与我们连接，我们就可以从getockname()获取。后面我们将使用该地址发送后面的消息。
typedef struct {
    char nodename[CLUSTER_NAMELEN];
    uint32_t ping_sent;
    uint32_t pong_received;
    char ip[NET_IP_STR_LEN];  /* IP address last time it was seen */
    uint16_t port;              /* base port last time it was seen */
    uint16_t cport;             /* cluster port last time it was seen */
    uint16_t flags;             /* node->flags copy */
    uint16_t pport;             /* plaintext-port, when base port is TLS */
    uint16_t notused1;
} clusterMsgDataGossip;

typedef struct {
    char nodename[CLUSTER_NAMELEN];
} clusterMsgDataFail;

typedef struct {
    uint32_t channel_len;
    uint32_t message_len;
    // 这里不能将bulk_data重新声明为bulk_data[]因为这个结构是嵌套的。所以这里使用了一个占位符扩展。
    unsigned char bulk_data[8]; /* 8 bytes just as placeholder. */
} clusterMsgDataPublish;

typedef struct {
    uint64_t configEpoch; /* Config epoch of the specified instance. */
    char nodename[CLUSTER_NAMELEN]; /* Name of the slots owner. */
    unsigned char slots[CLUSTER_SLOTS/8]; /* Slots bitmap. */
} clusterMsgDataUpdate;

typedef struct {
    uint64_t module_id;     /* ID of the sender module. */
    uint32_t len;           /* ID of the sender module. */
    uint8_t type;           /* Type from 0 to 255. */
    unsigned char bulk_data[3]; /* 3 bytes just as placeholder. */
} clusterMsgModule;

union clusterMsgData {
    /* PING, MEET and PONG */
    struct {
        /* Array of N clusterMsgDataGossip structures */
        clusterMsgDataGossip gossip[1];
    } ping;

    /* FAIL */
    struct {
        clusterMsgDataFail about;
    } fail;

    /* PUBLISH */
    struct {
        clusterMsgDataPublish msg;
    } publish;

    /* UPDATE */
    struct {
        clusterMsgDataUpdate nodecfg;
    } update;

    /* MODULE */
    struct {
        clusterMsgModule msg;
    } module;
};

#define CLUSTER_PROTO_VER 1 /* Cluster bus protocol version. */

typedef struct {
    // 消息签名，目前是"RCmb"
    char sig[4];        /* Signature "RCmb" (Redis Cluster message bus). */
    // 消息总长度，包含这些头信息
    uint32_t totlen;    /* Total length of this message */
    // 消息协议版本
    uint16_t ver;       /* Protocol version, currently set to 1. */
    // 对端节点TCP端口
    uint16_t port;      /* TCP base port number. */
    // 消息类型
    uint16_t type;      /* Message type */
    // clusterMsgData的数量，有些类型一次可以包含多条消息。
    uint16_t count;     /* Only used for some kind of messages. */
    // sender节点的当前纪元
    uint64_t currentEpoch;  /* The epoch accordingly to the sending node. */
    // sender是master，则代表它的配置纪元；sender是slave，则代表它的master通知它的最大的纪元
    uint64_t configEpoch;   /* The config epoch if it's a master, or the last
                               epoch advertised by its master if it is a
                               slave. */
    // sender是master，代表复制offset；sender是slave，则代表自己已处理的复制offset
    uint64_t offset;    /* Master replication offset if node is a master or
                           processed replication offset if node is a slave. */
    // sender节点的name，用于接收放从自己集群nodes中查询节点（不存在则根据name创建节点加入）。
    char sender[CLUSTER_NAMELEN]; /* Name of the sender node */
    // sender是master，代表负责的slots信息；sender是slave，则代表对应master负责的slots。
    unsigned char myslots[CLUSTER_SLOTS/8];
    // sender是slave的话，这个字段表示我们master name。
    char slaveof[CLUSTER_NAMELEN];
    // 如果非0，则表示sender节点的IP
    char myip[NET_IP_STR_LEN];    /* Sender IP, if not all zeroed. */
    // 32字节未使用字段
    char notused1[32];  /* 32 bytes reserved for future usage. */
    // sender port信息
    uint16_t pport;      /* Sender TCP plaintext port, if base port is TLS */
    uint16_t cport;      /* Sender TCP cluster bus port */
    // sender节点标识
    uint16_t flags;      /* Sender node flags */
    // sender视角下集群的状态
    unsigned char state; /* Cluster state from the POV of the sender */
    // 消息标识
    unsigned char mflags[3]; /* Message flags: CLUSTERMSG_FLAG[012]_... */
    // 发送的信息数据
    union clusterMsgData data;
} clusterMsg;

#define CLUSTERMSG_MIN_LEN (sizeof(clusterMsg)-sizeof(union clusterMsgData))

/* Message flags better specify the packet content or are used to
 * provide some information about the node state. */
#define CLUSTERMSG_FLAG0_PAUSED (1<<0) /* Master paused for manual failover. */
#define CLUSTERMSG_FLAG0_FORCEACK (1<<1) /* Give ACK to AUTH_REQUEST even if
                                            master is up. */

/* ---------------------- API exported outside cluster.c -------------------- */
clusterNode *getNodeByQuery(client *c, struct redisCommand *cmd, robj **argv, int argc, int *hashslot, int *ask);
int clusterRedirectBlockedClientIfNeeded(client *c);
void clusterRedirectClient(client *c, clusterNode *n, int hashslot, int error_code);
unsigned long getClusterConnectionsCount(void);

#endif /* __CLUSTER_H */
