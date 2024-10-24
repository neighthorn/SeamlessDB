#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>

#define BUFFER_LENGTH 8192
#define CORO_NUM 1

// #define MAX_CONN_LIMIT 32

/** Cycle detection is performed every CYCLE_DETECTION_INTERVAL milliseconds. */
extern std::chrono::milliseconds cycle_detection_interval;

/** True if logging should be enabled, false otherwise. */
extern std::atomic<bool> enable_logging;

/** If ENABLE_LOGGING is true, the log should be flushed to disk every LOG_TIMEOUT. */
extern std::chrono::duration<int64_t> log_timeout;

static constexpr int INVALID_FRAME_ID = -1;                                   // invalid frame id
static constexpr int INVALID_PAGE_ID = -1;                                    // invalid page id
static constexpr int INVALID_TXN_ID = -1;                                     // invalid transaction id
static constexpr int INVALID_CONN_ID = -1;                                     // invalid transaction id
static constexpr int INVALID_TIMESTAMP = -1;                                  // invalid transaction timestamp
static constexpr int INVALID_LSN = -1;                                        // invalid log sequence number
static constexpr int HEADER_PAGE_ID = 0;                                      // the header page id
static constexpr int PAGE_SIZE = 4096;                                        // size of a data page in byte  4KB
// static constexpr int BUFFER_POOL_SIZE = 65536;                                // size of buffer pool 256MB
// static constexpr int BUFFER_POOL_SIZE = 262144;                                // size of buffer pool 1GB
static constexpr int LOG_BUFFER_SIZE = (1024 * PAGE_SIZE);                    // size of a log buffer in byte
static constexpr int BUCKET_SIZE = 100;                                        // size of extendible hash bucket
static constexpr int BUFFER_POOL_NUM = 16;
static constexpr int JOIN_BUFFER_SIZE = 256 * 1024;                             // default 256k

using frame_id_t = int32_t;  // frame id type, 帧页ID, 页在BufferPool中的存储单元称为帧,一帧对应一页
using page_id_t = int32_t;   // page id type , 页ID
using bucket_id_t = int32_t;    // bucket id type, used for lock table
using txn_id_t = int32_t;    // transaction id type
using lsn_t = int32_t;       // log sequence number type
using slot_offset_t = size_t;  // slot offset type
using seg_id_t = int32_t;   // segment id type
using t_id_t = uint32_t;    // thread id type

using byte = unsigned char;
using file_id_t = int32_t;   // file id type

using byte = unsigned char;

using oid_t = uint16_t;
using timestamp_t = int32_t;  // timestamp type, used for transaction concurrency

// used for PageStore&LogStore
using log_offset_t = int64_t;
using node_id_t = int32_t;
using offset_t = int64_t;

using coro_id_t = int;
using rwlatch_t = uint64_t;     // latch that has to be modified by RDMA atomic operation, must be 64bit
using mr_id_t = int;        // memory region id type

// memory region ids for various state
const mr_id_t STATE_TXN_LIST_ID = 97;
const mr_id_t STATE_LOG_BUF_ID = 98;
const mr_id_t STATE_LOCK_BUF_ID = 99;
// memory region ids for MasterNode's local_mr
const mr_id_t MASTER_LOCAL_ID = 100;
const mr_id_t STATE_SQL_BUF_ID = 101;
const mr_id_t STATE_PLAN_BUF_ID = 102;

/* 
    join state
*/
const mr_id_t STATE_JOIN_PLAN_BUF_ID    = 103;
const mr_id_t STATE_JOIN_BLOCK_BUF_ID   = 104;

// log file
static const std::string LOG_FILE_NAME = "db.log";

// replacer
static const std::string REPLACER_TYPE = "LRU";

static const std::string DB_META_NAME = "db.meta";

// record header format
#define RECHDR_OFF_NEXT_RECORD_OFFSET 0
#define RECHDR_OFF_TXN_ID 4
#define RECHDR_OFF_DELETE_MARK 8
#define RECHDR_OFF_ROLL_SEG_ID 9
#define RECHDR_OFF_PAGE_NO 13
#define RECHDR_OFF_SLOT_NO 17
#define RECHDR_OFF_UNDO_LOG_LOCATION 17 // record undo location 用slot no，不用 undo log
#define RECHDR_RECORD_NO_LOCATION 21

#define INVALID_OFFSET -1
#define INVALID_FILE_ID -1  
#define INVALID_SEG_ID -1   //
#define INVALID_SLOT_NO -1

#define SLICE_NUM 50        // one slice contains 50 pages

#define ALWAYS_INLINE __attribute__((always_inline)) inline

#define MAX_REMOTE_NODE_NUM 10
#define MAX_THREAD_NUM 128
// #define MAX_THREAD_NUM 16

#define BITMAP_LOCKED 0x1
#define BITMAP_UNLOCKED 0x0

#define STATE_TXN_ACTIVE 0x2
#define STATE_TXN_COMMITING 0x3
#define STATE_TXN_COMMITTED 0x4
#define STATE_TXN_ABORTING 0x5
#define STATE_TXN_ABORTED 0x6

// #define LOCK_STATE_SIZE_REMOTE sizeof(txn_id_t) + sizeof(uint32_t) + sizeof(int) + sizeof(int32_t)
#define LOCK_STATE_SIZE_REMOTE 16
// #define LOCK_STATE_SIZE_LOCAL sizeof(txn_id_t) + sizeof(uint32_t) + sizeof(int) * 2 + sizeof(int32_t)
#define LOCK_STATE_SIZE_LOCAL 20

#define LOCK_MAX_COUNT 8192    
#define LOCK_REGION_SIZE 2621440     // RDMA_BATCH_MAX_SIZE * 20B (this is used in compute node)
#define RDMA_BATCH_MAX_SIZE 32768   // doorbell batching max batch size

#define LOG_REGION_SIZE 268435456   // 256MB 1024*1024*256

#define STATE_SIZE_PER_PLAN 1024
#define THREAD_LOCAL_SQLBUF_SIZE 4096
#define THREAD_LOCAL_PLANBUF_SIZE 1048576       // 1024*1024    1MB
#define THREAD_LOCAL_CURSORBUF_SIZE 1048576     // 1024*1024

/*
    thread local state size
*/

#define PER_THREAD_SQL_SIZE 10485760 // 10 MB
#define PER_THREAD_JOIN_PLAN_SIZE  10485760     // 10MB      = 1024 * 1024
// #define PER_THREAD_JOIN_BLOCK_SIZE 268435456   // 256MB    = 1024 * 1024 * 100
#define PER_THREAD_JOIN_BLOCK_SIZE 5368709120   // 5GB    = 1024 * 1024 * 1024
// #define PER_THREAD_OP_CK_READ_CACHE_SIZE 268435456  // 256 MB
#define PER_THREAD_OP_CK_READ_CACHE_SIZE 5368709120 // 5GB

/*
    状态转移参数
    state open     是否状态转移
    state theta    转移阈值
*/
extern int      state_open_;
extern double   state_theta_;
extern double   src_scale_factor_;
extern int      block_size_;
extern int      node_type_;
extern int      MB_;
extern int      RB_;
extern int      C_;
extern int      cost_model_;    // 0 代表seamlessDB, 1 代表对比工作, 2 代表周期性创建检查点
extern int      interval_;      // 创建检查点的间隔
extern bool     write_ckpt_;    // 1 代表正常使用cost model正常创建检查点，0 代表只使用cost model不创建检查点