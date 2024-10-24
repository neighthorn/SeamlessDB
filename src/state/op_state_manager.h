#pragma once

#include <queue>
#include "allocator/buffer_allocator.h"
#include "allocator/rdma_region_allocator.h"
#include "optimizer/plan.h"

class ContextManager;
class MetaManager;
class AbstractExecutor;
class QPManager;
class CheckPointMeta;
class OperatorState;

struct SQLState{
static const int SQL_STATE_MAX_SIZE = 1024;
    int sql_id;
    size_t sql_size;
    std::string sql;

    inline static  SQLState construct_sql_state(int sql_id, char *sql_str, size_t len) {
        if(len == 0) {
            return SQLState{.sql_id = -1, .sql_size = 0, .sql = ""};
        }
        
        return SQLState{.sql_id = sql_id, .sql_size = len, .sql = std::string(sql_str, len)};
    }

    inline size_t cal_size() { return sizeof(sql_id) + sizeof(sql_size) + sql_size; }

    /*
        序列化
    */
    inline size_t serialize(char *dest) {
        size_t offset = 0;
        memcpy(dest + offset, (char *)&sql_id, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, (char *)&sql_size, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, sql.c_str(), sql.size());
        offset += sql.size();
        return offset;
    }

    /*
        反序列化
    */
    inline bool deserialize(char *src) {

        /*
            反序列化过程
        */
        size_t offset = 0;

        /*
            sql id
        */
        memcpy((char *)&sql_id, src + offset, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)&sql_size, src + offset, sizeof(size_t));
        offset += sizeof(size_t);

        /*
            check sql_id and sql_size
        */
        if(sql_id < 0 || sql_size == 0) {
            sql = "";
            return false;
        }

        // memcpy(sql.data(), src + offset, sql_size);
        sql = std::string(src + offset, sql_size);
        
        return true;
    }
};

enum class CkptType: int16_t {
    BlockJoinCkpt = 0,      // incremental ckpt
    IndexScanCkpt,          // overlay ckpt
    HashJoinHashTableCkpt,  // hash join hash table, incremental ckpt, the first "int" in buffer represents the offset
    HashJoinOpCkpt,         // hash join left/right child state, fixed location, overlay ckpt
};

struct OpCheckpointBlock {
    char *buffer;
    size_t size;
    CkptType ckpt_type_;
};

/*
    operator state manager(thread local)
    管理每个rw/ro线程的算子内部checkpoint状态
*/
class OperatorStateManager {

public:
    static  int     write_cnts;
    static  int64_t write_tot_size;
    static  int     add_cktp_cnts;

public:

    int         connection_id_;         /*  connection id   */
    node_id_t   primary_node_id_;       /*  state node id   */

    CoroutineScheduler *coro_sched_;    /*  coro_sched      */

    MetaManager     *meta_manager_;
    QPManager       *qp_manager_;

    /*
        local memory
    */
    std::pair<char*, char*>     local_sql_region_;
    std::pair<char*, char*>     local_plan_region_;
    std::pair<char*, char*>     local_op_checkpoint_region_;
    std::pair<char*, char*>     local_op_checkpoint_read_cache_region_;

    /*
        local memory allocator
    */
    RDMABufferAllocator *sql_buffer_allocator_;
    RDMABufferAllocator *plan_buffer_allocator_;
    char                *op_checkpoint_meta_buffer_;
    std::mutex          op_meta_latch_;     // 保护op checkpoint meta
    RDMABufferAlloc     *op_checkpoint_buffer_allocator_;
    RDMABufferAllocator *op_checkpoint_read_cache_allocator_;

    /*
        queue pair
    */
    RCQP    *sql_qp_;
    RCQP    *plan_qp_;
    RCQP    *op_checkpoint_qp_;  

    /*
        op checkpoint write thread
        now we use one thread to write all operator checkpoint
    */
    std::thread     *op_checkpoint_write_thread_;

    std::queue<OpCheckpointBlock>  op_checkpoint_queue_;
    std::mutex op_latch_;   // 保护op_checkpoints_

    /*
        cv
    */
    std::condition_variable     op_checkpoint_not_full_;    
    std::condition_variable     op_checkpoint_not_empty_;

    /*
        op checkpoint    元数据
    */
    std::unique_ptr<CheckPointMeta>     ck_meta_;
    double                              total_src_op = 0;
    size_t                              op_next_write_offset_;

public:
    OperatorStateManager(int connection_id, CoroutineScheduler *coro_sched, MetaManager *meta_manager, QPManager *qp_manager);

    void reset_remote_sql_and_op_region();
public:

    /*
        write sql to state 同步写
    */
    void write_sql_to_state(int sql_id, char* sql, int len);

    /*
        read sql from state 同步读
    */
    std::unique_ptr<SQLState> read_sql_from_state();

    /*
        write plan to state 同步写
    */
    // TODO
    void write_plan_to_state(int sql_id, SmManager* sm_mgr, std::shared_ptr<Plan> plan);

    /*
        add operator checkpoint to buffer 向Buffer中增加operator checkpoint
        异步线程写
    */
    std::pair<bool, size_t> add_operator_state_to_buffer(AbstractExecutor *op, double src_op);

    /*
        read op checkpoint meta
        对应 write op checkpoint meta
    */
    std::unique_ptr<CheckPointMeta> read_op_checkpoint_meta();

    /*
        read op checkpoints from state node
    */
    std::vector<std::unique_ptr<OperatorState>> read_op_checkpoints(CheckPointMeta *ck_meta);

    /*
        clear op checkpoint meta
        事务已提交，不需要op checkpoint了
    */
    void clear_op_checkpoint();
    void clear_op_meta();

    bool finish_write();

private:
    /*
        异步写
    */  
    void write_operator_state_to_state_node();


    /*
        write thread
    */
    void write_op_state_thread();

    /*
        write op checkpoint meta
    */
    void write_op_checkpoint_meta();

    void generate_suspend_plan_for_query_tree(int sql_id);

};