#pragma once

#include <memory>

#include "record/record.h"
#include "execution/execution_defs.h"

/*
    remote checkpoint 结构
    +--- checkpoint meta ---+--- checkpoints ---+
    +---       4096      ---+---   reserve   ---+ 
*/
constexpr int CheckPointMetaSize = 4096;

/*
    预估计的state size最小值：非准确
*/
constexpr int operator_size_min = sizeof(int) * 2 + sizeof(size_t) + sizeof(time_t) + sizeof(ExecutionType);
constexpr int index_scan_state_size_min = operator_size_min + sizeof(Rid) * 3;
constexpr int block_join_state_size_min = operator_size_min + sizeof(int) * 7 + sizeof(bool) * 2 + index_scan_state_size_min * 2;

struct CheckPointMeta {
    int thread_id = -1;
    int checkpoint_num = -1;
    int total_size = -1;
    double total_src_op = 0;

    size_t serialize(char *dest);
    bool deserialize(char *src, size_t size);

    static inline size_t getSize() {
        return sizeof(int) * 3 + sizeof(double);
    }
};



/*
    OperatorState
    至少包含以下变量
    int sql_id_;
    int operator_id_;
    size_t  op_state_size_;
    time_t  op_state_time_;
    ExecutionType exec_type_;

    其中op_state_size需要根据具体的operator进行计算
*/
class OperatorState {
public:
    OperatorState();
    OperatorState(int sql_id, int operator_id, time_t op_state_time, ExecutionType exec_type);

    size_t  serialize(char *dest) ;

    bool    deserialize(char *src, size_t size) ;

    /*
        get size of Op
    */
    inline size_t getSize() {
        return sizeof(int) * 2 + sizeof(size_t) + sizeof(time_t) + sizeof(exec_type_);
    }

public:
    int sql_id_;
    int operator_id_;
    size_t  op_state_size_;
    time_t  op_state_time_;
    ExecutionType exec_type_;

public:
    /*
        恢复用
    */
    char *op_state_addr_ = nullptr;
};

class IndexScanExecutor;
class IndexScanOperatorState : public OperatorState {
public: 

    IndexScanOperatorState();

    IndexScanOperatorState(IndexScanExecutor *index_scan_op);


    
    size_t  serialize(char *dest) ;

    bool    deserialize(char *src, size_t size) ;

    inline size_t getSize() {
        return OperatorState::getSize() + sizeof(Rid) * 3;
    }

public:
    /*
        state info
    */
    Rid lower_rid_;
    Rid upper_rid_;
    Rid current_rid_;
private:
    IndexScanExecutor *index_scan_op_;
};

// inline bool index_scan_op_load_state(IndexScanExecutor *index_scan_op, IndexScanOperatorState *index_scan_state) {
//     index_scan_op->
// }

class BlockNestedLoopJoinExecutor;
class JoinBlock;
class BlockJoinOperatorState : OperatorState {
public:
    BlockJoinOperatorState();

    BlockJoinOperatorState(BlockNestedLoopJoinExecutor *block_join_op) ;

    size_t  serialize(char *dest) ;

    bool deserialize(char *src, size_t size) ;

    /*
        TODO
    */
    inline size_t getSize() {
        return OperatorState::getSize() + state_size;
    }

public:
    BlockNestedLoopJoinExecutor *block_join_op_;
    size_t state_size = 0;


    /*
        basic state
    */
    int     left_block_id_;
    size_t  left_block_max_size_;
    int     left_block_cursor_;
    int     left_child_call_times_;
    int     be_call_times_;

    bool    left_child_is_join_;
    IndexScanOperatorState  left_index_scan_state_;

    bool    right_child_is_join_;
    IndexScanOperatorState  right_index_scan_state_;

    /*
        left block info
    */
    int     left_block_num_;
    int     left_block_record_len;      // 没计算sizeof(recordhdr)，计算left block size的时候要加上
    int     left_block_size_;
    JoinBlock *left_block_;

    /*
        use for recovery()
    */
    std::unique_ptr<JoinBlock> join_block_;
};