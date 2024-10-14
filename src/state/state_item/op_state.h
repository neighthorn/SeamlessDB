#pragma once

#include <memory>
#include <iostream>

#include "record/record.h"
#include "execution/execution_defs.h"
#include <unordered_map>

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
constexpr int index_scan_state_size_min = operator_size_min + sizeof(Rid) * 3 + sizeof(bool);
constexpr int projection_state_size_min = operator_size_min + sizeof(bool) + sizeof(int);
constexpr int block_join_state_size_min = operator_size_min + projection_state_size_min + sizeof(int) * 7 + sizeof(bool) * 2 + sizeof(size_t) + index_scan_state_size_min;
constexpr int hash_join_state_size_min = operator_size_min + projection_state_size_min + sizeof(int) * 5 + sizeof(bool) * 4 + index_scan_state_size_min;
constexpr int sort_state_size_min = operator_size_min + sizeof(int) * 4 + sizeof(bool) * 2;
// constexpr int hash_join_state_size_min = operator_size_min;

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

#define EXECTYPE_OFFSET sizeof(int) * 2 + sizeof(size_t) + sizeof(time_t)
#define OPERATOR_STATE_HEADER_SIZE sizeof(int) * 2 + sizeof(size_t) + sizeof(time_t) + sizeof(ExecutionType) + sizeof(bool)

class OperatorState {
public:
    OperatorState();
    OperatorState(int sql_id, int operator_id, time_t op_state_time, ExecutionType exec_type, bool finish_begin_tuple) ;

    virtual size_t  serialize(char *dest) ;

    virtual bool    deserialize(char *src, size_t size) ;

    /*
        get size of Op
    */
    virtual size_t getSize() {
        // std::cout << "Base OperatorState size: " << OPERATOR_STATE_HEADER_SIZE << std::endl;
        return OPERATOR_STATE_HEADER_SIZE;
    }

public:
    int sql_id_;
    int operator_id_;
    size_t  op_state_size_;
    time_t  op_state_time_;
    ExecutionType exec_type_;
    bool finish_begin_tuple_;

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
    
    size_t  serialize(char *dest) override;

    bool    deserialize(char *src, size_t size) override;

    size_t getSize() override {
        // std::cout << "IndexScanOperatorState getSize(): " << op_state_size_ << std::endl;
        return OperatorState::getSize() + sizeof(Rid) * 3 + sizeof(bool);
    }

public:
    /*
        state info
    */
    Rid lower_rid_;
    Rid upper_rid_;
    Rid current_rid_;
    bool is_seq_scan_;
private:
    IndexScanExecutor *index_scan_op_;
};

class ProjectionExecutor;
class ProjectionOperatorState : public OperatorState {
public:
    ProjectionOperatorState();

    ProjectionOperatorState(ProjectionExecutor* projection_op);

    ~ProjectionOperatorState() {
        if(left_index_scan_state_ != nullptr) {
            delete left_index_scan_state_;
        }
    }

    size_t  serialize(char *dest) override;

    bool    deserialize(char *src, size_t size) override;

    size_t getSize() override {
        // std::cout << "ProjectionOperatorState size: " << op_state_size_ << std::endl;
        return op_state_size_;
    }

    /*
    如果儿子节点为join算子，只需要记录当前算子的be_call_time，儿子节点的be_call_time应该和当前节点的be_call_time一致
    如果儿子节点为scan算子，需要一起记录scan算子的状态
    */
    bool is_left_child_join_;       
    int left_child_call_times_;    // left child的be_call_times, 和当前算子的be_call_times一致

    ProjectionExecutor *projection_op_;

    IndexScanOperatorState* left_index_scan_state_;
};

// inline bool index_scan_op_load_state(IndexScanExecutor *index_scan_op, IndexScanOperatorState *index_scan_state) {
//     index_scan_op->
// }

class BlockNestedLoopJoinExecutor;
class JoinBlock;
class BlockJoinOperatorState : public OperatorState {
public:
    BlockJoinOperatorState();

    BlockJoinOperatorState(BlockNestedLoopJoinExecutor *block_join_op) ;

    size_t  serialize(char *dest) override;

    bool deserialize(char *src, size_t size) override;

    /*
        TODO
    */
    size_t getSize() override {
        // std::cout << "BlockJoinOperatorState getSize(): " << op_state_size_ << std::endl;
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
    // IndexScanOperatorState  left_index_scan_state_;
    OperatorState* left_child_state_;

    bool    right_child_is_join_;
    // IndexScanOperatorState  right_index_scan_state_;
    OperatorState* right_child_state_;

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

class HashJoinExecutor; 
// HashJoin的状态需要分为两部分，一部分是哈希表的数据，也就是哈希表中每个record，这部分是增量存储的，单独开辟一块区域，
// 一部分是left child和right child的状态，这部分可以覆盖写，不需要增量存储，因此需要分别维护着两块内存区域的元数据（也就是start和offset）
class HashJoinOperatorState : public OperatorState {
public:
    HashJoinOperatorState();

    HashJoinOperatorState(HashJoinExecutor *hash_join_op);

    size_t serialize(char *dest) override;

    bool deserialize(char *src, size_t size) override;

    void rebuild_hash_table(HashJoinExecutor *hash_join_op, char* src, size_t size);

    size_t getSize() override {
        // std::cout << "HashJoinExecutorState getSize(): " << op_state_size_ << std::endl;
        // return OperatorState::getSize() + state_size;
        return op_state_size_;
    }   

    HashJoinExecutor *hash_join_op_;

    bool hash_table_contained_;     // whether the incremental hash table is contained in the state
    int be_call_times_;             // the number of times the operator has been called
    int left_child_call_times_;
    bool is_hash_table_built_;       // whether the hash table has been built, the same as initialized_ in HashJoinExecutor

    bool left_child_is_join_;
    // IndexScanOperatorState left_index_scan_state_;
    OperatorState* left_child_state_;
    
    bool right_child_is_join_;
    // IndexScanOperatorState right_index_scan_state_;
    OperatorState* right_child_state_;

    int left_hash_table_size_;  // 当前检查点包含的哈希表中的tuple条数，也就是相对于上一个检查点的增量
    int left_record_len_;       // 哈希表中每个tuple的长度
    std::unordered_map<std::string, std::vector<std::unique_ptr<Record>>>* left_hash_table_; 
    std::unordered_map<std::string, size_t>* checkpointed_indexes_;
    
    std::string left_iter_key_;
    // 算子当前的cursor
    int left_tuples_index_;            // 哈希表中的vector的index, 同HashJoinExecutor中的left_tuples_index_
};


#define OFF_SORT_UNSORTED_TUPLE_CNT sizeof(int) * 3 + sizeof(bool)
#define OFF_SORT_IS_SORTED_INDEX_CKPT sizeof(int) * 3
class SortExecutor;
class SortOperatorState: public OperatorState {
public:
    SortOperatorState();
    SortOperatorState(SortExecutor *sort_op);
    size_t serialize(char *dest) override;
    bool deserialize(char *src, size_t size) override;
    void rebuild_sort_records(SortExecutor *sort_op, char* src, size_t size);
    void rebuild_sort_index(SortExecutor *sort_op, char* src, size_t size);
    size_t getSize() override {
        // std::cout << "SortOperatorState getSize(): " << op_state_size_ << std::endl;
        return op_state_size_;
    }

    SortExecutor *sort_op_;

    bool is_sort_index_checkpointed_;
    int unsorted_records_count_;    // 当前检查点中包含的unsorted_records的数量，也就是相对于上一个检查点的增量
    int tuple_len_;
    
    int be_call_times_;
    int left_child_call_times_;

    bool left_child_is_join_;
    OperatorState* left_child_state_;

    std::vector<std::unique_ptr<Record>>* unsorted_records_;
    int* sorted_index_;
};