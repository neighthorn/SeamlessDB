#include "op_state.h"

#include "execution/executor_index_scan.h"
#include "execution/executor_block_join.h"
#include "execution/executor_hash_join.h"


/*
    remote checkpoint 结构
    +--- checkpoint meta ---+--- checkpoints ---+
    +---       4096      ---+---   reserve   ---+ 
*/



size_t CheckPointMeta::serialize(char *dest) {
    size_t offset = 0;
    memcpy(dest + offset, (char *)&thread_id, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&checkpoint_num, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&total_size, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&total_src_op, sizeof(double));
    offset += sizeof(double);
    return offset;
}

bool CheckPointMeta::deserialize(char *src, size_t size) {
    if(size < getSize()) return false;
    size_t offset = 0;
    memcpy((char *)&thread_id, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&checkpoint_num, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&total_size, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&total_src_op, src + offset, sizeof(double));
    offset += sizeof(double);
    return true;
}


OperatorState::OperatorState() {};

OperatorState::OperatorState(int sql_id, int operator_id, time_t op_state_time, ExecutionType exec_type) : 
        sql_id_(sql_id), operator_id_(operator_id), op_state_time_(op_state_time), exec_type_(exec_type) {}

size_t OperatorState::serialize(char *dest) {
    size_t offset = 0;
    memcpy(dest + offset, (char *)&sql_id_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&operator_id_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&op_state_size_, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy(dest + offset, (char *)&op_state_time_, sizeof(time_t));
    offset += sizeof(time_t);
    memcpy(dest + offset, (char *)&exec_type_, sizeof(exec_type_));
    offset += sizeof(exec_type_);
    return offset;
}

bool OperatorState::deserialize(char *src, size_t size) {
    /*
        check size
    */
    if(size != getSize()) {
        return false;
    }

    /*
        deserialize
    */
    size_t offset = 0;
    memcpy((char *)&sql_id_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&operator_id_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&op_state_size_, src + offset, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy((char *)&op_state_time_, src + offset, sizeof(time_t));
    offset += sizeof(time_t);
    memcpy((char *)&exec_type_, src + offset, sizeof(exec_type_));
    offset += sizeof(exec_type_);

    return true;
}

IndexScanOperatorState::IndexScanOperatorState() : OperatorState(-1, -1, time(nullptr), ExecutionType::NOT_DEFINED){};

IndexScanOperatorState::IndexScanOperatorState(IndexScanExecutor *index_scan_op) :
    OperatorState(index_scan_op->sql_id_, index_scan_op->operator_id_, time(nullptr),index_scan_op->exec_type_), 
    index_scan_op_(index_scan_op)
{
    lower_rid_      = index_scan_op_->lower_rid_;
    upper_rid_      = index_scan_op_->upper_rid_;
    current_rid_    = index_scan_op_->rid_;

    op_state_size_ = getSize();
}


    
size_t  IndexScanOperatorState::serialize(char *dest) {
    size_t offset = OperatorState::serialize(dest);
    memcpy(dest + offset, (char *)&lower_rid_, sizeof(Rid));
    offset += sizeof(Rid);
    memcpy(dest + offset, (char *)&upper_rid_, sizeof(Rid));
    offset += sizeof(Rid);
    memcpy(dest + offset, (char *)&current_rid_, sizeof(Rid));
    offset += sizeof(Rid);
    return offset;
}

bool IndexScanOperatorState::deserialize(char *src, size_t size) {
    if(size != getSize()) return false;

    // RwServerDebug::getInstance()->DEBUG_PRINT("[IndexScanOperatorState::deserialize] size: " + std::to_string(size) + " getSize(): " + std::to_string(getSize()));
    // std::cout << "This line is number: " << __FILE__  << ":" << __LINE__ << std::endl;
    bool status = OperatorState::deserialize(src, OperatorState::getSize());
    if(!status) return false;

    size_t offset = OperatorState::getSize();
    memcpy((char *)&lower_rid_, src + offset, sizeof(Rid));
    offset += sizeof(Rid);
    memcpy((char *)&upper_rid_, src + offset, sizeof(Rid));
    offset += sizeof(Rid);
    memcpy((char *)&current_rid_, src + offset, sizeof(Rid));
    offset += sizeof(Rid);

    return true;
    // std::cout << "This line is number: " << __FILE__  << ":" << __LINE__ << std::endl;
    // RwServerDebug::getInstance()->DEBUG_PRINT("This line is number: " + std::string(__FILE__)  + ":" + std::to_string(__LINE__));
}

// inline bool index_scan_op_load_state(IndexScanExecutor *index_scan_op, IndexScanOperatorState *index_scan_state) {
//     index_scan_op->
// }

BlockJoinOperatorState::BlockJoinOperatorState() : OperatorState(-1, -1, time(nullptr),ExecutionType::NOT_DEFINED) {
    
    
    block_join_op_ = nullptr;
    state_size = sizeof(int) * 4 + sizeof(int) * 2 + sizeof(bool) + sizeof(int) * 3;
    /*
        basic state
    */
    left_block_id_ = -1;
    left_block_cursor_ = -1;
    left_child_call_times_ = -1;
    be_call_times_ = -1;

    left_child_is_join_ = false;
    left_index_scan_state_ = IndexScanOperatorState();

    right_child_is_join_ = false;
    right_index_scan_state_ = IndexScanOperatorState();

    /*
        left block info
    */
    left_block_num_ = -1;
    left_block_record_len = -1;      // 没计算sizeof(recordhdr)，计算left block size的时候要加上
    left_block_size_ = -1; 
    left_block_ = nullptr;

    /*
        use for recovery()
    */
    join_block_ = nullptr;
}

BlockJoinOperatorState::BlockJoinOperatorState(BlockNestedLoopJoinExecutor *block_join_op) : 
        OperatorState(block_join_op->sql_id_, block_join_op->operator_id_, time(nullptr), block_join_op->exec_type_), 
        block_join_op_(block_join_op)
{
    state_size = 0;

    /*
        basic state
    */
    left_block_id_          = block_join_op_->left_blocks_->current_block_id_;
    left_block_max_size_    = block_join_op_->left_block_->MAX_SIZE;
    left_block_cursor_      = block_join_op_->left_block_->cur_pos_;
    left_child_call_times_  = block_join_op_->left_child_call_times_;
    be_call_times_          = block_join_op_->be_call_times_;


    state_size += sizeof(int) * 4 + sizeof(size_t);

    /*
        left exec is join
    */
    if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(block_join_op_->left_.get())) {
        left_child_is_join_ = true;
        left_index_scan_state_ = IndexScanOperatorState();
    } else if(auto x = dynamic_cast<IndexScanExecutor *>(block_join_op_->left_.get())){
        left_child_is_join_ = false;
        left_index_scan_state_ = IndexScanOperatorState(x);
    } else {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    state_size += (sizeof(bool) + left_index_scan_state_.getSize());

    /*
        right exec is join
    */
    right_index_scan_state_ = IndexScanOperatorState();
    if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(block_join_op_->right_.get())) {
        right_child_is_join_ = true;
    } else if(auto x = dynamic_cast<IndexScanExecutor *>(block_join_op_->right_.get())) {
        right_child_is_join_ = false;
        right_index_scan_state_ = IndexScanOperatorState(x);
    } else {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    state_size += (sizeof(bool) + right_index_scan_state_.getSize());

    /*
        left block info
    */
    left_block_num_ = block_join_op_->left_block_->size_;
    left_block_record_len = block_join_op_->left_->tupleLen();  // without record len

    /*
        一个优化，如果left block cursor = left block size，那么说明这个block已经扫描完了，不需要序列化
    */
    if(left_block_num_ == left_block_cursor_) {
        left_block_size_ = 0;
        left_block_ = nullptr;
    }else {
        left_block_size_ = left_block_num_ * (left_block_record_len + sizeof(RecordHdr));
        left_block_ = block_join_op->left_block_;
    }



    state_size += sizeof(int) * 3 + left_block_size_;

    /*
        设置op_state_size_
    */
    op_state_size_ = getSize();
}

size_t BlockJoinOperatorState::serialize(char *dest) {
    size_t offset = OperatorState::serialize(dest);
    /*
        basic state
    */
    memcpy(dest + offset, (char *)&left_block_id_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&left_block_max_size_, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy(dest + offset, (char *)&left_block_cursor_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&left_child_call_times_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&be_call_times_, sizeof(int));
    offset += sizeof(int);

    /*
        left child is join
    */
    memcpy(dest + offset, (char *)&left_child_is_join_, sizeof(bool));
    offset += sizeof(bool);
    size_t left_index_scan_size = left_index_scan_state_.serialize(dest + offset);
    offset += left_index_scan_size;

    /*
        right child is join
    */
    memcpy(dest + offset, (char *)&right_child_is_join_, sizeof(bool));
    offset += sizeof(bool);
    size_t right_index_scan_size = right_index_scan_state_.serialize(dest + offset);
    offset += right_index_scan_size;


    /*
        left block info
    */
    memcpy(dest + offset, (char *)&left_block_num_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&left_block_record_len, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&left_block_size_, sizeof(int));
    offset += sizeof(int);
    // Serialize left_block

    /*
        if left block == nullptr, then left block is empty
    */
    if(left_block_ == nullptr) {
        assert(left_block_size_ == 0);
    } else {
        size_t block_size = left_block_->serialize_data(dest + offset);
        assert(block_size == left_block_size_);
        offset += block_size;
    }

    /*
        检查是否序列化完全
    */
    assert(offset == op_state_size_);
    return offset;
}

/*
    join block是new出来的内存，需要外部释放
*/
bool BlockJoinOperatorState::deserialize(char *src, size_t size) {
    RwServerDebug::getInstance()->DEBUG_PRINT("[BlockJoinOperatorState::deserialize] size: " +  std::to_string(size));
    if (size < OperatorState::getSize()) return false;

    bool status = OperatorState::deserialize(src, OperatorState::getSize());
    if (!status) return false;

    /*
        basic info
    */
    size_t offset = OperatorState::getSize();
    memcpy((char *)&left_block_id_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&left_block_max_size_, src + offset, sizeof(size_t));
    offset += sizeof(size_t);
    memcpy((char *)&left_block_cursor_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&left_child_call_times_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&be_call_times_, src + offset, sizeof(int));
    offset += sizeof(int);

    RwServerDebug::getInstance()->DEBUG_PRINT("This line is number: " + std::string(__FILE__)  + ":" + std::to_string(__LINE__));

    std::cout << "left_block_id_: " << left_block_id_ << std::endl;
    std::cout << "left block max size: " << left_block_max_size_ << std::endl; 
    std::cout << "left_block_cursor_: " << left_block_cursor_ << std::endl;
    std::cout << "left_child_call_times_: " << left_child_call_times_ << std::endl;
    std::cout << "be_call_times_: " << be_call_times_ << std::endl;

    
    /*
        left child is join
    */
    std::cout << "This line is number: " << __FILE__  << ":" << __LINE__ << std::endl;
    RwServerDebug::getInstance()->DEBUG_PRINT("offset= " + std::to_string(offset));
    memcpy((char *)&left_child_is_join_, src + offset, sizeof(bool));
    offset += sizeof(bool);
    if(!left_child_is_join_) {
        left_index_scan_state_.deserialize(src + offset, left_index_scan_state_.getSize());
    }
    offset += left_index_scan_state_.getSize();

    std::cout << "This line is number: " << __FILE__  << ":" << __LINE__ << std::endl;
    RwServerDebug::getInstance()->DEBUG_PRINT("This line is number: " + std::string(__FILE__)  + ":" + std::to_string(__LINE__));
    /*
        right child is join
    */
    std::cout << "This line is number: " << __FILE__  << ":" << __LINE__ << std::endl;
    memcpy((char *)&right_child_is_join_, src + offset, sizeof(bool));
    offset += sizeof(bool);
    std::cout << "This line is number: " << __FILE__  << ":" << __LINE__ << std::endl;
    right_index_scan_state_.deserialize(src + offset, right_index_scan_state_.getSize());
    offset += right_index_scan_state_.getSize();
    std::cout << "This line is number: " << __FILE__  << ":" << __LINE__ << std::endl;
    RwServerDebug::getInstance()->DEBUG_PRINT("This line is number: " + std::string(__FILE__)  + ":" + std::to_string(__LINE__));
    /*
        left block info
    */
    memcpy((char *)&left_block_num_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&left_block_record_len, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&left_block_size_, src + offset, sizeof(int));
    offset += sizeof(int);
    // TODO Deserialize left_block
    RwServerDebug::getInstance()->DEBUG_PRINT("[BlockJoinOperatorState::deserialize] left_block_size_: " + std::to_string(left_block_size_));

    /*
        if left block is empty
    */
    if(left_block_size_ == 0) {
        assert(left_block_cursor_ == left_block_num_);
        join_block_ = nullptr;
    } else {
        join_block_ = std::make_unique<JoinBlock>(left_block_max_size_);
        size_t acutal_size = join_block_->deserialize(src + offset, left_block_num_, left_block_cursor_, left_block_record_len);
        assert(acutal_size == left_block_size_);
        offset += left_block_size_;
    }
    
    RwServerDebug::getInstance()->DEBUG_PRINT("[BlockJoinOperatorState::deserialize] offset: " + std::to_string(offset));
    /*
        检查是否反序列化
    */
    assert(offset == op_state_size_);
    return true;
}

HashJoinOperatorState::HashJoinOperatorState() {

}

HashJoinOperatorState::HashJoinOperatorState(HashJoinExecutor* hash_join_op) {
    hash_join_op_ = hash_join_op;
    state_size = 0;
    be_call_times_ = hash_join_op->be_call_times_;
    left_child_call_times_ = hash_join_op->left_child_call_times_;
    left_child_is_join_ = false;
    left_index_scan_state_ = IndexScanOperatorState();
    right_child_is_join_ = false;
    right_index_scan_state_ = IndexScanOperatorState();
    state_size += sizeof(int) * 2 + sizeof(bool) * 2 + left_index_scan_state_.getSize() + right_index_scan_state_.getSize();
    op_state_size_ = getSize();
}

size_t HashJoinOperatorState::serialize(char *dest) {
    size_t offset = OperatorState::serialize(dest);

    // basic state
    memcpy(dest + offset, (char *)&be_call_times_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&left_child_call_times_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&left_child_is_join_, sizeof(bool));
    offset += sizeof(bool);
    memcpy(dest + offset, (char *)&right_child_is_join_, sizeof(bool));
    offset += sizeof(bool);

    // if left hash table has not been checkpointed completely, then serialize incremental hash table and left operator
    if(!hash_join_op_->is_hash_table_built()) {
        for(const auto& iter: *left_hash_table_) {
            const std::string& key = iter.first;
            const std::vector<std::unique_ptr<Record>>& record_vector = iter.second;
            size_t* last_checkpoint_index = &checkpointed_indexes_->find(key)->second;
            for(size_t i = *last_checkpoint_index; i < record_vector.size(); i++) {
                memcpy(dest + offset, record_vector[i]->raw_data_, left_record_len_);
                offset += left_record_len_;
            }
            *last_checkpoint_index = record_vector.size();
        }
        size_t left_index_scan_size = left_index_scan_state_.serialize(dest + offset);
        offset += left_index_scan_size;
    }
    // if left hash table has been checkpointed completely, just serialize the right operator
    else {
        // serialize right operator
        size_t right_index_scan_size = right_index_scan_state_.serialize(dest + offset);
        offset += right_index_scan_size;
    }

    assert(offset == op_state_size_);
    return offset;
}

bool HashJoinOperatorState::deserialize(char* src, size_t size) {
    if(size < OperatorState::getSize()) return false;

    bool status = OperatorState::deserialize(src, OperatorState::getSize());
    if(!status) return false;

    size_t offset = OperatorState::getSize();
    memcpy((char *)&be_call_times_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&left_child_call_times_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&left_child_is_join_, src + offset, sizeof(bool));
    offset += sizeof(bool);
    memcpy((char *)&right_child_is_join_, src + offset, sizeof(bool));
    offset += sizeof(bool);

    if(!hash_join_op_->is_hash_table_built()) {
        for(auto& iter: *left_hash_table_) {
            std::string key = iter.first;
            std::vector<std::unique_ptr<Record>>& record_vector = iter.second;
            size_t* last_checkpoint_index = &checkpointed_indexes_->find(key)->second;
            for(size_t i = *last_checkpoint_index; i < record_vector.size(); i++) {
                memcpy(record_vector[i]->raw_data_, src + offset, left_record_len_);
                offset += left_record_len_;
            }
            *last_checkpoint_index = record_vector.size();
        }
        left_index_scan_state_.deserialize(src + offset, left_index_scan_state_.getSize());
        offset += left_index_scan_state_.getSize();
    } else {
        right_index_scan_state_.deserialize(src + offset, right_index_scan_state_.getSize());
        offset += right_index_scan_state_.getSize();
    }

    assert(offset == op_state_size_);
    return offset;
}