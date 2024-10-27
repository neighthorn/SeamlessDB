#include "op_state.h"

#include "execution/executor_index_scan.h"
#include "execution/executor_block_join.h"
#include "execution/executor_hash_join.h"
#include "execution/executor_projection.h"
#include "execution/execution_sort.h"


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

OperatorState::OperatorState(int sql_id, int operator_id, time_t op_state_time, ExecutionType exec_type, bool finish_begin_tuple) : 
        sql_id_(sql_id), operator_id_(operator_id), op_state_time_(op_state_time), exec_type_(exec_type), finish_begin_tuple_(finish_begin_tuple) {}

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
    memcpy(dest + offset, (char *)&finish_begin_tuple_, sizeof(bool));
    offset += sizeof(bool);
    
    assert(offset == OPERATOR_STATE_HEADER_SIZE);
    return offset;
}

bool OperatorState::deserialize(char *src, size_t size) {
    /*
        check size
    */
    if(size < OperatorState::getSize()) {
        std::cerr << "OperatorState::deserialize failed, size: " << size << ", getSize(): " << OperatorState::getSize() << std::endl;
        return false;
    }

    /*
        deserialize
    */
//    std::cout << "OperatorState deserialize: src: " << src << std::endl;
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
    memcpy((char *)&finish_begin_tuple_, src + offset, sizeof(bool));
    offset += sizeof(bool);

    // std::cout << "OperatorState::deserialize: sql_id_: " << sql_id_ << ", operator_id_: " << operator_id_ << ", op_state_size_: " << op_state_size_ << ", op_state_time_: " << op_state_time_ << ", exec_type_: " << exec_type_ << std::endl;
    
    assert(offset == OPERATOR_STATE_HEADER_SIZE);
    return true;
}

IndexScanOperatorState::IndexScanOperatorState() : OperatorState(-1, -1, time(nullptr), ExecutionType::NOT_DEFINED, false){};

IndexScanOperatorState::IndexScanOperatorState(IndexScanExecutor *index_scan_op) :
    OperatorState(index_scan_op->sql_id_, index_scan_op->operator_id_, time(nullptr),index_scan_op->exec_type_, index_scan_op->finished_begin_tuple_), 
    index_scan_op_(index_scan_op)
{
    lower_rid_      = index_scan_op_->lower_rid_;
    upper_rid_      = index_scan_op_->upper_rid_;
    current_rid_    = index_scan_op_->rid_;
    is_seq_scan_    = index_scan_op_->is_seq_scan_;

    op_state_size_ = getSize();
}
    
size_t  IndexScanOperatorState::serialize(char *dest) {
    // RwServerDebug::getInstance()->DEBUG_PRINT("Serialize IndexScanOperatorState: op_id=" + std::to_string(operator_id_));
    size_t offset = OperatorState::serialize(dest);
    memcpy(dest + offset, (char *)&lower_rid_, sizeof(Rid));
    offset += sizeof(Rid);
    memcpy(dest + offset, (char *)&upper_rid_, sizeof(Rid));
    offset += sizeof(Rid);
    memcpy(dest + offset, (char *)&current_rid_, sizeof(Rid));
    offset += sizeof(Rid);
    memcpy(dest + offset, (char *)&is_seq_scan_, sizeof(bool));
    offset += sizeof(bool);

    // RwServerDebug::getInstance()->DEBUG_PRINT("lower_rid: " + std::to_string(lower_rid_.page_no) + ", " + std::to_string(lower_rid_.slot_no));
    // RwServerDebug::getInstance()->DEBUG_PRINT("upper_rid: " + std::to_string(upper_rid_.page_no) + ", " + std::to_string(upper_rid_.slot_no));
    // RwServerDebug::getInstance()->DEBUG_PRINT("current_rid: " + std::to_string(current_rid_.page_no) + ", " + std::to_string(current_rid_.slot_no));

    assert(offset == getSize());
    return offset;
}

bool IndexScanOperatorState::deserialize(char *src, size_t size) {
    if(size < OperatorState::getSize()) return false;

    // RwServerDebug::getInstance()->DEBUG_PRINT("[IndexScanOperatorState::deserialize] op_id: " + std::to_string(operator_id_) + " getSize(): " + std::to_string(getSize()));
    // std::cout << "This line is number: " << __FILE__  << ":" << __LINE__ << std::endl;
    bool status = OperatorState::deserialize(src, OperatorState::getSize());
    if(!status) return false;

    assert(size >= op_state_size_);

    size_t offset = OperatorState::getSize();
    memcpy((char *)&lower_rid_, src + offset, sizeof(Rid));
    offset += sizeof(Rid);
    memcpy((char *)&upper_rid_, src + offset, sizeof(Rid));
    offset += sizeof(Rid);
    memcpy((char *)&current_rid_, src + offset, sizeof(Rid));
    offset += sizeof(Rid);
    memcpy((char *)&is_seq_scan_, src + offset, sizeof(bool));
    offset += sizeof(bool);
    // RwServerDebug::getInstance()->DEBUG_PRINT("lower_rid: " + std::to_string(lower_rid_.page_no) + ", " + std::to_string(lower_rid_.slot_no));
    // RwServerDebug::getInstance()->DEBUG_PRINT("upper_rid: " + std::to_string(upper_rid_.page_no) + ", " + std::to_string(upper_rid_.slot_no));
    // RwServerDebug::getInstance()->DEBUG_PRINT("current_rid: " + std::to_string(current_rid_.page_no) + ", " + std::to_string(current_rid_.slot_no));

    return true;
    // std::cout << "This line is number: " << __FILE__  << ":" << __LINE__ << std::endl;
    // RwServerDebug::getInstance()->DEBUG_PRINT("This line is number: " + std::string(__FILE__)  + ":" + std::to_string(__LINE__));
}

ProjectionOperatorState::ProjectionOperatorState() : OperatorState(-1, -1, time(nullptr), ExecutionType::PROJECTION, false) {
    projection_op_ = nullptr;
    is_left_child_join_ = false;
    left_index_scan_state_ = nullptr;
    op_state_size_ = OperatorState::getSize() + sizeof(bool);
}

ProjectionOperatorState::ProjectionOperatorState(ProjectionExecutor* projection_op) : 
    OperatorState(projection_op->sql_id_, projection_op->operator_id_, time(nullptr), projection_op->exec_type_, projection_op->finished_begin_tuple_), 
    projection_op_(projection_op) {
    op_state_size_ = OperatorState::getSize();
    left_child_call_times_ = projection_op_->be_call_times_;
    op_state_size_ += sizeof(int);
    left_index_scan_state_ = nullptr;

    if(auto x = dynamic_cast<IndexScanExecutor *>(projection_op_->prev_.get())) {
        is_left_child_join_ = false;
        left_index_scan_state_ = new IndexScanOperatorState(x);
        op_state_size_ += left_index_scan_state_->getSize();
        op_state_size_ += sizeof(bool);
    } else if(dynamic_cast<BlockNestedLoopJoinExecutor *>(projection_op_->prev_.get())) {
        is_left_child_join_ = true;
        op_state_size_ += sizeof(bool);
    } else if(dynamic_cast<HashJoinExecutor *>(projection_op_->prev_.get())) {
        is_left_child_join_ = true;
        op_state_size_ += sizeof(bool);
    } else if(dynamic_cast<SortExecutor *>(projection_op_->prev_.get())) {
        is_left_child_join_ = true;
        op_state_size_ += sizeof(bool);
    }
     else {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
    // std::cout << "final op_state_size: " << "\n";
}

size_t ProjectionOperatorState::serialize(char *dest) {
    // RwServerDebug::getInstance()->DEBUG_PRINT("Serialize ProjectionOperatorState");

    size_t offset = OperatorState::serialize(dest);
    memcpy(dest + offset, (char *)&left_child_call_times_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&is_left_child_join_, sizeof(bool));
    offset += sizeof(bool);
    if(is_left_child_join_) {
        assert(offset == op_state_size_);
        return offset;
    } else {
        size_t left_index_scan_size = left_index_scan_state_->serialize(dest + offset);
        offset += left_index_scan_size;
        assert(offset == op_state_size_);
        return offset;
    }
    
}

bool ProjectionOperatorState::deserialize(char *src, size_t size) {
    if(size < OperatorState::getSize()) return false;

    // RwServerDebug::getInstance()->DEBUG_PRINT("Deserialize ProjectionOperatorState\n");

    bool status = OperatorState::deserialize(src, OperatorState::getSize());
    if(!status) return false;

    assert(size >= op_state_size_);

    size_t offset = OperatorState::getSize();

    // std::cout << "Deserialize ProjectionOperatorState\n";

    memcpy((char *)&left_child_call_times_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&is_left_child_join_, src + offset, sizeof(bool));
    offset += sizeof(bool);
    if(is_left_child_join_) {
        return true;
    } else {
        left_index_scan_state_ = new IndexScanOperatorState();
        left_index_scan_state_->deserialize(src + offset, size - offset);
        return true;
    }
}

// inline bool index_scan_op_load_state(IndexScanExecutor *index_scan_op, IndexScanOperatorState *index_scan_state) {
//     index_scan_op->
// }

BlockJoinOperatorState::BlockJoinOperatorState() : OperatorState(-1, -1, time(nullptr),ExecutionType::NOT_DEFINED, false) {
    
    
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
    // left_index_scan_state_ = IndexScanOperatorState();

    right_child_is_join_ = false;
    // right_index_scan_state_ = IndexScanOperatorState();

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
        OperatorState(block_join_op->sql_id_, block_join_op->operator_id_, time(nullptr), block_join_op->exec_type_, block_join_op->finished_begin_tuple_), 
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

    // RwServerDebug::getInstance()->DEBUG_PRINT("left_block_id_: " + std::to_string(left_block_id_));
    // RwServerDebug::getInstance()->DEBUG_PRINT("left block max size: " + std::to_string(left_block_max_size_));
    // RwServerDebug::getInstance()->DEBUG_PRINT("left_block_cursor_: " + std::to_string(left_block_cursor_));
    // RwServerDebug::getInstance()->DEBUG_PRINT("left_child_call_times_: " + std::to_string(left_child_call_times_));
    // RwServerDebug::getInstance()->DEBUG_PRINT("be_call_times_: " + std::to_string(be_call_times_));


    state_size += sizeof(int) * 4 + sizeof(size_t);

    /*
        left exec is join
    */
    if(dynamic_cast<BlockNestedLoopJoinExecutor *>(block_join_op_->left_.get())) {
        left_child_is_join_ = true;
        // left_index_scan_state_ = IndexScanOperatorState();
        state_size += sizeof(bool);
    } else if(auto x = dynamic_cast<IndexScanExecutor *>(block_join_op_->left_.get())){
        left_child_is_join_ = false;
        // left_index_scan_state_ = IndexScanOperatorState(x);
        left_child_state_ = new IndexScanOperatorState(x);
        state_size += sizeof(bool) + left_child_state_->getSize();
    } else if(dynamic_cast<HashJoinExecutor *>(block_join_op_->left_.get())) {
        left_child_is_join_ = true;
        // left_index_scan_state_ = IndexScanOperatorState();
        state_size += sizeof(bool);
    } else if(auto x = dynamic_cast<ProjectionExecutor *>(block_join_op_->left_.get())) {
        left_child_is_join_ = false;
        // left_index_scan_state_ = IndexScanOperatorState();
        left_child_state_ = new ProjectionOperatorState(x);
        state_size += sizeof(bool) + left_child_state_->getSize();
    }
    else {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    // state_size += (sizeof(bool) + left_index_scan_state_.getSize());
    // state_size += left_child_state_->getSize() + sizeof(bool);


    /*
        right exec is join
    */
    // right_index_scan_state_ = IndexScanOperatorState();
    if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(block_join_op_->right_.get())) {
        right_child_is_join_ = true;
        state_size += sizeof(bool);
    } else if(auto x = dynamic_cast<IndexScanExecutor *>(block_join_op_->right_.get())) {
        right_child_is_join_ = false;
        // right_index_scan_state_ = IndexScanOperatorState(x);
        right_child_state_ = new IndexScanOperatorState(x);
        state_size += sizeof(bool) + right_child_state_->getSize();
    } 
    else if(auto x = dynamic_cast<HashJoinExecutor *>(block_join_op_->right_.get())) {
        right_child_is_join_ = true;
        state_size += sizeof(bool);
    }
    else if(auto x = dynamic_cast<ProjectionExecutor *>(block_join_op_->right_.get())) {
        right_child_is_join_ = false;
        right_child_state_ = new ProjectionOperatorState(x);
        state_size += sizeof(bool) + right_child_state_->getSize();
        // std::cout << "right child projection state size: " << right_child_state_->getSize() << std::endl;
    }
    else {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    // state_size += (sizeof(bool) + right_index_scan_state_.getSize());
    // state_size += right_child_state_->getSize() + sizeof(bool);

    /*
        left block info
    */
    left_block_num_ = block_join_op_->left_block_->size_;
    left_block_record_len = block_join_op_->left_->tupleLen();  // without record len

    /*
        一个优化，如果left block cursor = left block size，那么说明这个block已经扫描完了，不需要序列化
    */
    if(left_block_num_ == left_block_cursor_) {
        // std::cout << "left_block_num_ == left_block_cursor_" << std::endl;
        left_block_size_ = 0;
        left_block_ = nullptr;
    }else {
        // std::cout << "left_block_num_ != left_block_cursor_" << std::endl;
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
    // RwServerDebug::getInstance()->DEBUG_PRINT("[BlockJoinOperatorState::serialize] op_id" + std::to_string(operator_id_) + ", state_size: " + std::to_string(op_state_size_));

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
    // size_t left_index_scan_size = left_index_scan_state_.serialize(dest + offset);
    if(left_child_is_join_ == false) {
        // RwServerDebug::getInstance()->DEBUG_PRINT("left child is not join, expect projection/index scan serialization following");
        size_t left_index_scan_size = left_child_state_->serialize(dest + offset);
        offset += left_index_scan_size;
    }
    else {
        // RwServerDebug::getInstance()->DEBUG_PRINT("left child is join");
    }

    /*
        right child is join
    */
    memcpy(dest + offset, (char *)&right_child_is_join_, sizeof(bool));
    offset += sizeof(bool);
    if(right_child_is_join_ == false) {
        // RwServerDebug::getInstance()->DEBUG_PRINT("right child is not join, expect projection/index scan serialization following");
        size_t right_index_scan_size = right_child_state_->serialize(dest + offset);
        offset += right_index_scan_size;
        // std::cout << "right index scan size: " << right_index_scan_size << std::endl;
    }
    else {
        // RwServerDebug::getInstance()->DEBUG_PRINT("right child is join");
    }


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

//    RwServerDebug::getInstance()->DEBUG_PRINT("[BlockJoinOperatorState::serialize] state_size: " + std::to_string(op_state_size_));
    assert(offset == op_state_size_);
    return offset;
}

/*
    join block是new出来的内存，需要外部释放
*/
bool BlockJoinOperatorState::deserialize(char *src, size_t size) {
    // RwServerDebug::getInstance()->DEBUG_PRINT("[BlockJoinOperatorState::deserialize] op_id: " + std::to_string(operator_id_) +  "size: " +  std::to_string(size));
    if (size < OperatorState::getSize()) return false;

    // std::cout << "BlockJoinOperatorState::deserialize: src: " << src << std::endl;
    bool status = OperatorState::deserialize(src, OperatorState::getSize());
    // assert(exec_type_ == ExecutionType::BLOCK_JOIN);
    // std::cout << "operator_id: " << operator_id_ << ", operator_state_size: " << op_state_size_ << ", exec_type: " << exec_type_ << std::endl;
    if (!status) return false;

    assert(size >= op_state_size_);

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

    // RwServerDebug::getInstance()->DEBUG_PRINT("This line is number: " + std::string(__FILE__)  + ":" + std::to_string(__LINE__));

    // std::cout << "left_block_id_: " << left_block_id_ << std::endl;
    // std::cout << "left block max size: " << left_block_max_size_ << std::endl; 
    // std::cout << "left_block_cursor_: " << left_block_cursor_ << std::endl;
    // std::cout << "left_child_call_times_: " << left_child_call_times_ << std::endl;
    // std::cout << "be_call_times_: " << be_call_times_ << std::endl;

    
    /*
        deserialize left child
    */
    // std::cout << "This line is number: " << __FILE__  << ":" << __LINE__ << std::endl;
    // RwServerDebug::getInstance()->DEBUG_PRINT("offset= " + std::to_string(offset));
    memcpy((char *)&left_child_is_join_, src + offset, sizeof(bool));
    offset += sizeof(bool);
    if(left_child_is_join_ == false) {
        // RwServerDebug::getInstance()->DEBUG_PRINT("left child is not join, expect projection/index scan deserialization following");
        // left_index_scan_state_.deserialize(src + offset, left_index_scan_state_.getSize());
        ExecutionType child_exec_type = *reinterpret_cast<ExecutionType*>(src + offset + EXECTYPE_OFFSET);
        if(child_exec_type == ExecutionType::INDEX_SCAN) {
            left_child_state_ = new IndexScanOperatorState();
            left_child_state_->deserialize(src + offset, size - offset);
            offset += left_child_state_->getSize();
        } else if(child_exec_type == ExecutionType::PROJECTION) {
            left_child_state_ = new ProjectionOperatorState();
            left_child_state_->deserialize(src + offset, size - offset);
            offset += left_child_state_->getSize();
        } else {
            std::cerr << "[Error]: Unexpected left child node type for BlockJoinExecutor! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
        }
    }
    else {
        // RwServerDebug::getInstance()->DEBUG_PRINT("left child is join");
    }
    // offset += left_index_scan_state_.getSize();
    // RwServerDebug::getInstance()->DEBUG_PRINT("BlockJoinState Deserialize: Success to deserialize left child. This line is number: " + std::string(__FILE__)  + ":" + std::to_string(__LINE__));
    
    /*
        deserialize right child
    */
    // std::cout << "This line is number: " << __FILE__  << ":" << __LINE__ << std::endl;
    memcpy((char *)&right_child_is_join_, src + offset, sizeof(bool));
    offset += sizeof(bool);
    // std::cout << "This line is number: " << __FILE__  << ":" << __LINE__ << std::endl;
    if(right_child_is_join_ == false) {
        // RwServerDebug::getInstance()->DEBUG_PRINT("right child is not join, expect projection/index scan deserialization following");
        ExecutionType child_exec_type = *reinterpret_cast<ExecutionType*>(src + offset + EXECTYPE_OFFSET);
        if(child_exec_type == ExecutionType::INDEX_SCAN) {
            right_child_state_ = new IndexScanOperatorState();
            right_child_state_->deserialize(src + offset, size - offset);
            offset += right_child_state_->getSize();
        } else if(child_exec_type == ExecutionType::PROJECTION) {
            right_child_state_ = new ProjectionOperatorState();
            right_child_state_->deserialize(src + offset, size - offset);
            offset += right_child_state_->getSize();
        } else {
            std::cerr << "[Error]: Unexpected right child node type for BlockJoinExecutor! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
        }
    }
    else {
        // RwServerDebug::getInstance()->DEBUG_PRINT("right child is join");
    }
    // RwServerDebug::getInstance()->DEBUG_PRINT("BlockJoinState Deserialize: Success to deserialize left child. This line is number: " + std::string(__FILE__)  + ":" + std::to_string(__LINE__));
    
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
    // RwServerDebug::getInstance()->DEBUG_PRINT("[BlockJoinOperatorState::deserialize] left_block_size_: " + std::to_string(left_block_size_));

    /*
        if left block is empty
    */
    if(left_block_size_ == 0) {
        // RwServerDebug::getInstance()->DEBUG_PRINT("[BlockJoinOperatorState::deserialize] left block is empty");
        assert(left_block_cursor_ == left_block_num_);
        join_block_ = nullptr;
    } else {
        // RwServerDebug::getInstance()->DEBUG_PRINT("[BlockJoinOperatorState::deserialize] left block is not empty");
        join_block_ = std::make_unique<JoinBlock>(left_block_max_size_);
        size_t acutal_size = join_block_->deserialize(src + offset, left_block_num_, left_block_cursor_, left_block_record_len);
        assert(acutal_size == left_block_size_);
        offset += left_block_size_;
    }
    
    // RwServerDebug::getInstance()->DEBUG_PRINT("[BlockJoinOperatorState::deserialize] offset: " + std::to_string(offset));
    /*
        检查是否反序列化
    */
    assert(offset == op_state_size_);
    return true;
}

HashJoinOperatorState::HashJoinOperatorState(): OperatorState(-1, -1, time(nullptr), ExecutionType::HASH_JOIN, false) {
    hash_join_op_ = nullptr;
    op_state_size_ = hash_join_state_size_min;
    
    // basic state
    hash_table_contained_ = false;
    be_call_times_ = -1;
    left_child_call_times_ = -1;

    left_child_is_join_ = false;
    // left_index_scan_state_ = IndexScanOperatorState();
    left_child_state_ = nullptr;

    right_child_is_join_ = false;
    // right_index_scan_state_ = IndexScanOperatorState();
    right_child_state_ = nullptr;

    left_hash_table_size_ = -1;
    left_record_len_ = -1;
    left_hash_table_ = nullptr;
    checkpointed_indexes_ = nullptr;
    left_tuples_index_ = -1;
}

HashJoinOperatorState::HashJoinOperatorState(HashJoinExecutor* hash_join_op):
    OperatorState(hash_join_op->sql_id_, hash_join_op->operator_id_, time(nullptr), hash_join_op->exec_type_, hash_join_op->finished_begin_tuple_), hash_join_op_(hash_join_op) {

    op_state_size_ = OperatorState::getSize();

    be_call_times_ = hash_join_op->be_call_times_;
    left_child_call_times_ = hash_join_op->left_child_call_times_;
    left_record_len_ = hash_join_op_->left_->tupleLen();
    left_hash_table_size_ = hash_join_op_->left_hash_table_curr_tuple_count_ - hash_join_op_->left_hash_table_checkpointed_tuple_count_;
    left_tuples_index_ = hash_join_op_->left_tuples_index_;
    if(left_tuples_index_ != -1) {
        left_iter_key_ = hash_join_op_->left_iter_->first;
        op_state_size_ += sizeof(int);
        op_state_size_ += hash_join_op->join_key_size_;
    }
    is_hash_table_built_ = hash_join_op_->initialized_;
    if(left_hash_table_size_ > 0) hash_table_contained_ = true;
    else hash_table_contained_ = false;

    op_state_size_ += sizeof(bool) * 2 + sizeof(int) * 5;

    // left exec info
    // state_size += sizeof(bool);
    op_state_size_ += sizeof(bool);
    if(auto x = dynamic_cast<IndexScanExecutor *>(hash_join_op_->left_.get())) {
        left_child_is_join_ = false;
        if(left_hash_table_size_ > 0) {
            // left_index_scan_state_ = IndexScanOperatorState(x);
            left_child_state_ = new IndexScanOperatorState(x);
            op_state_size_ += left_child_state_->getSize();
        }
    } else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(hash_join_op_->left_.get())) {
        left_child_is_join_ = true;
        // left_index_scan_state_ = IndexScanOperatorState();
    } else if(auto x = dynamic_cast<HashJoinExecutor *>(hash_join_op_->left_.get())) {
        left_child_is_join_ = true;
        // left_index_scan_state_ = IndexScanOperatorState();
    } else if (auto x = dynamic_cast<ProjectionExecutor *>(hash_join_op_->left_.get())) {
        left_child_is_join_ = false;
        if(left_hash_table_size_ > 0) {
            left_child_state_ = new ProjectionOperatorState(x);
            op_state_size_ += left_child_state_->getSize();
        }
    } else {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    // right exec info
    op_state_size_ += sizeof(bool);
    if(auto x = dynamic_cast<IndexScanExecutor *>(hash_join_op_->right_.get())) {
        // std::cout << "HashJoin: right child is index scan" << std::endl;
        right_child_is_join_ = false;
        // right_index_scan_state_ = IndexScanOperatorState(x);
        right_child_state_ = new IndexScanOperatorState(x);
        op_state_size_ += right_child_state_->getSize();
    } else if(dynamic_cast<BlockNestedLoopJoinExecutor *>(hash_join_op_->right_.get())) {
        right_child_is_join_ = true;
        // right_index_scan_state_ = IndexScanOperatorState();
    } else if(dynamic_cast<HashJoinExecutor *>(hash_join_op_->right_.get())) {
        right_child_is_join_ = true;
        // right_index_scan_state_ = IndexScanOperatorState();
    } else if (auto x = dynamic_cast<ProjectionExecutor *>(hash_join_op_->right_.get())) {
        right_child_is_join_ = false;
        // std::cout << "HashJoin: right child is projection" << std::endl;
        right_child_state_ = new ProjectionOperatorState(x);
        op_state_size_ += right_child_state_->getSize();
    } else {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    left_hash_table_ = &hash_join_op_->hash_table_;
    checkpointed_indexes_ = &hash_join_op_->checkpointed_indexes_;
    op_state_size_ += left_hash_table_size_ * left_record_len_; // 不需要记录recordhdr  

    // std::cout << "HashJoinOperatorState::HashJoinOperatorState: left_hash_table_num_: " << left_hash_table_size_ << ", size: " << left_hash_table_size_ * left_record_len_ << std::endl;

    // op_state_size_ = getSize();
}

size_t HashJoinOperatorState::serialize(char *dest) {
    size_t offset = OperatorState::serialize(dest);

    // basic state
    memcpy(dest + offset, (char*)&hash_table_contained_, sizeof(bool));
    offset += sizeof(bool);
    memcpy(dest + offset, (char *)&be_call_times_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&left_child_call_times_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&left_child_is_join_, sizeof(bool));
    offset += sizeof(bool);
    memcpy(dest + offset, (char *)&right_child_is_join_, sizeof(bool));
    offset += sizeof(bool);
    memcpy(dest + offset, (char *)&left_tuples_index_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char *)&is_hash_table_built_, sizeof(bool));
    offset += sizeof(bool);
    memcpy(dest + offset, (char *)&left_record_len_, sizeof(int));
    offset += sizeof(int);
    int incremental_tuples_count_off = offset;
    // serialize left_hash_table_size_(incremental_tuples_count)
    offset += sizeof(int);

    int incremental_tuples_count = 0;

    // if left hash table has not been checkpointed completely, then serialize incremental hash table and left operator
    if(hash_join_op_->left_hash_table_checkpointed_tuple_count_ < hash_join_op_->left_hash_table_curr_tuple_count_) {
        for(const auto& iter: *left_hash_table_) {
            const std::string& key = iter.first;
            const std::vector<std::unique_ptr<Record>>& record_vector = iter.second;
            size_t* last_checkpoint_index = &checkpointed_indexes_->find(key)->second;
            // std::cout << "checkpointed_index: " << *last_checkpoint_index << ", record_vector size: " << record_vector.size() << std::endl;
            for(size_t i = *last_checkpoint_index; i < record_vector.size(); i++) {
                memcpy(dest + offset, record_vector[i]->raw_data_, left_record_len_);
                offset += left_record_len_;
                incremental_tuples_count ++;
            }
            if(cost_model_ != 2)
                *last_checkpoint_index = record_vector.size();
        }
        assert(incremental_tuples_count == left_hash_table_size_);

        if(left_child_is_join_ == false) {
            // serialize left operator
            size_t left_index_scan_size = left_child_state_->serialize(dest + offset);
            offset += left_index_scan_size;
        }
        // size_t left_index_scan_size = left_index_scan_state_.serialize(dest + offset);
        // offset += left_index_scan_size;
    }

    memcpy(dest + incremental_tuples_count_off, (char*)&left_hash_table_size_, sizeof(int));

    // serialize right operator
    if(right_child_is_join_ == false) {
        size_t right_index_scan_size = right_child_state_->serialize(dest + offset);
        offset += right_index_scan_size;
    }

    if(left_tuples_index_ != -1) {
        memcpy(dest + offset, (char *)&hash_join_op_->join_key_size_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, left_iter_key_.c_str(), hash_join_op_->join_key_size_);
        offset += hash_join_op_->join_key_size_;
    }

    assert(offset == op_state_size_);
    return offset;
}

bool HashJoinOperatorState::deserialize(char* src, size_t size) {
    // hash join的deserialize比较特殊，只deserialize左右算子和一些元数据，不deserialize哈希表
    if(size < OperatorState::getSize()) return false;

    bool status = OperatorState::deserialize(src, OperatorState::getSize());
    if(!status) return false;

    size_t offset = OperatorState::getSize();
    memcpy((char *)&hash_table_contained_, src + offset, sizeof(bool));
    offset += sizeof(bool);
    memcpy((char *)&be_call_times_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&left_child_call_times_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&left_child_is_join_, src + offset, sizeof(bool));
    offset += sizeof(bool);
    memcpy((char *)&right_child_is_join_, src + offset, sizeof(bool));
    offset += sizeof(bool);
    memcpy((char *)&left_tuples_index_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&is_hash_table_built_, src + offset, sizeof(bool));
    offset += sizeof(bool);
    memcpy((char *)&left_record_len_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)&left_hash_table_size_, src + offset, sizeof(int));
    offset += sizeof(int);

    if(hash_table_contained_) {
        // 跳过哈希表，哈希表在rebuild_hash_table中重新构建
        offset += left_hash_table_size_ * left_record_len_;

        if(!left_child_is_join_) {
            //  如果左儿子不是join算子，那么需要反序列化左儿子的状态
            ExecutionType child_exec_type = *reinterpret_cast<ExecutionType*>(src + offset + EXECTYPE_OFFSET);
            if(child_exec_type == ExecutionType::INDEX_SCAN) {
                left_child_state_ = new IndexScanOperatorState();
                left_child_state_->deserialize(src + offset, size - offset);
                offset += left_child_state_->getSize();
            } else if(child_exec_type == ExecutionType::PROJECTION) {
                left_child_state_ = new ProjectionOperatorState();
                left_child_state_->deserialize(src + offset, size - offset);
                offset += left_child_state_->getSize();
            } else {
                std::cerr << "[Error]: Unexpected left child node type for HashJoinExecutor! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
            }
        }
    }

    if(right_child_is_join_ == false) {
        ExecutionType child_exec_type = *reinterpret_cast<ExecutionType*>(src + offset + EXECTYPE_OFFSET);
        if(child_exec_type == ExecutionType::INDEX_SCAN) {
            right_child_state_ = new IndexScanOperatorState();
            right_child_state_->deserialize(src + offset, size - offset);
            offset += right_child_state_->getSize();
        } else if(child_exec_type == ExecutionType::PROJECTION) {
            right_child_state_ = new ProjectionOperatorState();
            right_child_state_->deserialize(src + offset, size - offset);
            offset += right_child_state_->getSize();
        } else {
            std::cerr << "right child node type: " << child_exec_type << std::endl;
            std::cerr << "[Error]: Unexpected right child node type for HashJoinExecutor! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
        }
    }

    if(left_tuples_index_ != -1) {
        int join_key_size = *reinterpret_cast<int*>(src + offset);
        offset += sizeof(int);
        left_iter_key_ = std::string(src + offset, join_key_size);
        offset += join_key_size;
    }

    assert(offset == op_state_size_);
    return offset;
}

// 先调用rebuild_hash_table，然后调用load_state_info
void HashJoinOperatorState::rebuild_hash_table(HashJoinExecutor* hash_join_op, char* src, size_t size) {
    if(hash_join_op_ == nullptr) {
        hash_join_op_ = hash_join_op;
    }
    assert(hash_table_contained_ == true);

    left_hash_table_ = &hash_join_op_->hash_table_;
    int offset = OperatorState::getSize() + sizeof(bool) * 4 + sizeof(int) * 5;

    char* join_key = new char[hash_join_op_->join_key_size_];
    for(int i = 0; i < left_hash_table_size_; i++) {
        hash_join_op_->append_tuple_to_hash_table_from_state(src + offset, left_record_len_, join_key);
        offset += left_record_len_;
    }
    // std::cout << "rebuild hash table, count: " << hash_join_op_->left_hash_table_curr_tuple_count_ << "\n";
    delete[] join_key;
}

SortOperatorState::SortOperatorState(): OperatorState(-1, -1, time(nullptr), ExecutionType::SORT, false) {
    sort_op_ = nullptr;
    op_state_size_ = sort_state_size_min;
    be_call_times_ = -1;
    left_child_call_times_ = -1;
    left_child_is_join_ = false;
    left_child_state_ = nullptr;
    tuple_len_ = 0;
    unsorted_records_count_ = 0;
    is_sort_index_checkpointed_ = false;
    unsorted_records_ = nullptr;
    sorted_index_ = nullptr;
}

SortOperatorState::SortOperatorState(SortExecutor* sort_op): 
    OperatorState(sort_op->sql_id_, sort_op->operator_id_, time(nullptr), sort_op->exec_type_, sort_op->finished_begin_tuple_), sort_op_(sort_op) {
    op_state_size_ = OperatorState::getSize();

    be_call_times_ = sort_op->be_call_times_;
    left_child_call_times_ = sort_op->left_child_call_times_;
    tuple_len_ = sort_op->tuple_len_;

    op_state_size_ += sizeof(int) * 3;

    op_state_size_ += sizeof(bool);
    if(is_sort_index_checkpointed_ == false && sort_op->is_sorted_ == true) {
        op_state_size_ += sizeof(int) * sort_op->num_records_;
        is_sort_index_checkpointed_ = true;
    }
    else {
        is_sort_index_checkpointed_ = false;
    }

    unsorted_records_ = &sort_op->unsorted_records_;
    sorted_index_ = sort_op->sorted_index_;
    unsorted_records_count_ = sort_op->num_records_ - sort_op->checkpointed_tuple_num_;
    op_state_size_ += sizeof(int);
    op_state_size_ += unsorted_records_count_ * tuple_len_;

    op_state_size_ += sizeof(bool);
    if(auto x = dynamic_cast<IndexScanExecutor *>(sort_op->prev_.get())) {
        left_child_is_join_ = false;
        left_child_state_ = new IndexScanOperatorState(x);
        op_state_size_ += left_child_state_->getSize();
    } else if(auto x = dynamic_cast<ProjectionExecutor *>(sort_op->prev_.get())) {
        left_child_is_join_ = false;
        left_child_state_ = new ProjectionOperatorState(x);
        op_state_size_ += left_child_state_->getSize();
    } else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(sort_op->prev_.get())) {
        left_child_is_join_ = true;
    } else if(auto x = dynamic_cast<HashJoinExecutor *>(sort_op->prev_.get())) {
        left_child_is_join_ = true;
    } else {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
}

size_t SortOperatorState::serialize(char *dest) {
    size_t offset = OperatorState::serialize(dest);

    memcpy(dest + offset, (char*)&be_call_times_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char*)&left_child_call_times_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char*)&tuple_len_, sizeof(int));
    offset += sizeof(int);
    memcpy(dest + offset, (char*)&is_sort_index_checkpointed_, sizeof(bool));
    offset += sizeof(bool);
    memcpy(dest + offset, (char*)&unsorted_records_count_, sizeof(int));
    offset += sizeof(int);

    // serialize unsorted records
    for(int i = sort_op_->checkpointed_tuple_num_; i < sort_op_->num_records_; i++) {
        memcpy(dest + offset, (*unsorted_records_)[i]->raw_data_, tuple_len_);
        offset += tuple_len_;
    }

    if(is_sort_index_checkpointed_ == true) {
        memcpy(dest + offset, (char*)sorted_index_, sizeof(int) * sort_op_->num_records_);
        offset += sizeof(int) * sort_op_->num_records_;
    }

    memcpy(dest + offset, (char*)&left_child_is_join_, sizeof(bool));
    offset += sizeof(bool);
    // serialize left child
    if(left_child_is_join_ == false) {
        size_t left_child_size = left_child_state_->serialize(dest + offset);
        offset += left_child_size;
    }

    assert(offset == op_state_size_);
    return offset;
}

bool SortOperatorState::deserialize(char* src, size_t size) {
    if(size < OperatorState::getSize()) return false;

    bool status = OperatorState::deserialize(src, OperatorState::getSize());
    if(!status) return false;

    size_t offset = OperatorState::getSize();
    memcpy((char*)&be_call_times_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)&left_child_call_times_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)&tuple_len_, src + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char*)&is_sort_index_checkpointed_, src + offset, sizeof(bool));
    offset += sizeof(bool);
    memcpy((char*)&unsorted_records_count_, src + offset, sizeof(int));
    offset += sizeof(int);

    // 跳过unsorted records，在rebuild_sort_records中构建
    offset += unsorted_records_count_ * tuple_len_;

    if(is_sort_index_checkpointed_ == true) {
        // 跳过排序索引，在rebuild_sort_records中构建
        offset += sizeof(int) * sort_op_->num_records_;
    }

    // deserialize left child
    memcpy((char*)&left_child_is_join_, src + offset, sizeof(bool));
    offset += sizeof(bool);
    if(left_child_is_join_ == false) {
        left_child_state_ = new IndexScanOperatorState();
        left_child_state_->deserialize(src + offset, size - offset);
        offset += left_child_state_->getSize();
    }

    assert(offset == op_state_size_);
    return true;
}

void SortOperatorState::rebuild_sort_records(SortExecutor* sort_op, char* src, size_t size) {
    if(sort_op_ == nullptr) {
        sort_op_ = sort_op;
    }

    int offset = OperatorState::getSize() + sizeof(int) * 3 + sizeof(bool) + sizeof(int);

    for(int i = 0; i < unsorted_records_count_; i++) {
        auto record = std::make_unique<Record>(tuple_len_);
        memcpy(record->raw_data_, src + offset, tuple_len_);
        sort_op_->unsorted_records_.push_back(std::move(record));
        offset += tuple_len_;
        sort_op->num_records_++;
    }

}

void SortOperatorState::rebuild_sort_index(SortExecutor* sort_op, char* src, size_t size) {
    if(sort_op_ == nullptr) {
        sort_op_ = sort_op;
    }

    int offset = OperatorState::getSize() + sizeof(int) * 3 + sizeof(bool) + sizeof(int) + unsorted_records_count_ * tuple_len_;
    sort_op->sorted_index_ = new int[sort_op->num_records_];
    memcpy(sort_op_->sorted_index_, src + offset, sizeof(int) * sort_op_->num_records_);
}