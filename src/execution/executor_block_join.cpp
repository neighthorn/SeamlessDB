#include "executor_block_join.h"
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include "state/state_manager.h"
#include "state/op_state_manager.h"
#include "executor_index_scan.h"
#include "debug_log.h"
#include "errors.h"
#include "common/config.h"
#include "executor_hash_join.h"
#include "executor_projection.h"
#include "comp_ckpt_mgr.h"

/* 
    Join Block
*/

JoinBlock::JoinBlock(size_t max_size) : MAX_SIZE(max_size), size_(0), cur_pos_(0) {}

void JoinBlock::push_back(std::unique_ptr<Record> record) {
    assert(!is_full()); 
    buffer_.emplace_back(std::move(record));   
    // buffer_[size_] = std::move(record);
    size_++;
}

void JoinBlock::beginTuple() {
    cur_pos_ = 0;
}

void JoinBlock::nextTuple() {
    cur_pos_++;
}

Record* JoinBlock::get_record() {
    assert(!is_end());
    return buffer_[cur_pos_].get();
}

void JoinBlock::reset() {
    buffer_.clear();
    // memset(buffer_, 0, sizeof(buffer_));
    cur_pos_ = 0;
    size_ = 0;
}

bool JoinBlock::is_full() {
    return size_ >= MAX_SIZE;
}

bool JoinBlock::is_end() {
    return cur_pos_ >= size_;
}

// 序列化JoinBlock到dest指针指向的内存区域
int JoinBlock::serialize_data(char* dest) {
    if (dest == nullptr) {
        throw std::invalid_argument("Destination pointer cannot be null.");
    }

    // 计算需要序列化的数据总长度
    if(size_ == 0) {
        return 0;
    }

    size_t record_length    = buffer_[0]->data_length_ + sizeof(RecordHdr);
    size_t total_length     = size_ * record_length;

    // 序列化每个Record到dest
    char* current_dest = dest;
    for (size_t i = 0; i < size_; ++i) {
        // 复制RecordHdr和数据到dest
        memcpy(current_dest, buffer_[i]->record_, record_length);
        current_dest += record_length;
    }

    return total_length;
}

/*
    record_length 不包括sizeof(hdr)
*/
size_t JoinBlock::deserialize(char *src, int record_num, int current_pos, int record_length) {
    // RwServerDebug::getInstance()->DEBUG_PRINT("[JoinBlock]: [Deserialize]: [RecordNum]: " + std::to_string(record_num) + " [CurrentPos]: " + std::to_string(current_pos) + " [RecordLength]: " + std::to_string(record_length));
    /*
        reset重置block
    */
    reset();

    /*
        load from data
    */
    size_ = record_num;

    size_t offset = 0;
    for(int i = 0; i < record_num; i++) {
        /*
            copy to buffer_
        */
        auto rec = std::make_unique<Record>(record_length);
        memcpy(rec->record_, src + offset, record_length + sizeof(RecordHdr));
        buffer_.emplace_back(std::move(rec));

        /*
            add offset
        */
        offset += (record_length + sizeof(RecordHdr));
    }
    cur_pos_ = current_pos;

    return offset;
}

size_t JoinBlock::getEstimateSize() {
    if(size_ == 0) {
        return 0;
    }
    size_t record_length    = buffer_[0]->data_length_ + sizeof(RecordHdr);
    size_t total_length     = size_ * record_length;

    return total_length;
}


JoinBlockExecutor::JoinBlockExecutor(BlockNestedLoopJoinExecutor *father_exec, AbstractExecutor* exe) : father_exec_(father_exec), executor_(exe) {
        /*
            设置join block max size
            如果节点是join节点，则max size = 20, 如果是scan算子，则max size = 1000
        */
        size_t join_block_max_size = 500;
        if(auto x = dynamic_cast<IndexScanExecutor *>(exe)) {
            join_block_max_size = block_size_;
        } else {
            join_block_max_size = block_size_;
        }
        join_block_ = std::make_unique<JoinBlock>(join_block_max_size);
        father_exec->left_block_ = join_block_.get();
        /*
            初始化为-1
        */
        current_block_id_ = -1;
}

void JoinBlockExecutor::beginBlock() {
    executor_->beginTuple(); 
    join_block_->reset();
    current_block_id_ = 0;

    std::cout << "left_block_id: " << current_block_id_ << std::endl;
    while(!executor_->is_end() && !join_block_->is_full()) {
        join_block_->push_back(std::move(executor_->Next()));
        // father_exec_->write_state_if_allow();
        executor_->nextTuple();
    }
    // std::cout << "finish fill block: " << current_block_id_ << std::endl;
    
    // TODO: 在这里调用father_exec_->write_state_if_allow()

    // RwServerDebug::getInstance()->DEBUG_PRINT("[JoinBlockExecutor: " + std::to_string(father_exec_->operator_id_) +  "]: [BeginBlock]: [BlockId]: " + std::to_string(current_block_id_));
}

/*
    恢复用
*/
void JoinBlockExecutor::load_block_info(BlockJoinOperatorState *block_op_state) {
    /*
        load block info, 类似begin block的功能
    */
    if(block_op_state == nullptr) {
        return ;
    }
    if(block_op_state->join_block_ != nullptr) {
        join_block_ = std::move(block_op_state->join_block_);
    }
    else {
        // 代表join block已经消耗完了
        join_block_->size_ = -1;
        // nextBlock();
    }
    current_block_id_ = block_op_state->left_block_id_;
}

void JoinBlockExecutor::nextBlock() {
    assert(!is_end());
    join_block_->reset();
    current_block_id_++;
    std::cout << "left_block_id: " << current_block_id_ << std::endl;
    while(!executor_->is_end() && !join_block_->is_full()) {
        join_block_->push_back(std::move(executor_->Next()));
        // father_exec_->write_state_if_allow();
        executor_->nextTuple();
        // std::cout << "BNLJ leftchild rid: " << executor_->rid().page_no << ", " << executor_->rid().slot_no << std::endl;
    }
    // std::cout << "finish fill block: " << current_block_id_ << std::endl;
    
    // RwServerDebug::getInstance()->DEBUG_PRINT("[JoinBlockExecutor: " + std::to_string(father_exec_->operator_id_) + "]: [NextBlock]: [BlockId]: " + std::to_string(current_block_id_));
}

JoinBlock* JoinBlockExecutor::Next() {
    return join_block_.get();
}

bool JoinBlockExecutor::is_end() {
    return join_block_->size_ == 0;
    // return executor_->is_end();
}

/*
    BlockNestedLoopJoinExecutor implementation
*/

BlockNestedLoopJoinExecutor::BlockNestedLoopJoinExecutor(std::shared_ptr<AbstractExecutor> left, std::shared_ptr<AbstractExecutor> right, 
                        std::vector<Condition> conds, Context *context, int sql_id, int operator_id) :
                        AbstractExecutor(sql_id, operator_id) {
    left_ = left;
    right_ = right;
    len_ = left_->tupleLen() + right_->tupleLen();
    cols_ = left_->cols();
    auto right_cols = right_->cols();
    for (auto &col : right_cols) {
        col.offset += left_->tupleLen();
    }
    cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
    isend = false;
    fed_conds_ = std::move(conds);

    // 初始化join buffer
    left_blocks_ = std::make_unique<JoinBlockExecutor>(this, left_.get());

    context_ = context;
    //
    // context_->coro_sched_->RDMAWriteSync();

    exec_type_  = ExecutionType::BLOCK_JOIN;

    /*
        初始化push ck points
    */
    ck_infos_.push_back(BlockCheckpointInfo{.ck_timestamp_ = std::chrono::high_resolution_clock::now(), .left_block_id_ = -1, .left_block_size_ = 0, .left_rc_op_ = 0});

    left_child_call_times_  = 0;
    be_call_times_          = 0;

    finished_begin_tuple_ = false;
    is_in_recovery_ = false;
    /*
        debug test
    */
    {
        // RwServerDebug::getInstance()->DEBUG_PRINT("[BLOCK JOIN][LEFT: " + left_->getType() + "], [RIGHT: " + right_->getType());
    }

    state_change_time_ = 0;

}


// 
void BlockNestedLoopJoinExecutor::beginTuple() {
    if(is_in_recovery_ && finished_begin_tuple_)  {
        return;
    }
    std::cout << "BlockNestedLoopJoinExecutor::beginTuple()" << std::endl;
    left_child_call_times_ = 0;
    /*
        初始化 left blocks, left block, right abstractor
    */
    right_->beginTuple();
    
    left_blocks_->beginBlock();
    left_block_ = left_blocks_->Next();
    left_block_->beginTuple();

    
    
    /*
        写状态如果可以的话
    */
    left_child_call_times_ += left_block_->size_;
    state_change_time_ += left_block_->size_;
    write_state_if_allow();
        
    // 2. 初始化 isend并开启循环寻找第一个符合要求的节点
    isend = false;
    find_next_valid_tuple();
    finished_begin_tuple_ = true;
    write_state_if_allow();
    // be_call_times_++;
}

void BlockNestedLoopJoinExecutor::nextTuple() {
    assert(!is_end());

    if(left_block_->size_ == -1) {
        left_blocks_->nextBlock();
        left_block_ = left_blocks_->Next();
    }
    left_block_->nextTuple();
    
    find_next_valid_tuple();

    // be_call_times_++;
}

std::unique_ptr<Record> BlockNestedLoopJoinExecutor::Next() {
    assert(!is_end());

    // 1. 取left_record和right_record
    auto left_record = left_block_->get_record();
    auto right_record = right_->Next();
    // 2. 合并到一起
    auto ret = std::make_unique<Record>(len_);
    memcpy(ret->raw_data_, left_record->raw_data_, left_record->data_length_);
    memcpy(ret->raw_data_ + left_record->data_length_, right_record->raw_data_, right_record->data_length_);
    // std::cout << "Op_id: " << operator_id_ << ", BNLJ Next record: " << std::string(ret->raw_data_, len_) << std::endl;
    
    be_call_times_ ++;
    // write_state_if_allow();
    return ret;
}

// 找到下一个符合fed_cond的tuple
void BlockNestedLoopJoinExecutor::find_next_valid_tuple() {
    /*
        三重循环
        for every block in left:
            for every tuple in right:
                for every tuple in left block:
                    if(left_tuple match right tuple):
                        return 
                    else:
                        left_tuple++
                left_block.beginTuple();
                right_tuple++
            left_block++
        done
    */
//    auto find_start = std::chrono::high_resolution_clock::now();
    while(!left_blocks_->is_end()) {
        while(!right_->is_end()) {
            auto right_record = right_->Next();
            // auto right_check_start = std::chrono::high_resolution_clock::now();
            // std::cout << "Begin check cond for the right tuple: right rid: " << right_->rid().page_no << ", " << right_->rid().slot_no << std::endl;
            while(!left_block_->is_end()) {
                auto left_record = left_block_->get_record();
                /*
                    判断是否符合cond
                */
                bool is_fit = true;
                for(auto cond : fed_conds_) {
                    // 取left value
                    
                    auto left_cols = left_->cols();
                    auto left_col = *(left_->get_col(left_cols, cond.lhs_col));
                    auto left_value = fetch_value(left_record, left_col);

                    // 取right value
                    Value right_value;
                    if(cond.is_rhs_val) {
                        right_value = cond.rhs_val;
                    } else {
                        auto right_cols = right_->cols();
                        auto right_col = *(right_->get_col(right_cols, cond.rhs_col));
                        right_value = fetch_value(right_record, right_col);
                    }

                        // 比较是否符合条件
                    if(!compare_value(left_value, right_value, cond.op)) {
                        is_fit = false;
                        break;
                    }
                }
                // 如果符合要求，则返回
                if(is_fit) {
                    // auto find_end = std::chrono::high_resolution_clock::now();
                    // auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(find_end - find_start);
                    // std::cout << "find_next_valid_tuple time: " << duration.count() << "ms" << std::endl;
                    return ;
                }
                left_block_->nextTuple();
            }
            // auto right_check_end = std::chrono::high_resolution_clock::now();
            // std::cout << "End check cond for the right tuple: right rid: " << right_->rid().page_no << ", " << right_->rid().slot_no << std::endl;
            // std::cout << "Check time period is: " << std::chrono::duration_cast<std::chrono::microseconds>(right_check_end - right_check_start).count() << "us" << std::endl;

            right_->nextTuple();

            if(right_->is_end()) {
                // std::cout << "left_block->cur_pos_ = " << left_block_->cur_pos_  << ", left_block->size_ = " << left_block_->size_ << std::endl;
                if(node_type_ == 1)
                    write_state_if_allow(1);
                break;
            }
            left_block_->beginTuple();
        } 

        left_blocks_->nextBlock();
        left_block_ = left_blocks_->Next();
        left_block_->beginTuple();
        right_->beginTuple();

        left_child_call_times_ += left_block_->size_;
        state_change_time_ += left_block_->size_;
        write_state_if_allow();
    }
    isend = true;
    return ;
}

std::chrono::time_point<std::chrono::system_clock> BlockNestedLoopJoinExecutor::get_latest_ckpt_time() {
    return ck_infos_[ck_infos_.size() - 1].ck_timestamp_;
}

double BlockNestedLoopJoinExecutor::get_curr_suspend_cost() {
    BlockCheckpointInfo current_ck_info         = {.ck_timestamp_ = std::chrono::high_resolution_clock::now(), .left_block_id_ = left_blocks_->current_block_id_, .left_block_size_ = left_block_->getEstimateSize(), .left_rc_op_ = 0};
    BlockCheckpointInfo *latest_ck_info         = nullptr;
    // 取上一次的ck_info
    if(ck_infos_.empty()) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
    latest_ck_info = &ck_infos_[ck_infos_.size() - 1];
    double src_op = block_join_state_size_min;
    if(current_ck_info.left_block_id_ == latest_ck_info->left_block_id_) {
        if(current_ck_info.left_block_size_ < latest_ck_info->left_block_size_) {
            std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
            // std::cout << "current ck info size = " << current_ck_info->left_block_size_ << ", latest ck info size = " << latest_ck_info->left_block_size_ << std::endl;
            // std::cout << "latest ck info block id = " << latest_ck_info->left_block_id_ << ", current ck info block id = " << current_ck_info->left_block_id_ << std::endl;
        }
        src_op += double(current_ck_info.left_block_size_ - latest_ck_info->left_block_size_);
    } else if(current_ck_info.left_block_id_ > latest_ck_info->left_block_id_){
        src_op += double(current_ck_info.left_block_size_);
    } else {
        /*
            不可能出现的情况
        */
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
        std::cerr << "current_ck_info.left_block_id_ = " << current_ck_info.left_block_id_ << ", latest_ck_info->left_block_id_ = " << latest_ck_info->left_block_id_ << std::endl;
    }
    return src_op;
}

/*
    根据代价估计函数，判断是否需要写状态
*/
std::pair<bool, double> BlockNestedLoopJoinExecutor::judge_state_reward(BlockCheckpointInfo *current_ck_info) {
    /*
        current ck info: 当前状态 info
        latest  ck info: 上一次状态的 info
        left child latest info: 如果左儿子是
    */
    BlockCheckpointInfo *latest_ck_info         = nullptr;

    // 取上一次的ck_info
    if(ck_infos_.empty()) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
    latest_ck_info = &ck_infos_[ck_infos_.size() - 1];

    /*
        计算SRC op and RC op
    */
    double  src_op  = block_join_state_size_min;
    double  rc_op   = -1;
    /*
        SRC OP
    */
    if(current_ck_info->left_block_id_ == latest_ck_info->left_block_id_) {
        if(current_ck_info->left_block_size_ < latest_ck_info->left_block_size_) {
            std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
            // std::cout << "current ck info size = " << current_ck_info->left_block_size_ << ", latest ck info size = " << latest_ck_info->left_block_size_ << std::endl;
            // std::cout << "latest ck info block id = " << latest_ck_info->left_block_id_ << ", current ck info block id = " << current_ck_info->left_block_id_ << std::endl;
        }
        src_op += double(current_ck_info->left_block_size_ - latest_ck_info->left_block_size_);

        if(current_ck_info->left_block_size_ == latest_ck_info->left_block_size_) {
            src_op += (double)len_ *(state_change_time_ - latest_ck_info->state_change_time_);
        }
    } else if(current_ck_info->left_block_id_ > latest_ck_info->left_block_id_){
        src_op += double(current_ck_info->left_block_size_);
    } else {
        /*
            不可能出现的情况
        */
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
        std::cerr << "current_ck_info.left_block_id_ = " << current_ck_info->left_block_id_ << ", latest_ck_info->left_block_id_ = " << latest_ck_info->left_block_id_ << std::endl;
    }


    /*
        rc op
    */
    rc_op = getRCop(current_ck_info->ck_timestamp_);
    // current_ck_info->left_rc_op_ = rc_op;

    if(rc_op == 0) {
        return {false, -1};
    }

    /*
        对src_op 做放缩
    */
    // double new_src_op = src_op / src_scale_factor_;
    double new_src_op = src_op / MB_ + src_op / RB_ + C_;
    /*
        计算REW op
    */
    double rew_op = rc_op / new_src_op - state_theta_;

    RwServerDebug::getInstance()->DEBUG_PRINT("[BlockNestedLoopJoinExecutor][op_id: " + std::to_string(operator_id_) + "]: [Rew_op]: " + std::to_string(rew_op) \
     + "state_size: " + std::to_string(src_op) + " [Src_op]: " + std::to_string(new_src_op) + " [Rc_op]: " + std::to_string(rc_op) + " [State Theta]: " + std::to_string(state_theta_));
    /*
        判断是否需要写状态
    */
    if(rew_op > 0) {
        if(current_ck_info->left_block_id_ == latest_ck_info->left_block_id_ && current_ck_info->left_block_size_ == latest_ck_info->left_block_size_) {
            current_ck_info->left_rc_op_ = 0;
        }
        else {
            if(auto x = dynamic_cast<HashJoinExecutor*>(left_.get())) {
                current_ck_info->left_rc_op_ = x->getRCop(current_ck_info->ck_timestamp_);
            }
            else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor*>(left_.get())) {
                current_ck_info->left_rc_op_ = x->getRCop(current_ck_info->ck_timestamp_);
            }
            else if(auto x = dynamic_cast<ProjectionExecutor*>(left_.get())) {
                current_ck_info->left_rc_op_ = x->getRCop(current_ck_info->ck_timestamp_);
            }    
        }
        
        current_ck_info->state_change_time_ = state_change_time_;
        // RwServerDebug::getInstance()->DEBUG_PRINT("[BlockNestedLoopJoinExecutor][op_id: " + std::to_string(operator_id_) + "]: [Rew_op]: " + std::to_string(rew_op) \
        // + "state_size: " + std::to_string(src_op) + " [Src_op]: " + std::to_string(new_src_op) + " [Rc_op]: " + std::to_string(rc_op) + " [State Theta]: " + std::to_string(state_theta_));
        return {true, src_op};
    }
    
    return {false, -1};
}

/*
    递归计算RC op
*/
int64_t BlockNestedLoopJoinExecutor::getRCop(std::chrono::time_point<std::chrono::system_clock>  current_time) {
    /*
        找到小于current time的最新ck info
    */
    BlockCheckpointInfo *latest_ck_info         = nullptr;

    for(int i = ck_infos_.size() - 1; i >= 0; i--) {
        if(ck_infos_[i].ck_timestamp_ <= current_time) {
            latest_ck_info = &ck_infos_[i];
            break;
        }
    }
    if(latest_ck_info == nullptr) {
        std::cerr << "[Error]: No ck points found! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
        // std::cerr << "current_time =  " << current_time << ", ck_info[0].time = " << ck_infos_[0].ck_timestamp_ << std::endl;
        // throw RMDBError("Not Implemented!");
    }

    if(left_blocks_->current_block_id_== latest_ck_info->left_block_id_ && left_block_->getEstimateSize() == latest_ck_info->left_block_size_) {
        return std::chrono::duration_cast<std::chrono::microseconds>(current_time - latest_ck_info->ck_timestamp_).count();
    }

    // RwServerDebug::getInstance()->DEBUG_PRINT("[BlockNestedLoopJoinExecutor][op_id: " + std::to_string(operator_id_) + "]: [Current Time]: " + std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(current_time.time_since_epoch()).count()) \
    // + " [Latest Time]: " + std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(latest_ck_info->ck_timestamp_.time_since_epoch()).count()) \
    // + " [Left RC op]: " + std::to_string(latest_ck_info->left_rc_op_));

    /*
        计算ck info
    */
    if(auto x =  dynamic_cast<BlockNestedLoopJoinExecutor *>(left_.get())) {
        // return std::chrono::duration_cast<std::chrono::milliseconds>(current_time - latest_ck_info->ck_timestamp_).count() + x->getRCop(latest_ck_info->ck_timestamp_);
        return std::chrono::duration_cast<std::chrono::microseconds>(current_time - latest_ck_info->ck_timestamp_).count() + latest_ck_info->left_rc_op_;
    } 
    else if(auto x = dynamic_cast<HashJoinExecutor*>(left_.get())) {
        // return std::chrono::duration_cast<std::chrono::milliseconds>(current_time - latest_ck_info->ck_timestamp_).count() + x->getRCop(latest_ck_info->ck_timestamp_);
        return std::chrono::duration_cast<std::chrono::microseconds>(current_time - latest_ck_info->ck_timestamp_).count() + latest_ck_info->left_rc_op_;
    }
    else if(auto x = dynamic_cast<ProjectionExecutor*>(left_.get())) {
        return std::chrono::duration_cast<std::chrono::microseconds>(current_time - latest_ck_info->ck_timestamp_).count() + latest_ck_info->left_rc_op_;
    }
    else {
        return std::chrono::duration_cast<std::chrono::microseconds>(current_time - latest_ck_info->ck_timestamp_).count();
    }
} 

void BlockNestedLoopJoinExecutor::write_state() {
    BlockCheckpointInfo current_ck_info         = {.ck_timestamp_ = std::chrono::high_resolution_clock::now(), .left_block_id_ = left_blocks_->current_block_id_, .left_block_size_ = left_block_->getEstimateSize(), .left_rc_op_ = 0};
    BlockCheckpointInfo *latest_ck_info         = nullptr;
    // 取上一次的ck_info
    if(ck_infos_.empty()) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
    latest_ck_info = &ck_infos_[ck_infos_.size() - 1];
    double  src_op  = block_join_state_size_min;
    if(current_ck_info.left_block_id_ == latest_ck_info->left_block_id_) {
        if(current_ck_info.left_block_size_ < latest_ck_info->left_block_size_) {
            std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
            // std::cout << "current ck info size = " << current_ck_info->left_block_size_ << ", latest ck info size = " << latest_ck_info->left_block_size_ << std::endl;
            // std::cout << "latest ck info block id = " << latest_ck_info->left_block_id_ << ", current ck info block id = " << current_ck_info->left_block_id_ << std::endl;
        }
        src_op += double(current_ck_info.left_block_size_ - latest_ck_info->left_block_size_);
    } else if(current_ck_info.left_block_id_ > latest_ck_info->left_block_id_){
        src_op += double(current_ck_info.left_block_size_);
    } else {
        /*
            不可能出现的情况
        */
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
        std::cerr << "current_ck_info.left_block_id_ = " << current_ck_info.left_block_id_ << ", latest_ck_info->left_block_id_ = " << latest_ck_info->left_block_id_ << std::endl;
    }
    context_->op_state_mgr_->add_operator_state_to_buffer(this, src_op);
    if(cost_model_ != 2)
        ck_infos_.push_back(current_ck_info);
}

/*

*/
void BlockNestedLoopJoinExecutor::write_state_if_allow(int type) {
    if(cost_model_ >= 1) {
        CompCkptManager::get_instance()->solve_mip(context_->op_state_mgr_);
        return;
    }

    // RwServerDebug::getInstance()->DEBUG_PRINT("This line is number: " + std::string(__FILE__)  + ":" + std::to_string(__LINE__));
    BlockCheckpointInfo current_ck_info         = {.ck_timestamp_ = std::chrono::high_resolution_clock::now(), .left_block_id_ = left_blocks_->current_block_id_, .left_block_size_ = left_block_->getEstimateSize(), .left_rc_op_ = 0};

    switch (type)
    {
    case 1: {
        /*
            consume完一个block就写状态，绕过代价估计
            src op记录为block operator的最小size
        */
        RwServerDebug::getInstance()->DEBUG_PRINT("[JoinBlockExecutor: " + std::to_string(operator_id_) +  "][Consume block id: " + std::to_string(left_blocks_->current_block_id_) + "]");
        if(state_open_) {
            auto [status, actual_size] = context_->op_state_mgr_->add_operator_state_to_buffer(this, block_join_state_size_min);
            if(status) {
                ck_infos_.push_back(current_ck_info);
            }
        }
        // std::cout << "JoinBlock Consumed" << std::endl;
    } break;
    
    /*
        其余情况，默认走代价估计
    */
    default: {
        if(state_open_) {
            auto [able_to_write, src_op] = judge_state_reward(&current_ck_info);
            if(able_to_write) {
                auto [status, actual_size] = context_->op_state_mgr_->add_operator_state_to_buffer(this, src_op);
                // RwServerDebug::getInstance()->DEBUG_PRINT("[BlockNestedLoopJoinExecutor][op_id: " + std::to_string(operator_id_) + "]: [Write State]: [state size]: " + std::to_string(actual_size));
                if(status) {
                    ck_infos_.push_back(current_ck_info);
                }
            }
        }
    } break;
    }
}

/*
    load block join state
*/
void BlockNestedLoopJoinExecutor::load_state_info(BlockJoinOperatorState *block_join_op) {

    /*
        加载block join state
    */
    if(block_join_op != nullptr) {
        is_in_recovery_ = true;
        /*
            类似beginTuple的逻辑
        */
        /*
            先恢复儿子节点
        */
        if(block_join_op->left_child_is_join_ == false) {
            if(auto x = dynamic_cast<IndexScanExecutor *>(left_.get())) {
                if(block_join_op->left_child_state_->finish_begin_tuple_ == false) {
                    std::cout << "BlockNestedLoopJoinExecutor::load_state_info: IndexScanExecutor beginTuple\n";
                    x->beginTuple();
                }
                else {
                    x->load_state_info(dynamic_cast<IndexScanOperatorState *>(block_join_op->left_child_state_));
                    std::cout << "BlockNestedLoopJoinExecutor::load_state_info: IndexScanExecutor NextTuple\n";
                    x->nextTuple();
                }
            }
            else if(auto x = dynamic_cast<ProjectionExecutor *>(left_.get())) {
                if(block_join_op->left_child_state_->finish_begin_tuple_ == false) {
                    x->beginTuple();
                }
                else {
                    x->load_state_info(dynamic_cast<ProjectionOperatorState *>(block_join_op->left_child_state_));
                    x->nextTuple();
                }
            }
            else {
                std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
            }
        }
        if(block_join_op->right_child_is_join_ == false) {
            if(auto x = dynamic_cast<IndexScanExecutor *>(right_.get())) {
                if(block_join_op->right_child_state_->finish_begin_tuple_ == false) {
                    x->beginTuple();
                }
                else {
                    x->load_state_info(dynamic_cast<IndexScanOperatorState *>(block_join_op->right_child_state_));
                    x->nextTuple();
                }
            }
            else if(auto x = dynamic_cast<ProjectionExecutor *>(right_.get())) {
                if(block_join_op->right_child_state_->finish_begin_tuple_ == false) {
                    x->beginTuple();
                }
                else {
                    x->load_state_info(dynamic_cast<ProjectionOperatorState *>(block_join_op->right_child_state_));
                    x->nextTuple();
                }
            }
            else {
                std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
            }
        }

        finished_begin_tuple_ = block_join_op->finish_begin_tuple_;
        
        /*
            再恢复自己
        */
        left_blocks_->load_block_info(block_join_op);
        if(left_block_->size_ == -1)
            left_blocks_->nextBlock();
        left_block_ = left_blocks_->Next();

        left_child_call_times_ = block_join_op->left_child_call_times_;
        be_call_times_ = block_join_op->be_call_times_;
        isend = false;
        // find_next_valid_tuple();
        // be_call_times_++;
    }

    return ;
}