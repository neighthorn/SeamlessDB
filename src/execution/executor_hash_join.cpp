#include "executor_hash_join.h"
#include "executor_block_join.h"
#include "executor_index_scan.h"
#include "executor_projection.h"
#include "state/op_state_manager.h"
#include "state/state_manager.h"
#include "state/state_item/op_state.h"
#include "debug_log.h"
#include "comp_ckpt_mgr.h"

void HashJoinExecutor::beginTuple() {
    if(is_in_recovery_ && finished_begin_tuple_) return;

    if(!initialized_) {
        std::unique_ptr<Record> left_rec;
        char* left_key = new char[join_key_size_];
        int offset;
        
        if(is_in_recovery_ == false) {
            left_->beginTuple();
            if(left_->is_end()) {
                is_end_ = true;
                finished_begin_tuple_ = true;
                state_change_time_ ++;
                write_state_if_allow();
                return;
            }
        }
        else {
            left_->nextTuple();
            std::cout << "HashJoinOp Recovery BeginTuple: left_child_call_times: " << left_child_call_times_ << ", hash_table_size: " << left_hash_table_curr_tuple_count_ << std::endl;
        }

        for(;!left_->is_end(); left_->nextTuple()) {
            // auto find_start = std::chrono::high_resolution_clock::now();
            left_rec = left_->Next();
            left_child_call_times_ ++;
            memset(left_key, 0, join_key_size_);
            offset = 0;
            // auto find_end = std::chrono::high_resolution_clock::now();
            // std::cout << "HashJoinFindHashOneTupel time: " << std::chrono::duration_cast<std::chrono::milliseconds>(find_end - find_start).count() << "ms" << std::endl;

            // make record key
            for(const auto& cond: fed_conds_) {
                auto left_cols = left_->cols();
                auto left_col = *(left_->get_col(left_cols, cond.lhs_col));
                auto left_value = fetch_value(left_rec, left_col);
                memcpy(left_key + offset, left_value.raw->data, left_col.len);
                offset += left_col.len;
            }
            assert(offset == join_key_size_);

            std::string key = std::string(left_key, join_key_size_);

            // RwServerDebug::getInstance()->DEBUG_PRINT("[HashJoinExecutor::beginTuple, initiate left hash table][operator_id: " + std::to_string(operator_id_) + "][key: " + key + "]");

            if(hash_table_.find(key) == hash_table_.end()) {
                hash_table_[key] = std::vector<std::unique_ptr<Record>>();
                checkpointed_indexes_[key] = 0;
            }
            hash_table_[key].push_back(std::move(left_rec));
            left_hash_table_curr_tuple_count_ ++;
            // find_end = std::chrono::high_resolution_clock::now();
            // std::cout << "HashJoinPushOneTupleIntoHashTable time: " << std::chrono::duration_cast<std::chrono::milliseconds>(find_end - find_start).count() << "ms" << std::endl;

            state_change_time_ ++;
            if(node_type_ == 1) write_state_if_allow(1);
            else write_state_if_allow();
            // find_end = std::chrono::high_resolution_clock::now();
            // std::cout << "HashJoinWriteCkpt time: " << std::chrono::duration_cast<std::chrono::milliseconds>(find_end - find_start).count() << "ms" << std::endl;
        }

        std::cout << "HashJoinOp, op_id: " << operator_id_ <<  ", HashTableCount: " << left_hash_table_curr_tuple_count_ << ", size: " << (int64_t)left_hash_table_curr_tuple_count_ * len_ << std::endl;
        initialized_ = true;
        delete[] left_key;
    }

    if(is_in_recovery_ == false) right_->beginTuple();

    if(right_->is_end()) {
        is_end_ = true;
        return;
    }

    auto right_rec = right_->Next();
    while(!right_->is_end() && find_match_join_key(right_rec.get()) == hash_table_.end()) {
        right_->nextTuple();
        if(right_->is_end()) {
            is_end_ = true;
            return;
        }
        right_rec = right_->Next();
    }

    finished_begin_tuple_ = true;
    write_state_if_allow();
}

void HashJoinExecutor::append_tuple_to_hash_table_from_state(char* src, int left_rec_len, char* join_key) {
    std::unique_ptr<Record> left_rec = std::make_unique<Record>(left_rec_len);
    memcpy(left_rec->raw_data_, src, left_rec_len);
    memset(join_key, 0, join_key_size_);
    int off = 0;
    for(const auto& cond: fed_conds_) {
        auto left_cols = left_->cols();
        auto left_col = *(left_->get_col(left_cols, cond.lhs_col));
        auto left_value = fetch_value(left_rec.get(), left_col);
        memcpy(join_key + off, left_value.raw->data, left_col.len);
        off += left_col.len;
    }
    assert(off == join_key_size_);
    std::string key = std::string(join_key, join_key_size_);

    // RwServerDebug::getInstance()->DEBUG_PRINT("[HashJoinExecutor::append_tuple_to_hash_table_from_state][operator_id: " + std::to_string(operator_id_) + "][key: " + key + "]");
    
    if(hash_table_.find(key) == hash_table_.end()) {
        hash_table_[key] = std::vector<std::unique_ptr<Record>>();
        checkpointed_indexes_[key] = 0;
    }
    hash_table_[key].push_back(std::move(left_rec));
    checkpointed_indexes_[key] ++;
    left_hash_table_curr_tuple_count_ ++;
    left_hash_table_checkpointed_tuple_count_ ++;
}

void HashJoinExecutor::nextTuple(){
    assert(!is_end());
    std::unique_ptr<Record> right_rec;
    left_tuples_index_ ++;
    if(left_tuples_index_ < left_iter_->second.size()) {
        // std::cout << "HashJoinExecutor::nextTuple(), left_tuple_index < left_iter->second.size()\n";
        return;
    }

    right_->nextTuple();
    if(right_->is_end()) {
        is_end_ = true;
        return;
    }
    right_rec = right_->Next();

    while(!right_->is_end() && find_match_join_key(right_rec.get()) == hash_table_.end()) {
        right_->nextTuple();
        if(right_->is_end()) {
            is_end_ = true;
            return;
        }
        right_rec = right_->Next();
    }

}

std::unique_ptr<Record> HashJoinExecutor::Next() {
    assert(!is_end());

    // auto left_rec = left_->Next();
    const std::vector<std::unique_ptr<Record>>& record_vector = left_iter_->second;
    const std::unique_ptr<Record>& left_rec = left_iter_->second[left_tuples_index_];
    auto right_rec = right_->Next();
    auto res = std::make_unique<Record>(len_);
    memcpy(res->raw_data_, left_rec->raw_data_, left_rec->data_length_);
    memcpy(res->raw_data_ + left_rec->data_length_, right_rec->raw_data_, right_rec->data_length_);

    be_call_times_ ++;
    state_change_time_ ++;

    write_state_if_allow();
    
    return res;
}

std::unordered_map<std::string, std::vector<std::unique_ptr<Record>>>::const_iterator HashJoinExecutor::find_match_join_key(const Record* right_tuple) {
    char* key = new char[join_key_size_];
    int offset = 0;
    memset(key, 0, join_key_size_);

    for(const auto& cond: fed_conds_) {
        auto right_cols = right_->cols();
        assert(cond.is_rhs_val == false);
        auto right_col = *(right_->get_col(right_cols, cond.rhs_col));
        auto right_value = fetch_value(right_tuple, right_col);
        memcpy(key + offset, right_value.raw->data, right_col.len);
        offset += right_col.len;
    }
    assert(offset == join_key_size_);

    left_iter_ = hash_table_.find(std::string(key, join_key_size_));
    left_tuples_index_ = 0;

    delete[] key;
    return left_iter_;
}

std::chrono::time_point<std::chrono::system_clock> HashJoinExecutor::get_latest_ckpt_time() {
    return ck_infos_[ck_infos_.size() - 1].ck_timestamp_;
}

double HashJoinExecutor::get_curr_suspend_cost() {
    HashJoinCheckpointInfo* latest_ck_info = nullptr;
    if(ck_infos_.empty()) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
    latest_ck_info = &ck_infos_[ck_infos_.size() - 1];
    double src_op = hash_join_state_size_min;
    src_op += (double)left_->tupleLen() * (left_hash_table_curr_tuple_count_ - latest_ck_info->left_hash_table_curr_tuple_count_);
    return src_op;
}

std::pair<bool, double> HashJoinExecutor::judge_state_reward(HashJoinCheckpointInfo* curr_ck_info) {
    /*
        要记录的主要是左算子产生的哈希表的大小，哈希表的增量大小计算
    */
    HashJoinCheckpointInfo* latest_ck_info = nullptr;
    if(ck_infos_.empty()) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
    latest_ck_info = &ck_infos_[ck_infos_.size() - 1];
    
    double src_op = hash_join_state_size_min;
    double rc_op = -1;
    
    src_op += (double)left_->tupleLen() * (left_hash_table_curr_tuple_count_ - latest_ck_info->left_hash_table_curr_tuple_count_);

    rc_op = getRCop(curr_ck_info->ck_timestamp_);
    // curr_ck_info->left_rc_op_ = rc_op;

    if(rc_op == 0) {
        return {false, -1};
    }

    // TODO: 如果是在left_hash_table_size=0（也就是只有游标在变化的时候），srcop应该等于提供给父亲节点的中间结果的大小
    if(left_hash_table_curr_tuple_count_ == latest_ck_info->left_hash_table_curr_tuple_count_) {
        src_op += (double)len_ * (state_change_time_ - latest_ck_info->state_change_time_);
    }

    // double new_src_op = src_op / src_scale_factor_;
    double new_src_op = src_op / MB_ + src_op / RB_ + C_;
    double rew_op = rc_op / new_src_op - state_theta_;
    // RwServerDebug::getInstance()->DEBUG_PRINT("[HashJoinExecutor][op_id: " + std::to_string(operator_id_) + "]: [delta_hash_table_tuple_count]: " + std::to_string(left_hash_table_curr_tuple_count_ - latest_ck_info->left_hash_table_curr_tuple_count_) \
    // + " [Rew_op]: " + std::to_string(rew_op) + " [state_size]: " + std::to_string(src_op) + " [Src_op]: " + std::to_string(new_src_op) + " [Rc_op]: " + std::to_string(rc_op) + " [State Theta]: " + std::to_string(state_theta_));
    // if(rew_op > 1) {
    //     state_theta_ = state_theta_ + rew_op;
    // }
    // if(rew_op < -1) {
    //     state_theta_ = state_theta_ - rew_op;
    // }

    if(rew_op > 0) {
        // if create checkpoint in current time, calculate the rc_op(curr_time) for left child and right child
        if(!initialized_) {
            if(auto x = dynamic_cast<HashJoinExecutor* >(left_.get())) {
                curr_ck_info->left_rc_op_ = x->getRCop(curr_ck_info->ck_timestamp_);
            }
            else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor*>(left_.get())) {
                curr_ck_info->left_rc_op_ = x->getRCop(curr_ck_info->ck_timestamp_);
            }
            else if(auto x = dynamic_cast<ProjectionExecutor*>(left_.get())) {
                curr_ck_info->left_rc_op_ = x->getRCop(curr_ck_info->ck_timestamp_);
            }
        }
        curr_ck_info->state_change_time_ = state_change_time_;
        RwServerDebug::getInstance()->DEBUG_PRINT("[HashJoinExecutor][op_id: " + std::to_string(operator_id_) + "]: [delta_hash_table_tuple_count]: " + std::to_string(left_hash_table_curr_tuple_count_ - latest_ck_info->left_hash_table_curr_tuple_count_) \
        + " [Rew_op]: " + std::to_string(rew_op) + " [state_size]: " + std::to_string(src_op) + " [Src_op]: " + std::to_string(new_src_op) + " [Rc_op]: " + std::to_string(rc_op) + " [State Theta]: " + std::to_string(state_theta_));
        if(state_change_time_ - latest_ck_info->state_change_time_ < 10) return {false, -1};
        return {true, src_op};
    }

    return {false, -1};
}

int64_t HashJoinExecutor::getRCop(std::chrono::time_point<std::chrono::system_clock> curr_time) {
    HashJoinCheckpointInfo* latest_ck_info = nullptr;
    for(int i = ck_infos_.size() - 1; i >= 0; --i) {
        if(ck_infos_[i].ck_timestamp_ <= curr_time) {
            latest_ck_info = &ck_infos_[i];
            break;
        }
    }
    if(latest_ck_info == nullptr) {
        std::cerr << "[Error]: HashJoinExecutor: No ck points found! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    // RwServerDebug::getInstance()->DEBUG_PRINT("[HashJoinExecutor][op_id: " + std::to_string(operator_id_) + "]: [Get RCop]: [curr_time] " + std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(curr_time.time_since_epoch()).count()) \
    //  + " [latest_ck_info->ck_timestamp_]: " + std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(latest_ck_info->ck_timestamp_.time_since_epoch()).count()) \
    //  + " [left_rc_op]: " + std::to_string(latest_ck_info->left_rc_op_));
    if(initialized_ == true) {
        return std::chrono::duration_cast<std::chrono::microseconds>(curr_time - latest_ck_info->ck_timestamp_).count();
    }

    if(auto x =  dynamic_cast<HashJoinExecutor *>(left_.get())) {
        return std::chrono::duration_cast<std::chrono::microseconds>(curr_time - latest_ck_info->ck_timestamp_).count() + latest_ck_info->left_rc_op_;
    } 
    else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor*>(left_.get())) {
        return std::chrono::duration_cast<std::chrono::microseconds>(curr_time - latest_ck_info->ck_timestamp_).count() + latest_ck_info->left_rc_op_;
    }
    else if(auto x = dynamic_cast<ProjectionExecutor*>(left_.get())) {
        return std::chrono::duration_cast<std::chrono::microseconds>(curr_time - latest_ck_info->ck_timestamp_).count() + latest_ck_info->left_rc_op_;
    }
    else {
        return std::chrono::duration_cast<std::chrono::microseconds>(curr_time - latest_ck_info->ck_timestamp_).count();
    }
}

void HashJoinExecutor::write_state() {
    HashJoinCheckpointInfo curr_ckpt_info = {.ck_timestamp_ = std::chrono::high_resolution_clock::now(), .left_hash_table_curr_tuple_count_ = left_hash_table_curr_tuple_count_};
    HashJoinCheckpointInfo* latest_ck_info = nullptr;
    if(ck_infos_.empty()) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
    latest_ck_info = &ck_infos_[ck_infos_.size() - 1];
    double src_op = hash_join_state_size_min;
    // std::cout << "current_hash_table_count: " << left_hash_table_curr_tuple_count_ << ", checkpointed count: " << left_hash_table_checkpointed_tuple_count_ << "\n";
    src_op += (double)left_->tupleLen() * (left_hash_table_curr_tuple_count_ - latest_ck_info->left_hash_table_curr_tuple_count_);
    context_->op_state_mgr_->add_operator_state_to_buffer(this, src_op);
    if(cost_model_ != 2) {
        ck_infos_.push_back(curr_ckpt_info);
        left_hash_table_checkpointed_tuple_count_ = left_hash_table_curr_tuple_count_;
    }
        
}

void HashJoinExecutor::write_state_if_allow(int type) {
    if(cost_model_ >= 1) {
        CompCkptManager::get_instance()->solve_mip(context_->op_state_mgr_);
        return;
    }
    // if(type == 1) return;
    HashJoinCheckpointInfo curr_ckpt_info = {.ck_timestamp_ = std::chrono::high_resolution_clock::now(), .left_hash_table_curr_tuple_count_ = left_hash_table_curr_tuple_count_};
    if(state_open_) {
        auto [able_to_write, src_op] = judge_state_reward(&curr_ckpt_info);
        if(able_to_write) {
            // std::cout << "curr_ckpt_info.state_change_time_: " << state_change_time_ << std::endl;
            auto [status, actual_size] = context_->op_state_mgr_->add_operator_state_to_buffer(this, src_op);
            // bool status = true;
            // RwServerDebug::getInstance()->DEBUG_PRINT("[HashJoinExecutor][op_id: " + std::to_string(operator_id_) + "]: [Write State]: " + std::to_string(status) + " [Actual Size]: " + std::to_string(actual_size));
            if(status) {
                curr_ckpt_info.ck_timestamp_ = std::chrono::high_resolution_clock::now();
                ck_infos_.push_back(curr_ckpt_info);
                left_hash_table_checkpointed_tuple_count_ = left_hash_table_curr_tuple_count_;
            }
        }
    }
}

void HashJoinExecutor::load_state_info(HashJoinOperatorState* state) {
    // load state except hash_table
    assert(state != nullptr);
    is_in_recovery_ = true;

    // if(auto x = dynamic_cast<IndexScanExecutor *>(right_.get())) {
    //     x->load_state_info(&(state->right_index_scan_state_));
    // }
    if(state->right_child_is_join_ == false) {
        if(auto x = dynamic_cast<IndexScanExecutor *>(right_.get())) {
            // x->load_state_info(dynamic_cast<IndexScanOperatorState *>(state->right_child_state_));
            if(state->right_child_state_->finish_begin_tuple_ == false) {
                std::cout << "HashJoinExecutor::load_state_info: IndexScanExecutor beginTuple\n";
                x->beginTuple();
                std::cout << "finish begin tuple\n";
            }
            else {
                x->load_state_info(dynamic_cast<IndexScanOperatorState *>(state->right_child_state_));
                x->nextTuple();
            }
        }
        else if (auto x = dynamic_cast<ProjectionExecutor *>(right_.get())) {
            if(state->right_child_state_->finish_begin_tuple_ == false) {
                x->beginTuple();
            }
            else {
                // TODO!!!!!!!!!
                x->load_state_info(dynamic_cast<ProjectionOperatorState *>(state->right_child_state_));
                x->nextTuple();
            }
        }
    }
    

    left_tuples_index_ = state->left_tuples_index_;
    left_child_call_times_ = state->left_child_call_times_;
    be_call_times_ = state->be_call_times_;
    is_end_ = false;
    finished_begin_tuple_ = state->finish_begin_tuple_;
    initialized_ = state->is_hash_table_built_;
    if(left_tuples_index_ != -1) {
        left_iter_ = hash_table_.find(state->left_iter_key_);
    }

    HashJoinCheckpointInfo curr_ckpt_info = {.ck_timestamp_ = std::chrono::high_resolution_clock::now(), .left_hash_table_curr_tuple_count_ = left_hash_table_curr_tuple_count_, .left_rc_op_ = 0, .state_change_time_ = left_hash_table_curr_tuple_count_ + be_call_times_};
    state_change_time_ = left_hash_table_checkpointed_tuple_count_ + be_call_times_;
    ck_infos_.push_back(curr_ckpt_info);
    std::cout << "HashJoinOp, op_id: "  << operator_id_ << ", HashTableCount: " << left_hash_table_curr_tuple_count_  << "be_call_time: " << be_call_times_ << ", state_change_time: " << state_change_time_ << "\n";

    // 先build了哈希表，才能调用当前函数
    if(!initialized_) {
        // 如果哈希表没有构建完全，需要首先恢复左算子状态，哈希表在后面begintuple的时候再构建
        if(state->left_child_is_join_ == false) {
            if(auto x = dynamic_cast<IndexScanExecutor *>(left_.get())) {
                if(state->left_child_state_->finish_begin_tuple_ == false) {
                    x->beginTuple();
                }
                else {
                    x->load_state_info(dynamic_cast<IndexScanOperatorState *>(state->left_child_state_));
                    x->nextTuple();
                }
            }
            else if (auto x = dynamic_cast<ProjectionExecutor *>(left_.get())) {
                if(state->left_child_state_->finish_begin_tuple_ == false) {
                    x->beginTuple();
                }
                else {
                    x->load_state_info(dynamic_cast<ProjectionOperatorState *>(state->left_child_state_));
                    x->nextTuple();
                }
            }
        }
    }
}