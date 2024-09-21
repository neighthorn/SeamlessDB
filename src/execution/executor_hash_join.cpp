#include "executor_hash_join.h"
#include "executor_block_join.h"
#include "executor_index_scan.h"
#include "state/op_state_manager.h"
#include "state/state_manager.h"
#include "state/state_item/op_state.h"
#include "debug_log.h"

void HashJoinExecutor::beginTuple() {
    if(!initialized_) {
        std::unique_ptr<Record> left_rec;
        char* left_key = new char[join_key_size_];
        int offset;

        left_->beginTuple();
        if(left_->is_end()) {
            is_end_ = true;
            return;
        }

        for(;!left_->is_end(); left_->nextTuple()) {
            left_rec = left_->Next();
            memset(left_key, 0, join_key_size_);
            offset = 0;

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

            RwServerDebug::getInstance()->DEBUG_PRINT("[HashJoinExecutor::beginTuple, initiate left hash table][operator_id: " + std::to_string(operator_id_) + "][key: " + key + "]");

            if(hash_table_.find(key) == hash_table_.end()) {
                hash_table_[key] = std::vector<std::unique_ptr<Record>>();
                checkpointed_indexes_[key] = 0;
            }
            hash_table_[key].push_back(std::move(left_rec));
            left_hash_table_curr_tuple_count_ ++;
        }

        initialized_ = true;
        delete[] left_key;
    }

    right_->beginTuple();
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

    RwServerDebug::getInstance()->DEBUG_PRINT("[HashJoinExecutor::append_tuple_to_hash_table_from_state][operator_id: " + std::to_string(operator_id_) + "][key: " + key + "]");
    
    if(hash_table_.find(key) == hash_table_.end()) {
        hash_table_[key] = std::vector<std::unique_ptr<Record>>();
        checkpointed_indexes_[key] = 0;
    }
    hash_table_[key].push_back(std::move(left_rec));
    left_hash_table_curr_tuple_count_ ++;
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
    
    return left_iter_;
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
    
    double src_op;
    double rc_op = -1;
    
    
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

    if(auto x =  dynamic_cast<HashJoinExecutor *>(left_.get())) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(curr_time - latest_ck_info->ck_timestamp_).count() + x->getRCop(latest_ck_info->ck_timestamp_);
    } 
    else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor*>(left_.get())) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(curr_time - latest_ck_info->ck_timestamp_).count() + x->getRCop(latest_ck_info->ck_timestamp_);
    }
    else {
        return std::chrono::duration_cast<std::chrono::milliseconds>(curr_time - latest_ck_info->ck_timestamp_).count();
    }
}

void HashJoinExecutor::write_state_if_allow(int type) {
    HashJoinCheckpointInfo curr_ckpt_info = {.ck_timestamp_ = std::chrono::high_resolution_clock::now()};
    if(state_open_) {
        auto [able_to_write, src_op] = judge_state_reward(&curr_ckpt_info);
        if(able_to_write) {
            auto [status, actual_size] = context_->op_state_mgr_->add_operator_state_to_buffer(this, src_op);
            if(status) {
                ck_infos_.push_back(curr_ckpt_info);
            }
        }
    }
}

void HashJoinExecutor::load_state_info(HashJoinOperatorState* state) {
    // load state except hash_table
    assert(state != nullptr);

    if(auto x = dynamic_cast<IndexScanExecutor *>(right_.get())) {
        x->load_state_info(&(state->right_index_scan_state_));
    }

    right_->nextTuple();

    left_tuples_index_ = state->left_tuples_index_;
    left_child_call_times_ = state->left_child_call_times_;
    be_call_times_ = state->be_call_times_;
    is_end_ = false;

    // 先build了哈希表，才能调用当前函数
    if(!initialized_) {
        // 如果哈希表没有构建完全，需要首先恢复左算子状态，哈希表在后面begintuple的时候再构建
        if(auto x = dynamic_cast<IndexScanExecutor *>(left_.get())) {
            x->load_state_info(&(state->left_index_scan_state_));
        }
    }
}