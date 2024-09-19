#include "executor_hash_join.h"
#include "executor_block_join.h"

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

            hash_table_[std::string(left_key, join_key_size_)].push_back(std::move(left_rec));
        }

        initialized_ = true;
        delete[] left_key;
    }

    std::cout << "HashJoinExecutor::finish build hash_table for left table\n";

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

std::pair<bool, double> HashJoinExecutor::judge_state_reward(HashJoinExecutor* curr_ck_info) {
    /*
        要记录的主要是左算子产生的哈希表的大小，哈希表的增量大小计算
    */
   
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

}

void HashJoinExecutor::load_state_info(HashJoinCheckpointInfo* hash_join_op) {

}