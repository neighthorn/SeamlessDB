#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class HashJoinExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> left_;
    std::unique_ptr<AbstractExecutor> right_;
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    int join_key_size_;                         // join条件对应字段总长度
    bool initialized_;                                      // 是否已经构建完成哈希表
    std::unordered_map<std::string, std::vector<Record*>> hash_table_;   // left_算子中间结果的hash表

public:
    HashJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, std::vector<Condition> conds, Context* context, int sql_id, int operator_id) 
        : AbstractExecutor(sql_id, operator_id) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();

        join_key_size_ = 0;
        for(const auto& cond: fed_conds_) {
            auto left_col = *(left_->get_col(cols_, cond.lhs_col));
            join_key_size_ += left_col.len;
        }

        auto right_cols = right_->cols();
        for(auto& col: right_cols) {
            col.offset += left_->tupleLen();
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        fed_conds_ = std::move(conds);
        initialized_ = false;
    }

    std::string getType() override { return "HashJoin"; }

    size_t tupleLen() const override { return len_; }
    
    const std::vector<ColMeta> & cols() const override { return cols_; }

    bool is_end() const override {
        return right_->is_end();
    }

    void beginTuple() override {
        if(!initialized_) {
            left_->beginTuple();
            char* left_key = new char[join_key_size_];
            int offset;

            while(!left_->is_end()) {
                left_->nextTuple();
                auto rec = left_->Next();
                memset(left_key, 0, join_key_size_);
                offset = 0;

                // make record key
                for(const auto& cond: fed_conds_) {
                    auto left_cols = left_->cols();
                    auto left_col = *(left_->get_col(left_cols, cond.lhs_col));
                    auto left_value = fetch_value(rec, left_col);
                    memcpy(left_key + offset, left_value.raw->data, left_col.len);
                    offset += left_col.len;
                }
                assert(offset == join_key_size_);

                hash_table_[std::string(left_key, join_key_size_)].push_back(rec.get());
            }

            initialized_ = true;
            delete[] left_key;
        }

        right_->beginTuple();
        auto right_rec = right_->Next();
        while(!right_->is_end() && find_match_join_key(right_rec.get()) == hash_table_.end()) {
            right_->nextTuple();
            right_rec = right_->Next();
        }
    }

    std::unique_ptr<Record> Next() {
        assert(!is_end());

        auto left_rec = left_->Next();
        auto right_rec = right_->Next();
        auto res = std::make_unique<Record>(len_);
        memcpy(res->raw_data_, left_rec->raw_data_, left_rec->data_length_);
        memcpy(res->raw_data_ + left_rec->data_length_, right_rec->raw_data_, right_rec->data_length_);
        
        return res;
    }

    std::unordered_map<std::string, std::vector<Record*>>::const_iterator find_match_join_key(const Record* right_tuple) {
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
        
        return hash_table_.find(std::string(key, join_key_size_));
    }
};