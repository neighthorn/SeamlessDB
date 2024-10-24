#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::shared_ptr<AbstractExecutor> left_;
    std::shared_ptr<AbstractExecutor> right_;
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;

    char* left_tuple_block_;
    int left_len_;
    int left_offset_;
    int left_size_;

    char* right_tuple_block_;
    int right_len_;
    int right_offset_;
    int right_size_;

   public:
    NestedLoopJoinExecutor(std::shared_ptr<AbstractExecutor> left, std::shared_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
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

        left_tuple_block_ = new char[JOIN_BUFFER_SIZE];
        right_tuple_block_ = new char[JOIN_BUFFER_SIZE];
        left_len_ = left_->tupleLen();
        right_len_ = right->tupleLen();
        left_offset_ = right_offset_ = 0;
    }

    std::string getType() override { return "Join"; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    void update_left_buffer() {
        left_size_ = 0;
        while(left_size_ + left_len_ < JOIN_BUFFER_SIZE && !left_->is_end()) {
            auto left_rec = left_->Next();
            memcpy(left_tuple_block_ + left_size_, left_rec->raw_data_, left_len_);
            left_size_ += left_len_;
            left_->nextTuple();
            if(left_->is_end()) break;
        }
        left_offset_ = 0;
    }

    void update_right_buffer() {
        right_size_ = 0;
        while(right_size_ + right_len_ < JOIN_BUFFER_SIZE && !right_->is_end()) {
            auto right_rec = right_->Next();
            memcpy(right_tuple_block_ + right_size_, right_rec->raw_data_, right_len_);
            right_size_ += right_len_;
            right_->nextTuple();
            if(right_->is_end()) break;
        }
        right_offset_ = 0;
    }

    char* get_next_left_tuple() {
        if(left_offset_ >= left_size_) update_left_buffer();
        char* tuple = left_tuple_block_ + left_offset_;
        left_offset_ += left_len_;
        return tuple;
    }

    char* get_next_right_tuple() {
        if(right_offset_ >= right_size_) update_right_buffer();
        char* tuple = right_tuple_block_ + right_offset_;
        right_offset_ += right_len_;
        return tuple;
    }

    // 右表扫描完了获取左表的下一条

    void beginTuple() override {
        isend = false;
        left_->beginTuple();
        if (left_->is_end()) {
            isend = true;
            return;
        }
        right_->beginTuple();
        if (right_->is_end()) {
            isend = true;
            return;
        }
        // auto left_rec = left_->Next();
        // auto right_rec = right_->Next();
        char* left_data = get_next_left_tuple();
        char* right_data = get_next_right_tuple();

        while(!eval_conds(fed_conds_, left_data, right_data)) {
            
        }

        //TODO 判断是否符合连接条件
        // while (!eval_conds(fed_conds_, left_rec.get(), right_rec.get())) {
        //     right_->nextTuple();
        //     if (right_->is_end()) {
        //         left_->nextTuple();
        //         if(left_->is_end()) {
        //             isend = true;
        //             break;
        //         }
        //         right_->beginTuple();
        //     }
        //     left_rec = left_->Next();
        //     right_rec = right_->Next();
        // }

    }

    void nextTuple() override {
        std::unique_ptr<Record> left_rec, right_rec;
        do {
            right_->nextTuple();
            if (right_->is_end()) {
                left_->nextTuple();
                if(left_->is_end()) {
                    isend = true;
                    break;
                }
                right_->beginTuple();
            }
            left_rec = left_->Next();
            right_rec = right_->Next();
        } while(!eval_conds(fed_conds_, left_rec.get(), right_rec.get()));
    }

    bool is_end() const override { return isend; }

    std::unique_ptr<Record> Next() override {
        assert(!is_end());
        auto record = std::make_unique<Record>(len_);
        memcpy(record->raw_data_, left_->Next()->raw_data_, left_->tupleLen());
        memcpy(record->raw_data_ + left_->tupleLen(), right_->Next()->raw_data_, right_->tupleLen());
        return record;
    }

    Rid &rid() override { return _abstract_rid; }

    ColMeta get_col_offset(const TabCol &target) override {
        auto left_pos = left_->get_col_offset(target);
        if(left_pos.len == -1){
            auto right_pos = right_->get_col_offset(target);
            ColMeta col = {.tab_name = right_pos.tab_name,
               .name = right_pos.name,
               .type = right_pos.type,
               .len = right_pos.len,
               .offset = right_pos.offset + (int)left_->tupleLen()};
            return col;
        }
        return left_pos;
    }

    bool eval_cond(const Condition &cond, const Record *left, const Record *right) {
        auto lhs_col = left_->get_col_offset(cond.lhs_col);
        char *lhs = left->raw_data_ + lhs_col.offset;

        auto rhs_col = right_->get_col_offset(cond.rhs_col);
        char *rhs = right->raw_data_ + rhs_col.offset;
        
        assert(rhs_col.type == lhs_col.type);  // TODO convert to common type
        int cmp = ix_compare(lhs, rhs, lhs_col.type, lhs_col.len);
        if (cond.op == OP_EQ) {
            return cmp == 0;
        } else if (cond.op == OP_NE) {
            return cmp != 0;
        } else if (cond.op == OP_LT) {
            return cmp < 0;
        } else if (cond.op == OP_GT) {
            return cmp > 0;
        } else if (cond.op == OP_LE) {
            return cmp <= 0;
        } else if (cond.op == OP_GE) {
            return cmp >= 0;
        } else {
            throw InternalError("Unexpected op type");
        }
    }

    bool eval_conds(const std::vector<Condition> &conds, const Record *left, const Record *right) {
        return std::all_of(conds.begin(), conds.end(),
                           [&](const Condition &cond) { return eval_cond(cond, left, right); });
    }

    bool eval_cond(const Condition& cond, const char* left, const char* right) {
        auto lhs_col = left_->get_col_offset(cond.lhs_col);
        const char *lhs = left + lhs_col.offset;

        auto rhs_col = right_->get_col_offset(cond.rhs_col);
        const char *rhs = right + rhs_col.offset;
        
        assert(rhs_col.type == lhs_col.type);  // TODO convert to common type
        int cmp = ix_compare(lhs, rhs, lhs_col.type, lhs_col.len);
        if (cond.op == OP_EQ) {
            return cmp == 0;
        } else if (cond.op == OP_NE) {
            return cmp != 0;
        } else if (cond.op == OP_LT) {
            return cmp < 0;
        } else if (cond.op == OP_GT) {
            return cmp > 0;
        } else if (cond.op == OP_LE) {
            return cmp <= 0;
        } else if (cond.op == OP_GE) {
            return cmp >= 0;
        } else {
            throw InternalError("Unexpected op type");
        }
    }

    bool eval_conds(const std::vector<Condition>& conds, const char* left, const char* right) {
        return std::all_of(conds.begin(), conds.end(),
                            [&](const Condition& cond) { return eval_cond(cond, left, right); });
    }
    
    int checkpoint(char* dest) override {

    }

    std::chrono::time_point<std::chrono::system_clock> get_latest_ckpt_time() override {
        assert(0);
        return std::chrono::high_resolution_clock::now();
    }
};