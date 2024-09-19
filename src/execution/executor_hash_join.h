#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class HashJoinExecutor;
class HashJoinOperatorState;

struct HashJoinCheckpointInfo {
    std::chrono::time_point<std::chrono::system_clock> ck_timestamp_;
};

class HashJoinExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> left_;
    std::unique_ptr<AbstractExecutor> right_;
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    int join_key_size_;                         // join条件对应字段总长度
    bool initialized_;                                      // 是否已经构建完成哈希表
    std::unordered_map<std::string, std::vector<std::unique_ptr<Record>>> hash_table_;   // left_算子中间结果的hash表
    std::unordered_map<std::string, std::vector<std::unique_ptr<Record>>>::const_iterator left_iter_;   // 和右边tuple符合join条件的hash表中的iter
    int left_tuples_index_;                                                             // 符合条件的left_records的index（hash表中的vectorindex）
    std::unordered_map<std::string, size_t> checkpointed_indexes_;                      // 上一次检查点最后一次记录的index

    bool is_end_;

public:
    std::vector<HashJoinCheckpointInfo> ck_infos_;      // 记录建立检查点时的信息
    int left_child_call_times_;     // 左儿子调用次数
    int be_call_times_;             // 被调用次数

    HashJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, std::vector<Condition> conds, Context* context, int sql_id, int operator_id) 
        : AbstractExecutor(sql_id, operator_id) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();

        join_key_size_ = 0;
        for(const auto& cond: conds) {
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
        is_end_ = false;
    }

    bool is_hash_table_built() const {
        return initialized_;
    }

    std::string getType() override { return "HashJoin"; }

    size_t tupleLen() const override { return len_; }
    
    const std::vector<ColMeta> & cols() const override { return cols_; }

    bool is_end() const override {
        return is_end_;
    }

    void beginTuple() override;

    void nextTuple() override;

    Rid& rid() override {
        return _abstract_rid;
    }

    std::unique_ptr<Record> Next();

    std::unordered_map<std::string, std::vector<std::unique_ptr<Record>>>::const_iterator find_match_join_key(const Record* right_tuple);

    int checkpoint(char* dest) override { return -1; };

    std::pair<bool, double> judge_state_reward(HashJoinExecutor* curr_ck_info);
    int64_t getRCop(std::chrono::time_point<std::chrono::system_clock> curr_time);
    void write_state_if_allow(int type = 0);
    void load_state_info(HashJoinCheckpointInfo* hash_join_op);
};