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
    int left_hash_table_curr_tuple_count_;
    double left_rc_op_;
    int state_change_time_;
};

class HashJoinExecutor : public AbstractExecutor {
    
public:
    std::shared_ptr<AbstractExecutor> left_;
    std::shared_ptr<AbstractExecutor> right_;
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    int join_key_size_;                         // join条件对应字段总长度
    bool initialized_;                                      // 是否已经构建完成哈希表
    std::unordered_map<std::string, std::vector<std::unique_ptr<Record>>> hash_table_;   // left_算子中间结果的hash表
    std::unordered_map<std::string, std::vector<std::unique_ptr<Record>>>::const_iterator left_iter_;   // 和右边tuple符合join条件的hash表中的iter
    int left_tuples_index_;                                                             // 符合条件的left_records的index（hash表中的vectorindex）
    std::unordered_map<std::string, size_t> checkpointed_indexes_;                      // 上一次检查点最后一次记录的index
    int left_hash_table_checkpointed_tuple_count_;
    int left_hash_table_curr_tuple_count_;

    bool is_end_;
    int state_change_time_;

public:
    std::vector<HashJoinCheckpointInfo> ck_infos_;      // 记录建立检查点时的信息
    // int left_child_call_times_;     // 左儿子调用次数
    // int be_call_times_;             // 被调用次数

    HashJoinExecutor(std::shared_ptr<AbstractExecutor> left, std::shared_ptr<AbstractExecutor> right, std::vector<Condition> conds, Context* context, int sql_id, int operator_id) 
        : AbstractExecutor(sql_id, operator_id) {
        left_ = left;
        right_ = right;
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
        left_hash_table_checkpointed_tuple_count_ = 0;
        left_hash_table_curr_tuple_count_ = 0;

        left_iter_ = hash_table_.end();
        left_tuples_index_ = -1;

        ck_infos_.push_back(HashJoinCheckpointInfo{.ck_timestamp_ = std::chrono::high_resolution_clock::now(), .left_hash_table_curr_tuple_count_ = 0, .left_rc_op_ = 0, .state_change_time_ = 0});
        exec_type_ = ExecutionType::HASH_JOIN;

        be_call_times_ = 0;
        left_child_call_times_ = 0;

        finished_begin_tuple_ = false; 
        is_in_recovery_ = false;

        context_ = context;
        state_change_time_ = 0;
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

    std::pair<bool, double> judge_state_reward(HashJoinCheckpointInfo* curr_ck_info);
    int64_t getRCop(std::chrono::time_point<std::chrono::system_clock> curr_time);
    void write_state_if_allow(int type = 0);
    void load_state_info(HashJoinOperatorState* hash_join_op);
    void append_tuple_to_hash_table_from_state(char* src, int left_rec_len, char* join_key);
    std::chrono::time_point<std::chrono::system_clock> get_latest_ckpt_time() override;
    double get_curr_suspend_cost() override;
    void write_state();
};