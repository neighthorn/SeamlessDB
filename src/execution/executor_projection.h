#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class ProjectionExecutor;
class ProjectionOperatorState;

struct ProjectionCheckpointInfo {
    std::chrono::time_point<std::chrono::system_clock> ck_timestamp_;
    double left_rc_op_;
    int state_change_time_;
};

class ProjectionExecutor : public AbstractExecutor {
public:
    std::shared_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;
    size_t len_;
    std::vector<size_t> sel_idxs_;

    std::vector<ProjectionCheckpointInfo> ck_infos_;
    // int be_call_times_; // 只需要记录be_call_times，left_child_call_times_应该和当前节点的be_call_times一致

    bool is_root_;
    int curr_result_num_;
    int checkpointed_result_num_;
    int state_change_time_;

   public:
    ProjectionExecutor(std::shared_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols, Context* context, int sql_id, int operator_id) : AbstractExecutor(sql_id, operator_id) {
        prev_ = prev;

        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;

        exec_type_ = ExecutionType::PROJECTION;

        be_call_times_ = 0;
        is_root_ = false;
        left_child_call_times_ = 0;
        context_ = context;
        finished_begin_tuple_ = false;
        state_change_time_ = 0;
    }

    void set_root() { 
        is_root_ = true; 
        ck_infos_.push_back(ProjectionCheckpointInfo{.ck_timestamp_ = std::chrono::high_resolution_clock::now(), .left_rc_op_ = 0});
    }

    std::string getType() override { return "Projection"; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    void beginTuple() override { 
        // if(finished_begin_tuple_)  return;
        prev_->beginTuple(); finished_begin_tuple_ = true; 
    }

    void nextTuple() override {
        assert(!prev_->is_end());
        prev_->nextTuple();
    }

    bool is_end() const override { return prev_->is_end(); }

    std::unique_ptr<Record> Next() override {
        assert(!is_end());
        auto &prev_cols = prev_->cols();
        auto prev_rec = prev_->Next();
        auto &proj_cols = cols_;
        auto proj_rec = std::make_unique<Record>(len_);
        for (size_t proj_idx = 0; proj_idx < proj_cols.size(); proj_idx++) {
            size_t prev_idx = sel_idxs_[proj_idx];
            auto &prev_col = prev_cols[prev_idx];
            auto &proj_col = proj_cols[proj_idx];
            memcpy(proj_rec->raw_data_ + proj_col.offset, prev_rec->raw_data_ + prev_col.offset, proj_col.len);
        }

        be_call_times_ ++;
        left_child_call_times_ ++;

        if(is_root_) {
            curr_result_num_ ++;
            write_state_if_allow();
        }
        return proj_rec;
    }

    ColMeta get_col_offset(const TabCol &target) override {
        return prev_->get_col_offset(target);
    }

    Rid &rid() override { return _abstract_rid; }

    int checkpoint(char* dest) override {
        return 0;
    }

    void load_state_info(ProjectionOperatorState *proj_op_state);

    std::pair<bool, double> judge_state_reward(ProjectionCheckpointInfo *curr_ck_info);
    int64_t getRCop(std::chrono::time_point<std::chrono::system_clock> curr_time);
    void write_state_if_allow(int type = 0);

    std::chrono::time_point<std::chrono::system_clock> get_latest_ckpt_time() override;
    double get_curr_suspend_cost() override;
    void write_state();
};