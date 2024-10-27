#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor;
class SortOperatorState;

struct SortCheckpointInfo {
    std::chrono::time_point<std::chrono::system_clock> ck_timestamp_;
    double left_rc_op_;
    int state_change_time_;
};

class SortExecutor : public AbstractExecutor {
public:
    std::shared_ptr<AbstractExecutor> prev_;
    //目前只支持一个键排序
    ColMeta cols_;
    bool is_desc_;
    std::vector<std::unique_ptr<Record>> unsorted_records_;
    int* sorted_index_;     // 比如[3, 1, 0, 2]代表unsorted_records_中的第3个元素是排第一的
    int num_records_;
    Context* context_;
    bool is_sorted_;
    bool is_sort_index_checkpointed_;
    int tuple_len_;

    std::vector<SortCheckpointInfo> ck_infos_;
    int checkpointed_tuple_num_;
    int state_change_time_;

   public:
    SortExecutor(std::shared_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc, Context* context, int sql_id, int operator_id):
        AbstractExecutor(sql_id, operator_id) {
        prev_ = prev;
        cols_ = prev_->get_col_offset(sel_cols);
        tuple_len_ = prev_->tupleLen();
        is_desc_ = is_desc;
        num_records_ = 0;
        context_ = context;
        is_sorted_ = false;

        be_call_times_ = 0;
        finished_begin_tuple_ = false;
        is_sort_index_checkpointed_ = false;
        checkpointed_tuple_num_ = 0;
        exec_type_ = ExecutionType::SORT;
        is_in_recovery_ = false;
        state_change_time_ = 0;
        ck_infos_.push_back(SortCheckpointInfo{.ck_timestamp_ = std::chrono::high_resolution_clock::now(), .left_rc_op_ = 0, .state_change_time_ = 0});
    }

    std::string getType() override { return "Sort"; }

    size_t tupleLen() const override { return tuple_len_; }

    const std::vector<ColMeta> &cols() const override { return prev_->cols(); }

    void beginTuple() override { 
        std::cout << "SortExecutor: beginTuple" << std::endl;
        if(is_in_recovery_ && finished_begin_tuple_)  return;

        if(is_in_recovery_ == false) prev_->beginTuple();

        if(prev_->is_end()) {
            is_sorted_ = true;
            finished_begin_tuple_ = true;
            write_state_if_allow();
            return;
        }

        for(; !prev_->is_end(); prev_->nextTuple()) {
            // TODO: 这里直接move了，后面记录检查点的时候会不会有问题？
            unsorted_records_.push_back(std::move(prev_->Next()));
            num_records_ ++;
            left_child_call_times_ ++;
            state_change_time_ ++;
            write_state_if_allow();
        } 

        std::cout << "SortExecutor: beginTuple: num_records_=" << num_records_ << ", size: " << num_records_ * tuple_len_ << std::endl;

        sorted_index_ = new int[num_records_];
        for(int i = 0; i < unsorted_records_.size(); i++) {
            sorted_index_[i] = i;
        }

        std::sort(sorted_index_, sorted_index_ + num_records_, [&](int lhs, int rhs) {
            const Record* left = unsorted_records_[lhs].get();
            const Record* right = unsorted_records_[rhs].get();

            const char* left_field = left->raw_data_ + cols_.offset;
            const char* right_field = right->raw_data_ + cols_.offset;

            return std::memcmp(left_field, right_field, cols_.len) < 0;
        });
        state_change_time_ += 10;

        is_sorted_ = true;
        finished_begin_tuple_ = true;
        write_state_if_allow();
    }

    void nextTuple() override {
        be_call_times_ ++;
        state_change_time_ ++;
    }

    bool is_end() const override { return be_call_times_ >= num_records_; }

    std::unique_ptr<Record> Next() override {
        assert(!is_end());
        // std::cout << "sorted_index[be_call_times]: " << sorted_index_[be_call_times_] << ", be_call_times: " << be_call_times_ << std::endl;
        assert(sorted_index_[be_call_times_] < unsorted_records_.size());
        return std::move(unsorted_records_[sorted_index_[be_call_times_]]);
        // TODO: remove the annotation below
        write_state_if_allow();
    }
    
    ColMeta get_col_offset(const TabCol &target) override {
        return prev_->get_col_offset(target);
    }

    Rid &rid() override { return _abstract_rid; }

    int checkpoint(char* dest) {
        return 0;
    }

    std::pair<bool, double> judge_state_reward(SortCheckpointInfo* curr_ck_info);
    int64_t getRCop(std::chrono::time_point<std::chrono::system_clock> curr_time);
    void write_state_if_allow(int type = 0);
    void load_state_info(SortOperatorState *sort_op);

    // 对比工作
    std::chrono::time_point<std::chrono::system_clock> get_latest_ckpt_time() override;
    double get_curr_suspend_cost() override;
    void write_state();
};