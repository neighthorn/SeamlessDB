#pragma once
#include "executor_abstract.h"

class GatherExecutor;
class GatherOperatorState;

struct GatherCheckpointInfo {
    std::chrono::time_point<std::chrono::system_clock> ck_timestamp_;
    std::vector<int> result_buffer_curr_tuple_counts_;
    std::vector<double> left_rc_ops_;
    int state_change_time_;
};

// @TODO 支持并行IndexScan算子（在当前系统中SequentialScan也是走的IndexScan算子的接口）
// gather 算子的不保证结果的顺序
class GatherExecutor : public AbstractExecutor {
public:
    int worker_thread_num_;    // 有多少个并行线程用于执行，为了简单，当前gather节点所在的主线程不进行实际的执行任务，只负责merge其他worker线程的结果
    // 为了防止多线程的竞争，每个线程都有自己的buffer，最后再合并到result_buffer_中
    std::vector<std::vector<std::unique_ptr<Record>>> result_buffers_;
    std::vector<std::shared_ptr<AbstractExecutor>> workers_;
    std::vector<std::thread> worker_threads_;   // worker线程
    std::vector<int> consumed_result_counts_;    // 记录每个worker已经消费的结果数量
    
    // 结果记录字段
    std::vector<ColMeta> cols_;
    // 结果字段总长度
    size_t len_;

    int next_worker_index_;     // 下一个要消费结果的worker的index

    std::vector<GatherCheckpointInfo> ck_infos_;      // 记录建立检查点时的信息

    bool is_end_;
    int state_change_time_;


    GatherExecutor(int worker_thread_num, std::vector<std::shared_ptr<AbstractExecutor>> workers, Context* context, int sql_id, int operator_id)
        :AbstractExecutor(sql_id, operator_id) {
        worker_thread_num_ = worker_thread_num;
        workers_ = workers;
        assert(workers_.size() == worker_thread_num_);
        assert(workers_.size() > 0);
        for(int i = 0; i < worker_thread_num_; ++i) {
            result_buffers_.emplace_back();
            consumed_result_counts_.push_back(0);
        }

        // 所有的worker应该都是一样的算子，只是access的数据范围不同
        len_ = workers_[0]->tupleLen();
        cols_ = workers_[0]->cols();

        ck_infos_.emplace_back(GatherCheckpointInfo{.ck_timestamp_ = std::chrono::high_resolution_clock::now(), .result_buffer_curr_tuple_counts_ = std::vector<int>(worker_thread_num_, 0), .left_rc_ops_ = std::vector<double>(worker_thread_num_, 0), .state_change_time_ = 0});
        exec_type_ = ExecutionType::GATHER;
        
        be_call_times_ = 0;
        
        finished_begin_tuple_ = false;
        is_in_recovery_ = false;

        context_ = context;
        state_change_time_ = 0;
    }

    void beginTuple() override;
    void nextTuple() override;
    Rid& rid() override {
        return _abstract_rid;
    }
    std::unique_ptr<Record> Next() override;

    int checkpoint(char* dest) override { return -1; };
    
    std::pair<bool, double> judge_state_reward(GatherCheckpointInfo* curr_ck_info);
    int64_t getRCop(std::chrono::time_point<std::chrono::system_clock> curr_time);
    void write_state_if_allow(int type = 0);
    void load_state_info(GatherOperatorState* state);
    std::chrono::time_point<std::chrono::system_clock> get_latest_ckpt_time() override;
    double get_curr_suspend_cost() override;
    void write_state();
};