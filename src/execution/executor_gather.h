#pragma once

#include <atomic>
#include <queue>
#include <mutex>
#include <chrono>
#include <condition_variable>
#include "common/context.h"
#include "executor_abstract.h"

class GatherExecutor;
class GatherOperatorState;

/**
 * Gather算子当前时刻的状态：
 * 1. 当前算子的被调用次数
 * 2. 当前时刻每个result_queue中的剩余未消耗tuple
 * 3. 截止到目前为止，每个worker一共产生的tuple数量（儿子算子Next()接口的调用次数）
 * Gather算子的记录代价：从上一次的checkpoint恢复到当前时刻的代价
 * 1. 每个worker中上一次检查点没有记录而当前检查点需要记录的tuple代价总和
 * Gather算子的恢复代价：
 * 1. ti-t(last_ckpt) + max{child_rc_op} 
 */
struct GatherCheckpointInfo {
    std::chrono::time_point<std::chrono::system_clock> ck_timestamp_;
    // std::vector<int> result_buffer_curr_tuple_counts_;
    int64_t max_child_rc_op_;
    int state_change_time_;
};

// 为了能够保证恢复的正确性，需要保证gather算子输出结果顺序的确定性，因此，在消费结果时，按照轮转的方式来依次消费每个worker的结果
class GatherExecutor : public AbstractExecutor {
public:
    int worker_thread_num_;    // 有多少个并行线程用于执行，为了简单，当前gather节点所在的主线程不进行实际的执行任务，只负责merge其他worker线程的结果
    // 为了防止多线程的竞争，每个线程都有自己的buffer，最后再合并到result_buffer_中
    // std::vector<std::queue<std::unique_ptr<Record>>> result_queues_;
    std::vector<std::vector<std::unique_ptr<Record>>> result_queues_;
    std::vector<int> consumed_sizes_;
    std::vector<std::mutex> result_queues_mutex_;
    std::vector<std::atomic<bool>> worker_is_end_;  // 记录每个worker是否已经结束
    std::vector<std::atomic<size_t>> queue_sizes_;   // 记录每个worker的结果队列大小
    int* result_buffer_curr_tuple_counts_;   // 记录当前时刻每个worker的结果队列大小，用于获取状态

    std::vector<std::shared_ptr<AbstractExecutor>> workers_;
    std::vector<std::thread> worker_threads_;   // worker线程
    std::condition_variable next_tuple_cv_;         // 用于通知主线程有新的结果
    
    
    // 结果记录字段
    std::vector<ColMeta> cols_;
    // 结果字段总长度
    size_t len_;

    int next_worker_index_;     // 下一个要消费结果的worker的index

    std::vector<GatherCheckpointInfo> ck_infos_;      // 记录建立检查点时的信息

    int state_change_time_;

    Context* context_;

    GatherExecutor(int worker_thread_num, std::vector<std::shared_ptr<AbstractExecutor>>& workers, Context* context, int sql_id, int operator_id)
        :AbstractExecutor(sql_id, operator_id), result_queues_(worker_thread_num), consumed_sizes_(worker_thread_num),
        worker_is_end_(worker_thread_num), queue_sizes_(worker_thread_num), context_(context), result_queues_mutex_(worker_thread_num) {
        worker_thread_num_ = worker_thread_num;
        workers_ = std::move(workers);
        assert(workers_.size() == worker_thread_num_);
        assert(workers_.size() > 0);
        // consumed_result_counts_.reserve(worker_thread_num_);
        // result_total_counts_.reserve(worker_thread_num_);

        // for(int i = 0; i < worker_thread_num_; ++i) {
        //     result_queues_.emplace_back(std::queue<std::unique_ptr<Record>>());
        //     result_queues_mutex_.emplace_back(std::mutex());
        // }

        for(int i = 0; i < worker_thread_num_; ++i) {
            worker_is_end_[i] = false;
            queue_sizes_[i] = 0;
            consumed_sizes_[i] = 0;
        }

        next_worker_index_ = 0;
        result_buffer_curr_tuple_counts_ = new int[worker_thread_num_];
        
        // 所有的worker应该都是一样的算子，只是access的数据范围不同
        len_ = workers_[0]->tupleLen();
        cols_ = workers_[0]->cols();

        ck_infos_.emplace_back(GatherCheckpointInfo{.ck_timestamp_ = std::chrono::high_resolution_clock::now(), .max_child_rc_op_ = 0, .state_change_time_ = 0});
        exec_type_ = ExecutionType::GATHER;
        
        be_call_times_ = 0;
        
        finished_begin_tuple_ = false;
        is_in_recovery_ = false;

        context_ = context;
        state_change_time_ = 0;

        std::cout << "GatherExecutor init" << std::endl;
        std::cout << "worker_thread_num: " << worker_thread_num_ << std::endl;
    }

    ~GatherExecutor() {
        std::cout << "GatherExecutor destruct" << std::endl;
        for(int i = 0; i < worker_thread_num_; ++i) {
            worker_threads_[i].join();
        }
        worker_threads_.clear();
        for(int i = 0; i < worker_thread_num_; ++i) {
            workers_[i].reset();
        }
        workers_.clear();
        if(result_buffer_curr_tuple_counts_ != nullptr) {
            delete[] result_buffer_curr_tuple_counts_;
        }
    }

    void print_debug() {
        std::cout << "Status for workers in GatherExecutor" << std::endl;
        std::cout << "Next worker index: " << next_worker_index_ << std::endl;
        for(int i = 0; i < worker_thread_num_; ++i) {
            std::cout << "Worker " << i << " is_end: " << workers_[i]->is_end() << std::endl;
        }
        std::cout << "Record count in ResultBuffer" << std::endl;
        for(int i = 0; i < worker_thread_num_; ++i) {
            std::cout << "Worker " << i << " Record count: " << result_queues_[i].size() << std::endl;
        }
        assert(0);
    }

    void launch_workers();

    std::string getType() override { return "Gather"; }
    
    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    void beginTuple() override;
    void nextTuple() override;
    Rid& rid() override {
        return _abstract_rid;
    }
    std::unique_ptr<Record> Next() override;

    bool is_end() const override;

    int checkpoint(char* dest) override { return -1; };
    
    std::pair<bool, double> judge_state_reward(GatherCheckpointInfo* curr_ck_info);
    int64_t getRCop(std::chrono::time_point<std::chrono::system_clock> curr_time);
    void write_state_if_allow(int type = 0);
    void load_state_info(GatherOperatorState* state);
    std::chrono::time_point<std::chrono::system_clock> get_latest_ckpt_time() override;
    double get_curr_suspend_cost() override;
    void write_state();
};