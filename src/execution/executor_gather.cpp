#include "executor_gather.h"

void GatherExecutor::beginTuple() {
    if(is_in_recovery_ && finished_begin_tuple_) return;

    // 开启worker_thread_num_个线程，每个线程负责一个subplan的执行
    for(int i = 0; i < worker_thread_num_; ++i) {
        workers_[i]->beginTuple();
    }

    for(int i = 0; i < worker_thread_num_; ++i) {
        worker_threads_.push_back(std::thread([this, i](){
            while(!workers_[i]->is_end()) {
                result_buffers_[i].push_back(workers_[i]->Next());
                workers_[i]->nextTuple();
            }
        }));
    }
}

void GatherExecutor::nextTuple() {
    // 找一个待消耗result最多的result buffer来输出next tuple
    int max_unconsumed_worker_index = 0;
    int max_unconsumed_tuple_count = result_buffers_[0].size() - consumed_result_counts_[0];

    for(int i = 1; i < worker_thread_num_; ++i) {
        int curr_result_cnt = result_buffers_[i].size() - consumed_result_counts_[i];
        if(curr_result_cnt > max_unconsumed_tuple_count) {
            max_unconsumed_tuple_count = curr_result_cnt;
            max_unconsumed_worker_index = i;
        }
    }
}

std::unique_ptr<Record> GatherExecutor::Next() {
    assert(!is_end());

    consumed_result_counts_[next_worker_index_] ++;

    return std::move(result_buffers_[next_worker_index_][consumed_result_counts_[next_worker_index_] - 1]);
}

std::chrono::time_point<std::chrono::system_clock> GatherExecutor::get_latest_ckpt_time() {
    return ck_infos_[ck_infos_.size() - 1].ck_timestamp_;
}

double GatherExecutor::get_curr_suspend_cost() {
    return 0;
}

std::pair<bool, double> GatherExecutor::judge_state_reward(GatherCheckpointInfo* curr_ck_info) {
    return {true, 0};
}

int64_t GatherExecutor::getRCop(std::chrono::time_point<std::chrono::system_clock> curr_time) {
    return 0;
}

void GatherExecutor::write_state() {

}

void GatherExecutor::write_state_if_allow(int type) {

}

void GatherExecutor::load_state_info(GatherOperatorState* state) {

}