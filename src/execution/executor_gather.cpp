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
                auto record = workers_[i]->Next();
                if(record == nullptr) {
                    assert(0);
                }
                else {
                    {
                        std::lock_guard<std::mutex> lock(result_mutexes_[i]);
                        result_queues_[i].push(std::move(record));
                    }
                    result_total_counts_[i].fetch_add(1);
                }
                workers_[i]->nextTuple();
            }
            std::cout << "Worker " << i << " finished!" << std::endl;
        }));
    }

    nextTuple();
}

void GatherExecutor::nextTuple() {
    assert(!is_end());
    // 找一个待消耗result最多的result buffer来输出next tuple
    int max_unconsumed_worker_index = -1;
    int max_unconsumed_tuple_count = -1;
    int wait_count = 0;
    while(max_unconsumed_worker_index == -1) {
        // 找到第一个不为空的result buffer
        int i = 0;
        for(; i < worker_thread_num_; ++i) {
            int curr_result_cnt = result_total_counts_[i].load();
            int curr_consumed_cnt = consumed_result_counts_[i].load();
            if(curr_result_cnt > 0 && curr_result_cnt > curr_consumed_cnt) {
                max_unconsumed_worker_index = i;
                max_unconsumed_tuple_count = curr_result_cnt - curr_consumed_cnt;
                break;
            }
        }

        for(++i; i < worker_thread_num_; ++i) {
            int curr_unconsumed_result_cnt = result_total_counts_[i].load() - consumed_result_counts_[i].load();
            if(curr_unconsumed_result_cnt > max_unconsumed_tuple_count) {
                max_unconsumed_tuple_count = curr_unconsumed_result_cnt;
                max_unconsumed_worker_index = i;
            }
        }

        if(max_unconsumed_tuple_count == -1) {
            // 所有的result buffer都已经消费完了，等待一段时间
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            wait_count ++;
            if(wait_count > 100) {
                std::cout << "GatherExecutor is waiting for result buffer!" << std::endl;
                assert(0);
            }
        }
    }

    next_worker_index_ = max_unconsumed_worker_index;
}

std::unique_ptr<Record> GatherExecutor::Next() {
    int consume_index = consumed_result_counts_[next_worker_index_].fetch_add(1);

    std::cout << "result buffer size: " << result_buffers_[next_worker_index_].size() << ", consumed_index: " << consume_index << std::endl;
    assert(result_buffers_[next_worker_index_][consume_index] != nullptr);

    return std::move(result_buffers_[next_worker_index_][consume_index]);
}

bool GatherExecutor::is_end() const {
    // 当所有的worker线程都是is_end()并且所有的result buffer都已经消费完时，Gather算子才是end
    for(int i = 0; i < worker_thread_num_; ++i) {
        if(!workers_[i]->is_end() || consumed_result_counts_[i].load() < result_total_counts_[i].load()) {
            return false;
        }
    }
    std::cout << "GatherExecutor is end!" << std::endl;
    return true;
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