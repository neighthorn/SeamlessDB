#include "executor_gather.h"
#include "debug_log.h"

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
                        std::lock_guard<std::mutex> lock(result_queues_mutex_[i]);
                        result_queues_[i].push(std::move(record));
                        queue_sizes_[i] = result_queues_[i].size();
                        // std::cout << "Worker " << i << " produce a record, result_queues[" << i << "].size()=" << result_queues_[i].size() << std::endl;
                        // RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: worker[" + std::to_string(i) + "] produce a record, result_queues[" + std::to_string(i) + "].size()=" + std::to_string(queue_sizes_[i]));
                    }
                    next_tuple_cv_.notify_one();
                }
                workers_[i]->nextTuple();
            }
            worker_is_end_[i] = true;
            std::cout << "Worker " << i << " finished!" << std::endl;
        }));
    }

    nextTuple();
}

void GatherExecutor::nextTuple() {
    // RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: nextTuple() called");
    if(is_end()) {
        return;
    }
    std::unique_lock<std::mutex> lock(next_tuple_mutex_);
    next_tuple_cv_.wait(lock, [this](){
        for(size_t i = 0; i < worker_thread_num_; ++i) {
            if(queue_sizes_[i] > 0) {
                return true;
            }
        }
        return false;
    });

    // RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: nextTuple() wake up");
    // 找一个待消耗result最多的result buffer来输出next tuple
    int max_unconsumed_worker_index = -1;
    int max_unconsumed_tuple_count = -1;
    
    for(int i = 0; i < worker_thread_num_; ++i) {
        int curr_unconsumed_result_cnt = queue_sizes_[i];
        if(curr_unconsumed_result_cnt > 0 && curr_unconsumed_result_cnt > max_unconsumed_tuple_count) {
            max_unconsumed_worker_index = i;
            max_unconsumed_tuple_count = curr_unconsumed_result_cnt;
        }
    }

    if(max_unconsumed_worker_index == -1) {
        std::cout << "GatherExecutor is end!" << std::endl;
        // assert(0);
        return;
    }

    next_worker_index_ = max_unconsumed_worker_index;
    // RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: next_worker_index_=" + std::to_string(next_worker_index_));
}

std::unique_ptr<Record> GatherExecutor::Next() {
    // RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: Next() called");
    std::lock_guard<std::mutex> lock(result_queues_mutex_[next_worker_index_]);
    std::unique_ptr<Record> record = std::move(result_queues_[next_worker_index_].front());
    result_queues_[next_worker_index_].pop();
    queue_sizes_[next_worker_index_] = result_queues_[next_worker_index_].size();
    return std::move(record);
}

bool GatherExecutor::is_end() const {
    // RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: is_end() called");
    // 当所有的worker线程都是is_end()并且所有的result buffer都已经消费完时，Gather算子才是end
    for(int i = 0; i < worker_thread_num_; ++i) {
        if(!worker_is_end_[i] || queue_sizes_[i] > 0) {
            // std::cout << "worker[" << i << "] has not been consumed, workers[i].end=" << workers_[i]->is_end() << ", worker[i].result_count: " << result_queues_.size() << "\n";
            // RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: worker[" + std::to_string(i) + "] has not been consumed, workers[i].end=" + std::to_string(workers_[i]->is_end()) + ", worker[i].result_count: " + std::to_string(queue_sizes_[i]));
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