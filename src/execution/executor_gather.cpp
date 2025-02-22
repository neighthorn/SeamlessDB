#include "executor_gather.h"
#include "debug_log.h"
#include "executor_hash_join.h"
#include "executor_index_scan.h"
#include "executor_block_join.h"
#include "state/op_state_manager.h"
#include "state/state_item/op_state.h"

void GatherExecutor::beginTuple() {
    if(is_in_recovery_ && finished_begin_tuple_) return;

    for(int i = 0; i < worker_thread_num_; ++i) {
        workers_[i]->beginTuple();
    }

    finished_begin_tuple_ = true;

    // 开启worker_thread_num_个线程，每个线程负责一个subplan的执行
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
                        result_queues_[i].emplace_back(std::move(record));
                        queue_sizes_[i].fetch_add(1);
                        // if(workers_[i]->is_end()) {
                        //     worker_is_end_[i] = true;
                        // }
                        // std::cout << "Worker " << i << " produce a record, result_queues[" << i << "].size()=" << result_queues_[i].size() << std::endl;
                        // RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: worker[" + std::to_string(i) + "] produce a record, result_queues[" + std::to_string(i) + "].size()=" + std::to_string(queue_sizes_[i]));
                    }
                    next_tuple_cv_.notify_one();
                }
                workers_[i]->nextTuple();
            }
            worker_is_end_[i] = true;
            next_tuple_cv_.notify_one();
            std::cout << "Worker " << i << " finished!" << std::endl;
        }));
    }

    nextTuple();
}

void GatherExecutor::launch_workers() {
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
                        result_queues_[i].emplace_back(std::move(record));
                        queue_sizes_[i].fetch_add(1);
                        // if(workers_[i]->is_end()) {
                        //     worker_is_end_[i] = true;
                        // }
                        // std::cout << "Worker " << i << " produce a record, result_queues[" << i << "].size()=" << result_queues_[i].size() << std::endl;
                        // RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: worker[" + std::to_string(i) + "] produce a record, result_queues[" + std::to_string(i) + "].size()=" + std::to_string(queue_sizes_[i]));
                    }
                    next_tuple_cv_.notify_one();
                }
                workers_[i]->nextTuple();
            }
            worker_is_end_[i] = true;
            next_tuple_cv_.notify_one();
            std::cout << "Worker " << i << " finished!" << std::endl;
        }));
    }

    nextTuple();
}

// 保证结果输出顺序的确定性
void GatherExecutor::nextTuple() {
    // if(consumed_sizes_[0] >= 15714269 && consumed_sizes_[1] >= 15714269)
    //     RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: nextTuple() called");
    
    if(is_end()) {
        return;
    }

    next_worker_index_ = (next_worker_index_ + 1) % worker_thread_num_;
    // 判断当前worker是否已经结束，如果结束，则找到下一个未结束的worker
    while(worker_is_end_[next_worker_index_] == true && queue_sizes_[next_worker_index_] == consumed_sizes_[next_worker_index_]) {
        next_worker_index_ = (next_worker_index_ + 1) % worker_thread_num_;
    }

    std::unique_lock<std::mutex> lock(result_queues_mutex_[next_worker_index_]);
    next_tuple_cv_.wait(lock, [this](){
        if(is_end()) {
            return true;
        }
        if(queue_sizes_[next_worker_index_] > consumed_sizes_[next_worker_index_]) {
            return true;
        }
        return false;
    });

    // if(consumed_sizes_[0] >= 15714269 && consumed_sizes_[1] >= 15714269)
    //     RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: nextTuple() wake up");
    // // 找一个待消耗result最多的result buffer来输出next tuple
    // int max_unconsumed_worker_index = -1;
    // int max_unconsumed_tuple_count = -1;
    
    // for(int i = 0; i < worker_thread_num_; ++i) {
    //     int curr_unconsumed_result_cnt = queue_sizes_[i];
    //     if(curr_unconsumed_result_cnt > 0 && curr_unconsumed_result_cnt > max_unconsumed_tuple_count) {
    //         max_unconsumed_worker_index = i;
    //         max_unconsumed_tuple_count = curr_unconsumed_result_cnt;
    //     }
    // }

    // if(max_unconsumed_worker_index == -1) {
    //     std::cout << "GatherExecutor is end!" << std::endl;
    //     // assert(0);
    //     return;
    // }

    // next_worker_index_ = max_unconsumed_worker_index;
    // RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: next_worker_index_=" + std::to_string(next_worker_index_));
}

std::unique_ptr<Record> GatherExecutor::Next() {
    std::unique_ptr<Record> record;
    {
        std::lock_guard<std::mutex> lock(result_queues_mutex_[next_worker_index_]);
        record = std::move(result_queues_[next_worker_index_][consumed_sizes_[next_worker_index_]]);
        consumed_sizes_[next_worker_index_]++;
        be_call_times_++;
    }
    if(state_open_) write_state_if_allow();
    return record;
}

bool GatherExecutor::is_end() const {
    // if(consumed_sizes_[0] >= 15714269 && consumed_sizes_[1] >= 15714269)
    //     RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: is_end() called");
    // 当所有的worker线程都是is_end()并且所有的result buffer都已经消费完时，Gather算子才是end
    for(int i = 0; i < worker_thread_num_; ++i) {
        if(!worker_is_end_[i] || queue_sizes_[i] > consumed_sizes_[i]) {
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
    double src_op = 0;
    for(int i = 0; i < worker_thread_num_; ++i) {
        src_op = src_op + (double)(workers_[i]->tupleLen()) * (double)(queue_sizes_[i]);
    }
    return 0;
}

std::pair<bool, double> GatherExecutor::judge_state_reward(GatherCheckpointInfo* curr_ck_info) {
    GatherCheckpointInfo* latest_ck_info = nullptr;
    if(ck_infos_.empty()) {
        std::cerr << "[Error]: GatherOp ck_infos_ is empty!" << std::endl;
        assert(0);
    }

    latest_ck_info = &ck_infos_[ck_infos_.size() - 1];

    double src_op = (double)gather_state_size_min;
    for(int i = 0; i < worker_thread_num_; ++i) {
        result_buffer_curr_tuple_counts_[i] = queue_sizes_[i];
    }
    for(int i = 0; i < worker_thread_num_; ++i) {
        src_op = src_op + (double)len_ * (double)(result_buffer_curr_tuple_counts_[i] - consumed_sizes_[i]);
    }
    double rc_op = getRCop(curr_ck_info->ck_timestamp_);

    if(rc_op == 0) {
        std::cerr << "[Error]: RCop is 0! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
        return {false, -1};
    }

    double new_src_op = src_op / MB_ + src_op / RB_ + C_;
    double rew_op = rc_op / new_src_op - state_theta_;

    // RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: src_op=" + std::to_string(src_op) + ", rc_op=" + std::to_string(rc_op) + ", new_src_op=" + std::to_string(new_src_op) + ", rew_op=" + std::to_string(rew_op));

    if(rew_op > 0) {
        if(dynamic_cast<IndexScanExecutor *>(workers_[0].get())) {
            curr_ck_info->max_child_rc_op_ = 0;
            curr_ck_info->state_change_time_ = state_change_time_;
        }
        else {
            double max_child_rc_op = 0;
            for(int i = 0; i < worker_thread_num_; ++i) {
                if(auto x = dynamic_cast<HashJoinExecutor *>(workers_[i].get())) {
                    max_child_rc_op = std::max(max_child_rc_op ,(double)x->getRCop(curr_ck_info->ck_timestamp_));
                }
                else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(workers_[i].get())) {
                    max_child_rc_op = std::max(max_child_rc_op, (double)x->getRCop(curr_ck_info->ck_timestamp_));
                }
            }
            curr_ck_info->max_child_rc_op_ = max_child_rc_op;
            curr_ck_info->state_change_time_ = state_change_time_;
        }
        // RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: [delta result num]: " + std::to_string(result_buffer_curr_tuple_counts_[0] - consumed_sizes_[0]) \
        //     + " [Rew_op]: " + std::to_string(rew_op) + " [state_size]: " + std::to_string(src_op) + " [Src_op]: " + std::to_string(new_src_op) + " [Rc_op]: " + std::to_string(rc_op) + " [State Theta]: " + std::to_string(state_theta_));
        RwServerDebug::getInstance()->DEBUG_PRINT("[GatherExecutor]: [be_call_times]: " + std::to_string(be_call_times_));
        // if(state_change_time_ - latest_ck_info->state_change_time_ < 10) return {false, -1};
        return {true, src_op};
    }
    return {false, 0};
}

int64_t GatherExecutor::getRCop(std::chrono::time_point<std::chrono::system_clock> curr_time) {
    GatherCheckpointInfo* latest_ck_info = nullptr;
    for(int i = ck_infos_.size() - 1; i >= 0; --i) {
        if(ck_infos_[i].ck_timestamp_ <= curr_time) {
            latest_ck_info = &ck_infos_[i];
            break;
        }
    }

    if(latest_ck_info == nullptr) {
        std::cerr << "[Error]: No ck points found! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
        assert(0);
    }

    int64_t rc_op = std::chrono::duration_cast<std::chrono::microseconds>(curr_time - latest_ck_info->ck_timestamp_).count();
    int64_t max_child_rc_op = 0;

    for(int i = 0; i < worker_thread_num_; ++i) {
        if(auto x = dynamic_cast<HashJoinExecutor *>(workers_[i].get())) {
            max_child_rc_op = std::max(max_child_rc_op ,x->getRCop(curr_time));
        }
        else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(workers_[i].get())) {
            max_child_rc_op = std::max(max_child_rc_op, x->getRCop(curr_time));
        }
        else if(auto x = dynamic_cast<IndexScanExecutor *>(workers_[i].get())) {
            // TODO, whether to suspend index scan dependently
        }
    }
    
    return rc_op + max_child_rc_op;
}

void GatherExecutor::write_state() {
    GatherCheckpointInfo curr_ck_info = {.ck_timestamp_ = std::chrono::high_resolution_clock::now()};
    double src_op = (double)gather_state_size_min;

    // 获取当前时刻的状态（也就是所有的queue_sizes_）
    for(int i = 0; i < worker_thread_num_; ++i) {
        result_buffer_curr_tuple_counts_[i] = queue_sizes_[i];
    }

    for(int i = 0; i < worker_thread_num_; ++i) {
        src_op = src_op + (double)(workers_[i]->tupleLen()) * (double)(result_buffer_curr_tuple_counts_[i] - consumed_sizes_[i]);
    }
    context_->op_state_mgr_->add_operator_state_to_buffer(this, src_op);
    ck_infos_.push_back(curr_ck_info);
}

void GatherExecutor::write_state_if_allow(int type) {
    if(state_open_ == 0) return;

    GatherCheckpointInfo curr_ck_info = {.ck_timestamp_ = std::chrono::high_resolution_clock::now()};
    auto [able_to_write, src_op] = judge_state_reward(&curr_ck_info);

    if(able_to_write) {
        // 首先获取所有的锁
        // TODO: this src_op is not correct
        context_->op_state_mgr_->add_operator_state_to_buffer(this, src_op);
        ck_infos_.push_back(curr_ck_info);
    }
}

void GatherExecutor::load_state_info(GatherOperatorState* state) {
    if(state_open_ == 0) return;

    std::cout << "GatherExecutor enters load_state_info()\n";

    is_in_recovery_ = true;
    be_call_times_ = state->be_call_times_;
    finished_begin_tuple_ = state->finish_begin_tuple_;
    assert(state->subplan_num_ == worker_thread_num_);

    for(int i = 0; i < worker_thread_num_; ++i) {
        if(auto x = dynamic_cast<IndexScanExecutor *>(workers_[i].get())) {
            if(state->subplan_states_[i].finish_begin_tuple_ == false) {
                x->beginTuple();
            }
            else {
                x->load_state_info(&state->subplan_states_[i]);
                x->nextTuple();
            }
            worker_is_end_[i] = x->is_end();
            std::cout << "worker_is_end_[" << i << "]=" << worker_is_end_[i] << std::endl;
        }
        else {
            std::cerr << "[Error]: GatherExecutor only support IndexScanExecutor as worker!" << std::endl;
            assert(0);
        }
    }
}