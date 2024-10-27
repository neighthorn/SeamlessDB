#pragma once
#include "execution_sort.h"
#include "state/state_item/op_state.h"
#include "state/op_state_manager.h"
#include "debug_log.h"
#include "executor_block_join.h"
#include "executor_hash_join.h"
#include "executor_projection.h"
#include "executor_index_scan.h"
#include "comp_ckpt_mgr.h"

std::pair<bool, double> SortExecutor::judge_state_reward(SortCheckpointInfo* curr_ck_info) {
    SortCheckpointInfo* latest_ck_info = nullptr;
    if(ck_infos_.empty()) {
        std::cerr << "[Error]: SortOp Initial ckpt not created! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
        assert(0);
    }

    latest_ck_info = &ck_infos_[ck_infos_.size() - 1];

    double src_op = (double)sort_state_size_min + (double)(num_records_ - checkpointed_tuple_num_) * tuple_len_;
    double rc_op = getRCop(curr_ck_info->ck_timestamp_);

    if(is_sorted_ == true && is_sort_index_checkpointed_ == false) {
        src_op += (double)num_records_ * sizeof(int);
    }

    if(rc_op == 0) {
        std::cerr << "[Error]: SortOp RC op is 0! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
        return {false, -1};
    }

    if(finished_begin_tuple_ == true) {
        src_op += (double)tuple_len_ * (state_change_time_ - latest_ck_info->state_change_time_);
    }

    // double new_src_op = src_op / src_scale_factor_;
    double new_src_op = src_op / MB_ + src_op / RB_ + C_;
    double rew_op = rc_op / new_src_op - state_theta_;
    // RwServerDebug::getInstance()->DEBUG_PRINT("[SortExecutor][op_id: " + std::to_string(operator_id_) + "]: [delta tuple num]: " + std::to_string(num_records_ - checkpointed_tuple_num_) \
    //  + " [State size]: " + std::to_string(src_op) + " [Rew_op]: " + std::to_string(rew_op) + " [Src_op]: " + std::to_string(new_src_op) + " [Rc_op]: " + std::to_string(rc_op) + " [State Theta]: " + std::to_string(state_theta_));

    if(rew_op > 0) {
        if(auto x = dynamic_cast<HashJoinExecutor *>(prev_.get())) {
            curr_ck_info->left_rc_op_ = x->getRCop(curr_ck_info->ck_timestamp_);
        } else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(prev_.get())) {
            curr_ck_info->left_rc_op_ = x->getRCop(curr_ck_info->ck_timestamp_);
        } else if(auto x = dynamic_cast<ProjectionExecutor *>(prev_.get())) {
            curr_ck_info->left_rc_op_ = x->getRCop(curr_ck_info->ck_timestamp_);
        }
        curr_ck_info->state_change_time_ = state_change_time_;
        if(state_change_time_ - latest_ck_info->state_change_time_ < 10) return {false, -1};
        RwServerDebug::getInstance()->DEBUG_PRINT("[SortExecutor][op_id: " + std::to_string(operator_id_) + "]: [delta tuple num]: " + std::to_string(num_records_ - checkpointed_tuple_num_) \
        + " [State size]: " + std::to_string(src_op) + " [Rew_op]: " + std::to_string(rew_op) + " [Src_op]: " + std::to_string(new_src_op) + " [Rc_op]: " + std::to_string(rc_op) + " [State Theta]: " + std::to_string(state_theta_));
        return {true, src_op};
    }

    return {false, -1};
}

int64_t SortExecutor::getRCop(std::chrono::time_point<std::chrono::system_clock> curr_time) {
    SortCheckpointInfo* latest_ck_info = nullptr;
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

    if(finished_begin_tuple_ == true) {
        return std::chrono::duration_cast<std::chrono::microseconds>(curr_time - latest_ck_info->ck_timestamp_).count();
    }
    // RwServerDebug::getInstance()->DEBUG_PRINT("[SortExecutor][op_id: " + std::to_string(operator_id_) + "]: [Curr time]: " + std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(curr_time.time_since_epoch()).count()) \
    // + " [Latest ck time]: " + std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(latest_ck_info->ck_timestamp_.time_since_epoch()).count()) \
    // + " [Left rc op]: " + std::to_string(latest_ck_info->left_rc_op_));

    if(dynamic_cast<HashJoinExecutor *>(prev_.get())) {
        return std::chrono::duration_cast<std::chrono::microseconds>(curr_time - latest_ck_info->ck_timestamp_).count() + latest_ck_info->left_rc_op_;
    } else if(dynamic_cast<BlockNestedLoopJoinExecutor *>(prev_.get())) {
        return std::chrono::duration_cast<std::chrono::microseconds>(curr_time - latest_ck_info->ck_timestamp_).count() + latest_ck_info->left_rc_op_;
    } else if(dynamic_cast<ProjectionExecutor *>(prev_.get())) {
        return std::chrono::duration_cast<std::chrono::microseconds>(curr_time - latest_ck_info->ck_timestamp_).count() + latest_ck_info->left_rc_op_;
    }
    else {
        return std::chrono::duration_cast<std::chrono::microseconds>(curr_time - latest_ck_info->ck_timestamp_).count();
    }
    
}

std::chrono::time_point<std::chrono::system_clock> SortExecutor::get_latest_ckpt_time() {
    return ck_infos_[ck_infos_.size() - 1].ck_timestamp_;
}

double SortExecutor::get_curr_suspend_cost() {
    double src_op = (double)sort_state_size_min + (double)(num_records_ - checkpointed_tuple_num_) * tuple_len_;
    if(is_sorted_ == true && is_sort_index_checkpointed_ == false) {
        src_op += (double)num_records_ * sizeof(int);
    }
    return src_op;
}

void SortExecutor::write_state() {
    SortCheckpointInfo curr_ck_info = {.ck_timestamp_ = std::chrono::high_resolution_clock::now()};
    double src_op = (double)sort_state_size_min + (double)(num_records_ - checkpointed_tuple_num_) * tuple_len_;
    if(is_sorted_ == true && is_sort_index_checkpointed_ == false) {
        src_op += (double)num_records_ * sizeof(int);
    }
    context_->op_state_mgr_->add_operator_state_to_buffer(this, src_op);
    ck_infos_.push_back(curr_ck_info);
    checkpointed_tuple_num_ = num_records_;
}

void SortExecutor::write_state_if_allow(int type) {
    if(cost_model_ >= 1) {
        CompCkptManager::get_instance()->solve_mip(context_->op_state_mgr_);
        return;
    }
    // if(type == 1) return;
    SortCheckpointInfo curr_ck_info = {.ck_timestamp_ = std::chrono::high_resolution_clock::now()};
    if(state_open_) {
        auto [able_to_write, src_op] = judge_state_reward(&curr_ck_info);
        if(able_to_write) {
            auto [status, actual_size] = context_->op_state_mgr_->add_operator_state_to_buffer(this, src_op);
            // RwServerDebug::getInstance()->DEBUG_PRINT("[SortExecutor][op_id: " + std::to_string(operator_id_) + "]: [Write State]: [state size]: " + std::to_string(actual_size));
            if(status) {
                ck_infos_.push_back(curr_ck_info);
                checkpointed_tuple_num_ = num_records_;
            }
        }
    }
}

void SortExecutor::load_state_info(SortOperatorState *sort_op_state) {
    assert(sort_op_state != nullptr);
    is_in_recovery_ = true;

    // num_records_ = sort_op_state->num_records_;
    // is_sorted_ = sort_op_state->is_sorted_;
    // is_sort_index_checkpointed_ = sort_op_state->is_sort_index_checkpointed_;
    be_call_times_ = sort_op_state->be_call_times_;
    left_child_call_times_ = sort_op_state->left_child_call_times_;
    finished_begin_tuple_ = sort_op_state->finish_begin_tuple_;

    // std::cout << "SortExecutor: load_state_info: be_call_times_=" << be_call_times_ << ", left_child_call_times: " << left_child_call_times_ << ", num_records: " << num_records_ << std::endl;
    
    // 当unsorted_records_没有构建完的时候，需要先等待左子树恢复到一致性状态再去进行unsorted_records_和sorted_index的恢复
    if(sort_op_state->left_child_is_join_ == false) {
        if(auto x = dynamic_cast<ProjectionExecutor *>(prev_.get())) {
            x->load_state_info(dynamic_cast<ProjectionOperatorState *>(sort_op_state->left_child_state_));
            
        }
        else if(auto x = dynamic_cast<IndexScanExecutor *>(prev_.get())) {
            x->load_state_info(dynamic_cast<IndexScanOperatorState *>(sort_op_state->left_child_state_));
        }
    }
}