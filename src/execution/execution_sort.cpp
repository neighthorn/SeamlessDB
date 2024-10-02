#pragma once
#include "execution_sort.h"
#include "state/state_item/op_state.h"
#include "state/op_state_manager.h"
#include "debug_log.h"
#include "executor_block_join.h"
#include "executor_hash_join.h"
#include "executor_projection.h"
#include "executor_index_scan.h"

std::pair<bool, double> SortExecutor::judge_state_reward(SortCheckpointInfo* curr_ck_info) {
    SortCheckpointInfo* latest_ck_info = nullptr;
    if(ck_infos_.empty()) {
        assert(0);
    }

    latest_ck_info = &ck_infos_[ck_infos_.size() - 1];

    double src_op = (double)sort_state_size_min + (double)(num_records_ - checkpointed_tuple_num_) * tuple_len_;
    double rc_op = getRCop(curr_ck_info->ck_timestamp_);

    if(is_sorted_ == true && is_sort_index_checkpointed_ == false) {
        src_op += (double)num_records_ * sizeof(int);
    }

    if(rc_op == 0) {
        return {false, -1};
    }

    double new_src_op = src_op / src_scale_factor_;
    double rew_op = rc_op / new_src_op - state_theta_;
    // RwServerDebug::getInstance()->DEBUG_PRINT("[SortExecutor][op_id: " + std::to_string(operator_id_) + "]: [delta tuple num]: " + std::to_string(num_records_ - checkpointed_tuple_num_) + " [Rew_op]: " + std::to_string(rew_op) + " [Src_op]: " + std::to_string(new_src_op) + " [Rc_op]: " + std::to_string(rc_op) + " [State Theta]: " + std::to_string(state_theta_));

    if(rew_op > 0) {
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
        assert(0);
    }

    if(auto x = dynamic_cast<HashJoinExecutor *>(prev_.get())) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(curr_time - latest_ck_info->ck_timestamp_).count() + x->getRCop(latest_ck_info->ck_timestamp_);
    } else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(prev_.get())) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(curr_time - latest_ck_info->ck_timestamp_).count() + x->getRCop(latest_ck_info->ck_timestamp_);
    } else {
        return std::chrono::duration_cast<std::chrono::milliseconds>(curr_time - latest_ck_info->ck_timestamp_).count();
    }
    
}

void SortExecutor::write_state_if_allow(int type) {
    if(type == 1) return;
    SortCheckpointInfo curr_ck_info = {.ck_timestamp_ = std::chrono::high_resolution_clock::now()};
    if(state_open_) {
        auto [able_to_write, src_op] = judge_state_reward(&curr_ck_info);
        if(able_to_write) {
            auto [status, actual_size] = context_->op_state_mgr_->add_operator_state_to_buffer(this, src_op);
            // RwServerDebug::getInstance()->DEBUG_PRINT("[SortExecutor][op_id: " + std::to_string(operator_id_) + "]: [state size]: " + std::to_string(actual_size));
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

    std::cout << "SortExecutor: load_state_info: be_call_times_=" << be_call_times_ << ", left_child_call_times: " << left_child_call_times_ << ", num_records: " << num_records_ << std::endl;
    
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