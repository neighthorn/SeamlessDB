#pragma once

#include "executor_projection.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_block_join.h"
#include "execution/executor_hash_join.h"
#include "state/state_item/op_state.h"
#include "state/op_state_manager.h"
#include "state/state_item/op_state.h"
#include "execution_sort.h"

void ProjectionExecutor::load_state_info(ProjectionOperatorState *proj_op_state) {
    be_call_times_ = proj_op_state->left_child_call_times_;

    if(is_root_) {
        curr_result_num_ = be_call_times_;
        checkpointed_result_num_ = be_call_times_;
    }

    if(proj_op_state->is_left_child_join_ == false) {
        if(auto x = dynamic_cast<IndexScanExecutor *>(prev_.get())) {
            x->load_state_info(proj_op_state->left_index_scan_state_);
        }
    }
}

std::pair<bool, double> ProjectionExecutor::judge_state_reward(ProjectionCheckpointInfo *current_ck_info) {
    ProjectionCheckpointInfo* latest_ck_info = nullptr;
    if(ck_infos_.empty()) {
        assert(0);
    }

    latest_ck_info = &ck_infos_[ck_infos_.size() - 1];

    double src_op = (double)projection_state_size_min + (double)(curr_result_num_ - checkpointed_result_num_) * len_;
    double rc_op = getRCop(current_ck_info->ck_timestamp_);

    if(rc_op == 0) {
        return {false, -1};
    }

    double new_src_op = src_op / src_scale_factor_;
    double rew_op = rc_op / new_src_op - state_theta_;
    RwServerDebug::getInstance()->DEBUG_PRINT("[ProjectionExecutor][op_id: " + std::to_string(operator_id_) + "]: [delta result num]: " + std::to_string(curr_result_num_ - checkpointed_result_num_) + " [Rew_op]: " + std::to_string(rew_op) + " [Src_op]: " + std::to_string(new_src_op) + " [Rc_op]: " + std::to_string(rc_op) + " [State Theta]: " + std::to_string(state_theta_));

    if(rew_op > 0) {
        return {true, src_op};
    }

    return {false, -1};
}

int64_t ProjectionExecutor::getRCop(std::chrono::time_point<std::chrono::system_clock> curr_time) {
    // TODO: 如果是root的projection，按照当前这个逻辑就可以，如果是非root的projection，直接返回儿子节点的RCop
    ProjectionCheckpointInfo* latest_ck_info = nullptr;
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
    }
    else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(prev_.get())) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(curr_time - latest_ck_info->ck_timestamp_).count() + x->getRCop(latest_ck_info->ck_timestamp_);
    }
    else if(auto x = dynamic_cast<SortExecutor *>(prev_.get())) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(curr_time - latest_ck_info->ck_timestamp_).count() + x->getRCop(latest_ck_info->ck_timestamp_);
    }
    else {
        return std::chrono::duration_cast<std::chrono::milliseconds>(curr_time - latest_ck_info->ck_timestamp_).count();
    }
}

void ProjectionExecutor::write_state_if_allow(int type) {
    assert(is_root_);
    ProjectionCheckpointInfo curr_ck_info = {.ck_timestamp_ = std::chrono::high_resolution_clock::now()};
    if(state_open_) {
        auto [able_to_write, src_op] = judge_state_reward(&curr_ck_info);
        if(able_to_write) {
            auto [status, actual_size] = context_->op_state_mgr_->add_operator_state_to_buffer(this, src_op);
            RwServerDebug::getInstance()->DEBUG_PRINT("[ProjectionExecutor: " + std::to_string(operator_id_) + "]: [Write State]: [Src_op]: " + std::to_string(src_op));
            if(status) {
                ck_infos_.push_back(curr_ck_info);
                checkpointed_result_num_ = curr_result_num_;
            }
        }
    }
}