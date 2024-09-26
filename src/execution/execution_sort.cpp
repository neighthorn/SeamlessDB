#pragma once
#include "execution_sort.h"
#include "state/state_item/op_state.h"
#include "state/op_state_manager.h"
#include "debug_log.h"

std::pair<bool, double> SortExecutor::judge_state_reward(SortCheckpointInfo* curr_ck_info) {
    SortCheckpointInfo* latest_ck_info = nullptr;
    if(ck_infos_.empty()) {
        assert(0);
    }

    latest_ck_info = &ck_infos_[ck_infos_.size() - 1];

    double src_op = (double)sort_state_size_min + (double)(tuple_num - checkpointed_tuple_num) * len_;
    double rc_op = getRCop(curr_ck_info->ck_timestamp_);

    if(rc_op == 0) {
        return {false, -1};
    }

    double new_src_op = src_op / src_scale_factor_;
    double rew_op = rc_op / new_src_op - state_theta_;
    RwServerDebug::getInstance()->DEBUG_PRINT("[SortExecutor][op_id: " + std::to_string(operator_id_) + "]: [delta tuple num]: " + std::to_string(tuple_num - checkpointed_tuple_num) + " [Rew_op]: " + std::to_string(rew_op) + " [Src_op]: " + std::to_string(new_src_op) + " [Rc_op]: " + std::to_string(rc_op) + " [State Theta]: " + std::to_string(state_theta_);

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

    return {true, 0};
}