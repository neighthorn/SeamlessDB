#pragma once

#include "executor_projection.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_block_join.h"
#include "execution/executor_hash_join.h"
#include "state/state_item/op_state.h"

void ProjectionExecutor::load_state_info(ProjectionOperatorState *proj_op_state) {
    be_call_times_ = proj_op_state->left_child_call_times_;

    if(proj_op_state->is_left_child_join_ == false) {
        if(auto x = dynamic_cast<IndexScanExecutor *>(prev_.get())) {
            x->load_state_info(proj_op_state->left_index_scan_state_);
        }
    }
}