#pragma once

#include "transaction/transaction.h"
#include "transaction/concurrency/lock_manager.h"
#include "recovery/log_manager.h"
#include "common/common.h"
#include "state/coroutine/coroutine_scheduler.h"
#include "state/qp_manager.h"
#include "state/meta_manager.h"
// class TransactionManager;

class OperatorStateManager;
// used for data_send
static int const_offset = -1;

class Context {
public:
    Context (LockManager *lock_mgr, LogManager *log_mgr, 
            Transaction *txn, CoroutineScheduler* coro_sched, 
            OperatorStateManager *op_state_mgr, QPManager* qp_mgr,
            char *data_send = nullptr, int *offset = &const_offset,
            bool rdma_allocated = false, int state_slot_index = -1, PlanTag plan_tag = T_Invalid)
        : lock_mgr_(lock_mgr), log_mgr_(log_mgr), txn_(txn), rdma_allocated_(rdma_allocated), qp_mgr_(qp_mgr),
          data_send_(data_send), offset_(offset), plan_tag_(plan_tag), coro_sched_(coro_sched), op_state_mgr_(op_state_mgr),
          state_slot_index_(state_slot_index) {
            ellipsis_ = false;
          }

    inline void clear() {
      ellipsis_ = false;
      plan_tag_ = T_Invalid;
    }

    // TransactionManager *txn_mgr_;
    LockManager *lock_mgr_;
    LogManager *log_mgr_;
    Transaction *txn_;
    char *data_send_;
    int *offset_;
    bool ellipsis_;
    PlanTag plan_tag_;
    CoroutineScheduler* coro_sched_;
    RDMABufferAllocator* rdma_buffer_allocator_;
    /*
      管理算子内状态
    */
    OperatorStateManager *op_state_mgr_;
    /*
      thread local sql, join plan, join block buffer allocator
    */
    // RDMABufferAllocator *sql_buffer_allocator_;
    // RDMABufferAllocator *join_plan_buffer_allocator_;
    // RDMABufferAllocator *join_block_buffer_allocator_;
    // OffsetAllocator* sql_allocator_;
    // OffsetAllocator* plan_allocator_;
    // OffsetAllocator* cursor_allocator_;
    // offset_t sql_remote_offset_;
    // offset_t plan_remote_offset_;
    // offset_t cursor_remote_offset_;
    QPManager* qp_mgr_;
    bool rdma_allocated_ = false;
    int state_slot_index_;


};