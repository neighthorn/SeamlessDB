#include "doorbell.h"

void LockWriteBatch::set_next_lock_write_req(char* local_addr, uint64_t remote_off, size_t size) {
    sr_[req_idx_].opcode = IBV_WR_RDMA_WRITE;
    sr_[req_idx_].wr.rdma.remote_addr = remote_off;
    sge_[req_idx_].addr = (uint64_t)local_addr;
    sge_[req_idx_].length = size;
    if(size < 64) {
        sr_[req_idx_].send_flags |= IBV_SEND_INLINE;
    }
    req_idx_ ++;
}

bool LockWriteBatch::send_reqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, MemoryAttr& remote_mr) {
    for(int i = 0; i < doorbell_num_; ++i) {
        sr_[i].wr.rdma.remote_addr += remote_mr.buf;
        sr_[i].wr.rdma.rkey = remote_mr.key;
        sge_[i].lkey = qp->local_mr_.key;
    }

    assert(req_idx_ == doorbell_num_);

    if(!coro_sched->RDMABatchSync(coro_id, qp, sr_, &bad_sr_, doorbell_num_ - 1)) {
        RDMA_LOG(ERROR) << "failed to send lock_write batch requests";
        return false;
    }

    return true;
}

void LogWriteBatch::set_log_write_req(char* local_addr, uint64_t remote_off, size_t size) {
    sr_[0].opcode = IBV_WR_RDMA_WRITE;
    sr_[0].wr.rdma.remote_addr = remote_off;
    sge_[0].addr = (uint64_t)local_addr;
    sge_[0].length = size;
    if(size < 64) {
        sr_[0].send_flags |= IBV_SEND_INLINE;
    }
}

void LogWriteBatch::set_head_write_req(char* local_addr, uint64_t remote_off) {
    sr_[1].opcode = IBV_WR_RDMA_WRITE;
    sr_[1].wr.rdma.remote_addr = remote_off;
    sge_[1].addr = (uint64_t)local_addr;
    sge_[1].length = sizeof(int64_t);
}

void LogWriteBatch::set_tail_write_req(char* local_addr, uint64_t remote_off) {
    sr_[2].opcode = IBV_WR_RDMA_WRITE;
    sr_[2].wr.rdma.remote_addr = remote_off;
    sge_[2].addr = (uint64_t)local_addr;
    sge_[2].length = sizeof(int64_t);
}

void LogWriteBatch::set_state_tail_write_req(char* local_addr, uint64_t remote_off) {
    sr_[3].opcode = IBV_WR_RDMA_WRITE;
    sr_[3].wr.rdma.remote_addr = remote_off;
    sge_[3].addr = (uint64_t)local_addr;
    sge_[3].length = sizeof(int64_t);
}

bool LogWriteBatch::send_reqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, MemoryAttr& remote_mr) {
    for(int i = 0; i < doorbell_num_; ++i) {
        sr_[i].wr.rdma.remote_addr += remote_mr.buf;
        sr_[i].wr.rdma.rkey = remote_mr.key;
        sge_[i].lkey = qp->local_mr_.key;
    }

    if(!coro_sched->RDMABatchSync(coro_id, qp, sr_, &bad_sr_, doorbell_num_ - 1)) {
        RDMA_LOG(ERROR) << "failed to send log_write batch requests";
        return false;
    }

    return true;
}

void LogWriteTwoRangeBatch::set_log_write_req1(char* local_addr, uint64_t remote_off, size_t size) {
    sr_[0].opcode = IBV_WR_RDMA_WRITE;
    sr_[0].wr.rdma.remote_addr = remote_off;
    sge_[0].addr = (uint64_t)local_addr;
    sge_[0].length = size;
    if(size < 64) {
        sr_[0].send_flags |= IBV_SEND_INLINE;
    }
}

void LogWriteTwoRangeBatch::set_log_write_req2(char* local_addr, uint64_t remote_off, size_t size) {
    sr_[1].opcode = IBV_WR_RDMA_WRITE;
    sr_[1].wr.rdma.remote_addr = remote_off;
    sge_[1].addr = (uint64_t)local_addr;
    sge_[1].length = size;
    if(size < 64) {
        sr_[1].send_flags |= IBV_SEND_INLINE;
    }
}

void LogWriteTwoRangeBatch::set_head_write_req(char* local_addr, uint64_t remote_off) {
    sr_[2].opcode = IBV_WR_RDMA_WRITE;
    sr_[2].wr.rdma.remote_addr = remote_off;
    sge_[2].addr = (uint64_t)local_addr;
    sge_[2].length = sizeof(int64_t);
}

void LogWriteTwoRangeBatch::set_tail_write_req(char* local_addr, uint64_t remote_off) {
    sr_[3].opcode = IBV_WR_RDMA_WRITE;
    sr_[3].wr.rdma.remote_addr = remote_off;
    sge_[3].addr = (uint64_t)local_addr;
    sge_[3].length = sizeof(int64_t);
}

void LogWriteTwoRangeBatch::set_state_tail_write_req(char* local_addr, uint64_t remote_off) {
    sr_[4].opcode = IBV_WR_RDMA_WRITE;
    sr_[4].wr.rdma.remote_addr = remote_off;
    sge_[4].addr = (uint64_t)local_addr;
    sge_[4].length = sizeof(int64_t);
}

bool LogWriteTwoRangeBatch::send_reqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, MemoryAttr& remote_mr) {
    for(int i = 0; i < doorbell_num_; ++i) {
        sr_[i].wr.rdma.remote_addr += remote_mr.buf;
        sr_[i].wr.rdma.rkey = remote_mr.key;
        sge_[i].lkey = qp->local_mr_.key;
    }

    if(!coro_sched->RDMABatchSync(coro_id, qp, sr_, &bad_sr_, doorbell_num_ - 1)) {
        RDMA_LOG(ERROR) << "failed to send log_write batch requests";
        return false;
    }

    return true;
}