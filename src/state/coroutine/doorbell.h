#pragma once

#include "coroutine_scheduler.h"

class DoorbellBatch {
public:
    DoorbellBatch() {
        doorbell_num_ = 0;
    }

    DoorbellBatch(int doorbell_num) : doorbell_num_(doorbell_num) {
        assert(doorbell_num > 0);
        sr_ = new ibv_send_wr[doorbell_num_];
        sge_ = new ibv_sge[doorbell_num_];

        for(int i = 0; i < doorbell_num_ - 1; ++i) {
            sr_[i].num_sge = 1;
            sr_[i].sg_list = &sge_[i];
            sr_[i].send_flags = 0;
            sr_[i].next = &sr_[i + 1];
        }

        sr_[doorbell_num_ - 1].num_sge = 1;
        sr_[doorbell_num_ - 1].sg_list = &sge_[doorbell_num_ - 1];
        sr_[doorbell_num_ - 1].send_flags = IBV_SEND_SIGNALED;
        sr_[doorbell_num_ - 1].next = NULL;
    }

    ~DoorbellBatch() {
        delete[] sr_;
        delete[] sge_;
    }

    int doorbell_num_;
    struct ibv_send_wr* sr_;
    struct ibv_sge* sge_;
    struct ibv_send_wr* bad_sr_;
};

class LockWriteBatch: public DoorbellBatch {
public:
    LockWriteBatch(int doorbell_num) : DoorbellBatch(doorbell_num) {
        req_idx_ = 0;
    }

    void set_next_lock_write_req(char* local_addr, uint64_t remote_off, size_t size);

    bool send_reqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, MemoryAttr& remote_mr);

    int req_idx_;
};

class LogWriteBatch: public DoorbellBatch {
public:
    LogWriteBatch() : DoorbellBatch(4) {

    }

    void set_log_write_req(char* local_addr, uint64_t remote_off, size_t size);

    void set_head_write_req(char* local_addr, uint64_t remote_off);

    void set_tail_write_req(char* local_addr, uint64_t remote_off);

    void set_state_tail_write_req(char* local_addr, uint64_t remote_off);

    bool send_reqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, MemoryAttr& remote_mr);
};

class LogWriteTwoRangeBatch: public DoorbellBatch {
public:
    LogWriteTwoRangeBatch(): DoorbellBatch(5) {

    }

    void set_log_write_req1(char* local_addr, uint64_t remote_off, size_t size);
    
    void set_log_write_req2(char* local_addr, uint64_t remote_off, size_t size);

    void set_head_write_req(char* local_addr, uint64_t remote_off);

    void set_tail_write_req(char* local_addr, uint64_t remote_off);

    void set_state_tail_write_req(char* local_addr, uint64_t remote_off);

    bool send_reqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, MemoryAttr& remote_mr);
};