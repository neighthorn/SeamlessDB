#pragma once

#include <list>
#include <cassert>

#include "coroutine.h"
#include "rlib/rdma_ctrl.hpp"
// #include "coroutine.h"

using namespace rdmaio;

// Scheduling coroutines. Each txn thread only has ONE scheduler
class CoroutineScheduler {
 public:
  // The coro_num includes all the coroutines
  CoroutineScheduler(t_id_t thread_id, coro_id_t coro_num) { 
    t_id_ = thread_id; 
    coro_num_ = coro_num;
    pending_counts_ = new int[coro_num_];
    for(coro_id_t c = 0; c < coro_num; ++c) {
      pending_counts_[c] = 0;
    }
    coro_array_ = new Coroutine[coro_num_];
  }
  ~CoroutineScheduler() {
    if(pending_counts_) delete[] pending_counts_;
    if(coro_array_) delete[] coro_array_;
  }

  // void AddPendingQP(coro_id_t coro_id, RCQP* qp);
  
  // bool RDMAWrite(coro_id_t coro_id, RCQP* qp, char* wt_data, uint64_t remote_offset, size_t size);

  // bool RDMARead(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size);

  // bool RDMAFAA(coro_id_t coro_id, RCQP* qp, char* local_buf, uint64_t remote_offset, uint64_t add);

  // bool RDMACAS(coro_id_t coro_id, RCQP* qp, char* local_buf, uint64_t remote_offset, uint64_t compare, uint64_t swap);
  
  bool RDMAWriteSync(coro_id_t coro_id, RCQP* qp, char* wt_data, uint64_t remote_offset, size_t size);

  bool RDMAReadSync(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size);

  bool RDMACASSync(coro_id_t coro_id, RCQP* qp, char* local_buf, uint64_t remote_offset, uint64_t compare, uint64_t swap);

  bool RDMABatchSync(coro_id_t coro_id, RCQP* qp, ibv_send_wr* send_sr, ibv_send_wr** bad_sr_addr, int doorbell_num);

  // void PollCompletion();

  // void PollRegularCOmpletion();

  // void LoopLinkCoroutine(coro_id_t coro_num);   // Link coroutines in a loop manner

  // void Yield(coro_yield_t& yield, coro_id_t coro_id);

  // void AppendCoroutine(Coroutine* coro);

  // void RunCoroutine(coro_yield_t& yield, Coroutine* coro);

  t_id_t t_id_;
  coro_id_t coro_num_;
  Coroutine* coro_array_;
  Coroutine* coro_head_;
  Coroutine* coro_tail_;
  std::list<RCQP*> pending_qps_;
  int* pending_counts_;
};