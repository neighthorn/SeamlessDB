#include "coroutine_scheduler.h"
#include <algorithm>

// ALWAYS_INLINE
// bool CoroutineScheduler::RDMAWrite(coro_id_t coro_id, RCQP* qp, char* wt_data, uint64_t remote_offset, size_t size) {
//   auto rc = qp->post_send(IBV_WR_RDMA_WRITE, wt_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
//   if(rc != SUCC) {
//     RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
//     return false;
//   }
//   AddPendingQP(coro_id, qp);
//   return true;
// }

// ALWAYS_INLINE
// bool CoroutineScheduler::RDMARead(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size) {
//   auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
//   if (rc != SUCC) {
//     RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
//     return false;
//   }
//   AddPendingQP(coro_id, qp);
//   return true;
// }

// ALWAYS_INLINE
// bool CoroutineScheduler::RDMAFAA(coro_id_t coro_id, RCQP* qp, char* local_buf, uint64_t remote_offset, uint64_t add) {
//   auto rc = qp->post_faa(local_buf, remote_offset, add, IBV_SEND_SIGNALED, coro_id);
//   if (rc != SUCC) {
//     RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
//     return false;
//   }
//   AddPendingQP(coro_id, qp);
//   return true;
// }

// ALWAYS_INLINE
// bool CoroutineScheduler::RDMACAS(coro_id_t coro_id, RCQP* qp, char* local_buf, uint64_t remote_offset, uint64_t compare, uint64_t swap) {
//   auto rc = qp->post_cas(local_buf, remote_offset, compare, swap, IBV_SEND_SIGNALED, coro_id);
//   if (rc != SUCC) {
//     RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
//     return false;
//   }
//   AddPendingQP(coro_id, qp);
//   return true;
// }

const size_t MAX_MSG_SIZE = 1024*1024*1024;  // 1GB

// ALWAYS_INLINE
bool CoroutineScheduler::RDMAReadSync(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size) {
  if(size >= MAX_MSG_SIZE) {
    int offset = remote_offset;
    size_t readed_size = 0;
    while(readed_size < size) {
      int curr_read_size = std::min(MAX_MSG_SIZE, size - readed_size);
      auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data + readed_size, curr_read_size, remote_offset + readed_size, IBV_SEND_SIGNALED, coro_id);
      readed_size += curr_read_size;
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
        return false;
      }
      ibv_wc wc{};
      rc = qp->poll_till_completion(wc, no_timeout);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
        return false;
      }
    }
    return true;
  }
  auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
    return false;
  }
  ibv_wc wc{};
  rc = qp->poll_till_completion(wc, no_timeout);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
    return false;
  }
  return true;
}

// ALWAYS_INLINE
bool CoroutineScheduler::RDMACASSync(coro_id_t coro_id, RCQP* qp, char* local_buf, uint64_t remote_offset, uint64_t compare, uint64_t swap) {
  auto rc = qp->post_cas(local_buf, remote_offset, compare, swap, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
    return false;
  }
  ibv_wc wc{};
  rc = qp->poll_till_completion(wc, no_timeout);
  if(rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
    return false;
  }
  return true;
}

// ALWAYS_INLINE
bool CoroutineScheduler::RDMAWriteSync(coro_id_t coro_id, RCQP* qp, char* wt_data, uint64_t remote_offset, size_t size) {
  if(size >= MAX_MSG_SIZE) {
    int offset = remote_offset;
    size_t posted_size = 0;
    while(posted_size < size) {
      int curr_post_size = std::min(MAX_MSG_SIZE, size - posted_size);
      auto rc = qp->post_send(IBV_WR_RDMA_WRITE, wt_data + posted_size, curr_post_size, remote_offset + posted_size, IBV_SEND_SIGNALED, coro_id);
      posted_size += curr_post_size;
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
        return false;
      }
      ibv_wc wc{};
      rc = qp->poll_till_completion(wc, no_timeout);
      if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
        return false;
      }
    }
    return true;
  }
  auto rc = qp->post_send(IBV_WR_RDMA_WRITE, wt_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
    return false;
  }
  ibv_wc wc{};
  rc = qp->poll_till_completion(wc, no_timeout);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
    return false;
  }
  return true;
}

// ALWAYS_INLINE
bool CoroutineScheduler::RDMABatchSync(coro_id_t coro_id, RCQP* qp, ibv_send_wr* send_sr, ibv_send_wr** bad_sr_addr, int doorbell_num) {
  send_sr[doorbell_num].wr_id = coro_id;
  auto rc = qp->post_batch(send_sr, bad_sr_addr);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post batch fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
    return false;
  }
  ibv_wc wc{};
  rc = qp->poll_till_completion(wc, no_timeout);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll batch fail. rc=" << rc << ", tid = " << t_id_ << ", coroid = " << coro_id;
    return false;
  }
  return true;
}


