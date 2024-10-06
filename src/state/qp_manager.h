#pragma once
#include <unistd.h>
#include "meta_manager.h"

/** This QPManager builds qp connections (compute node <-> memory node) 
 * for each txn thread in MasterNode
*/
class QPManager {
 public:
  static bool create_instance(int qp_mgr_num);
  static void destroy_instance();
  static QPManager* get_instance() {
    // assert(global_qp_mgr_ != nullptr);
    // return global_qp_mgr_;
    // assert(next_qp_mgr_idx_ < qp_mgr_num_);
    if(next_qp_mgr_idx_ >= qp_mgr_num_) next_qp_mgr_idx_ = 0;
    assert(global_qp_mgr_[next_qp_mgr_idx_] != nullptr);
    return global_qp_mgr_[next_qp_mgr_idx_ ++];
  }
  static void BuildALLQPConnection(MetaManager* meta_man);
  void BuildQPConnection(MetaManager* meta_man);

  ALWAYS_INLINE
  RCQP* GetRemoteTxnListQPWithNodeID(const node_id_t node_id) const {
    return txn_list_qps[node_id];
  }

  ALWAYS_INLINE
  RCQP* GetRemoteLockBufQPWithNodeID(const node_id_t node_id) const {
    return lock_buf_qps[node_id];
  }

  ALWAYS_INLINE
  RCQP* GetRemoteLogBufQPWithNodeID(const node_id_t node_id) const {
    return log_buf_qps[node_id];
  }

  ALWAYS_INLINE
  RCQP* GetRemoteSqlBufQPWithNodeID(const node_id_t node_id) const {
    return sql_buf_qps[node_id];
  }

  ALWAYS_INLINE
  RCQP* GetRemotePlanBufQPWithNodeID(const node_id_t node_id) const {
    return plan_buf_qps[node_id];
  }

  // ALWAYS_INLINE
  // RCQP* GetRemoteCursorBufQPWithNodeID(const node_id_t node_id) const {
  //   return cursor_buf_qps[node_id];
  // }

  /*
    join plan && join buf QP
  */
  ALWAYS_INLINE
  RCQP* GetRemoteJoinPlanBufQPWithNodeID(const node_id_t node_id) const {
    return join_plan_buf_qps[node_id];
  }

  ALWAYS_INLINE
  RCQP* GetRemoteJoinBlockBufQPWithNodeID(const node_id_t node_id) const {
    return join_block_buf_qps[node_id];
  }


  ALWAYS_INLINE
  void GetRemoteTxnListQPsWithNodeIDs(const std::vector<node_id_t>* node_ids, std::vector<RCQP*>& qps) {
    for (node_id_t node_id : *node_ids) {
      RCQP* qp = txn_list_qps[node_id];
      if (qp) {
        qps.push_back(qp);
      }
    }
  }

  ALWAYS_INLINE
  void GetRemoteLockBufQPsWithNodeIDs(const std::vector<node_id_t>* node_ids, std::vector<RCQP*>& qps) {
    for (node_id_t node_id : *node_ids) {
      RCQP* qp = lock_buf_qps[node_id];
      if (qp) {
        qps.push_back(qp);
      }
    }
  }

  ALWAYS_INLINE
  void GetRemoteLogBufQPsWithNodeIDs(const std::vector<node_id_t>* node_ids, std::vector<RCQP*>& qps) {
    for (node_id_t node_id : *node_ids) {
      RCQP* qp = log_buf_qps[node_id];
      if (qp) {
        qps.push_back(qp);
      }
    }
  }

 private:
  QPManager(int tid) {
    global_tid = tid + (int)getpid();
  }
  RCQP* txn_list_qps[MAX_REMOTE_NODE_NUM]{nullptr};

  RCQP* lock_buf_qps[MAX_REMOTE_NODE_NUM]{nullptr};

  RCQP* log_buf_qps[MAX_REMOTE_NODE_NUM]{nullptr};

  RCQP* sql_buf_qps[MAX_REMOTE_NODE_NUM]{nullptr};

  RCQP* cursor_buf_qps[MAX_REMOTE_NODE_NUM]{nullptr};

  RCQP* plan_buf_qps[MAX_REMOTE_NODE_NUM]{nullptr};
  
  /*
    QP for join state
  */
  RCQP *join_plan_buf_qps[MAX_REMOTE_NODE_NUM]{nullptr};

  RCQP *join_block_buf_qps[MAX_REMOTE_NODE_NUM]{nullptr};

  t_id_t global_tid;

  static int qp_mgr_num_;
  static int next_qp_mgr_idx_;
  static QPManager* global_qp_mgr_[MAX_THREAD_NUM];
};
