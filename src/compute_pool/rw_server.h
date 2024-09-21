#pragma once

#include <brpc/channel.h>

#include "optimizer/optimizer.h"
#include "recovery/log_recovery.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "state/state_manager.h"
#include "portal.h"
#include "analyze/analyze.h"
#include "benchmark/test/test_wk.h"
#include "benchmark/tpcc/tpcc_wk.h"
#include "benchmark/tpch/tpch_wk.h"

class RWNode {
public:
    RWNode(int local_rpc_port, std::string workload, int record_num, int thread_num, int buffer_pool_size, std::string config_path);

    ~RWNode() {}
    void start_server();

    DiskManager* disk_mgr_;                 // disk_manager is used to store intermediate results
    BufferPoolManager* buffer_pool_mgr_;
    IxManager* index_mgr_;
    MultiVersionManager *mvcc_mgr_;
    SmManager* sm_mgr_;
    LockManager* lock_mgr_;
    TransactionManager* txn_mgr_;
    QlManager* ql_mgr_;
    LogManager* log_mgr_;
    Planner* planner_;
    Optimizer* optimizer_;
    Portal* portal_;
    Analyze* analyze_;
    SliceMetaManager* slice_mgr_;
    pthread_mutex_t *buffer_mutex;
    pthread_mutex_t *sockfd_mutex;
    int local_rpc_port_;
    brpc::Channel* page_channel_;
    brpc::Channel* log_channel_;
    std::string workload_;
    int record_num_;        // for tpcc, record_num represents warehousenum
    int thread_local_sql_size_;
    int thread_local_cursor_size_;
    int thread_local_plan_size_;
    int thread_num_;
    int buffer_pool_size_;      // the max size count in bufferpool
    // int state_open_;
    std::string config_path_;

    /*
        RDMA asyn write thread pool
    */
    // ThreadPool *thread_pool_;
};