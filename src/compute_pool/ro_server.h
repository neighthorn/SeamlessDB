#pragma once

#include <brpc/channel.h>

#include "optimizer/optimizer.h"
#include "recovery/log_recovery.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "analyze/analyze.h"
#include "benchmark/test/test_wk.h"
#include "state/state_manager.h"

DEFINE_string(protocol, "baidu_std", "Protocol type");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "127.0.0.1:12190", "IP address of server");
DEFINE_int32(timeout_ms, 0x7fffffff, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 10, "Milliseconds between consecutive requests");

class RONode {
public:
    RONode(int local_rpc_port, std::string workload, int record_num, int thread_num) 
        : local_rpc_port_(local_rpc_port), workload_(workload), record_num_(record_num), thread_num_(thread_num){
        page_channel_ = new brpc::Channel();
        log_channel_ = new brpc::Channel();
        brpc::ChannelOptions options;
        options.protocol = FLAGS_protocol;
        options.connection_type = FLAGS_connection_type;
        options.timeout_ms = FLAGS_timeout_ms;
        options.max_retry = FLAGS_max_retry;
        if(page_channel_->Init(FLAGS_server.c_str(), &options) != 0) {
            std::cout << "Failed to initialize page_channel.\n";
            exit(1);
        }
        if(log_channel_->Init(FLAGS_server.c_str(), &options) != 0) {
            std::cout << "Failed to initialize log_channel.\n";
            exit(1);
        }

        disk_mgr_ = new DiskManager();
        slice_mgr_ = new SliceMetaManager();
        buffer_pool_mgr_ = new BufferPoolManager(NodeType::COMPUTE_NODE, BUFFER_POOL_SIZE, page_channel_, disk_mgr_, slice_mgr_);
        index_mgr_ = new IxManager(buffer_pool_mgr_, disk_mgr_);
        sm_mgr_ = new SmManager(disk_mgr_, buffer_pool_mgr_, index_mgr_);
        lock_mgr_ = new LockManager();
        txn_mgr_ = new TransactionManager(lock_mgr_, sm_mgr_);
        ql_mgr_ = new QlManager(sm_mgr_, txn_mgr_);
        log_mgr_ = new LogManager(log_channel_);
        planner_ = new Planner(sm_mgr_);
        optimizer_ = new Optimizer(planner_);
        portal_ = new Portal(sm_mgr_);
        analyze_ = new Analyze(sm_mgr_);

        if(workload_ == "test") {
            TestWK* test_wk = new TestWK(sm_mgr_, index_mgr_);
            test_wk->load_meta(record_num);
        }

        auto table = sm_mgr_->db_.get_table("test_table");
        std::cout << table.table_id_ << " " << table.record_length_ << "\n";
        
        ContextManager::create_instance();
        MetaManager::create_instance();
        std::cout << "finish create meta manager\n";
        RDMARegionAllocator::create_instance(MetaManager::get_instance(), thread_num_);
        std::cout << "finish create rdma region allocator\n";
        QPManager::create_instance(thread_num_);
        std::cout << "finish create qp manager\n";
        QPManager::BuildALLQPConnection(MetaManager::get_instance());
    }

    ~RONode() {}
    void start_server();

    DiskManager* disk_mgr_;                 // disk_manager is used to store intermediate results
    BufferPoolManager* buffer_pool_mgr_;
    IxManager* index_mgr_;
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
    int record_num_;
    int thread_local_sql_size_;
    int thread_local_cursor_size_;
    int thread_local_plan_size_;
    int thread_num_;
};