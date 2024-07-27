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

DEFINE_string(protocol, "baidu_std", "Protocol type");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "10.77.110.148:12190", "IP address of server");
DEFINE_int32(timeout_ms, 0x7fffffff, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 10, "Milliseconds between consecutive requests");

class RWNode {
public:
    RWNode(int local_rpc_port, std::string workload, int record_num, int thread_num, int buffer_pool_size, std::string config_path) 
        : local_rpc_port_(local_rpc_port), workload_(workload), record_num_(record_num), thread_num_(thread_num),
        buffer_pool_size_(buffer_pool_size), config_path_(config_path){

        // init page channel and log channel
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

        MetaManager::create_instance(config_path_);
        std::cout << "finish create meta manager\n";
        RDMARegionAllocator::create_instance(MetaManager::get_instance(), thread_num_);
        std::cout << "finish create rdma region allocator\n";
        QPManager::create_instance(thread_num_);
        std::cout << "finish create qp manager\n";
        QPManager::BuildALLQPConnection(MetaManager::get_instance());
        StateManager::create_instance(thread_num);
        std::cout << "finishi create state manager\n";
        
        // /*
        //     RDMA asyn write thread pool
        // */
        // thread_pool_ = new ThreadPool(16);
        // std::cout << "finish create RDMA asyn write thread pool\n";

        // create database components
        disk_mgr_ = new DiskManager();
        slice_mgr_ = new SliceMetaManager();
        buffer_pool_mgr_ = new BufferPoolManager(NodeType::COMPUTE_NODE, buffer_pool_size_, page_channel_, disk_mgr_, slice_mgr_);
        index_mgr_ = new IxManager(buffer_pool_mgr_, disk_mgr_);
        mvcc_mgr_ = new MultiVersionManager(disk_mgr_, buffer_pool_mgr_);
        sm_mgr_ = new SmManager(disk_mgr_, buffer_pool_mgr_, index_mgr_, mvcc_mgr_);
        lock_mgr_ = new LockManager();
        txn_mgr_ = new TransactionManager(lock_mgr_, sm_mgr_, thread_num_);
        ql_mgr_ = new QlManager(sm_mgr_, txn_mgr_);
        log_mgr_ = new LogManager(log_channel_, StateManager::get_instance()->log_rdma_buffer_);
        planner_ = new Planner(sm_mgr_);
        optimizer_ = new Optimizer(planner_);
        portal_ = new Portal(sm_mgr_);
        analyze_ = new Analyze(sm_mgr_);

        // load test workload
        if(workload_ == "test") {
            TestWK* test_wk = new TestWK(sm_mgr_, index_mgr_, record_num_, mvcc_mgr_);
            test_wk->create_table();
            // test_wk->load_meta();
        }
        else if(workload == "tpcc") {
            TPCCWK* tpcc_wk = new TPCCWK(sm_mgr_, index_mgr_, record_num_, mvcc_mgr_);
            tpcc_wk->create_table();
            // tpcc_wk->load_meta();
        } 
        else if(workload == "tpch") {
            TPCHWK* tpch_wk = new TPCHWK(sm_mgr_, index_mgr_, record_num_, mvcc_mgr_);
            tpch_wk->create_table();
            // tpch_wk->load_meta();
        }
        else {
            std::cerr << "workload not supported!\n";
        }
    }

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