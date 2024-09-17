#pragma once
/**
 * 直接构建Query tree进行执行
 * 限定查询执行树的层数
 * 1、table_num为输入参数，首先对前table_num个表进行join，
 * 2、然后接下来在已经生成的算子树的基础上随机生成算子作为新的跟节点，并随机选择表
*/

#include "system/sm.h"
#include "execution/executor_block_join.h"
#include "execution/executor_hash_join.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_projection.h"
#include "execution/execution_sort.h"
#include "execution/executor_seq_scan.h"
#include "tpch_wk.h"
#include "compute_pool/rw_server.h"
#include "tpch_table.h"

static const int max_table_num = 8;
static const std::string tables[8] = {"region", "nation", "part", "customer", "orders", "supplier", "partsupp", "lineitem"};

class ComparativeExp {
public:
    int tree_height_;   // query tree的高度, tree_height_ = join_node_num + 3
    int node_num_;      // query tree中节点个数, node_num_ = 3 * (join_node_num_ + 1)
    int join_node_num_;  // query tree中join节点个数
    int record_num_;
    DiskManager* disk_mgr_;                 // disk_manager is used to store intermediate results
    BufferPoolManager* buffer_pool_mgr_;
    IxManager* index_mgr_;
    MultiVersionManager *mvcc_mgr_;
    SmManager* sm_mgr_;
    Portal* portal_;
    LockManager* lock_mgr_;
    TransactionManager* txn_mgr_;
    QlManager* ql_mgr_;
    LogManager* log_mgr_;
    SliceMetaManager* slice_mgr_;
    brpc::Channel* page_channel_;
    brpc::Channel* log_channel_;
    int buffer_pool_size_;
    int thread_num_;
    TPCH_TABLE::Region* region_;
    TPCH_TABLE::Customer* customer_;
    TPCH_TABLE::Lineitem* lineitem_;
    TPCH_TABLE::Nation* nation_;
    TPCH_TABLE::Orders* orders_;
    TPCH_TABLE::Part* part_;
    TPCH_TABLE::PartSupp* partsupp_;
    TPCH_TABLE::Supplier* supplier_;

    ComparativeExp(int join_num, int buffer_pool_size, int thread_num): join_node_num_(join_num), 
        buffer_pool_size_(buffer_pool_size), thread_num_(thread_num) {
        init_db_meta();
        assert(max_table_num == sm_mgr_->db_.tabs_.size());
    }

    void init_db_meta() {
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
        buffer_pool_mgr_ = new BufferPoolManager(NodeType::COMPUTE_NODE, buffer_pool_size_, page_channel_, disk_mgr_, slice_mgr_);
        index_mgr_ = new IxManager(buffer_pool_mgr_, disk_mgr_);
        mvcc_mgr_ = new MultiVersionManager(disk_mgr_, buffer_pool_mgr_);
        sm_mgr_ = new SmManager(disk_mgr_, buffer_pool_mgr_, index_mgr_, mvcc_mgr_);
        portal_ = new Portal(sm_mgr_);
        lock_mgr_ = new LockManager();
        txn_mgr_ = new TransactionManager(lock_mgr_, sm_mgr_, thread_num_);
        ql_mgr_ = new QlManager(sm_mgr_, txn_mgr_);
        log_mgr_ = new LogManager(log_channel_, ContextManager::get_instance()->log_rdma_buffer_);
        TPCHWK* tpch_wk = new TPCHWK(sm_mgr_, index_mgr_, record_num_, mvcc_mgr_);
        tpch_wk->create_table();
        region_ = new TPCH_TABLE::Region();
        customer_ = new TPCH_TABLE::Customer();
        lineitem_ = new TPCH_TABLE::Lineitem();
        nation_ = new TPCH_TABLE::Nation();
        orders_ = new TPCH_TABLE::Orders();
        part_ = new TPCH_TABLE::Part();
        partsupp_ = new TPCH_TABLE::PartSupp();
        supplier_ = new TPCH_TABLE::Supplier();
    }

    void get_table_cond(int table_id, std::vector<Condition>& filter_conds, std::vector<Condition>& index_conds);
    void get_join_cond(int left_tab_id, int right_tab_id, std::vector<Condition>& join_conds);
    std::string get_table_join_col(int tab_id);
    std::shared_ptr<Plan> generate_proj_plan(int tab_id, std::shared_ptr<Plan> scan_plan);
    std::shared_ptr<Plan> generate_total_proj_plan(int table_num, std::shared_ptr<Plan> prev_plan);

    std::shared_ptr<Plan> generate_query_tree(Context* context);
    void normal_exec();
    void re_exec();
};