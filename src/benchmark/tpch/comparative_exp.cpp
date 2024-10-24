#include <chrono>

#include "comparative_exp.h"

#include "optimizer/planner.h"
#include "util/json_util.h"
#include "state/resume_util.h"
#include "execution/comp_ckpt_mgr.h"

DEFINE_string(protocol, "baidu_std", "Protocol type");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "127.0.0.1:12190", "IP address of server");
DEFINE_int32(timeout_ms, 0x7fffffff, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 10, "Milliseconds between consecutive requests");

void ComparativeExp::init_db_meta() {
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

void ComparativeExp::get_table_cond(int table_id, std::vector<Condition>& filter_conds, std::vector<Condition>& index_conds) {
    switch(table_id) {
        case 0: {
            region_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        case 1: {
            nation_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        case 2: {
            customer_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        case 3: {
            supplier_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        case 4: {
            orders_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        case 5: {
            lineitem_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        case 6: {
            partsupp_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        case 7: {
            part_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        default:
        break;
    }
}

std::string ComparativeExp::get_table_join_col(int tab_id) {
    switch(tab_id) {
        case 0: {
            return std::move("r_regionkey");
        } break;
        case 1: {
            int rnd = RandomGenerator::generate_random_int(1, 2);
            if(rnd == 1) return std::move("n_regionkey");
            else return std::move("n_nationkey");
        } break;
        case 2: {
            int rnd = RandomGenerator::generate_random_int(1, 2);
            if(rnd == 1) return std::move("c_custkey");
            else return std::move("c_nationkey");
        } break;
        case 3: {
            int rnd = RandomGenerator::generate_random_int(1, 2);
            if(rnd == 1) return std::move("s_suppkey");
            else return std::move("s_nationkey");
        } break;
        case 4: {
            int rnd = RandomGenerator::generate_random_int(1, 2);
            if(rnd == 1) return std::move("o_orderkey");
            else return std::move("o_custkey");
        } break;
        case 5: {
            int rnd = RandomGenerator::generate_random_int(1, 2);
            if(rnd == 1) return std::move("l_suppkey");
            else return std::move("l_orderkey");
        } break;
        case 6: {
            int rnd = RandomGenerator::generate_random_int(1, 2);
            if(rnd == 1) return std::move("ps_suppkey");
            else return std::move("ps_partkey");
        } break;
        case 7: {
            return std::move("p_partkey");
        } break;
        default:
        break;
    }
}

// void ComparativeExp::get_join_cond(int left_tab_id, int right_tab_id, std::vector<Condition>& join_conds) {
//     Condition cond;
//     cond.is_rhs_val = false;
//     cond.lhs_col = TabCol{.tab_name = tables[left_tab_id], .col_name = std::move(get_table_join_col(left_tab_id))};
//     cond.rhs_col = TabCol{.tab_name = tables[right_tab_id], .col_name = std::move(get_table_join_col(right_tab_id))};
//     cond.op = OP_EQ;
//     join_conds.push_back(std::move(cond));
// }

#define make_lhs_col(left_tab_id, left_col) cond.lhs_col = TabCol{.tab_name = tables[left_tab_id], .col_name = left_col};
#define make_rhs_col(right_tab_id, right_col) cond.rhs_col = TabCol{.tab_name = tables[right_tab_id], .col_name = right_col};

int ComparativeExp::get_join_cond(int left_table_range, int right_tab_id, std::vector<Condition>& join_conds) {
    // left_table_range = std::min(left_table_range, 7);
    Condition cond;
    cond.is_rhs_val = false;
    cond.op = OP_EQ;
    switch(right_tab_id) {
        case 0: {   // region
            assert(left_table_range >= 7);
            // r_regionkey = n_regionkey
            make_lhs_col(1, "n_regionkey");
            make_rhs_col(0, "r_regionkey");
            join_conds.push_back(std::move(cond));
            return 1;
        } break;
        case 1: {   // nation
            int rnd;
            if(left_table_range >= 7) rnd = (left_table_range % 2) + 1;
            else rnd = 1;

            if(rnd == 1) {
                make_lhs_col(0, "r_regionkey");
                make_rhs_col(1, "n_regionkey");
                join_conds.push_back(std::move(cond));
                return 0;
            }
            else {
                make_lhs_col(2, "c_nationkey");
                make_rhs_col(1, "n_nationkey");
                join_conds.push_back(std::move(cond));
                return 2;
            }
        } break;
        case 2: {   // customer
            int rnd;
            if(left_table_range >= 7) rnd = (left_table_range % 3) + 1;
            else rnd = 1;

            if(rnd == 1) {
                make_lhs_col(1, "n_nationkey");
                make_rhs_col(2, "c_nationkey");
                join_conds.push_back(std::move(cond));
                return 1;
            }
            else if(rnd == 2) {
                make_lhs_col(3, "s_nationkey");
                make_rhs_col(2, "c_nationkey");
                join_conds.push_back(std::move(cond));
                return 3;
            }
            else {
                make_lhs_col(4, "o_custkey");
                make_rhs_col(2, "c_custkey");
                join_conds.push_back(std::move(cond));
                return 4;
            }
        } break;
        case 3: {   // supplier
            int rnd;
            if(left_table_range >= 7) rnd = (left_table_range % 4) + 1;
            else rnd = (left_table_range % 2) + 1;  

            if(rnd == 1) {
                make_lhs_col(1, "n_nationkey");
                make_rhs_col(3, "s_nationkey");
                join_conds.push_back(std::move(cond));
                return 1;
            }
            else if(rnd == 2) {
                make_lhs_col(2, "c_nationkey");
                make_rhs_col(3, "s_nationkey");
                join_conds.push_back(std::move(cond));
                return 2;
            }
            else if(rnd == 3) {
                make_lhs_col(5, "l_suppkey");
                make_rhs_col(3, "s_suppkey");
                join_conds.push_back(std::move(cond));
                return 5;
            }
            else {
                make_lhs_col(6, "ps_suppkey");
                make_rhs_col(3, "s_suppkey");
                join_conds.push_back(std::move(cond));
                return 6;
            }
        } break;
        case 4: {   // orders
            int rnd;
            if(left_table_range >= 7) rnd = (left_table_range % 2) + 1;
            else rnd = 1;

            if(rnd == 1) {
                make_lhs_col(2, "c_custkey");
                make_rhs_col(4, "o_custkey");
                join_conds.push_back(std::move(cond));
                return 2;
            }
            else {
                make_lhs_col(5, "l_orderkey");
                make_rhs_col(4, "o_orderkey");
                join_conds.push_back(std::move(cond));
                return 5;
            }
        } break;
        case 5: {   // lineitem
            int rnd;
            rnd = (left_table_range % 2) + 1;

            if(rnd == 1) {
                make_lhs_col(3, "s_suppkey");
                make_rhs_col(5, "l_suppkey");
                join_conds.push_back(std::move(cond));
                return 3;
            }
            else {
                make_lhs_col(4, "o_orderkey");
                make_rhs_col(5, "l_orderkey");
                join_conds.push_back(std::move(cond));
                return 4;
            }
        } break;
        case 6: {   // partsupp
            int rnd;
            if(left_table_range >= 7) rnd = (left_table_range % 2) + 1;
            else rnd = 1;

            if(rnd == 1) {
                make_lhs_col(3, "s_suppkey");
                make_rhs_col(6, "ps_suppkey");
                join_conds.push_back(std::move(cond));
                return 3;
            }
            else {
                make_lhs_col(7, "p_partkey");
                make_rhs_col(6, "ps_partkey");
                join_conds.push_back(std::move(cond));
                return 7;
            }
        } break;
        case 7: {   // part
            make_lhs_col(6, "ps_partkey");
            make_rhs_col(7, "p_partkey");
            join_conds.push_back(std::move(cond));
            return 6;
        } break;
        default:
        break;
    }
    assert(0);
    return -1;
}

std::shared_ptr<Plan> ComparativeExp::generate_proj_plan(int tab_id, std::shared_ptr<Plan> scan_plan, Context* context, int& curr_sql_id, int& curr_plan_id) {
    switch(tab_id) {
        case 0: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "region", .col_name = "r_regionkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "region", .col_name = "r_name"}));
            return std::make_shared<ProjectionPlan>(T_Projection, curr_sql_id, curr_plan_id ++, std::move(scan_plan), std::move(cols));
        } break;
        case 1: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_nationkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_regionkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_name"}));
            return std::make_shared<ProjectionPlan>(T_Projection, curr_sql_id, curr_plan_id ++, std::move(scan_plan), std::move(cols));
        } break;
        case 2: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "customer", .col_name = "c_custkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "customer", .col_name = "c_nationkey"}));
            return std::make_shared<ProjectionPlan>(T_Projection, curr_sql_id, curr_plan_id ++, std::move(scan_plan), std::move(cols));
        } break;
        case 3: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "supplier", .col_name = "s_suppkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "supplier", .col_name = "s_nationkey"}));
            return std::make_shared<ProjectionPlan>(T_Projection, curr_sql_id, curr_plan_id ++, std::move(scan_plan), std::move(cols));
        } break;
        case 4: {
            std::vector<TabCol> cols;
            
            cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_orderkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_orderdate"}));
            cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_custkey"}));
            return std::make_shared<ProjectionPlan>(T_Projection, curr_sql_id, curr_plan_id ++, std::move(scan_plan), std::move(cols));
        } break;
        case 5: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_suppkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_linenumber"}));
            cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_orderkey"}));
            return std::make_shared<ProjectionPlan>(T_Projection, curr_sql_id, curr_plan_id ++, std::move(scan_plan), std::move(cols));
        } break;
        case 6: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "partsupp", .col_name = "ps_partkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "partsupp", .col_name = "ps_suppkey"}));
            return std::make_shared<ProjectionPlan>(T_Projection, curr_sql_id, curr_plan_id ++, std::move(scan_plan), std::move(cols));    
        } break;
        case 7: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "part", .col_name = "p_partkey"}));
            return std::make_shared<ProjectionPlan>(T_Projection, curr_sql_id, curr_plan_id ++, std::move(scan_plan), std::move(cols));
        } break;
        default:
        break;
    }
    assert(0);
    return nullptr;
}

std::shared_ptr<Plan> ComparativeExp::generate_total_proj_plan(int table_num, std::shared_ptr<Plan> prev_plan, Context* context, int& curr_sql_id, int& curr_plan_id) {
    std::vector<TabCol> cols;
    cols.push_back(std::move(TabCol{.tab_name = "region", .col_name = "r_regionkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "region", .col_name = "r_name"}));
    if(table_num == 1) goto FINAL_PROJ_PLAN;
    cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_nationkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_regionkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_name"}));
    if(table_num == 2) goto FINAL_PROJ_PLAN;
    cols.push_back(std::move(TabCol{.tab_name = "customer", .col_name = "c_id"}));
    cols.push_back(std::move(TabCol{.tab_name = "customer", .col_name = "c_custkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "customer", .col_name = "c_nationkey"}));
    if(table_num == 3) goto FINAL_PROJ_PLAN;
    cols.push_back(std::move(TabCol{.tab_name = "supplier", .col_name = "s_suppkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "supplier", .col_name = "s_nationkey"}));
    if(table_num == 4) goto FINAL_PROJ_PLAN;
    cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_id"}));
    cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_orderkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_orderdate"}));
    cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_custkey"}));
    if(table_num == 5) goto FINAL_PROJ_PLAN;
    cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_id"}));
    cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_shipdate"}));
    cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_suppkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_linenumber"}));
    cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_orderkey"}));
    if(table_num == 6) goto FINAL_PROJ_PLAN;
    cols.push_back(std::move(TabCol{.tab_name = "partsupp", .col_name = "ps_partkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "partsupp", .col_name = "ps_suppkey"}));
    if(table_num == 7) goto FINAL_PROJ_PLAN;
    cols.push_back(std::move(TabCol{.tab_name = "part", .col_name = "p_partkey"}));

FINAL_PROJ_PLAN:
    return std::make_shared<ProjectionPlan>(T_Projection, curr_sql_id, curr_plan_id ++, std::move(prev_plan), std::move(cols));
}

void get_tab_cols(int table_id, std::vector<TabCol>& tab_cols) {
    switch(table_id) {
        case 0: {
            tab_cols.push_back(std::move(TabCol{.tab_name = "region", .col_name = "r_regionkey"}));
            tab_cols.push_back(std::move(TabCol{.tab_name = "region", .col_name = "r_name"}));
        } break;
        case 1: {
            tab_cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_nationkey"}));
            tab_cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_regionkey"}));
            tab_cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_name"}));
        } break;
        case 2: {
            tab_cols.push_back(std::move(TabCol{.tab_name = "customer", .col_name = "c_id"}));
            tab_cols.push_back(std::move(TabCol{.tab_name = "customer", .col_name = "c_custkey"}));
            tab_cols.push_back(std::move(TabCol{.tab_name = "customer", .col_name = "c_nationkey"}));
        } break;
        case 3: {
            tab_cols.push_back(std::move(TabCol{.tab_name = "supplier", .col_name = "s_suppkey"}));
            tab_cols.push_back(std::move(TabCol{.tab_name = "supplier", .col_name = "s_nationkey"}));
        } break;
        case 4: {
            tab_cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_id"}));
            tab_cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_orderkey"}));
            tab_cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_orderdate"}));
            tab_cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_custkey"}));
        } break;
        case 5: {
            tab_cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_id"}));
            tab_cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_shipdate"}));
            tab_cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_suppkey"}));
            tab_cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_linenumber"}));
            tab_cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_orderkey"}));
        } break;
        case 6: {
            tab_cols.push_back(std::move(TabCol{.tab_name = "partsupp", .col_name = "ps_partkey"}));
            tab_cols.push_back(std::move(TabCol{.tab_name = "partsupp", .col_name = "ps_suppkey"}));
        } break;
        case 7: {
            tab_cols.push_back(std::move(TabCol{.tab_name = "part", .col_name = "p_partkey"}));
        } break;
        default:
        break;
    }
}

std::shared_ptr<Plan> ComparativeExp::generate_query_tree(Context* context) {
    // assert(join_node_num + 1 >= max_table_num);
    node_num_ = 3 * (join_node_num_ + 1);
    tree_height_ = join_node_num_ + 3;
    
    std::shared_ptr<Plan> plan;
    int curr_sql_id_ = 0;
    int curr_plan_id_ = 0;
    
    // 如果table_num 超过了表总个数，先对前面几张表进行join
    int first_join_table_num = std::min(join_node_num_ + 1, max_table_num);
    for(int i = 0; i < first_join_table_num; ++i) {
        std::vector<Condition> index_conds;
        std::vector<Condition> filter_conds;
        std::vector<Condition> join_conds;
        std::vector<TabCol> tab_cols;

        // index_cond 和 filter cond 用generate_cond随机生成
        get_table_cond(i, filter_conds, index_conds);        
        get_tab_cols(i, tab_cols);

        if(i == 0) {
            // 第一张表直接生成scan executor
            auto scan_plan = std::make_shared<ScanPlan>(T_IndexScan, curr_sql_id_, curr_plan_id_ ++, sm_mgr_, tables[i], filter_conds, index_conds, tab_cols);
            assert(scan_plan != nullptr);
            plan = std::move(scan_plan);
            // plan = generate_proj_plan(i, std::move(scan_plan), context, curr_sql_id_, curr_plan_id_);
        }
        else {
            assert(plan != nullptr);
            // 后面的表直接放到join executor里
            //join_cond为当前表和随机选取前面已经join的表的固定join条件
            // int left_join_table_id = RandomGenerator::generate_random_int(0, i - 1);
            // get_join_cond(left_join_table_id, i, join_conds);
            int left_join_table_id = get_join_cond(i - 1, i, join_conds);
            
            auto scan_plan = std::make_shared<ScanPlan>(T_IndexScan, curr_sql_id_, curr_plan_id_ ++, sm_mgr_, tables[i], filter_conds, index_conds, tab_cols);
            assert(scan_plan != nullptr);
            // auto proj_plan = generate_proj_plan(i, std::move(scan_plan), context, curr_sql_id_, curr_plan_id_);
            // 随机生成 hash join or nestedloop join
            // int rnd = RandomGenerator::generate_random_int(1, 2);
            // plan = std::make_shared<JoinPlan>(T_NestLoop, curr_sql_id_, curr_plan_id_ ++, std::move(plan), std::move(scan_plan), join_conds);
            plan = std::make_shared<JoinPlan>(T_HashJoin, curr_sql_id_, curr_plan_id_ ++, std::move(plan), std::move(scan_plan), join_conds);
        }
    }

    // 只有join_node_num_ > max_table_num的情况下才会进入下面的循环，因此所有的表都已经在当前的query tree出现过了
    for(int i = first_join_table_num; i <= join_node_num_; ++i) {
        if(i % 8 == 0) {
            // 每7个join算子进行一次projection，防止tuple长度无限扩张
            plan = generate_total_proj_plan(join_node_num_ + 1, std::move(plan), context, curr_sql_id_, curr_plan_id_);
        }
        assert(plan != nullptr);
        std::vector<Condition> index_conds;
        std::vector<Condition> filter_conds;
        std::vector<Condition> join_conds;
        std::vector<TabCol> tab_cols;

        // int right_tab_id = RandomGenerator::generate_random_int(0, max_table_num - 1);
        int right_tab_id = i % 8;
        // int left_tab_id = RandomGenerator::generate_random_int(0, max_table_num - 1);
        int left_tab_id = get_join_cond(i, right_tab_id, join_conds);

        get_table_cond(right_tab_id, filter_conds, index_conds);
        get_tab_cols(right_tab_id, tab_cols);
        
        // get_join_cond(left_tab_id, right_tab_id, join_conds);
        
        auto scan_plan = std::make_shared<ScanPlan>(T_IndexScan, curr_sql_id_, curr_plan_id_ ++, sm_mgr_, tables[right_tab_id], filter_conds, index_conds, tab_cols);
        assert(scan_plan != nullptr);
        // auto proj_plan = generate_proj_plan(right_tab_id, std::move(scan_plan), context, curr_sql_id_, curr_plan_id_);
        // int rnd = RandomGenerator::generate_random_int(1, 2);
        // plan = std::make_shared<JoinPlan>(T_NestLoop, curr_sql_id_, curr_plan_id_ ++, std::move(plan), std::move(proj_plan), join_conds);
        plan = std::make_shared<JoinPlan>(T_HashJoin, curr_sql_id_, curr_plan_id_ ++, std::move(plan), std::move(scan_plan), join_conds);
    }

    TabCol order_col;
    order_col.tab_name = "region";
    order_col.col_name = "r_regionkey";
    plan = std::make_shared<SortPlan>(T_Sort, curr_sql_id_, curr_plan_id_ ++, std::move(plan), std::move(order_col), false);
    
    // 最后再进行一次projection
    plan = generate_total_proj_plan(join_node_num_ + 1, std::move(plan), context, curr_plan_id_, curr_sql_id_);
    
    plan->format_print();

    plan = std::make_shared<DMLPlan>(T_select, std::move(plan), std::string(), 0, std::vector<Value>(),
                                                    std::vector<Condition>(), std::vector<SetClause>());
    context->plan_tag_ = T_select;

    return plan;
}

int state_open_ = 0;
double state_theta_ = -1.0;
double src_scale_factor_ = 1000.0;
int block_size_ = 500;
int node_type_ = 0; // rw_server default
int MB_ = 1024;
int RB_ = 1024;
int C_ = 1000;
int cost_model_ = 0;
int interval_ = 0;
bool write_ckpt_ = true;

void ComparativeExp::normal_exec() {
    std::cout << "************************ normal_exec ************************" << std::endl;
    auto normal_start = std::chrono::high_resolution_clock::now();

    int connection_id = 0;
    CoroutineScheduler* coro_sched = new CoroutineScheduler(connection_id, CORO_NUM);
    auto local_rdma_region_range = RDMARegionAllocator::get_instance()->GetThreadLocalRegion(connection_id);
    RDMABufferAllocator* rdma_buffer_allocator = new RDMABufferAllocator(local_rdma_region_range.first, local_rdma_region_range.second);
    QPManager* qp_mgr = QPManager::get_instance();
    bool rdma_allocated = true;
    Transaction* txn = txn_mgr_->get_transaction(connection_id);
    MetaManager* meta_mgr = MetaManager::get_instance();
    OperatorStateManager* op_state_manager = new OperatorStateManager(connection_id, coro_sched, meta_mgr, qp_mgr);
    char* result_str = new char[BUFFER_LENGTH];
    int offset = 0;
    Context* context = new Context(lock_mgr_, log_mgr_, txn, coro_sched, op_state_manager, qp_mgr, result_str, &offset, rdma_allocated);

    std::shared_ptr<Plan> plan = generate_query_tree(context);
    std::shared_ptr<PortalStmt> portal_stmt = portal_->start(plan, context);
    if(portal_stmt != nullptr) {
        CompCkptManager::get_instance()->add_new_query_tree(portal_stmt->root);
    }
    else {
        std::cerr << "query tree is nullptr\n";
    }
    portal_->run(portal_stmt, ql_mgr_, context);

    auto normal_end = std::chrono::high_resolution_clock::now();
    // while(!op_state_manager->finish_write()) {
    //     normal_end = std::chrono::high_resolution_clock::now();
    // }
    if(op_state_manager->finish_write()) {
        std::cout << "finish\n";
    }
    else {
        std::cout << "nofinish\n";
    }

    auto normal_duration = std::chrono::duration_cast<std::chrono::duration<double>>(normal_end - normal_start);

    // print_char_array(result_str, offset);

    std::cout << "time for normal exec: " << normal_duration.count() << "s" << std::endl;
    std::cout << "write op checkpoints: " << OperatorStateManager::write_cnts << "\n";
    std::cout << "write op checkpoint size(Bytes): " << OperatorStateManager::write_tot_size << "\n";
    delete rdma_buffer_allocator;
    delete[] result_str;
    delete context;
    std::cout << "--------------------------- finish ---------------------------" << std::endl;
}

void ComparativeExp::re_exec() {
    std::cout << "************************ re_exec ************************" << std::endl;
    int connection_id = 0;
    CoroutineScheduler* coro_sched = new CoroutineScheduler(connection_id, CORO_NUM);
    auto local_rdma_region_range = RDMARegionAllocator::get_instance()->GetThreadLocalRegion(connection_id);
    RDMABufferAllocator* rdma_buffer_allocator = new RDMABufferAllocator(local_rdma_region_range.first, local_rdma_region_range.second);
    QPManager* qp_mgr = QPManager::get_instance();
    bool rdma_allocated = true;
    Transaction* txn = txn_mgr_->get_transaction(connection_id);
    MetaManager* meta_mgr = MetaManager::get_instance();
    OperatorStateManager* op_state_manager = new OperatorStateManager(connection_id, coro_sched, meta_mgr, qp_mgr);
    char* result_str = new char[BUFFER_LENGTH];
    int offset = 0;
    Context* context = new Context(lock_mgr_, log_mgr_, txn, coro_sched, op_state_manager, qp_mgr, result_str, &offset, rdma_allocated);

    std::shared_ptr<Plan> plan = generate_query_tree(context);
    std::shared_ptr<PortalStmt> portal_stmt = portal_->start(plan, context);
    if(portal_stmt != nullptr) {
        CompCkptManager::get_instance()->add_new_query_tree(portal_stmt->root);
    }
    else {
        std::cerr << "query tree is nullptr\n";
    }
    
    // begin to 
    auto op_ck_meta = context->op_state_mgr_->read_op_checkpoint_meta();

    if(op_ck_meta->checkpoint_num != 0) {
        RwServerDebug::getInstance()->DEBUG_PRINT("Recover from checkpoint\n");
        auto op_checkpoints = context->op_state_mgr_->read_op_checkpoints(op_ck_meta.get());
        rebuild_exec_plan_with_query_tree(context, portal_stmt, op_ck_meta.get(), op_checkpoints);
        portal_->re_run(portal_stmt, ql_mgr_, context);
    }
    else {
        RwServerDebug::getInstance()->DEBUG_PRINT("Execute SQL from the beginning\n");
        portal_->run(portal_stmt, ql_mgr_, context);
    }

    print_char_array(result_str, offset);
    delete rdma_buffer_allocator;
    delete[] result_str;
    delete context;
}

const int bytes_num = 1024*8;
char* test_buffer = new char[bytes_num];

int main(int argc, char** argv) {
    /*
    整体执行流程：执行normal_time * break_percentage秒之后，kill掉正常执行的进程，进行恢复
    */

    std::string config_path = "../src/config/benchmark_config.json";
    cJSON* cjson = parse_json_file(config_path);
    cJSON* comp_exp_config = cJSON_GetObjectItem(cjson, "comparative_exp");
    int join_num = cJSON_GetObjectItem(comp_exp_config, "join_num")->valueint;
    std::string cost_model_type = cJSON_GetObjectItem(comp_exp_config, "cost_model")->valuestring;
    node_type_ = cJSON_GetObjectItem(comp_exp_config, "node_type")->valueint;
    int buffer_pool_size = cJSON_GetObjectItem(comp_exp_config, "buffer_pool_size")->valueint;
    int thread_num = cJSON_GetObjectItem(comp_exp_config, "thread_num")->valueint;
    double normal_time = cJSON_GetObjectItem(comp_exp_config, "normal_time")->valuedouble;
    double break_percentage = cJSON_GetObjectItem(comp_exp_config, "break_percentage")->valuedouble;

    state_open_ = cJSON_GetObjectItem(comp_exp_config, "state_open")->valueint;
    state_theta_ = cJSON_GetObjectItem(comp_exp_config, "state_theta")->valuedouble;
    src_scale_factor_ = cJSON_GetObjectItem(comp_exp_config, "src_scale_factor")->valuedouble;
    block_size_ = cJSON_GetObjectItem(comp_exp_config, "block_size")->valueint;
    interval_ = cJSON_GetObjectItem(comp_exp_config, "interval")->valueint;
    int write_ckpt_num = cJSON_GetObjectItem(comp_exp_config, "write_ckpt")->valueint;
    if(write_ckpt_num == 1) write_ckpt_ = true;
    else write_ckpt_ = false;

    if(cost_model_type.compare("SeamlessDB") == 0) {
        cost_model_ = 0;
    }
    else if(cost_model_type.compare("PREDATOR") == 0) {
        cost_model_ = 1;
    }
    else if(cost_model_type.compare("IntervalCkpt") == 0) {
        cost_model_ = 2;
    }

    MetaManager::create_instance(config_path);
    RDMARegionAllocator::create_instance(MetaManager::get_instance(), 1);
    QPManager::create_instance(1);
    QPManager::BuildALLQPConnection(MetaManager::get_instance());
    ContextManager::create_instance(1);
    CompCkptManager::create_instance();
    ComparativeExp* comparative_exp = new ComparativeExp(join_num, buffer_pool_size, thread_num);

    std::thread normal_thread(&ComparativeExp::normal_exec, comparative_exp);

    std::this_thread::sleep_for(std::chrono::duration<double>(normal_time * break_percentage));
    std::cout << "kill normal thread" << std::endl;
    pthread_cancel(normal_thread.native_handle());
    
    std::cout << "start re_exec" << std::endl;
    comparative_exp->re_exec();

    if(normal_thread.joinable()) normal_thread.join();

    
    // memset(test_buffer, 0, bytes_num);
    // CoroutineScheduler* coro_sched = new CoroutineScheduler(0, CORO_NUM);
    // auto rdma_region = RDMARegionAllocator::get_instance()->GetThreadLocalJoinBlockRegion(0);
    // MetaManager* meta_mgr = MetaManager::get_instance();
    // QPManager* qp_mgr = QPManager::get_instance();
    // OperatorStateManager* op_state_manager = new OperatorStateManager(0, coro_sched, meta_mgr, qp_mgr);
    // RCQP* qp = qp_mgr->GetRemoteJoinBlockBufQPWithNodeID(0);
    // size_t remote_offset = meta_mgr->GetJoinBlockAddrByThread(0);

    // memset(test_buffer, 1, bytes_num);

    // auto begin = std::chrono::high_resolution_clock::now();
    
    // // op_state_manager->add_operator_state_to_buffer()
    // // RCQP* qp = QPManager::get_instance()->GetRemoteJoinBlockBufQPWithNodeID(0);
    
    // if(!coro_sched->RDMAReadSync(0, qp, rdma_region.first, remote_offset, bytes_num)) {
    //     std::cerr << "RDMAReadSync failed" << std::endl;
    // }
    
    // auto end = std::chrono::high_resolution_clock::now();
    
    // if(!coro_sched->RDMAWriteSync(0, qp, rdma_region.first + bytes_num, remote_offset, bytes_num)) {
    //     std::cerr << "RDMAWriteSync failed" << std::endl;
    // }
    

    // auto rdma_write_end = std::chrono::high_resolution_clock::now();

    
    // memcpy(rdma_region.first, test_buffer, bytes_num);

    // auto rdma_read_end = std::chrono::high_resolution_clock::now();

    // std::cout << "time for memcpy 1MB: " << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << " miu s" << std::endl;

    // std::cout << "time for RDMAWrite 1MB: " << std::chrono::duration_cast<std::chrono::microseconds>(rdma_write_end - end).count() << " miu s" << std::endl;

    // std::cout << "time for RDMARead 1MB: " << std::chrono::duration_cast<std::chrono::microseconds>(rdma_read_end - rdma_write_end).count() << " miu s" << std::endl;
    return 0;
}