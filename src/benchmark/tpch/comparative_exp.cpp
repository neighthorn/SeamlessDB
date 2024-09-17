#include "comparative_exp.h"

#include "optimizer/planner.h"
#include "util/json_util.h"

void ComparativeExp::get_table_cond(int table_id, std::vector<Condition>& filter_conds, std::vector<Condition>& index_conds) {
    switch(table_id) {
        case 0: {
            region_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        case 1: {
            nation_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        case 2: {
            part_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        case 3: {
            customer_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        case 4: {
            orders_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        case 5: {
            supplier_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        case 6: {
            partsupp_->get_random_condition(1, index_conds, filter_conds, true);
        } break;
        case 7: {
            lineitem_->get_random_condition(1, index_conds, filter_conds, true);
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
            return std::move("p_partkey");
        } break;
        case 3: {
            int rnd = RandomGenerator::generate_random_int(1, 2);
            if(rnd == 1) return std::move("c_custkey");
            else return std::move("c_nationkey");
        } break;
        case 4: {
            int rnd = RandomGenerator::generate_random_int(1, 2);
            if(rnd == 1) return std::move("o_orderkey");
            else return std::move("o_custkey");
        } break;
        case 5: {
            int rnd = RandomGenerator::generate_random_int(1, 2);
            if(rnd == 1) return std::move("s_suppkey");
            else return std::move("s_nationkey");
        } break;
        case 6: {
            int rnd = RandomGenerator::generate_random_int(1, 2);
            if(rnd == 1) return std::move("ps_suppkey");
            else return std::move("ps_partkey");
        } break;
        case 7: {
            int rnd = RandomGenerator::generate_random_int(1, 2);
            if(rnd == 1) return std::move("l_suppkey");
            else return std::move("l_orderkey");
        } break;
        default:
        break;
    }
}

void ComparativeExp::get_join_cond(int left_tab_id, int right_tab_id, std::vector<Condition>& join_conds) {
    Condition cond;
    cond.is_rhs_val = false;
    cond.lhs_col = TabCol{.tab_name = tables[left_tab_id], .col_name = std::move(get_table_join_col(left_tab_id))};
    cond.rhs_col = TabCol{.tab_name = tables[right_tab_id], .col_name = std::move(get_table_join_col(right_tab_id))};
    cond.op = OP_EQ;
    join_conds.push_back(std::move(cond));
}

std::shared_ptr<Plan> ComparativeExp::generate_proj_plan(int tab_id, std::shared_ptr<Plan> scan_plan) {
    switch(tab_id) {
        case 0: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "region", .col_name = "r_regionkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "region", .col_name = "r_name"}));
            return std::make_shared<ProjectionPlan>(T_Projection, std::move(scan_plan), std::move(cols));
        } break;
        case 1: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_nationkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_regionkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_name"}));
            return std::make_shared<ProjectionPlan>(T_Projection, std::move(scan_plan), std::move(cols));
        } break;
        case 2: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "part", .col_name = "p_partkey"}));
            return std::make_shared<ProjectionPlan>(T_Projection, std::move(scan_plan), std::move(cols));
        } break;
        case 3: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "customer", .col_name = "c_custkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "customer", .col_name = "c_nationkey"}));
            return std::make_shared<ProjectionPlan>(T_Projection, std::move(scan_plan), std::move(cols));
        } break;
        case 4: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_orderkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_orderdate"}));
            cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_custkey"}));
            return std::make_shared<ProjectionPlan>(T_Projection, std::move(scan_plan), std::move(cols));
        } break;
        case 5: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "supplier", .col_name = "s_suppkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "supplier", .col_name = "s_nationkey"}));
            return std::make_shared<ProjectionPlan>(T_Projection, std::move(scan_plan), std::move(cols));
        } break;
        case 6: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "partsupp", .col_name = "ps_partkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "partsupp", .col_name = "ps_suppkey"}));
            return std::make_shared<ProjectionPlan>(T_Projection, std::move(scan_plan), std::move(cols));
        } break;
        case 7: {
            std::vector<TabCol> cols;
            cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_suppkey"}));
            cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_linenumber"}));
            cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_orderkey"}));
            return std::make_shared<ProjectionPlan>(T_Projection, std::move(scan_plan), std::move(cols));
        } break;
        default:
        break;
    }
    assert(0);
    return nullptr;
}

std::shared_ptr<Plan> ComparativeExp::generate_total_proj_plan(int table_num, std::shared_ptr<Plan> prev_plan) {
    std::vector<TabCol> cols;
    cols.push_back(std::move(TabCol{.tab_name = "region", .col_name = "r_regionkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "region", .col_name = "r_name"}));
    if(table_num == 1) goto FINAL_PROJ_PLAN;
    cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_nationkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_regionkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "nation", .col_name = "n_name"}));
    if(table_num == 2) goto FINAL_PROJ_PLAN;
    cols.push_back(std::move(TabCol{.tab_name = "part", .col_name = "p_partkey"}));
    if(table_num == 3) goto FINAL_PROJ_PLAN;
    cols.push_back(std::move(TabCol{.tab_name = "customer", .col_name = "c_custkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "customer", .col_name = "c_nationkey"}));
    if(table_num == 4) goto FINAL_PROJ_PLAN;
    cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_orderkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_orderdate"}));
    cols.push_back(std::move(TabCol{.tab_name = "orders", .col_name = "o_custkey"}));
    if(table_num == 5) goto FINAL_PROJ_PLAN;
    cols.push_back(std::move(TabCol{.tab_name = "supplier", .col_name = "s_suppkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "supplier", .col_name = "s_nationkey"}));
    if(table_num == 6) goto FINAL_PROJ_PLAN;
    cols.push_back(std::move(TabCol{.tab_name = "partsupp", .col_name = "ps_partkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "partsupp", .col_name = "ps_suppkey"}));
    if(table_num == 7) goto FINAL_PROJ_PLAN;
    cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_suppkey"}));
    cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_linenumber"}));
    cols.push_back(std::move(TabCol{.tab_name = "lineitem", .col_name = "l_orderkey"}));

FINAL_PROJ_PLAN:
    return std::make_shared<ProjectionPlan>(T_Projection, std::move(prev_plan), std::move(cols));
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

        // index_cond 和 filter cond 用generate_cond随机生成
        get_table_cond(i, filter_conds, index_conds);

        if(i == 0) {
            // 第一张表直接生成scan executor
            auto scan_plan = std::make_shared<ScanPlan>(T_IndexScan, curr_sql_id_, curr_plan_id_ ++, sm_mgr_, tables[i], filter_conds, index_conds);
            plan = generate_proj_plan(i, std::move(scan_plan));
        }
        else {
            // 后面的表直接放到join executor里
            //join_cond为当前表和随机选取前面已经join的表的固定join条件
            int left_join_table_id = RandomGenerator::generate_random_int(0, i - 1);
            get_join_cond(left_join_table_id, i, join_conds);
            
            auto scan_plan = std::make_shared<ScanPlan>(T_IndexScan, curr_sql_id_, curr_plan_id_ ++, sm_mgr_, tables[i], filter_conds, index_conds);
            auto proj_plan = generate_proj_plan(i, std::move(scan_plan));
            // 随机生成 hash join or nestedloop join
            int rnd = RandomGenerator::generate_random_int(1, 2);
            plan = std::make_shared<JoinPlan>(rnd == 1 ? T_HashJoin : T_NestLoop, curr_sql_id_, curr_plan_id_ ++, std::move(plan), std::move(proj_plan), join_conds);
        }
    }

    // 只有join_node_num_ > max_table_num的情况下才会进入下面的循环，因此所有的表都已经在当前的query tree出现过了
    for(int i = first_join_table_num; i <= join_node_num_; ++i) {
        if(i % 8 == 0) {
            // 每7个join算子进行一次projection，防止tuple长度无限扩张
            plan = generate_total_proj_plan(join_node_num_ + 1, std::move(plan));
        }
        std::vector<Condition> index_conds;
        std::vector<Condition> filter_conds;
        std::vector<Condition> join_conds;

        int right_tab_id = RandomGenerator::generate_random_int(0, max_table_num - 1);
        int left_tab_id = RandomGenerator::generate_random_int(0, max_table_num - 1);
        while(left_tab_id == right_tab_id) {
            left_tab_id = RandomGenerator::generate_random_int(0, max_table_num - 1);
        }

        get_table_cond(right_tab_id, filter_conds, index_conds);
        get_join_cond(left_tab_id, right_tab_id, join_conds);
        
        auto scan_plan = std::make_shared<ScanPlan>(T_IndexScan, curr_sql_id_, curr_plan_id_ ++, sm_mgr_, tables[right_tab_id], filter_conds, index_conds);
        auto proj_plan = generate_proj_plan(right_tab_id, std::move(scan_plan));
        int rnd = RandomGenerator::generate_random_int(1, 2);
        plan = std::make_shared<JoinPlan>(rnd == 1 ? T_HashJoin : T_NestLoop, curr_sql_id_, curr_plan_id_ ++, std::move(plan), std::move(proj_plan), join_conds);
    }

    // 最后再进行一次projection
    plan = generate_total_proj_plan(join_node_num_ + 1, std::move(plan));
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

int main(int argc, char** argv) {
    std::string config_path = "../src/config/benchmark_config.json";
    cJSON* cjson = parse_json_file(config_path);
    cJSON* comp_exp_config = cJSON_GetObjectItem(cjson, "comparative_exp");
    int join_num = cJSON_GetObjectItem(comp_exp_config, "join_num")->valueint;
    std::string cost_model = cJSON_GetObjectItem(comp_exp_config, "cost_model")->valuestring;
    node_type_ = cJSON_GetObjectItem(comp_exp_config, "node_type")->valueint;
    int buffer_pool_size = cJSON_GetObjectItem(comp_exp_config, "buffer_pool_size")->valueint;
    int thread_num = cJSON_GetObjectItem(comp_exp_config, "thread_num")->valueint;

    state_open_ = 0;
    state_theta_ = -1.0;
    src_scale_factor_ = 1000.0;
    block_size_ = 500;

    MetaManager::create_instance(config_path);
    RDMARegionAllocator::create_instance(MetaManager::get_instance(), 1);
    QPManager::create_instance(1);
    QPManager::BuildALLQPConnection(MetaManager::get_instance());
    ContextManager::create_instance(1);
    ComparativeExp* comparative_exp = new ComparativeExp(join_num, buffer_pool_size, thread_num);

    int connection_id = 0;
    CoroutineScheduler* coro_sched = new CoroutineScheduler(connection_id, CORO_NUM);
    auto local_rdma_region_range = RDMARegionAllocator::get_instance()->GetThreadLocalRegion(connection_id);
    RDMABufferAllocator* rdma_buffer_allocator = new RDMABufferAllocator(local_rdma_region_range.first, local_rdma_region_range.second);
    QPManager* qp_mgr = QPManager::get_instance();
    bool rdma_allocated = true;
    Transaction* txn = comparative_exp->txn_mgr_->get_transaction(connection_id);
    MetaManager* meta_mgr = MetaManager::get_instance();
    OperatorStateManager* op_state_manager = new OperatorStateManager(connection_id, coro_sched, meta_mgr, qp_mgr);
    char* result_str = new char[BUFFER_LENGTH];
    int offset = 0;
    Context* context = new Context(comparative_exp->lock_mgr_, comparative_exp->log_mgr_, txn, coro_sched, op_state_manager, qp_mgr, result_str, &offset, rdma_allocated);

    std::shared_ptr<Plan> plan = comparative_exp->generate_query_tree(context);
    std::shared_ptr<PortalStmt> portal_stmt = comparative_exp->portal_->start(plan, context);
    comparative_exp->portal_->run(portal_stmt, comparative_exp->ql_mgr_, context);

    print_char_array(result_str, offset);

    return 0;
}