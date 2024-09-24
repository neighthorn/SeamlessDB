#include <stack>

#include "resume_util.h"
#include "state_item/op_state.h"

std::shared_ptr<PortalStmt> rebuild_exec_plan_without_state(RWNode *node, Context *context, SQLState *sql_state) {
    std::shared_ptr<PortalStmt> portal_stmt;
    /*
        从sql重构exec plan
    */ 

    #ifdef TIME_OPEN
        auto recover_state_start = std::chrono::high_resolution_clock::now();
    #endif

    yyscan_t scanner;
    if(yylex_init(&scanner)) {
        std::cout << "failed to init scanner\n";
        exit(0);
    }
    YY_BUFFER_STATE buf = yy_scan_string(sql_state->sql.c_str(), scanner);
    if (yyparse(scanner) == 0) {
        if (ast::parse_tree != nullptr) {
            // analyze and rewrite
            std::shared_ptr<Query> query = node->analyze_->do_analyze(ast::parse_tree);
            yy_delete_buffer(buf, scanner);
            // 优化器
            node->optimizer_->set_planner_sql_id(sql_state->sql_id);
            std::shared_ptr<Plan> plan = node->optimizer_->plan_query(query, context);
            // portal
            portal_stmt = node->portal_->start(plan, context);
        }
    }

    #ifdef TIME_OPEN
        auto recover_state_end = std::chrono::high_resolution_clock::now();
        auto recover_state_period = std::chrono::duration_cast<std::chrono::microseconds>(recover_state_end - recover_state_start).count();
        std::cout << "time for recover state: " << recover_state_period << "\n";
        RwServerDebug::getInstance()->DEBUG_PRINT("[recover State][sql_id: " + std::to_string(sql_state->sql_id) + "][recover_state_period: " + std::to_string(recover_state_period) + "]");   
    #endif

    return portal_stmt;
}

void rebuild_exec_plan_with_query_tree(Context* context, std::shared_ptr<PortalStmt> portal_stmt, CheckPointMeta *op_ck_meta, std::vector<std::unique_ptr<OperatorState>> &op_checkpoints) {
    /*
        load checkpoint state
    */
    AbstractExecutor *need_to_begin_tuple = nullptr;
    {
        // up down load checkpoint state
        time_t latest_time = op_checkpoints[op_checkpoints.size() - 1]->op_state_time_;
        int last_checkpoint_index = op_checkpoints.size() - 1;
        auto exec_plan = portal_stmt->root.get();
        while(exec_plan != nullptr) {
            if(auto x = dynamic_cast<ProjectionExecutor *>(exec_plan)) {
                exec_plan = (x->prev_).get();
            } else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(exec_plan)) {
                RwServerDebug::getInstance()->DEBUG_PRINT("[REBUILD EXEC PLAN][BlockNestedLoopJoinExecutor][operator_id: " + std::to_string(x->operator_id_) + "]");
                /*
                    find the latest checkpoint
                */          
                bool find_match_checkpoint = false;  
                int  checkpoint_index = -1;
                for(int i = last_checkpoint_index; i >= 0; i--) {
                    if(op_checkpoints[i]->operator_id_ == x->operator_id_ && op_checkpoints[i]->op_state_time_ <= latest_time) {
                        find_match_checkpoint = true;
                        checkpoint_index = i;
                        break;
                    }
                }
                if(checkpoint_index == -1) {
                    std::cout << "[Warning]: Checkpoints Not Found! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
                    RwServerDebug::getInstance()->DEBUG_PRINT("[Warning]: Checkpoints Not Found! [Location]: " + std::string(__FILE__) + ":" + std::to_string(__LINE__));
                    std::cout << "operator_id: " << x->operator_id_ << std::endl;
                    break ;
                    /*
                        没有找到checkpoint，即需要从改exec_plan开始，需要手动执行beginTuple
                    */
                }
                last_checkpoint_index = checkpoint_index;

                RwServerDebug::getInstance()->DEBUG_PRINT("[REBUILD EXEC PLAN][BlockNestedLoopJoinExecutor][checkpoint_index: " + std::to_string(checkpoint_index) + "][operator_id: " + std::to_string(op_checkpoints[checkpoint_index]->operator_id_) + "][op_state_time: " + std::to_string(op_checkpoints[checkpoint_index]->op_state_time_) + "][op_state_size: " + std::to_string(op_checkpoints[checkpoint_index]->op_state_size_) + "]");
                latest_time = op_checkpoints[checkpoint_index]->op_state_time_;
                /*
                    恢复算子状态
                */
               std::cout << "recover operator " << x->operator_id_ << std::endl;
                std::unique_ptr<BlockJoinOperatorState> block_join_op_state = std::make_unique<BlockJoinOperatorState>();
                block_join_op_state->deserialize(op_checkpoints[checkpoint_index]->op_state_addr_, op_checkpoints[checkpoint_index]->op_state_size_);
                x->load_state_info(block_join_op_state.get());

                RwServerDebug::getInstance()->DEBUG_PRINT("[RECOVER EXEC PLAN][BlockNestedLoopJoinExecutor][operator_id: " + std::to_string(x->operator_id_) + "][current block id " + std::to_string(x->left_blocks_->current_block_id_) + "][be_call_time: " + std::to_string(x->be_call_times_) + "][left child call times: " + std::to_string(x->left_child_call_times_) + "]");
                /*
                    检查左儿子节点
                */
                if(auto left_child = dynamic_cast<BlockNestedLoopJoinExecutor *>(x->left_.get())) {
                    exec_plan = left_child;
                } 
                else if(auto left_child = dynamic_cast<HashJoinExecutor *>(x->left_.get())) {
                    exec_plan = left_child;
                }
                else {
                    exec_plan = nullptr;
                }
            }
            else if(auto x = dynamic_cast<HashJoinExecutor *>(exec_plan)) {
                // 对于hash join来说，由于是增量记录状态，因此，除了找到最新的检查点之外，还需要找到之前所有的包含hash_table的检查点来重构hash_table
                RwServerDebug::getInstance()->DEBUG_PRINT("[REBUILD EXEC PLAN][HashJoinExecutor][operator_id: " + std::to_string(x->operator_id_) + "]");
                // find latest checkpoint   
                bool find_match_checkpoint = false;
                int checkpoint_index = -1;
                for(int i = last_checkpoint_index; i >= 0; i--) {
                    if(op_checkpoints[i]->operator_id_ == x->operator_id_ && op_checkpoints[i]->op_state_time_ <= latest_time) {
                        find_match_checkpoint = true;
                        checkpoint_index = i;
                        break;
                    }
                }
                if(checkpoint_index == -1) {
                    std::cout << "[Warning]: Checkpoints Not Found! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
                    RwServerDebug::getInstance()->DEBUG_PRINT("[Warning]: Checkpoints Not Found! [Location]: " + std::string(__FILE__) + ":" + std::to_string(__LINE__));
                    break ;
                    /*
                        没有找到checkpoint，即需要从改exec_plan开始，需要手动执行beginTuple
                    */
                }
                last_checkpoint_index = checkpoint_index;

                RwServerDebug::getInstance()->DEBUG_PRINT("[REBUILD EXEC PLAN][HashJoinExecutor][checkpoint_index: " + std::to_string(checkpoint_index) + "][operator_id: " + std::to_string(op_checkpoints[checkpoint_index]->operator_id_) + "][op_state_time: " + std::to_string(op_checkpoints[checkpoint_index]->op_state_time_) + "][op_state_size: " + std::to_string(op_checkpoints[checkpoint_index]->op_state_size_) + "]");
                latest_time = op_checkpoints[checkpoint_index]->op_state_time_;

                // 首先找到所有包含hash_table的检查点，然后重构hash_table
                for(int i = 0; i <= last_checkpoint_index; ++i) {
                    // 通过hash_table_contained_变量来判断是否包含hash_table的数据
                    if(op_checkpoints[i]->operator_id_ == x->operator_id_ && *reinterpret_cast<bool*>(op_checkpoints[i]->op_state_addr_ + OPERATOR_STATE_HEADER_SIZE) == true) {
                        // op_checkpoints[i]->rebuild_hash_table(x);
                        std::unique_ptr<HashJoinOperatorState> hash_join_op_state = std::make_unique<HashJoinOperatorState>();
                        hash_join_op_state->deserialize(op_checkpoints[i]->op_state_addr_, op_checkpoints[i]->op_state_size_);
                        hash_join_op_state->rebuild_hash_table(x, op_checkpoints[i]->op_state_addr_, op_checkpoints[i]->op_state_size_);
                    }
                    else if(op_checkpoints[i]->operator_id_ == x->operator_id_) {
                        // 不包含hash table的相关数据，则后面的检查点也不包含
                        break;
                    }
                }

                // 使用最后一个检查点恢复除hash table之外的其他状态信息
                std::unique_ptr<HashJoinOperatorState> hash_join_op_state = std::make_unique<HashJoinOperatorState>();
                hash_join_op_state->deserialize(op_checkpoints[checkpoint_index]->op_state_addr_, op_checkpoints[checkpoint_index]->op_state_size_);
                x->load_state_info(hash_join_op_state.get());

                RwServerDebug::getInstance()->DEBUG_PRINT("[RECOVER EXEC PLAN][HashJoinExecutor][operator_id: " + std::to_string(x->operator_id_) + "][be_call_time: " + std::to_string(x->be_call_times_) + "][left child call times: " + std::to_string(x->left_child_call_times_) + "]");
                if(auto left_child = dynamic_cast<BlockNestedLoopJoinExecutor *>(x->left_.get())) {
                    exec_plan = left_child;
                }
                else if(auto left_child = dynamic_cast<HashJoinExecutor *>(x->left_.get())) {
                    exec_plan = left_child;
                }
                else {
                    exec_plan = nullptr;
                }
            }
            else {
                exec_plan = nullptr;
            }
        }

        need_to_begin_tuple = exec_plan;
    }

    #ifdef TIME_OPEN
        auto recover_state_end = std::chrono::high_resolution_clock::now();
        auto recover_state_period = std::chrono::duration_cast<std::chrono::microseconds>(recover_state_end - recover_state_start).count();
        std::cout << "time for recover state: " << recover_state_period << "\n";   
        RwServerDebug::getInstance()->DEBUG_PRINT("[time for recover state: " + std::to_string(recover_state_period) + "]");
    #endif

    /*
        从需要beginTuple的节点开始，手动执行beginTuple
    */
    if(need_to_begin_tuple != nullptr) {
        need_to_begin_tuple->beginTuple();
    }

    /*
        所有算子恢复到一致性的状态(自底向上)
    */

    #ifdef TIME_OPEN
        auto recover_consistency_start = std::chrono::high_resolution_clock::now();
    #endif
    
    {
        std::stack<BlockNestedLoopJoinExecutor *> block_ops_;
        auto exec_plan = portal_stmt->root.get();
        while(exec_plan != nullptr) {
            if(auto x = dynamic_cast<ProjectionExecutor *>(exec_plan)) {
                exec_plan = (x->prev_).get();
                continue;
            } else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(exec_plan)) {
                block_ops_.push(x);

                if(auto left_child = dynamic_cast<BlockNestedLoopJoinExecutor *>(x->left_.get())) {
                    exec_plan = left_child;
                } else {
                    exec_plan = nullptr;
                }
            } else {
                exec_plan = nullptr;
            }
        }

        while(block_ops_.size() > 1) {
            auto child = block_ops_.top();
            block_ops_.pop();
            auto father = block_ops_.top();
            /*
                重做
            */
            while(child->be_call_times_ < father->left_child_call_times_) {
                child->nextTuple();
            }
        }
    }

    #ifdef TIME_OPEN
        auto recover_consistency_end = std::chrono::high_resolution_clock::now();
        auto recover_consistency_period = std::chrono::duration_cast<std::chrono::microseconds>(recover_consistency_end - recover_consistency_start).count();
        std::cout << "time for recover consistency: " << recover_consistency_period << "\n";
        RwServerDebug::getInstance()->DEBUG_PRINT("[time for recover consistency: " + std::to_string(recover_consistency_period) + "]");
    #endif
    /*
        检查是否恢复到一致性状态
    */
    {
        auto exec_plan = portal_stmt->root.get();
        while(exec_plan != nullptr) {
            if(auto x = dynamic_cast<ProjectionExecutor *>(exec_plan)) {
                exec_plan = (x->prev_).get();
                continue;
            } else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(exec_plan)) {
                std::cout << "Be call times: " << x->be_call_times_ << ", left child call times: " << x->left_child_call_times_ << "\n";
                RwServerDebug::getInstance()->DEBUG_PRINT("[be call times:" + std::to_string(x->be_call_times_) + "][left child call times: " + std::to_string(x->left_child_call_times_) + "]");
                if(auto left_child = dynamic_cast<BlockNestedLoopJoinExecutor *>(x->left_.get())) {
                    exec_plan = left_child;
                } else {
                    exec_plan = nullptr;
                }
            } else {
                exec_plan = nullptr;
            }
        }
    }    
}

/*
    恢复操作符状态
*/
std::shared_ptr<PortalStmt> rebuild_exec_plan_from_state(RWNode *node, Context *context, SQLState *sql_state, CheckPointMeta *op_ck_meta, std::vector<std::unique_ptr<OperatorState>> &op_checkpoints) {
   
    std::shared_ptr<PortalStmt> portal_stmt;
    /*
        从sql重构exec plan
    */ 

    #ifdef TIME_OPEN
        auto recover_state_start = std::chrono::high_resolution_clock::now();
    #endif

    yyscan_t scanner;
    if(yylex_init(&scanner)) {
        std::cout << "failed to init scanner\n";
        exit(0);
    }
    YY_BUFFER_STATE buf = yy_scan_string(sql_state->sql.c_str(), scanner);
    if (yyparse(scanner) == 0) {
        if (ast::parse_tree != nullptr) {
            // analyze and rewrite
            std::shared_ptr<Query> query = node->analyze_->do_analyze(ast::parse_tree);
            yy_delete_buffer(buf, scanner);
            // 优化器
            node->optimizer_->set_planner_sql_id(sql_state->sql_id);
            std::shared_ptr<Plan> plan = node->optimizer_->plan_query(query, context);
            // portal
            portal_stmt = node->portal_->start(plan, context);
        }
    }

    rebuild_exec_plan_with_query_tree(context, portal_stmt, op_ck_meta, op_checkpoints);
    return portal_stmt;
}