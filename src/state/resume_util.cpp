#include <stack>

#include "resume_util.h"
#include "state_item/op_state.h"
#include "execution/comp_ckpt_mgr.h"

#define TIME_OPEN 1

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

/**
 * @description: 恢复以exec_plan为根节点的查询树中每个算子的状态
 * @param {AbstractExecutor*} exec_plan: 算子树根节点
 * @param {AbstractExecutor*} need_to_begin_tuple: 需要手动执行beginTuple的左子树（可以为nullptr）
 * @note 这里只主动对左子树进行beginTuple，如果某个右儿子需要beginTuple，由其父亲节点在一致性状态恢复时负责
 * @param {AbstractExecutor*} first_ckpt_op: 整个算子树中第一个恢复了状态的算子
 * @param {std::vector<std::unique_ptr<OperatorState>> &} op_checkpoints: 所有算子状态检查点
 * @param {int} last_checkpoint_index: 当前以exec_plan为root的子算子树中的，可以恢复的检查点索引不能超过last_checkpoint_index
 * @param {time_t} latest_time: 当前以exec_plan为root的子算子树中的，可以恢复的检查点时间不能超过latest_time
 */
void recover_query_tree_state(AbstractExecutor* exec_plan, AbstractExecutor*& need_to_begin_tuple, bool find_begin, AbstractExecutor*& first_ckpt_op, 
    const std::vector<std::unique_ptr<OperatorState>>& op_checkpoints, int last_checkpoint_index, time_t latest_time) {
    
    if(exec_plan == nullptr) return;

    if(cost_model_ == 2) {
        // 如果是周期性创建检查点，那么每个算子都需要恢复到最新的检查点
        last_checkpoint_index = op_checkpoints.size() - 1;
        latest_time = op_checkpoints[op_checkpoints.size() - 1]->op_state_time_;
    }

    if(auto x = dynamic_cast<ProjectionExecutor *>(exec_plan)) {
        if(x->is_root_) {
            int  checkpoint_index = -1;
            for(int i = last_checkpoint_index; i >= 0; i--) {
                if(op_checkpoints[i]->operator_id_ == x->operator_id_ && op_checkpoints[i]->op_state_time_ <= latest_time) {
                    checkpoint_index = i;
                    break;
                }
            }
            if(checkpoint_index == -1) {
                std::cout << "[Warning]: Checkpoints Not Found! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
                RwServerDebug::getInstance()->DEBUG_PRINT("[Warning]: Checkpoints Not Found! [Location]: " + std::string(__FILE__) + ":" + std::to_string(__LINE__));
                std::cout << "operator_id: " << x->operator_id_ << std::endl;
                exec_plan = (x->prev_).get();
                recover_query_tree_state(exec_plan, need_to_begin_tuple, true, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);
                return;
            }
            last_checkpoint_index = checkpoint_index;
            latest_time = op_checkpoints[checkpoint_index]->op_state_time_;
            if(first_ckpt_op == nullptr) {
                first_ckpt_op = x;
            }

            std::unique_ptr<ProjectionOperatorState> projection_op_state = std::make_unique<ProjectionOperatorState>();
            projection_op_state->deserialize(op_checkpoints[checkpoint_index]->op_state_addr_, op_checkpoints[checkpoint_index]->op_state_size_);
            x->load_state_info(projection_op_state.get());
            exec_plan = (x->prev_).get();
            recover_query_tree_state(exec_plan, need_to_begin_tuple, true, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);
            return;
        }
        else {
            exec_plan = (x->prev_).get();
            recover_query_tree_state(exec_plan, need_to_begin_tuple, true, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);
            return;
        }
    } else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(exec_plan)) {
        RwServerDebug::getInstance()->DEBUG_PRINT("[REBUILD EXEC PLAN][BlockNestedLoopJoinExecutor][operator_id: " + std::to_string(x->operator_id_) + "]");
        /*
            find the latest checkpoint
        */          
        // bool find_match_checkpoint = false;  
        int  checkpoint_index = -1;
        for(int i = last_checkpoint_index; i >= 0; i--) {
            if(op_checkpoints[i]->operator_id_ == x->operator_id_ && op_checkpoints[i]->op_state_time_ <= latest_time) {
                // find_match_checkpoint = true;
                checkpoint_index = i;
                break;
            }
        }
        if(checkpoint_index == -1) {
            std::cout << "[Warning]: Checkpoints Not Found! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
            RwServerDebug::getInstance()->DEBUG_PRINT("[Warning]: Checkpoints Not Found! [Location]: " + std::string(__FILE__) + ":" + std::to_string(__LINE__));
            std::cout << "operator_id: " << x->operator_id_ << std::endl;
            
            if(first_ckpt_op != nullptr) {
                if(find_begin) need_to_begin_tuple = exec_plan;
                return;
            }
            else {
                // 左右儿子都可能是stateful算子
                AbstractExecutor* left_exec_plan = nullptr;
                AbstractExecutor* right_exec_plan = nullptr;

                if(auto left_child = dynamic_cast<BlockNestedLoopJoinExecutor *>(x->left_.get())) {
                    left_exec_plan = left_child;
                } 
                else if(auto left_child = dynamic_cast<HashJoinExecutor *>(x->left_.get())) {
                    left_exec_plan = left_child;
                }
                else if(auto left_child = dynamic_cast<ProjectionExecutor *>(x->left_.get())) {
                    left_exec_plan = left_child;
                }
                else if(auto left_child = dynamic_cast<GatherExecutor *>(x->left_.get())) {
                    left_exec_plan = left_child;
                }
                else {
                    left_exec_plan = nullptr;
                }
                // 将左儿子的状态恢复到不超过last_checkpoint_index的最新检查点
                recover_query_tree_state(left_exec_plan, need_to_begin_tuple, true, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);
                
                if(auto right_child = dynamic_cast<GatherExecutor *>(x->right_.get())) {
                    right_exec_plan = right_child;
                }
                else {
                    right_exec_plan = nullptr;
                }
                // 将右儿子的状态恢复到不超过last_checkpoint_index的最新检查点
                recover_query_tree_state(right_exec_plan, need_to_begin_tuple, false, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);
                return;
            }
            
            /*
                没有找到checkpoint，即需要从改exec_plan开始，需要手动执行beginTuple
            */
        }
        last_checkpoint_index = checkpoint_index;

        RwServerDebug::getInstance()->DEBUG_PRINT("[REBUILD EXEC PLAN][BlockNestedLoopJoinExecutor][checkpoint_index: " + std::to_string(checkpoint_index) + "][operator_id: " + std::to_string(op_checkpoints[checkpoint_index]->operator_id_) + "][op_state_time: " + std::to_string(op_checkpoints[checkpoint_index]->op_state_time_) + "][op_state_size: " + std::to_string(op_checkpoints[checkpoint_index]->op_state_size_) + "]");
        latest_time = op_checkpoints[checkpoint_index]->op_state_time_;
        if(first_ckpt_op == nullptr) {
            first_ckpt_op = x;
        }
        /*
            恢复算子状态
        */
        std::cout << "recover operator " << x->operator_id_ << std::endl;
        std::unique_ptr<BlockJoinOperatorState> block_join_op_state = std::make_unique<BlockJoinOperatorState>();
        block_join_op_state->deserialize(op_checkpoints[checkpoint_index]->op_state_addr_, op_checkpoints[checkpoint_index]->op_state_size_);
        x->load_state_info(block_join_op_state.get());

        RwServerDebug::getInstance()->DEBUG_PRINT("[RECOVER EXEC PLAN][BlockNestedLoopJoinExecutor][operator_id: " + std::to_string(x->operator_id_) + "][current block id " + std::to_string(x->left_blocks_->current_block_id_) + "][be_call_time: " + std::to_string(x->be_call_times_) + "][left child call times: " + std::to_string(x->left_child_call_times_) + "]");
        
        AbstractExecutor* left_child_exec = nullptr;
        AbstractExecutor* right_child_exec = nullptr;

        if(auto left_child = dynamic_cast<BlockNestedLoopJoinExecutor *>(x->left_.get())) {
            left_child_exec = left_child;
        } 
        else if(auto left_child = dynamic_cast<HashJoinExecutor *>(x->left_.get())) {
            left_child_exec = left_child;
        }
        else if(auto left_child = dynamic_cast<ProjectionExecutor *>(x->left_.get())) {
            left_child_exec = left_child;
        }
        else if(auto left_child = dynamic_cast<GatherExecutor *>(x->left_.get())) {
            left_child_exec = left_child;
        }
        else {
            left_child_exec = nullptr;
        }
        recover_query_tree_state(left_child_exec, need_to_begin_tuple, true, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);

        if(auto right_child = dynamic_cast<GatherExecutor *>(x->right_.get())) {
            right_child_exec = right_child;
        }
        else {
            right_child_exec = nullptr;
        }
        recover_query_tree_state(right_child_exec, need_to_begin_tuple, false, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);
        return;
    } else if(auto x = dynamic_cast<HashJoinExecutor *>(exec_plan)) {
        // 对于hash join来说，由于是增量记录状态，因此，除了找到最新的检查点之外，还需要找到之前所有的包含hash_table的检查点来重构hash_table
        RwServerDebug::getInstance()->DEBUG_PRINT("[REBUILD EXEC PLAN][HashJoinExecutor][operator_id: " + std::to_string(x->operator_id_) + "]");
        // find latest checkpoint   
        // bool find_match_checkpoint = false;
        int checkpoint_index = -1;
        for(int i = last_checkpoint_index; i >= 0; i--) {
            if(op_checkpoints[i]->operator_id_ == x->operator_id_ && op_checkpoints[i]->op_state_time_ <= latest_time) {
                // find_match_checkpoint = true;
                checkpoint_index = i;
                std::cout << "HashJoinOperator, op_id: " << x->operator_id_ << ", checkpoint index: " << checkpoint_index << "  " << __FILE__ << ":" << __LINE__ << std::endl;
                break;
            }
        }
        if(checkpoint_index == -1) {
            std::cout << "HashJoinOperator, op_id: " << x->operator_id_ << ", [Warning]: Checkpoints Not Found! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
            RwServerDebug::getInstance()->DEBUG_PRINT("[Warning]: Checkpoints Not Found! [Location]: " + std::string(__FILE__) + ":" + std::to_string(__LINE__));
            
            if(first_ckpt_op != nullptr) {
                if(find_begin) need_to_begin_tuple = exec_plan;
                return;
            }
            else {
                // 左右儿子都可能是stateful算子
                AbstractExecutor* left_exec_plan = nullptr;
                AbstractExecutor* right_exec_plan = nullptr;

                if(auto left_child = dynamic_cast<BlockNestedLoopJoinExecutor *>(x->left_.get())) {
                    left_exec_plan = left_child;
                } 
                else if(auto left_child = dynamic_cast<HashJoinExecutor *>(x->left_.get())) {
                    left_exec_plan = left_child;
                }
                else if(auto left_child = dynamic_cast<ProjectionExecutor *>(x->left_.get())) {
                    left_exec_plan = left_child;
                }
                else if(auto left_child = dynamic_cast<GatherExecutor *>(x->left_.get())) {
                    left_exec_plan = left_child;
                }
                else {
                    left_exec_plan = nullptr;
                }
                // 将左儿子的状态恢复到不超过last_checkpoint_index的最新检查点
                recover_query_tree_state(left_exec_plan, need_to_begin_tuple, true, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);
                
                if(auto right_child = dynamic_cast<GatherExecutor *>(x->right_.get())) {
                    right_exec_plan = right_child;
                }
                else {
                    right_exec_plan = nullptr;
                }
                // 将右儿子的状态恢复到不超过last_checkpoint_index的最新检查点
                recover_query_tree_state(right_exec_plan, need_to_begin_tuple, false, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);
                return;
            }
        }
        last_checkpoint_index = checkpoint_index;

        // RwServerDebug::getInstance()->DEBUG_PRINT("[REBUILD EXEC PLAN][HashJoinExecutor][checkpoint_index: " + std::to_string(checkpoint_index) + "][operator_id: " + std::to_string(op_checkpoints[checkpoint_index]->operator_id_) + "][op_state_time: " + std::to_string(op_checkpoints[checkpoint_index]->op_state_time_) + "][op_state_size: " + std::to_string(op_checkpoints[checkpoint_index]->op_state_size_) + "]");
        latest_time = op_checkpoints[checkpoint_index]->op_state_time_;
        if(first_ckpt_op == nullptr) {
            first_ckpt_op = x;
        }

        // 首先找到所有包含hash_table的检查点，然后重构hash_table
        int i = 0;
        if(cost_model_ == 2) i = last_checkpoint_index;
        for(; i <= last_checkpoint_index; ++i) {
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
        
        AbstractExecutor* left_child_exec = nullptr;
        AbstractExecutor* right_child_exec = nullptr;

        if(auto left_child = dynamic_cast<BlockNestedLoopJoinExecutor *>(x->left_.get())) {
            left_child_exec = left_child;
        }
        else if(auto left_child = dynamic_cast<HashJoinExecutor *>(x->left_.get())) {
            left_child_exec = left_child;
        }
        else if(auto left_child = dynamic_cast<ProjectionExecutor *>(x->left_.get())) {
            left_child_exec = left_child;
        }
        else if(auto left_child = dynamic_cast<GatherExecutor *>(x->left_.get())) {
            left_child_exec = left_child;
        }
        else {
            left_child_exec = nullptr;
        }
        recover_query_tree_state(left_child_exec, need_to_begin_tuple, true, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);

        if(auto right_child = dynamic_cast<GatherExecutor *>(x->right_.get())) {
            right_child_exec = right_child;
        }
        else {
            right_child_exec = nullptr;
        }
        recover_query_tree_state(right_child_exec, need_to_begin_tuple, false, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);
        return;
    } else if(auto x = dynamic_cast<SortExecutor *>(exec_plan)) {
        // RwServerDebug::getInstance()->DEBUG_PRINT("[REBUILD EXEC PLAN][SortExecutor][operator_id: " + std::to_string(x->operator_id_) + "][be_call_time: " + std::to_string(x->be_call_times_) + "][left child call times: " + std::to_string(x->left_child_call_times_) + "]");
        // bool find_match_checkpoint = false;
        int checkpoint_index = -1;
        for(int i = last_checkpoint_index; i >= 0; i--) {
            if(op_checkpoints[i]->operator_id_ == x->operator_id_ && op_checkpoints[i]->op_state_time_ <= latest_time) {
                // find_match_checkpoint = true;
                checkpoint_index = i;
                std::cout << "SortOperator, op_id: " << x->operator_id_ << ", checkpoint index: " << checkpoint_index << "  " << __FILE__ << ":" << __LINE__ << std::endl;
                break;
            }
        }
        if(checkpoint_index == -1) {
            std::cout << "[Warning]: Checkpoints Not Found! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
            RwServerDebug::getInstance()->DEBUG_PRINT("[Warning]: Checkpoints Not Found! [Location]: " + std::string(__FILE__) + ":" + std::to_string(__LINE__));
            
            if(first_ckpt_op != nullptr) {
                if(find_begin) need_to_begin_tuple = exec_plan;
                return;
            }
            else {
                if(auto left_child = dynamic_cast<BlockNestedLoopJoinExecutor *>(x->prev_.get())) {
                    exec_plan = left_child;
                } 
                else if(auto left_child = dynamic_cast<HashJoinExecutor *>(x->prev_.get())) {
                    exec_plan = left_child;
                }
                else if(auto left_child = dynamic_cast<ProjectionExecutor *>(x->prev_.get())) {
                    exec_plan = left_child;
                }
                else if(auto left_child = dynamic_cast<GatherExecutor *>(x->prev_.get())) {
                    exec_plan = left_child;
                }
                else {
                    exec_plan = nullptr;
                }
                recover_query_tree_state(exec_plan, need_to_begin_tuple, true, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);
                return;
            }
            /*
                没有找到checkpoint，即需要从改exec_plan开始，需要手动执行beginTuple
            */
        }
        last_checkpoint_index = checkpoint_index;

        RwServerDebug::getInstance()->DEBUG_PRINT("[REBUILD EXEC PLAN][SortExecutor][checkpoint_index: " + std::to_string(checkpoint_index) + "][operator_id: " + std::to_string(op_checkpoints[checkpoint_index]->operator_id_) + "][op_state_time: " + std::to_string(op_checkpoints[checkpoint_index]->op_state_time_) + "][op_state_size: " + std::to_string(op_checkpoints[checkpoint_index]->op_state_size_) + "]");
        latest_time = op_checkpoints[checkpoint_index]->op_state_time_;
        if(first_ckpt_op == nullptr) {
            first_ckpt_op = x;
        }

        int i = 0;
        if(cost_model_ == 2) i = last_checkpoint_index;
        // 找到所有包含unsorted_tuple的检查点，重构unsorted_tuple
        for(; i <= last_checkpoint_index; ++i) {
            if(op_checkpoints[i]->operator_id_ == x->operator_id_ && *reinterpret_cast<int*>(op_checkpoints[i]->op_state_addr_ + OPERATOR_STATE_HEADER_SIZE + OFF_SORT_UNSORTED_TUPLE_CNT) > 0) {
                std::unique_ptr<SortOperatorState> sort_op_state = std::make_unique<SortOperatorState>();
                sort_op_state->deserialize(op_checkpoints[i]->op_state_addr_, op_checkpoints[i]->op_state_size_);
                sort_op_state->rebuild_sort_records(x, op_checkpoints[i]->op_state_addr_, op_checkpoints[i]->op_state_size_);
            }
            // 如果存在sort_index，则反序列化sorted_index
            if(op_checkpoints[i]->operator_id_ == x->operator_id_ && *reinterpret_cast<bool*>(op_checkpoints[i]->op_state_addr_ + OPERATOR_STATE_HEADER_SIZE + OFF_SORT_IS_SORTED_INDEX_CKPT) == true) {
                // x->sorted_index_ = reinterpret_cast<int*>(op_checkpoints[i]->op_state_addr_ + OPERATOR_STATE_HEADER_SIZE + OFF_SORT_IS_SORTED_INDEX_CKPT);
                std::unique_ptr<SortOperatorState> sort_op_state = std::make_unique<SortOperatorState>();
                sort_op_state->deserialize(op_checkpoints[i]->op_state_addr_, op_checkpoints[i]->op_state_size_);
                sort_op_state->rebuild_sort_index(x, op_checkpoints[i]->op_state_addr_, op_checkpoints[i]->op_state_size_);
            }
        }

        // 使用最后一个检查点恢复除unsorted_tuples和sorted_index之外的其他状态信息
        std::unique_ptr<SortOperatorState> sort_op_state = std::make_unique<SortOperatorState>();
        sort_op_state->deserialize(op_checkpoints[checkpoint_index]->op_state_addr_, op_checkpoints[checkpoint_index]->op_state_size_);
        x->load_state_info(sort_op_state.get());

        RwServerDebug::getInstance()->DEBUG_PRINT("[RECOVER EXEC PLAN][SortExecutor][operator_id: " + std::to_string(x->operator_id_) + "][be_call_time: " + std::to_string(x->be_call_times_) + "][left child call times: " + std::to_string(x->left_child_call_times_));
        if(auto left_child = dynamic_cast<BlockNestedLoopJoinExecutor *>(x->prev_.get())) {
            exec_plan = left_child;
        }
        else if(auto left_child = dynamic_cast<HashJoinExecutor *>(x->prev_.get())) {
            exec_plan = left_child;
        }
        else if(auto left_child = dynamic_cast<ProjectionExecutor *>(x->prev_.get())) {
            exec_plan = left_child;
        }
        else if(auto left_child = dynamic_cast<GatherExecutor *>(x->prev_.get())) {
            exec_plan = left_child;
        }
        else {
            exec_plan = nullptr;
        }
        recover_query_tree_state(exec_plan, need_to_begin_tuple, true, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);
        return;
    } else if(auto x = dynamic_cast<GatherExecutor *>(exec_plan)) {
        RwServerDebug::getInstance()->DEBUG_PRINT("[REBUILD EXEC PLAN][GatherExecutor][operator_id: " + std::to_string(x->operator_id_) + "]");
        // bool find_match_checkpoint = false;
        int checkpoint_index = -1;
        for(int i = last_checkpoint_index; i >= 0; i--) {
            if(op_checkpoints[i]->operator_id_ == x->operator_id_ && op_checkpoints[i]->op_state_time_ <= latest_time) {
                // find_match_checkpoint = true;
                checkpoint_index = i;
                std::cout << "GatherOperator, op_id: " << x->operator_id_ << ", checkpoint index: " << checkpoint_index << "  " << __FILE__ << ":" << __LINE__ << std::endl;
                break;
            }
        }

        if(checkpoint_index == -1) {
            RwServerDebug::getInstance()->DEBUG_PRINT("[Warning]: Checkpoints Not Found! [GatherOperator] [op_id]: " + x->operator_id_);

            if(first_ckpt_op != nullptr) {
                if(find_begin) need_to_begin_tuple = exec_plan;
                return;
            }
            else {
                // TODO: 这里需要根据具体的operator类型来判断，暂时因为只有IndexScan所以不需要做任何处理，直接return
                return;
            }
        }

        last_checkpoint_index = checkpoint_index;
        latest_time = op_checkpoints[checkpoint_index]->op_state_time_;
        RwServerDebug::getInstance()->DEBUG_PRINT("[REBUILD EXEC PLAN][GatherExecutor][checkpoint_index: " + std::to_string(checkpoint_index) + "][operator_id: " + std::to_string(op_checkpoints[checkpoint_index]->operator_id_) + "][op_state_time: " + std::to_string(op_checkpoints[checkpoint_index]->op_state_time_) + "][op_state_size: " + std::to_string(op_checkpoints[checkpoint_index]->op_state_size_) + "]");
        if(first_ckpt_op == nullptr) {
            first_ckpt_op = x;
        }

        std::unique_ptr<GatherOperatorState> gather_op_state = std::make_unique<GatherOperatorState>();
        gather_op_state->deserialize(op_checkpoints[checkpoint_index]->op_state_addr_, op_checkpoints[checkpoint_index]->op_state_size_);
        gather_op_state->rebuild_result_queues(x, op_checkpoints[checkpoint_index]->op_state_addr_, op_checkpoints[checkpoint_index]->op_state_size_);
        x->load_state_info(gather_op_state.get());

        RwServerDebug::getInstance()->DEBUG_PRINT("[RECOVER EXEC PLAN][GatherExecutor][operator_id: " + std::to_string(x->operator_id_) + "][be_call_time: " + std::to_string(x->be_call_times_) + "][left child call times: " + std::to_string(x->left_child_call_times_));

        for(int i = 0; i < x->worker_thread_num_; ++i) {
            /**
             * @todo: The code here is not completely right.
             * we can run correctly due to the fact that workers can only be IndexScanExecutor operators, so they do not need to be recovered
             */
            if(auto child = dynamic_cast<BlockNestedLoopJoinExecutor *>(x->workers_[i].get())) {
                exec_plan = child;
            }
            else if(auto child = dynamic_cast<HashJoinExecutor *>(x->workers_[i].get())) {
                exec_plan = child;
            }
            else if(auto child = dynamic_cast<ProjectionExecutor *>(x->workers_[i].get())) {
                exec_plan = child;
            }
            else if(auto child = dynamic_cast<GatherExecutor *>(x->workers_[i].get())) {
                exec_plan = child;
            }
            else {
                exec_plan = nullptr;
            }
            if(exec_plan != nullptr)
                recover_query_tree_state(exec_plan, need_to_begin_tuple, true, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);
        }
        return;
    } else {
        exec_plan = nullptr;
    }

    return;
}

void rebuild_exec_plan_with_query_tree(Context* context, std::shared_ptr<PortalStmt> portal_stmt, CheckPointMeta *op_ck_meta, std::vector<std::unique_ptr<OperatorState>> &op_checkpoints) {
    #ifdef TIME_OPEN
        auto recover_state_start = std::chrono::high_resolution_clock::now();
    #endif
    /*
        load checkpoint state
    */
    AbstractExecutor *need_to_begin_tuple = nullptr;    // 这是需要beginTuple的非root节点
    AbstractExecutor *first_ckpt_op = nullptr;      // 找到第一个有检查点的算子，作为根节点去恢复一致性状态
    {
        // from top to bottom load checkpoint state
        time_t latest_time = op_checkpoints[op_checkpoints.size() - 1]->op_state_time_;
        int last_checkpoint_index = op_checkpoints.size() - 1;
        auto exec_plan = portal_stmt->root.get();
        recover_query_tree_state(exec_plan, need_to_begin_tuple, true, first_ckpt_op, op_checkpoints, last_checkpoint_index, latest_time);
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
   recover_exec_plan_to_consistent_state(context, first_ckpt_op, first_ckpt_op->be_call_times_);
    

    #ifdef TIME_OPEN
        auto recover_consistency_end = std::chrono::high_resolution_clock::now();
        auto recover_consistency_period = std::chrono::duration_cast<std::chrono::microseconds>(recover_consistency_end - recover_consistency_start).count();
        std::cout << "time for recover consistency: " << recover_consistency_period << "\n";
        RwServerDebug::getInstance()->DEBUG_PRINT("[time for recover consistency: " + std::to_string(recover_consistency_period) + "]");
    #endif
    /*
        检查是否恢复到一致性状态
    */
    // {
    //     auto exec_plan = portal_stmt->root.get();
    //     while(exec_plan != nullptr) {
    //         if(auto x = dynamic_cast<ProjectionExecutor *>(exec_plan)) {
    //             exec_plan = (x->prev_).get();
    //             continue;
    //         } else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(exec_plan)) {
    //             std::cout << "Be call times: " << x->be_call_times_ << ", left child call times: " << x->left_child_call_times_ << "\n";
    //             RwServerDebug::getInstance()->DEBUG_PRINT("[be call times:" + std::to_string(x->be_call_times_) + "][left child call times: " + std::to_string(x->left_child_call_times_) + "]");
    //             if(auto left_child = dynamic_cast<BlockNestedLoopJoinExecutor *>(x->left_.get())) {
    //                 exec_plan = left_child;
    //             } else {
    //                 exec_plan = nullptr;
    //             }
    //         } else {
    //             exec_plan = nullptr;
    //         }
    //     }
    // } 

    #ifdef TIME_OPEN
        auto root_begin_start = std::chrono::high_resolution_clock::now();
    #endif
    // std::cout << "first_ckpt_op->operator_id_: " << first_ckpt_op->operator_id_ << ", portal_stmt->root->operator_id_: " << portal_stmt->root->operator_id_ << std::endl;
    if(first_ckpt_op->operator_id_ != portal_stmt->root->operator_id_ && portal_stmt->root->finished_begin_tuple_ == false) {
        portal_stmt->root->beginTuple();
    }
    #ifdef TIME_OPEN
        auto root_begin_end = std::chrono::high_resolution_clock::now();
        auto root_begin_period = std::chrono::duration_cast<std::chrono::microseconds>(root_begin_end - root_begin_start).count();
        std::cout << "time for root beginTuple(): " << root_begin_period << "\n";
        RwServerDebug::getInstance()->DEBUG_PRINT("[time for root beginTuple(): " + std::to_string(root_begin_period) + "]");
    #endif
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
            if(portal_stmt->root != nullptr)
                CompCkptManager::get_instance()->add_new_query_tree(portal_stmt->root);
        }
    }

    #ifdef TIME_OPEN
        auto generate_query_tree_end = std::chrono::high_resolution_clock::now();
        auto generate_query_tree_period = std::chrono::duration_cast<std::chrono::microseconds>(generate_query_tree_end - recover_state_start).count();
        std::cout << "time for generate query tree: " << generate_query_tree_period << "\n";
    #endif

    rebuild_exec_plan_with_query_tree(context, portal_stmt, op_ck_meta, op_checkpoints);
    return portal_stmt;
}

void recover_exec_plan_to_consistent_state(Context* context, AbstractExecutor* root, int need_to_be_call_time) {
    if(root == nullptr) return;

    if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(root)) {
         if(x->finished_begin_tuple_ == false) {
            // std::cout << "Recover: BlockNestedLoopJoinExecutor beginTuple" << std::endl;
            x->beginTuple();
         }
         else {
            recover_exec_plan_to_consistent_state(context, x->left_.get(), x->left_child_call_times_);
            recover_exec_plan_to_consistent_state(context, x->right_.get(), x->right_child_call_times_);
         }

        // std::cout << "BlockJoinOp: x->be_call_times: " << x->be_call_times_ << ", need_to_be_call_time: " << need_to_be_call_time << std::endl;
         while(x->be_call_times_ < need_to_be_call_time) {
            x->nextTuple();
            x->be_call_times_ ++;
         }
    }
    else if(auto x = dynamic_cast<HashJoinExecutor *>(root)) {
        if(x->finished_begin_tuple_ == false) {
            // 如果未完成beginTuple，则需要首先将左子树恢复到一致性状态，然后将左儿子恢复到调用次数为left_child_call_times_的状态，再完成beginTuple
            // 如果已经完成了beginTuple，则无需恢复左子树的状态
            std::cout << "Recover: HashJoinExecutor beginTuple, operator_id: " << x->operator_id_ << std::endl;
            std::cout << "Recover left child, left_child_call_times_: " << x->left_child_call_times_ << std::endl;
            recover_exec_plan_to_consistent_state(context, x->left_.get(), x->left_child_call_times_);
            std::cout << "Recover right child, right_child_call_times_: " << x->right_child_call_times_ << std::endl;
            recover_exec_plan_to_consistent_state(context, x->right_.get(), x->right_child_call_times_);
            x->beginTuple();
        }
        else {
            recover_exec_plan_to_consistent_state(context, x->right_.get(), x->right_child_call_times_);
        }
        
        // @TODO: 如果当前HashJoin算子已经完成了beginTuple，并且右儿子是Gather算子，那么Gather算子需要调用launch_workers函数
        std::cout << "HashJoinOp: " << x->operator_id_ << ", x->be_call_times: " << x->be_call_times_ << ", need_to_be_call_time: " << need_to_be_call_time << std::endl;
        // 将当前算子恢复到被调用次数为need_to_be_call_time的状态
        while(x->be_call_times_ < need_to_be_call_time) {
            x->nextTuple();
            x->be_call_times_ ++;
        }
    }
    else if(auto x = dynamic_cast<SortExecutor *>(root)) {
        if(x->finished_begin_tuple_ == false) {
            recover_exec_plan_to_consistent_state(context, x->prev_.get(), x->left_child_call_times_);
            x->beginTuple();
        }

        // std::cout << "x->be_call_times: " << x->be_call_times_ << ", need_to_be_call_time: " << need_to_be_call_time << std::endl;
        while(x->be_call_times_ < need_to_be_call_time) {
            x->nextTuple();
            x->be_call_times_ ++;
        }
    }
    else if(auto x = dynamic_cast<ProjectionExecutor *>(root)) {
        recover_exec_plan_to_consistent_state(context, x->prev_.get(), x->left_child_call_times_);

        // std::cout << "x->be_call_times: " << x->be_call_times_ << ", need_to_be_call_time: " << need_to_be_call_time << std::endl;
        while(x->be_call_times_ < need_to_be_call_time) {
            x->nextTuple();
            x->be_call_times_ ++;
        }
    }
    else if(auto x = dynamic_cast<GatherExecutor *>(root)) {
        // std::cout << "Recover GatherExecutor, operator_id: " << x->operator_id_ << std::endl;
        if(x->finished_begin_tuple_ == false) {
            /** Gather算子的finished_begin_tuple_比较特殊，指的是所有的worker线程都完成了beginTuple，而不是Gather算子的beginTuple函数是否执行完
             * 因为在Gather算子中采用的是多线程并行的方式来进行儿子算子的处理，因此，Gather算子beginTuple执行完不代表所有算子的beginTuple都执行完了
             * TODO: 如果儿子算子是IndexScan算子，则不存在finish_begin_tuple没有完成的情况，只有当儿子算子是其他算子的时候才会出现这种情况 ，当前不支持其他算子，所以还没有做处理
             * */ 
            x->beginTuple();
        }
        else {
            std::cout << "Recover GatherOp, operator_id: " << x->operator_id_ << ": launch_workers\n";
            x->launch_workers();
        }

        std::cout << "Recover GatherOp: " << x->operator_id_ << ", x->be_call_times: " << x->be_call_times_ << ", need_to_be_call_time: " << need_to_be_call_time << std::endl;
        while(x->be_call_times_ < need_to_be_call_time) {
            x->nextTuple();
            x->Next_without_output();
            x->be_call_times_ ++;
        }

    }
}