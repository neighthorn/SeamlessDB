#pragma once

#include <cerrno>
#include <cstring>
#include <string>
#include "optimizer/plan.h"
#include "execution/executor_abstract.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_hash_join.h"
#include "execution/executor_block_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_update.h"
#include "execution/executor_insert.h"
#include "execution/executor_delete.h"
#include "execution/execution_sort.h"
#include "state/op_state_manager.h"
#include "common/common.h"


typedef enum portalTag{
    PORTAL_Invalid_Query = 0,
    PORTAL_ONE_SELECT,
    PORTAL_DML_WITHOUT_SELECT,
    PORTAL_MULTI_QUERY,
    PORTAL_CMD_UTILITY
} portalTag;


struct PortalStmt {
    portalTag tag;
    
    std::vector<TabCol> sel_cols;
    std::shared_ptr<AbstractExecutor> root;
    std::shared_ptr<Plan> plan;
    
    PortalStmt(portalTag tag_, std::vector<TabCol> sel_cols_, std::shared_ptr<AbstractExecutor> root_, std::shared_ptr<Plan> plan_) :
            tag(tag_), sel_cols(std::move(sel_cols_)), root(root_), plan(std::move(plan_)) {}
};

class Portal
{
   private:
    SmManager *sm_manager_;
    

   public:
    Portal(SmManager *sm_manager) : sm_manager_(sm_manager){}
    ~Portal(){}

    // 将查询执行计划转换成对应的算子树
    std::shared_ptr<PortalStmt> start(std::shared_ptr<Plan> plan, Context *context)
    {
        // 这里可以将select进行拆分，例如：一个select，带有return的select等
        if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(), std::shared_ptr<AbstractExecutor>(),plan);
        } else if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<TabCol>(), std::shared_ptr<AbstractExecutor>(),plan);
        } else if (auto x = std::dynamic_pointer_cast<DMLPlan>(plan)) {
            switch(x->tag) {
                case T_select:
                {
                    std::shared_ptr<ProjectionPlan> p = std::dynamic_pointer_cast<ProjectionPlan>(x->subplan_);
                    std::shared_ptr<AbstractExecutor> root= convert_plan_executor(p, context);
                    dynamic_cast<ProjectionExecutor*>(root.get())->set_root();
                    return std::make_shared<PortalStmt>(PORTAL_ONE_SELECT, std::move(p->sel_cols_), std::move(root), plan);
                }
                    
                case T_Update:
                {
                    std::shared_ptr<AbstractExecutor> scan= convert_plan_executor(x->subplan_, context);
                    std::vector<Rid> rids;
                    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                        rids.push_back(scan->rid());
                    }
                    std::shared_ptr<AbstractExecutor> root =std::make_shared<UpdateExecutor>(sm_manager_, 
                                                            x->tab_name_, x->set_clauses_, x->conds_, rids, context);
                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }
                case T_Delete:
                {
                    std::shared_ptr<AbstractExecutor> scan= convert_plan_executor(x->subplan_, context);
                    std::vector<Rid> rids;
                    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                        rids.push_back(scan->rid());
                    }

                    std::shared_ptr<AbstractExecutor> root =
                        std::make_shared<DeleteExecutor>(sm_manager_, x->tab_name_, x->conds_, rids, context);

                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }

                case T_Insert:
                {
                    std::shared_ptr<AbstractExecutor> root =
                            std::make_shared<InsertExecutor>(sm_manager_, x->tab_name_, x->values_, context);
            
                    return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }


                default:
                    throw InternalError("Unexpected field type");
                    break;
            }
        } else {
            throw InternalError("Unexpected field type");
        }
        return nullptr;
    }

    // 遍历算子树并执行算子生成执行结果
    void run(std::shared_ptr<PortalStmt> portal, QlManager* ql, Context *context){
        switch(portal->tag) {
            case PORTAL_ONE_SELECT:
            {
                ql->select_from(portal->root, std::move(portal->sel_cols), context);
                break;
            }

            case PORTAL_DML_WITHOUT_SELECT:
            {
                ql->run_dml(portal->root);
                break;
            }
            case PORTAL_MULTI_QUERY:
            {
                ql->run_mutli_query(portal->plan, context);
                break;
            }
            case PORTAL_CMD_UTILITY:
            {
                ql->run_cmd_utility(portal->plan, context);
                break;
            }
            default:
            {
                throw InternalError("Unexpected field type");
            }
        }
    }

    /*
        重做查询
    */
    void re_run(std::shared_ptr<PortalStmt> portal, QlManager* ql, Context *context) {
        switch(portal->tag) {
            case PORTAL_ONE_SELECT:
            {
                ql->re_run_select_from(portal->root, std::move(portal->sel_cols), context);
                break;
            }

            case PORTAL_DML_WITHOUT_SELECT:
            {
                ql->run_dml(portal->root);
                break;
            }
            case PORTAL_MULTI_QUERY:
            {
                ql->run_mutli_query(portal->plan, context);
                break;
            }
            case PORTAL_CMD_UTILITY:
            {
                ql->run_cmd_utility(portal->plan, context);
                break;
            }
            default:
            {
                throw InternalError("Unexpected field type");
            }
        }
    }

    // 清空资源
    void drop(){}


    std::shared_ptr<AbstractExecutor> convert_plan_executor(std::shared_ptr<Plan> plan, Context *context)
    {
        if(auto x = std::dynamic_pointer_cast<ProjectionPlan>(plan)){
            return std::make_shared<ProjectionExecutor>(convert_plan_executor(x->subplan_, context), 
                                                        x->sel_cols_, context, x->sql_id_, x->plan_id_);
        } else if(auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
            if(x->tag == T_SeqScan) {
                return std::make_shared<SeqScanExecutor>(sm_manager_, x->tab_name_, x->filter_conds_, context);
            }
            else {
                return std::make_shared<IndexScanExecutor>(sm_manager_, x->tab_name_, x->proj_cols_, x->filter_conds_, x->index_conds_, context, x->sql_id_, x->plan_id_);
            } 
        } else if(auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
            std::shared_ptr<AbstractExecutor> left = convert_plan_executor(x->left_, context);
            std::shared_ptr<AbstractExecutor> right = convert_plan_executor(x->right_, context);
            if(x->tag == T_NestLoop) {
                return std::make_shared<BlockNestedLoopJoinExecutor>(std::move(left), 
                                std::move(right), std::move(x->conds_), context, x->sql_id_, x->plan_id_);
            }
            else {
                return std::make_shared<HashJoinExecutor>(std::move(left), 
                                std::move(right), std::move(x->conds_), context, x->sql_id_, x->plan_id_);
            }
        } else if(auto x = std::dynamic_pointer_cast<SortPlan>(plan)) {
            return std::make_shared<SortExecutor>(convert_plan_executor(x->subplan_, context), 
                                            x->sel_col_, x->is_desc_, context, x->sql_id_, x->plan_id_);
        }
        return nullptr;
    }

    std::unique_ptr<AbstractExecutor> load_op_state(std::shared_ptr<ProjectionPlan> plan, Context *context)
    {
        /*
            read op state from state node
        */
        
        /*

        */
        
        return std::make_unique<ProjectionExecutor>(convert_plan_executor(plan->subplan_, context),
                                                    plan->sel_cols_, context, plan->sql_id_, plan->plan_id_);
    }

};