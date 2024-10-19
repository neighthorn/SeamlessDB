#include "planner.h"

#include <memory>

#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "index/ix.h"
#include "record_printer.h"

// 目前的索引匹配规则为：完全匹配索引字段，且全部为单点查询，不会自动调整where条件的顺序
bool Planner::get_index_cols(std::string tab_name, std::vector<Condition> curr_conds, std::vector<std::string>& index_col_names) {
    index_col_names.clear();
    for(auto& cond: curr_conds) {
        if(cond.is_rhs_val && cond.op == OP_EQ && cond.lhs_col.tab_name.compare(tab_name) == 0)
            index_col_names.push_back(cond.lhs_col.col_name);
    }
    TabMeta& tab = sm_manager_->db_.get_table(tab_name);
    if(tab.is_index(index_col_names)) return true;
    return false;
}

void Planner::get_proj_cols(std::shared_ptr<Query> query, const std::string& tab_name, std::vector<TabCol>& proj_cols) {
    proj_cols.clear();
    const auto& curr_conds = query->conds;
    size_t curr_offset;
    for(auto& col: query->cols) {
        if(col.tab_name.compare(tab_name) == 0) {
            proj_cols.push_back(col);
            std::cout << "proj_col: " << col.tab_name << "." << col.col_name << std::endl;
        }
    }
    for(auto& cond: curr_conds) {
        if(cond.lhs_col.tab_name.compare(tab_name) == 0) {
            bool is_in_proj = false;
            for(auto& proj_col: proj_cols) {
                if(proj_col.col_name.compare(cond.lhs_col.col_name) == 0) {
                    is_in_proj = true;
                    break;
                }
            }
            if(is_in_proj == false) {
                proj_cols.push_back(cond.lhs_col);
                std::cout << "proj_col: " << cond.lhs_col.tab_name << "." << cond.lhs_col.col_name << std::endl;
            }
        }
        if(cond.is_rhs_val == false && cond.rhs_col.tab_name.compare(tab_name) == 0) {
            bool is_in_proj = false;
            for(auto& proj_col: proj_cols) {
                if(proj_col.col_name.compare(cond.rhs_col.col_name) == 0) {
                    is_in_proj = true;
                    break;
                }
            }
            if(is_in_proj == false) {
                proj_cols.push_back(cond.rhs_col);
                std::cout << "proj_col: " << cond.rhs_col.tab_name << "." << cond.rhs_col.col_name << std::endl;
            }
        }
    }
}

/*
    curr_conds: 当前的条件
    index_conds: 索引列的条件
    filter_conds: 非索引列的条件
*/
bool Planner::check_primary_index_match(std::string tab_name, std::vector<Condition> curr_conds, std::vector<Condition>& index_conds, std::vector<Condition>& filter_conds) {
    index_conds.clear();
    filter_conds.clear();
    TabMeta& tab = sm_manager_->db_.get_table(tab_name);
    IndexMeta pindex_meta = *(tab.get_primary_index_meta());
    std::vector<std::string> index_cols;
    bool is_op_eq = true;
    bool find_col = false;

    for(auto& index_col: pindex_meta.cols) {
        
        for(size_t i = 0; i < curr_conds.size(); ++i) {
            if(curr_conds[i].lhs_col.col_name.compare(index_col.name) == 0) {
                find_col = true;
                index_conds.emplace_back(curr_conds[i]);
                if(curr_conds[i].op != OP_EQ) is_op_eq = false;
            }
        }
        if(find_col == false) return false;

        index_cols.emplace_back(index_col.name);
        if(is_op_eq == false) break;
    }
    if(index_conds.size() == 0) return false;

    bool is_in_index;
    for(auto cond: curr_conds) {
        is_in_index = false;
        for(auto& index_col: index_cols) {
            if(index_col.compare(cond.lhs_col.col_name) == 0) {
                is_in_index = true;
                break;
            }
        }
        if(is_in_index == false) {
            filter_conds.emplace_back(cond);
        }
    }
    return true;
}

/**
 * @brief 表算子条件谓词生成
 *
 * @param conds 条件
 * @param tab_names 表名
 * @return std::vector<Condition>
 */
std::vector<Condition> pop_conds(std::vector<Condition> &conds, std::string tab_names) {
    // auto has_tab = [&](const std::string &tab_name) {
    //     return std::find(tab_names.begin(), tab_names.end(), tab_name) != tab_names.end();
    // };
    std::vector<Condition> solved_conds;
    auto it = conds.begin();
    while (it != conds.end()) {
        if ((tab_names.compare(it->lhs_col.tab_name) == 0 && it->is_rhs_val) || (it->lhs_col.tab_name.compare(it->rhs_col.tab_name) == 0)) {
            solved_conds.emplace_back(std::move(*it));
            it = conds.erase(it);
        } else {
            it++;
        }
    }
    return solved_conds;
}

int push_conds(Condition *cond, std::shared_ptr<Plan> plan)
{
    if(auto x = std::dynamic_pointer_cast<ScanPlan>(plan))
    {
        if(x->tab_name_.compare(cond->lhs_col.tab_name) == 0) {
            return 1;
        } else if(x->tab_name_.compare(cond->rhs_col.tab_name) == 0){
            return 2;
        } else {
            return 0;
        }
    }
    else if(auto x = std::dynamic_pointer_cast<JoinPlan>(plan))
    {
        int left_res = push_conds(cond, x->left_);
        // 条件已经下推到左子节点
        if(left_res == 3){
            return 3;
        }
        int right_res = push_conds(cond, x->right_);
        // 条件已经下推到右子节点
        if(right_res == 3){
            return 3;
        }
        // 左子节点或右子节点有一个没有匹配到条件的列
        if(left_res == 0 || right_res == 0) {
            return left_res + right_res;
        }
        // 左子节点匹配到条件的右边
        if(left_res == 2) {
            // 需要将左右两边的条件变换位置
            std::map<CompOp, CompOp> swap_op = {
                {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
            };
            std::swap(cond->lhs_col, cond->rhs_col);
            cond->op = swap_op.at(cond->op);
        }
        x->conds_.emplace_back(std::move(*cond));
        return 3;
    }
    return false;
}

// 查找table表的scan plan并返回，同时将table表加入到已经join到表队列joined_tables
std::shared_ptr<Plan> pop_scan(int *scantbl, std::string table, std::vector<std::string> &joined_tables, 
                std::vector<std::shared_ptr<Plan>> plans)
{
    for (size_t i = 0; i < plans.size(); i++) {
        auto x = std::dynamic_pointer_cast<ScanPlan>(plans[i]);
        if(x->tab_name_.compare(table) == 0)
        {
            scantbl[i] = 1;
            joined_tables.emplace_back(x->tab_name_);
            return plans[i];
        }
    }
    return nullptr;
}


std::shared_ptr<Query> Planner::logical_optimization(std::shared_ptr<Query> query, Context *context)
{
    
    //TODO 实现逻辑优化规则

    return query;
}

std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query, Context *context)
{
    // 
    std::shared_ptr<Plan> plan = make_one_rel(query);
    
    // 其他物理优化

    // 处理orderby
    plan = generate_sort_plan(query, std::move(plan)); 

    return plan;
}

static std::map<CompOp, CompOp> swap_op = {
                        {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
                    };

std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query)
{
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    std::vector<std::string> tables = query->tables;
    // // Scan table , 生成表算子列表tab_nodes
    std::vector<std::shared_ptr<Plan>> table_scan_executors(tables.size());
    for (size_t i = 0; i < tables.size(); i++) {
        std::vector<TabCol> proj_cols;
        get_proj_cols(query, tables[i], proj_cols);

        auto curr_conds = pop_conds(query->conds, tables[i]);
        // int index_no = get_indexNo(tables[i], curr_conds);
        std::vector<Condition> index_conds;
        std::vector<Condition> filter_conds;
        bool primary_index_match = check_primary_index_match(tables[i], curr_conds, index_conds, filter_conds);

        if (primary_index_match == false) {  // 该表没有索引
            index_conds.clear();
            filter_conds.clear();
            table_scan_executors[i] = 
                std::make_shared<ScanPlan>(T_IndexScan, current_sql_id_, current_plan_id_++, sm_manager_, tables[i], curr_conds, std::vector<Condition>(), proj_cols);
        } else {  // 存在索引
            table_scan_executors[i] =
                std::make_shared<ScanPlan>(T_IndexScan, current_sql_id_, current_plan_id_++, sm_manager_, tables[i], filter_conds, index_conds, proj_cols);
        }
    }
    // 只有一个表，不需要join。
    if(tables.size() == 1)
    {
        return table_scan_executors[0];
    }
    // 获取where条件
    auto conds = std::move(query->conds);
    std::shared_ptr<Plan> table_join_executors;
    table_join_executors = table_scan_executors[0];

    for(size_t i = 1; i < tables.size(); ++i) {
        auto it = conds.begin();
        std::vector<Condition> join_conds;

        // 找到当前待连接表(tables[i])的连接条件
        while(it != conds.end()) {
            if(it->lhs_col.tab_name == tables[i]) {
                // rhs是已经join了的表（左子树）
                if(std::find(tables.begin(), tables.begin() + i, it->rhs_col.tab_name) != tables.begin() + i) {
                    // 此时需要将条件反转，即待连接表上的谓词放到右边
                    std::swap(it->lhs_col, it->rhs_col);
                    it->op = swap_op.at(it->op);

                    join_conds.push_back(*it);
                
                    it = conds.erase(it);
                    continue;
                }
            } 
            else if(it->rhs_col.tab_name == tables[i]) {
                if(std::find(tables.begin(), tables.begin() + i, it->lhs_col.tab_name) != tables.begin() + i) {
                    join_conds.push_back(*it);
                    it = conds.erase(it);
                    continue;
                }
            }

            ++it;
        }

        if(i <= 0) {
            table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, 
                                                            current_sql_id_, current_plan_id_++, 
                                                            std::move(table_join_executors), 
                                                            std::move(table_scan_executors[i]), 
                                                            join_conds);
        }
        else {
            table_join_executors = std::make_shared<JoinPlan>(T_HashJoin, 
                                                        current_sql_id_, current_plan_id_++, 
                                                        std::move(table_join_executors), 
                                                        std::move(table_scan_executors[i]), 
                                                        join_conds);
        }
        
    }
    
    // int scantbl[tables.size()]; // 标记是否已经被加入到了算子树中
    // for(size_t i = 0; i < tables.size(); i++)
    // {
    //     scantbl[i] = -1;
    // }
    // // 假设在ast中已经添加了jointree，这里需要修改的逻辑是，先处理jointree，然后再考虑剩下的部分
    // if(conds.size() >= 1)
    // {
    //     // 有连接条件

    //     // 根据连接条件，生成第一层join
    //     std::vector<std::string> joined_tables(tables.size());
    //     auto it = conds.begin();
    //     while (it != conds.end()) {
    //         std::shared_ptr<Plan> left , right;
    //         left = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
    //         right = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
    //         std::vector<Condition> join_conds{*it};
    //         //建立join
    //         table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, current_sql_id_, current_plan_id_++, std::move(left), std::move(right), join_conds);
    //         it = conds.erase(it);
    //         break;
    //     }
    //     // 根据连接条件，生成第2-n层join
    //     it = conds.begin();
    //     while (it != conds.end()) {
    //         std::shared_ptr<Plan> left_need_to_join_executors = nullptr;
    //         std::shared_ptr<Plan> right_need_to_join_executors = nullptr;
    //         bool isneedreverse = false;
    //         if (std::find(joined_tables.begin(), joined_tables.end(), it->lhs_col.tab_name) == joined_tables.end()) {
    //             left_need_to_join_executors = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
    //             isneedreverse = true;
    //         }
    //         if (std::find(joined_tables.begin(), joined_tables.end(), it->rhs_col.tab_name) == joined_tables.end()) {
    //             right_need_to_join_executors = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
    //         } 

    //         if(left_need_to_join_executors != nullptr && right_need_to_join_executors != nullptr) {
    //             std::vector<Condition> join_conds{*it};
    //             std::shared_ptr<Plan> temp_join_executors = std::make_shared<JoinPlan>(T_NestLoop, 
    //                                                                 current_sql_id_, current_plan_id_++,
    //                                                                 std::move(left_need_to_join_executors), 
    //                                                                 std::move(right_need_to_join_executors), 
    //                                                                 join_conds);
    //             table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, 
    //                                                                 current_sql_id_, current_plan_id_++,
    //                                                                 std::move(temp_join_executors), 
    //                                                                 std::move(table_join_executors), 
    //                                                                 std::vector<Condition>());
    //         } else if(left_need_to_join_executors != nullptr || right_need_to_join_executors != nullptr) {
    //             if(isneedreverse) {
    //                 std::map<CompOp, CompOp> swap_op = {
    //                     {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
    //                 };
    //                 std::swap(it->lhs_col, it->rhs_col);
    //                 it->op = swap_op.at(it->op);
    //                 right_need_to_join_executors = std::move(left_need_to_join_executors);
    //             }
    //             std::vector<Condition> join_conds{*it};
    //             table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, 
    //                                                                 current_sql_id_, current_plan_id_++,
    //                                                                 std::move(table_join_executors),
    //                                                                 std::move(right_need_to_join_executors), 
    //                                                                 join_conds);
    //         } else {
    //             push_conds(std::move(&(*it)), table_join_executors);
    //         }
    //         it = conds.erase(it);
    //     }
    // } else {
    //     table_join_executors = table_scan_executors[0];
    //     scantbl[0] = 1;
    // }

    // //连接剩余表
    // for (size_t i = 0; i < tables.size(); i++) {
    //     if(scantbl[i] == -1) {
    //         table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, 
    //                                                 current_sql_id_, current_plan_id_++,
    //                                                 std::move(table_scan_executors[i]), 
    //                                                 std::move(table_join_executors), std::vector<Condition>());
    //     }
    // }

    return table_join_executors;
}

std::shared_ptr<Plan> Planner::generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan)
{
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    if(!x->has_sort) {
        return plan;
    }
    std::vector<std::string> tables = query->tables;
    std::vector<ColMeta> all_cols;
    for (auto &sel_tab_name : tables) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols_;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
    TabCol sel_col;
    for (auto &col : all_cols) {
        if(col.name.compare(x->order->cols->col_name) == 0 )
        sel_col = {.tab_name = col.tab_name, .col_name = col.name};
    }
    return std::make_shared<SortPlan>(T_Sort, current_sql_id_, current_plan_id_ ++, std::move(plan), sel_col, 
                                    x->order->orderby_dir == ast::OrderBy_DESC);
}


/**
 * @brief select plan 生成
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 */
std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query, Context *context) {
    //逻辑优化
    query = logical_optimization(std::move(query), context);

    //物理优化
    auto sel_cols = query->cols;
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context);
    plannerRoot = std::make_shared<ProjectionPlan>(T_Projection, current_sql_id_, current_plan_id_ ++, std::move(plannerRoot), 
                                                        std::move(sel_cols));

    return plannerRoot;
}

// 生成DDL语句和DML语句的查询执行计划
std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query, Context *context)
{
    std::shared_ptr<Plan> plannerRoot;
    if (auto x = std::dynamic_pointer_cast<ast::CreateTable>(query->parse)) {
        // create table;
        std::vector<ColDef> col_defs;
        for (auto &field : x->fields) {
            if (auto sv_col_def = std::dynamic_pointer_cast<ast::ColDef>(field)) {
                ColDef col_def(sv_col_def->col_name,
                            interp_sv_type(sv_col_def->type_len->type),
                            sv_col_def->type_len->len);
                col_defs.push_back(col_def);
            } else {
                throw InternalError("Unexpected field type");
            }
        }
        std::vector<std::string> pkeys;
        for(auto pkey: x->pkeys->pkeys_) {
            if(auto key_name = std::dynamic_pointer_cast<ast::Col>(pkey)) {
                pkeys.push_back(key_name->col_name);
            } else {
                throw InternalError("Unexpected field type");
            }
        }
        plannerRoot = std::make_shared<DDLPlan>(T_CreateTable, x->tab_name, std::vector<std::string>(), col_defs, pkeys);
    } else if (auto x = std::dynamic_pointer_cast<ast::DropTable>(query->parse)) {
        // drop table;
        plannerRoot = std::make_shared<DDLPlan>(T_DropTable, x->tab_name, std::vector<std::string>(), std::vector<ColDef>(), std::vector<std::string>());
    } else if (auto x = std::dynamic_pointer_cast<ast::CreateIndex>(query->parse)) {
        // create index;
        plannerRoot = std::make_shared<DDLPlan>(T_CreateIndex, x->tab_name, x->col_names, std::vector<ColDef>(), std::vector<std::string>());
    } else if (auto x = std::dynamic_pointer_cast<ast::DropIndex>(query->parse)) {
        // drop index
        plannerRoot = std::make_shared<DDLPlan>(T_DropIndex, x->tab_name, x->col_names, std::vector<ColDef>(), std::vector<std::string>());
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(query->parse)) {
        // insert;
        plannerRoot = std::make_shared<DMLPlan>(T_Insert, std::shared_ptr<Plan>(),  x->tab_name, sm_manager_->get_table_id(x->tab_name),
                                                    query->values, std::vector<Condition>(), std::vector<SetClause>());
        context->plan_tag_ = T_Insert;
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(query->parse)) {
        // delete;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<Condition> index_conds;     // 索引列条件
        std::vector<Condition> filter_conds;    // 非索引列条件
        std::vector<TabCol> proj_cols;
        get_proj_cols(query, x->tab_name, proj_cols);
        // bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);
        bool primary_index_match = check_primary_index_match(x->tab_name, query->conds, index_conds, filter_conds);
        
        if (primary_index_match == false) {  // 该表没有索引
            index_conds.clear();
            filter_conds.clear();
            table_scan_executors = 
                std::make_shared<ScanPlan>(T_SeqScan, current_sql_id_, current_plan_id_++, sm_manager_, x->tab_name, query->conds, std::vector<Condition>(), std::vector<TabCol>());
        } else {  // 存在索引
            table_scan_executors =
                std::make_shared<ScanPlan>(T_IndexScan, current_sql_id_, current_plan_id_++, sm_manager_, x->tab_name, filter_conds, index_conds, proj_cols);
        }

        plannerRoot = std::make_shared<DMLPlan>(T_Delete, table_scan_executors, x->tab_name,  sm_manager_->get_table_id(x->tab_name), 
                                                std::vector<Value>(), query->conds, std::vector<SetClause>());
        
        context->plan_tag_ = T_Delete;
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(query->parse)) {
        // update;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<Condition> index_conds;     // 索引列条件
        std::vector<Condition> filter_conds;    // 非索引列条件
        std::vector<TabCol> proj_cols;
        get_proj_cols(query, x->tab_name, proj_cols);
        // bool index_exist = get_index_cols(x->tab_name, query->conds, index_col_names);
        bool primary_index_match = check_primary_index_match(x->tab_name, query->conds, index_conds, filter_conds);
        
        if (primary_index_match == false) {  // 该表没有索引
            index_conds.clear();
            filter_conds.clear();
            table_scan_executors = 
                std::make_shared<ScanPlan>(T_SeqScan, current_sql_id_, current_plan_id_++, sm_manager_, x->tab_name, query->conds, std::vector<Condition>(), std::vector<TabCol>());
        } else {  // 存在索引
            table_scan_executors =
                std::make_shared<ScanPlan>(T_IndexScan, current_sql_id_, current_plan_id_++, sm_manager_, x->tab_name, filter_conds, index_conds, proj_cols);
        }

        plannerRoot = std::make_shared<DMLPlan>(T_Update, table_scan_executors, x->tab_name, sm_manager_->get_table_id(x->tab_name), 
                                                     std::vector<Value>(), query->conds, 
                                                     query->set_clauses);
        context->plan_tag_ = T_Update;
    } else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse)) {

        std::shared_ptr<plannerInfo> root = std::make_shared<plannerInfo>(x);
        // 生成select语句的查询执行计划
        std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
        plannerRoot = std::make_shared<DMLPlan>(T_select, projection, std::string(), 0, std::vector<Value>(),
                                                    std::vector<Condition>(), std::vector<SetClause>());
        context->plan_tag_ = T_select;
    } else {
        throw InternalError("Unexpected AST root");
    }
    return plannerRoot;
}