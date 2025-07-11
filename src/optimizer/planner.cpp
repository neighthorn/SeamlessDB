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
    // 当前函数保证了返回的iindex_conds中谓词条件的顺序和主键中的顺序一致
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

        if(find_col == false) {
            return false;
        }

        index_cols.emplace_back(index_col.name);
        if(is_op_eq == false) break;
    }
    if(index_conds.size() == 0) {
        return false;
    }

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
    std::shared_ptr<Plan> plan = make_one_rel(query, context);
    
    // 其他物理优化

    // 处理orderby
    plan = generate_sort_plan(query, std::move(plan)); 

    return plan;
}

static std::map<CompOp, CompOp> swap_op = {
                        {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
                    };

int Planner::convert_date_to_int(std::string date) {
    // 将日期转换为int，1992-01-01 -> 0, 1998-12-01 -> 83
    int year = std::stoi(date.substr(0, 4));
    int month = std::stoi(date.substr(5, 2));
    return (year - 1992) * 12 + month - 1;
}

std::string Planner::get_date_from_int(int date_index) {
    // 将int转换为日期，0 -> 1992-01-01, 83 -> 1998-12-01
    int year = date_index / 12 + 1992;
    int month = date_index % 12 + 1;
    std::string month_str = std::to_string(month);
    if(month_str.size() == 1) {
        month_str = "0" + month_str;
    }
    return std::to_string(year) + "-" + month_str + "-01";
}

std::shared_ptr<GatherPlan> Planner::convert_scan_to_parallel_scan(std::shared_ptr<ScanPlan> scan_plan, Context* context) {
    // 不需要转换成parallel scan
    if(context->parallel_worker_num_ == 1) return nullptr;
    // 如果scan范围不超过MIN_PARALLEL_SCAN_RANGE，那么就不需要转换为并行scan
    if(scan_plan->tag != T_IndexScan) {
        return nullptr;
    }
    if(scan_plan->index_conds_.size() == 0) {
        return nullptr;
    }

std::cout << "ConvertScanToParallelScan" << std::endl;
    // 按照index_conds来进行scan range的划分
    // 1. 找到index_conds中第一个非等值条件的最大值和最小值
    bool range_scan_exist = false;

    TabCol parallel_col;
    Value min_value, max_value;
    CompOp left_op = CompOp::OP_NONE, right_op = CompOp::OP_NONE;   // 四种：[], [), (], ()
    if(scan_plan->index_conds_.size() == 0) {
std::cout << "ConvertScanToParallelScan: Full Table Scan" << std::endl;
        // 如果index_conds为空，代表当前是全表扫描，那么min_value和max_value设置为第一个主键字段的最大值和最小值
        parallel_col = sm_manager_->get_table_first_col(scan_plan->tab_name_);
        min_value = sm_manager_->get_min_value(parallel_col.col_name);
        max_value = sm_manager_->get_max_value(parallel_col.col_name);
        // 全表扫描，所以是[]区间
        left_op = OP_GE;
        right_op = OP_LE;
        range_scan_exist = true;
    }
    
    // @assumption: 谓词中的条件是按照索引字段的顺序排列的
    // 这个assumption在check_primary_index_match中保证了
    for(int i = 0; i < scan_plan->index_conds_.size(); ++i) {
        if(scan_plan->index_conds_[i].op != OP_EQ) {
            if(range_scan_exist == true && parallel_col.col_name.compare(scan_plan->index_conds_[i].lhs_col.col_name) == 0) {
                // 如果已经找到了一个非等值条件，那么则需要查找该parallel_col上的其他谓词
                if(scan_plan->index_conds_[i].op == OP_LT || scan_plan->index_conds_[i].op == OP_LE) {
                    // 如果是<或<=，那么该cond为右边界
                    max_value = scan_plan->index_conds_[i].rhs_val;
                    right_op = scan_plan->index_conds_[i].op;
                    // min_value = sm_manager_->get_min_value(scan_plan->index_conds_[i].lhs_col.col_name);
                }
                else if(scan_plan->index_conds_[i].op == OP_GT || scan_plan->index_conds_[i].op == OP_GE) {
                    min_value = scan_plan->index_conds_[i].rhs_val;
                    left_op = scan_plan->index_conds_[i].op;
                    // max_value = sm_manager_->get_max_value(scan_plan->index_conds_[i].lhs_col.col_name);
                }
            }
            else if(range_scan_exist == false){
                range_scan_exist = true;
                parallel_col = scan_plan->index_conds_[i].lhs_col;
                if(scan_plan->index_conds_[i].op == OP_LT || scan_plan->index_conds_[i].op == OP_LE) {
                    // 如果是<或<=，那么该cond为右边界
                    max_value = scan_plan->index_conds_[i].rhs_val;
                    right_op = scan_plan->index_conds_[i].op;
                    // min_value = sm_manager_->get_min_value(scan_plan->index_conds_[i].lhs_col.col_name);
                }
                else if(scan_plan->index_conds_[i].op == OP_GT || scan_plan->index_conds_[i].op == OP_GE) {
                    min_value = scan_plan->index_conds_[i].rhs_val;
                    left_op = scan_plan->index_conds_[i].op;
                    // max_value = sm_manager_->get_max_value(scan_plan->index_conds_[i].lhs_col.col_name);
                }
            }
        }
    }

    if(range_scan_exist == false) {
        std::cout << "ConvertScanToParallelScan: No Range Scan" << std::endl;
        return nullptr;
    }
std::cout << "ConvertScanToParallelScan: Parallel_col: " << parallel_col.col_name << std::endl;

    // 如果左边界或者右边界没有赋值，那么用最大值和最小值代替
    if(left_op == CompOp::OP_NONE) {
        left_op = OP_GE;
        min_value = sm_manager_->get_min_value(parallel_col.col_name);
    }
    if(right_op == CompOp::OP_NONE) {
        right_op = OP_LE;
        max_value = sm_manager_->get_max_value(parallel_col.col_name);
    }

    // 2. 计算每个worker的扫描范围
    int worker_num = context->parallel_worker_num_;
std::cout << "ConvertScanToParallelScan: WorkerNum: " << worker_num << std::endl;
    std::vector<std::pair<Value, Value>> scan_ranges;
    switch(min_value.type) {
        case ColType::TYPE_INT: {
            int interval = (max_value.int_val - min_value.int_val) / worker_num;
            for(int i = 0; i < worker_num; ++i) {
                Value start_val, end_val;
                start_val.set_int(min_value.int_val + i * interval);
                start_val.init_raw(sizeof(int));
                if(i == worker_num - 1) 
                    end_val.set_int(max_value.int_val);
                else
                    end_val.set_int(min_value.int_val + (i + 1) * interval);
                end_val.init_raw(sizeof(int));
                scan_ranges.emplace_back(std::make_pair(start_val, end_val));
                std::cout << "ConvertScanToParallelScan: Worker" << i << " Range: " << start_val.int_val << " ~ " << end_val.int_val << std::endl;
            }
        } break;
        case ColType::TYPE_STRING: {
            // 扫描范围划分：日期的可能取值为1992-01-01 ~ 1998-12-01，并且只可能是每个月的第一天，如果划分条件是string类型那么一定是日期类型
            // 日期的字符串格式为"YYYY-MM-DD"，并且只可能是1992-1998年之间的每个月的第一天，如何划分？
            // 1. 先将日期转换为int，1992-01-01 -> 0, 1998-12-01 -> 83
            // 2. 按照int划分为worker_num份
            int start_date = convert_date_to_int(min_value.str_val);
            int end_date = convert_date_to_int(max_value.str_val);
            int interval = (end_date - start_date) / worker_num;
            for(int i = 0; i < worker_num; ++i) {
                Value start_val, end_val;
                start_val.set_str(get_date_from_int(start_date + i * interval));
                start_val.init_raw(start_val.str_val.length());
                if(i == worker_num - 1) 
                    end_val.set_str(max_value.str_val);
                else 
                    end_val.set_str(get_date_from_int(start_date + (i + 1) * interval));
                end_val.init_raw(end_val.str_val.length());
                scan_ranges.emplace_back(std::make_pair(start_val, end_val));
                std::cout << "ConvertScanToParallelScan: Worker" << i << " Range: " << start_val.str_val << " ~ " << end_val.str_val << std::endl;
            }
        } break;
        default:
        break;
    }

    // 3. 生成并行scan plan
    std::vector<std::shared_ptr<Plan>> parallel_scan_plans;
    for(int i = 0; i < worker_num; ++i) {
        std::vector<Condition> index_conds = scan_plan->index_conds_;
        Condition left_cond, right_cond;
        if(i == 0) {
            // 第一个range的左边界和整体range的左边界对齐
            left_cond = Condition{.lhs_col = parallel_col, .op = left_op, .is_rhs_val = true, .rhs_val = scan_ranges[i].first};
            right_cond = Condition{.lhs_col = parallel_col, .op = OP_LT, .is_rhs_val = true, .rhs_val = scan_ranges[i].second}; 
        }
        else if(i == worker_num - 1) {
            // 最后一个range的右边界和整体range的右边界对齐
            left_cond = Condition{.lhs_col = parallel_col, .op = OP_GE, .is_rhs_val = true, .rhs_val = scan_ranges[i].first};
            right_cond = Condition{.lhs_col = parallel_col, .op = right_op, .is_rhs_val = true, .rhs_val = scan_ranges[i].second};
        }
        else {
            // 中间的range为[)区间
            left_cond = Condition{.lhs_col = parallel_col, .op = OP_GE, .is_rhs_val = true, .rhs_val = scan_ranges[i].first};
            right_cond = Condition{.lhs_col = parallel_col, .op = OP_LT, .is_rhs_val = true, .rhs_val = scan_ranges[i].second};
        }

        if(index_conds.size() > 0) {
            // @assumption：index_conds中一定有非等值查询
            size_t last_removed_index = index_conds.size(); // 初始化为vector大小，表示没有移除元素
            auto it = index_conds.begin();
            while (it != index_conds.end()) {
                if (it->lhs_col.col_name.compare(parallel_col.col_name) == 0) {
                    last_removed_index = std::distance(index_conds.begin(), it); // 记录移除元素的位置
                    it = index_conds.erase(it); // erase 返回下一个迭代器
                } else {
                    ++it;
                }
            }

            if(last_removed_index < index_conds.size())  {
                index_conds.insert(index_conds.begin() + last_removed_index, left_cond);
                index_conds.insert(index_conds.begin() + last_removed_index + 1, right_cond);
            }
            else {
                index_conds.emplace_back(left_cond);
                index_conds.emplace_back(right_cond);
            }
            
        }
        else {
            index_conds.emplace_back(left_cond);
            index_conds.emplace_back(right_cond);
        }
        std::shared_ptr<ScanPlan> worker_plan = std::make_shared<ScanPlan>(T_IndexScan, scan_plan->sql_id_, current_plan_id_++, sm_manager_, scan_plan->tab_name_, scan_plan->filter_conds_, index_conds, scan_plan->proj_cols_);
        parallel_scan_plans.emplace_back(std::move(worker_plan));
    }

    // 4. 将并行scan plan合并到Gather算子上
    std::shared_ptr<GatherPlan> gather_plan = std::make_shared<GatherPlan>(T_Gather, scan_plan->sql_id_, current_plan_id_++, parallel_scan_plans);
    return gather_plan;
}

std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query, Context* context)
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
        auto gather_plan = convert_scan_to_parallel_scan(std::dynamic_pointer_cast<ScanPlan>(table_scan_executors[i]), context);
        if(gather_plan != nullptr)
            table_scan_executors[i] = gather_plan;
    }
    // 只有一个表，不需要join。
    if(tables.size() == 1) {
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

        if(i <= 1) {
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