#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "execution/execution_defs.h"
#include "execution/execution_manager.h"
#include "system/sm.h"
#include "common/context.h"
#include "plan.h"
#include "parser/parser.h"
#include "common/common.h"
#include "analyze/analyze.h"

class Planner {
   private:
    SmManager *sm_manager_;
    /*
        sql id && plan id
    */
    std::atomic<int>     current_sql_id_;
    std::atomic<int>     current_plan_id_;

   public:
    Planner(SmManager *sm_manager) : sm_manager_(sm_manager) {}

    inline void set_sql_id(int sql_id) { 
        current_sql_id_     = sql_id;
        current_plan_id_    = 0;
    }
    
    std::shared_ptr<Plan> do_planner(std::shared_ptr<Query> query, Context *context);

   private:
    std::shared_ptr<Query> logical_optimization(std::shared_ptr<Query> query, Context *context);
    std::shared_ptr<Plan> physical_optimization(std::shared_ptr<Query> query, Context *context);

    std::shared_ptr<Plan> make_one_rel(std::shared_ptr<Query> query);

    std::shared_ptr<Plan> generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);
    
    std::shared_ptr<Plan> generate_select_plan(std::shared_ptr<Query> query, Context *context);


    // int get_indexNo(std::string tab_name, std::vector<Condition> curr_conds);
    bool get_index_cols(std::string tab_name, std::vector<Condition> curr_conds, std::vector<std::string>& index_col_names);
    bool check_primary_index_match(std::string tab_name, std::vector<Condition> curr_conds, std::vector<Condition>& index_conds, std::vector<Condition>& filter_conds);
    void get_proj_cols(std::shared_ptr<Query> query, const std::string& tab_name, std::vector<TabCol>& proj_cols);

    ColType interp_sv_type(ast::SvType sv_type) {
        std::map<ast::SvType, ColType> m = {
            {ast::SV_TYPE_INT, TYPE_INT}, {ast::SV_TYPE_FLOAT, TYPE_FLOAT}, {ast::SV_TYPE_STRING, TYPE_STRING}};
        return m.at(sv_type);
    }
};
