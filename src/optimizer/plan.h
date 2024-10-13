#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
// #include <gtest/gtest.h>
#include "parser/ast.h"

#include "parser/parser.h"
#include "system/sm_manager.h"
#include "common/common.h"

// 查询执行计划
class Plan
{
public:
    PlanTag tag;
    int     sql_id_;
    int     plan_id_;

    Plan(int sql_id = -1, int plan_id = -1) : sql_id_(sql_id), plan_id_(plan_id) {}    

    virtual ~Plan() = default;
    virtual int serialize(char* dest) = 0;      // return the total length of this node and its subtrees(recursive)
    virtual int plan_tree_size() = 0;           // get the plan node count
    // virtual void deserialize(char* src);
    // virtual std::shared_ptr<Plan> deserialize(char* src) = 0;
    virtual void format_print() = 0;
};

class ScanPlan : public Plan
{
    public:
        ScanPlan(PlanTag tag, int sql_id, int plan_id, SmManager *sm_manager, std::string tab_name, std::vector<Condition> filter_conds, std::vector<Condition> index_conds, std::vector<TabCol> proj_cols) :
            Plan(sql_id, plan_id)
        {
            Plan::tag = tag;
            tab_name_ = std::move(tab_name);
            filter_conds_ = std::move(filter_conds);
            TabMeta &tab = sm_manager->db_.get_table(tab_name_);
            cols_ = tab.cols_;
            len_ = cols_.back().offset + cols_.back().len;
            index_conds_ = std::move(index_conds);
            proj_cols_ = std::move(proj_cols);
        }
        ~ScanPlan(){}

        void format_print() override {
            std::cout << "op_id: " << plan_id_ << ", ";
            if(Plan::tag == T_IndexScan)
                std::cout << "IndexScan: ";
            else
                std::cout << "SeqScan: ";
            std::cout << tab_name_ << ", conds: ";
            for(const auto& cond: index_conds_) {
                std::cout << cond.lhs_col.col_name << CompOpString[cond.op] << cond.rhs_val.int_val << ", ";
            }
            for(const auto& cond: filter_conds_) {
                std::cout << cond.lhs_col.col_name << CompOpString[cond.op] << cond.rhs_val.int_val << ", ";
            }
            std::cout << std::endl;
        }

        int plan_tree_size() override {
            return 1;
        }

        int serialize(char* dest) {
            
            
            int offset = sizeof(int);

            /*
                sql_id & plan_id
            */
            memcpy(dest + offset, (char *)&sql_id_, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, (char *)&plan_id_, sizeof(int));
            offset += sizeof(int);

            // plantag, tot_size, table_id, 
            memcpy(dest + offset, &tag, sizeof(PlanTag));
            offset += sizeof(PlanTag);

            int tab_name_size = tab_name_.length();
            memcpy(dest + offset, &tab_name_size, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, tab_name_.c_str(), tab_name_size);
            offset += tab_name_size;

            // int col_num = cols_.size();
            // memcpy(dest + offset, &col_num, sizeof(int));
            // offset += col_num;
            // for(auto& col: cols_) col.serialize(dest, offset);
            int filter_cond_num = filter_conds_.size();
            memcpy(dest + offset, &filter_cond_num, sizeof(int));
            offset += sizeof(int);
            for(auto& filter_cond: filter_conds_) filter_cond.serialize(dest, offset);

            int index_cond_num = index_conds_.size();
            memcpy(dest + offset, &index_cond_num, sizeof(int));
            offset += sizeof(int);
            for(auto& index_cond: index_conds_) index_cond.serialize(dest, offset);

            memcpy(dest, &offset, sizeof(int));
            return offset;
        }

        static std::shared_ptr<Plan> deserialize(char* src, SmManager* sm_manager) {
            int offset = 0;
            int tot_size = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);

            /*
                sql_id & plan_id
            */
            int sql_id = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);
            int plan_id = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);

            PlanTag tag = *reinterpret_cast<const PlanTag*>(src + offset);
            offset += sizeof(PlanTag);

            int tab_name_size = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::string tab_name_(src + offset, tab_name_size);
            offset += tab_name_size;

            // int col_num = *reinterpret_cast<const int*>(src + offset);
            // offset += sizeof(int);
            // std::vector<ColMeta> cols_;
            // for(int i = 0; i < col_num; ++i) {
            //     ColMeta col;
            //     col.deserialize(src, offset);
            //     cols_.push_back(std::move(col));
            // }
            int filter_cond_num = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::vector<Condition> filter_conds_;
            for(int i = 0; i < filter_cond_num; ++i) {
                Condition cond;
                cond.deserialize(src, offset);
                filter_conds_.push_back(std::move(cond));
            }

            // int len_ = *reinterpret_cast<const size_t*>(src + offset);
            // offset += sizeof(size_t);
            int index_cond_num = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::vector<Condition> index_conds_;
            for(int i = 0; i < index_cond_num; ++i) {
                Condition cond;
                cond.deserialize(src, offset);
                index_conds_.push_back(std::move(cond));
            }

            return std::make_shared<ScanPlan>(tag, sql_id, plan_id, sm_manager, tab_name_, filter_conds_, index_conds_, std::vector<TabCol>());
        }

        // 以下变量同ScanExecutor中的变量
        std::string tab_name_;                     
        std::vector<ColMeta> cols_;                
        std::vector<Condition> filter_conds_;             
        size_t len_;                               
        std::vector<Condition> index_conds_;
        std::vector<TabCol> proj_cols_;
};

class JoinPlan : public Plan
{
    public:
        JoinPlan(PlanTag tag, int sql_id, int plan_id, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds)
            : Plan(sql_id, plan_id)
        {
            Plan::tag = tag;
            left_ = std::move(left);
            right_ = std::move(right);
            conds_ = std::move(conds);
            type = INNER_JOIN;

            if(Plan::tag == T_NestLoop)
                std::cout << "BlockNestedLoopJoin: ";
            else if(Plan::tag == T_HashJoin)
                std::cout << "HashJoin: ";
            for(const auto& cond: conds_) {
                std::cout << cond.lhs_col.col_name << CompOpString[cond.op] << cond.rhs_col.col_name << ", ";
            }
            std::cout << "\n";
        }
        ~JoinPlan(){}

        void format_print() override {
            std::cout << "op_id: " << plan_id_ << ", ";
            if(Plan::tag == T_NestLoop)
                std::cout << "BlockNestedLoopJoin: ";
            else if(Plan::tag == T_HashJoin)
                std::cout << "HashJoin: ";
            
            std::cout << "join_condition: ";
            for(const auto& cond: conds_) {
                std::cout << cond.lhs_col.col_name << CompOpString[cond.op] << cond.rhs_col.col_name << ", ";
            }
            std::cout << "\n****************left operator*******************\n";
            left_->format_print();
            std::cout << "\n----------------right operator------------------\n";
            right_->format_print();
        }

        int plan_tree_size() override {
            return left_->plan_tree_size() + right_->plan_tree_size() + 1;
        }

        int serialize(char* dest) override {
            int offset = sizeof(int);

            /*
                sql_id & plan_id
            */
            memcpy(dest + offset, (char *)&sql_id_, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, (char *)&plan_id_, sizeof(int));
            offset += sizeof(int);

            memcpy(dest + offset, &tag, sizeof(PlanTag));
            offset += sizeof(PlanTag);

            int cond_num = conds_.size();
            memcpy(dest + offset, &cond_num, sizeof(int));
            offset += sizeof(int);
            for(auto& cond: conds_) cond.serialize(dest, offset);

            int off_left = 0;       // the offset of left_ (start from the end of the current operator)
            int off_right = 0;      // the offset of righth_ (start from the end of the current operator)
            // | tot_size of this plan | cond_num | conds | off_left | off_right| ... left_ ... | right_ |
            memcpy(dest + offset, &off_left, sizeof(int));
            offset += sizeof(int);

            off_right = left_->serialize(dest + offset + sizeof(int));

            memcpy(dest + offset, &off_right, sizeof(int));
            offset += sizeof(int);

            memcpy(dest, &offset, sizeof(int));

            int right_size = right_->serialize(dest + off_right + offset);

            return right_size + offset + off_right;
        }

        static std::shared_ptr<JoinPlan> deserialize(char* src, SmManager* sm_mgr) {
            int offset = 0;
            int tot_size = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);

            /*
                sql_id & plan_id
            */
            int sql_id = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);
            int plan_id = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);

            PlanTag tag = *reinterpret_cast<const PlanTag*>(src + offset);
            offset += sizeof(PlanTag);

            int cond_num = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::vector<Condition> conds_;
            for(int i = 0; i < cond_num; ++i) {
                Condition cond;
                cond.deserialize(src, offset);
                conds_.push_back(std::move(cond));
            }
            
            int off_left = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            int off_right = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);

            assert(tot_size == offset);
            src = src + offset;
            PlanTag left_tag = *reinterpret_cast<const PlanTag*>(src + off_left + sizeof(int));
            PlanTag right_tag = *reinterpret_cast<const PlanTag*>(src + off_right + sizeof(int));

            std::shared_ptr<Plan> left_;
            std::shared_ptr<Plan> right_;

            if(left_tag == PlanTag::T_NestLoop) {
                left_ = JoinPlan::deserialize(src + off_left, sm_mgr);
            }
            else if(left_tag == PlanTag::T_SeqScan || left_tag == PlanTag::T_IndexScan) {
                left_ = ScanPlan::deserialize(src + off_left, sm_mgr);
            }

            if(right_tag == PlanTag::T_NestLoop) {
                right_ = JoinPlan::deserialize(src + off_right, sm_mgr);
            }
            else if(right_tag == PlanTag::T_SeqScan || right_tag == PlanTag::T_IndexScan) {
                right_ = ScanPlan::deserialize(src + off_right, sm_mgr);
            }

            return std::make_shared<JoinPlan>(tag, sql_id, plan_id, left_, right_, conds_);
        }

        // 左节点
        std::shared_ptr<Plan> left_;
        // 右节点
        std::shared_ptr<Plan> right_;
        // 连接条件
        std::vector<Condition> conds_;
        // future TODO: 后续可以支持的连接类型
        JoinType type;
        
};

class SortPlan : public Plan
{
    public:
        SortPlan(PlanTag tag, int sql_id, int plan_id, std::shared_ptr<Plan> subplan, TabCol sel_col, bool is_desc)
        : Plan(sql_id, plan_id) {
            Plan::tag = tag;
            subplan_ = std::move(subplan);
            sel_col_ = sel_col;
            is_desc_ = is_desc;
        }
        ~SortPlan(){}

        void format_print() override {

        }

        int plan_tree_size() override {
            return 1 + subplan_->plan_tree_size();
        }

        int serialize(char* dest) override {
            int offset = sizeof(int);

            /*
                sql_id & plan_id
            */
            memcpy(dest + offset, (char *)&sql_id_, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, (char *)&plan_id_, sizeof(int));
            offset += sizeof(int);

            memcpy(dest + offset, &tag, sizeof(PlanTag));
            offset += sizeof(PlanTag);

            sel_col_.serialize(dest, offset);

            memcpy(dest + offset, &is_desc_, sizeof(bool));
            offset += sizeof(bool);

            int off_subplan = 0;
            memcpy(dest + offset, &off_subplan, sizeof(int));
            offset += sizeof(int);

            memcpy(dest, &offset, sizeof(int));

            int subplan_size = subplan_->serialize(dest + offset);
            return offset + subplan_size;
        }

        static std::shared_ptr<SortPlan> deserialize(char* src, SmManager* sm_mgr) {
            int offset = 0;
            int tot_size = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);

            /*
                sql_id & plan_id
            */
            int sql_id = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);
            int plan_id = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);

            PlanTag tag = *reinterpret_cast<const PlanTag*>(src + offset);
            offset += sizeof(PlanTag);

            TabCol sel_col_;
            sel_col_.deserialize(src, offset);

            bool is_desc_ = *reinterpret_cast<const bool*>(src + offset);
            offset += sizeof(bool);

            int off_subplan = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);

            assert(offset == tot_size);
            src = src + offset;
            PlanTag subplan_tag = *reinterpret_cast<const PlanTag*>(src + off_subplan + sizeof(int));
            std::shared_ptr<Plan> subplan_;

            if(subplan_tag == PlanTag::T_NestLoop) {
                subplan_ = JoinPlan::deserialize(src + off_subplan, sm_mgr);
            }
            else if(subplan_tag == PlanTag::T_SeqScan || subplan_tag == PlanTag::T_IndexScan) {
                subplan_ = ScanPlan::deserialize(src + off_subplan, sm_mgr);
            }

            return std::make_shared<SortPlan>(tag, sql_id, plan_id, subplan_, sel_col_, is_desc_);
        }

        std::shared_ptr<Plan> subplan_;
        TabCol sel_col_;
        bool is_desc_;
        
};

class ProjectionPlan : public Plan
{
    public:
        ProjectionPlan(PlanTag tag, int sql_id, int plan_id, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols)
        : Plan(sql_id, plan_id) {
            Plan::tag = tag;
            subplan_ = std::move(subplan);
            sel_cols_ = std::move(sel_cols);
        }
        ~ProjectionPlan(){}

        void format_print() override {
            std::cout << "op_id: " << plan_id_ << ", ";
            std::cout << "Projection: ";
            for(const auto& col: sel_cols_) {
                std::cout << col.col_name << ", ";
            }
            std::cout << std::endl;
            subplan_->format_print();
        }

        int plan_tree_size() override {
            return 1 + subplan_->plan_tree_size();
        }

        int serialize(char* dest) override {
            int offset = sizeof(int);


            /*
                sql_id & plan_id
            */
            memcpy(dest + offset, (char *)&sql_id_, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, (char *)&plan_id_, sizeof(int));
            offset += sizeof(int);

            memcpy(dest + offset, &tag, sizeof(PlanTag));
            offset += sizeof(PlanTag);

            int col_num = sel_cols_.size();
            memcpy(dest + offset, &col_num, sizeof(int));
            offset += sizeof(int);
            for(auto& col: sel_cols_) col.serialize(dest, offset);

            int off_subplan = 0;
            memcpy(dest + offset, &off_subplan, sizeof(int));
            offset += sizeof(int);

            memcpy(dest, &offset, sizeof(int));

            int subplan_size = subplan_->serialize(dest + offset);

            return offset + subplan_size;
        }

        static std::shared_ptr<ProjectionPlan> deserialize(char* src, SmManager* sm_mgr) {
            int offset = 0;
            int tot_size = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);

            /*
                sql_id & plan_id
            */
            int sql_id = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);
            int plan_id = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);

            PlanTag tag = *reinterpret_cast<const PlanTag*>(src + offset);
            offset += sizeof(PlanTag);

            int col_num = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::vector<TabCol> sel_cols_;
            for(int i = 0; i < col_num; ++i) {
                TabCol col;
                col.deserialize(src, offset);
                sel_cols_.push_back(std::move(col));
            }

            int off_subplan = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);

            assert(offset == tot_size);
            src = src + offset;
            PlanTag subplan_tag = *reinterpret_cast<const PlanTag*>(src + off_subplan + sizeof(int));
            std::shared_ptr<Plan> subplan_;

            if(subplan_tag == PlanTag::T_NestLoop) {
                subplan_ = JoinPlan::deserialize(src + off_subplan, sm_mgr);
            }
            else if(subplan_tag == PlanTag::T_SeqScan || subplan_tag == PlanTag::T_IndexScan) {
                subplan_ = ScanPlan::deserialize(src + off_subplan, sm_mgr);
            }
            else if(subplan_tag == PlanTag::T_Sort) {
                subplan_ = SortPlan::deserialize(src + off_subplan, sm_mgr);
            }

            return std::make_shared<ProjectionPlan>(tag, sql_id, plan_id, subplan_, sel_cols_);
        }

        std::shared_ptr<Plan> subplan_;
        std::vector<TabCol> sel_cols_;
        
};

// dml语句，包括insert; delete; update; select语句　
class DMLPlan : public Plan
{
    public:
        DMLPlan(PlanTag tag, std::shared_ptr<Plan> subplan,std::string tab_name, int tab_id, 
                std::vector<Value> values, std::vector<Condition> conds,
                std::vector<SetClause> set_clauses)
        {
            Plan::tag = tag;
            subplan_ = std::move(subplan);
            tab_name_ = std::move(tab_name);
            tab_id_ = tab_id;
            values_ = std::move(values);
            conds_ = std::move(conds);
            set_clauses_ = std::move(set_clauses);
        }

        void format_print() override {
            subplan_->format_print();
        }

        int plan_tree_size() override {
            return 1 + subplan_->plan_tree_size();
        }

        int serialize(char* dest) override {
            int offset = sizeof(int);

            /*
                sql_id & plan_id
            */
            memcpy(dest + offset, (char *)&sql_id_, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, (char *)&plan_id_, sizeof(int));
            offset += sizeof(int);


            memcpy(dest + offset, &tag, sizeof(PlanTag));
            offset += sizeof(PlanTag);

            int tab_name_size = tab_name_.length();
            memcpy(dest + offset, &tab_name_size, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, tab_name_.c_str(), tab_name_size);
            offset += tab_name_size;

            memcpy(dest + offset, &tab_id_, sizeof(int));
            offset += sizeof(int);

            int val_num = values_.size();
            memcpy(dest + offset, &val_num, sizeof(int));
            offset += sizeof(int);
            for(auto& val: values_) val.serialize(dest, offset);

            int cond_num = conds_.size();
            memcpy(dest + offset, &cond_num, sizeof(int));
            offset += sizeof(int);
            for(auto& cond: conds_) cond.serialize(dest, offset);

            int clause_num = set_clauses_.size();
            memcpy(dest + offset, &clause_num, sizeof(int));
            offset += sizeof(int);
            for(auto& clause: set_clauses_) clause.serialize(dest, offset);

            int off_subplan = 0;
            memcpy(dest + offset, &off_subplan, sizeof(int));
            offset += sizeof(int);

            memcpy(dest, &offset, sizeof(int));

            int subplan_size = subplan_->serialize(dest + offset);

            return offset + subplan_size;
        }

        static std::shared_ptr<DMLPlan> deserialize(char* src, SmManager* sm_mgr) {
            int offset = 0;
            int tot_size = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);

            /*
                sql_id & plan_id
            */
            int sql_id = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);
            int plan_id = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);


            PlanTag tag = *reinterpret_cast<const PlanTag*>(src + offset);
            offset += sizeof(PlanTag);

            int tab_name_size = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::string tab_name_(src + offset, tab_name_size);
            offset += tab_name_size;

            int tab_id_ = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);

            int val_num = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::vector<Value> values_;
            for(int i = 0; i < val_num; ++i) {
                Value val;
                val.deserialize(src, offset);
                values_.push_back(std::move(val));
            }

            int cond_num = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::vector<Condition> conds_;
            for(int i = 0; i < cond_num; ++i) {
                Condition cond;
                cond.deserialize(src, offset);
                conds_.push_back(std::move(cond));
            }

            int clause_num = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::vector<SetClause> set_clauses_;
            for(int i = 0; i < clause_num; ++i) {
                SetClause clause;
                clause.deserialize(src, offset);
                set_clauses_.push_back(std::move(clause));
            }

            int off_subplan = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);

            assert(offset == tot_size);
            src = src + offset;
            PlanTag subplan_tag = *reinterpret_cast<const PlanTag*>(src + off_subplan + sizeof(int));
            std::shared_ptr<Plan> subplan_;

            if(subplan_tag == PlanTag::T_Projection) {
                subplan_ = ProjectionPlan::deserialize(src + off_subplan, sm_mgr);
            }
            else if(subplan_tag == PlanTag::T_SeqScan || PlanTag::T_IndexScan) {
                subplan_ = ScanPlan::deserialize(src + off_subplan, sm_mgr);
            }
            else if(subplan_tag == PlanTag::T_Sort) {
                subplan_ = SortPlan::deserialize(src + off_subplan, sm_mgr);
            }
            else if(subplan_tag == PlanTag::T_NestLoop) {
                subplan_ = JoinPlan::deserialize(src + offset, sm_mgr);
            }

            return std::make_shared<DMLPlan>(tag, subplan_, tab_name_, tab_id_, values_, conds_, set_clauses_);
        }

        ~DMLPlan(){}
        std::shared_ptr<Plan> subplan_;
        std::string tab_name_;
        int tab_id_;
        std::vector<Value> values_;
        std::vector<Condition> conds_;
        std::vector<SetClause> set_clauses_;
};

// ddl语句, 包括create/drop table; create/drop index;
class DDLPlan : public Plan
{
    public:
        DDLPlan(PlanTag tag, std::string tab_name, std::vector<std::string> col_names, std::vector<ColDef> cols, std::vector<std::string> pkeys)
        {
            Plan::tag = tag;
            tab_name_ = std::move(tab_name);
            cols_ = std::move(cols);
            tab_col_names_ = std::move(col_names);
            pkeys_ = std::move(pkeys);
        }
        ~DDLPlan(){}

        void format_print() override {

        }

        int plan_tree_size() override {
            return 1;
        }

        int serialize(char* dest) override {
            int offset = sizeof(int);

            /*
                sql_id & plan_id
            */
            memcpy(dest + offset, (char *)&sql_id_, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, (char *)&plan_id_, sizeof(int));
            offset += sizeof(int);

            memcpy(dest + offset, &tag, sizeof(PlanTag));
            offset += sizeof(PlanTag);

            int tab_name_len = tab_name_.length();
            memcpy(dest + offset, &tab_name_len, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, tab_name_.c_str(), tab_name_len);
            offset += tab_name_len;

            int tab_col_size = tab_col_names_.size();
            memcpy(dest + offset, &tab_col_size, sizeof(int));
            offset += sizeof(int);
            for(auto& col_name: tab_col_names_) {
                int name_size = col_name.length();
                memcpy(dest + offset, &name_size, sizeof(int));
                offset += sizeof(int);
                memcpy(dest + offset, col_name.c_str(), name_size);
                offset += name_size;
            }

            int col_num = cols_.size();
            memcpy(dest + offset, &col_num, sizeof(int));
            offset += sizeof(int);
            for(auto& col: cols_) col.serialize(dest, offset);

            int pkey_num = pkeys_.size();
            memcpy(dest + offset, &pkey_num, sizeof(int));
            offset += sizeof(int);
            for(auto& pkey: pkeys_) {
                int key_size = pkey.length();
                memcpy(dest + offset, &key_size, sizeof(int));
                offset += sizeof(int);
                memcpy(dest + offset, pkey.c_str(), key_size);
                offset += key_size;
            }

            memcpy(dest, &offset, sizeof(int));
            
            return offset;
        }

        static std::shared_ptr<DDLPlan> deserialize(char* src, SmManager* sm_mgr) {
            int offset = 0;
            int tot_size = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);

            /*
                sql_id & plan_id
            */
            int sql_id = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);
            int plan_id = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);


            PlanTag tag = *reinterpret_cast<const PlanTag*>(src + offset);
            offset += sizeof(PlanTag);

            int tab_name_len = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::string tab_name(src + offset, tab_name_len);
            offset += tab_name_len;

            int tab_col_size = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::vector<std::string> tab_col_names_;
            for(int i = 0; i < tab_col_size; ++i) {
                int name_size = *reinterpret_cast<const int*>(src + offset);
                offset += sizeof(int);
                std::string col_name(src + offset, name_size);
                offset += name_size;
                tab_col_names_.push_back(std::move(col_name));
            }

            int col_num = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::vector<ColDef> cols_;
            for(int i = 0; i < col_num; ++i) {
                ColDef col;
                col.deserialize(src, offset);
                cols_.push_back(std::move(col));
            }

            int pkey_num = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::vector<std::string> pkeys_;
            for(int i = 0; i < pkey_num; ++i) {
                int key_size = *reinterpret_cast<const int*>(src + offset);
                offset += sizeof(int);
                std::string key(src + offset, key_size);
                offset += key_size;
                pkeys_.push_back(std::move(key));
            }

            assert(offset == tot_size);

            return std::make_shared<DDLPlan>(tag, tab_name, tab_col_names_, cols_, pkeys_);
        }

        std::string tab_name_;
        std::vector<std::string> tab_col_names_;
        std::vector<ColDef> cols_;
        std::vector<std::string> pkeys_;
};

// help; show tables; desc tables; begin; abort; commit; rollback语句对应的plan
class OtherPlan : public Plan
{
    public:
        OtherPlan(PlanTag tag, std::string tab_name)
        {
            Plan::tag = tag;
            tab_name_ = std::move(tab_name);            
        }
        ~OtherPlan(){}

        void format_print() override {
            
        }

        int plan_tree_size() override {
            return 1;
        }
        
        int serialize(char* dest) override {
            int offset = sizeof(int);

            /*
                sql_id & plan_id
            */
            memcpy(dest + offset, (char *)&sql_id_, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, (char *)&plan_id_, sizeof(int));
            offset += sizeof(int);

            memcpy(dest + offset, &tag, sizeof(PlanTag));
            offset += sizeof(PlanTag);
            int tab_name_size = tab_name_.length();
            memcpy(dest + offset, &tab_name_size, sizeof(int));
            offset += sizeof(int);
            memcpy(dest + offset, tab_name_.c_str(), tab_name_size);
            offset += tab_name_size;
            memcpy(dest, &offset, sizeof(int));
            return offset;
        }

        static std::shared_ptr<OtherPlan> deserialize(char* src, SmManager* sm_mgr) {
            int offset = 0;
            int tot_size = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);

            /*
                sql_id & plan_id
            */
            int sql_id = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);
            int plan_id = *reinterpret_cast<const int *>(src + offset);
            offset += sizeof(int);
            

            PlanTag tag = *reinterpret_cast<const PlanTag*>(src + offset);
            offset += sizeof(PlanTag);

            int tab_name_size = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            std::string tab_name_(src + offset, tab_name_size);
            offset += tab_name_size;

            assert(offset == tot_size);

            return std::make_shared<OtherPlan>(tag, tab_name_);
        }

        std::string tab_name_;
};

class plannerInfo{
    public:
    std::shared_ptr<ast::SelectStmt> parse;
    std::vector<Condition> where_conds;
    std::vector<TabCol> sel_cols;
    std::shared_ptr<Plan> plan;
    std::vector<std::shared_ptr<Plan>> table_scan_executors;
    std::vector<SetClause> set_clauses;
    plannerInfo(std::shared_ptr<ast::SelectStmt> parse_):parse(std::move(parse_)){}

};
