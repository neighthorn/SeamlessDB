#include <gtest/gtest.h>

#include "system/sm_manager.h"
#include "plan.h"

TEST(PlanSerializeTest, SinglePlan_TEST_1) {
    const int per_plan_size = 1024;

    auto sm_manager_ = std::make_unique<SmManager>(nullptr, nullptr, nullptr, nullptr);
    sm_manager_->db_.name_ = "test_db";
    /*
        std::string name_;                  // table name
        std::vector<ColMeta> cols_;         // column meta
        std::vector<IndexMeta> indexes_;    // index meta, the first is the primary index
        int record_length_;                 // the length of the record (fixed), does not contain the RecordHdr
        int table_id_;

    */
    TabMeta table_test;
    table_test.name_ = "test";
    table_test.cols_.push_back(ColMeta{"test", "col1", ColType::TYPE_INT, 4, 0});
    table_test.cols_.push_back(ColMeta{"test", "col2", ColType::TYPE_FLOAT, 4, 4});
    table_test.cols_.push_back(ColMeta{"test", "col3", ColType::TYPE_STRING, 10, 8});
    table_test.cols_.push_back(ColMeta{"test", "col4", ColType::TYPE_INT, 4, 18});
    table_test.cols_.push_back(ColMeta{"test", "col5", ColType::TYPE_INT, 4, 22});
    table_test.record_length_ = 26;

    IndexMeta index_test;
    index_test.tab_name = "test";
    index_test.col_num = 1;
    index_test.col_tot_len = 4;
    index_test.cols.push_back(ColMeta{"test", "col1", ColType::TYPE_INT, 4, 0});
    table_test.indexes_.push_back(index_test);
    table_test.table_id_ = 0;

    sm_manager_->db_.SetTabMeta("test", table_test);

    // seq scan
    {
        
        //  ScanPlan(PlanTag tag, int sql_id, int plan_id, SmManager *sm_manager, std::string tab_name, std::vector<Condition> filter_conds, std::vector<Condition> index_conds) :
            // Plan(sql_id, plan_id)

        // struct Condition {
        // TabCol lhs_col;   // left-hand side column
        // CompOp op;        // comparison operator
        // bool is_rhs_val;  // true if right-hand side is a value (not a column)
        // TabCol rhs_col;   // right-hand side column
        // Value rhs_val; 
        std::vector<Condition> scan_cond;
        /*
            condition
        */
        Value value_1;  value_1.set_int(100);   value_1.init_raw(sizeof(int));
        Condition  condition_1 = {.lhs_col = {"test", "col1"}, .op = CompOp::OP_EQ, .is_rhs_val = true, .rhs_col = {"", ""}, .rhs_val = value_1};
        scan_cond.push_back(condition_1);

        Value value_2;  value_2.set_float(123.456); value_2.init_raw(sizeof(float));
        Condition   condition_2 = {.lhs_col = {"test", "col2"}, .op = CompOp::OP_GE, .is_rhs_val = true, .rhs_col = {"", ""}, .rhs_val = value_2};
        scan_cond.push_back(condition_2);

        Value value_3;  value_3.set_str("12456");   value_3.init_raw(5);
        Condition   condition_3 = {.lhs_col = {"test", "col3"}, .op = CompOp::OP_LE, .is_rhs_val = true, .rhs_col = {"", ""}, .rhs_val = value_3};
        scan_cond.push_back(condition_3);

        Condition   condition_4 = {.lhs_col = {"test", "col4"}, .op = CompOp::OP_LE, .is_rhs_val = false, .rhs_col = {"test", "col5"}};
        scan_cond.push_back(condition_4);
        
        auto scan_plan = std::make_shared<ScanPlan>(PlanTag::T_SeqScan, 0, 0, sm_manager_.get(), "test", scan_cond, std::vector<Condition>(), std::vector<TabCol>());
        char *buf = new char[per_plan_size];
        int actual_size = scan_plan->serialize(buf);

        // deserialize
        auto basic_plan = ScanPlan::deserialize(buf, sm_manager_.get());
        auto de_scan_plan = std::reinterpret_pointer_cast<ScanPlan>(basic_plan);
        //
        EXPECT_EQ(de_scan_plan->sql_id_, 0);
        EXPECT_EQ(de_scan_plan->plan_id_, 0);
        EXPECT_EQ(de_scan_plan->tag, PlanTag::T_SeqScan);

        EXPECT_EQ(de_scan_plan->tab_name_, "test");
        EXPECT_EQ(de_scan_plan->cols_.size(), 5);
        EXPECT_EQ(de_scan_plan->cols_[0].name, "col1");
        EXPECT_EQ(de_scan_plan->cols_[0].type, ColType::TYPE_INT);
        // 测试de_scan_plan的所有cols和所有condition属性
        EXPECT_EQ(de_scan_plan->cols_[0].len, 4);
        EXPECT_EQ(de_scan_plan->cols_[0].offset, 0);
            table_test.cols_.push_back(ColMeta{"test", "col5", ColType::TYPE_INT, 4, 22});
        EXPECT_EQ(de_scan_plan->cols_[4].name, "col5");
        EXPECT_EQ(de_scan_plan->cols_[4].type, ColType::TYPE_INT);
        EXPECT_EQ(de_scan_plan->cols_[4].offset, 22);
        EXPECT_EQ(de_scan_plan->filter_conds_[0].rhs_val.int_val, scan_cond[0].rhs_val.int_val);
        EXPECT_EQ(de_scan_plan->filter_conds_[1].rhs_val.float_val, scan_cond[1].rhs_val.float_val);
        EXPECT_EQ(de_scan_plan->filter_conds_[2].lhs_col.col_name, scan_cond[2].lhs_col.col_name);
        EXPECT_EQ(de_scan_plan->filter_conds_[3].rhs_col.col_name, scan_cond[3].rhs_col.col_name);
        EXPECT_EQ(de_scan_plan->len_, scan_plan->len_);
    }

    // index scan
    {

    }
    // join plan
    {

    }
}

TEST(PlanSerializeTest, SQL_TEST_1){
    
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

