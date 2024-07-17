#include <gtest/gtest.h>

#include "state_item/op_state.h"
#include "op_state_manager.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_block_join.h"

int state_open_ = 1;
double state_theta_ = 1.0;
double src_scale_factor_ = 1000.0;

TEST(OperatorStateTest, indexScanOperatorTest) {
    // 构造sm_manager
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

    sm_manager_->primary_index_.emplace("test", nullptr);
    sm_manager_->old_versions_.emplace("test", nullptr);

    /*
        构造
    */

    /*
        构造Context
    */
    Context *context = nullptr;


    /*
        index scan operator
    */
    {
       
        /*
            构造sql_id and op_id
        */
        int sql_id  = 100;
        int op_id   = 1;

        /*
            构造conditions
        */
        std::vector<Condition> conds;


        /*
            构造index scan operator
        */

        std::vector<Condition> filter_conds;
        std::vector<Condition> index_conds;
        auto index_scan_op_left     = std::make_unique<IndexScanExecutor>(sm_manager_.get(), table_test.name_, filter_conds, index_conds, context, sql_id, op_id++);
        auto index_scan_op_right    = std::make_unique<IndexScanExecutor>(sm_manager_.get(), table_test.name_, filter_conds, index_conds, context, sql_id, op_id++);


        IndexScanOperatorState index_scan_op_state(index_scan_op_left.get());

        EXPECT_EQ(index_scan_op_state.sql_id_, 100);
        EXPECT_EQ(index_scan_op_state.operator_id_, 1);
        EXPECT_EQ(index_scan_op_state.exec_type_, ExecutionType::INDEX_SCAN);
        

        /*
            serialize
        */
        char dest[4096];
        size_t actual_size = index_scan_op_state.serialize(dest);
        /*
            deserialize
        */
        IndexScanOperatorState de_scan_op_state;
        de_scan_op_state.deserialize(dest, actual_size);

        EXPECT_EQ(de_scan_op_state.sql_id_, index_scan_op_state.sql_id_);
        EXPECT_EQ(de_scan_op_state.operator_id_, index_scan_op_state.operator_id_);
        EXPECT_EQ(de_scan_op_state.exec_type_, index_scan_op_state.exec_type_);
        EXPECT_EQ(de_scan_op_state.lower_rid_, index_scan_op_state.lower_rid_);
        EXPECT_EQ(de_scan_op_state.upper_rid_, index_scan_op_state.upper_rid_);
        EXPECT_EQ(de_scan_op_state.current_rid_, index_scan_op_state.current_rid_);
        EXPECT_EQ(de_scan_op_state.getSize(), actual_size);

        // index_scan_op_left->
        /*
            Test 
        */

        // auto block_join_op = std::make_shared<BlockNestedLoopJoinExecutor>(std::move(index_scan_op_left), std::move(index_scan_op_right), 
        //                     conds, context, int sql_id, int operator_id); 
    }
}


int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}