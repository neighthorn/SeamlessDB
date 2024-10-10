#pragma once
#include <vector>

#include "benchmark/benchmark.h"
#include "test_txn.h"

class TestWK : public BenchMark{
public:
    TestWK() {}
    TestWK(SmManager* sm_mgr, IxManager* ix_mgr, int record_num, MultiVersionManager *mvcc_mgr) 
        : sm_mgr_(sm_mgr), ix_mgr_(ix_mgr), record_num_(record_num), mvcc_mgr_(mvcc_mgr) {}
    // functions used for storage_pool
    bool create_table() override;
    void load_data() override;
    std::string generate_name(int id);
    // fuctions used for compute_pool
    void load_meta() override;
    void init_transaction(int thread_num) override;
    NativeTransaction* generate_transaction(int thread_index) override;
    NativeTransaction* get_transaction(int thread_index) override;

    SmManager* sm_mgr_;
    IxIndexHandle* index_handle_;
    MultiVersionFileHandle *oldversion_handle_;
    IxManager* ix_mgr_;
    MultiVersionManager *mvcc_mgr_;
    int record_num_;
    std::vector<TestTxn*> test_txns_;
    
    int thread_num_ = -1;
};