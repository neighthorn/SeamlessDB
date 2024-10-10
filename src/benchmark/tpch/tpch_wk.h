#pragma once

#include <vector>

#include "benchmark/benchmark.h"
#include "tpch_queries.h"

class TPCHWK: public BenchMark {
public:
    TPCHWK() {}
    TPCHWK(SmManager* sm_mgr, IxManager* ix_mgr, int sf, MultiVersionManager* mvcc_mgr)
        : sm_mgr_(sm_mgr), ix_mgr_(ix_mgr), sf_(sf), mvcc_mgr_(mvcc_mgr) {}
    bool create_table() override;
    void load_data() override;
    void load_meta() override;
    void init_transaction(int thread_num) override;
    NativeTransaction* generate_transaction(int thread_index) override;
    NativeTransaction* get_transaction(int thread_index) override;

    SmManager* sm_mgr_;
    IxManager* ix_mgr_;
    MultiVersionManager* mvcc_mgr_;
    
    int sf_;                            // scale factor
    int thread_num_ = -1;

    std::vector<Query5  *>      queries5;
    std::vector<Query10 *>      queries10;
    std::vector<QueryExample *> queries_example;

};