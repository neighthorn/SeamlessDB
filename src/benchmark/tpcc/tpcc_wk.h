#pragma once

#include <vector>

#include "new_order_txn.h"
#include "payment_txn.h"
#include "delivery_txn.h"
#include "order_status_txn.h"
#include "stock_level_txn.h"
#include "benchmark/benchmark.h"

class TPCCWK: public BenchMark {
public:
    TPCCWK() {}
    TPCCWK(SmManager* sm_mgr, IxManager* ix_mgr, int warehouse_num, MultiVersionManager* mvcc_mgr)
        : sm_mgr_(sm_mgr), ix_mgr_(ix_mgr), warehouse_num_(warehouse_num), mvcc_mgr_(mvcc_mgr) {}
    bool create_table() override;
    void load_data() override;
    void load_meta() override;
    void init_transaction(int thread_num) override;
    NativeTransaction* generate_transaction(int thread_index) override;
    NativeTransaction* get_transaction(int thread_index) override;

    SmManager* sm_mgr_;
    IxManager* ix_mgr_;
    MultiVersionManager* mvcc_mgr_;
    int warehouse_num_;
    int thread_num_ = -1;

    std::vector<NewOrderTransaction*> new_order_txns_;
    std::vector<PaymentTransaction*> payment_txns_;
};