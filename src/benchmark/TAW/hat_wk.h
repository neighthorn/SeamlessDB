#pragma once
#include <vector>
#include "benchmark/benchmark.h"
#include "new_order.h"
#include "payment.h"
#include "count_order.h"

class HATtrickWK: public BenchMark {
public:
    HATtrickWK() {}
    HATtrickWK(SmManager* sm_mgr, IxManager* ix_mgr, int sf, MultiVersionManager* mvcc_mgr)
        : sm_mgr_(sm_mgr), ix_mgr_(ix_mgr), sf_(sf), mvcc_mgr_(mvcc_mgr) {}
    
    bool create_table() override;
    void load_data() override;
    void load_meta() override;
    void init_transaction(int thread_num) override;
    NativeTransaction* generate_transaction(int thread_index) override;
    NativeTransaction* get_transaction(int thread_index) override;

    int sf_;
    int thread_num_ = -1;
    
    SmManager* sm_mgr_;
    IxManager* ix_mgr_;
    MultiVersionManager* mvcc_mgr_;

    std::vector<HATNewOrderTransaction*>      new_order_txns_;
    std::vector<HATPaymentTransaction*>       payment_txns_;
};