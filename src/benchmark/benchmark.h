#pragma once

#include "system/sm_meta.h"
#include "system/sm_manager.h"
#include "native_txn.h"

class BenchMark{
public:
    BenchMark() {}
    virtual bool create_table() = 0;
    virtual void load_data() = 0;
    virtual void load_meta() = 0;
    virtual void init_transaction(int thread_num) = 0;
    virtual NativeTransaction* generate_transaction(int thread_index) = 0;
    virtual NativeTransaction* get_transaction(int thread_index) = 0;
};