#pragma once

#include "benchmark/native_txn.h"
#include "benchmark/util/clock.h"
#include "benchmark/util/random.h"

/**
 * // Count orders transaction's commands
    "SELECT COUNT(DISTINCT LO_ORDERKEY)\n"
    "FROM HAT.LINEORDER WHERE LO_CUSTKEY = ?"
 */

class HATCountOrderTransaction : public NativeTransaction {
public:
    int cust_key;

    SystemClock* clock;

    HATCountOrderTransaction() {
        clock = new SystemClock();
    }
    ~HATCountOrderTransaction() {
        delete clock;
    }

    void generate_new_txn() override {
        queries.clear();
        // Generate parameters for the count orders transaction here
        // For simplicity, we use random values; in practice, these should follow TPC-H specifications

        cust_key = RandomGenerator::generate_random_int(1, 150000);
        
        // Add the generated parameter to the queries vector
        queries.push_back(std::to_string(cust_key));
    }
};