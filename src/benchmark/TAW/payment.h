#pragma once

#include "benchmark/native_txn.h"
#include "benchmark/util/clock.h"
#include "benchmark/util/random.h"

/**
 * // Payment transaction's commands
    "SELECT C_CUSTKEY, C_NAME FROM HAT.CUSTOMER WHERE C_NATION = ?",
    "UPDATE HAT.CUSTOMER\n"
    "   SET C_PAYMENTCNT = 0\n" //C_PAYMENTCNT + 1\n"
    "   WHERE C_CUSTKEY = ?",
    "UPDATE HAT.SUPPLIER\n"
    "   SET S_YTD = S_YTD - ?\n"
    "   WHERE S_SUPPKEY = ?",
    "INSERT INTO HAT.HISTORY(H_ORDERKEY, H_CUSTKEY, H_AMOUNT)\n"
    "   VALUES(?,?,?)\n",
 */

class HATPaymentTransaction : public NativeTransaction {
public:
    int cust_key;
    char cust_name[25];
    int supp_key;
    float amount;
    int order_key;

    SystemClock* clock;

    HATPaymentTransaction() {
        clock = new SystemClock();
    }
    ~HATPaymentTransaction() {
        delete clock;
    }

    void generate_new_txn() override {
        queries.clear();
        // Generate parameters for the payment transaction here
        // For simplicity, we use random values; in practice, these should follow TPC-H specifications

        cust_key = RandomGenerator::generate_random_int(1, 150000);
        strcpy(cust_name, "Customer#000000001"); // Fixed name for simplicity
        supp_key = RandomGenerator::generate_random_int(1, 10000);
        amount = RandomGenerator::generate_random_float(1, 10000);
        order_key = RandomGenerator::generate_random_int(1, 1000000);
        
        queries.push_back("begin;");
        // Add the generated parameters to the queries vector
        // update customer set c_paymentcnt = 0 where c_custkey = ?;
        queries.push_back("update customer set c_paymentcnt = 0 where c_custkey = " + std::to_string(cust_key) + ";");
        // update supplier set s_ytd = 0? where s_suppkey = ?;
        queries.push_back("update supplier set s_ytd = " + std::to_string(amount) + " where s_suppkey = " + std::to_string(supp_key) + ";");
        // insert into history(h_orderkey, h_custkey, h_amount) values(?, ?, ?);
        queries.push_back("insert into history(h_orderkey, h_custkey, h_amount) values(" + std::to_string(order_key) + ", " + std::to_string(cust_key) + ", " + std::to_string(amount) + ");");

        // add Q1.2
        // SELECT SUM(LO_EXTENDEDPRICE * LO_DISCOUNT) AS REVENUE
        // FROM HAT.LINEORDER, HAT.DATE
        // WHERE LO_ORDERDATE = D_DATEKEY
        // AND D_DATEKEY BETWEEN 19940101 AND 19940131
        // AND LO_DISCOUNT BETWEEN 4 AND 6
        // AND LO_QUANTITY BETWEEN 26 AND 35;
        queries.push_back(
            "SELECT SUM(LO_EXTENDEDPRICE * LO_DISCOUNT) AS REVENUE "
            "FROM HAT.LINEORDER, HAT.DATE "
            "WHERE LO_ORDERDATE = D_DATEKEY "
            "AND D_DATEKEY BETWEEN 19940101 AND 19940131 "
            "AND LO_DISCOUNT BETWEEN 4 AND 6 "
            "AND LO_QUANTITY BETWEEN 26 AND 35;"
        );

        queries.push_back("commit;");
    }
};