#pragma once

#include "benchmark/native_txn.h"
#include "benchmark/util/clock.h"
#include "benchmark/util/random.h"


/**
 * // New order transaction's commands
    "SELECT C_CUSTKEY FROM HAT.CUSTOMER WHERE C_NAME = ?",
    "SELECT P_PRICE FROM HAT.PART WHERE P_PARTKEY = ?",
    "SELECT S_SUPPKEY FROM HAT.SUPPLIER WHERE S_NAME = ?",
    "SELECT D_DATEKEY FROM HAT.DATE WHERE D_DATE = ?",
    "INSERT INTO HAT.LINEORDER(LO_ORDERKEY, LO_LINENUMBER, LO_CUSTKEY, LO_PARTKEY, LO_SUPPKEY, LO_ORDERDATE, "\
    "LO_ORDPRIORITY, LO_SHIPPRIORITY, LO_QUANTITY, LO_EXTENDEDPRICE, LO_DISCOUNT, LO_REVENUE, LO_SUPPLYCOST, "\
    "LO_TAX, LO_COMMITDATE, LO_SHIPMODE)\n"
    "   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\n",
 */

class HATNewOrderTransaction : public NativeTransaction {
public:
    int order_key;
    int line_number;
    int cust_key;
    int part_key;
    int supp_key;
    int order_date;
    char ord_priority[15];
    char ship_priority;
    int quantity;
    float extended_price;
    int discount;
    float revenue;
    float supply_cost;
    int tax;
    int commit_date;
    char ship_mode[10];

    SystemClock* clock;

    HATNewOrderTransaction() {
        clock = new SystemClock();
    }
    ~HATNewOrderTransaction() {
        delete clock;
    }

    void generate_new_txn() override {
        queries.clear();
        // Generate parameters for the new order transaction here
        // For simplicity, we use random values; in practice, these should follow TPC-H specifications

        order_key = RandomGenerator::generate_random_int(1, 1000000);
        line_number = 1; // For simplicity, we use 1; in practice, this would increment per line item
        cust_key = RandomGenerator::generate_random_int(1, 150000);
        part_key = RandomGenerator::generate_random_int(1, 200000);
        supp_key = RandomGenerator::generate_random_int(1, 10000);
        order_date = 20240101; // Fixed date for simplicity
        strcpy(ord_priority, "5-LOW");
        ship_priority = '1';
        quantity = RandomGenerator::generate_random_int(1, 50);
        extended_price = RandomGenerator::generate_random_float(100.0, 10000.0);
        discount = RandomGenerator::generate_random_int(0, 10);
        revenue = extended_price * (1 - discount / 100.0);
        supply_cost = extended_price * 0.6; // Assume supply cost is 60% of extended price
        tax = RandomGenerator::generate_random_int(0, 8);
        commit_date = order_date + 30; // Assume commit date is 30 days after order date
        strcpy(ship_mode, "AIR");

        queries.push_back("begin;");
        std::string sql = "INSERT INTO LINEORDER VALUES (";
        sql += std::to_string(order_key) + ", " + std::to_string(line_number) + ", " + std::to_string(cust_key) + ", " + std::to_string(part_key) + ", ";
        sql += std::to_string(supp_key) + ", " + std::to_string(order_date) + ", '" + ord_priority + "', '" + ship_priority + "', ";
        sql += std::to_string(quantity) + ", " + std::to_string(extended_price) + ", " + std::to_string(discount) + ", ";
        sql += std::to_string(revenue) + ", " + std::to_string(supply_cost) + ", " + std::to_string(tax) + ", ";
        sql += std::to_string(commit_date) + ", '" + ship_mode + "');";
        queries.push_back(sql);


        // add Q1.1
        // SELECT SUM(LO_EXTENDEDPRICE * LO_DISCOUNT) AS REVENUE
        // FROM HAT.LINEORDER, HAT.DATE
        // WHERE LO_ORDERDATE = D_DATEKEY
        // AND D_DATEKEY >= 19930101 AND D_DATEKEY <= 19931231
        // AND LO_ORDERDATE >= 19930101 AND LO_ORDERDATE <= 19931231
        // AND LO_DISCOUNT BETWEEN 1 AND 3
        // AND LO_QUANTITY < 25;
        queries.push_back("SELECT LO_EXTENDEDPRICE, LO_DISCOUNT FROM lineorder, date WHERE LO_ORDERDATE = D_DATEKEY \
        AND D_DATEKEY >= 19930101 AND D_DATEKEY <= 19931231 \
        AND LO_ORDERDATE >= 19930101 AND LO_ORDERDATE <= 19931231 \
        AND LO_DISCOUNT <= 3 \
        AND LO_QUANTITY < 25;");

        queries.push_back("commit;");
    }
};