#pragma once

#include "benchmark/native_txn.h"
#include "tpch_config.h"
#include "benchmark/util/random.h"
#include "benchmark/util/clock.h"


/*
Query1
select 
    l_returnflag, 
    l_linestatus, 
    l_quantity,
    l_extendedprice, 
    l_discount, 
    l_tax, 
    l_quantity 
from 
    lineitem
where 
    l_shipdate <= lower_shipdat'e;
*/

class Query1 : public NativeTransaction {
public:
    char lower_shipdate[Clock::DATETIME_SIZE + 1];
    void generate_new_txn() override {
        RandomGenerator::generate_random_str(lower_shipdate, Clock::DATETIME_SIZE + 1);
        std::string sql = "select l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax, l_quantity from lineitem where l_shipdate <= \'" + std::string(lower_shipdate, Clock::DATETIME_SIZE + 1) + "\';";
        queries.push_back(sql);
    }
};

/*
Query3
-- 运送优先级查询
select
    l_orderkey,
    l_extendedprice, 
    l_discount, 
    o_orderdate,
    o_shippriority
from
    customer, orders, lineitem 
where
    o_custkey > 1000
    and o_custkey < 5000
    and c_custkey > 1000
    and c_custkey < 5000
    and l_orderkey > 1000
    and l_orderkey < 5000
    and c_mktsegment = '[SEGMENT]' 
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '2024-03-03' 
    and l_shipdate > date '2024-01-01'
*/

// select l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority from customer, orders, lineitem where o_orderkey < 5000 and c_custkey < 5000 and l_orderkey < 5000 and c_custkey = o_custkey and l_orderkey = o_orderkey;
// select * from orders where o_orderkey > 1000 and o_orderkey < 5000;
// select * from customer where c_custkey > 1000 and c_custkey < 5000;
// select * from orders, customer, lineitem where o_orderkey < 5000 and c_custkey < 5000 and l_orderkey < 5000 and c_custkey = o_custkey and l_orderkey = o_orderkey;

// class

// select r_name, n_name, l_linenumber from region, nation, customer, supplier, orders, lineitem, partsupp, part where r_regionkey < 5 and n_nationkey < 5 and c_nationkey = 1 and s_suppkey < 10000 and o_orderdate = '1992-01-01' and o_id <= 10000 and l_shipdate = '1992-01-01' and l_id <= 10000 and ps_partkey < 10000 and p_partkey < 10000 and r_regionkey = n_regionkey and n_nationkey=c_nationkey and n_nationkey=s_nationkey and c_custkey=o_custkey and s_suppkey=l_suppkey and s_suppkey=ps_suppkey and ps_partkey=p_partkey;

class QueryExample : public NativeTransaction {
public:

    void generate_new_txn() override {

        queries.push_back("begin;");
        // queries.push_back("select l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'AUTOMOBILE' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= '1992-01-01' and o_orderdate < '1995-02-01' and l_shipdate >= '1995-05-01' and l_shipdate < '1998-12-01' order by l_orderkey;");
        queries.push_back("select n_name, l_extendedprice, l_discount from region, nation, customer, orders, lineitem, supplier where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = n_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and c_nationkey >= 6 and c_nationkey < 11 and s_nationkey >= 6 and s_nationkey < 11 and o_orderdate >= '1995-01-01' and o_orderdate < '1996-01-01' and l_shipdate >= '1992-01-01' and l_shipdate < '1998-01-01' order by l_extendedprice desc;");
        queries.push_back("commit;");

        return ;
    }
};

// select * from orders, customer, lineitem where o_orderdate = '1992-01-01' and o_id < 20000 and c_mktsegment = 'AUTOMOBILE' and c_id < 20000 and l_shipdate = '1992-01-01' and l_id < 10000 and c_custkey = o_custkey and l_orderkey = o_orderkey;
/*
Query5
select
    n_name, l_extendedprice, l_discount 
from
    customer, orders, lineitem, supplier, nation, region 
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'AFRICA'
    and o_orderdate >= date '2024-01-01' 
    and o_orderdate < date '2024-06-06' + interval '1' year
*/

class Query5 : public NativeTransaction {
public:
    char lower_orderdate[Clock::DATETIME_SIZE + 1];
    char upper_orderdate[Clock::DATETIME_SIZE + 1];
    void generate_new_txn() override {
        RandomGenerator::generate_random_str(lower_orderdate, Clock::DATETIME_SIZE + 1);
        RandomGenerator::generate_random_str(upper_orderdate, Clock::DATETIME_SIZE + 1);

        std::string sql = "select n_name, l_extendedprice, l_discount from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = \'" + std::string("AFRICA") +  "\' and o_orderdate >= \'" + std::string(lower_orderdate, Clock::DATETIME_SIZE + 1) + "\' and o_orderdate < \'" + std::string(upper_orderdate, Clock::DATETIME_SIZE + 1) + "\';";
        queries.push_back(sql);
    }
};

/*
    Q10 simpliefied:
    select
        c_custkey, c_name, l_extendedprice, l_discount, c_acctbal,
        n_name, c_address, c_phone, c_comment 
    from
        customer, order, lineitem, nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= date '[DATE]' // DATE是位于1993年一月到1994年十二月中任一月的一号
        and o_orderdate < date '[DATE]' + interval '3' month //3个月内
        and l_returnflag = 'R' //货物被回退
        and c_nationkey = n_nationkey;
*/
class Query10 : public NativeTransaction {
public:

    char lower_orderdate[Clock::DATETIME_SIZE + 1];
    char upper_orderdate[Clock::DATETIME_SIZE + 1];
    void generate_new_txn() override {
        RandomGenerator::generate_random_str(lower_orderdate, Clock::DATETIME_SIZE + 1);
        RandomGenerator::generate_random_str(upper_orderdate, Clock::DATETIME_SIZE + 1);

        std::string sql = "select c_custkey, c_name, l_extendedprice, l_discount, c_acctbal, n_name, c_address, c_phone, c_comment from customer, orders, lineitem, nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= \'" + std::string(lower_orderdate, Clock::DATETIME_SIZE + 1) + "\' and o_orderdate < \'" + std::string(upper_orderdate, Clock::DATETIME_SIZE + 1) + "\' and l_returnflag = 'R' and c_nationkey = n_nationkey;";
        queries.push_back(sql);
    }
};