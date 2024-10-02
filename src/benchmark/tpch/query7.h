/*
QUERY PLAN:
----------------------------------------------------------------------
  GroupAggregate  (cost=123456.00..123466.00 rows=100 width=64)
    -> Sort  (cost=123456.00..123457.00 rows=1000 width=64)
         Sort Key: supp_nation, cust_nation, l_year
         -> Hash Join  (cost=112345.00..123000.00 rows=1000 width=64)
             Hash Cond: (supplier.s_suppkey = lineitem.l_suppkey)
             -> Hash Join  (cost=111000.00..112000.00 rows=10000 width=64)
                 Hash Cond: (orders.o_orderkey = lineitem.l_orderkey)
                 -> Hash Join  (cost=110000.00..111000.00 rows=100000 width=64)
                     Hash Cond: (customer.c_custkey = orders.o_custkey)
                     -> Hash Join  (cost=109000.00..110000.00 rows=1000000 width=64)
                         Hash Cond: (supplier.s_nationkey = n1.n_nationkey)
                         -> Seq Scan on supplier  (cost=0.00..10000.00 rows=100000 width=64)
                         -> Seq Scan on nation n1  (cost=0.00..10000.00 rows=10000 width=64)
                     -> Seq Scan on customer  (cost=0.00..10000.00 rows=1000000 width=64)
                 -> Seq Scan on orders  (cost=0.00..10000.00 rows=10000000 width=64)
             -> Seq Scan on lineitem  (cost=0.00..10000.00 rows=100000000 width=64)
             -> Hash Join  (cost=105000.00..110000.00 rows=100 width=64)
                 Hash Cond: (customer.c_nationkey = n2.n_nationkey)
                 -> Seq Scan on nation n2  (cost=0.00..10000.00 rows=10000 width=64)
----------------------------------------------------------------------
*/