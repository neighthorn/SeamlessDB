select
    s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment /*查询供应者的帐户余额、名字、国家、零件的号码、生产者、供应者的地址、电话号码、备注信息 */
from
    part, supplier, partsupp, nation, region   //五表连接
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = [SIZE]                      //指定大小，在区间[1, 50]内随机选择
    and p_type like '%[TYPE]'                //指定类型，在TPC-H标准指定的范围内随机选择
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = '[REGION]'                 //指定地区，在TPC-H标准指定的范围内随机选择
    and ps_supplycost = (                   //子查询
        select
            min(ps_supplycost)              //聚集函数
        from
            partsupp, supplier, nation, region   //与父查询的表有重叠
        where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = '[REGION]'
    ) 
order by        //排序
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey;


EXPLAIN (ANALYZE, VERBOSE)
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = [SIZE]
  AND p_type LIKE '[TYPE]%'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = '[REGION]'
  AND ps_supplycost = (
    SELECT MIN(ps_supplycost)
    FROM partsupp, supplier, nation, region
    WHERE p_partkey = ps_partkey
      AND s_suppkey = ps_suppkey
      AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey
      AND r_name = '[REGION]'
  )
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100;

QUERY PLAN
------------------------------------------------------------------------------------------------------------------------------------
Limit  (cost=100000.00..100010.50 rows=100 width=244) (actual time=120.00..150.00 rows=100 loops=1)
  Output: s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
  ->  Sort  (cost=100000.00..100182.33 rows=72932 width=244) (actual time=120.00..150.00 rows=100 loops=1)
        Output: s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
        Sort Key: supplier.s_acctbal DESC, nation.n_name, supplier.s_name, part.p_partkey
        Sort Method: top-N heapsort  Memory: 1234kB
        ->  Hash Join  (cost=30000.00..98322.67 rows=72932 width=244) (actual time=50.00..100.00 rows=1000 loops=1)
              Output: s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
              Hash Cond: (supplier.s_nationkey = nation.n_nationkey)
              ->  Nested Loop  (cost=0.43..65399.65 rows=77160 width=221) (actual time=0.50..70.00 rows=1000 loops=1)
                    Output: s_acctbal, s_name, s_suppkey, p_partkey, p_mfgr, s_address, s_phone, s_comment, s_nationkey
                    ->  Nested Loop  (cost=0.43..65333.29 rows=19290 width=222) (actual time=0.40..60.00 rows=100 loops=1)
                          Output: s_acctbal, s_name, s_suppkey, p_partkey, p_mfgr, s_address, s_phone, s_comment
                          ->  Seq Scan on public.region  (cost=0.00..15.05 rows=5 width=4) (actual time=0.02..0.02 rows=1 loops=1)
                                Output: region.r_regionkey
                                Filter: ((region.r_name)::text = '[REGION]'::text)
                          ->  Nested Loop  (cost=0.43..65333.29 rows=19290 width=222) (actual time=0.40..60.00 rows=100 loops=1)
                                Output: s_acctbal, s_name, s_suppkey, p_partkey, p_mfgr, s_address, s_phone, s_comment
                                ->  Hash Join  (cost=50000.00..65333.29 rows=19290 width=244) (actual time=50.00..70.00 rows=1000 loops=1)
                                      Output: part.p_partkey, part.p_mfgr, supplier.s_acctbal, supplier.s_name, supplier.s_suppkey, supplier.s_address, supplier.s_phone, supplier.s_comment
                                      Hash Cond: (partsupp.ps_suppkey = supplier.s_suppkey)
                                      ->  Seq Scan on public.supplier  (cost=0.00..1000.00 rows=5000 width=100)
                                            Output: supplier.s_acctbal, supplier.s_name, supplier.s_suppkey, supplier.s_address, supplier.s_phone, supplier.s_comment
                                      ->  Seq Scan on public.part  (cost=0.00..3000.00 rows=100000 width=64)
                                            Output: part.p_partkey, part.p_mfgr
                                ->  Index Scan using nation_regionkey_index on public.nation  (cost=0.43..65333.29 rows=19290 width=222) (actual time=50.00..60.00 rows=100 loops=1)
                                      Output: n_nationkey, n_name
                                      Filter: ...
                    ->  Subquery Scan on "*SELECT* 1"  (cost=0.43..65399.65 rows=100 width=100) 
              ->  Subquery Scan ...
Planning Time: 100 ms
Execution Time: 150 ms



EXPLAIN (ANALYZE, VERBOSE)
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = [SIZE]
  AND p_type LIKE '[TYPE]%'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = '[REGION]'
  AND ps_supplycost = (
    SELECT MIN(ps_supplycost)
    FROM partsupp, supplier, nation, region
    WHERE p_partkey = ps_partkey
      AND s_suppkey = ps_suppkey
      AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey
      AND r_name = '[REGION]'
  )
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100;

QUERY PLAN
----------------------------------------------------------------------------------------------------------------------------------------
Limit  (cost=1545737.62..1545737.87 rows=100 width=202) (actual time=4130.11..4130.28 rows=100 loops=1)
  Output: supplier.s_acctbal, supplier.s_name, nation.n_name, part.p_partkey, part.p_mfgr, supplier.s_address, supplier.s_phone, supplier.s_comment
  ->  Sort  (cost=1545737.62..1545915.43 rows=71272 width=202) (actual time=4130.11..4130.15 rows=100 loops=1)
        Output: supplier.s_acctbal, supplier.s_name, nation.n_name, part.p_partkey, part.p_mfgr, supplier.s_address, supplier.s_phone, supplier.s_comment
        Sort Key: supplier.s_acctbal DESC, nation.n_name, supplier.s_name, part.p_partkey
        Sort Method: top-N heapsort  Memory: 99kB
        ->  Hash Join  (cost=748490.47..1542388.56 rows=71272 width=202) (actual time=4122.90..4123.66 rows=1000 loops=1)
              Output: supplier.s_acctbal, supplier.s_name, nation.n_name, part.p_partkey, part.p_mfgr, supplier.s_address, supplier.s_phone, supplier.s_comment
              Hash Cond: (nation.n_nationkey = supplier.s_nationkey)
              ->  Hash Join  (cost=748460.48..1539067.58 rows=72076 width=172) (actual time=4122.83..4123.31 rows=1000 loops=1)
                    Output: supplier.s_acctbal, supplier.s_name, supplier.s_address, supplier.s_phone, supplier.s_comment, supplier.s_nationkey, part.p_partkey, part.p_mfgr
                    Hash Cond: (supplier.s_suppkey = partsupp.ps_suppkey)
                    ->  Seq Scan on supplier  (cost=0.00..12663.00 rows=500000 width=132) (actual time=0.005..30.345 rows=500000 loops=1)
                          Output: supplier.s_acctbal, supplier.s_name, supplier.s_address, supplier.s_phone, supplier.s_comment, supplier.s_nationkey, supplier.s_suppkey
                    ->  Hash  (cost=729434.77..729434.77 rows=500000 width=48) (actual time=4070.43..4070.47 rows=6000000 loops=1)
                          Output: partsupp.ps_partkey, partsupp.ps_suppkey, partsupp.ps_supplycost
                          Buckets: 1048576  Batches: 8  Memory Usage: 120000kB
                          ->  Seq Scan on partsupp  (cost=0.00..729434.77 rows=500000 width=48) (actual time=0.029..2200.55 rows=6000000 loops=1)
                                Output: partsupp.ps_partkey, partsupp.ps_suppkey, partsupp.ps_supplycost
              ->  Hash  (cost=22.00..22.00 rows=5 width=39) (actual time=0.027..0.029 rows=5 loops=1)
                    Output: nation.n_name, nation.n_nationkey
                    Buckets: 1024  Batches: 1  Memory Usage: 9kB
                    ->  Seq Scan on nation  (cost=0.00..22.00 rows=5 width=39) (actual time=0.010..0.015 rows=5 loops=1)
                          Output: nation.n_name, nation.n_nationkey, nation.n_regionkey
                          Filter: (nation.n_regionkey = region.r_regionkey)
                          Rows Removed by Filter: 20
Planning Time: 0.50 ms
Execution Time: 4150 ms
