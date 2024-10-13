-- 某地区供货商为公司带来的收入查询
select
n_name,
sum(l_extendedprice * (1 - l_discount)) as revenue 
from
customer,orders,lineitem,supplier,nation,region 
where
c_custkey = o_custkey
and l_orderkey = o_orderkey
and l_suppkey = s_suppkey
and c_nationkey = s_nationkey
and s_nationkey = n_nationkey
and n_regionkey = r_regionkey
and r_name = '[REGION]' 
and o_orderdate >= date '2024-01-01' 
and o_orderdate < date '2024-06-06' + interval '1' year
group by 
n_name
order by 
revenue desc;

select n_name, l_extendedprice, l_discount 
from region, nation, supplier, customer, orders, lineitem 
where c_custkey = o_custkey 
    and l_orderkey = o_orderkey 
    and l_suppkey = s_suppkey 
    and c_nationkey = s_nationkey 
    and s_nationkey = n_nationkey 
    and n_regionkey = r_regionkey 
    and r_name = 'ASIA' 
    and o_orderdate >= '1995-01-01' 
    and o_orderdate < '1996-01-01' 
    and o_orderkey < 1500000 
    and c_custkey < 15000 
    and l_orderkey < 1500000 
    and s_suppkey < 1000 order by l_extendedprice desc;