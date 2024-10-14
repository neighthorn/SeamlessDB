-- 运送优先级查询
select
l_orderkey,
sum(l_extendedprice*(1-l_discount)) as revenue, 
o_orderdate,
o_shippriority
from
customer, orders, lineitem 
where
c_mktsegment = 'AUTOMOBILE' 
and c_custkey = o_custkey
and l_orderkey = o_orderkey
and o_orderdate < date '1994-03-01' 
and l_shipdate < date '1994-03-01'
group by 
l_orderkey,
o_orderdate, 
o_shippriority 
order by 
revenue desc, 
o_orderdate;


select l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority from customer, orders, lineitem where c_custkey < 75000 and o_orderkey < 750000 and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-01' and l_shipdate > '1996-06-01' order by l_orderkey;
select l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'AUTOMOBILE' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-01' and l_shipdate < '1994-06-01' order by l_orderkey;