-- 运送优先级查询
select
l_orderkey,
sum(l_extendedprice*(1-l_discount)) as revenue, 
o_orderdate,
o_shippriority
from
customer, orders, lineitem 
where
c_mktsegment = '[SEGMENT]' 
and c_custkey = o_custkey
and l_orderkey = o_orderkey
and o_orderdate < date '2024-03-03' 
and l_shipdate > date '2024-01-01'
group by 
l_orderkey,
o_orderdate, 
o_shippriority 
order by 
revenue desc, 
o_orderdate;

