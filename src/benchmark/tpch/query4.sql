--  订单优先级查询

select
o_orderpriority, 
count(*) as order_count 
from orders
where
o_orderdate >= date '2024-01-01'
and o_orderdate < date '2024-06-06' + interval '3' month 
and exists ( 
select
*
from
lineitem
where
l_orderkey = o_orderkey
and l_commitdate < l_receiptdate
)
group by 
o_orderpriority
order by 
o_orderpriority;
