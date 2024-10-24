-- 货运存在问题的查询
select
c_custkey, c_name, 
sum(l_extendedprice * (1 - l_discount)) as revenue, 
c_acctbal,
n_name, c_address, c_phone, c_comment
from
customer, orders, lineitem, nation
where
c_custkey = o_custkey
and l_orderkey = o_orderkey
and o_orderdate >= date '1993-10-01' 
and o_orderdate < date '1993-10-01' + interval '3' month 
and l_returnflag = 'R' 
and c_nationkey = n_nationkey
group by
c_custkey,
c_name,
c_acctbal,
c_phone,
n_name,
c_address,
c_comment
order by
revenue desc;

select c_custkey, c_name, l_extendedprice, l_discount, c_acctbal, n_name, c_address, c_phone, c_comment from lineitem, orders, customer, nation where c_nationkey = 3 and c_nationkey = n_nationkey and o_orderdate <= '1992-03-01' and l_shipdate <= '1992-02-01' and c_custkey = o_custkey and l_orderkey = o_orderkey order by n_name desc;