SELECT
    n1.n_name AS supp_nation,        -- 供货商国家
    n2.n_name AS cust_nation,        -- 顾客国家
    EXTRACT(YEAR FROM l_shipdate) AS l_year,  -- 年度
    SUM(l_extendedprice * (1 - l_discount)) AS revenue  -- 年度的货运收入
FROM
    supplier,
    lineitem,
    orders,
    customer,
    nation n1,       -- 供货商所在国家
    nation n2        -- 顾客所在国家
WHERE
    s_suppkey = l_suppkey           -- 供货商和订单项的关联
    AND o_orderkey = l_orderkey     -- 订单和订单项的关联
    AND c_custkey = o_custkey       -- 顾客和订单的关联
    AND s_nationkey = n1.n_nationkey -- 供货商和国家的关联
    AND c_nationkey = n2.n_nationkey -- 顾客和国家的关联
    AND (
        (n1.n_name = '[NATION1]' AND n2.n_name = '[NATION2]') 
        OR (n1.n_name = '[NATION2]' AND n2.n_name = '[NATION1]')
    ) 
    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' -- 日期范围
GROUP BY
    n1.n_name,          -- 供货商国家
    n2.n_name,          -- 顾客国家
    EXTRACT(YEAR FROM l_shipdate) -- 年度
ORDER BY
    n1.n_name,          -- 供货商国家
    n2.n_name,          -- 顾客国家
    EXTRACT(YEAR FROM l_shipdate); -- 年度

select 
n_name, n2_name, l_shipdate, l_extendedprice, l_discount 
from 
nation, supplier, lineitem, orders, nation2, customer 
where s_suppkey = l_suppkey 
    and o_orderkey = l_orderkey 
    and c_custkey = o_custkey 
    and s_nationkey = n_nationkey 
    and c_nationkey = n2_nationkey 
    and n_name = 'PERU' 
    and n2_name = 'FRANCE' 
    and c_nationkey = 16
    and s_nationkey = 9
    and l_shipdate <= '1992-12-01' order by l_shipdate;

select n_name, l_shipdate, l_extendedprice, l_discount from nation, supplier, lineitem, orders, customer where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n_nationkey and n_name = 'PERU' and c_nationkey = 16 and s_nationkey = 9 and l_shipdate <= '1992-12-01' order by l_shipdate;