select
sum(l_extendedprice*l_discount) as revenue //潜在的收入增加量
from
lineitem //单表查询
where
l_shipdate >= date '[DATE]' //DATE是从[1993, 1997]中随机选择的一年的1月1日
and l_shipdate < date '[DATE]' + interval '1' year //一年内
and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01 //between
and l_quantity < [QUANTITY]; // QUANTITY在区间[24, 25]中随机选择

select l_extendedprice, l_discount from lineitem where l_shipdate < '1993-01-01';