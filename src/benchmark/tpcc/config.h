#ifndef CONFIG_H
#define CONFIG_H

#include <mutex>

#define TABLE_NUM 9                     // 共9张表
#define NUM_WARE 32                      // 仓库数量
#define STOCK_PER_WARE  100000          // 每个仓库有十万种商品的库存数据
#define DISTRICT_PER_WARE 10            // 每个仓库为10个地区提供服务
#define CUSTOMER_PER_DISTRICT 3000      // 每个地区有3000个用户
#define HISTORY_PER_CUSTOMER 1          // 每个用户有一条交易历史
#define ORDER_PER_DISTRICT 3000         // 每个地区有3000个订单
#define FIRST_UNPROCESSED_O_ID 2101     // 第一个未处理的订单
#define MAXITEMS 1000                 // 有多少个item

// #define TABLE_NUM 9                     // 共9张表
// #define NUM_WARE 100                     // 仓库数量
// #define STOCK_PER_WARE  10000          // 每个仓库有十万种商品的库存数据
// #define DISTRICT_PER_WARE 10            // 每个仓库为10个地区提供服务
// #define CUSTOMER_PER_DISTRICT 3000      // 每个地区有3000个用户
// #define HISTORY_PER_CUSTOMER 1          // 每个用户有一条交易历史
// #define ORDER_PER_DISTRICT 3000         // 每个地区有3000个订单
// #define FIRST_UNPROCESSED_O_ID 2101     // 第一个未处理的订单
// #define MAXITEMS 10000                 // 有多少个item

// #define TABLE_NUM 9                     // 共9张表
// #define NUM_WARE 1                      // 仓库数量
// #define STOCK_PER_WARE  10000          // 每个仓库有十万种商品的库存数据
// #define DISTRICT_PER_WARE 10            // 每个仓库为10个地区提供服务
// #define CUSTOMER_PER_DISTRICT 3000      // 每个地区有3000个用户
// #define HISTORY_PER_CUSTOMER 1          // 每个用户有一条交易历史
// #define ORDER_PER_DISTRICT 3000         // 每个地区有3000个订单
// #define FIRST_UNPROCESSED_O_ID 2101     // 第一个未处理的订单
// #define MAXITEMS 10000                 // 有多少个item

// #define TABLE_NUM 9                     // 共9张表
// #define NUM_WARE 1                      // 仓库数量
// #define STOCK_PER_WARE  1000            // 每个仓库有十万种商品的库存数据
// #define DISTRICT_PER_WARE 10            // 每个仓库为10个地区提供服务
// #define CUSTOMER_PER_DISTRICT 300       // 每个地区有3000个用户
// #define HISTORY_PER_CUSTOMER 1          // 每个用户有一条交易历史
// #define ORDER_PER_DISTRICT 300          // 每个地区有3000个订单
// #define FIRST_UNPROCESSED_O_ID 201      // 第一个未处理的订单
// #define MAXITEMS 1000                   // 有多少个item

// #define TABLE_NUM 9                     // 共9张表
// #define NUM_WARE 1                      // 仓库数量
// #define STOCK_PER_WARE  10            // 每个仓库有十万种商品的库存数据
// #define DISTRICT_PER_WARE 3            // 每个仓库为10个地区提供服务
// #define CUSTOMER_PER_DISTRICT 10       // 每个地区有3000个用户
// #define HISTORY_PER_CUSTOMER 1          // 每个用户有一条交易历史
// #define ORDER_PER_DISTRICT 10          // 每个地区有3000个订单
// #define FIRST_UNPROCESSED_O_ID 5      // 第一个未处理的订单
// #define MAXITEMS 10                   // 有多少个item

static int next_o_id[NUM_WARE+1][DISTRICT_PER_WARE+1];
static std::mutex next_o_id_mutex;

// warehouse: w=100
// item: item_count=10000
// stock: w*item_coount=1000000
// district: w*10=1000
// customer: w*10*3000=3000000
// history: w*10*3000=3000000
// orders: w*10*3000=3000000
// new_orders: 
// order_line: w*10*3000*5=15000000


#endif