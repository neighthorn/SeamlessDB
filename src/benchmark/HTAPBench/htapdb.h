#pragma once

/**
 * TPC-C: 
 * Warehouse(W)
 * District(10*W)
 * Customer(30000*W)
 * History(30000*W)+
 * Stock(100000*W)
 * Orderline(300000*W)+
 * Order(30000*W)+
 * Item(100000)
 * NewOrder(9000*W)+
 * TPC-H:
 * Suppiler(10000)
 * Nation(62)
 * Region(5)
 * 
 * Fixed OLTP metric with controllable OLAP exectuion, dynamic time window
*/