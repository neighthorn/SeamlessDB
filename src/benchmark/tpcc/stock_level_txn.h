#pragma once

#include "benchmark/native_txn.h"
#include "config.h"
#include "benchmark/util/random.h"
#include "benchmark/util/clock.h"

class StockLevelTransaction : public NativeTransaction {
public:
    int w_id;
    int d_id;
    int level;
    int o_id;

    void generate_new_txn() override {
        w_id = RandomGenerator::generate_random_int(1, NUM_WARE);
        d_id = RandomGenerator::generate_random_int(1, DISTRICT_PER_WARE);
        level = RandomGenerator::generate_random_int(1, 30);
        // o_id = next_o_id[d_id - 1];
        o_id = RandomGenerator::generate_random_int(1, next_o_id[w_id][d_id] - 1);

        queries.push_back("begin;");
        for(int i = 1; i < 4; ++i)
            generate_next_query(i);
        queries.push_back("commit;");
    }

    void generate_next_query(int query_index) {
        std::string sql = "";
        switch(query_index) {
            case 1: {
                /*EXEC_SQL SELECT d_next_o_id
                            INTO :d_next_o_id
                            FROM district
                            WHERE d_id = :d_id
                            AND d_w_id = :w_id;*/
                sql = "select d_next_o_id from district where d_id=" + std::to_string(d_id) + " and d_w_id=" + std::to_string(w_id) + ";";
                queries.push_back(sql);
            } break;
            case 2: {
                /* find the most recent 20 orders for this district */
                /*EXEC_SQL SELECT DISTINCT ol_i_id
                                FROM order_line
                                WHERE ol_w_id = :w_id
                        AND ol_d_id = :d_id
                        AND ol_o_id < : d_next_o_id
                        AND ol_o_id >= (:d_next_o_id - 20);*/
                sql = "select ol_i_id from order_line where ol_w_id=" + std::to_string(w_id) + " and ol_d_id=" + std::to_string(d_id);
                sql += " and ol_o_id<" + std::to_string(o_id) + " and ol_o_id>=" + std::to_string(o_id - 20) + ";";
                queries.push_back(sql);
            } break;
            case 3: {
                /*EXEC_SQL SELECT count(*) INTO :i_count
                            FROM stock
                            WHERE s_w_id = :w_id
                                AND s_i_id = :ol_i_id
                            AND s_quantity < :level;*/
                int ol_i_id = RandomGenerator::generate_random_int(1, 123);
                // sql = "select count(*) from stock where s_w_id=" + std::to_string(w_id) + " and s_i_id=" + std::to_string(ol_i_id) + " and s_quantity<" + std::to_string(level) + ";";
                sql = "select COUNT(*) as count_stock from stock where s_w_id=" + std::to_string(w_id) + " and s_i_id=" + std::to_string(ol_i_id) + " and s_quantity<" + std::to_string(level) + ";";
                // sql = "select * from stock where s_w_id=" + std::to_string(w_id) + " and s_i_id=" + std::to_string(ol_i_id) + " and s_quantity<" + std::to_string(level) + ";";
                queries.push_back(sql);
            } break;
            default:
            break;
        }
    }
};