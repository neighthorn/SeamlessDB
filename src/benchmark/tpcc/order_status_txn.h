#ifndef ORDER_STATUS_TXN_H
#define ORDER_STATUS_TXN_H

#include "benchmark/native_txn.h"
#include "config.h"
#include "benchmark/util/random.h"
#include "benchmark/util/clock.h"

class OrderStatusTransaction : public NativeTransaction {
public:
    int w_id;
    int d_id;
    int c_id;
    int o_id;
    char c_last[17];
    OrderStatusTransaction() {}

    void generate_new_txn() override {
        queries.clear();
        w_id = RandomGenerator::generate_random_int(1, NUM_WARE);
        d_id = RandomGenerator::generate_random_int(1, DISTRICT_PER_WARE);
        c_id = RandomGenerator::NURand(1023, 1, CUSTOMER_PER_DISTRICT);
        memset(c_last, '\0', sizeof(c_last));
        int lastname_num = RandomGenerator::NURand(255, 0, 999);
        RandomGenerator::generate_random_lastname(lastname_num, c_last);
        o_id = RandomGenerator::generate_random_int(1, 3000);

        queries.push_back("begin;");
        if(RandomGenerator::generate_random_int(1, 100) <= 60) generate_next_query(1);
        else generate_next_query(2);
        for(int i = 3; i < 5; ++i) generate_next_query(i);
        queries.push_back("commit;");
    }

    void generate_next_query(int query_index) {
        std::string sql = "";
        switch(query_index) {
            case 1: {
                // find by name

                /*EXEC_SQL SELECT count(c_id)
                            INTO :namecnt
                                FROM customer
                            WHERE c_w_id = :c_w_id
                            AND c_d_id = :c_d_id
                                AND c_last = :c_last;*/
                // sql = "select count(c_id) from customer where c_w_id=" + std::to_string(w_id);
                sql = "select count(c_id) as count_c_id from customer where c_w_id=" + std::to_string(w_id);
                sql += " and c_d_id=" + std::to_string(d_id) + " and c_last='";
                sql.append(c_last);
                sql += "';";
                queries.push_back(sql);

                /*EXEC_SQL DECLARE c_byname_o CURSOR FOR
                            SELECT c_balance, c_first, c_middle, c_last
                            FROM customer
                            WHERE c_w_id = :c_w_id
                            AND c_d_id = :c_d_id
                            AND c_last = :c_last
                            ORDER BY c_first;
                            proceed = 3;
                            EXEC_SQL OPEN c_byname_o;*/
                /*
                select c_balance, c_first, c_middle, c_last from customer where c_w_id = c_w_id and c_d_id = c_d_id and c_last = c_last order by c_first;
                */
               sql = "select c_balance, c_first, c_middle, c_last from customer where c_w_id=" + std::to_string(w_id) + " and c_d_id=";
               sql += std::to_string(d_id) + " and c_last='";
               sql.append(c_last);
               sql += "' order by c_first;";
               queries.push_back(sql);
            } break;
            case 2: {
                // fine by number
                /*EXEC_SQL SELECT c_balance, c_first, c_middle, c_last
                            INTO :c_balance, :c_first, :c_middle, :c_last
                                FROM customer
                                WHERE c_w_id = :c_w_id
                            AND c_d_id = :c_d_id
                            AND c_id = :c_id;*/
                sql = "select c_balance, c_first, c_middle, c_last from customer where c_w_id=" + std::to_string(w_id) + " and c_d_id=";
                sql += std::to_string(d_id) + " and c_id=" + std::to_string(c_id) + ";";
                queries.push_back(sql);
            } break;
            case 3: {
                /*EXEC_SQL SELECT o_id, o_entry_d, o_carrier_id
                            INTO :o_id, :o_entry_d, :o_carrier_id
                                FROM orders
                                WHERE o_w_id = :c_w_id
                            AND o_d_id = :c_d_id
                            AND o_c_id = :c_id
                            AND o_id = :o_id;*/
                sql = "select o_id, o_entry_d, o_carrier_id from orders where o_w_id=" + std::to_string(w_id) + " and o_d_id=" + std::to_string(d_id);
                sql += " and o_c_id=" + std::to_string(c_id) + " and o_id=" + std::to_string(o_id) + ";";
                queries.push_back(sql);
            } break;
            case 4: {
                /*EXEC_SQL SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount,
                            ol_delivery_d
                            FROM order_line
                                WHERE ol_w_id = :c_w_id
                            AND ol_d_id = :c_d_id
                            AND ol_o_id = :o_id;*/
                sql = "select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from order_line where ol_w_id=";
                sql += std::to_string(w_id) + " and ol_d_id=" + std::to_string(d_id) + " and ol_o_id=" + std::to_string(o_id) + ";";
                queries.push_back(sql);
            } break;
            default:
            break;
        }
    }
};

#endif