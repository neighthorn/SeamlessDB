#ifndef NEW_ORDER_H
#define NEW_ORDER_H

#include "benchmark/native_txn.h"
#include "config.h"
#include "benchmark/util/random.h"
#include "benchmark/util/clock.h"

class NewOrderTransaction : public NativeTransaction {
public:
    int w_id;
    int d_id;
    int c_id;
    int o_id;
    int ol_cnt;
    int itemid[650];
    char ol_dist_info[25];
    char datetime[Clock::DATETIME_SIZE + 1];
    SystemClock* clock;

    NewOrderTransaction() {
        clock = new SystemClock();
    }

    void generate_new_txn() override {
        queries.clear();
        w_id = RandomGenerator::generate_random_int(1, NUM_WARE);
        d_id = RandomGenerator::generate_random_int(1, DISTRICT_PER_WARE);
        c_id = RandomGenerator::NURand(1023, 1, CUSTOMER_PER_DISTRICT);
        next_o_id_mutex.lock();
        o_id = next_o_id[w_id][d_id];
        next_o_id[w_id][d_id] ++;
        next_o_id_mutex.unlock();
        // ol_cnt = RandomGenerator::generate_random_int(5, 15);
        ol_cnt = 600;
        for(int i = 0; i < ol_cnt; ++i) itemid[i] = RandomGenerator::NURand(8191, 1, MAXITEMS);

        queries.push_back("begin;");
        for(int i = 1; i < 7; ++i) generate_next_query(i);
        queries.push_back("commit;");
    }

    void generate_next_query(int query_index) {
        std::string sql = "";
        switch(query_index) {
            case 1: {
                /*EXEC_SQL SELECT c_discount, c_last, c_credit, w_tax
                            INTO :c_discount, :c_last, :c_credit, :w_tax
                            FROM customer, warehouse
                            WHERE w_id = :w_id 
                            AND c_w_id = w_id 
                            AND c_d_id = :d_id 
                            AND c_id = :c_id;*/
                sql = "select c_discount, c_last, c_credit, w_tax from customer, warehouse" \
                        " where w_id=" + std::to_string(w_id); 
                sql += " and c_w_id=" + std::to_string(w_id) + " and c_d_id=" + std::to_string(d_id);
                sql += " and c_id=" + std::to_string(c_id) + ";";
                queries.push_back(sql);
            } break;
            case 2: {
                /*EXEC_SQL SELECT d_next_o_id, d_tax INTO :d_next_o_id, :d_tax
                           FROM district
                           WHERE d_id = :d_id
                           AND d_w_id = :w_id
                           FOR UPDATE;*/
                sql = "select d_next_o_id, d_tax from district where d_id=" + std::to_string(d_id) + " and d_w_id=" + std::to_string(w_id) + ";";
                queries.push_back(sql);
            } break;
            case 3: {
                /*EXEC_SQL UPDATE district SET d_next_o_id = :d_next_o_id + 1
                            WHERE d_id = :d_id 
                            AND d_w_id = :w_id;*/
                int d_next_o_id = o_id;
                sql = "update district set d_next_o_id=" + std::to_string(d_next_o_id) + " where d_id=" + std::to_string(d_id) + " and d_w_id=" + std::to_string(w_id) + ";";
                queries.push_back(sql);
            } break;
            case 4: {
                /*EXEC_SQL INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,
			               o_entry_d, o_ol_cnt, o_all_local)
		                   VALUES(:o_id, :d_id, :w_id, :c_id, 
		                   :datetime,
                           :o_ol_cnt, :o_all_local);*/
                /*
                insert into orders values(o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local);
                */
               memset(datetime, 0, Clock::DATETIME_SIZE);
                clock->getDateTimestamp(datetime);
                int o_carrier_id = RandomGenerator::generate_random_int(1, 100);

                sql = "insert into orders values (";
                sql += std::to_string(o_id) + ", " + std::to_string(d_id) + ", " + std::to_string(w_id) + ", " + std::to_string(c_id) + ", '";
                sql.append(datetime);
                sql += "', " + std::to_string(o_carrier_id) + ", " + std::to_string(ol_cnt) + ", 1);";
                // queries.push_back(sql);
            } break;
            case 5: {
                /* EXEC_SQL INSERT INTO new_orders (no_o_id, no_d_id, no_w_id)
	                        VALUES (:o_id,:d_id,:w_id); */
                /*
                insert into new_orders values(o_id, d_id, w_id);
                */
                sql = "insert into new_orders values (" + std::to_string(o_id) + ", " + std::to_string(d_id) + ", " + std::to_string(w_id) + ");";
                // queries.push_back(sql);
            } break;
            case 6: {
                for(int ol_number = 1; ol_number <= ol_cnt; ++ol_number) {
                    int ol_supply_w_id = w_id;
                    int ol_i_id = itemid[ol_number - 1];
                    /*EXEC_SQL SELECT i_price, i_name, i_data
                           INTO :i_price, :i_name, :i_data
                           FROM item
                           WHERE i_id = :ol_i_id;*/
                    sql = "select i_price, i_name, i_data from item where i_id=" + std::to_string(ol_i_id) + ";";
                    queries.push_back(sql);

                    /*EXEC_SQL SELECT s_quantity, s_data, s_dist_01, s_dist_02,
		                   s_dist_03, s_dist_04, s_dist_05, s_dist_06,
		                   s_dist_07, s_dist_08, s_dist_09, s_dist_10
                           INTO :s_quantity, :s_data, :s_dist_01, :s_dist_02,
                           :s_dist_03, :s_dist_04, :s_dist_05, :s_dist_06,
                           :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
                           FROM stock
                           WHERE s_i_id = :ol_i_id 
                           AND s_w_id = :ol_supply_w_id
                           FOR UPDATE;*/
                    sql = "select s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 ";
                    sql += "from stock where s_i_id=" + std::to_string(ol_i_id) + " and s_w_id=" + std::to_string(ol_supply_w_id) + ";";
                    queries.push_back(sql);

                    /*EXEC_SQL UPDATE stock SET s_quantity = :s_quantity
                           WHERE s_i_id = :ol_i_id 
                           AND s_w_id = :ol_supply_w_id;*/
                    int s_quantity = RandomGenerator::generate_random_int(1, 10);
                    sql = "update stock set s_quantity=" + std::to_string(s_quantity) + " where s_i_id=" +std::to_string(ol_i_id) + " and s_w_id=" + std::to_string(ol_supply_w_id) + ";";
                    queries.push_back(sql);

                    /*EXEC_SQL INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, 
						   ol_number, ol_i_id, 
						   ol_supply_w_id, ol_quantity, 
						   ol_amount, ol_dist_info)
                           VALUES (:o_id, :d_id, :w_id, :ol_number, :ol_i_id,
                           :ol_supply_w_id, :ol_quantity, :ol_amount,
                           :ol_dist_info);*/
                    /* insert into order_line values (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info);
                    */
                    memset(datetime, 0, Clock::DATETIME_SIZE );
                    clock->getDateTimestamp(datetime);
                    float ol_amount = RandomGenerator::generate_random_float(1, 1000);
                    
                    memset(ol_dist_info, 0, strlen(ol_dist_info));
                    RandomGenerator::generate_random_str(ol_dist_info, 24);

                    sql = "insert into order_line values (" + std::to_string(o_id) + ", " + std::to_string(d_id) + ", " + std::to_string(w_id);
                    sql += ", " + std::to_string(ol_number) + ", " + std::to_string(ol_i_id) + ", " + std::to_string(ol_supply_w_id) + ", ";
                    sql += "'";
                    sql.append(datetime);
                    sql += "', " + std::to_string(s_quantity) + ", " + std::to_string(ol_amount) + ", '";
                    sql.append(ol_dist_info);
                    sql += "');";
                    // queries.push_back(sql);
                }
            } break;
            default:
            break;
        }
    }
};

#endif