#pragma once

#include "benchmark/native_txn.h"
#include "benchmark/util/random.h"
#include "config.h"
#include "benchmark/util/clock.h"

class DeliveryTransaction : public NativeTransaction {
public:
    int w_id;
    int d_id;
    int o_id;
    int c_id;
    int o_carrier_id;
    SystemClock* clock;
    DeliveryTransaction() {
        clock = new SystemClock();
    }

    void generate_new_txn() override {
        queries.clear();
        w_id = RandomGenerator::generate_random_int(1, NUM_WARE);
        o_id = RandomGenerator::generate_random_int(1, ORDER_PER_DISTRICT);
        c_id = RandomGenerator::generate_random_int(1, CUSTOMER_PER_DISTRICT);
        o_carrier_id = RandomGenerator::generate_random_int(1, 10);

        queries.push_back("begin;");
        for(d_id = 1; d_id <= DISTRICT_PER_WARE; ++d_id) {
            for(int i = 1; i < 8; ++i)
                generate_next_query(i);
        }
        queries.push_back("commit;");
    }

    void generate_next_query(int query_index) {
        std::string sql = "";
        switch(query_index) {
            case 1: {
                /*EXEC_SQL SELECT COALESCE(MIN(no_o_id),0) INTO :no_o_id
		                FROM new_orders
		                WHERE no_d_id = :d_id AND no_w_id = :w_id;*/
                /*
                select min(no_o_id, 0) from new_orders where no_d_id = :d_id and no_w_id = :w_id;
                */
                // sql = "select min(no_o_id, 0) as min_o_id from new_orders where no_d_id=" + std::to_string(d_id) + " and no_w_id=" + std::to_string(w_id) + ";";
                sql = "select no_o_id from new_orders where no_d_id=" + std::to_string(d_id) + " and no_w_id=" + std::to_string(w_id) + ";";
                queries.push_back(sql);
            } break;
            case 2: {
                /*EXEC_SQL DELETE FROM new_orders WHERE no_o_id = :no_o_id AND no_d_id = :d_id
		                    AND no_w_id = :w_id;*/
                sql = "delete from new_orders where no_o_id=" + std::to_string(o_id) + " and no_d_id=" + std::to_string(d_id) + " and no_w_id=" + std::to_string(w_id) + ";";
                queries.push_back(sql);
            } break;
            case 3: {
                /*EXEC_SQL SELECT o_c_id INTO :c_id FROM orders
		                WHERE o_id = :no_o_id AND o_d_id = :d_id
				AND o_w_id = :w_id;*/
                /*
                select o_c_id from orders where o_id = :no_o_id and o_d_id = :d_id
                */
                sql = "select o_c_id from orders where o_id=" + std::to_string(o_id) + " and o_d_id=" + std::to_string(d_id) + " and o_w_id=" + std::to_string(w_id) + ";";
                queries.push_back(sql);
            } break;
            case 4: {
                /*EXEC_SQL UPDATE orders SET o_carrier_id = :o_carrier_id
		                WHERE o_id = :no_o_id AND o_d_id = :d_id AND
				o_w_id = :w_id;*/
                sql = "update orders set o_carrier_id=" + std::to_string(o_carrier_id) + " where o_id=" + std::to_string(o_id) + " and o_d_id=" + std::to_string(d_id);
                sql += " and o_w_id=" + std::to_string(w_id) + ";";
                queries.push_back(sql);
            } break;
            case 5: {
                /*EXEC_SQL UPDATE order_line
		                SET ol_delivery_d = :datetime
		                WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND
				ol_w_id = :w_id;*/
                char datetime[Clock::DATETIME_SIZE + 1];
                clock->getDateTimestamp(datetime);
                sql = "update order_line set ol_delivery_d='";
                sql.append(datetime);
                sql += "' where ol_o_id=" + std::to_string(o_id) + " and ol_d_id=" + std::to_string(d_id) + " and ol_w_id=" + std::to_string(w_id) + ";";
                queries.push_back(sql);
            } break;
            case 6: {
                /*EXEC_SQL SELECT SUM(ol_amount) INTO :ol_total
		                FROM order_line
		                WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id
				AND ol_w_id = :w_id;*/
                // sql = "select sum(ol_amount) from order_line where ol_o_id=" + std::to_string(o_id) + " and ol_d_id=" + std::to_string(d_id);
                sql = "select sum(ol_amount) as sum_amount from order_line where ol_o_id=" + std::to_string(o_id) + " and ol_d_id=" + std::to_string(d_id);
                // sql = "select ol_amount from order_line where ol_o_id=" + std::to_string(o_id) + " and ol_d_id=" + std::to_string(d_id);
                sql += " and ol_w_id=" + std::to_string(w_id) + ";";
                queries.push_back(sql);
            } break;
            case 7: {
                /*EXEC_SQL UPDATE customer SET c_balance = c_balance + :ol_total ,
		                             c_delivery_cnt = c_delivery_cnt + 1
		                WHERE c_id = :c_id AND c_d_id = :d_id AND
				c_w_id = :w_id;*/
                /*
                update customer set c_balance = :c_balance, c_delivery_cnt = :c_delivery_cnt where c_id = :c_id and c_d_id = :d_id and c_w_id = :w_id;
                */
                float c_balance = RandomGenerator::generate_random_float(1, 20);
                int c_delivery_cnt = RandomGenerator::generate_random_int(1, 100);
                sql = "update customer set c_balance=" + std::to_string(c_balance) + ", c_delivery_cnt=" + std::to_string(c_delivery_cnt) + " where c_id=" + std::to_string(c_id);
                sql += " and c_d_id=" + std::to_string(d_id) + " and c_w_id=" + std::to_string(w_id) + ";";
                queries.push_back(sql);
            } break;
            default:
            break;
        }
    }
};