#ifndef PAYMENT_TXN_H
#define PAYMENT_TXN_H

#include "benchmark/native_txn.h"
#include "config.h"
#include "benchmark/util/random.h"
#include "benchmark/util/clock.h"

class PaymentTransaction : public NativeTransaction {
public:
    int w_id;
    int d_id;
    int c_id;
    float h_amount;
    SystemClock* clock;

    PaymentTransaction() {
        clock = new SystemClock();    
    }

    void generate_new_txn() override {
        queries.clear();
        w_id = RandomGenerator::generate_random_int(1, NUM_WARE);
        d_id = RandomGenerator::generate_random_int(1, DISTRICT_PER_WARE);
        c_id = RandomGenerator::NURand(1023, 1, CUSTOMER_PER_DISTRICT);
        h_amount = RandomGenerator::generate_random_float(1, 5000);

        queries.push_back("begin;");
        for(int i = 1; i < 8; ++i) generate_next_query(i);
        queries.push_back("commit;");
    }

    void generate_next_query(int query_index) {
        std::string sql = "";
        switch(query_index) {
            case 1: {
                /*EXEC_SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
	                       WHERE w_id =:w_id;*/
                sql = "update warehouse set w_ytd=" + std::to_string(h_amount);
                sql += " where w_id=" + std::to_string(w_id) + ";";
                queries.push_back(sql);
            } break;
            case 2: {
                /*EXEC_SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
                            INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
                            FROM warehouse
                            WHERE w_id = :w_id;*/
                sql = "select w_street_1, w_street_2, w_city, w_state, w_zip, w_name from warehouse where w_id=" + std::to_string(w_id) + ";";
                queries.push_back(sql);
            } break;
            case 3: {
                /*EXEC_SQL UPDATE district SET d_ytd = d_ytd + :h_amount
                            WHERE d_w_id = :w_id 
                            AND d_id = :d_id;*/
                sql = "update district set d_ytd=" + std::to_string(h_amount) + " where d_w_id=" + std::to_string(w_id) + " and d_id=" + std::to_string(d_id) + ";";
                queries.push_back(sql);
            } break;
            case 4: {
                /*EXEC_SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip,
                            d_name
                            INTO :d_street_1, :d_street_2, :d_city, :d_state,
                            :d_zip, :d_name
                            FROM district
                            WHERE d_w_id = :w_id 
                            AND d_id = :d_id;*/
                sql = "select d_street_1, d_street_2, d_city, d_state, d_zip, d_name from district where d_w_id=" + std::to_string(w_id) + " and d_id=" + std::to_string(d_id) + ";";
                queries.push_back(sql);
            } break;
            case 5: {
                /*EXEC_SQL SELECT c_first, c_middle, c_last, c_street_1,
                            c_street_2, c_city, c_state, c_zip, c_phone,
                            c_credit, c_credit_lim, c_discount, c_balance,
                            c_since
                            INTO :c_first, :c_middle, :c_last, :c_street_1,
                            :c_street_2, :c_city, :c_state, :c_zip, :c_phone,
                            :c_credit, :c_credit_lim, :c_discount, :c_balance,
                            :c_since
                            FROM customer
                            WHERE c_w_id = :c_w_id 
                            AND c_d_id = :c_d_id 
                            AND c_id = :c_id
                            FOR UPDATE;*/
                sql = "select c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since";
                sql += " from customer where c_w_id=" + std::to_string(w_id) + " and c_d_id=" + std::to_string(d_id) + " and c_id=" + std::to_string(c_id) + ";";
                queries.push_back(sql);
            } break;
            case 6: {
                /*EXEC_SQL UPDATE customer 
                            SET c_balance = :c_balance
                            WHERE c_w_id = :c_w_id 
                            AND c_d_id = :c_d_id 
                            AND c_id = :c_id;*/
                float c_balance = RandomGenerator::generate_random_float(1, 20);
                sql = "update customer set c_balance=" + std::to_string(c_balance);
                sql += " where c_w_id=" + std::to_string(w_id) + " and c_d_id=" + std::to_string(d_id) + " and c_id=" + std::to_string(c_id) + ";";
                queries.push_back(sql);
            } break;
            case 7: {
                /*EXEC_SQL INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id,
			                      h_w_id, h_date, h_amount, h_data)
                                 VALUES(:c_d_id, :c_w_id, :c_id, :d_id,
                                 :w_id, 
                                 :datetime,
                                 :h_amount, :h_data);*/
                /*
                insert into history values(h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data);
                */
                sql = "insert into history values(";
                sql += std::to_string(c_id) + ", " + std::to_string(d_id) + ", " + std::to_string(w_id) + ", " + std::to_string(d_id) + ", " + std::to_string(w_id) + ", '";
                char datetime[Clock::DATETIME_SIZE + 1];
                clock->getDateTimestamp(datetime);
                sql.append(datetime);
                int data_len = RandomGenerator::generate_random_int(1, 24);
                char data[25];
                RandomGenerator::generate_random_str(data, data_len);
                sql += "', " + std::to_string(h_amount) + ", '";
                sql.append(data);
                sql += "');";
                // queries.push_back(sql);
            } break;
            default:
            break;
        }
    }
};

#endif