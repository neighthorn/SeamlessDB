#ifndef TABLE_H
#define TABLE_H

#include "config.h"
#include "benchmark/util/random.h"
#include "benchmark/util/clock.h"
#include <cstring>
#include <iostream>
#include <cstdio>
#include <vector>
#include <fstream>

#include "system/sm_meta.h"
#include "system/sm_manager.h"

class Warehouse {
public:
    int w_id;
    char w_name[11];    // varchar
    char w_street_1[21];    // varchar
    char w_street_2[21];    // varchar
    char w_city[21];        // varchar
    char w_state[3];
    char w_zip[10];
    float w_tax;
    float w_ytd;

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "warehouse";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("w_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("w_name", ColType::TYPE_STRING, 11));
        col_defs.emplace_back(ColDef("w_street_1", ColType::TYPE_STRING, 21));
        col_defs.emplace_back(ColDef("w_street_2", ColType::TYPE_STRING, 21));
        col_defs.emplace_back(ColDef("w_city", ColType::TYPE_STRING, 21));
        col_defs.emplace_back(ColDef("w_state", ColType::TYPE_STRING, 3));
        col_defs.emplace_back(ColDef("w_zip", ColType::TYPE_STRING, 10));
        col_defs.emplace_back(ColDef("w_tax", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("w_ytd", ColType::TYPE_FLOAT, 4));
        std::vector<std::string> pkeys;
        pkeys.emplace_back("w_id");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    Warehouse() {}

    void print_record() {
        printf("(w_id: %d, w_name: %s, w_street_1: %s, w_city: %s, w_state: %s, w_state: %s, w_zip: %s, w_tax: %lf, w_ytd: %lf)\n", 
        w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd);
    }

    void generate_table_data(int warehouse_num, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        w_ytd = 3000.5;
        auto tab_meta = sm_mgr->db_.get_table("warehouse");
        auto index_handle = sm_mgr->primary_index_.at("warehouse").get();
        Record record(tab_meta.record_length_);

        for(w_id = 1; w_id <= warehouse_num; ++w_id) {
            memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));
            // RandomGenerator::generate_random_varchar(w_name, 6, 10);
            RandomGenerator::generate_random_str(w_name, 10);
            RandomGenerator::generate_randome_address(w_street_1, w_street_2, w_city, w_state, w_zip);
            w_tax = (float)RandomGenerator::generate_random_int(10, 20) / 100.0;    // 0.1-0.2

            int off = 0;
            // Copy w_id
            memcpy(record.raw_data_ + off, &w_id, sizeof(int));
            off += sizeof(int);

            // Copy w_name
            memcpy(record.raw_data_ + off, w_name, 11);
            off += 11;

            // Copy w_street_1
            memcpy(record.raw_data_ + off, w_street_1, 21);
            off += 21;

            // Copy w_street_2
            memcpy(record.raw_data_ + off, w_street_2, 21);
            off += 21;

            // Copy w_city
            memcpy(record.raw_data_ + off, w_city, 21);
            off += 21;

            // Copy w_state
            memcpy(record.raw_data_ + off, w_state, 3);
            off += 3;

            // Copy w_zip
            memcpy(record.raw_data_ + off, w_zip, 10);
            off += 10;

            // Copy w_tax
            memcpy(record.raw_data_ + off, &w_tax, sizeof(float));
            off += sizeof(float);

            // Copy w_ytd
            memcpy(record.raw_data_ + off, &w_ytd, sizeof(float));
            off += sizeof(float);

            assert(off == tab_meta.record_length_);
            index_handle->insert_entry(record.raw_data_, record.record_, txn);
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "w_id,w_name,w_street_1,w_street_2,w_city,w_state,w_zip,w_tax,w_ytd" << std::endl;
        outfile.close();
        write_data_into_file(file_name);
    }

    void write_data_into_file(std::string file_name) {
        w_ytd = 3000.5;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(w_id = 1; w_id <= NUM_WARE; ++w_id) {
            RandomGenerator::generate_random_str(w_name, 10);
            RandomGenerator::generate_randome_address(w_street_1, w_street_2, w_city, w_state, w_zip);
            int tmp = RandomGenerator::generate_random_int(1, 2);
            if(tmp == 1)
                w_tax = 0.125;
            else
                w_tax = 0.3125;

            outfile << w_id << "," << w_name << "," << w_street_1 << "," << w_street_2 << "," << w_city << "," << w_state << "," << w_zip << "," << w_tax << "," << w_ytd << std::endl;
        }
        outfile.close();
    }
};

class District {
public:
    int d_id;
    int d_w_id;
    char d_name[11];    // varchar
    char d_street_1[21];    // varchar
    char d_street_2[21];    // varchar
    char d_city[21];    // varchar
    char d_state[3];
    char d_zip[10];
    float d_tax;
    float d_ytd;
    int d_next_o_id;
    std::vector<std::string> sqls;

    District() {
        sqls.clear();
    }

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "district";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("d_w_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("d_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("d_name", ColType::TYPE_STRING, 11));
        col_defs.emplace_back(ColDef("d_street_1", ColType::TYPE_STRING, 21));
        col_defs.emplace_back(ColDef("d_street_2", ColType::TYPE_STRING, 21));
        col_defs.emplace_back(ColDef("d_city", ColType::TYPE_STRING, 21));
        col_defs.emplace_back(ColDef("d_state", ColType::TYPE_STRING, 3));
        col_defs.emplace_back(ColDef("d_zip", ColType::TYPE_STRING, 10));
        col_defs.emplace_back(ColDef("d_tax", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("d_ytd", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("d_next_o_id", ColType::TYPE_INT, 4));
        std::vector<std::string> pkeys;
        pkeys.emplace_back("d_w_id");
        pkeys.emplace_back("d_id");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    void generate_table_data(int warehouse_num, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("district");
        auto index_handle = sm_mgr->primary_index_.at("district").get();
        Record record(tab_meta.record_length_);

        for(int w_id = 1; w_id <= warehouse_num; ++w_id) {
            d_w_id = w_id;
            d_ytd = 3000.5;
            d_next_o_id = ORDER_PER_DISTRICT + 1;
            
            for(d_id = 1; d_id <= DISTRICT_PER_WARE; ++d_id) {
                memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));
                // RandomGenerator::generate_random_varchar(d_name, 6, 10);
                RandomGenerator::generate_random_str(d_name, 10);
                RandomGenerator::generate_randome_address(d_street_1, d_street_2, d_city, d_state, d_zip);
                // d_tax = (float)RandomGenerator::generate_random_int(10, 20) / 100.0;
                d_tax = RandomGenerator::generate_random_float(10, 20);

                int off = 0;

                // Copy d_w_id
                memcpy(record.raw_data_ + off, &d_w_id, sizeof(int));
                off += sizeof(int);

                // Copy d_id
                memcpy(record.raw_data_ + off, &d_id, sizeof(int));
                off += sizeof(int);

                // Copy d_name
                memcpy(record.raw_data_ + off, d_name, 11);
                off += 11;

                // Copy d_street_1
                memcpy(record.raw_data_ + off, d_street_1, 21);
                off += 21;

                // Copy d_street_2
                memcpy(record.raw_data_ + off, d_street_2, 21);
                off += 21;

                // Copy d_city
                memcpy(record.raw_data_ + off, d_city, 21);
                off += 21;

                // Copy d_state
                memcpy(record.raw_data_ + off, d_state, 3);
                off += 3;

                // Copy d_zip
                memcpy(record.raw_data_ + off, d_zip, 10);
                off += 10;

                // Copy d_tax
                memcpy(record.raw_data_ + off, &d_tax, sizeof(float));
                off += sizeof(float);

                // Copy d_ytd
                memcpy(record.raw_data_ + off, &d_ytd, sizeof(float));
                off += sizeof(float);

                // Copy d_next_o_id
                memcpy(record.raw_data_ + off, &d_next_o_id, sizeof(int));
                off += sizeof(int);
                assert(off == tab_meta.record_length_);
                index_handle->insert_entry(record.raw_data_, record.record_, txn);
            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "d_id,d_w_id,d_name,d_street_1,d_street_2,d_city,d_state,d_zip,d_tax,d_ytd,d_next_o_id" << std::endl;
        outfile.close();
        for(int i = 1; i <= NUM_WARE; ++i) {
            write_data_into_file(file_name, i);
        }
    }

    void write_data_into_file(std::string file_name, int w_id) {
        d_w_id = w_id;
        d_ytd = 3000.5;
        // d_next_o_id = 3001;
        d_next_o_id = ORDER_PER_DISTRICT + 1;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(d_id = 1; d_id <= DISTRICT_PER_WARE; ++d_id) {
            RandomGenerator::generate_random_str(d_name, 10);
            RandomGenerator::generate_randome_address(d_street_1, d_street_2, d_city, d_state, d_zip);
            int tmp = RandomGenerator::generate_random_int(1, 2);
            if(tmp == 1)
                d_tax = 0.125;
            else
                d_tax = 0.3125;

            outfile << d_id << "," << d_w_id << "," << d_name << "," << d_street_1 << "," << d_street_2 << ",";
            outfile << d_city << "," << d_state << "," << d_zip << "," << d_tax << "," << d_ytd << "," << d_next_o_id << std::endl;
        }
        outfile.close();
    }
};

class Customer {
public:
    int c_id;
    int c_d_id;
    int c_w_id;
    char c_first[17];   // varchar
    char c_middle[3]; 
    char c_last[17];    // varchar
    char c_street_1[21];    // varchar
    char c_street_2[21];    // varchar
    char c_city[21];    // varchar
    char c_state[3];
    char c_zip[10];
    char c_phone[17];
    char c_since[Clock::DATETIME_SIZE + 1];
    char c_credit[3];
    int c_credit_lim;
    float c_discount;
    float c_balance;
    float c_ytd_payment;
    int c_paymeny_cnt;
    int c_delivery_cnt;
    char c_data[51];
    SystemClock* clock;

    Customer() {
        clock = new SystemClock();
    }

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "customer";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("c_w_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("c_d_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("c_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("c_first", ColType::TYPE_STRING, 17));
        col_defs.emplace_back(ColDef("c_middle", ColType::TYPE_STRING, 3));
        col_defs.emplace_back(ColDef("c_last", ColType::TYPE_STRING, 17));
        col_defs.emplace_back(ColDef("c_street_1", ColType::TYPE_STRING, 21));
        col_defs.emplace_back(ColDef("c_street_2", ColType::TYPE_STRING, 21));
        col_defs.emplace_back(ColDef("c_city", ColType::TYPE_STRING, 21));
        col_defs.emplace_back(ColDef("c_state", ColType::TYPE_STRING, 3));
        col_defs.emplace_back(ColDef("c_zip", ColType::TYPE_STRING, 10));
        col_defs.emplace_back(ColDef("c_phone", ColType::TYPE_STRING, 17));
        col_defs.emplace_back(ColDef("c_since", ColType::TYPE_STRING, Clock::DATETIME_SIZE + 1));
        col_defs.emplace_back(ColDef("c_credit", ColType::TYPE_STRING, 3));
        col_defs.emplace_back(ColDef("c_credit_lim", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("c_discount", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("c_balance", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("c_ytd_payment", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("c_paymeny_cnt", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("c_delivery_cnt", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("c_data", ColType::TYPE_STRING, 51));
        std::vector<std::string> pkeys;
        pkeys.emplace_back("c_w_id");
        pkeys.emplace_back("c_d_id");
        pkeys.emplace_back("c_id");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    void generate_table_data(int warehouse_num, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("customer");
        auto index_handle = sm_mgr->primary_index_.at("customer").get();
        Record record(tab_meta.record_length_);

        for(int w_id = 1; w_id <= warehouse_num; ++w_id) {
            c_w_id = w_id;
            c_credit_lim = 50000;
            c_balance = 10.5;
            c_ytd_payment = 10.5;
            c_paymeny_cnt = 1;
            c_delivery_cnt = 0;
            
            for(c_d_id = 1; c_d_id <= DISTRICT_PER_WARE; ++c_d_id) {
                for(c_id = 1; c_id <= CUSTOMER_PER_DISTRICT; ++c_id) {
                    RandomGenerator::generate_random_str(c_first, 16);
                    c_middle[0] = 'O';
                    c_middle[1] = 'E';
                    c_middle[2] = 0;
                    if(c_id <= 1000) {
                        RandomGenerator::generate_random_lastname(c_id - 1, c_last);
                    } 
                    else {
                        RandomGenerator::generate_random_lastname(RandomGenerator::NURand(255, 0, 999), c_last);
                    }

                    RandomGenerator::generate_randome_address(c_street_1, c_street_2, c_city, c_state, c_zip);
                    RandomGenerator::generate_random_numer_str(c_phone, 16);
                    clock->getDateTimestamp(c_since);
                    if (RandomGenerator::generate_random_int(0, 1))
                        c_credit[0] = 'G';
                    else
                        c_credit[0] = 'B';
                    c_credit[1] = 'C';
                    c_credit[2] = 0;
                    // c_discount = (float)RandomGenerator::generate_random_int(0, 50) / 100.0;
                    c_discount = RandomGenerator::generate_random_float(0, 50);
                    // RandomGenerator::generate_random_varchar(c_data, 300, 500);
                    // RandomGenerator::generate_random_varchar(c_data, 30, 50);
                    RandomGenerator::generate_random_str(c_data, 50);

                    int off = 0;
                    memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));

                    // Copy c_w_id
                    memcpy(record.raw_data_ + off, &c_w_id, sizeof(int));
                    off += sizeof(int);

                    // Copy c_d_id
                    memcpy(record.raw_data_ + off, &c_d_id, sizeof(int));
                    off += sizeof(int);

                    // Copy c_id
                    memcpy(record.raw_data_ + off, &c_id, sizeof(int));
                    off += sizeof(int);

                    // Copy c_first
                    memcpy(record.raw_data_ + off, c_first, 17);
                    off += 17;

                    // Copy c_middle
                    memcpy(record.raw_data_ + off, c_middle, 3);
                    off += 3;

                    // Copy c_last
                    memcpy(record.raw_data_ + off, c_last, 17);
                    off += 17;

                    // Copy c_street_1
                    memcpy(record.raw_data_ + off, c_street_1, 21);
                    off += 21;

                    // Copy c_street_2
                    memcpy(record.raw_data_ + off, c_street_2, 21);
                    off += 21;

                    // Copy c_city
                    memcpy(record.raw_data_ + off, c_city, 21);
                    off += 21;

                    // Copy c_state
                    memcpy(record.raw_data_ + off, c_state, 3);
                    off += 3;

                    // Copy c_zip
                    memcpy(record.raw_data_ + off, c_zip, 10);
                    off += 10;

                    // Copy c_phone
                    memcpy(record.raw_data_ + off, c_phone, 17);
                    off += 17;

                    // Copy c_since
                    memcpy(record.raw_data_ + off, c_since, Clock::DATETIME_SIZE + 1);
                    off += Clock::DATETIME_SIZE + 1;

                    // Copy c_credit
                    memcpy(record.raw_data_ + off, c_credit, 3);
                    off += 3;

                    // Copy c_credit_lim
                    memcpy(record.raw_data_ + off, &c_credit_lim, sizeof(int));
                    off += sizeof(int);

                    // Copy c_discount
                    memcpy(record.raw_data_ + off, &c_discount, sizeof(float));
                    off += sizeof(float);

                    // Copy c_balance
                    memcpy(record.raw_data_ + off, &c_balance, sizeof(float));
                    off += sizeof(float);

                    // Copy c_ytd_payment
                    memcpy(record.raw_data_ + off, &c_ytd_payment, sizeof(float));
                    off += sizeof(float);

                    // Copy c_paymeny_cnt
                    memcpy(record.raw_data_ + off, &c_paymeny_cnt, sizeof(int));
                    off += sizeof(int);

                    // Copy c_delivery_cnt
                    memcpy(record.raw_data_ + off, &c_delivery_cnt, sizeof(int));
                    off += sizeof(int);

                    // Copy c_data
                    memcpy(record.raw_data_ + off, c_data, 51);
                    off += 51;

                    assert(off == tab_meta.record_length_);
                    index_handle->insert_entry(record.raw_data_, record.record_, txn);
                }
            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "c_id,c_d_id,c_w_id,c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,";
        outfile << "c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data" << std::endl;
        outfile.close();
        for(int i = 1; i <= NUM_WARE; ++i) {
            write_data_into_file(file_name, i);
        }
    }

    void write_data_into_file(std::string file_name, int w_id) {
        c_w_id = w_id;
        c_credit_lim = 50000;
        c_balance = 10.5;
        c_ytd_payment = 10.5;
        c_paymeny_cnt = 1;
        c_delivery_cnt = 0;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(c_d_id = 1; c_d_id <= DISTRICT_PER_WARE; ++c_d_id) {
            for(c_id = 1; c_id <= CUSTOMER_PER_DISTRICT; ++c_id) {
                RandomGenerator::generate_random_str(c_first, 16);
                c_middle[0] = 'O';
                c_middle[1] = 'E';
                c_middle[2] = 0;
                if(c_id <= 1000) {
                    RandomGenerator::generate_random_lastname(c_id - 1, c_last);
                } 
                else {
                    RandomGenerator::generate_random_lastname(RandomGenerator::NURand(255, 0, 999), c_last);
                }

                RandomGenerator::generate_randome_address(c_street_1, c_street_2, c_city, c_state, c_zip);
                RandomGenerator::generate_random_numer_str(c_phone, 16);
                clock->getDateTimestamp(c_since);
                if (RandomGenerator::generate_random_int(0, 1))
                    c_credit[0] = 'G';
                else
                    c_credit[0] = 'B';
                c_credit[1] = 'C';
                c_credit[2] = 0;
                // c_discount = (float)RandomGenerator::generate_random_int(0, 50) / 100.0;
                c_discount = RandomGenerator::generate_random_float(0, 1);
                // RandomGenerator::generate_random_varchar(c_data, 300, 500);
                // RandomGenerator::generate_random_varchar(c_data, 30, 50);
                RandomGenerator::generate_random_str(c_data, 50);

                outfile << c_id << "," << c_d_id << "," << c_w_id << "," << c_first << "," << c_middle << "," << c_last << ",";
                outfile << c_street_1 << "," << c_street_2 << "," << c_city << "," << c_state << "," << c_zip << "," << c_phone << ",";
                outfile << c_since << "," << c_credit << "," << c_credit_lim << "," << c_discount << "," << c_balance << ",";
                outfile << c_ytd_payment << "," << c_paymeny_cnt << "," << c_delivery_cnt << "," << c_data << std::endl;
            }
        }
        outfile.close();
    }
};

class History {
public:
    int h_id;
    int h_c_id;
    int h_c_d_id;
    int h_c_w_id;
    int h_d_id;
    int h_w_id;
    char h_date[Clock::DATETIME_SIZE + 1];
    float h_amount;
    char h_data[25];    // varchar
    SystemClock* clock;
    std::vector<std::string> sqls;

    History() {
        clock = new SystemClock();
        sqls.clear();
    }

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "history";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("h_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("h_c_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("h_c_d_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("h_c_w_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("h_d_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("h_w_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("h_date", ColType::TYPE_STRING, Clock::DATETIME_SIZE + 1));
        col_defs.emplace_back(ColDef("h_amount", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("h_data", ColType::TYPE_STRING, 25));
        std::vector<std::string> pkeys;
        pkeys.emplace_back("h_id");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    void generate_table_data(int warehouse_num, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("history");
        auto index_handle = sm_mgr->primary_index_.at("history").get();
        Record record(tab_meta.record_length_);
        h_id = 0;

        for(int w_id = 1; w_id <= warehouse_num; ++w_id) {
            h_w_id = w_id;
            h_c_w_id = w_id;
            h_amount = 10.5;

            for(h_d_id = 1; h_d_id <= DISTRICT_PER_WARE; ++h_d_id) {
                for(h_c_id = 1; h_c_id <= CUSTOMER_PER_DISTRICT; ++h_c_id) {
                    h_id ++;
                    
                    h_c_d_id = h_d_id;
                    clock->getDateTimestamp(h_date);
                    RandomGenerator::generate_random_str(h_data, 24);

                    int off = 0;
                    memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));

                    // Copy h_id
                    memcpy(record.raw_data_, &h_id, sizeof(int));
                    off += sizeof(int);

                    // Copy h_c_id
                    memcpy(record.raw_data_ + off, &h_c_id, sizeof(int));
                    off += sizeof(int);

                    // Copy h_c_d_id
                    memcpy(record.raw_data_ + off, &h_c_d_id, sizeof(int));
                    off += sizeof(int);

                    // Copy h_c_w_id
                    memcpy(record.raw_data_ + off, &h_c_w_id, sizeof(int));
                    off += sizeof(int);

                    // Copy h_d_id
                    memcpy(record.raw_data_ + off, &h_d_id, sizeof(int));
                    off += sizeof(int);

                    // Copy h_w_id
                    memcpy(record.raw_data_ + off, &h_w_id, sizeof(int));
                    off += sizeof(int);

                    // Copy h_date
                    memcpy(record.raw_data_ + off, h_date, Clock::DATETIME_SIZE + 1);
                    off += Clock::DATETIME_SIZE + 1;

                    // Copy h_amount
                    memcpy(record.raw_data_ + off, &h_amount, sizeof(float));
                    off += sizeof(float);

                    // Copy h_data
                    memcpy(record.raw_data_ + off, h_data, 25);
                    off += 25;

                    assert(off == tab_meta.record_length_);
                    index_handle->insert_entry(record.raw_data_, record.record_, txn);
                }
            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "h_c_id,h_c_d_id,h_c_w_id,h_d_id,h_w_id,h_date,h_amount,h_data" << std::endl;
        outfile.close();
        for(int i = 1; i <= NUM_WARE; ++i) {
            write_data_into_file(file_name, i);
        }
    }

    void write_data_into_file(std::string file_name, int w_id) {
        h_w_id = w_id;
        h_c_w_id = w_id;
        h_amount = 10.5;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(h_d_id = 1; h_d_id <= DISTRICT_PER_WARE; ++h_d_id) {
            for(h_c_id = 1; h_c_id <= CUSTOMER_PER_DISTRICT; ++h_c_id) {
                h_c_d_id = h_d_id;
                clock->getDateTimestamp(h_date);
                RandomGenerator::generate_random_str(h_data, 24);

                outfile << h_c_id << "," << h_c_d_id << "," << h_c_w_id << "," << h_d_id << "," << h_w_id << "," << h_date << ",";
                outfile << h_amount << "," << h_data << std::endl;
            }
        }
        outfile.close();
    }
};

class NewOrders {
public:
    int no_o_id;
    int no_d_id;
    int no_w_id;
    std::vector<std::string> sqls;

    NewOrders() {
        sqls.clear();
    }

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "new_orders";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("no_w_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("no_d_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("no_o_id", ColType::TYPE_INT, 4));
        std::vector<std::string> pkeys;
        pkeys.emplace_back("no_w_id");
        pkeys.emplace_back("no_d_id");
        pkeys.emplace_back("no_o_id");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    void generate_table_data(int warehouse_num, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("new_orders");
        auto index_handle = sm_mgr->primary_index_.at("new_orders").get();
        Record record(tab_meta.record_length_);

        for(int w_id = 1; w_id <= warehouse_num; ++w_id) {
            no_w_id = w_id;
            
            for(no_d_id = 1; no_d_id <= DISTRICT_PER_WARE; ++no_d_id) {
                for(no_o_id = FIRST_UNPROCESSED_O_ID; no_o_id <= ORDER_PER_DISTRICT; ++ no_o_id) {
                    memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));
                    int off = 0;

                    // Copy no_w_id
                    memcpy(record.raw_data_ + off, &no_w_id, sizeof(int));
                    off += sizeof(int);

                    // Copy no_d_id
                    memcpy(record.raw_data_ + off, &no_d_id, sizeof(int));
                    off += sizeof(int);

                    // Copy no_o_id
                    memcpy(record.raw_data_ + off, &no_o_id, sizeof(int));
                    off += sizeof(int);

                    assert(off == tab_meta.record_length_);
                    index_handle->insert_entry(record.raw_data_, record.record_, txn);
                }
            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "no_o_id,no_d_id,no_w_id" << std::endl;
        outfile.close();
        for(int i = 1; i <= NUM_WARE; ++i) {
            write_data_into_file(file_name, i);
        }
    }

    void write_data_into_file(std::string file_name, int w_id) {
        no_w_id = w_id;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(no_d_id = 1; no_d_id <= DISTRICT_PER_WARE; ++no_d_id) {
            for(no_o_id = FIRST_UNPROCESSED_O_ID; no_o_id <= ORDER_PER_DISTRICT; ++ no_o_id) {
                outfile << no_o_id << "," << no_d_id << "," << no_w_id << std::endl;
            }
        }
        outfile.close();
    }
};

class Orders {
public:
    int o_id;
    int o_d_id;
    int o_w_id;
    int o_c_id;
    char o_entry_d[Clock::DATETIME_SIZE + 1];
    int o_carrier_id;
    int o_ol_cnt;
    int o_all_local;
    SystemClock* clock;

    Orders() {
        clock = new SystemClock();
    }

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "orders";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("o_w_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("o_d_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("o_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("o_c_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("o_entry_d", ColType::TYPE_STRING, Clock::DATETIME_SIZE + 1));
        col_defs.emplace_back(ColDef("o_carrier_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("o_ol_cnt", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("o_all_local", ColType::TYPE_INT, 4));
        std::vector<std::string> pkeys;
        pkeys.emplace_back("o_w_id");
        pkeys.emplace_back("o_d_id");
        pkeys.emplace_back("o_id");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    void generate_table_data(int warehouse_num, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("orders");
        auto index_handle = sm_mgr->primary_index_.at("orders").get();
        Record record(tab_meta.record_length_);
        
        for(int w_id = 1; w_id <= warehouse_num; ++w_id) {
            o_w_id = w_id;
            
            for(o_d_id = 1; o_d_id <= DISTRICT_PER_WARE; ++o_d_id) {
                // o_c_id must be a permutation of [1, 3000]
                int c_ids[CUSTOMER_PER_DISTRICT + 1];
                for(int i = 1; i <= CUSTOMER_PER_DISTRICT; ++i)
                    c_ids[i - 1] = i;
                for(int i = 1; i <= CUSTOMER_PER_DISTRICT; ++i) {
                    int index = RandomGenerator::generate_random_int(0, CUSTOMER_PER_DISTRICT - 1);
                    std::swap(c_ids[i - 1], c_ids[index]);
                }
                for(int i = 1; i <= CUSTOMER_PER_DISTRICT; ++i) {
                    o_c_id = c_ids[i -1];
                    o_id = i;
                    clock->getDateTimestamp(o_entry_d);
                    if(o_id < FIRST_UNPROCESSED_O_ID) {
                        o_carrier_id = RandomGenerator::generate_random_int(1, 10);
                    }
                    else {
                        o_carrier_id = 0;
                    }
                    o_ol_cnt = RandomGenerator::generate_random_int(5, 15);
                    o_all_local = 1;

                    memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));
                    int off = 0;

                    // Copy o_w_id
                    memcpy(record.raw_data_ + off, &o_w_id, sizeof(int));
                    off += sizeof(int);

                    // Copy o_d_id
                    memcpy(record.raw_data_ + off, &o_d_id, sizeof(int));
                    off += sizeof(int);

                    // Copy o_id
                    memcpy(record.raw_data_ + off, &o_id, sizeof(int));
                    off += sizeof(int);

                    // Copy o_c_id
                    memcpy(record.raw_data_ + off, &o_c_id, sizeof(int));
                    off += sizeof(int);

                    // Copy o_entry_d
                    memcpy(record.raw_data_ + off, o_entry_d, Clock::DATETIME_SIZE + 1);
                    off += Clock::DATETIME_SIZE + 1;

                    // Copy o_carrier_id
                    memcpy(record.raw_data_ + off, &o_carrier_id, sizeof(int));
                    off += sizeof(int);

                    // Copy o_ol_cnt
                    memcpy(record.raw_data_ + off, &o_ol_cnt, sizeof(int));
                    off += sizeof(int);

                    // Copy o_all_local
                    memcpy(record.raw_data_ + off, &o_all_local, sizeof(int));
                    off += sizeof(int);

                    assert(off == tab_meta.record_length_);
                    index_handle->insert_entry(record.raw_data_, record.record_, txn);

                }
            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "o_id,o_d_id,o_w_id,o_c_id,o_entry_d,o_carrier_id,o_ol_cnt,o_all_local" << std::endl;
        outfile.close();
        for(int i = 1; i <= NUM_WARE; ++i) {
            write_data_into_file(file_name, i);
        }
    }

    void write_data_into_file(std::string file_name, int w_id) {
        o_w_id = w_id;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(o_d_id = 1; o_d_id <= DISTRICT_PER_WARE; ++o_d_id) {
            // o_c_id must be a permutation of [1, 3000]
            int c_ids[CUSTOMER_PER_DISTRICT + 1];
            for(int i = 1; i <= CUSTOMER_PER_DISTRICT; ++i)
                c_ids[i - 1] = i;
            for(int i = 1; i <= CUSTOMER_PER_DISTRICT; ++i) {
                int index = RandomGenerator::generate_random_int(0, CUSTOMER_PER_DISTRICT - 1);
                std::swap(c_ids[i - 1], c_ids[index]);
            }
            for(int i = 1; i <= CUSTOMER_PER_DISTRICT; ++i) {
                o_c_id = c_ids[i -1];
                o_id = i;
                clock->getDateTimestamp(o_entry_d);
                if(o_id < FIRST_UNPROCESSED_O_ID) {
                    o_carrier_id = RandomGenerator::generate_random_int(1, 10);
                }
                else {
                    o_carrier_id = 0;
                }
                o_ol_cnt = RandomGenerator::generate_random_int(5, 15);
                o_all_local = 1;

                outfile << o_id << "," << o_d_id << "," << o_w_id << "," << o_c_id << "," << o_entry_d << "," << o_carrier_id << ",";
                outfile << o_ol_cnt << "," << o_all_local << std::endl;
            }
        }
        outfile.close();
    }
};

class OrderLine{
public:
    int ol_o_id;
    int ol_d_id;
    int ol_w_id;
    int ol_number;
    int ol_i_id;
    int ol_supply_w_id;
    char ol_delivery_d[Clock::DATETIME_SIZE + 1];
    int ol_quantity;
    float ol_amount;
    char ol_dist_info[25];
    SystemClock* clock;
    std::vector<std::string> sqls;

    OrderLine() {
        clock = new SystemClock();
        sqls.clear();
    }

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "order_line";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("ol_w_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("ol_d_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("ol_o_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("ol_number", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("ol_i_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("ol_supply_w_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("ol_delivery_d", ColType::TYPE_STRING, Clock::DATETIME_SIZE + 1));
        col_defs.emplace_back(ColDef("ol_quantity", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("ol_amount", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("ol_dist_info", ColType::TYPE_STRING, 25));
        std::vector<std::string> pkeys;
        pkeys.emplace_back("ol_w_id");
        pkeys.emplace_back("ol_d_id");
        pkeys.emplace_back("ol_o_id");
        pkeys.emplace_back("ol_number");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    void generate_table_data(int warehouse_num, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("order_line");
        auto index_handle = sm_mgr->primary_index_.at("order_line").get();
        Record record(tab_meta.record_length_);

        for(int w_id = 1; w_id <= warehouse_num; ++w_id) {
            ol_w_id = w_id;
            for(ol_d_id = 1; ol_d_id <= DISTRICT_PER_WARE; ++ol_d_id) {
                for(ol_o_id = 1; ol_o_id <= ORDER_PER_DISTRICT; ++ol_o_id) {

                    int ol_cnt = 10;
                    for(ol_number = 1; ol_number <= ol_cnt; ++ol_number) {

                        ol_i_id = RandomGenerator::generate_random_int(1, MAXITEMS);
                        ol_supply_w_id = w_id;
                        clock->getDateTimestamp(ol_delivery_d);
                        ol_quantity = 5;
                        ol_amount = 0.5;
                        RandomGenerator::generate_random_str(ol_dist_info, 24);

                        memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));
                        int off = 0;

                        // Copy ol_w_id
                        memcpy(record.raw_data_ + off, &ol_w_id, sizeof(int));
                        off += sizeof(int);

                        // Copy ol_d_id
                        memcpy(record.raw_data_ + off, &ol_d_id, sizeof(int));
                        off += sizeof(int);

                        // Copy ol_o_id
                        memcpy(record.raw_data_ + off, &ol_o_id, sizeof(int));
                        off += sizeof(int);

                        // Copy ol_number
                        memcpy(record.raw_data_ + off, &ol_number, sizeof(int));
                        off += sizeof(int);

                        // Copy ol_i_id
                        memcpy(record.raw_data_ + off, &ol_i_id, sizeof(int));
                        off += sizeof(int);

                        // Copy ol_supply_w_id
                        memcpy(record.raw_data_ + off, &ol_supply_w_id, sizeof(int));
                        off += sizeof(int);

                        // Copy ol_delivery_d
                        memcpy(record.raw_data_ + off, ol_delivery_d, Clock::DATETIME_SIZE + 1);
                        off += Clock::DATETIME_SIZE + 1;

                        // Copy ol_quantity
                        memcpy(record.raw_data_ + off, &ol_quantity, sizeof(int));
                        off += sizeof(int);

                        // Copy ol_amount
                        memcpy(record.raw_data_ + off, &ol_amount, sizeof(float));
                        off += sizeof(float);

                        // Copy ol_dist_info
                        memcpy(record.raw_data_ + off, ol_dist_info, 25);
                        off += 25;

                        assert(off == tab_meta.record_length_);
                        index_handle->insert_entry(record.raw_data_, record.record_, txn);

                    }
                }
            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "ol_o_id,ol_d_id,ol_w_id,ol_number,ol_i_id,ol_supply_w_id,ol_delivery_d,ol_quantity,ol_amount,ol_dist_info" << std::endl;
        outfile.close();
        for(int i = 1; i <= NUM_WARE; ++i) {
            write_data_into_file(file_name, i);
        }
    }

    void write_data_into_file(std::string file_name, int w_id) {
        ol_w_id = w_id;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(ol_d_id = 1; ol_d_id <= DISTRICT_PER_WARE; ++ol_d_id) {
            for(ol_o_id = 1; ol_o_id <= ORDER_PER_DISTRICT; ++ol_o_id) {
                // sql = "insert into order_line values ";
                // int ol_cnt = RandomGenerator::generate_random_int(5, 15);
                int ol_cnt = 10;
                for(ol_number = 1; ol_number <= ol_cnt; ++ol_number) {
                    ol_i_id = RandomGenerator::generate_random_int(1, MAXITEMS);
                    ol_supply_w_id = w_id;
                    clock->getDateTimestamp(ol_delivery_d);
                    ol_quantity = 5;
                    ol_amount = 0.5;
                    RandomGenerator::generate_random_str(ol_dist_info, 24);

                    outfile << ol_o_id << "," << ol_d_id << "," << ol_w_id << "," << ol_number << "," << ol_i_id << "," << ol_supply_w_id << ",";
                    outfile << ol_delivery_d << "," << ol_quantity << "," << ol_amount << "," << ol_dist_info << std::endl;
                }
            }
        }
        outfile.close();
    }
};

class Item {
public:
    int i_id;
    int i_im_id;
    char i_name[25];    // varchar
    float i_price;
    char i_data[51];    // varchar

    Item() {
    }

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "item";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("i_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("i_im_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("i_name", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("i_price", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("i_data", ColType::TYPE_STRING, 51));
        std::vector<std::string> pkeys;
        pkeys.emplace_back("i_id");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    void generate_table_data(int warehouse_num, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("item");
        auto index_handle = sm_mgr->primary_index_.at("item").get();
        Record record(tab_meta.record_length_);
        
        for(i_id = 1; i_id <= MAXITEMS; ++i_id) {
            i_im_id = RandomGenerator::generate_random_int(1, 10000);
            // RandomGenerator::generate_random_varchar(i_name, 14, 24);
            RandomGenerator::generate_random_str(i_name, 24);
            // i_price = (float)RandomGenerator::generate_random_int(100, 10000) / 100.0;
            i_price = RandomGenerator::generate_random_float(100, 1000);
            // RandomGenerator::generate_random_varchar(i_data, 26, 50);
            RandomGenerator::generate_random_str(i_data, 50);

            memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));
            int off = 0; 

            // Copy i_id
            memcpy(record.raw_data_, &i_id, sizeof(int));
            off += sizeof(int);

            // Copy i_im_id
            memcpy(record.raw_data_ + off, &i_im_id, sizeof(int));
            off += sizeof(int);

            // Copy i_name
            memcpy(record.raw_data_ + off, i_name, 25);
            off += 25;

            // Copy i_price
            memcpy(record.raw_data_ + off, &i_price, sizeof(float));
            off += sizeof(float);

            // Copy i_data
            memcpy(record.raw_data_ + off, i_data, 51);
            off += 51;

            assert(off == tab_meta.record_length_);
            index_handle->insert_entry(record.raw_data_, record.record_, txn);

        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "i_id,i_im_id,i_name,i_price,i_data" << std::endl;
        outfile.close();
        write_data_into_file(file_name);
    }

    void write_data_into_file(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(i_id = 1; i_id <= MAXITEMS; ++i_id) {
            i_im_id = RandomGenerator::generate_random_int(1, 10000);
            // RandomGenerator::generate_random_varchar(i_name, 14, 24);
            RandomGenerator::generate_random_str(i_name, 24);
            // i_price = (float)RandomGenerator::generate_random_int(100, 10000) / 100.0;
            i_price = RandomGenerator::generate_random_float(100, 1000);
            // RandomGenerator::generate_random_varchar(i_data, 26, 50);
            RandomGenerator::generate_random_str(i_data, 50);

            outfile << i_id << "," << i_im_id << "," << i_name << "," << i_price << "," << i_data << std::endl;
        }
        outfile.close();
    }
};

class Stock {
public:
    int s_i_id;
    int s_w_id;
    int s_quantity;
    char s_dist_01[25];
    char s_dist_02[25];
    char s_dist_03[25];
    char s_dist_04[25];
    char s_dist_05[25];
    char s_dist_06[25];
    char s_dist_07[25];
    char s_dist_08[25];
    char s_dist_09[25];
    char s_dist_10[25];
    float s_ytd;
    int s_order_cnt;
    int s_remote_cnt;
    char s_data[51];    // varchar

    Stock() {
    }

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "stock";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("s_w_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("s_i_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("s_quantity", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("s_dist_01", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("s_dist_02", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("s_dist_03", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("s_dist_04", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("s_dist_05", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("s_dist_06", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("s_dist_07", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("s_dist_08", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("s_dist_09", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("s_dist_10", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("s_ytd", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("s_order_cnt", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("s_remote_cnt", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("s_data", ColType::TYPE_STRING, 51));
        std::vector<std::string> pkeys;
        pkeys.emplace_back("s_w_id");
        pkeys.emplace_back("s_i_id");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    void generate_table_data(int warehouse_num, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("stock");
        auto index_handle = sm_mgr->primary_index_.at("stock").get();
        Record record(tab_meta.record_length_);

        for(int w_id = 1; w_id <= warehouse_num; ++w_id) {
            s_w_id = w_id;
            for(s_i_id = 1; s_i_id <= MAXITEMS; ++s_i_id) {

                s_quantity = RandomGenerator::generate_random_int(10, 100);
                RandomGenerator::generate_random_str(s_dist_01, 24);
                RandomGenerator::generate_random_str(s_dist_02, 24);
                RandomGenerator::generate_random_str(s_dist_03, 24);
                RandomGenerator::generate_random_str(s_dist_04, 24);
                RandomGenerator::generate_random_str(s_dist_05, 24);
                RandomGenerator::generate_random_str(s_dist_06, 24);
                RandomGenerator::generate_random_str(s_dist_07, 24);
                RandomGenerator::generate_random_str(s_dist_08, 24);
                RandomGenerator::generate_random_str(s_dist_09, 24);
                RandomGenerator::generate_random_str(s_dist_10, 24);
                s_ytd = 0.5;
                s_order_cnt = 0;
                s_remote_cnt = 0;
                // RandomGenerator::generate_random_varchar(s_data, 26, 50);
                RandomGenerator::generate_random_str(s_data, 50);

                memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));
                int off = 0;

                // Copy s_w_id
                memcpy(record.raw_data_ + off, &s_w_id, sizeof(int));
                off += sizeof(int);

                // Copy s_i_id
                memcpy(record.raw_data_ + off, &s_i_id, sizeof(int));
                off += sizeof(int);

                // Copy s_quantity
                memcpy(record.raw_data_ + off, &s_quantity, sizeof(int));
                off += sizeof(int);

                // Copy s_dist_01
                memcpy(record.raw_data_ + off, s_dist_01, 25);
                off += 25;

                // Copy s_dist_02
                memcpy(record.raw_data_ + off, s_dist_02, 25);
                off += 25;

                // Copy s_dist_03
                memcpy(record.raw_data_ + off, s_dist_03, 25);
                off += 25;

                // Copy s_dist_04
                memcpy(record.raw_data_ + off, s_dist_04, 25);
                off += 25;

                // Copy s_dist_05
                memcpy(record.raw_data_ + off, s_dist_05, 25);
                off += 25;

                // Copy s_dist_06
                memcpy(record.raw_data_ + off, s_dist_06, 25);
                off += 25;

                // Copy s_dist_07
                memcpy(record.raw_data_ + off, s_dist_07, 25);
                off += 25;

                // Copy s_dist_08
                memcpy(record.raw_data_ + off, s_dist_08, 25);
                off += 25;

                // Copy s_dist_09
                memcpy(record.raw_data_ + off, s_dist_09, 25);
                off += 25;

                // Copy s_dist_10
                memcpy(record.raw_data_ + off, s_dist_10, 25);
                off += 25;

                // Copy s_ytd
                memcpy(record.raw_data_ + off, &s_ytd, sizeof(float));
                off += sizeof(float);

                // Copy s_order_cnt
                memcpy(record.raw_data_ + off, &s_order_cnt, sizeof(int));
                off += sizeof(int);

                // Copy s_remote_cnt
                memcpy(record.raw_data_ + off, &s_remote_cnt, sizeof(int));
                off += sizeof(int);

                // Copy s_data
                memcpy(record.raw_data_ + off, s_data, 51);
                off += 51;

                assert(off == tab_meta.record_length_);
                index_handle->insert_entry(record.raw_data_, record.record_, txn);

            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::trunc);
        outfile << "s_i_id,s_w_id,s_quantity,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,";
        outfile << "s_dist_10,s_ytd,s_order_cnt,s_remote_cnt,s_data" << std::endl;
        outfile.close();
        for(int i = 1; i <= NUM_WARE; ++i) {
            write_data_into_file(file_name, i);
        }
    }

    void write_data_into_file(std::string file_name, int w_id) {
        s_w_id = w_id;
        std::ofstream outfile;
        outfile.open(file_name, std::ios::out | std::ios::app);
        for(s_i_id = 1; s_i_id <= MAXITEMS; ++s_i_id) {
            s_quantity = RandomGenerator::generate_random_int(10, 100);
            RandomGenerator::generate_random_str(s_dist_01, 24);
            RandomGenerator::generate_random_str(s_dist_02, 24);
            RandomGenerator::generate_random_str(s_dist_03, 24);
            RandomGenerator::generate_random_str(s_dist_04, 24);
            RandomGenerator::generate_random_str(s_dist_05, 24);
            RandomGenerator::generate_random_str(s_dist_06, 24);
            RandomGenerator::generate_random_str(s_dist_07, 24);
            RandomGenerator::generate_random_str(s_dist_08, 24);
            RandomGenerator::generate_random_str(s_dist_09, 24);
            RandomGenerator::generate_random_str(s_dist_10, 24);
            s_ytd = 0.5;
            s_order_cnt = 0;
            s_remote_cnt = 0;
            // RandomGenerator::generate_random_varchar(s_data, 26, 50);
            RandomGenerator::generate_random_str(s_data, 50);

            outfile << s_i_id << "," << s_w_id << "," << s_quantity << "," << s_dist_01 << "," << s_dist_02 << "," << s_dist_03 << ",";
            outfile << s_dist_04 << "," << s_dist_05 << "," << s_dist_06 << "," << s_dist_07 << "," << s_dist_08 << "," << s_dist_09 << ",";
            outfile << s_dist_10 << "," << s_ytd << "," << s_order_cnt << "," << s_remote_cnt << "," << s_data << std::endl;
        }
        outfile.close();
    }
};

#endif