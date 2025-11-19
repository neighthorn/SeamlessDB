#include "benchmark/util/random.h"
#include "benchmark/util/clock.h"

#include <cstring>
#include <iostream>
#include <cstdio>
#include <vector>
#include <fstream>

#include "system/sm.h"

#include "debug_log.h"

namespace HATtrick {

// 生成ssb的table
class Customer {
/**
 * "CREATE TABLE HAT.CUSTOMER (\n"
                "	C_CUSTKEY INT NOT NULL,\n"
                "	C_NAME VARCHAR(25),\n"
                "	C_ADDRESS VARCHAR(25),\n"
                "	C_CITY CHAR(10),\n"
                "	C_NATION VARCHAR(15),\n"
                "	C_REGION VARCHAR(12),\n"
                "	C_PHONE CHAR(15),\n"
                "	C_MKTSEGMENT VARCHAR(10),\n"
                "	C_PAYMENTCNT INTEGER,\n"
                "	PRIMARY KEY (C_CUSTKEY)\n"
                ")",
 */

// SF * 30000 records

public:
    int     c_custkey;          // int  primary key
    char    c_name[25];
    char    c_address[25];
    char    c_city[10];
    char    c_nation[15];
    char    c_region[12];
    char    c_phone[15];
    char    c_mktsegment[11];
    int     c_paymentcnt;

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "customer";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("c_custkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("c_name", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("c_address", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("c_city", ColType::TYPE_STRING, 10));
        col_defs.emplace_back(ColDef("c_nation", ColType::TYPE_STRING, 15));
        col_defs.emplace_back(ColDef("c_region", ColType::TYPE_STRING, 12));
        col_defs.emplace_back(ColDef("c_phone", ColType::TYPE_STRING, 15));
        col_defs.emplace_back(ColDef("c_mktsegment", ColType::TYPE_STRING, 10));
        col_defs.emplace_back(ColDef("c_paymentcnt", ColType::TYPE_INT, 4));
        std::vector<std::string> pkeys;
        pkeys.emplace_back("c_custkey");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }
    void print_record() {}
    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan) {}
    void generate_table_data(int SF, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("customer");
        auto index_handle = sm_mgr->primary_index_.at("customer").get();
        Record record(tab_meta.record_length_);

        for(c_custkey = 1; c_custkey <= SF * 30000; c_custkey++) {
            RandomGenerator::generate_random_str(c_name, 25);
            RandomGenerator::generate_random_str(c_address, 25);
            RandomGenerator::generate_random_str(c_city, 10);
            RandomGenerator::generate_random_str(c_nation, 15);
            RandomGenerator::generate_random_str(c_region, 12);
            RandomGenerator::generate_random_str(c_phone, 15);
            RandomGenerator::generate_random_str(c_mktsegment, 10);
            c_paymentcnt = RandomGenerator::generate_random_int(0, 100);

            size_t offset = 0;
            memcpy(record.record_ + offset, &c_custkey, sizeof(int));
            offset += sizeof(int);
            memcpy(record.record_ + offset, c_name, 25);
            offset += 25;
            memcpy(record.record_ + offset, c_address, 25);
            offset += 25;
            memcpy(record.record_ + offset, c_city, 10);
            offset += 10;
            memcpy(record.record_ + offset, c_nation, 15);
            offset += 15;
            memcpy(record.record_ + offset, c_region, 12);
            offset += 12;
            memcpy(record.record_ + offset, c_phone, 15);
            offset += 15;
            memcpy(record.record_ + offset, c_mktsegment, 10);
            offset += 10;
            memcpy(record.record_ + offset, &c_paymentcnt, sizeof(int));
            offset += sizeof(int);

            assert(offset == tab_meta.record_length_);

            index_handle->insert_entry(record.raw_data_, record.record_, txn);
        }

    }
};

class Supplier {
/**
 * "CREATE TABLE HAT.SUPPLIER (\n"
                "	S_SUPPKEY INTEGER NOT NULL,\n"
                "	S_NAME VARCHAR(25),\n"
                "	S_ADDRESS VARCHAR(25),\n"
                "	S_CITY CHAR(10),\n"
                "	S_NATION VARCHAR(15),\n"
                "	S_REGION CHAR(12),\n"
                "	S_PHONE CHAR(15),\n"
                "	S_YTD DECIMAL,\n"
                "	PRIMARY KEY (S_SUPPKEY)\n"
                ")",
 */

// SF * 2000 records

public:
    int     s_suppkey       ;         // int primary key
    char    s_name[25]      ;    
    char    s_address[25]   ;
    char    s_city[10]      ;
    char    s_nation[15]    ;
    char    s_region[12]    ;
    char    s_phone[15]     ;
    float   s_ytd           ;

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "supplier";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("s_suppkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("s_name", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("s_address", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("s_city", ColType::TYPE_STRING, 10));
        col_defs.emplace_back(ColDef("s_nation", ColType::TYPE_STRING, 15));
        col_defs.emplace_back(ColDef("s_region", ColType::TYPE_STRING, 12));
        col_defs.emplace_back(ColDef("s_phone", ColType::TYPE_STRING, 15));
        col_defs.emplace_back(ColDef("s_ytd", ColType::TYPE_FLOAT, 4));
        std::vector<std::string> pkeys;
        pkeys.emplace_back("s_suppkey");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }
    void print_record() {}
    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan) {}
    void generate_table_data(int SF, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("supplier");
        auto index_handle = sm_mgr->primary_index_.at("supplier").get();
        Record record(tab_meta.record_length_);

        for(s_suppkey = 1; s_suppkey <= SF * 2000; s_suppkey++) {
            RandomGenerator::generate_random_str(s_name, 25);
            RandomGenerator::generate_random_str(s_address, 25);
            RandomGenerator::generate_random_str(s_city, 10);
            RandomGenerator::generate_random_str(s_nation, 15);
            RandomGenerator::generate_random_str(s_region, 12);
            RandomGenerator::generate_random_str(s_phone, 15);
            s_ytd = RandomGenerator::generate_random_float(0.0, 100000.0);

            size_t offset = 0;
            memcpy(record.record_ + offset, &s_suppkey, sizeof(int));
            offset += sizeof(int);
            memcpy(record.record_ + offset, s_name, 25);
            offset += 25;
            memcpy(record.record_ + offset, s_address, 25);
            offset += 25;
            memcpy(record.record_ + offset, s_city, 10);
            offset += 10;
            memcpy(record.record_ + offset, s_nation, 15);
            offset += 15;
            memcpy(record.record_ + offset, s_region, 12);
            offset += 12;
            memcpy(record.record_ + offset, s_phone, 15);
            offset += 15;
            memcpy(record.record_ + offset, &s_ytd, sizeof(float));
            offset += sizeof(float);

            assert(offset == tab_meta.record_length_);

            index_handle->insert_entry(record.raw_data_, record.record_, txn);
        }
    }
};


class LineOrder {
/**
 * "CREATE TABLE HAT.LINEORDER (\n"
                "	LO_ORDERKEY INTEGER NOT NULL,\n"
                "	LO_LINENUMBER INTEGER NOT NULL,\n"
                "	LO_CUSTKEY INTEGER,\n"
                "	LO_PARTKEY INTEGER,\n"
                "	LO_SUPPKEY INTEGER,\n"
                "	LO_ORDERDATE INTEGER,\n"
                "	LO_ORDPRIORITY CHAR(15),\n"
                "	LO_SHIPPRIORITY CHAR(1),\n"
                "	LO_QUANTITY INTEGER,\n"
                "	LO_EXTENDEDPRICE DECIMAL,\n"
                "	LO_DISCOUNT INTEGER,\n"
                "	LO_REVENUE DECIMAL,\n"
                "	LO_SUPPLYCOST DECIMAL,\n"
                "	LO_TAX INTEGER,\n"
                "	LO_COMMITDATE INTEGER,\n"
                "	LO_SHIPMODE CHAR(10),\n"
                "	PRIMARY KEY (LO_ORDERKEY,LO_LINENUMBER)\n"
                ")",
 */

// SF * 6000000 records

public:
    int     lo_orderkey        ;
    int     lo_linenumber      ;
    int     lo_custkey         ;
    int     lo_partkey         ;
    int     lo_suppkey         ;
    int     lo_orderdate       ;
    char    lo_ordpriority[15] ;
    char    lo_shippriority[1] ;
    int     lo_quantity        ;
    float   lo_extendedprice   ;
    int     lo_discount        ;
    float   lo_revenue         ;
    float   lo_supplycost      ;
    int     lo_tax             ;
    int     lo_commitdate      ;
    char    lo_shipmode[10]    ;

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "lineorder";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("lo_orderdate", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("lo_orderkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("lo_linenumber", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("lo_custkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("lo_partkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("lo_suppkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("lo_ordpriority", ColType::TYPE_STRING, 15));
        col_defs.emplace_back(ColDef("lo_shippriority", ColType::TYPE_STRING, 1));
        col_defs.emplace_back(ColDef("lo_quantity", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("lo_extendedprice", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("lo_discount", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("lo_revenue", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("lo_supplycost", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("lo_tax", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("lo_commitdate", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("lo_shipmode", ColType::TYPE_STRING, 10));
        std::vector<std::string> pkeys;
        pkeys.emplace_back("lo_orderdate");
        pkeys.emplace_back("lo_orderkey");
        pkeys.emplace_back("lo_linenumber");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }
    void print_record() {}
    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan) {}
    void generate_table_data(int SF, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("lineorder");
        auto index_handle = sm_mgr->primary_index_.at("lineorder").get();
        Record record(tab_meta.record_length_);

        // lo_datekey 从19920101到19981231, 一共有 2922 天
        // 平均每个lo_datekey 对应大约2058条记录
        lo_orderkey = 1;
        lo_linenumber = 1;
        int MAX_LO_ORDERKEY = SF * 6000000;
        int record_per_orderdate = (MAX_LO_ORDERKEY / 2922);
        // lo_orderkey: 1-1500000 * SF
        // 每个lo_orderkey对应四个linenumber
        for(int year = 1992; year <= 1998; ++year) {
            for(int month = 1; month <= 12; ++month) {
                int days_in_month;
                if(month == 2) {
                    // February
                    if((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
                        days_in_month = 29; // Leap year
                    } else {
                        days_in_month = 28;
                    }
                } else if(month == 4 || month == 6 || month == 9 || month == 11) {
                    days_in_month = 30;
                } else {
                    days_in_month = 31;
                }

                for(int day = 1; day <= days_in_month; ++day) {
                    lo_orderdate = year * 10000 + month * 100 + day;
                    for(int record_idx = 0; record_idx < record_per_orderdate; ++record_idx) {
                        if(lo_orderkey > MAX_LO_ORDERKEY) {
                            break;
                        }

                        lo_custkey = RandomGenerator::generate_random_int(1, SF * 30000);
                        lo_partkey = RandomGenerator::generate_random_int(1, (1 + static_cast<int>(std::log2(SF))) * 200000);
                        lo_suppkey = RandomGenerator::generate_random_int(1, SF * 2000);
                        RandomGenerator::generate_random_str(lo_ordpriority, 15);
                        RandomGenerator::generate_random_str(lo_shippriority, 1);
                        lo_quantity = RandomGenerator::generate_random_int(1, 50);
                        lo_extendedprice = RandomGenerator::generate_random_float(100.0, 10000.0);
                        lo_discount = RandomGenerator::generate_random_int(0, 10);
                        lo_revenue = lo_extendedprice * (1 - lo_discount / 100.0);
                        lo_supplycost = RandomGenerator::generate_random_float(50.0, 5000.0);
                        lo_tax = RandomGenerator::generate_random_int(0, 8);
                        lo_commitdate = RandomGenerator::generate_random_int(19920101, 19981231);
                        RandomGenerator::generate_random_str(lo_shipmode, 10);

                        memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));

                        size_t offset = 0;
                        memcpy(record.record_ + offset, &lo_orderdate, sizeof(int));
                        offset += sizeof(int);
                        memcpy(record.record_ + offset, &lo_orderkey, sizeof(int));
                        offset += sizeof(int);
                        memcpy(record.record_ + offset, &lo_linenumber, sizeof(int));
                        offset += sizeof(int);
                        memcpy(record.record_ + offset, &lo_custkey, sizeof(int));
                        offset += sizeof(int);
                        memcpy(record.record_ + offset, &lo_partkey, sizeof(int));
                        offset += sizeof(int);
                        memcpy(record.record_ + offset, &lo_suppkey, sizeof(int));
                        offset += sizeof(int);
                        memcpy(record.record_ + offset, lo_ordpriority, 15);
                        offset += 15;
                        memcpy(record.record_ + offset, lo_shippriority, 1);
                        offset += 1;
                        memcpy(record.record_ + offset, &lo_quantity, sizeof(int));
                        offset += sizeof(int);
                        memcpy(record.record_ + offset, &lo_extendedprice, sizeof(float));
                        offset += sizeof(float);
                        memcpy(record.record_ + offset, &lo_discount, sizeof(int));
                        offset += sizeof(int);
                        memcpy(record.record_ + offset, &lo_revenue, sizeof(float));
                        offset += sizeof(float);
                        memcpy(record.record_ + offset, &lo_supplycost, sizeof(float));
                        offset += sizeof(float);
                        memcpy(record.record_ + offset, &lo_tax, sizeof(int));
                        offset += sizeof(int);
                        memcpy(record.record_ + offset, &lo_commitdate, sizeof(int));
                        offset += sizeof(int);
                        memcpy(record.record_ + offset, lo_shipmode, 10);
                        offset += 10;
                        assert(offset == tab_meta.record_length_);
                        index_handle->insert_entry(record.raw_data_, record.record_, txn);

                        lo_linenumber++;
                        if(lo_linenumber > 4) {
                            lo_linenumber = 1;
                            lo_orderkey++;
                        }
                    }
                }
            }
        }


        // for (lo_orderkey = 1; lo_orderkey <= SF * 6000000; lo_orderkey++) {
        //     lo_linenumber = 1;
        //     lo_custkey = RandomGenerator::generate_random_int(1, SF * 30000);
        //     lo_partkey = RandomGenerator::generate_random_int(1, (1 + static_cast<int>(std::log2(SF))) * 200000);
        //     lo_suppkey = RandomGenerator::generate_random_int(1, SF * 2000);
        //     lo_orderdate = RandomGenerator::generate_random_int(19920101, 19981231);
        //     RandomGenerator::generate_random_str(lo_ordpriority, 15);
        //     RandomGenerator::generate_random_str(lo_shippriority, 1);
        //     lo_quantity = RandomGenerator::generate_random_int(1, 50);
        //     lo_extendedprice = RandomGenerator::generate_random_float(100.0, 10000.0);
        //     lo_discount = RandomGenerator::generate_random_int(0, 10);
        //     lo_revenue = lo_extendedprice * (1 - lo_discount / 100.0);
        //     lo_supplycost = RandomGenerator::generate_random_float(50.0, 5000.0);
        //     lo_tax = RandomGenerator::generate_random_int(0, 8);
        //     lo_commitdate = RandomGenerator::generate_random_int(19920101, 19981231);
        //     RandomGenerator::generate_random_str(lo_shipmode, 10);

        //     size_t offset = 0;
        //     memcpy(record.record_ + offset, &lo_orderkey, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.record_ + offset, &lo_linenumber, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.record_ + offset, &lo_custkey, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.record_ + offset, &lo_partkey, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.record_ + offset, &lo_suppkey, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.record_ + offset, &lo_orderdate, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.record_ + offset, lo_ordpriority, 15);
        //     offset += 15;
        //     memcpy(record.record_ + offset, lo_shippriority, 1);
        //     offset += 1;
        //     memcpy(record.record_ + offset, &lo_quantity, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.record_ + offset, &lo_extendedprice, sizeof(float));
        //     offset += sizeof(float);
        //     memcpy(record.record_ + offset, &lo_discount, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.record_ + offset, &lo_revenue, sizeof(float));
        //     offset += sizeof(float);
        //     memcpy(record.record_ + offset, &lo_supplycost, sizeof(float));
        //     offset += sizeof(float);
        //     memcpy(record.record_ + offset, &lo_tax, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.record_ + offset, &lo_commitdate, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.record_ + offset, lo_shipmode, 10);
        //     offset += 10;
        //     assert(offset == tab_meta.record_length_);
        //     index_handle->insert_entry(record.raw_data_, record.record_, txn);
        // }

    }
};

class Part {
/**
 * "CREATE TABLE HAT.PART (\n"
                "	P_PARTKEY INT NOT NULL,\n"
                "	P_NAME VARCHAR(22),\n"
                "	P_MFGR CHAR(6),\n"
                "	P_CATEGORY CHAR(7),\n"
                "	P_BRAND1 CHAR(9),\n"
                "	P_COLOR VARCHAR(11),\n"
                "	P_TYPE VARCHAR(25),\n"
                "	P_SIZE INT,\n"
                "	P_CONTAINER VARCHAR(10),\n"
                "	P_PRICE DECIMAL,\n"
                "	PRIMARY KEY (P_PARTKEY)\n"
                ")",
 */

// (1+log2(SF)) * 200000 records

public:
    int     p_partkey;          // int  primary key
    char    p_name[22];         
    char    p_mfgr[6];         
    char    p_category[7];       
    char    p_brand1[9];         
    char    p_color[11];
    char    p_type[25];
    int     p_size;
    char    p_container[10];
    float   p_price;

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "part";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("p_partkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("p_name", ColType::TYPE_STRING, 22));
        col_defs.emplace_back(ColDef("p_mfgr", ColType::TYPE_STRING, 6));
        col_defs.emplace_back(ColDef("p_category", ColType::TYPE_STRING, 7));
        col_defs.emplace_back(ColDef("p_brand1", ColType::TYPE_STRING, 9));
        col_defs.emplace_back(ColDef("p_color", ColType::TYPE_STRING, 11));
        col_defs.emplace_back(ColDef("p_type", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("p_size", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("p_container", ColType::TYPE_STRING, 10));
        col_defs.emplace_back(ColDef("p_price", ColType::TYPE_FLOAT, 4));
        std::vector<std::string> pkeys;
        pkeys.emplace_back("p_partkey");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }
    void print_record();
    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan);
    void generate_table_data(int SF, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("part");
        auto index_handle = sm_mgr->primary_index_.at("part").get();
        Record record(tab_meta.record_length_);

        int total_records = (1 + static_cast<int>(std::log2(SF))) * 200000;
        for(p_partkey = 1; p_partkey <= total_records; p_partkey++) {
            /*
                p_name, p_mfgr, p_category, p_brand1, p_color,
                p_type, p_size, p_container, p_price
            */
            // For simplicity, we fill in dummy data for other fields
            snprintf(p_name, sizeof(p_name), "PartName%d", p_partkey);
            snprintf(p_mfgr, sizeof(p_mfgr), "MFGR%d", (p_partkey % 5) + 1);
            snprintf(p_category, sizeof(p_category), "Category%d", (p_partkey % 10) + 1);
            snprintf(p_brand1, sizeof(p_brand1), "Brand%d", (p_partkey % 20) + 1);
            snprintf(p_color, sizeof(p_color), "Color%d", (p_partkey % 15) + 1);
            snprintf(p_type, sizeof(p_type), "Type%d", (p_partkey % 25) + 1);
            p_size = (p_partkey % 50) + 1;
            snprintf(p_container, sizeof(p_container), "Container%d", (p_partkey % 10) + 1);
            p_price = static_cast<float>((p_partkey % 1000) + 1);

            // Serialize the record
            size_t offset = 0;
            memcpy(record.record_ + offset, &p_partkey, sizeof(p_partkey));
            offset += sizeof(p_partkey);
            memcpy(record.record_ + offset, p_name, sizeof(p_name));
            offset += sizeof(p_name);
            memcpy(record.record_ + offset, p_mfgr, sizeof(p_mfgr));
            offset += sizeof(p_mfgr);
            memcpy(record.record_ + offset, p_category, sizeof(p_category));
            offset += sizeof(p_category);
            memcpy(record.record_ + offset, p_brand1, sizeof(p_brand1));
            offset += sizeof(p_brand1);
            memcpy(record.record_ + offset, p_color, sizeof(p_color));
            offset += sizeof(p_color);
            memcpy(record.record_ + offset, p_type, sizeof(p_type));
            offset += sizeof(p_type);
            memcpy(record.record_ + offset, &p_size, sizeof(p_size));
            offset += sizeof(p_size);
            memcpy(record.record_ + offset, p_container, sizeof(p_container));
            offset += sizeof(p_container);
            memcpy(record.record_ + offset, &p_price, sizeof(p_price));
            offset += sizeof(p_price);

            assert(offset == tab_meta.record_length_);

            index_handle->insert_entry(record.raw_data_, record.record_, txn);
        }
    }
};

class Date {
/**
 * "CREATE TABLE HAT.DATE (\n"
                "	D_DATEKEY INTEGER NOT NULL,\n"
                "	D_DATE VARCHAR(18),\n"
                "	D_DATEOFWEEK VARCHAR(9),\n"
                "	D_MONTH VARCHAR(9),\n"
                "	D_YEAR INTEGER,\n"
                "	D_YEARMONTHNUM INTEGER,\n"
                "	D_YEARMONTH CHAR(7),\n"
                "	D_DAYNUMINWEEK INTEGER,\n"
                "	D_DAYNUMINMONTH INTEGER,\n"
                "	D_DAYNUMINYEAR INTEGER,\n"
                "	D_MONTHNUMINYEAR INTEGER,\n"
                "	D_WEEKNUMINYEAR INTEGER,\n"
                "	D_SELLINGSEASON CHAR(15),\n"
                "	D_LASTDAYINWEEKFL BOOLEAN,\n"
                "	D_LASTDAYINMONTHFL BOOLEAN,\n"
                "	D_HOLIDAYFL BOOLEAN,\n"
                "	D_WEEKDAYFL BOOLEAN,\n"
                "	PRIMARY KEY (D_DATEKEY)\n"
                ")",
 */

public:
    int     d_datekey               ;
    char    d_date[18]              ;
    char    d_dateofweek[9]         ;
    char    d_month[9]              ;
    int     d_year                  ;
    int     d_yearmonthnum          ;
    char    d_yearmonth[7]          ;
    int     d_daynuminweek          ;
    int     d_daynuminmonth         ;
    int     d_daynuminyear          ;
    int     d_monthnuminyear        ;
    int     d_weeknuminyear         ;
    char    d_sellingseason[15]     ;
    char    d_lastdayinweekfl       ;
    char    d_lastdayinmonthfl      ;
    char    d_holidayfl             ;
    char    d_weekdayfl             ;

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "date";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("d_datekey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("d_date", ColType::TYPE_STRING, 18));
        col_defs.emplace_back(ColDef("d_dateofweek", ColType::TYPE_STRING, 9));
        col_defs.emplace_back(ColDef("d_month", ColType::TYPE_STRING, 9));
        col_defs.emplace_back(ColDef("d_year", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("d_yearmonthnum", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("d_yearmonth", ColType::TYPE_STRING, 7));
        col_defs.emplace_back(ColDef("d_daynuminweek", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("d_daynuminmonth", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("d_daynuminyear", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("d_monthnuminyear", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("d_weeknuminyear", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("d_sellingseason", ColType::TYPE_STRING, 15));
        col_defs.emplace_back(ColDef("d_lastdayinweekfl", ColType::TYPE_STRING, 1));
        col_defs.emplace_back(ColDef("d_lastdayinmonthfl", ColType::TYPE_STRING, 1));
        col_defs.emplace_back(ColDef("d_holidayfl", ColType::TYPE_STRING, 1));
        col_defs.emplace_back(ColDef("d_weekdayfl", ColType::TYPE_STRING, 1));
        std::vector<std::string> pkeys;
        pkeys.emplace_back("d_datekey");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }
    void print_record() {}
    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan) {}
    void generate_table_data(int SF, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("date");
        auto index_handle = sm_mgr->primary_index_.at("date").get();
        Record record(tab_meta.record_length_);

        // d_datekey 代表YYYYMMDD，从19920101开始，到19981231结束，共2922天
        // 写一个循环，遍历d_datekey, 从19920101到19981231，注意闰年2月有29天
        for(int year = 1992; year <= 1998; ++year) {
            for(int month = 1; month <= 12; ++month) {
                int days_in_month;
                if(month == 2) {
                    // February
                    if((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
                        days_in_month = 29; // Leap year
                    } else {
                        days_in_month = 28;
                    }
                } else if(month == 4 || month == 6 || month == 9 || month == 11) {
                    days_in_month = 30;
                } else {
                    days_in_month = 31;
                }

                for(int day = 1; day <= days_in_month; ++day) {
                    d_datekey = year * 10000 + month * 100 + day;

                    /*
                        d_date, d_dateofweek, d_month, d_year, d_yearmonthnum, d_yearmonth,
                        d_daynuminweek, d_daynuminmonth, d_daynuminyear, d_monthnuminyear,
                        d_weeknuminyear, d_sellingseason, d_lastdayinweekfl,
                        d_lastdayinmonthfl, d_holidayfl, d_weekdayfl
                    */
                    // For simplicity, we fill in dummy data for other fields
                    snprintf(d_date, sizeof(d_date), "%d", d_datekey);
                    snprintf(d_dateofweek, sizeof(d_dateofweek), "Monday");
                    snprintf(d_month, sizeof(d_month), "January");
                    d_year = year;
                    d_yearmonthnum = year * 100 + month;
                    snprintf(d_yearmonth, sizeof(d_yearmonth), "%04d-%02d", year, month);
                    d_daynuminweek = 1;
                    d_daynuminmonth = day;
                    d_daynuminyear = 1; // Simplified
                    d_monthnuminyear = month;
                    d_weeknuminyear = 1; // Simplified
                    snprintf(d_sellingseason, sizeof(d_sellingseason), "Winter");
                    d_lastdayinweekfl = 'N';
                    d_lastdayinmonthfl = (day == days_in_month) ? 'Y' : 'N';
                    d_holidayfl = 'N';
                    d_weekdayfl = 'Y';
                    
                    memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));

                    int offset = 0;

                    memcpy(record.raw_data_ + offset, (char *)&d_datekey, sizeof(int));
                    offset += sizeof(int);
                    memcpy(record.raw_data_ + offset, d_date, 18);
                    offset += 18;
                    memcpy(record.raw_data_ + offset, d_dateofweek, 9);
                    offset += 9;
                    memcpy(record.raw_data_ + offset, d_month, 9);
                    offset += 9;
                    memcpy(record.raw_data_ + offset, (char *)&d_year, sizeof(int));
                    offset += sizeof(int);
                    memcpy(record.raw_data_ + offset, (char *)&d_yearmonthnum, sizeof(int));
                    offset += sizeof(int);
                    memcpy(record.raw_data_ + offset, d_yearmonth, 7);
                    offset += 7;
                    memcpy(record.raw_data_ + offset, (char *)&d_daynuminweek, sizeof(int));
                    offset += sizeof(int);
                    memcpy(record.raw_data_ + offset, (char *)&d_daynuminmonth, sizeof(int));
                    offset += sizeof(int);
                    memcpy(record.raw_data_ + offset, (char *)&d_daynuminyear, sizeof(int));
                    offset += sizeof(int);
                    memcpy(record.raw_data_ + offset, (char *)&d_monthnuminyear, sizeof(int));
                    offset += sizeof(int);
                    memcpy(record.raw_data_ + offset, (char *)&d_weeknuminyear, sizeof(int));
                    offset += sizeof(int);
                    memcpy(record.raw_data_ + offset, d_sellingseason, 15);
                    offset += 15;
                    memcpy(record.raw_data_ + offset, (char *)&d_lastdayinweekfl, 1);
                    offset += 1;
                    memcpy(record.raw_data_ + offset, (char *)&d_lastdayinmonthfl, 1);
                    offset += 1;
                    memcpy(record.raw_data_ + offset, (char *)&d_holidayfl, 1);
                    offset += 1;
                    memcpy(record.raw_data_ + offset, (char *)&d_weekdayfl, 1);
                    offset += 1;
                    assert(offset == tab_meta.record_length_);
                    
                    index_handle->insert_entry(record.raw_data_, record.record_, txn);
                }
            }
        }

    }
};

class History {
/**
 * "CREATE TABLE HAT.HISTORY (\n"
                "	H_ORDERKEY INTEGER NOT NULL,\n"
                "	H_CUSTKEY INTEGER NOT NULL,\n"
                "	H_AMOUNT DECIMAL\n"
                ")",
 */

// SF * 1500000 records

public:
    int     h_orderkey      ;
    int     h_custkey       ;
    float   h_amount        ;

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "history";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("h_orderkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("h_custkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("h_amount", ColType::TYPE_FLOAT, 4));
        std::vector<std::string> pkeys;
        // pkeys.emplace_back("h_orderkey");
        // pkeys.emplace_back("h_custkey");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }
    void print_record();
    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan);
    void generate_table_data(int SF, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr);
};
}