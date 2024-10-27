#include "tpch_config.h"

#include "benchmark/util/random.h"
#include "benchmark/util/clock.h"

#include <cstring>
#include <iostream>
#include <cstdio>
#include <vector>
#include <fstream>

#include "system/sm.h"

#include "debug_log.h"

#define REGION_COND_RANGE 5     // 不超过5
#define NATION_COND_RANGE 25     // 不超过25
#define NORMAL_INT_COND_RANGE 10000
#define DATE_COND_RANGE "1992-01-01"
#define DATE_MAX_NUM 84
#define MKTSEGMENT_MAX_NUM 5

namespace TPCH_TABLE{

/*
    table region
*/
class Region {

// CREATE TABLE REGION (
//   R_REGIONKEY INT NOT NULL,
//   R_NAME CHAR(25) NOT NULL,
//   R_COMMENT VARCHAR(152) DEFAULT NULL,
//   PRIMARY KEY (R_REGIONKEY)
// );

public:
    int     r_regionkey;    // int  primary key
    char    r_name[25];     // varchar
    char    r_comment[152]; // varchar

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "region";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("r_regionkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("r_name", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("r_comment", ColType::TYPE_STRING, 152));
        std::vector<std::string> pkeys;
        /*** pkeys for Q5 ***/
        pkeys.emplace_back("r_regionkey");
        /*** pkeys for Q5 ***/
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }
    
    // 为了尽可能避免最后查询结果为空集，非join条件统一用小于符号，join条件统一用等于符号
    // region比较特殊，r_name查询应该是等值查询，所以region只能出现一次filtercond
    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan) {
        if(is_index_scan) {
            TabCol lhs_col = {.tab_name = "region", .col_name = "r_regionkey"};
            Value val;
            // val.set_int(RandomGenerator::generate_random_int(1, REGION_NUM));
            val.set_int(REGION_COND_RANGE);
            val.init_raw(sizeof(int));
            Condition cond = {.lhs_col = std::move(lhs_col), .op = OP_LT, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
            index_conds.push_back(std::move(cond));
        }
        else {
            TabCol lhs_col = {.tab_name = "region", .col_name = "r_name"};
            Value val;
            RandomGenerator::generate_random_str(r_name, 25);
            val.set_str(std::move(std::string(r_name, 25)));
            val.init_raw(25);
            Condition fil_cond = {.lhs_col = std::move(lhs_col), .op = OP_EQ, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
            filter_conds.push_back(std::move(fil_cond));
        }
    }

    Region() {}

    void print_record() {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void generate_table_data(int sf, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("region");
        auto index_handle = sm_mgr->primary_index_.at("region").get();
        Record record(tab_meta.record_length_);

        for(r_regionkey = 1; r_regionkey <= REGION_NUM; ++r_regionkey) {

            /*
                r_name, r_comment
            */
            // RandomGenerator::generate_random_str(r_name, 25);
            RandomGenerator::get_region_from_region_key(r_name, 25, r_regionkey);
            RandomGenerator::generate_random_str(r_comment, 152);

            memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));
            
            int offset = 0;

            memcpy(record.raw_data_ + offset, (char *)&r_regionkey, sizeof(int));
            offset += sizeof(int);
            memcpy(record.raw_data_ + offset, (char *)r_name, 25);
            offset += 25;
            memcpy(record.raw_data_ + offset, (char *)r_comment, 152);
            offset += 152;

            assert(offset == tab_meta.record_length_);

            index_handle->insert_entry(record.raw_data_, record.record_, txn);
        }
    }

    void generate_data_csv(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void write_data_into_file(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
};


/*
    table nation
    
    CREATE TABLE NATION (
    N_NATIONKEY INT NOT NULL,
    N_NAME CHAR(25) NOT NULL,
    N_REGIONKEY INT NOT NULL,
    N_COMMENT VARCHAR(152) DEFAULT NULL,
    PRIMARY KEY (N_NATIONKEY)
);
*/
class Nation {

public:
    int     n_nationkey;    // int  primary key
    char    n_name[25];     // varchar
    int     n_regionkey;    // region key
    char    n_comment[152];      // varchar

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "nation";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("n_nationkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("n_name", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("n_regionkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("n_comment", ColType::TYPE_STRING, 152));
        std::vector<std::string> pkeys;
        /*** pkeys for Q5 ***/
        pkeys.emplace_back("n_nationkey");
        /*** pkeys for Q5 ***/
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    Nation() {}

    void print_record() {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan) {
        int nation_key;
        if(is_index_scan) {
            TabCol lhs_col = {.tab_name = "nation", .col_name = "n_nationkey"};
            Value val;
            // nation_key = RandomGenerator::generate_random_int(10, REGION_NUM * ONE_REGION_PER_NATION);
            val.set_int(NATION_COND_RANGE);
            val.init_raw(sizeof(int));
            Condition cond = {.lhs_col = std::move(lhs_col), .op = OP_LT, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
            index_conds.push_back(std::move(cond));
        }

        TabCol lhs_col = {.tab_name = "nation", .col_name = "n_regionkey"};
        Value val;
        // val.set_int(RandomGenerator::generate_random_int(1, 5));
        val.set_int(REGION_COND_RANGE);
        val.init_raw(sizeof(int));
        Condition cond = {.lhs_col = std::move(lhs_col), .op = OP_LT, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
        filter_conds.push_back(std::move(cond));
    }

    void generate_table_data(int sf, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("nation");
        auto index_handle = sm_mgr->primary_index_.at("nation").get();
        Record record(tab_meta.record_length_);

        // int nationkey_max = REGION_NUM * ONE_REGION_PER_NATION;
        n_nationkey = 0;
        for(n_regionkey = 1; n_regionkey <= REGION_NUM; ++n_regionkey) {
            for(int cnt = 1; cnt <= ONE_REGION_PER_NATION; ++cnt) {
                n_nationkey ++;

                /*
                    random generate: n_name, n_comment
                */
               RandomGenerator::get_nation_from_region_nation_key(n_regionkey, cnt, n_name);
                // RandomGenerator::generate_random_str(n_name, 25);
                RandomGenerator::generate_random_str(n_comment, 152);

                /*
                    generate record content
                */
                memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));

                int offset = 0;
                
                memcpy(record.raw_data_ + offset, (char *)&n_nationkey, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, n_name, 25);
                offset += 25;
                memcpy(record.raw_data_ + offset, (char *)&n_regionkey, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, n_comment, 152);
                offset += 152;

                assert(offset == tab_meta.record_length_);

                /*
                    insert data
                */
                index_handle->insert_entry(record.raw_data_, record.record_, txn);
            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void write_data_into_file(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
};

class Nation2 {

public:
    int     n_nationkey;    // int  primary key
    char    n_name[25];     // varchar
    int     n_regionkey;    // region key
    char    n_comment[152];      // varchar

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "nation2";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("n2_nationkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("n2_name", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("n2_regionkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("n2_comment", ColType::TYPE_STRING, 152));
        std::vector<std::string> pkeys;
        /*** pkeys for Q5 ***/
        pkeys.emplace_back("n2_nationkey");
        /*** pkeys for Q5 ***/
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    Nation2() {}

    void print_record() {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan) {
        int nation_key;
        if(is_index_scan) {
            TabCol lhs_col = {.tab_name = "nation2", .col_name = "n2_nationkey"};
            Value val;
            // nation_key = RandomGenerator::generate_random_int(10, REGION_NUM * ONE_REGION_PER_NATION);
            val.set_int(NATION_COND_RANGE);
            val.init_raw(sizeof(int));
            Condition cond = {.lhs_col = std::move(lhs_col), .op = OP_LT, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
            index_conds.push_back(std::move(cond));
        }

        TabCol lhs_col = {.tab_name = "nation2", .col_name = "n2_regionkey"};
        Value val;
        // val.set_int(RandomGenerator::generate_random_int(1, 5));
        val.set_int(REGION_COND_RANGE);
        val.init_raw(sizeof(int));
        Condition cond = {.lhs_col = std::move(lhs_col), .op = OP_LT, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
        filter_conds.push_back(std::move(cond));
    }

    void generate_table_data(int sf, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("nation2");
        auto index_handle = sm_mgr->primary_index_.at("nation2").get();
        Record record(tab_meta.record_length_);

        // int nationkey_max = REGION_NUM * ONE_REGION_PER_NATION;
        n_nationkey = 0;
        for(n_regionkey = 1; n_regionkey <= REGION_NUM; ++n_regionkey) {
            for(int cnt = 1; cnt <= ONE_REGION_PER_NATION; ++cnt) {
                n_nationkey ++;

                /*
                    random generate: n_name, n_comment
                */
               RandomGenerator::get_nation_from_region_nation_key(n_regionkey, cnt, n_name);
                // RandomGenerator::generate_random_str(n_name, 25);
                RandomGenerator::generate_random_str(n_comment, 152);

                /*
                    generate record content
                */
                memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));

                int offset = 0;
                
                memcpy(record.raw_data_ + offset, (char *)&n_nationkey, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, n_name, 25);
                offset += 25;
                memcpy(record.raw_data_ + offset, (char *)&n_regionkey, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, n_comment, 152);
                offset += 152;

                assert(offset == tab_meta.record_length_);

                /*
                    insert data
                */
                index_handle->insert_entry(record.raw_data_, record.record_, txn);
            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void write_data_into_file(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
};


/*
    table part

    CREATE TABLE PART (
    P_PARTKEY INT NOT NULL,
    P_NAME VARCHAR(55) NOT NULL,
    P_MFGR CHAR(25) NOT NULL,
    P_BRAND CHAR(10) NOT NULL,
    P_TYPE VARCHAR(25) NOT NULL,
    P_SIZE INT NOT NULL,
    P_CONTAINER CHAR(10) NOT NULL,
    P_RETAILPRICE DECIMAL(15,2) NOT NULL,
    P_COMMENT VARCHAR(23) NOT NULL,
    PRIMARY KEY (P_PARTKEY)
);
*/

class Part {

public:
    int     p_partkey;          // int  primary key
    char    p_name[55];         
    char    p_mfgr[25];         
    char    p_brand[10];       
    char    p_type[25];         
    int     p_size;
    char    p_container[10];
    float   p_retailprice;
    char    p_comment[23];

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "part";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("p_partkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("p_name", ColType::TYPE_STRING, 55));
        col_defs.emplace_back(ColDef("p_mfgr", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("p_brand", ColType::TYPE_STRING, 10));
        col_defs.emplace_back(ColDef("p_type", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("p_size", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("p_container", ColType::TYPE_STRING, 10));
        col_defs.emplace_back(ColDef("p_retailprice", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("p_comment", ColType::TYPE_STRING, 23));
        std::vector<std::string> pkeys;
        /*** pkeys for Q5 ***/
        pkeys.emplace_back("p_partkey");
        /*** pkeys for Q5 ***/
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    Part() {}

    void print_record() {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan) {
        assert(is_index_scan == true);
        
        TabCol lhs_col = {.tab_name = "part", .col_name = "p_partkey"};
        Value val;
        // val.set_int(RandomGenerator::generate_random_int(1, SF * ONE_SF_PER_PART));
        // val.set_int(RandomGenerator::generate_random_int(1, MAX_COND_RANGE));
        val.set_int(NORMAL_INT_COND_RANGE);
        val.init_raw(sizeof(int));
        Condition cond = {.lhs_col = std::move(lhs_col), .op = OP_LT, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
        index_conds.push_back(std::move(cond));
    }

    void generate_table_data(int SF, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("part");
        auto index_handle = sm_mgr->primary_index_.at("part").get();
        Record record(tab_meta.record_length_);

        int p_partkey_max = SF * ONE_SF_PER_PART;
        for(p_partkey = 1; p_partkey <= p_partkey_max; ++p_partkey) {
        
            /*
                random generate: p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment
            */
            RandomGenerator::generate_random_str(p_name, 55);
            RandomGenerator::generate_random_str(p_mfgr, 25);
            RandomGenerator::generate_random_str(p_brand, 10);
            RandomGenerator::generate_random_str(p_type, 25);
            p_size = RandomGenerator::generate_random_int(1, 2000);
            RandomGenerator::generate_random_str(p_container, 10);
            p_retailprice = RandomGenerator::generate_random_float(1, 20000);
            RandomGenerator::generate_random_str(p_comment, 23);

            /*
                generate record content
            */
            memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));

            int offset = 0;
            
            memcpy(record.raw_data_ + offset, (char *)&p_partkey, sizeof(int));
            offset += sizeof(int);
            memcpy(record.raw_data_ + offset, p_name, 55);
            offset += 55;
            memcpy(record.raw_data_ + offset, p_mfgr, 25);
            offset += 25;
            memcpy(record.raw_data_ + offset, p_brand, 10);
            offset += 10;
            memcpy(record.raw_data_ + offset, p_type, 25);
            offset += 25;
            memcpy(record.raw_data_ + offset, (char *)&p_size, sizeof(int));
            offset += sizeof(int);
            memcpy(record.raw_data_ + offset, p_container, 10);
            offset += 10;
            memcpy(record.raw_data_ + offset, (char *)&p_retailprice, sizeof(float));
            offset += sizeof(float);
            memcpy(record.raw_data_ + offset, p_comment, 23);
            offset += 23;

            assert(offset == tab_meta.record_length_);

            /*
                insert data
            */
            index_handle->insert_entry(record.raw_data_, record.record_, txn);
        }
    }

    void generate_data_csv(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void write_data_into_file(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
};


/*
    table customer

    CREATE TABLE CUSTOMER (
    C_CUSTKEY INT NOT NULL,
    C_NAME VARCHAR(25) NOT NULL,
    C_ADDRESS VARCHAR(40) NOT NULL,
    C_NATIONKEY INT NOT NULL,
    C_PHONE CHAR(15) NOT NULL,
    C_ACCTBAL DECIMAL(15,2) NOT NULL,
    C_MKTSEGMENT CHAR(10) NOT NULL,
    C_COMMENT VARCHAR(117) NOT NULL,
    PRIMARY KEY (C_CUSTKEY),
    KEY CUSTOMER_FK1 (C_NATIONKEY),
    CONSTRAINT CUSTOMER_IBFK_1 FOREIGN KEY (C_NATIONKEY) REFERENCES NATION (N_NATIONKEY)
    );
);
*/

class Customer {

public:
    int     c_custkey;          // int  primary key
    char    c_name[25];
    char    c_address[40];
    int     c_nationkey;
    char    c_phone[15];
    float   c_acctbal;
    char    c_mktsegment[11];
    char    c_comment[117];

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "customer";
        std::vector<ColDef> col_defs;

        col_defs.emplace_back(ColDef("c_mktsegment", ColType::TYPE_STRING, 10));
        col_defs.emplace_back(ColDef("c_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("c_nationkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("c_custkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("c_name", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("c_address", ColType::TYPE_STRING, 40));
        col_defs.emplace_back(ColDef("c_phone", ColType::TYPE_STRING, 15));
        col_defs.emplace_back(ColDef("c_acctbal", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("c_comment", ColType::TYPE_STRING, 117));
        std::vector<std::string> pkeys;
        /*** pkeys for Q3 ***/
        pkeys.emplace_back("c_mktsegment");
        pkeys.emplace_back("c_id");
        /*** pkeys for Q3 ***/

        /*** pkeys for Q7 ***/
        // pkeys.emplace_back("c_nationkey");
        // pkeys.emplace_back("c_id");
        /*** pkeys for Q7 ***/
        // pkeys.emplace_back("c_custkey");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    Customer() {}

    void print_record() {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan) {
        assert(is_index_scan == true);
        
        // TabCol lhs_col = {.tab_name = "customer", .col_name = "c_custkey"};
        // Value val;
        // // val.set_int(RandomGenerator::generate_random_int(1, SF * ONE_SF_PER_CUSTOMER));
        // // val.set_int(RandomGenerator::generate_random_int(1, MAX_COND_RANGE));
        // val.set_int(NORMAL_INT_COND_RANGE);
        // val.init_raw(sizeof(int));
        // Condition cond = {.lhs_col = std::move(lhs_col), .op = OP_LT, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
        // index_conds.push_back(std::move(cond));
        TabCol lhs_col = {.tab_name = "customer", .col_name = "c_nationkey"};
        Value val;
        val.set_int(1);
        val.init_raw(sizeof(int));
        Condition cond = {.lhs_col = std::move(lhs_col), .op = OP_EQ, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
        index_conds.push_back(std::move(cond));
    }

    void generate_table_data(int SF, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("customer");
        auto index_handle = sm_mgr->primary_index_.at("customer").get();
        Record record(tab_meta.record_length_);

        /*
            1 <= c_custkey <= SF * ONE_SF_PER_CUSTOMER
        */
        int c_custkey_max = SF * ONE_SF_PER_CUSTOMER;

        /*
            1 <= c_nationkey <= 25
        */
        int record_per_key = c_custkey_max / MKTSEGMENT_MAX_NUM;
        // int record_per_key = c_custkey_max / (REGION_NUM * ONE_REGION_PER_NATION);
        c_custkey = 1;

        RandomMapping random_mapping(c_custkey_max);

        for(int mktseg_idx = 0; mktseg_idx < MKTSEGMENT_MAX_NUM; ++ mktseg_idx) {
            RandomGenerator::generate_mktsegment_from_idx(c_mktsegment, mktseg_idx);
            std::cout << c_mktsegment << std::endl;
        // for(int c_nationkey = 1; c_nationkey <= REGION_NUM * ONE_ORDER_PER_LINENUM; ++c_nationkey) {

            for(int record_idx = 0; record_idx < record_per_key && c_custkey <= c_custkey_max; ++record_idx) {
                RandomGenerator::generate_random_str(c_name, 25);
                RandomGenerator::generate_random_str(c_address, 40);
                // c_nationkey = RandomGenerator::generate_random_int(1, REGION_NUM * ONE_REGION_PER_NATION);
                RandomGenerator::generate_random_mktsegment(c_mktsegment);
                RandomGenerator::generate_random_str(c_phone, 15);
                c_acctbal = RandomGenerator::generate_random_float(1, 20000);
                // RandomGenerator::generate_random_str(c_mktsegment, 10);
                RandomGenerator::generate_random_str(c_comment, 117);

                memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));
                

                int offset = 0;
                memcpy(record.raw_data_ + offset, c_mktsegment, 10);
                offset += 10;
                memcpy(record.raw_data_ + offset, (char *)&c_custkey, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, (char *)&c_nationkey, sizeof(int));
                offset += sizeof(int);
                int actual_custkey = random_mapping.f(c_custkey);
                // std::cout << "c_custkey: " << c_custkey << " actual_custkey: " << actual_custkey << std::endl;
                memcpy(record.raw_data_ + offset, (char *)&actual_custkey, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, c_name, 25);
                offset += 25;
                memcpy(record.raw_data_ + offset, c_address, 40);
                offset += 40;
                memcpy(record.raw_data_ + offset, c_phone, 15);
                offset += 15;
                memcpy(record.raw_data_ + offset, (char *)&c_acctbal, sizeof(float));
                offset += sizeof(float);
                memcpy(record.raw_data_ + offset, c_comment, 117);
                offset += 117;

                assert(offset == tab_meta.record_length_);

                /*
                    insert data
                */
                index_handle->insert_entry(record.raw_data_, record.record_, txn);
                c_custkey ++;
            }

        }
        // for(c_custkey = 1; c_custkey <= c_custkey_max; ++c_custkey) {
        
        //     /*
        //         col_defs.emplace_back(ColDef("c_custkey", ColType::TYPE_INT, 4));
        //         col_defs.emplace_back(ColDef("c_name", ColType::TYPE_STRING, 25));
        //         col_defs.emplace_back(ColDef("c_address", ColType::TYPE_STRING, 40));
        //         col_defs.emplace_back(ColDef("c_nationkey", ColType::TYPE_INT, 4));
        //         col_defs.emplace_back(ColDef("c_phone", ColType::TYPE_STRING, 15));
        //         col_defs.emplace_back(ColDef("c_acctbal", ColType::TYPE_FLOAT, 4));
        //         col_defs.emplace_back(ColDef("c_mktsegment", ColType::TYPE_STRING, 10));
        //         col_defs.emplace_back(ColDef("c_comment", ColType::TYPE_STRING, 117));
        //     */
        //     RandomGenerator::generate_random_str(c_name, 25);
        //     RandomGenerator::generate_random_str(c_address, 40);
        //     c_nationkey = RandomGenerator::generate_random_int(1, REGION_NUM * ONE_REGION_PER_NATION);
        //     RandomGenerator::generate_random_str(c_phone, 15);
        //     c_acctbal = RandomGenerator::generate_random_float(1, 20000);
        //     // RandomGenerator::generate_random_str(c_mktsegment, 10);
        //     RandomGenerator::generate_random_mktsegment(c_mktsegment);
        //     RandomGenerator::generate_random_str(c_comment, 117);

        //     /*
        //         generate record content
        //     */
        //     memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));

        //     int offset = 0;
            
        //     memcpy(record.raw_data_ + offset, (char *)&c_custkey, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.raw_data_ + offset, c_name, 25);
        //     offset += 25;
        //     memcpy(record.raw_data_ + offset, c_address, 40);
        //     offset += 40;
        //     memcpy(record.raw_data_ + offset, (char *)&c_nationkey, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.raw_data_ + offset, c_phone, 15);
        //     offset += 15;
        //     memcpy(record.raw_data_ + offset, (char *)&c_acctbal, sizeof(float));
        //     offset += sizeof(float);
        //     memcpy(record.raw_data_ + offset, c_mktsegment, 10);
        //     offset += 10;
        //     memcpy(record.raw_data_ + offset, c_comment, 117);
        //     offset += 117;

        //     assert(offset == tab_meta.record_length_);

        //     /*
        //         insert data
        //     */
        //     index_handle->insert_entry(record.raw_data_, record.record_, txn);
        // }
    }

    void generate_data_csv(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void write_data_into_file(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
};


/*
    table orders

    CREATE TABLE ORDERS (
    O_ORDERKEY INT NOT NULL,
    O_CUSTKEY INT NOT NULL,
    O_ORDERSTATUS CHAR(1) NOT NULL,
    O_TOTALPRICE DECIMAL(15,2) NOT NULL,
    O_ORDERDATE DATE NOT NULL,
    O_ORDERPRIORITY CHAR(15) NOT NULL,
    O_CLERK CHAR(15) NOT NULL,
    O_SHIPPRIORITY INT NOT NULL,
    O_COMMENT VARCHAR(79) NOT NULL,
    PRIMARY KEY (O_ORDERKEY),
    KEY ORDERS_FK1 (O_CUSTKEY),
    CONSTRAINT ORDERS_IBFK_1 FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER (C_CUSTKEY)
    );
);
*/

class Orders {

public:
    int     o_orderkey;          // int  primary key
    int     o_custkey;              
    char    o_orderstatus[1]; 
    float   o_totalprice;
    char    o_orderdate[RandomGenerator::DATE_SIZE + 1];
    char    o_orderpriority[15];
    char    o_clerk[15];
    int     o_shippriority;
    char    o_comment[79];


    void create_table(SmManager* sm_mgr) {
        std::string table_name = "orders";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("o_orderdate", ColType::TYPE_STRING, RandomGenerator::DATE_SIZE));
        col_defs.emplace_back(ColDef("o_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("o_orderkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("o_custkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("o_orderstatus", ColType::TYPE_STRING, 1));
        col_defs.emplace_back(ColDef("o_totalprice", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("o_orderpriority", ColType::TYPE_STRING, 15));
        col_defs.emplace_back(ColDef("o_clerk", ColType::TYPE_STRING, 15));
        col_defs.emplace_back(ColDef("o_shippriority", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("o_comment", ColType::TYPE_STRING, 79));
        
        std::vector<std::string> pkeys;
        // pkeys.emplace_back("o_orderkey");
        // pkeys.emplace_back("o_orderdate");
        // pkeys.emplace_back("o_custkey");
        /*** pkeys for Q5 ***/
        pkeys.emplace_back("o_orderdate");
        pkeys.emplace_back("o_id");
        /*** pkeys for Q5 ***/

        /*** pkeys for Q10 ***/
        // pkeys.emplace_back("o_id");
        // pkeys.emplace_back("o_orderdate");
        /*** pkeys for Q10 ***/
        // pkeys.emplace_back("o_orderkey");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    Orders() {}

    void print_record() {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan) {
        assert(is_index_scan == true);
        
        // TabCol lhs_col = {.tab_name = "orders", .col_name = "o_orderkey"};
        // Value val;
        // // val.set_int(RandomGenerator::generate_random_int(1, SF * ONE_SF_PER_ORDER));
        // // val.set_int(RandomGenerator::generate_random_int(1, MAX_COND_RANGE));
        // val.set_int(NORMAL_INT_COND_RANGE);
        // val.init_raw(sizeof(int));
        // Condition cond = {.lhs_col = std::move(lhs_col), .op = OP_LE, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
        // index_conds.push_back(std::move(cond));

        TabCol lhs_col2 = {.tab_name = "orders", .col_name = "o_orderdate"};
        Value val2;
        // RandomGenerator::generate_random_date(o_orderdate);
        // val2.set_str(std::string(o_orderdate, RandomGenerator::DATE_SIZE));
        val2.set_str(DATE_COND_RANGE);
        val2.init_raw(RandomGenerator::DATE_SIZE);
        Condition cond2 = {.lhs_col = std::move(lhs_col2), .op = OP_EQ, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val2};
        index_conds.push_back(std::move(cond2));
        
        TabCol lhs_col = {.tab_name = "orders", .col_name = "o_id"};
        Value val;
        // val.set_int(RandomGenerator::generate_random_int(1, SF * ONE_SF_PER_ORDER));
        // val.set_int(RandomGenerator::generate_random_int(1, MAX_COND_RANGE));
        val.set_int(NORMAL_INT_COND_RANGE);
        val.init_raw(sizeof(int));
        Condition cond = {.lhs_col = std::move(lhs_col), .op = OP_LE, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
        index_conds.push_back(std::move(cond));
    }

    void generate_table_data(int SF, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("orders");
        auto index_handle = sm_mgr->primary_index_.at("orders").get();
        Record record(tab_meta.record_length_);

        /*
            1 <= o_orderkey <= SF * ONE_SF_PER_ORDER
        */
        int o_orderkey_max = SF * ONE_SF_PER_ORDER;

        int record_per_key = o_orderkey_max / DATE_MAX_NUM;
        o_orderkey = 1;

        /*
            1 <= o_custkey <= SF * ONE_SF_PER_CUSTOMER
        */
        int c_custkey_max = SF * ONE_SF_PER_CUSTOMER;

        RandomMapping random_mapping(o_orderkey_max);

        for(int o_orderdate_idx = 0; o_orderdate_idx < DATE_MAX_NUM; ++o_orderdate_idx) {
            RandomGenerator::generate_date_from_idx(o_orderdate, o_orderdate_idx);

            for(int record_idx = 0; record_idx < record_per_key && o_orderkey <= o_orderkey_max; ++record_idx) {
                o_custkey = RandomGenerator::generate_random_int(1, c_custkey_max);
                RandomGenerator::generate_random_str(o_orderstatus, 1);
                o_totalprice = RandomGenerator::generate_random_float(1, 20000);
                // RandomGenerator::generate_random_str(o_orderdate, Clock::DATETIME_SIZE + 1);
                RandomGenerator::generate_random_str(o_orderpriority, 15);
                RandomGenerator::generate_random_str(o_clerk, 15);
                o_shippriority = RandomGenerator::generate_random_int(1, 20000);
                RandomGenerator::generate_random_str(o_comment, 79);

                /*
                    generate record content
                */
                memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));

                int offset = 0;
                memcpy(record.raw_data_ + offset, o_orderdate, RandomGenerator::DATE_SIZE);
                offset += (RandomGenerator::DATE_SIZE);
                memcpy(record.raw_data_ + offset, (char *)&o_orderkey, sizeof(int));
                offset += sizeof(int);
                int actual_orderkey = random_mapping.f(o_orderkey);
                memcpy(record.raw_data_ + offset, (char *)&actual_orderkey, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, (char *)&o_custkey, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, o_orderstatus, 1);
                offset += 1;
                memcpy(record.raw_data_ + offset, (char *)&o_totalprice, sizeof(float));
                offset += sizeof(float);
                memcpy(record.raw_data_ + offset, o_orderpriority, 15);
                offset += 15;
                memcpy(record.raw_data_ + offset, o_clerk, 15);
                offset += 15;
                memcpy(record.raw_data_ + offset, (char *)&o_shippriority, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, o_comment, 79);
                offset += 79;

                assert(offset == tab_meta.record_length_);

                /*
                    insert data
                */
                index_handle->insert_entry(record.raw_data_, record.record_, txn);

                o_orderkey ++;
            }
        }

        // for(o_orderkey = 1; o_orderkey <= o_orderkey_max; ++o_orderkey) {
        
        //     /*
        //         col_defs.emplace_back(ColDef("o_orderkey", ColType::TYPE_INT, 4));
        //         col_defs.emplace_back(ColDef("o_custkey", ColType::TYPE_INT, 4));
        //         col_defs.emplace_back(ColDef("o_orderstatus", ColType::TYPE_STRING, 1));
        //         col_defs.emplace_back(ColDef("o_totalprice", ColType::TYPE_FLOAT, 4));
        //         col_defs.emplace_back(ColDef("o_orderdate", ColType::TYPE_STRING, Clock::DATETIME_SIZE + 1));
        //         col_defs.emplace_back(ColDef("o_orderpriority", ColType::TYPE_STRING, 15));
        //         col_defs.emplace_back(ColDef("o_clerk", ColType::TYPE_STRING, 15));
        //         col_defs.emplace_back(ColDef("o_shippriority", ColType::TYPE_INT, 4));
        //         col_defs.emplace_back(ColDef("o_comment", ColType::TYPE_STRING, 79));
        //     */
        // //    int rnd = RandomGenerator::generate_random_int(1, 2);
        // //    if(rnd == 1)
        // //         o_custkey = RandomGenerator::generate_random_int(1, c_custkey_max);
        // //     else 
        // //         o_custkey = o_orderkey;
        //     o_custkey = RandomGenerator::generate_random_int(1, c_custkey_max);
        //     RandomGenerator::generate_random_str(o_orderstatus, 1);
        //     o_totalprice = RandomGenerator::generate_random_float(1, 20000);
        //     // RandomGenerator::generate_random_str(o_orderdate, Clock::DATETIME_SIZE + 1);
        //     RandomGenerator::generate_random_date(o_orderdate);
        //     RandomGenerator::generate_random_str(o_orderpriority, 15);
        //     RandomGenerator::generate_random_str(o_clerk, 15);
        //     o_shippriority = RandomGenerator::generate_random_int(1, 20000);
        //     RandomGenerator::generate_random_str(o_comment, 79);

        //     /*
        //         generate record content
        //     */
        //     memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));

        //     int offset = 0;
        //     memcpy(record.raw_data_ + offset, o_orderdate, RandomGenerator::DATE_SIZE + 1);
        //     offset += (RandomGenerator::DATE_SIZE + 1);
        //     memcpy(record.raw_data_ + offset, (char *)&o_orderkey, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.raw_data_ + offset, (char *)&o_custkey, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.raw_data_ + offset, o_orderstatus, 1);
        //     offset += 1;
        //     memcpy(record.raw_data_ + offset, (char *)&o_totalprice, sizeof(float));
        //     offset += sizeof(float);
        //     memcpy(record.raw_data_ + offset, o_orderpriority, 15);
        //     offset += 15;
        //     memcpy(record.raw_data_ + offset, o_clerk, 15);
        //     offset += 15;
        //     memcpy(record.raw_data_ + offset, (char *)&o_shippriority, sizeof(int));
        //     offset += sizeof(int);
        //     memcpy(record.raw_data_ + offset, o_comment, 79);
        //     offset += 79;

        //     assert(offset == tab_meta.record_length_);

        //     /*
        //         insert data
        //     */
        //     index_handle->insert_entry(record.raw_data_, record.record_, txn);
        // }
    }

    void generate_data_csv(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void write_data_into_file(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
};


/*
    table supplier

    CREATE TABLE SUPPLIER (
    S_SUPPKEY INT NOT NULL,
    S_NAME CHAR(25) NOT NULL,
    S_ADDRESS VARCHAR(40) NOT NULL,
    S_NATIONKEY INT NOT NULL,
    S_PHONE CHAR(15) NOT NULL,
    S_ACCTBAL DECIMAL(15,2) NOT NULL,
    S_COMMENT VARCHAR(101) NOT NULL,
    PRIMARY KEY (S_SUPPKEY),
    KEY SUPPLIER_FK1 (S_NATIONKEY),
    CONSTRAINT SUPPLIER_IBFK_1 FOREIGN KEY (S_NATIONKEY) REFERENCES NATION (N_NATIONKEY)
    );

);
*/

class Supplier {

public:
    int     s_suppkey       ;         // int primary key
    char    s_name[25]      ;    
    char    s_address[40]   ;
    int     s_nationkey     ;
    char    s_phone[15]     ;
    float   s_acctbal       ;
    char    s_comment[101]  ;

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "supplier";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("s_suppkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("s_nationkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("s_name", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("s_address", ColType::TYPE_STRING, 40));
        col_defs.emplace_back(ColDef("s_phone", ColType::TYPE_STRING, 15));
        col_defs.emplace_back(ColDef("s_acctbal", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("s_comment", ColType::TYPE_STRING, 101));

        std::vector<std::string> pkeys;
        /*** pkeys for Q5 ***/
        pkeys.emplace_back("s_suppkey");
        /*** pkeys for Q5 ***/

        /*** pkeys for Q7 ***/
        // pkeys.emplace_back("s_nationkey");
        // pkeys.emplace_back("s_suppkey");
        /*** pkeys for Q7 ***/
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    Supplier() {}

    void print_record() {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan) {
        assert(is_index_scan == true);
        
        TabCol lhs_col = {.tab_name = "supplier", .col_name = "s_suppkey"};
        Value val;
        // val.set_int(RandomGenerator::generate_random_int(1, SF * ONE_SF_PER_SUPPLIER));
        // val.set_int(RandomGenerator::generate_random_int(1, MAX_COND_RANGE));
        val.set_int(NORMAL_INT_COND_RANGE);
        val.init_raw(sizeof(int));
        Condition cond = {.lhs_col = std::move(lhs_col), .op = OP_LT, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
        index_conds.push_back(std::move(cond));
    }

    void generate_table_data(int SF, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("supplier");
        auto index_handle = sm_mgr->primary_index_.at("supplier").get();
        Record record(tab_meta.record_length_);

        /*
            1 <= s_suppkey <= SF * ONE_SF_PER_SUPPLIER
        */
        int s_suppkey_max = SF * ONE_SF_PER_SUPPLIER;

        /*
            1 <= s_nationkey <= SF * ONE_SF_PER_CUSTOMER
        */
        int s_nationkey_max = REGION_NUM * ONE_REGION_PER_NATION;
        for(s_suppkey = 1; s_suppkey <= s_suppkey_max; ++s_suppkey) {
        
            /*
                col_defs.emplace_back(ColDef("s_suppkey", ColType::TYPE_INT, 4));
                col_defs.emplace_back(ColDef("s_name", ColType::TYPE_STRING, 25));
                col_defs.emplace_back(ColDef("s_address", ColType::TYPE_STRING, 40));
                col_defs.emplace_back(ColDef("s_nationkey", ColType::TYPE_INT, 4));
                col_defs.emplace_back(ColDef("s_phone", ColType::TYPE_STRING, 15));
                col_defs.emplace_back(ColDef("s_acctbal", ColType::TYPE_FLOAT, 4));
                col_defs.emplace_back(ColDef("s_comment", ColType::TYPE_STRING, 101));
            */
            RandomGenerator::generate_random_str(s_name, 25);
            RandomGenerator::generate_random_str(s_address, 40);
            s_nationkey = RandomGenerator::generate_random_int(1, s_nationkey_max);
            RandomGenerator::generate_random_str(s_phone, 15);
            s_acctbal = RandomGenerator::generate_random_float(1, 200000);
            RandomGenerator::generate_random_str(s_comment, 101);

            /*
                generate record content
            */
            memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));

            int offset = 0;
            
            memcpy(record.raw_data_ + offset, (char *)&s_suppkey, sizeof(int));
            offset += sizeof(int);
            memcpy(record.raw_data_ + offset, (char *)&s_nationkey, sizeof(int));
            offset += sizeof(int);
            memcpy(record.raw_data_ + offset, s_name, 25);
            offset += 25;
            memcpy(record.raw_data_ + offset, s_address, 40);
            offset += 40;
            memcpy(record.raw_data_ + offset, s_phone, 15);
            offset += 15;            
            memcpy(record.raw_data_ + offset, (char *)&s_acctbal, sizeof(float));
            offset += sizeof(float); 
            memcpy(record.raw_data_ + offset, s_comment, 101);
            offset += 101;              

            assert(offset == tab_meta.record_length_);

            /*
                insert data
            */
            index_handle->insert_entry(record.raw_data_, record.record_, txn);
        }
    }

    void generate_data_csv(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void write_data_into_file(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
};

/*
    table partsupp

    CREATE TABLE PARTSUPP (
    PS_PARTKEY INT NOT NULL,
    PS_SUPPKEY INT NOT NULL,
    PS_AVAILQTY INT NOT NULL,
    PS_SUPPLYCOST DECIMAL(15,2) NOT NULL,
    PS_COMMENT VARCHAR(199) NOT NULL,
    PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY),
    KEY PARTSUPP_FK1 (PS_SUPPKEY),
    CONSTRAINT PARTSUPP_IBFK_1 FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER (S_SUPPKEY),
    CONSTRAINT PARTSUPP_IBFK_2 FOREIGN KEY (PS_PARTKEY) REFERENCES PART (P_PARTKEY)
    );
*/

class PartSupp {

public:
    int     ps_partkey      ;    
    int     ps_suppkey      ; 
    int     ps_availqty     ;
    float   ps_supplycost   ; 
    char    ps_comment[199] ;

    // primary key (ps_partkey, ps_suppkey)   

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "partsupp";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("ps_partkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("ps_suppkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("ps_availqty", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("ps_supplycost", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("ps_comment", ColType::TYPE_STRING, 199));

        std::vector<std::string> pkeys;
        pkeys.emplace_back("ps_partkey");
        pkeys.emplace_back("ps_suppkey");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
    }

    PartSupp() {}

    void print_record() {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan) {
        assert(is_index_scan == true);
        
        TabCol lhs_col = {.tab_name = "partsupp", .col_name = "ps_partkey"};
        Value val;
        // val.set_int(RandomGenerator::generate_random_int(1, SF * ONE_SF_PER_PART));
        // val.set_int(RandomGenerator::generate_random_int(1, MAX_COND_RANGE));
        val.set_int(NORMAL_INT_COND_RANGE);
        val.init_raw(sizeof(int));
        Condition cond = {.lhs_col = std::move(lhs_col), .op = OP_LT, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
        index_conds.push_back(std::move(cond));

        // TabCol col2 = {.tab_name = "partsupp", .col_name = "ps_suppkey"};
        // Value val2;
        // // val2.set_int(RandomGenerator::generate_random_int(1, SF * ONE_SF_PER_SUPPLIER));
        // // val2.set_int(RandomGenerator::generate_random_int(1, MAX_COND_RANGE));
        // val2.set_int(NORMAL_INT_COND_RANGE);
        // val2.init_raw(sizeof(int));
        // Condition cond2 = {.lhs_col = std::move(col2), .op = OP_LT, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val2};
        // filter_conds.push_back(std::move(cond2));
    }

    void generate_table_data(int SF, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("partsupp");
        auto index_handle = sm_mgr->primary_index_.at("partsupp").get();
        Record record(tab_meta.record_length_);

        /*
            1 <= ps_partkey <= SF * ONE_SF_PER_PART
        */
        int ps_partkey_max = SF * ONE_SF_PER_PART;

        /*
            1 <= ps_suppkey <= SF * ONE_SF_PER_
        */
        int ps_suppkey_max = SF * ONE_SF_PER_SUPPLIER;

        for(ps_partkey = 1; ps_partkey <= ps_partkey_max; ++ps_partkey) {
            for(int ps_suppkey_i = 1; ps_suppkey_i <= ONE_PART_PER_SUPP; ++ps_suppkey_i) {
                /*
                    col_defs.emplace_back(ColDef("ps_partkey", ColType::TYPE_INT, 4));
                    col_defs.emplace_back(ColDef("ps_suppkey", ColType::TYPE_INT, 4));
                    col_defs.emplace_back(ColDef("ps_availqty", ColType::TYPE_INT, 4));
                    col_defs.emplace_back(ColDef("ps_supplycost", ColType::TYPE_FLOAT, 4));
                    col_defs.emplace_back(ColDef("ps_comment", ColType::TYPE_STRING, 199));
                */
                // int rnd = RandomGenerator::generate_random_int(1, 2);
                // if(rnd == 1)
                //     ps_suppkey = RandomGenerator::generate_random_int(1, ps_suppkey_max);
                // else
                //     ps_suppkey = ps_partkey + ps_suppkey_i - 1;
                ps_suppkey = RandomGenerator::generate_random_int(1, ps_suppkey_max);
                ps_availqty = RandomGenerator::generate_random_int(1, 200000);
                ps_supplycost = RandomGenerator::generate_random_float(1, 200000);
                RandomGenerator::generate_random_str(ps_comment, 199);

                /*
                    generate record content
                */
                memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));

                int offset = 0;
                
                memcpy(record.raw_data_ + offset, (char *)&ps_partkey, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, (char *)&ps_suppkey, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, (char *)&ps_availqty, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, (char *)&ps_supplycost, sizeof(float));
                offset += sizeof(float);
                memcpy(record.raw_data_ + offset, ps_comment, 199);
                offset += 199;

                assert(offset == tab_meta.record_length_);

                /*
                    insert data
                */
                index_handle->insert_entry(record.raw_data_, record.record_, txn);
                
            }
        }
    }

    void generate_data_csv(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void write_data_into_file(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
};


/*
    table lineitem

    CREATE TABLE LINEITEM (
    L_ORDERKEY INT NOT NULL,
    L_PARTKEY INT NOT NULL,
    L_SUPPKEY INT NOT NULL,
    L_LINENUMBER INT NOT NULL,
    L_QUANTITY DECIMAL(15,2) NOT NULL,
    L_EXTENDEDPRICE DECIMAL(15,2) NOT NULL,
    L_DISCOUNT DECIMAL(15,2) NOT NULL,
    L_TAX DECIMAL(15,2) NOT NULL,
    L_RETURNFLAG CHAR(1) NOT NULL,
    L_LINESTATUS CHAR(1) NOT NULL,
    L_SHIPDATE DATE NOT NULL,
    L_COMMITDATE DATE NOT NULL,
    L_RECEIPTDATE DATE NOT NULL,
    L_SHIPINSTRUCT CHAR(25) NOT NULL,
    L_SHIPMODE CHAR(10) NOT NULL,
    L_COMMENT VARCHAR(44) NOT NULL,
    PRIMARY KEY (L_ORDERKEY,L_LINENUMBER),
    KEY LINEITEM_FK2 (L_PARTKEY,L_SUPPKEY),
    CONSTRAINT LINEITEM_IBFK_1 FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS (O_ORDERKEY),
    CONSTRAINT LINEITEM_IBFK_2 FOREIGN KEY (L_PARTKEY, L_SUPPKEY) REFERENCES PARTSUPP (PS_PARTKEY, PS_SUPPKEY)
    );
*/

class Lineitem {
public:
    int     l_orderkey      ;
    int     l_linenumber    ;
    int     l_partkey       ;
    int     l_suppkey       ; 
    float   l_quantity      ;
    float   l_extendedprice ;
    float   l_discount      ;
    float   l_tax           ;
    char    l_returnflag[1] ;
    char    l_linestatus[1] ;
    char    l_shipdate[RandomGenerator::DATE_SIZE + 1] ;
    char    l_commitdate[RandomGenerator::DATE_SIZE + 1] ;
    char    l_receiptdate[RandomGenerator::DATE_SIZE + 1] ;
    char    l_shipinstruct[25]  ;
    char    l_shipmode[10]      ;
    char    l_comment[4]       ;
    // primary key (l_orderkey,l_linenumber)

    void create_table(SmManager* sm_mgr) {
        std::string table_name = "lineitem";
        std::vector<ColDef> col_defs;
        col_defs.emplace_back(ColDef("l_shipdate", ColType::TYPE_STRING, RandomGenerator::DATE_SIZE));
        col_defs.emplace_back(ColDef("l_id", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("l_orderkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("l_linenumber", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("l_partkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("l_suppkey", ColType::TYPE_INT, 4));
        col_defs.emplace_back(ColDef("l_quantity", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("l_extendedprice", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("l_discount", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("l_tax", ColType::TYPE_FLOAT, 4));
        col_defs.emplace_back(ColDef("l_returnflag", ColType::TYPE_STRING, 1));
        col_defs.emplace_back(ColDef("l_linestatus", ColType::TYPE_STRING, 1));
        col_defs.emplace_back(ColDef("l_commitdate", ColType::TYPE_STRING, RandomGenerator::DATE_SIZE + 1));
        col_defs.emplace_back(ColDef("l_receiptdate", ColType::TYPE_STRING, RandomGenerator::DATE_SIZE + 1));
        col_defs.emplace_back(ColDef("l_shipinstruct", ColType::TYPE_STRING, 25));
        col_defs.emplace_back(ColDef("l_shipmode", ColType::TYPE_STRING, 10));
        col_defs.emplace_back(ColDef("l_comment", ColType::TYPE_STRING, 4));

        std::vector<std::string> pkeys;
        // pkeys.emplace_back("l_orderkey");
        // pkeys.emplace_back("l_linenumber");
        /*** pkeys for Q5 ***/
        pkeys.emplace_back("l_shipdate");
        pkeys.emplace_back("l_id");
        /*** pkeys for Q5 ***/

        /*** pkeys for Q10 ***/
        // pkeys.emplace_back("l_id");
        // pkeys.emplace_back("l_shipdate");
        /*** pkeys for Q10 ***/
        // pkeys.emplace_back("l_orderkey");
        // pkeys.emplace_back("l_linenumber");
        sm_mgr->create_table(table_name, col_defs, pkeys, nullptr);
        // print_name = true;
    }

    Lineitem() {}

    void print_record() {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void get_random_condition(int SF, std::vector<Condition>& index_conds, std::vector<Condition> filter_conds, bool is_index_scan) {
        assert(is_index_scan == true);
        
        // TabCol lhs_col = {.tab_name = "lineitem", .col_name = "l_orderkey"};
        // Value val;
        // // val.set_int(RandomGenerator::generate_random_int(1, SF * ONE_SF_PER_ORDER));
        // // val.set_int(RandomGenerator::generate_random_int(1, MAX_COND_RANGE));
        // val.set_int(NORMAL_INT_COND_RANGE);
        // val.init_raw(sizeof(int));
        // Condition cond = {.lhs_col = std::move(lhs_col), .op = OP_LT, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
        // index_conds.push_back(std::move(cond));

        // TabCol col2 = {.tab_name = "lineitem", .col_name = "l_linenumber"};
        // Value val2;
        // // val2.set_int(RandomGenerator::generate_random_int(1, ONE_ORDER_PER_LINENUM));
        // val2.set_int(ONE_ORDER_PER_LINENUM);
        // // val2.set_int(NORMAL_INT_COND_RANGE);
        // val2.init_raw(sizeof(int));
        // Condition cond2 = {.lhs_col = std::move(col2), .op = OP_LT, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val2};
        // filter_conds.push_back(std::move(cond2));

        TabCol lhs_col = {.tab_name = "lineitem", .col_name = "l_shipdate"};
        Value val;
        // val.set_int(RandomGenerator::generate_random_int(1, SF * ONE_SF_PER_ORDER));
        // val.set_int(RandomGenerator::generate_random_int(1, MAX_COND_RANGE));
        // val.set_int(NORMAL_INT_COND_RANGE);
        val.set_str(DATE_COND_RANGE);
        val.init_raw(RandomGenerator::DATE_SIZE);
        Condition cond = {.lhs_col = std::move(lhs_col), .op = OP_EQ, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val};
        index_conds.push_back(std::move(cond));

        TabCol col2 = {.tab_name = "lineitem", .col_name = "l_id"};
        Value val2;
        // val2.set_int(RandomGenerator::generate_random_int(1, ONE_ORDER_PER_LINENUM));
        // val2.set_int(ONE_ORDER_PER_LINENUM);
        // val2.set_int(NORMAL_INT_COND_RANGE);
        val2.set_int(NORMAL_INT_COND_RANGE);
        val2.init_raw(sizeof(int));
        Condition cond2 = {.lhs_col = std::move(col2), .op = OP_LE, .is_rhs_val = true, .rhs_col = TabCol{}, .rhs_val = val2};
        index_conds.push_back(std::move(cond2));
    }

    void generate_table_data(int SF, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("lineitem");
        auto index_handle = sm_mgr->primary_index_.at("lineitem").get();
        Record record(tab_meta.record_length_);

        /*
            1 <= l_orderkey <= SF * ONE_SF_PER_ORDER
        */
        int l_orderkey_max = SF * ONE_SF_PER_ORDER;

        /*
            1 <= l_partkey <= SF * ONE_SF_PER_PART
        */
        int l_partkey_max = SF * ONE_SF_PER_PART;

        /*
            1 <= l_suppkey <= SF * ONE_SF_PER_SUPPLIER
        */
        int l_suppkey_max = SF * ONE_SF_PER_SUPPLIER;

        /*
            1 <= l_linenumber <= ONE_ORDER_PER_LINENUM
        */
        int l_linenumber_max = ONE_ORDER_PER_LINENUM; 

        int record_per_key = l_orderkey_max * l_linenumber_max / DATE_MAX_NUM;
        l_orderkey = 1;
        l_linenumber = 1;
        int l_id = 1;

        RandomMapping random_mapping(l_orderkey_max);

        for(int l_shipdate_idx = 0; l_shipdate_idx < DATE_MAX_NUM; ++l_shipdate_idx) {
            RandomGenerator::generate_date_from_idx(l_shipdate, l_shipdate_idx);

            for(int record_idx = 0; record_idx < record_per_key; ++record_idx) {
                // std::cout << "l_orderkey = " << l_orderkey << ", l_linenumber = " << l_linenumber << ", begin insert!"<< std::endl;
                if(l_orderkey > l_orderkey_max) {
                    break;
                }

                l_partkey = RandomGenerator::generate_random_int(1, l_partkey_max);
                l_suppkey = RandomGenerator::generate_random_int(1, l_suppkey_max);
                l_quantity = RandomGenerator::generate_random_float(1, 20000);
                l_extendedprice = RandomGenerator::generate_random_float(1, 20000);
                l_discount = RandomGenerator::generate_random_float(1, 20000);
                l_tax = RandomGenerator::generate_random_float(1, 20000);
                RandomGenerator::generate_random_str(l_returnflag, 1);
                RandomGenerator::generate_random_str(l_linestatus, 1);
                // RandomGenerator::generate_random_str(l_shipdate, Clock::DATETIME_SIZE + 1);
                // RandomGenerator::generate_random_str(l_commitdate, Clock::DATETIME_SIZE + 1);
                RandomGenerator::generate_random_date(l_commitdate);
                // RandomGenerator::generate_random_str(l_receiptdate, Clock::DATETIME_SIZE + 1);
                RandomGenerator::generate_random_date(l_receiptdate);
                RandomGenerator::generate_random_str(l_shipinstruct, 25);
                RandomGenerator::generate_random_str(l_shipmode, 10);
                RandomGenerator::generate_random_str(l_comment, 4);

                memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));

                int offset = 0;
                memcpy(record.raw_data_ + offset, l_shipdate, RandomGenerator::DATE_SIZE);
                offset += RandomGenerator::DATE_SIZE;
                memcpy(record.raw_data_ + offset, (char *)&l_id, sizeof(int));
                offset += sizeof(int);
                int actual_orderkey = random_mapping.f(l_orderkey);
                memcpy(record.raw_data_ + offset, (char *)&actual_orderkey, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, (char *)&l_linenumber, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, (char *)&l_partkey, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, (char *)&l_suppkey, sizeof(int));
                offset += sizeof(int);
                memcpy(record.raw_data_ + offset, (char *)&l_quantity, sizeof(float));
                offset += sizeof(float);
                memcpy(record.raw_data_ + offset, (char *)&l_extendedprice, sizeof(float));
                offset += sizeof(float);
                memcpy(record.raw_data_ + offset, (char *)&l_discount, sizeof(float));
                offset += sizeof(float);
                memcpy(record.raw_data_ + offset, (char *)&l_tax, sizeof(float));
                offset += sizeof(float);
                memcpy(record.raw_data_ + offset, l_returnflag, 1);
                offset += 1;
                memcpy(record.raw_data_ + offset, l_linestatus, 1);
                offset += 1;
                memcpy(record.raw_data_ + offset, l_commitdate, RandomGenerator::DATE_SIZE + 1);
                offset += RandomGenerator::DATE_SIZE + 1;
                memcpy(record.raw_data_ + offset, l_receiptdate, RandomGenerator::DATE_SIZE + 1);
                offset += RandomGenerator::DATE_SIZE + 1;
                memcpy(record.raw_data_ + offset, l_shipinstruct, 25);
                offset += 25;
                memcpy(record.raw_data_ + offset, l_shipmode, 10);
                offset += 10;
                memcpy(record.raw_data_ + offset, l_comment, 4);
                offset += 4;

                assert(offset == tab_meta.record_length_);

                // std::cout << "l_orderkey = " << l_orderkey << ", l_linenumber = " << l_linenumber << ", begin insert!"<< std::endl;
                /*
                    insert data
                */
                index_handle->insert_entry(record.raw_data_, record.record_, txn);
                l_id ++;

                l_linenumber ++;
                if(l_linenumber > l_linenumber_max) {
                    l_linenumber = 1;
                    l_orderkey ++;
                }
            }
        }

        // for(l_orderkey = 1; l_orderkey <= l_orderkey_max; ++l_orderkey) {
        //     for(int l_linenumber = 1; l_linenumber <= l_linenumber_max; ++l_linenumber) {
        //         /*
        //             col_defs.emplace_back(ColDef("l_orderkey", ColType::TYPE_INT, 4));
        //             col_defs.emplace_back(ColDef("l_partkey", ColType::TYPE_INT, 4));
        //             col_defs.emplace_back(ColDef("l_suppkey", ColType::TYPE_INT, 4));
        //             col_defs.emplace_back(ColDef("l_linenumber", ColType::TYPE_INT, 4));
        //             col_defs.emplace_back(ColDef("l_quantity", ColType::TYPE_FLOAT, 4));
        //             col_defs.emplace_back(ColDef("l_extendedprice", ColType::TYPE_FLOAT, 4));
        //             col_defs.emplace_back(ColDef("l_discount", ColType::TYPE_FLOAT, 4));
        //             col_defs.emplace_back(ColDef("l_tax", ColType::TYPE_FLOAT, 4));
        //             col_defs.emplace_back(ColDef("l_returnflag", ColType::TYPE_STRING, 1));
        //             col_defs.emplace_back(ColDef("l_linestatus", ColType::TYPE_STRING, 1));
        //             col_defs.emplace_back(ColDef("l_shipdate", ColType::TYPE_STRING, Clock::DATETIME_SIZE + 1));
        //             col_defs.emplace_back(ColDef("l_commitdate", ColType::TYPE_STRING, Clock::DATETIME_SIZE + 1));
        //             col_defs.emplace_back(ColDef("l_receiptdate", ColType::TYPE_STRING, Clock::DATETIME_SIZE + 1));
        //             col_defs.emplace_back(ColDef("l_shipinstruct", ColType::TYPE_STRING, 25));
        //             col_defs.emplace_back(ColDef("l_shipmode", ColType::TYPE_STRING, 10));
        //             col_defs.emplace_back(ColDef("l_comment", ColType::TYPE_STRING, 44));
        //         */
        //         // int rnd = RandomGenerator::generate_random_int(1, 2);
        //         // if(rnd == 1) {
        //         //     l_partkey = RandomGenerator::generate_random_int(1, l_partkey_max);
        //         //     l_suppkey = RandomGenerator::generate_random_int(1, l_suppkey_max);
        //         // }
        //         // else {
        //         //     l_partkey = l_orderkey;
        //         //     l_suppkey = l_orderkey;
        //         // }
                
        //         l_partkey = RandomGenerator::generate_random_int(1, l_partkey_max);
        //         l_suppkey = RandomGenerator::generate_random_int(1, l_suppkey_max);
        //         l_quantity = RandomGenerator::generate_random_float(1, 20000);
        //         l_extendedprice = RandomGenerator::generate_random_float(1, 20000);
        //         l_discount = RandomGenerator::generate_random_float(1, 20000);
        //         l_tax = RandomGenerator::generate_random_float(1, 20000);
                
        //         RandomGenerator::generate_random_str(l_returnflag, 1);
        //         RandomGenerator::generate_random_str(l_linestatus, 1);
        //         // RandomGenerator::generate_random_str(l_shipdate, Clock::DATETIME_SIZE + 1);
        //         RandomGenerator::generate_random_date(l_shipdate);
        //         // RandomGenerator::generate_random_str(l_commitdate, Clock::DATETIME_SIZE + 1);
        //         RandomGenerator::generate_random_date(l_commitdate);
        //         // RandomGenerator::generate_random_str(l_receiptdate, Clock::DATETIME_SIZE + 1);
        //         RandomGenerator::generate_random_date(l_receiptdate);
        //         RandomGenerator::generate_random_str(l_shipinstruct, 25);
        //         RandomGenerator::generate_random_str(l_shipmode, 10);
        //         RandomGenerator::generate_random_str(l_comment, 4);

        //         /*
        //             generate record content
        //         */
        //         memset(record.record_, 0, record.data_length_ + sizeof(RecordHdr));

        //         int offset = 0;
        //         memcpy(record.raw_data_ + offset, l_shipdate, RandomGenerator::DATE_SIZE + 1);
        //         offset += RandomGenerator::DATE_SIZE + 1;
        //         memcpy(record.raw_data_ + offset, (char *)&l_orderkey, sizeof(int));
        //         offset += sizeof(int);
        //         memcpy(record.raw_data_ + offset, (char *)&l_linenumber, sizeof(int));
        //         offset += sizeof(int);
        //         memcpy(record.raw_data_ + offset, (char *)&l_partkey, sizeof(int));
        //         offset += sizeof(int);
        //         memcpy(record.raw_data_ + offset, (char *)&l_suppkey, sizeof(int));
        //         offset += sizeof(int);
        //         memcpy(record.raw_data_ + offset, (char *)&l_quantity, sizeof(float));
        //         offset += sizeof(float);
        //         memcpy(record.raw_data_ + offset, (char *)&l_extendedprice, sizeof(float));
        //         offset += sizeof(float);
        //         memcpy(record.raw_data_ + offset, (char *)&l_discount, sizeof(float));
        //         offset += sizeof(float);
        //         memcpy(record.raw_data_ + offset, (char *)&l_tax, sizeof(float));
        //         offset += sizeof(float);
        //         memcpy(record.raw_data_ + offset, l_returnflag, 1);
        //         offset += 1;
        //         memcpy(record.raw_data_ + offset, l_linestatus, 1);
        //         offset += 1;
        //         memcpy(record.raw_data_ + offset, l_commitdate, RandomGenerator::DATE_SIZE + 1);
        //         offset += RandomGenerator::DATE_SIZE + 1;
        //         memcpy(record.raw_data_ + offset, l_receiptdate, RandomGenerator::DATE_SIZE + 1);
        //         offset += RandomGenerator::DATE_SIZE + 1;
        //         memcpy(record.raw_data_ + offset, l_shipinstruct, 25);
        //         offset += 25;
        //         memcpy(record.raw_data_ + offset, l_shipmode, 10);
        //         offset += 10;
        //         memcpy(record.raw_data_ + offset, l_comment, 4);
        //         offset += 4;

        //         assert(offset == tab_meta.record_length_);

        //         // std::cout << "l_orderkey = " << l_orderkey << ", l_linenumber = " << l_linenumber << ", begin insert!"<< std::endl;
        //         /*
        //             insert data
        //         */
        //         index_handle->insert_entry(record.raw_data_, record.record_, txn);
                
        //     }
        //     std::cout << "finish insert l_orderkey = " << l_orderkey << std::endl;
        // }
    }

    void generate_data_csv(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }

    void write_data_into_file(std::string file_name) {
        std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    }
};

}