#pragma once

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

namespace HyBenchTable {
class Customer {
    Customer() {}

    void create_table(SmManager* sm_mgr) {
        std::vector<ColDef> col_defs = {
            {"CUSTKEY", ColType::TYPE_INT, 4},
            {"COMPANYKEY", ColType::TYPE_INT, 4},
            {"NAME", ColType::TYPE_STRING, 21},
            {"GENDER", ColType::TYPE_STRING, 2},
            {"PROVINCE", ColType::TYPE_STRING, 21},
            {"CITY", ColType::TYPE_STRING, 21},
            {"LOANBALANCE", ColType::TYPE_FLOAT, 4},
            {"ISBLOCKED", ColType::TYPE_INT, 4},
            {"FRESH_TS", ColType::TYPE_INT, 4}
        };
        sm_mgr->create_table("CUSTOMER", col_defs, {"CUSTKEY"}, nullptr);
    }

    void generate_table_data(int SF, Transaction* txn, SmManager* sm_mgr, IxManager* ix_mgr) {
        auto tab_meta = sm_mgr->db_.get_table("CUSTOMER");
        auto index_handle = sm_mgr->primary_index_.at("CUSTOMER").get();
        Record record(tab_meta.record_length_);

        // SF * 30000
        int c_custkey_max = SF * 30000;
        int company_key_max = SF * 2000;

        for(int c_custkey = 1; c_custkey <= c_custkey_max; ++c_custkey) {
            int c_companykey = RandomGenerator::generate_random_int(1, company_key_max);
            char c_name[21];
            char c_gender[2];
            char c_province[21];
            char c_city[21];
            char c_loanbalance[4];
            int c_isblocked = RandomGenerator::generate_random_int(0, 1);
            int c_fresh_ts;
        }
    }
        
};

}

class HyBenchTable {
public:
    void create_tables(SmManager* sm_mgr) {
        create_customer_table(sm_mgr);
        create_company_table(sm_mgr);
        create_saving_account_table(sm_mgr);
        create_checking_account_table(sm_mgr);
        create_transfer_table(sm_mgr);
        create_checking_table(sm_mgr);
        create_loanapp_table(sm_mgr);
        create_loan_table(sm_mgr);
    }

    void create_customer_table(SmManager* sm_mgr) {
        std::vector<ColDef> col_defs = {
            {"CUSTKEY", ColType::TYPE_INT, 4},
            {"NAME", ColType::TYPE_STRING, 21},
            {"PROVINCE", ColType::TYPE_STRING, 21},
            {"CITY", ColType::TYPE_STRING, 21},
            {"LOANBALANCE", ColType::TYPE_FLOAT, 4},
            {"GENDER", ColType::TYPE_STRING, 2},
            {"ISBLOCKED", ColType::TYPE_INT, 4},
            {"FRESH_TS", ColType::TYPE_INT, 4}
        };
        sm_mgr->create_table("CUSTOMER", col_defs, {"CUSTKEY"}, nullptr);
    }

    void create_company_table(SmManager* sm_mgr) {
        std::vector<ColDef> col_defs = {
            {"COMPANYKEY", ColType::TYPE_INT, 4},
            {"NAME", ColType::TYPE_STRING, 21},
            {"CATEGORY", ColType::TYPE_STRING, 21},
            {"PROVINCE", ColType::TYPE_STRING, 21},
            {"CITY", ColType::TYPE_STRING, 21},
            {"LOANBALANCE", ColType::TYPE_FLOAT, 4},
            {"ISBLOCKED", ColType::TYPE_INT, 4},
            {"FRESH_TS", ColType::TYPE_INT, 4}
        };
        sm_mgr->create_table("COMPANY", col_defs, {"COMPANYKEY"}, nullptr);
    }

    void create_saving_account_table(SmManager* sm_mgr) {
        std::vector<ColDef> col_defs = {
            {"ACCOUNTKEY", ColType::TYPE_INT, 4},
            {"BALANCE", ColType::TYPE_FLOAT, 4},
            {"TIMESTAMP", ColType::TYPE_INT, 4},
            {"ISBLOCKED", ColType::TYPE_INT, 4},
            {"FRESH_TS", ColType::TYPE_INT, 4}
        };
        sm_mgr->create_table("SAVINGACCOUNT", col_defs, {"ACCOUNTKEY"}, nullptr);
    }

    void create_checking_account_table(SmManager* sm_mgr) {
        std::vector<ColDef> col_defs = {
            {"ACCOUNTKEY", ColType::TYPE_INT, 4},
            {"BALANCE", ColType::TYPE_FLOAT, 4},
            {"TIMESTAMP", ColType::TYPE_INT, 4},
            {"ISBLOCKED", ColType::TYPE_INT, 4},
            {"FRESH_TS", ColType::TYPE_INT, 4}
        };
        sm_mgr->create_table("CHECKINGACCOUNT", col_defs, {"ACCOUNTKEY"}, nullptr);
    }

    void create_transfer_table(SmManager* sm_mgr) {
        std::vector<ColDef> col_defs = {
            {"TRANSKEY", ColType::TYPE_INT, 4},
            {"SOURCEID", ColType::TYPE_INT, 4},
            {"TARGETID", ColType::TYPE_INT, 4},
            {"AMOUNT", ColType::TYPE_FLOAT, 4},
            {"TYPE", ColType::TYPE_STRING, 11},
            {"TIMESTAMP", ColType::TYPE_INT, 4},
            {"FRESH_TS", ColType::TYPE_INT, 4}
        };
        sm_mgr->create_table("TRANSFER", col_defs, {"TRANSKEY"}, nullptr);
    }

    void create_checking_table(SmManager* sm_mgr) {
        std::vector<ColDef> col_defs = {
            {"CHECKKEY", ColType::TYPE_INT, 4},
            {"SOURCEID", ColType::TYPE_INT, 4},
            {"TARGETID", ColType::TYPE_INT, 4},
            {"AMOUNT", ColType::TYPE_FLOAT, 4},
            {"TYPE", ColType::TYPE_STRING, 11},
            {"TIMESTAMP", ColType::TYPE_INT, 4},
            {"FRESH_TS", ColType::TYPE_INT, 4}
        };
        sm_mgr->create_table("CHECKING", col_defs, {"CHECKKEY"}, nullptr);
    }

    void create_loanapp_table(SmManager* sm_mgr) {
        std::vector<ColDef> col_defs = {
            {"LOANAPPKEY", ColType::TYPE_INT, 4},
            {"APPLICANTID", ColType::TYPE_INT, 4},
            {"AMOUNT", ColType::TYPE_FLOAT, 4},
            {"DURATION", ColType::TYPE_INT, 4},
            {"STATUS", ColType::TYPE_STRING, 11},
            {"FRESH_TS", ColType::TYPE_INT, 4}
        };
        sm_mgr->create_table("LOANAPP", col_defs, {"LOANAPPKEY"}, nullptr);
    }

    void create_loan_table(SmManager* sm_mgr) {
        std::vector<ColDef> col_defs = {
            {"LOANKEY", ColType::TYPE_INT, 4},
            {"APPLICANTID", ColType::TYPE_INT, 4},
            {"AMOUNT", ColType::TYPE_FLOAT, 4},
            {"DURATION", ColType::TYPE_INT, 4},
            {"STATUS", ColType::TYPE_STRING, 11},
            {"CONTRACTDATE", ColType::TYPE_INT, 4},
            {"DELINQUENCY", ColType::TYPE_INT, 4},
            {"FRESH_TS", ColType::TYPE_INT, 4}
        };
        sm_mgr->create_table("LOAN", col_defs, {"LOANKEY"}, nullptr);
    }

    void generate_data_csv(std::string file_name) {
        std::ofstream outfile(file_name, std::ios::out | std::ios::trunc);
        outfile << "CUSTKEY,NAME,PROVINCE,CITY,LOANBALANCE,GENDER,ISBLOCKED,FRESH_TS" << std::endl;
        outfile.close();
        write_data_into_file(file_name);
    }

    void write_data_into_file(std::string file_name) {
        std::ofstream outfile(file_name, std::ios::out | std::ios::app);
        for(int custkey = 1; custkey <= 30000; ++custkey) {
            char name[21], province[21], city[21], gender[2];
            RandomGenerator::generate_random_str(name, 20);
            RandomGenerator::generate_random_str(province, 20);
            RandomGenerator::generate_random_str(city, 20);
            gender[0] = (RandomGenerator::generate_random_int(0, 1) == 0) ? 'M' : 'F';
            gender[1] = '\0';
            float loan_balance = RandomGenerator::generate_random_float(1000, 50000);
            int is_blocked = RandomGenerator::generate_random_int(0, 1);
            int fresh_ts = RandomGenerator::generate_random_int(100000, 200000);

            outfile << custkey << "," << name << "," << province << "," << city << "," 
                    << loan_balance << "," << gender << "," << is_blocked << "," << fresh_ts << std::endl;
        }
        outfile.close();
    }
};
