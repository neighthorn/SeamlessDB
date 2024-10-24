#include "tpch_wk.h"
#include "tpch_table.h"
#include "tpch_queries.h"

// return true if need to generate data, false if data already exists
bool TPCHWK::create_table() {
    /*
        create database tpch
    */
    std::string db_name = "db_tpch";
    struct stat st;
    if(stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
        std::cout << "directory already exists, just load meta\n";
       chdir(db_name.c_str());
       load_meta();
       return false;
    }
    else {
        std::cout << "make new directory\n";

        std::string cmd = "mkdir " + db_name;
        system(cmd.c_str());
        chdir(db_name.c_str());
    }

    sm_mgr_->db_.name_  = db_name;


    /*
        create table
    */
    std::unique_ptr<TPCH_TABLE::Region> region = std::make_unique<TPCH_TABLE::Region>();
    region->create_table(sm_mgr_);

    std::unique_ptr<TPCH_TABLE::Nation> nation = std::make_unique<TPCH_TABLE::Nation>();
    nation->create_table(sm_mgr_);

    // std::unique_ptr<TPCH_TABLE::Nation2> nation2 = std::make_unique<TPCH_TABLE::Nation2>();
    // nation2->create_table(sm_mgr_);

    std::unique_ptr<TPCH_TABLE::Part> part = std::make_unique<TPCH_TABLE::Part>();
    part->create_table(sm_mgr_);

    std::unique_ptr<TPCH_TABLE::Customer> customer = std::make_unique<TPCH_TABLE::Customer>();
    customer->create_table(sm_mgr_);

    std::unique_ptr<TPCH_TABLE::Orders> orders = std::make_unique<TPCH_TABLE::Orders>();
    orders->create_table(sm_mgr_);

    std::unique_ptr<TPCH_TABLE::Supplier> supplier = std::make_unique<TPCH_TABLE::Supplier>();
    supplier->create_table(sm_mgr_);

    std::unique_ptr<TPCH_TABLE::PartSupp> part_supp = std::make_unique<TPCH_TABLE::PartSupp>();
    part_supp->create_table(sm_mgr_);

    std::unique_ptr<TPCH_TABLE::Lineitem> lineitem = std::make_unique<TPCH_TABLE::Lineitem>();
    lineitem->create_table(sm_mgr_);
    
    std::cout << "finish create table\n";
    return true;
}

/*
    load table data
*/
#define tpch_load_table_data(table_name) \
    TPCH_TABLE::table_name* table_name##_tab = new TPCH_TABLE::table_name(); \
    table_name##_tab->generate_table_data(sf_, txn, sm_mgr_, ix_mgr_); \
    delete table_name##_tab;

#define tpch_flush_index(table_name) \
    auto table_name##_tab_meta = sm_mgr_->db_.get_table(#table_name); \
    auto table_name##_index_handle = sm_mgr_->primary_index_.at(#table_name).get(); \
    ix_mgr_->close_index(table_name##_index_handle); \

#define tpch_reload_index(table_name) \
    auto& table_name##_pindex = table_name##_tab_meta.indexes_[0]; \
    auto table_name##_pindex_name = ix_mgr_->get_index_name(#table_name, table_name##_pindex.cols); \
    sm_mgr_->primary_index_.emplace(#table_name, ix_mgr_->open_index(#table_name, table_name##_pindex.cols, table_name##_tab_meta));

void TPCHWK::load_data() {
    Transaction* txn = new Transaction(0);
    /*
        load table data
    */

    tpch_load_table_data(Customer);
    std::cout << "finish load customer\n";

    tpch_load_table_data(Lineitem)
    std::cout << "finish load lineitem\n";

    tpch_load_table_data(Region);
    std::cout << "finish load region\n";

    tpch_load_table_data(Nation);
    std::cout << "finish load nation\n";

    // tpch_load_table_data(Nation2);
    // std::cout << "finish load nation2";

    tpch_load_table_data(Part);
    std::cout << "finish load part\n";

    tpch_load_table_data(Orders);
    std::cout << "finish load orders\n";

    tpch_load_table_data(Supplier);
    std::cout << "finish load supplier\n";

    tpch_load_table_data(PartSupp);
    std::cout << "finish load partsupp\n";

    

    /*
        flush index
    */

    std::cout << "flush index:\n";
    tpch_flush_index(region);
    tpch_flush_index(nation);
    // tpch_flush_index(nation2);
    tpch_flush_index(part);
    tpch_flush_index(customer);
    tpch_flush_index(orders);
    tpch_flush_index(supplier);
    tpch_flush_index(partsupp);
    tpch_flush_index(lineitem);

    sm_mgr_->primary_index_.clear();
    std::cout << "tpch flush index done!\n";

    tpch_reload_index(region);
    tpch_reload_index(nation);
    // tpch_reload_index(nation2);
    tpch_reload_index(part);
    tpch_reload_index(customer);
    std::cout << "tpch begin reload orders\n";
    tpch_reload_index(orders);
    tpch_reload_index(supplier);
    tpch_reload_index(partsupp);
    tpch_reload_index(lineitem);
    std::cout << "tpch reload index done!\n";

    delete txn;
}

#define load_index(table_name) \
    std::string table_name##_str = #table_name; \
    auto& table_name##_tab = sm_mgr_->db_.get_table(table_name##_str); \
    auto& table_name##_pindex = table_name##_tab.indexes_[0]; \
    auto table_name##_pindex_name = ix_mgr_->get_index_name(table_name##_tab.name_, table_name##_pindex.cols); \
    sm_mgr_->primary_index_.emplace(table_name##_tab.name_, ix_mgr_->open_index(table_name##_tab.name_, table_name##_pindex.cols, table_name##_tab)); \
    sm_mgr_->old_versions_.emplace(table_name##_tab.name_, mvcc_mgr_->open_file(table_name##_tab.name_));

void TPCHWK::load_meta() {
    std::string db_name = "db_tpch";
    // struct stat st;
    
    // if(stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
    //     chdir(db_name.c_str());
    // }
    // else {
    //     std::cout << "failed to find db_tpch database\n";
    //     assert(0);
    // }

    std::ifstream ifs(DB_META_NAME);
    ifs >> sm_mgr_->db_;
    std::cout << "load region index\n";
    load_index(region);
    load_index(nation);
    // load_index(nation2);
    load_index(part);
    load_index(customer);
    load_index(orders);
    load_index(supplier);
    load_index(partsupp);
    load_index(lineitem);

    // TODO？？ 是否需要chdir ..
    // chdir("..");
}

void TPCHWK::init_transaction(int thread_num) {
    this->thread_num_ = thread_num;
    
    for(int i = 0; i < thread_num; i++) {
        queries5.push_back(new Query5());
        queries10.push_back(new Query10());
        queries_example.push_back(new QueryExample());
    }
}

NativeTransaction* TPCHWK::generate_transaction(int thread_index) {
    if(thread_index >= thread_num_) {
        return nullptr;
    }
    queries_example[thread_index]->generate_new_txn();
    return queries_example[thread_index];
    // switch (rand() % 1)
    // {
    //     case 0: {
    //         queries5[thread_index]->generate_new_txn();
    //         return queries5[thread_index];
    //     }   break;
    //     case 1: {
    //         queries10[thread_index]->generate_new_txn();
    //         return queries10[thread_index];
    //     }   break;
    //     default: {
    //         std::cerr << "[Error]: Not Implemented! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
    //     }   break;
    // }
    return nullptr;
}

NativeTransaction* TPCHWK::get_transaction(int thread_index) {
    return queries_example[thread_index];
}
