#include "common/config.h"

#include "hat_wk.h"
#include "HATtrick_table.h"

bool HATtrickWK::create_table() {
    /*
        create database hattrickbench
    */
    std::string db_name = "db_TAW";
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
    std::unique_ptr<HATtrick::Customer> customer = std::make_unique<HATtrick::Customer>();
    customer->create_table(sm_mgr_);
    std::unique_ptr<HATtrick::Supplier> supplier = std::make_unique<HATtrick::Supplier>();
    supplier->create_table(sm_mgr_);
    std::unique_ptr<HATtrick::Part> part = std::make_unique<HATtrick::Part>();
    part->create_table(sm_mgr_);
    std::unique_ptr<HATtrick::LineOrder> lineorder = std::make_unique<HATtrick::LineOrder>();
    lineorder->create_table(sm_mgr_);
    std::unique_ptr<HATtrick::Date> date = std::make_unique<HATtrick::Date>();
    date->create_table(sm_mgr_);
    std::unique_ptr<HATtrick::History> history = std::make_unique<HATtrick::History>();
    history->create_table(sm_mgr_);
    std::cout << "finish create table\n";

    return true;
}

#define load_index(table_name) \
    std::string table_name##_str = #table_name; \
    auto& table_name##_tab = sm_mgr_->db_.get_table(table_name##_str); \
    auto& table_name##_pindex = table_name##_tab.indexes_[0]; \
    auto table_name##_pindex_name = ix_mgr_->get_index_name(table_name##_tab.name_, table_name##_pindex.cols); \
    sm_mgr_->primary_index_.emplace(table_name##_tab.name_, ix_mgr_->open_index(table_name##_tab.name_, table_name##_pindex.cols, table_name##_tab)); \
    sm_mgr_->old_versions_.emplace(table_name##_tab.name_, mvcc_mgr_->open_file(table_name##_tab.name_));

#define hattrick_load_table_data(table_name) \
    { \
        HATtrick::table_name* table_name##_table = new HATtrick::table_name(); \
        table_name##_table->generate_table_data(sf_, txn, sm_mgr_, ix_mgr_); \
        delete table_name##_table; \
    }

#define hattrick_flush_index(table_name) \
    { \
        auto& table_name##_tab_meta = sm_mgr_->db_.get_table(#table_name); \
        auto table_name##_index_handle = sm_mgr_->primary_index_.at(#table_name).get(); \
        ix_mgr_->close_index(table_name##_index_handle); \
    }

#define hattrick_reload_index(table_name) \
    { \
        auto& table_name##_tab = sm_mgr_->db_.get_table(#table_name); \
        auto& table_name##_pindex = table_name##_tab.indexes_[0]; \
        auto table_name##_pindex_name = ix_mgr_->get_index_name(table_name##_tab.name_, table_name##_pindex.cols); \
        sm_mgr_->primary_index_.emplace(table_name##_tab.name_, ix_mgr_->open_index(table_name##_tab.name_, table_name##_pindex.cols, table_name##_tab)); \
        std::cout << "reload index " << table_name##_pindex_name << " done!\n"; \
    }

void HATtrickWK::load_meta() {
    std::string db_name = "db_TAW";
    // struct stat st;
    
    // if(stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
    //     chdir(db_name.c_str());
    // }
    std::ifstream ifs(DB_META_NAME);
    ifs >> sm_mgr_->db_;
    std::cout << "load customer index\n";
    load_index(customer);
    load_index(supplier);
    load_index(part);
    load_index(lineorder);
    load_index(date);
    load_index(history);

    // TODO？？ 是否需要chdir ..
    // chdir("..");
}

void HATtrickWK::load_data() {
    Transaction* txn = new Transaction(0);

    std::cout << "begin load data\n";

    hattrick_load_table_data(Customer);
    std::cout << "finish load customer\n";

    hattrick_load_table_data(Supplier);
    std::cout << "finish load supplier\n";

    hattrick_load_table_data(Part);
    std::cout << "finish load part\n";

    hattrick_load_table_data(LineOrder);
    std::cout << "finish load lineorder\n";

    hattrick_load_table_data(Date);
    std::cout << "finish load date\n";

    hattrick_load_table_data(History);
    std::cout << "finish load history\n";
    /*
        flush index
    */
    std::cout << "flush index:\n";
    hattrick_flush_index(customer);
    hattrick_flush_index(supplier);
    hattrick_flush_index(part);
    hattrick_flush_index(lineorder);
    hattrick_flush_index(date);
    hattrick_flush_index(history);
    sm_mgr_->primary_index_.clear();
    std::cout << "hattrick flush index done!\n";
    hattrick_reload_index(customer);
    hattrick_reload_index(supplier);
    hattrick_reload_index(part);
    hattrick_reload_index(lineorder);
    hattrick_reload_index(date);
    hattrick_reload_index(history);
    std::cout << "hattrick reload index done!\n";
    delete txn;
}

void HATtrickWK::init_transaction(int thread_num) {
    thread_num_ = thread_num;
    for(int i = 0; i < thread_num; ++i) {
        new_order_txns_.emplace_back(new HATNewOrderTransaction());
        payment_txns_.emplace_back(new HATPaymentTransaction());
    }
}

NativeTransaction* HATtrickWK::generate_transaction(int thread_index) {
    assert(thread_index <= thread_num_);
    if(thread_index % 2 == 0) {
        new_order_txns_[thread_index]->generate_new_txn();
        return new_order_txns_[thread_index];
    }
    else {
        payment_txns_[thread_index]->generate_new_txn();
        return payment_txns_[thread_index];
    }
}

NativeTransaction* HATtrickWK::get_transaction(int thread_index) {
    assert(thread_index <= thread_num_);
    if(thread_index % 2 == 0) {
        return new_order_txns_[thread_index];
    }
    else {
        return payment_txns_[thread_index];
    }
}