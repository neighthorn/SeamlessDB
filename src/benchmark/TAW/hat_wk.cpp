#include "hat_wk.h"
#include "HATtrick_table.h"

bool HATtrickWK::create_table() {
    /*
        create database hattrickbench
    */
    std::string db_name = "db_hattrickbench";
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
        auto table_name##_table = std::make_unique<HATtrick::table_name>(); \
        table_name##_table->generate_table_data(sf_, txn, sm_mgr_, ix_mgr_); \
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
    std::string db_name = "db_hattrickbench";
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
    hattrick_flush_index(Customer);
    hattrick_flush_index(Supplier);
    hattrick_flush_index(Part);
    hattrick_flush_index(LineOrder);
    hattrick_flush_index(Date);
    hattrick_flush_index(History);
    sm_mgr_->primary_index_.clear();
    std::cout << "hattrick flush index done!\n";
    hattrick_reload_index(Customer);
    hattrick_reload_index(Supplier);
    hattrick_reload_index(Part);
    hattrick_reload_index(LineOrder);
    hattrick_reload_index(Date);
    hattrick_reload_index(History);
    std::cout << "hattrick reload index done!\n";
    delete txn;
}