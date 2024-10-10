#include <random>

#include "tpcc_wk.h"
#include "table.h"
#include "record/record.h"

bool TPCCWK::create_table() {
    std::string db_name = "db_tpcc";
    struct stat st;
    if(stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
        std::cout << "exist dir, just load meta\n";
    //    chdir(db_name.c_str());
       load_meta();
       return false;
    }
    else {
        std::string cmd = "mkdir " + db_name;
        system(cmd.c_str());
        chdir(db_name.c_str());
    }

    sm_mgr_->db_.name_  = db_name;

    Warehouse* warehouse = new Warehouse();
    warehouse->create_table(sm_mgr_);
    District* district = new District();
    district->create_table(sm_mgr_);
    Customer* customer = new Customer();
    customer->create_table(sm_mgr_);
    History* history = new History();
    history->create_table(sm_mgr_);
    NewOrders* new_orders = new NewOrders();
    new_orders->create_table(sm_mgr_);
    Orders* orders = new Orders();
    orders->create_table(sm_mgr_);
    OrderLine* order_line = new OrderLine();
    order_line->create_table(sm_mgr_);
    Item* item = new Item();
    item->create_table(sm_mgr_);
    Stock* stock = new Stock();
    stock->create_table(sm_mgr_);
    return true;
}

#define load_index(table_name) \
    std::string table_name##_str = #table_name; \
    auto& table_name##_tab = sm_mgr_->db_.get_table(table_name##_str); \
    auto& table_name##_pindex = table_name##_tab.indexes_[0]; \
    auto table_name##_pindex_name = ix_mgr_->get_index_name(table_name##_tab.name_, table_name##_pindex.cols); \
    sm_mgr_->primary_index_.emplace(table_name##_tab.name_, ix_mgr_->open_index(table_name##_tab.name_, table_name##_pindex.cols, table_name##_tab)); \
    sm_mgr_->old_versions_.emplace(table_name##_tab.name_, mvcc_mgr_->open_file(table_name##_tab.name_));

void TPCCWK::load_meta() {
    std::string db_name = "db_tpcc";
    struct stat st;
    if(stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
        chdir(db_name.c_str());
    }
    else {
        std::cout << "failed to find db_test database\n";
        assert(0);
    }

    std::ifstream ifs(DB_META_NAME);
    ifs >> sm_mgr_->db_;
    load_index(warehouse);
    load_index(district);
    load_index(customer);
    load_index(history);
    load_index(new_orders);
    load_index(orders);
    load_index(order_line);
    load_index(item);
    load_index(stock);

    // chdir("..");
}

#define load_table_data(table_name) \
    table_name* table_name##_tab = new table_name(); \
    table_name##_tab->generate_table_data(warehouse_num_, txn, sm_mgr_, ix_mgr_);

#define flush_index(table_name) \
    auto table_name##_tab_meta = sm_mgr_->db_.get_table(#table_name); \
    auto table_name##_index_handle = sm_mgr_->primary_index_.at(#table_name).get(); \
    ix_mgr_->close_index(table_name##_index_handle); \

#define reload_index(table_name) \
    auto& table_name##_pindex = table_name##_tab_meta.indexes_[0]; \
    auto table_name##_pindex_name = ix_mgr_->get_index_name(#table_name, table_name##_pindex.cols); \
    sm_mgr_->primary_index_.emplace(#table_name, ix_mgr_->open_index(#table_name, table_name##_pindex.cols, table_name##_tab_meta));

void TPCCWK::load_data() {
    Transaction* txn = new Transaction(0);

    load_table_data(Warehouse);
    std::cout << "finish load warehouse\n";
    load_table_data(District);
    std::cout << "finish load district\n";
    load_table_data(Customer);
    std::cout << "finish load customer\n";
    load_table_data(History);
    std::cout << "finish load history\n";
    load_table_data(NewOrders);
    std::cout << "finish load new_orders\n";
    load_table_data(Orders);
    std::cout << "finish load orders\n";
    load_table_data(OrderLine);
    std::cout << "finish load orderline\n";
    load_table_data(Item);
    std::cout << "finish load item\n";
    load_table_data(Stock);
    std::cout << "finish load stock\n";

    flush_index(warehouse);
    flush_index(district);
    flush_index(customer);
    flush_index(history);
    flush_index(new_orders);
    flush_index(orders);
    flush_index(order_line);
    flush_index(item);
    flush_index(stock);

    sm_mgr_->primary_index_.clear();

    reload_index(warehouse);
    reload_index(district);
    reload_index(customer);
    reload_index(history);
    reload_index(new_orders);
    reload_index(orders);
    reload_index(order_line);
    reload_index(item);
    reload_index(stock);
}

void TPCCWK::init_transaction(int thread_num) {
    thread_num_ = thread_num;
    for(int i = 0; i < thread_num_; ++i) {
        new_order_txns_.emplace_back(new NewOrderTransaction());
        new_order_txns_[i]->w_id = i;
        payment_txns_.emplace_back(new PaymentTransaction());
        payment_txns_[i]->w_id = i;
    }
}

NativeTransaction* TPCCWK::generate_transaction(int thread_index) {
    assert(thread_index <= thread_num_);
    if(thread_index % 2) {
        // std::cout << "client " << thread_index << " try to generate new_order transactions\n";
        new_order_txns_[thread_index]->generate_new_txn();
        return new_order_txns_[thread_index];
    }
    else {
        // std::cout << "client " << thread_index << " try to generate payment transactions\n";
        payment_txns_[thread_index]->generate_new_txn();
        return payment_txns_[thread_index];
        // new_order_txns_[thread_index]->generate_new_txn();
        // return new_order_txns_[thread_index];
        // std::cout << "client " << thread_index << " try to generate new_order transactions\n";
        // new_order_txns_[thread_index]->generate_new_txn();
        // return new_order_txns_[thread_index];
    }
}

NativeTransaction* TPCCWK::get_transaction(int thread_index) {
    assert(thread_index <= thread_num_);
    if(thread_index % 2) {
        return new_order_txns_[thread_index];
    }
    else {
        return payment_txns_[thread_index];
        // return new_order_txns_[thread_index];
    }
}