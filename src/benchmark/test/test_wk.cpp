#include "test_wk.h"
#include "record/record.h"

// test_table(id int, name char(4), score float)
bool TestWK::create_table() {
    std::string db_name = "db_test";
    struct stat st;
    if(stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
       load_meta();
       return false;
    }
    else {
        std::string cmd = "mkdir " + db_name;
        system(cmd.c_str());
        chdir(db_name.c_str());
    }
    
    std::string table_name = "test_table";
    ColDef id;
    ColDef name;
    ColDef score;
    std::vector<ColDef> col_defs;
    id.name = "id";
    id.type = ColType::TYPE_INT;
    id.len = 4;
    name.name = "name";
    name.type = ColType::TYPE_STRING;
    name.len = 4;
    score.name = "score";
    score.type = ColType::TYPE_FLOAT;
    score.len = 4;
    col_defs.push_back(id);
    col_defs.push_back(name);
    col_defs.push_back(score);
    std::vector<std::string> pkeys;
    pkeys.push_back("id");
    pkeys.push_back("name");
    sm_mgr_->db_.name_ = "db_test";
    sm_mgr_->create_table(table_name, col_defs, pkeys, nullptr);
    index_handle_ = sm_mgr_->primary_index_.at(table_name).get();
    oldversion_handle_ = sm_mgr_->old_versions_.at(table_name).get();
    return true;
}

// this function is specifically used for compute pool to get table meta
void TestWK::load_meta() {
    std::string db_name = "db_test";
    struct stat st;
    if(stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
        chdir(db_name.c_str());
    }
    else {
        std::cout << "failed to find db_test database\n";
        assert(0);
    }

    system("pwd");

    std::ifstream ifs(DB_META_NAME);
    ifs >> sm_mgr_->db_;

    std::cout << sm_mgr_->db_.name_ << ", " << sm_mgr_->db_.next_table_id_ << ", " << sm_mgr_->db_.get_table("test_table").name_ << "\n";

    std::string table_name = "test_table";
    auto& tab = sm_mgr_->db_.get_table(table_name);
    auto& pindex = tab.indexes_[0];
    auto pindex_name = ix_mgr_->get_index_name(tab.name_, pindex.cols);
    std::cout << "pindex_name: " << pindex_name << "\n";
    sm_mgr_->primary_index_.emplace(tab.name_, ix_mgr_->open_index(tab.name_, pindex.cols, tab));
    sm_mgr_->old_versions_.emplace(tab.name_, mvcc_mgr_->open_file(tab.name_));

    chdir("..");
}

std::string TestWK::generate_name(int id) {
    char name[5];
    snprintf(name, sizeof(name), "%04d", id);
    return std::string(name);
}

void TestWK::load_data() {
    auto tab_meta = sm_mgr_->db_.get_table("test_table");
    std::cout << tab_meta.record_length_ << "\n";
    std::cout << "try to load " << record_num_ << "tuples into test_table\n";
    Transaction* txn = new Transaction(0);  // this transaction is only used to collecting the latch on pages in index operations
    for(int i = 0; i < record_num_; ++i) {
        Record record(tab_meta.record_length_);
        std::string name = generate_name(i);
        float score = 100.0;
        memcpy(record.raw_data_, &i, sizeof(int));
        memcpy(record.raw_data_ + 4, name.c_str(), 4);
        memcpy(record.raw_data_ + 8, &score, sizeof(float));
        
        index_handle_->insert_entry(record.raw_data_, record.record_, txn);
        std::cout << "inserted record " << i << "\n";
    }
    ix_mgr_->close_index(index_handle_);
    std::cout << "finish flush all pages in the pindex\n";
    sm_mgr_->primary_index_.clear();
    auto& pindex = tab_meta.indexes_[0];
    auto pindex_name = ix_mgr_->get_index_name("test_table", pindex.cols);
    sm_mgr_->primary_index_.emplace("test_table", ix_mgr_->open_index("test_table", pindex.cols, tab_meta));
    index_handle_ = sm_mgr_->primary_index_.at("test_table").get();
    std::cout << "test: read record num in test_table: " << index_handle_->get_next_record_no() << "\n";
}

void TestWK::init_transaction(int thread_num) {
    thread_num_ = thread_num;
    std::cout << "thread_num: " << thread_num_ << ", record_num: " << record_num_ << "\n";
    for(int i = 0; i < thread_num_; ++i) test_txns_.emplace_back(new TestTxn(record_num_));
    std::cout << "test_txns.size(): " << test_txns_.size() << "\n";
}

NativeTransaction* TestWK::generate_transaction(int thread_index) {
    // std::cout << "thread index: " << thread_index << "\n";
    assert(thread_index <= test_txns_.size());
    test_txns_[thread_index]->generate_new_txn();
    return test_txns_[thread_index];
}

NativeTransaction* TestWK::get_transaction(int thread_index) {
    assert(thread_index <= test_txns_.size());
    return test_txns_[thread_index];
}