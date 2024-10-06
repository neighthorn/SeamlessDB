#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;


    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    // cd to database dir
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }
    // Load meta
    // 打开一个名为DB_META_NAME的文件
    std::ifstream ifs(DB_META_NAME);
    // 将ofs打开的DB_META_NAME文件中的信息，按照定义好的operator>>操作符，读出到db_中
    ifs >> db_;  // 注意：此处重载了操作符>>
    // Open all record files & index files
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        auto& pindex = tab.indexes_[0];
        auto pindex_name = ix_manager_->get_index_name(tab.name_, pindex.cols);
        primary_index_.emplace(tab.name_, ix_manager_->open_index(tab.name_, pindex.cols, tab));

        for(size_t i = 1; i < tab.indexes_.size(); ++i) {
            auto &index = tab.indexes_[i];
            auto index_name = ix_manager_->get_index_name(tab.name_, index.cols);
            assert(ihs_.count(index_name) == 0);
            ihs_.emplace(index_name, ix_manager_->open_index(tab.name_, index.cols, tab));
        }
    }

    int fd = disk_manager_->open_file(LOG_FILE_NAME);
    disk_manager_->SetLogFd(fd);
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    // Dump meta
    flush_meta();
    db_.name_.clear();
    db_.tabs_.clear();

    for (auto &entry : ihs_) {
        ix_manager_->close_index(entry.second.get());
    }
    for(auto& entry: primary_index_) {
        ix_manager_->close_index(entry.second.get());
    }
    ihs_.clear();
    primary_index_.clear();
    disk_manager_->close_file(disk_manager_->GetLogFd());
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name_}, context);
        outfile << "| " << tab.name_ << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

int SmManager::get_table_id(const std::string& tab_name) {
    for(auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        if(strcmp(tab.name_.c_str(), tab_name.c_str()) == 0)
            return tab.table_id_;
    }
    throw TableNotFoundError(tab_name);
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols_) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type)};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, const std::vector<std::string>& pkeys, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name_ = tab_name;
    tab.table_id_ = db_.next_table_id_ ++;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset};
        curr_offset += col_def.len;
        tab.cols_.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    // rm_manager_->create_file(tab_name, record_size);
    tab.record_length_ = record_size;

    std::vector<ColMeta> pindex_cols;
    int col_tot_len = 0;
    for(size_t i = 0; i < pkeys.size(); ++i) {
        pindex_cols.push_back(*(tab.get_col(pkeys[i])));
        col_tot_len += pindex_cols[i].len;
    }

    IndexMeta index_meta;
    index_meta.col_num = pkeys.size();
    index_meta.col_tot_len = col_tot_len;
    index_meta.cols = std::move(pindex_cols);
    index_meta.tab_name = tab_name;
    tab.indexes_.push_back(index_meta);

    ix_manager_->create_index(tab_name, index_meta.cols, tab);
    primary_index_.emplace(tab_name, ix_manager_->open_index(tab_name, index_meta.cols, tab));

    // record size = sizeof(recordhdr) + record raw length
    multi_version_manager_->create_file(tab_name, tab.record_length_ + sizeof(RecordHdr));
    // auto multi_version_file_handle = 
    old_versions_.emplace(tab_name, multi_version_manager_->open_file(tab_name));
    // create table.old 文件保存old_version
    

    std::cout << " finish create table: " << tab_name << std::endl;

    db_.tabs_.emplace(tab_name, tab);
    
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    // fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    // if(context != nullptr) {
    //     context->lock_mgr_->lock_exclusive_on_table(context->txn_, fhs_[tab_name]->GetFd());
    // }

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    if(!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    // if(context != nullptr) {
    //     context->lock_mgr_->lock_exclusive_on_table(context->txn_, fhs_[tab_name]->GetFd());
    // }
    // Find table index in db_ meta
    TabMeta &tab = db_.get_table(tab_name);

    // drop main table
    ix_manager_->close_index(primary_index_.at(tab_name).get());
    ix_manager_->destroy_index(tab_name, tab.indexes_[0].cols);
    primary_index_.erase(tab_name);

    for(size_t i = 1; i < tab.indexes_.size(); ++i) {
        auto &index = tab.indexes_[i];
        SmManager::drop_index(tab_name, index.cols, context);
    }
    db_.tabs_.erase(tab_name);
    // fhs_.erase(tab_name);

    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // context->lock_mgr_->lock_shared_on_table(context->txn_, fhs_[tab_name]->GetFd());

    // TabMeta &tab = db_.get_table(tab_name);
    // std::vector<ColMeta> index_cols;
    // int col_tot_len = 0;
    // for(size_t i = 0; i < col_names.size(); ++i) {
    //     index_cols.push_back(*(tab.get_col(col_names[i])));
    //     col_tot_len += index_cols[i].len;
    // }

    // if (tab.is_index(col_names)) {
    //     throw IndexExistsError(tab_name, col_names);
    // }

    // IndexMeta index_meta;
    // index_meta.col_num = col_names.size();
    // index_meta.col_tot_len = col_tot_len;
    // assert(index_meta.col_num == index_cols.size());
    // index_meta.cols = std::move(index_cols);
    // index_meta.tab_name = tab_name;
    // tab.indexes_.push_back(index_meta);

    // // Create index file
    // // int col_idx = col - tab.cols.begin();
    
    // ix_manager_->create_index(tab_name, index_meta.cols, &tab);  // 这里调用了
    // // Open index file
    // auto ih = ix_manager_->open_index(tab_name, index_meta.cols);
    // // Get record file handle
    // auto file_handle = fhs_.at(tab_name).get();
    // // Index all records into index
    // for (RmScan rm_scan(file_handle); !rm_scan.is_end(); rm_scan.next()) {
    //     auto rec = file_handle->get_record(rm_scan.rid(), context);  // rid是record的存储位置，作为value插入到索引里
    //     // const char *key = rec->data + col->offset;
    //     char* key = new char[col_tot_len];
    //     int offset = 0;
    //     for(size_t i = 0; i < index_meta.cols.size(); ++i) {
    //         memcpy(key + offset, rec->data + index_meta.cols[i].offset, index_meta.cols[i].len);
    //         offset += index_meta.cols[i].len;
    //     }
    //     // record data里以各个属性的offset进行分隔，属性的长度为col len，record里面每个属性的数据作为key插入索引里
    //     ih->insert_entry(key, rm_scan.rid(), context->txn_);  // ljw: 此处调用了！
    // }
    // // Store index handle
    // auto index_name = ix_manager_->get_index_name(tab_name, index_meta.cols);
    // assert(ihs_.count(index_name) == 0);
    // // ihs_[index_name] = std::move(ih);
    // ihs_.emplace(index_name, std::move(ih));
    // // Mark column index as created
    // // col->index = true;

    // flush_meta();

    std::cout << "finish create index\n";
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // context->lock_mgr_->lock_shared_on_table(context->txn_, fhs_[tab_name]->GetFd());
    TabMeta &tab = db_.tabs_[tab_name];
    
    if (tab.is_index(col_names) == false) {
        throw IndexNotFoundError(tab_name, col_names);
    }
    
    auto index_name = ix_manager_->get_index_name(tab_name, col_names);
    ix_manager_->close_index(ihs_.at(index_name).get());
    ix_manager_->destroy_index(tab_name, col_names);
    ihs_.erase(index_name);
    tab.indexes_.erase(tab.get_index_meta(col_names));

    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    std::vector<std::string> col_names;
    for(auto& col: cols) {
        col_names.push_back(col.name);
    }
    
    SmManager::drop_index(tab_name, col_names, context);
}