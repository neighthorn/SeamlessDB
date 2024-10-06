#include "log_store.h"
#include "debug_log.h"

LogStore::LogStore(const std::string &log_store_path) : log_store_path_(log_store_path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, log_store_path, &db_);
    if (!status.ok()) {
        std::cerr << "Failed to create log store. Error: " << status.ToString() << std::endl;
        // return -1;
    }
    std::cout << "Create log store successfully! Redo log path: " << log_store_path << std::endl;
}

// write log
bool LogStore::write_log(lsn_t lsn, const std::string &redo_log) {
    rocksdb::WriteOptions write_options;
    std::string key = std::to_string(lsn);
    // std::string value; // TODO: redo_log-> to string
    rocksdb::Status status = db_->Put(write_options, key, redo_log);
    #ifdef PRINT_LOG
        StorageDebug::getInstance()->DEBUG_WRITE_LOG(lsn, status.ok(), redo_log);
    #endif
    if (!status.ok()) {
        std::cerr << "Failed to write data. Error: " << status.ToString() << std::endl;
        std::cout << redo_log;

        return false;
    }
    else {
        // std::cout << "success to write log (lsn = " << lsn << ")\n";
    }
    return true;
}

// read log
std::string LogStore::read_log(lsn_t lsn) {
    rocksdb::ReadOptions read_options;
    std::string key = std::to_string(lsn);
    std::string value;
    rocksdb::Status status = db_->Get(read_options, key, &value);
    if (status.ok()) {
        // std::cout << "Retrieved value: " << value << std::endl;
        return value;
    } else {
        std::cerr << "Failed to retrieve data. Error: " << status.ToString() << std::endl;
        return "";
    }
}

// flush to disk
void LogStore::flush_log_to_disk() {
    // Flush data to disk
    rocksdb::FlushOptions options;
    db_->Flush(options);
}

// clear all data
void LogStore::clear_all() {
    rocksdb::Status status = rocksdb::DestroyDB(log_store_path_, rocksdb::Options());
    if (status.ok()) {
        std::cout << "Clear all log data in " << log_store_path_ << "\n";
    } else {
        std::cout << "Unable to clear all log data in " << log_store_path_ << "\n";
    }
}

