#pragma once

#include "recovery/redo_log/redolog_defs.h"
#include "recovery/redo_log/redo_log.h"
#include "rocksdb/db.h"

// 使用rocksdb支持log store
class LogStore {
public:
    LogStore(const std::string &log_store_path);

    // insert log
    bool write_log(lsn_t lsn, const std::string &redo_log);

    // read log
    std::string read_log(lsn_t lsn);

    // flush disk
    void flush_log_to_disk();

    // Be cautious when using this function.
    // clear all data

    void clear_all();

private:
    rocksdb::DB *db_;
    std::string log_store_path_;
};