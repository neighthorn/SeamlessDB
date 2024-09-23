#pragma once

#include <iostream>
#include "common/config.h"
#include "record/record.h"
#include "storage/disk_manager.h"
#include "record/rm_defs.h"
#include "common/macro.h"
#include "redolog_defs.h"

/* 日志记录对应操作的类型 */
enum RedoLogType: int {
    UPDATE = 0,
    INSERT,
    DELETE,
    BEGIN,
    COMMIT,
    ABORT,
    SPLIT
};
static std::string RedoLogTypeStr[] = {
    "UPDATE",
    "INSERT",
    "DELETE",
    "BEGIN",
    "COMMIT",
    "ABORT",
    "SPLIT"
};

// redo log
class RedoLogRecord {
public:
    RedoLogType log_type_;          /* 日志对应操作的类型 */
    lsn_t lsn_;                     /* 当前日志的lsn */
    uint32_t log_tot_len_;          /* 整个日志记录的长度 */
    txn_id_t log_tid_;              /* 创建当前日志的事务ID */
    lsn_t prev_lsn_;                /* 事务创建的前一条日志记录的lsn，用于undo */
    bool is_persisit_;              /* 是否是一个原子操作的结尾 */

    ~RedoLogRecord() {}

    // 把日志记录序列化到dest中
    virtual void serialize (char* dest) const {
        memcpy(dest + REDO_LOG_TYPE_OFFSET, &log_type_, sizeof(RedoLogType));
        memcpy(dest + REDO_LOG_LSN_OFFSET, &lsn_, sizeof(lsn_t));
        memcpy(dest + REDO_LOG_TOTLEN_OFFSET, &log_tot_len_, sizeof(uint32_t));
        memcpy(dest + REDO_LOG_TID_OFFSET, &log_tid_, sizeof(txn_id_t));
        memcpy(dest + REDO_LOG_PREV_LSN_OFFSET, &prev_lsn_, sizeof(lsn_t));
        memcpy(dest + REDO_LOG_IS_PERSIST_OFFSET, &is_persisit_, sizeof(bool));
    }
    // 从src中反序列化出一条日志记录
    virtual void  deserialize(const char* src) {
        log_type_ = *reinterpret_cast<const RedoLogType*>(src + REDO_LOG_TYPE_OFFSET);
        lsn_ = *reinterpret_cast<const lsn_t*>(src + REDO_LOG_LSN_OFFSET);
        log_tot_len_ = *reinterpret_cast<const uint32_t*>(src + REDO_LOG_TOTLEN_OFFSET);
        log_tid_ = *reinterpret_cast<const txn_id_t*>(src + REDO_LOG_TID_OFFSET);
        prev_lsn_ = *reinterpret_cast<const lsn_t*>(src + REDO_LOG_PREV_LSN_OFFSET);
        is_persisit_ = *reinterpret_cast<const bool*>(src + REDO_LOG_IS_PERSIST_OFFSET);
    }
    // used for debug
    virtual void format_print() {
        std::cout << "log type in father_function: " << RedoLogTypeStr[log_type_] << "\n";
        printf("Print Log Record:\n");
        printf("log_type_: %s\n", RedoLogTypeStr[log_type_].c_str());
        printf("lsn: %d\n", lsn_);
        printf("log_tot_len: %d\n", log_tot_len_);
        printf("log_tid: %d\n", log_tid_);
        printf("prev_lsn: %d\n", prev_lsn_);
        printf("is_persist: %d\n", is_persisit_);
    }
};

class CommitLogRecord: public RedoLogRecord {
public:
    CommitLogRecord(){
        log_type_ = RedoLogType::COMMIT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = REDO_LOG_DATA_OFFSET;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        is_persisit_ = true;
    }

    CommitLogRecord(txn_id_t txn_id) :CommitLogRecord() {
        log_tid_ = txn_id;
    }

    ~CommitLogRecord() {}

    void serialize(char* dest) {
        RedoLogRecord::serialize(dest);
    }

    void deserialize(const char* src) {
        RedoLogRecord::deserialize(src);
    }

};

class AbortLogRecord: public RedoLogRecord {
public:
    AbortLogRecord(){
        log_type_ = RedoLogType::ABORT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = REDO_LOG_DATA_OFFSET;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        is_persisit_ = true;
    }

    AbortLogRecord(txn_id_t txn_id) :AbortLogRecord() {
        log_tid_ = txn_id;
    }

    ~AbortLogRecord() {}

    void serialize(char* dest) {
        RedoLogRecord::serialize(dest);
    }

    void deserialize(const char* src) {
        RedoLogRecord::deserialize(src);
    }
};

// update redo log

/**
 * TODO: update操作的日志记录
*/
class UpdateRedoLogRecord: public RedoLogRecord {
public:
    UpdateRedoLogRecord() {
        log_type_ = RedoLogType::UPDATE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = REDO_LOG_DATA_OFFSET;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
        is_persisit_ = false;
    }
    UpdateRedoLogRecord(txn_id_t txn_id, RmRecord& old_value, RmRecord& new_value, Rid& rid, std::string table_name)
        : UpdateRedoLogRecord() {
        log_tid_ = txn_id;
        old_value_ = old_value;
        new_value_ = new_value;
        log_tot_len_ += sizeof(int) * 2;
        log_tot_len_ += old_value_.size + new_value_.size;
        rid_ = rid;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        log_tot_len_ += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += table_name_size_;
    }

    ~UpdateRedoLogRecord() {
        if(table_name_ != nullptr) {
            delete[] table_name_;
        }
    }

    void serialize(char* dest) const override {
        RedoLogRecord::serialize(dest);
        int offset = REDO_LOG_DATA_OFFSET;
        memcpy(dest + offset, &old_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, old_value_.data, old_value_.size);
        offset += old_value_.size;
        memcpy(dest + offset, &new_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, new_value_.data, new_value_.size);
        offset += new_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
    }
    void deserialize(const char* src) override {
        RedoLogRecord::deserialize(src);
        // printf("finish deserialize log header\n");
        old_value_.Deserialize(src + REDO_LOG_DATA_OFFSET);
        // printf("finish deserialze old value\n");
        int offset = REDO_LOG_DATA_OFFSET + old_value_.size + sizeof(int);
        new_value_.Deserialize(src + offset);
        // printf("finish deserialze new value\n");
        offset += sizeof(int) + new_value_.size;
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        // printf("finish deserialze rid\n");
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
    }
    void format_print() override {
        RedoLogRecord::format_print(); 
        // printf("old_value: %s\n", old_value_.data);
        printf("old_value: "); print_char_array(old_value_.data, old_value_.size);
        printf("new_value: "); print_char_array(new_value_.data, new_value_.size);
        printf("update rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", table_name_);
    }

    RmRecord old_value_;
    RmRecord new_value_;
    Rid rid_;
    char* table_name_;
    size_t table_name_size_;
};

/**
 * TODO: delete操作的日志记录
*/
class DeleteRedoLogRecord: public RedoLogRecord {
public:
    DeleteRedoLogRecord() {
        log_type_ = RedoLogType::DELETE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = REDO_LOG_DATA_OFFSET;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
        is_persisit_ = false;
    }
    DeleteRedoLogRecord(txn_id_t txn_id, RmRecord& delete_value, Rid& rid, std::string table_name)
        : DeleteRedoLogRecord() {
        log_tid_ = txn_id;
        delete_value_ = delete_value;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += delete_value_.size;
        rid_ = rid;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        log_tot_len_ += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += table_name_size_;
    }

    ~DeleteRedoLogRecord() {
        if(table_name_ != nullptr) {
            delete[] table_name_;
        }
    }

    void serialize(char* dest) const override {
        RedoLogRecord::serialize(dest);
        int offset = REDO_LOG_DATA_OFFSET;
        memcpy(dest + offset, &delete_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, delete_value_.data, delete_value_.size);
        offset += delete_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
    }
    void deserialize(const char* src) override {
        RedoLogRecord::deserialize(src);
        delete_value_.Deserialize(src + REDO_LOG_DATA_OFFSET);
        int offset = REDO_LOG_DATA_OFFSET + delete_value_.size + sizeof(int);
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
    }
    void format_print() override {
        RedoLogRecord::format_print();
        printf("delete_value: %s\n", delete_value_.data);
        printf("delete rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", table_name_);
    }

    RmRecord delete_value_;
    Rid rid_;
    char* table_name_;
    size_t table_name_size_;
};

// insert redo log
class InsertRedoLogRecord: public RedoLogRecord {
public:
    InsertRedoLogRecord() {
        log_type_ = RedoLogType::INSERT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = REDO_LOG_DATA_OFFSET;
        log_tid_ = INVALID_TXN_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
        is_persisit_ = false;
    }
    
    InsertRedoLogRecord(txn_id_t txn_id, char *key, int key_size, RmRecord& insert_value, Rid& rid, std::string table_name) 
        : InsertRedoLogRecord() {
        log_tid_ = txn_id;

        // key
        key_ = new char[key_size];
        key_size_ = key_size;
        memcpy(key_, key, key_size);
        log_tot_len_ += sizeof(size_t);
        log_tot_len_ += key_size;

        // insert value
        insert_value_ = insert_value;
        log_tot_len_ += sizeof(int);
        log_tot_len_ += insert_value_.size;

        // rid
        rid_ = rid;
        log_tot_len_ += sizeof(Rid);

        // table name
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t);
        log_tot_len_ += table_name_size_;
    }

    ~InsertRedoLogRecord() {
        if(key_ != nullptr) {
            delete[] key_;
        }
        if(table_name_ != nullptr) {
            delete[] table_name_;
        }
    }

    // 把insert日志记录序列化到dest中
    void serialize(char* dest) const override {
        RedoLogRecord::serialize(dest);
        int offset = REDO_LOG_DATA_OFFSET;
        
        memcpy(dest + offset, &key_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, key_, key_size_);
        offset += key_size_;
        memcpy(dest + offset, &insert_value_.size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, insert_value_.data, insert_value_.size);
        offset += insert_value_.size;
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
    }
    // 从src中反序列化出一条Insert日志记录
    void deserialize(const char* src) override {
        RedoLogRecord::deserialize(src); 
        int offset = REDO_LOG_DATA_OFFSET;
        key_size_ = *reinterpret_cast<const size_t *>(src + offset);
        offset += sizeof(size_t);
        key_ = new char[key_size_];
        memcpy(key_, src + offset, key_size_);
        offset += key_size_;
        insert_value_.Deserialize(src + offset);
        offset += insert_value_.size + sizeof(int);
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
    }
    void format_print() override {
        printf("insert record\n");
        RedoLogRecord::format_print();
        // printf("insert key: %s\n", key_);
        printf("insert_value: %s\n", insert_value_.data);
        printf("insert rid: %d, %d\n", rid_.page_no, rid_.slot_no);
        printf("table name: %s\n", table_name_);
    }

    char *key_;                 // insert key;
    size_t key_size_;           // key size;
    RmRecord insert_value_;     // 插入的记录
    Rid rid_;                   // 记录插入的位置
    char* table_name_;          // 插入记录的表名称
    size_t table_name_size_;    // 表名称的大小
};

// class InsertWithoutSplitRedologRecord : public RedoLogRecord {
// public:
//     InsertWithoutSplitRedoLogRecord() {
//         log_type_ = RedoLogType::INSERT_WITHOUT_SPLIT;
//         lsn_ = INVALID_LSN;
//         log_tot_len_ = REDO_LOG_DATA_OFFSET;
//         log_tid_ = INVALID_TXN_ID;
//         prev_lsn_ = INVALID_LSN;
//         is_persisit_ = false;
//     }


// private:
//     PageId page_id_;
// }
//     RmRecord delete_value_;
//     Rid rid_;
//     char* table_name_;
//     size_t table_name_size_;
// };

// // begin 
// class BeginRedoLogRecord: public RedoLogRecord {
// public:
//     BeginRedoLogRecord() {
//         log_type_ = RedoLogType::BEGIN;
//         lsn_ = INVALID_LSN;
//         log_tot_len_ = REDO_LOG_HEADER_SIZE;
//         log_tid_ = INVALID_TXN_ID;
//         prev_lsn_ = INVALID_LSN;
//     }
//     BeginRedoLogRecord(txn_id_t txn_id) : BeginRedoLogRecord() {
//         log_tid_ = txn_id;
//     }
//     // 序列化Begin日志记录到dest中
//     void serialize(char* dest) const override {
//         RedoLogRecord::serialize(dest);
//     }
//     // 从src中反序列化出一条Begin日志记录
//     void deserialize(const char* src) override {
//         RedoLogRecord::deserialize(src);   
//     }
//     virtual void format_print() override {
//         std::cout << "log type in son_function: " << RedoLogTypeStr[log_type_] << "\n";
//         RedoLogRecord::format_print();
//     }
// };

// /**
//  * TODO: commit操作的日志记录
// */
// class CommitLogRecord: public RedoLogRecord {
// public:
//     CommitLogRecord() {
//         log_type_ = RedoLogType::COMMIT;
//         lsn_ = INVALID_LSN;
//         log_tot_len_ = REDO_LOG_HEADER_SIZE;
//         log_tid_ = INVALID_TXN_ID;
//         prev_lsn_ = INVALID_LSN;
//     }
//     CommitLogRecord(txn_id_t txn_id) : CommitLogRecord() {
//         log_tid_ = txn_id;
//     }

//     void serialize(char* dest) const override {
//         RedoLogRecord::serialize(dest);
//     }
//     void deserialize(const char* src) override {
//         RedoLogRecord::deserialize(src);  
//     }
//     void format_print() override {
//         RedoLogRecord::format_print();
//     }
// };

// /**
//  * TODO: abort操作的日志记录
// */
// class AbortLogRecord: public RedoLogRecord {
// public:
//     AbortLogRecord() {
//         log_type_ = RedoLogType::ABORT;
//         lsn_ = INVALID_LSN;
//         log_tot_len_ = REDO_LOG_HEADER_SIZE;
//         log_tid_ = INVALID_TXN_ID;
//         prev_lsn_ = INVALID_LSN;
//     }
//     AbortLogRecord(txn_id_t txn_id) : AbortLogRecord() {
//         log_tid_ = txn_id;
//     }
//     void serialize(char* dest) const override {
//         RedoLogRecord::serialize(dest);
//     }
//     void deserialize(const char* src) override {
//         RedoLogRecord::deserialize(src);  
//     }
//     void format_print() override {
//         RedoLogRecord::format_print();
//     }
// };

// // insert log


// /**
//  * TODO: delete操作的日志记录
// */
// class DeleteLogRecord: public RedoLogRecord {
// public:
//     DeleteLogRecord() {
//         log_type_ = RedoLogType::DELETE;
//         lsn_ = INVALID_LSN;
//         log_tot_len_ = REDO_LOG_DATA_OFFSET;
//         log_tid_ = INVALID_TXN_ID;
//         prev_lsn_ = INVALID_LSN;
//         table_name_ = nullptr;
//     }
//     DeleteLogRecord(txn_id_t txn_id, RmRecord& delete_value, Rid& rid, std::string table_name)
//         : DeleteLogRecord() {
//         log_tid_ = txn_id;
//         delete_value_ = delete_value;
//         log_tot_len_ += sizeof(int);
//         log_tot_len_ += delete_value_.size;
//         rid_ = rid;
//         log_tot_len_ += sizeof(Rid);
//         table_name_size_ = table_name.length();
//         log_tot_len_ += sizeof(size_t);
//         table_name_ = new char[table_name_size_];
//         memcpy(table_name_, table_name.c_str(), table_name_size_);
//         log_tot_len_ += table_name_size_;
//     }

//     void serialize(char* dest) const override {
//         RedoLogRecord::serialize(dest);
//         int offset = REDO_LOG_DATA_OFFSET;
//         memcpy(dest + offset, &delete_value_.size, sizeof(int));
//         offset += sizeof(int);
//         memcpy(dest + offset, delete_value_.data, delete_value_.size);
//         offset += delete_value_.size;
//         memcpy(dest + offset, &rid_, sizeof(Rid));
//         offset += sizeof(Rid);
//         memcpy(dest + offset, &table_name_size_, sizeof(size_t));
//         offset += sizeof(size_t);
//         memcpy(dest + offset, table_name_, table_name_size_);
//     }
//     void deserialize(const char* src) override {
//         RedoLogRecord::deserialize(src);
//         delete_value_.Deserialize(src + REDO_LOG_DATA_OFFSET);
//         int offset = REDO_LOG_DATA_OFFSET + delete_value_.size + sizeof(int);
//         rid_ = *reinterpret_cast<const Rid*>(src + offset);
//         offset += sizeof(Rid);
//         table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
//         offset += sizeof(size_t);
//         table_name_ = new char[table_name_size_];
//         memcpy(table_name_, src + offset, table_name_size_);
//     }
//     void format_print() override {
//         RedoLogRecord::format_print();
//         printf("delete_value: %s\n", delete_value_.data);
//         printf("delete rid: %d, %d\n", rid_.page_no, rid_.slot_no);
//         printf("table name: %s\n", table_name_);
//     }

//     RmRecord delete_value_;
//     Rid rid_;
//     char* table_name_;
//     size_t table_name_size_;
// };

