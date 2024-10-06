#pragma once

#include <iostream>
#include "log_defs.h"
#include "common/config.h"
#include "record/record.h"
#include "storage/disk_manager.h"
#include "record/rm_defs.h"

// /* 日志记录对应操作的类型 */
// enum LogType: int {
//     UPDATE = 0,
//     INSERT,
//     DELETE,
//     begin,
//     commit,
//     ABORT
// };
// static std::string LogTypeStr[] = {
//     "UPDATE",
//     "INSERT",
//     "DELETE",
//     "BEGIN",
//     "COMMIT",
//     "ABORT"
// };

// class LogRecord {
// public:
//     LogType log_type_;         /* 日志对应操作的类型 */
//     lsn_t lsn_;                /* 当前日志的lsn */
//     uint32_t log_tot_len_;     /* 整个日志记录的长度 */
//     txn_id_t log_tid_;         /* 创建当前日志的事务ID */
//     lsn_t prev_lsn_;           /* 事务创建的前一条日志记录的lsn，用于undo */

//     // 把日志记录序列化到dest中
//     virtual void serialize (char* dest) const {
//         memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));
//         memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_t));
//         memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(uint32_t));
//         memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(txn_id_t));
//         memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(lsn_t));
//     }
//     // 从src中反序列化出一条日志记录
//     virtual void deserialize(const char* src) {
//         log_type_ = *reinterpret_cast<const LogType*>(src);
//         lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_LSN);
//         log_tot_len_ = *reinterpret_cast<const uint32_t*>(src + OFFSET_LOG_TOT_LEN);
//         log_tid_ = *reinterpret_cast<const txn_id_t*>(src + OFFSET_LOG_TID);
//         prev_lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_PREV_LSN);
//     }
//     // used for debug
//     virtual void format_print() {
//         std::cout << "log type in father_function: " << LogTypeStr[log_type_] << "\n";
//         printf("Print Log Record:\n");
//         printf("log_type_: %s\n", LogTypeStr[log_type_].c_str());
//         printf("lsn: %d\n", lsn_);
//         printf("log_tot_len: %d\n", log_tot_len_);
//         printf("log_tid: %d\n", log_tid_);
//         printf("prev_lsn: %d\n", prev_lsn_);
//     }
// };

// class BeginLogRecord: public LogRecord {
// public:
//     BeginLogRecord() {
//         log_type_ = LogType::begin;
//         lsn_ = INVALID_LSN;
//         log_tot_len_ = LOG_HEADER_SIZE;
//         log_tid_ = INVALID_TXN_ID;
//         prev_lsn_ = INVALID_LSN;
//     }
//     BeginLogRecord(txn_id_t txn_id) : BeginLogRecord() {
//         log_tid_ = txn_id;
//     }
//     // 序列化Begin日志记录到dest中
//     void serialize(char* dest) const override {
//         LogRecord::serialize(dest);
//     }
//     // 从src中反序列化出一条Begin日志记录
//     void deserialize(const char* src) override {
//         LogRecord::deserialize(src);   
//     }
//     virtual void format_print() override {
//         std::cout << "log type in son_function: " << LogTypeStr[log_type_] << "\n";
//         LogRecord::format_print();
//     }
// };

// /**
//  * TODO: commit操作的日志记录
// */
// class CommitLogRecord: public LogRecord {
// public:
//     CommitLogRecord() {
//         log_type_ = LogType::commit;
//         lsn_ = INVALID_LSN;
//         log_tot_len_ = LOG_HEADER_SIZE;
//         log_tid_ = INVALID_TXN_ID;
//         prev_lsn_ = INVALID_LSN;
//     }
//     CommitLogRecord(txn_id_t txn_id) : CommitLogRecord() {
//         log_tid_ = txn_id;
//     }

//     void serialize(char* dest) const override {
//         LogRecord::serialize(dest);
//     }
//     void deserialize(const char* src) override {
//         LogRecord::deserialize(src);  
//     }
//     void format_print() override {
//         LogRecord::format_print();
//     }
// };

// /**
//  * TODO: abort操作的日志记录
// */
// class AbortLogRecord: public LogRecord {
// public:
//     AbortLogRecord() {
//         log_type_ = LogType::ABORT;
//         lsn_ = INVALID_LSN;
//         log_tot_len_ = LOG_HEADER_SIZE;
//         log_tid_ = INVALID_TXN_ID;
//         prev_lsn_ = INVALID_LSN;
//     }
//     AbortLogRecord(txn_id_t txn_id) : AbortLogRecord() {
//         log_tid_ = txn_id;
//     }

//     void serialize(char* dest) const override {
//         LogRecord::serialize(dest);
//     }
//     void deserialize(const char* src) override {
//         LogRecord::deserialize(src);  
//     }
//     void format_print() override {
//         LogRecord::format_print();
//     }
// };

// class InsertLogRecord: public LogRecord {
// public:
//     InsertLogRecord() {
//         log_type_ = LogType::INSERT;
//         lsn_ = INVALID_LSN;
//         log_tot_len_ = LOG_HEADER_SIZE;
//         log_tid_ = INVALID_TXN_ID;
//         prev_lsn_ = INVALID_LSN;
//         table_name_ = nullptr;
//     }
//     InsertLogRecord(txn_id_t txn_id, RmRecord& insert_value, Rid& rid, std::string table_name) 
//         : InsertLogRecord() {
//         log_tid_ = txn_id;
//         insert_value_ = insert_value;
//         rid_ = rid;
//         log_tot_len_ += sizeof(int);
//         log_tot_len_ += insert_value_.size;
//         log_tot_len_ += sizeof(Rid);
//         table_name_size_ = table_name.length();
//         table_name_ = new char[table_name_size_];
//         memcpy(table_name_, table_name.c_str(), table_name_size_);
//         log_tot_len_ += sizeof(size_t) + table_name_size_;
//     }

//     // 把insert日志记录序列化到dest中
//     void serialize(char* dest) const override {
//         LogRecord::serialize(dest);
//         int offset = OFFSET_LOG_DATA;
//         memcpy(dest + offset, &insert_value_.size, sizeof(int));
//         offset += sizeof(int);
//         memcpy(dest + offset, insert_value_.data, insert_value_.size);
//         offset += insert_value_.size;
//         memcpy(dest + offset, &rid_, sizeof(Rid));
//         offset += sizeof(Rid);
//         memcpy(dest + offset, &table_name_size_, sizeof(size_t));
//         offset += sizeof(size_t);
//         memcpy(dest + offset, table_name_, table_name_size_);
//     }
//     // 从src中反序列化出一条Insert日志记录
//     void deserialize(const char* src) override {
//         LogRecord::deserialize(src);  
//         insert_value_.Deserialize(src + OFFSET_LOG_DATA);
//         int offset = OFFSET_LOG_DATA + insert_value_.size + sizeof(int);
//         rid_ = *reinterpret_cast<const Rid*>(src + offset);
//         offset += sizeof(Rid);
//         table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
//         offset += sizeof(size_t);
//         table_name_ = new char[table_name_size_];
//         memcpy(table_name_, src + offset, table_name_size_);
//     }
//     void format_print() override {
//         printf("insert record\n");
//         LogRecord::format_print();
//         printf("insert_value: %s\n", insert_value_.data);
//         printf("insert rid: %d, %d\n", rid_.page_no, rid_.slot_no);
//         printf("table name: %s\n", table_name_);
//     }

//     RmRecord insert_value_;     // 插入的记录
//     Rid rid_;                   // 记录插入的位置
//     char* table_name_;          // 插入记录的表名称
//     size_t table_name_size_;    // 表名称的大小
// };

// /**
//  * TODO: delete操作的日志记录
// */
// class DeleteLogRecord: public LogRecord {
// public:
//     DeleteLogRecord() {
//         log_type_ = LogType::DELETE;
//         lsn_ = INVALID_LSN;
//         log_tot_len_ = LOG_HEADER_SIZE;
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
//         LogRecord::serialize(dest);
//         int offset = OFFSET_LOG_DATA;
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
//         LogRecord::deserialize(src);
//         delete_value_.Deserialize(src + OFFSET_LOG_DATA);
//         int offset = OFFSET_LOG_DATA + delete_value_.size + sizeof(int);
//         rid_ = *reinterpret_cast<const Rid*>(src + offset);
//         offset += sizeof(Rid);
//         table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
//         offset += sizeof(size_t);
//         table_name_ = new char[table_name_size_];
//         memcpy(table_name_, src + offset, table_name_size_);
//     }
//     void format_print() override {
//         LogRecord::format_print();
//         printf("delete_value: %s\n", delete_value_.data);
//         printf("delete rid: %d, %d\n", rid_.page_no, rid_.slot_no);
//         printf("table name: %s\n", table_name_);
//     }

//     RmRecord delete_value_;
//     Rid rid_;
//     char* table_name_;
//     size_t table_name_size_;
// };

// /**
//  * TODO: update操作的日志记录
// */
// class UpdateLogRecord: public LogRecord {
// public:
//     UpdateLogRecord() {
//         log_type_ = LogType::UPDATE;
//         lsn_ = INVALID_LSN;
//         log_tot_len_ = LOG_HEADER_SIZE;
//         log_tid_ = INVALID_TXN_ID;
//         prev_lsn_ = INVALID_LSN;
//         table_name_ = nullptr;
//     }
//     UpdateLogRecord(txn_id_t txn_id, RmRecord& old_value, RmRecord& new_value, Rid& rid, std::string table_name)
//         : UpdateLogRecord() {
//         log_tid_ = txn_id;
//         old_value_ = old_value;
//         new_value_ = new_value;
//         log_tot_len_ += sizeof(int) * 2;
//         log_tot_len_ += old_value_.size + new_value_.size;
//         rid_ = rid;
//         log_tot_len_ += sizeof(Rid);
//         table_name_size_ = table_name.length();
//         log_tot_len_ += sizeof(size_t);
//         table_name_ = new char[table_name_size_];
//         memcpy(table_name_, table_name.c_str(), table_name_size_);
//         log_tot_len_ += table_name_size_;
//     }

//     void serialize(char* dest) const override {
//         LogRecord::serialize(dest);
//         int offset = OFFSET_LOG_DATA;
//         memcpy(dest + offset, &old_value_.size, sizeof(int));
//         offset += sizeof(int);
//         memcpy(dest + offset, old_value_.data, old_value_.size);
//         offset += old_value_.size;
//         memcpy(dest + offset, &new_value_.size, sizeof(int));
//         offset += sizeof(int);
//         memcpy(dest + offset, new_value_.data, new_value_.size);
//         offset += new_value_.size;
//         memcpy(dest + offset, &rid_, sizeof(Rid));
//         offset += sizeof(Rid);
//         memcpy(dest + offset, &table_name_size_, sizeof(size_t));
//         offset += sizeof(size_t);
//         memcpy(dest + offset, table_name_, table_name_size_);
//     }
//     void deserialize(const char* src) override {
//         LogRecord::deserialize(src);
//         // printf("finish deserialize log header\n");
//         old_value_.Deserialize(src + OFFSET_LOG_DATA);
//         // printf("finish deserialze old value\n");
//         int offset = OFFSET_LOG_DATA + old_value_.size + sizeof(int);
//         new_value_.Deserialize(src + offset);
//         // printf("finish deserialze new value\n");
//         offset += sizeof(int) + new_value_.size;
//         rid_ = *reinterpret_cast<const Rid*>(src + offset);
//         // printf("finish deserialze rid\n");
//         offset += sizeof(Rid);
//         table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
//         offset += sizeof(size_t);
//         table_name_ = new char[table_name_size_];
//         memcpy(table_name_, src + offset, table_name_size_);
//     }
//     void format_print() override {
//         LogRecord::format_print(); 
//         printf("old_value: %s\n", old_value_.data);
//         printf("new_value: %s\n", new_value_.data);
//         printf("update rid: %d, %d\n", rid_.page_no, rid_.slot_no);
//         printf("table name: %s\n", table_name_);
//     }

//     RmRecord old_value_;
//     RmRecord new_value_;
//     Rid rid_;
//     char* table_name_;
//     size_t table_name_size_;
// };