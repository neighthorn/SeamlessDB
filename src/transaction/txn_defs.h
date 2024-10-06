#pragma once

#include <atomic>

#include "common/config.h"
#include "record/record.h"
#include "storage/page.h"

/* 标识事务状态 */
enum TransactionState: int { DEFAULT, GROWING, SHRINKING, COMMITTED, ABORTED };

static const std::string TransactionStateStr[5] = {
    "DEFAULT", "GROWING", "SHRINKING", "COMMITTED", "ABORTED"
};

/* 系统的隔离级别，当前赛题中为可串行化隔离级别 */
enum class IsolationLevel { READ_UNCOMMITTED, REPEATABLE_READ, READ_COMMITTED, SERIALIZABLE };

/* 事务写操作类型，包括插入、删除、更新三种操作 */
enum class WType { INSERT_TUPLE = 0, DELETE_TUPLE, UPDATE_TUPLE};

class WriteRecord {
   public:
    WriteRecord() = default;

    // constructor for insert & delete operation
    WriteRecord(WType wtype, const std::string& tab_name, char* pkey, int pkey_len)
        : wtype_(wtype), table_name_(tab_name) {
        pkey_ = new char[pkey_len];
        memcpy(pkey_, pkey, pkey_len);
        pkey_len_ = pkey_len;
    }

    // constructor for update operation
    WriteRecord(WType wtype, const std::string& tab_name, char* pkey, int pkey_len, const Record& record)
        : wtype_(wtype), table_name_(tab_name), record_(record) {
        pkey_ = new char[pkey_len];
        memcpy(pkey_, pkey, pkey_len);
        pkey_len_ = pkey_len;
    }

    ~WriteRecord() {
        delete pkey_;
    }

    WType wtype_;
    std::string table_name_;
    char* pkey_;            // we store the pkey to identify the modified record because Rid can change during the transaction's exectuion
    int pkey_len_;
    Record record_;         // store the old value of update record
};

/* 事务回滚原因 */
enum class AbortReason { LOCK_ON_SHIRINKING = 0, UPGRADE_CONFLICT, DEADLOCK_PREVENTION };

/* 事务回滚异常，在rmdb.cpp中进行处理 */
class TransactionAbortException : public std::exception {
    txn_id_t txn_id_;
    AbortReason abort_reason_;

   public:
    explicit TransactionAbortException(txn_id_t txn_id, AbortReason abort_reason)
        : txn_id_(txn_id), abort_reason_(abort_reason) {}

    txn_id_t get_transaction_id() { return txn_id_; }
    AbortReason GetAbortReason() { return abort_reason_; }
    std::string GetInfo() {
        switch (abort_reason_) {
            case AbortReason::LOCK_ON_SHIRINKING: {
                return "Transaction " + std::to_string(txn_id_) +
                       " aborted because it cannot request locks on SHRINKING phase\n";
            } break;

            case AbortReason::UPGRADE_CONFLICT: {
                return "Transaction " + std::to_string(txn_id_) +
                       " aborted because another transaction is waiting for upgrading\n";
            } break;

            case AbortReason::DEADLOCK_PREVENTION: {
                return "Transaction " + std::to_string(txn_id_) + " aborted for deadlock prevention\n";
            } break;

            default: {
                return "Transaction aborted\n";
            } break;
        }
    }
};