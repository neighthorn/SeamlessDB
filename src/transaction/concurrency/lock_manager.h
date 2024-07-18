#pragma once

#include <mutex>
#include <unordered_map>
#include "transaction/transaction.h"
#include "lock.h"

// static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};

class LockManager {

public:
    LockManager() {}

    ~LockManager() {}
    
    LockRequestQueue* get_record_request_queue_(const Rid& rid, LockListInBucket* lock_list);

    // lock request functions for record
    Lock* check_record_lock_conflic(RecordLockType lock_type, LockMode lock_mode, LockRequestQueue* request_queue, Lock* req);

    Lock* upgrade_record_lock_type_mode(RecordLockType lock_type, LockMode lock_mode, LockRequestQueue* request_queue, Lock* req, int thread_index);

    Lock* request_record_lock(int table_id, const Rid& rid, Transaction* txn, RecordLockType lock_type, LockMode lock_mode, int thread_index);

    // lock request functions for table
    Lock* upgrade_table_lock_mode(LockMode lock_mode, LockRequestQueue* request_queue, Lock* req, int thread_index);
    
    Lock* request_table_lock(int table_id, Transaction* txn, LockMode lock_mode, int thread_index);

    bool unlock(Transaction* txn, Lock* lock);

    void recover_lock_table(Transaction** active_txn_list, int thread_num);

private:
    std::mutex latch_;      // 用于锁表的并发
    std::unordered_map<LockDataId, LockListInBucket> lock_table_;   // 全局锁表
};

/**
 * LockTable
 * <(tableid, pageid), LockListInPage>
 * LockListInPage(page_id): LockRequestQueue->LockRequestQeueu-> ...
 * LockRequestQueue(rid): Lock->Lock-> ...
*/
