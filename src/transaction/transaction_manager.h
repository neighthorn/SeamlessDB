/*
 * @Description: 
 */
#pragma once

#include <atomic>
#include <unordered_map>

#include "transaction.h"
#include "recovery/log_manager.h"
#include "concurrency/lock_manager.h"
#include "system/sm_manager.h"

/* 系统采用的并发控制算法，当前题目中要求两阶段封锁并发控制算法 */
enum class ConcurrencyMode { TWO_PHASE_LOCKING = 0, BASIC_TO };

class TransactionManager{
public:
    explicit TransactionManager(LockManager *lock_manager, SmManager *sm_manager, int thread_num) {
        thread_num_ = thread_num;
        sm_manager_ = sm_manager;
        lock_manager_ = lock_manager;
        active_transactions_ = new Transaction*[thread_num];
        for(int i = 0; i < thread_num_; ++i) {
            active_transactions_[i] = new Transaction(i);
        }
    }
    
    ~TransactionManager() {
        for(int i = 0; i < thread_num_; ++i) delete active_transactions_[i];
        delete[] active_transactions_;
    }

    // void clear_txn(Transaction* txn) {
    //     if(txn == nullptr) return;
    //     std::unique_lock<std::mutex> lock(latch_);
    //     auto iter = TransactionManager::txn_map.find(txn->get_transaction_id());
    //     if(iter != txn_map.end()) txn_map.erase(iter);
    // }

    void begin(Transaction* txn, LogManager* log_manager);

    void commit(Transaction* txn, Context* context);

    void abort(Transaction* txn, Context* context);

    LockManager* get_lock_manager() { return lock_manager_; }

    /**
     * @description: 获取事务ID为txn_id的事务对象
     * @return {Transaction*} 事务对象的指针
     * @param {txn_id_t} txn_id 事务ID
     */    
    Transaction* get_transaction(int thread_id) {
        assert(thread_id < thread_num_);

        return active_transactions_[thread_id];
    }

    void recover_active_txn_lists(Context* context);

    void get_active_txn_ids() {
        
    }

    // static std::unordered_map<txn_id_t, Transaction *> txn_map;     // 全局事务表，存放事务ID与事务对象的映射关系
    Transaction** active_transactions_;
    int thread_num_;

private:
    // ConcurrencyMode concurrency_mode_;      // 事务使用的并发控制算法，目前只需要考虑2PL
    std::atomic<txn_id_t> next_txn_id_{0};  // 用于分发事务ID
    // std::atomic<timestamp_t> next_timestamp_{0};    // 用于分发事务时间戳
    // std::mutex latch_;  // 用于txn_map的并发
    SmManager *sm_manager_;
    LockManager *lock_manager_;
};