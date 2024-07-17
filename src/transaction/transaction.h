#pragma once

#include <atomic>
#include <deque>
#include <string>
#include <thread>
#include <unordered_set>
#include <list>

#include "readview/readview.h"
#include "txn_defs.h"
#include "concurrency/lock.h"

class Transaction {
    public:
    Transaction(int thread_id) {
        write_set_ = std::make_shared<std::deque<WriteRecord *>>();
        lock_set_ = std::make_shared<std::unordered_set<Lock *>>();
        index_latch_page_set_ = std::make_shared<std::deque<Page *>>();
        index_deleted_page_set_ = std::make_shared<std::deque<Page*>>();
        prev_lsn_ = INVALID_LSN;
        thread_id_ = thread_id;
        is_read_only_txn_ = false;
        readview_ = std::make_shared<ReadView>();
    }
    // explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::SERIALIZABLE)
    //     : state_(TransactionState::DEFAULT), isolation_level_(isolation_level), txn_id_(txn_id) {
    //     write_set_ = std::make_shared<std::deque<WriteRecord *>>();
    //     lock_set_ = std::make_shared<std::unordered_set<Lock *>>();
    //     index_latch_page_set_ = std::make_shared<std::deque<Page *>>();
    //     index_deleted_page_set_ = std::make_shared<std::deque<Page*>>();
    //     prev_lsn_ = INVALID_LSN;
    //     thread_id_ = std::this_thread::get_id();
    // }

    ~Transaction() = default;

    inline void clear() {
        for(auto& write_record: *write_set_) delete write_record;
        write_set_->clear();

        lock_set_->clear();
        index_deleted_page_set_->clear();
        index_latch_page_set_->clear();
        prev_lsn_ = INVALID_LSN;

        if(is_read_only_txn_) readview_->clear();
    }

    inline void set_transaction_id(txn_id_t txn_id) { txn_id_ = txn_id; }
    inline txn_id_t get_transaction_id() { return txn_id_; }

    inline int get_thread_id() { return thread_id_; }

    inline void set_start_ts(timestamp_t start_ts) { start_ts_ = start_ts; }
    inline timestamp_t get_start_ts() { return start_ts_; }

    inline TransactionState get_state() { return state_; }
    inline void set_state(TransactionState state) { state_ = state; }

    inline lsn_t get_prev_lsn() { return prev_lsn_; }
    inline void set_prev_lsn(lsn_t prev_lsn) { prev_lsn_ = prev_lsn; }

    inline std::shared_ptr<std::deque<WriteRecord *>> get_write_set() { return write_set_; }  
    inline void append_write_record(WriteRecord* write_record) { write_set_->push_back(write_record); }

    inline std::shared_ptr<std::deque<Page*>> get_index_deleted_page_set() { return index_deleted_page_set_; }
    inline void append_index_deleted_page(Page* page) { index_deleted_page_set_->push_back(page); }

    inline std::shared_ptr<std::deque<Page*>> get_index_latch_page_set() { return index_latch_page_set_; }
    inline void append_index_latch_page_set(Page* page) { index_latch_page_set_->push_back(page); }

    inline std::shared_ptr<std::unordered_set<Lock*>> get_lock_set() { return lock_set_; }
    inline void append_lock(Lock* lock) { lock_set_->emplace(lock); }

    inline void set_read_view(std::vector<txn_id_t>&& active_txn_ids, txn_id_t next_txn_id) {
        txn_id_t min_txn_id = INT32_MAX;
        for(auto txn_id: active_txn_ids) {
            if(txn_id < min_txn_id) min_txn_id = txn_id;
        }
        readview_->udpate_readview(txn_id_, std::move(active_txn_ids), min_txn_id, next_txn_id);
    }
    inline std::shared_ptr<ReadView> get_read_view() { return readview_; }

   private:
    TransactionState state_;          // 事务状态
    int64_t thread_id_;       // 当前事务对应的线程id
    lsn_t prev_lsn_;                  // 当前事务执行的最后一条操作对应的lsn，用于系统故障恢复
    txn_id_t txn_id_;                 // 事务的ID，唯一标识符
    timestamp_t start_ts_;            // 事务的开始时间戳

    bool is_read_only_txn_;                             // 是否为只读事务
    
    std::shared_ptr<ReadView> readview_;                // 事务的可见快照

    std::shared_ptr<std::deque<WriteRecord *>> write_set_;  // 事务包含的所有写操作
    std::shared_ptr<std::unordered_set<Lock*>> lock_set_;  // 事务申请的所有锁
    std::shared_ptr<std::deque<Page*>> index_latch_page_set_;          // 维护事务执行过程中加锁的索引页面
    std::shared_ptr<std::deque<Page*>> index_deleted_page_set_;    // 维护事务执行过程中删除的索引页面
};
