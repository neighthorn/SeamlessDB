# pragma once

#include <set>
#include <mutex>
#include <list>

#include "region_manager.h"
#include "rlib/rdma_ctrl.hpp"
#include "transaction/concurrency/lock.h"
#include "allocator/buffer_allocator.h"
#include "coroutine/coroutine_scheduler.h"
#include "allocator/rdma_region_allocator.h"
#include "state_item/common_use.h"
#include "state/qp_manager.h"

using namespace rdmaio;

/**
 * the lock info is written into the state node before the log info;
 * the cursor uses lsn_ to store the relationship with the log;
 * the corresponding log info is written into the state node before the cursor;
 * after a commit/abort log, the state info has to be flushed into the state node;
*/

struct CompareLockPtr {
    CompareLockPtr(Lock* lock): targetlock(lock) {}

    Lock* targetlock;

    bool operator()(Lock* lock) const {
        return lock == targetlock;
    }
};

class ContextManager {
public:
    static bool create_instance(int thread_num);
    static void destroy_instance();
    static ContextManager* get_instance() {
        assert(state_mgr_ != nullptr);
        return state_mgr_;
    }

    // implementation of the write_context_entry() for lock
    void append_lock_state(Lock* lock, int thread_index);

    void fetch_and_print_lock_states();

    // implementation of the write_context_entry() for lock
    void erase_lock_state(Lock* lock);

    // implementation of the r_write_context_entry() for lock
    void flush_locks(int64_t curr_flush_last_lock, int64_t free_size);

    // // implementation of the r_write_context_entry() for log
    void flush_logs(int64_t curr_state_tail, int64_t curr_head, int64_t curr_tail);

    void checkpoint();

    // implementation for read_context() interfaces
    void fetch_lock_states(std::unordered_map<LockDataId, LockListInBucket>* lock_table, Transaction** active_txn_list, int thread_num);
    LockRequestQueue* get_record_request_queue_(int record_no, LockListInBucket* lock_list);
    void fetch_log_states();
    void fetch_active_txns(Transaction** active_txn_list, int thread_num);

    // global state
    std::mutex state_latch_;                    // the latch is used for the statemanager to get state checkpoint
    bool is_flushing_state_;                    // to signify whether the statemanager is currently flushing a checkpoint
    CoroutineScheduler* coro_sched_;

    // lock state
    // std::set<Lock*, CompareLockPtr> lock_states_;       // the locks that are not stored in the state node
    std::mutex lock_latch_;                     
    // std::list<Lock*> lock_states_;
    RegionBitmap* lock_bitmap_;
    char* lock_bitmap_rdma_buffer_;
    // std::list<Lock*>::iterator flush_first_lock_;
    // std::list<Lock*>::iterator flush_last_lock_;
    // int lock_num_;
    RCQP* lock_qp_;
    RDMACircularBuffer* lock_rdma_buffer_;
    int64_t flush_first_lock_;
    int64_t flush_last_lock_;
    
    // log state, flush_first_log_ and flush_last_log_ is obtained when invoking flush_states()
    int64_t flushed_log_offset_;
    int64_t need_flush_offset_;
    int64_t curr_log_head_;
    int64_t curr_log_tail_;
    RDMACircularBuffer* log_rdma_buffer_;
    RCQP* log_qp_;
    int remote_log_head_off_;
    int remote_log_tail_off_;
    int remote_log_state_tail_off_;
    char* log_meta_mr_;

private:
     ContextManager(int thread_num) {
        lock_bitmap_ = new RegionBitmap(LOCK_MAX_COUNT, thread_num);
        auto lock_buffer = RDMARegionAllocator::get_instance()->GetLockRegion();
        // lock_rdma_buffer_ = new RDMABufferAllocator(lock_buffer.first, lock_buffer.second);
        lock_rdma_buffer_ = new RDMACircularBuffer(lock_buffer.first, RDMARegionAllocator::get_instance()->lock_buf_size);
        lock_bitmap_rdma_buffer_ = RDMARegionAllocator::get_instance()->GetLockBitmapRegion();
        flush_first_lock_ = lock_rdma_buffer_->head_;
        coro_sched_ = new CoroutineScheduler(0, CORO_NUM);
        node_id_t primary_id = MetaManager::get_instance()->GetPrimaryNodeID();
        lock_qp_ = QPManager::get_instance()->GetRemoteLockBufQPWithNodeID(primary_id);
        
        char* log_buffer = RDMARegionAllocator::get_instance()->GetLogRegion();
        int log_buf_size = RDMARegionAllocator::get_instance()->log_buf_size;
        log_rdma_buffer_ = new RDMACircularBuffer(log_buffer, log_buf_size);
        need_flush_offset_ = 0;
        flushed_log_offset_ = 0;
        log_qp_ = QPManager::get_instance()->GetRemoteLogBufQPWithNodeID(primary_id);
        remote_log_head_off_ = log_buf_size;
        remote_log_tail_off_ = remote_log_head_off_ + sizeof(int64_t);
        remote_log_state_tail_off_ = remote_log_tail_off_ + sizeof(int64_t);
        log_meta_mr_ = RDMARegionAllocator::get_instance()->GetLogMetaRegion();

        stop_thread_.store(false);
        log_flush_thread_ = std::thread(&ContextManager::log_flush_thread_function, this);
        lock_flush_thread_ = std::thread(&ContextManager::lock_flush_thread_function, this);
    }

    ~ContextManager() {
        stop_thread_.store(true);
        if(lock_flush_thread_.joinable()) {
            lock_flush_thread_.join();
        }
        if(log_flush_thread_.joinable()) {
            log_flush_thread_.join();
        }
        delete lock_bitmap_;
    }

    void lock_flush_thread_function() {
        while(!stop_thread_.load()) {
            std::unique_lock<std::mutex> lock(lock_flush_mutex_);
            lock_flush_thread_cv_.wait(lock, [this]{
                return flush_last_lock_ != flush_first_lock_;
            });
            int64_t curr_flush_last_lock_;
            int64_t curr_free_size_;
            {
                std::scoped_lock<std::mutex> latch(lock_latch_);
                lock_rdma_buffer_->get_curr_tail_free_size_(curr_flush_last_lock_, curr_free_size_);
            }

            flush_locks(curr_flush_last_lock_, curr_free_size_);
        }
    }

    void log_flush_thread_function() {
        while(!stop_thread_.load()) {
            std::unique_lock<std::mutex> lock(log_flush_mutex_);
            log_flush_thread_cv_.wait(lock, [this] {
                return need_flush_offset_ != flushed_log_offset_;
            });
            int64_t curr_need_flush_offset;
            int64_t curr_head;
            int64_t curr_tail;
            {
                std::scoped_lock<std::mutex> latch(state_latch_);
                curr_need_flush_offset = need_flush_offset_;
                curr_head = curr_log_head_;
                curr_tail = curr_log_tail_;
            }
            flush_logs(curr_need_flush_offset, curr_head, curr_tail);
        }
    }

    static ContextManager* state_mgr_;

    std::atomic<bool> stop_thread_;
    std::thread log_flush_thread_;
    std::mutex log_flush_mutex_;
    std::condition_variable log_flush_thread_cv_;

    std::thread lock_flush_thread_;
    std::mutex lock_flush_mutex_;
    std::condition_variable lock_flush_thread_cv_;
};