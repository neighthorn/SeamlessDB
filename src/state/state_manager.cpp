#include "state_manager.h"
#include "state/coroutine/doorbell.h"
#include "common/macro.h"

#define get_lock_remote_offset(off, bitmap_size) off * LOCK_STATE_SIZE_REMOTE + bitmap_size

StateManager* StateManager::state_mgr_ = nullptr;

bool StateManager::create_instance() {
    if(state_mgr_ == nullptr)
        state_mgr_ = new (std::nothrow) StateManager();
    return (state_mgr_ == nullptr);
}

void StateManager::destroy_instance() {
    delete state_mgr_;
    state_mgr_ = nullptr;
}

void StateManager::append_lock_state(Lock* lock)  {
    // if(lock->offset_ == -1) {
    //     int lock_off = lock_bitmap_->get_first_free_bit();
    //     lock->offset_ = lock_off;
    // }
    // lock_rdma_buffer_->write_lock(lock);
    // lock_states_.push_back(lock);
}

void StateManager::erase_lock_state(Lock* lock) {
    // int offset = lock->offset_;
    // // lock_states_.remove_if(CompareLockPtr(lock));
    // lock_bitmap_->set_bit_to_free(offset);
}

void StateManager::flush_locks(int64_t free_size) {
    // int lock_num = lock_states_.size();
    // int req_num = 0;
    // int tot_size = lock_num_ * (sizeof(txn_id_t) + sizeof(uint32_t));
    // char* lock_buf = lock_rdma_buffer_->Alloc(tot_size);
    // int offset = 0;
    int lock_num;
    if(flush_first_lock_ >= flush_last_lock_ && free_size < lock_rdma_buffer_->buf_size_) {
        lock_num = (flush_last_lock_ + lock_rdma_buffer_->buf_size_ - flush_first_lock_) / LOCK_STATE_SIZE_LOCAL;
    }
    else {
        lock_num = (flush_last_lock_ - flush_first_lock_) / LOCK_STATE_SIZE_LOCAL;
        // std::cout << "0/LOCK_STATE_SIZE_LOCAL: " << 0/LOCK_STATE_SIZE_LOCAL << "\n";
        // std::cout << "0/20" << 0/20 << "\n";
        // std::cout << "flush_last_lock: " << flush_last_lock_ << ", flush_first_lock: " << flush_first_lock_ << "\n";
    }

    if(lock_num <= 0) return;

    LockWriteBatch* doorbell = new LockWriteBatch(lock_num);
    LockState* lock_state = new LockState();

    for(int offset = flush_first_lock_, i = 0; i < lock_num; ++i) {
        lock_rdma_buffer_->read_lock(lock_state, offset);

        doorbell->set_next_lock_write_req(lock_rdma_buffer_->buffer_ + offset, get_lock_remote_offset(lock_state->offset_, lock_bitmap_->bitmap_size_), LOCK_STATE_SIZE_REMOTE);

        offset += LOCK_STATE_SIZE_LOCAL;
        if(offset >= lock_rdma_buffer_->buf_size_) offset -= lock_rdma_buffer_->buf_size_;
    }

    if(lock_num > 0 && !doorbell->send_reqs(coro_sched_, lock_qp_, 0, lock_qp_->remote_mr_)) {
        RDMA_LOG(ERROR) << "Failed to flush batch lock_state operations into the state node.";
        assert(0);
    }

    lock_rdma_buffer_->release(flush_first_lock_, flush_last_lock_, lock_num * LOCK_STATE_SIZE_LOCAL);
    flush_first_lock_ = flush_last_lock_;
    // lock_states_.clear();
}

void StateManager::flush_logs(int64_t curr_state_tail, int64_t curr_head, int64_t curr_tail) {
    if(flushed_log_offset_ < curr_state_tail) {
        LogWriteBatch* doorbell = new LogWriteBatch();

        doorbell->set_log_write_req(log_rdma_buffer_->buffer_ + flushed_log_offset_, flushed_log_offset_, curr_state_tail - flushed_log_offset_);
        doorbell->set_head_write_req((char*)&curr_head, remote_log_head_off_);
        doorbell->set_tail_write_req((char*)&curr_tail, remote_log_tail_off_);
        doorbell->set_state_tail_write_req((char*)&curr_state_tail, remote_log_state_tail_off_);

        if(!doorbell->send_reqs(coro_sched_, log_qp_, 0, log_qp_->remote_mr_)) {
            RDMA_LOG(ERROR) << "Failed to flush batch log_state operations into the state node.";
            assert(0);
        }
    }
    else {
        LogWriteTwoRangeBatch* doorbell = new LogWriteTwoRangeBatch();
        int size1 = log_rdma_buffer_->buf_size_ - flushed_log_offset_;
        doorbell->set_log_write_req1(log_rdma_buffer_->buffer_ + flushed_log_offset_, flushed_log_offset_, size1);
        doorbell->set_log_write_req2(log_rdma_buffer_->buffer_, 0, curr_state_tail);
        doorbell->set_head_write_req((char*)&curr_head, remote_log_head_off_);
        doorbell->set_tail_write_req((char*)&curr_tail, remote_log_tail_off_);
        doorbell->set_state_tail_write_req((char*)&curr_state_tail, remote_log_state_tail_off_);

        if(!doorbell->send_reqs(coro_sched_, log_qp_, 0, log_qp_->remote_mr_)) {
            RDMA_LOG(ERROR) << "Failed to flush batch log_state operations into the state node.";
            assert(0);
        }
    }

    flushed_log_offset_ = curr_state_tail;
}

void StateManager::flush_states() {
    // only one flush_states operation can be invoked simultaneously
    std::unique_lock<std::mutex> latch(state_latch_);

    // log state second
    // flush_last_log_ = curr_log_offset_.load();
    log_rdma_buffer_->get_curr_tail_free_size_(curr_log_head_, curr_log_tail_);
    need_flush_offset_ = curr_log_tail_;


    int64_t curr_lock_free_size;
    // lock state third
    {
        std::lock_guard<std::mutex> lock_latch(lock_latch_);
        // flush_last_lock_ = std::prev(lock_states_.end());
        // flush_first_lock_ = lock_states_.begin();
        // lock_num_ = lock_states_.size();
        lock_rdma_buffer_->get_curr_tail_free_size_(flush_last_lock_, curr_lock_free_size);
        lock_bitmap_->get_curr_bitmap(lock_bitmap_rdma_buffer_);
        // std::cout << "flush_last_lock: " << flush_last_lock_ << ", flush_first_lock: " << flush_first_lock_ << "\n";
    }

    // write lock first
    flush_locks(curr_lock_free_size);
    coro_sched_->RDMAWriteSync(0, lock_qp_, lock_bitmap_rdma_buffer_, 0, lock_bitmap_->bitmap_size_);

    // write log second
    need_flush_offset_ = curr_log_tail_;

    // cursor state is flushed after invoking flush_states by the thread itself
}

// used for test
void StateManager::fetch_and_print_lock_states() {
    char* lock_region = RDMARegionAllocator::get_instance()->GetLockRegion().first;
    char* origin_bitmap = new char[lock_bitmap_->bitmap_size_];

    memcpy(origin_bitmap, lock_bitmap_rdma_buffer_, lock_bitmap_->bitmap_size_);
    memset(lock_bitmap_rdma_buffer_, 0, lock_bitmap_->bitmap_size_);
    coro_sched_->RDMAReadSync(0, lock_qp_, lock_bitmap_rdma_buffer_, 0, lock_bitmap_->bitmap_size_);

    puts("origin_bitmap: ");
    // print_char_array(origin_bitmap, lock_bitmap_->bitmap_size_);
    // print_bitmap(origin_bitmap, lock_bitmap_->bitmap_size_);
    // print_bitmap_valid_bits(origin_bitmap, lock_bitmap_->bitmap_size_);
    puts("");
    puts("readed bitmap from RDMA");
    // print_bitmap(lock_bitmap_rdma_buffer_, lock_bitmap_->bitmap_size_);
    // print_char_array(lock_bitmap_rdma_buffer_, lock_bitmap_->bitmap_size_);
    // print_bitmap_valid_bits(lock_bitmap_rdma_buffer_, lock_bitmap_->bitmap_size_);

    // std::vector<int> valid_bits = lock_bitmap_->get_all_valid_bits(lock_bitmap_rdma_buffer_, lock_bitmap_->bitmap_size_);
    // LockState* lock = new LockState();
    // for(auto bit: valid_bits) {
    //     coro_sched_->RDMAReadSync(0, lock_qp_, lock_region, get_lock_remote_offset(bit, lock_bitmap_->bitmap_size_), LOCK_STATE_SIZE_REMOTE);
    //     memcpy(lock, lock_region, LOCK_STATE_SIZE_REMOTE);
    //     std::cout << "lock trx_id: " << lock->trx_id_ << ", table_id: " << lock->table_id_ << ", record_no:" << lock->record_no_ << "\n";
    // }
}

LockRequestQueue* StateManager::get_record_request_queue_(int record_no, LockListInBucket* lock_list) {
    LockRequestQueue* request_queue = lock_list->first_lock_queue_;
    while(request_queue != nullptr) {
        if(request_queue->record_no_ == record_no) {
            return request_queue;
        }
        request_queue = request_queue->next_;
    }   

    return nullptr;
}

void StateManager::fetch_lock_states(std::unordered_map<LockDataId, LockListInBucket>* lock_table) {
    char* lock_region = RDMARegionAllocator::get_instance()->GetLockRegion().first;
    memset(lock_bitmap_rdma_buffer_, 0, lock_bitmap_->bitmap_size_);
    coro_sched_->RDMAReadSync(0, lock_qp_, lock_bitmap_rdma_buffer_, 0, lock_bitmap_->bitmap_size_);

    std::vector<int> valid_bits = lock_bitmap_->get_all_valid_bits(lock_bitmap_rdma_buffer_, lock_bitmap_->bitmap_size_);
    // LockState* lock = new LockState();
    for(auto bit: valid_bits) {
        Lock* lock = new Lock();
        coro_sched_->RDMAReadSync(0, lock_qp_, lock_region, get_lock_remote_offset(bit, lock_bitmap_->bitmap_size_), LOCK_STATE_SIZE_REMOTE);
        memcpy(lock, lock_region, LOCK_STATE_SIZE_REMOTE);
        lock->offset_ = bit;
        if(lock->record_no_ == -1) {
            // table lock
            LockDataId lock_data_id(lock->table_id_, LockDataType::TABLE);
            if(lock_table->count(lock_data_id) == 0) {
                LockRequestQueue* request_queue = new LockRequestQueue();
                request_queue->request_queue_->next_ = lock;
                lock->prev_ = request_queue->request_queue_;
                lock_table->emplace(std::piecewise_construct, std::forward_as_tuple(lock_data_id), 
                                    std::forward_as_tuple());
                LockListInBucket* lock_list_in_page = &lock_table->find(lock_data_id)->second;
                lock_list_in_page->bucket_id_ = -1;
                lock_list_in_page->first_lock_queue_ = request_queue;
                continue;
            }
            LockListInBucket* lock_list_in_page = &lock_table->find(lock_data_id)->second;
            LockRequestQueue* request_queue = lock_list_in_page->first_lock_queue_;
            if(request_queue == nullptr) {
                LockRequestQueue* request_queue = new LockRequestQueue();
                request_queue->request_queue_->next_ = lock;
                lock->prev_ = request_queue->request_queue_;
                lock_list_in_page->first_lock_queue_ = request_queue;
            }
            else {
                lock->next_ = request_queue->request_queue_->next_;
                lock->prev_ = request_queue->request_queue_;
                if(request_queue->request_queue_->next_ != nullptr) {
                    request_queue->request_queue_->next_->prev_ = lock;
                }
                request_queue->request_queue_->next_ = lock;
            }
        }
        else {
            // record lock
            LockDataId lock_data_id(lock->table_id_, lock->record_no_, LockDataType::RECORD);
            if(lock_table->count(lock_data_id) == 0) {
                LockRequestQueue* request_queue = new LockRequestQueue(lock->record_no_, lock);
                lock_table->emplace(std::piecewise_construct, std::forward_as_tuple(lock_data_id), 
                            std::forward_as_tuple());
                LockListInBucket* lock_list_in_page = &lock_table->find(lock_data_id)->second;
                lock_list_in_page->bucket_id_ = lock->record_no_ / BUCKET_SIZE;
                lock_list_in_page->first_lock_queue_ = request_queue;
                continue;
            }
            LockListInBucket* lock_list_in_page = &lock_table->find(lock_data_id)->second;
            LockRequestQueue* request_queue = get_record_request_queue_(lock->record_no_, lock_list_in_page);
            if(request_queue == nullptr) {
                request_queue = new LockRequestQueue(lock->record_no_, lock);
                request_queue->next_ = lock_list_in_page->first_lock_queue_;
                lock_list_in_page->first_lock_queue_ = request_queue;
            }
            else {
                lock->next_ = request_queue->request_queue_->next_;
                lock->prev_ = request_queue->request_queue_;
                if(request_queue->request_queue_->next_ != nullptr) {
                    request_queue->request_queue_->next_->prev_ = lock;
                }
                request_queue->request_queue_->next_ = lock;
            }
        }
        // std::cout << "lock trx_id: " << lock->trx_id_ << ", table_id: " << lock->table_id_ << ", record_no:" << lock->record_no_ << "\n";
    }
}