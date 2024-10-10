#include "lock_manager.h"
#include "state/state_manager.h"

bool LockCheckState(Transaction *txn) {
    if(txn->get_state() == TransactionState::COMMITTED || 
        txn->get_state() == TransactionState::ABORTED) {
        return false;
    }

    if(txn->get_state() == TransactionState::DEFAULT) {
        txn->set_state(TransactionState::GROWING);
    }
    
    if(txn->get_state() != TransactionState::GROWING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
        return false;
    }

    return true;
}

bool UnlockCheckState(Transaction *txn) {
    if(txn->get_state() == TransactionState::COMMITTED ||
        txn->get_state() == TransactionState::ABORTED) {
        return false;
    }

    if(txn->get_state() == TransactionState::GROWING) {
        txn->set_state(TransactionState::SHRINKING);
    }

    return true;
}

LockRequestQueue* LockManager::get_record_request_queue_(const Rid& rid, LockListInBucket* lock_list) {
    LockRequestQueue* request_queue = lock_list->first_lock_queue_;
    while(request_queue != nullptr) {
        if(request_queue->record_no_ == rid.record_no) {
            return request_queue;
        }
        request_queue = request_queue->next_;
    }   

    return nullptr;
}

Lock* LockManager::check_record_lock_conflic(RecordLockType lock_type, LockMode lock_mode, LockRequestQueue* request_queue, Lock* req) {
    Lock* iterator = nullptr;
    
    switch(lock_type) {
        case RECORD_LOCK_ORDINARY: {
            if(lock_mode == LOCK_S) {
                // if there are other write/insert_intention locks, cannot upgrade lock
                iterator = request_queue->request_queue_->next_;
                while(iterator != nullptr) {
                    if(req != nullptr && iterator->trx_id_ == req->trx_id_) {
                        iterator = iterator->next_;
                        continue;
                    }

                    if(iterator->is_insert_intention()) {
                        return iterator;
                    }
                    else if(iterator->get_record_lock_mode(RECORD_LOCK_GAP) & (1 << LOCK_X)) {
                        return iterator;
                    }
                    else if(iterator->get_record_lock_mode(RECORD_LOCK_ORDINARY) & (1 << LOCK_X)) {
                        return iterator;
                    }
                    else if(iterator->get_record_lock_mode(RECORD_LOCK_REC_NOT_GAP) & (1 << LOCK_X)) {
                        return iterator;
                    }
                    iterator = iterator->next_;
                }
            }
            else {
                // if there are other locks, cannot upgrade lock
                iterator = request_queue->request_queue_->next_;
                while(iterator != nullptr) {
                    if(req != nullptr && iterator->trx_id_ == req->trx_id_) {
                        iterator = iterator->next_;
                        continue;
                    }
                    return iterator;
                }
            }
        } break;
        case RECORD_LOCK_GAP: {
            if(lock_mode == LOCK_S) {
                // if there are other write gap/write next_key/insert_intention locks, cannot upgrade lock
                iterator = request_queue->request_queue_->next_;
                while(iterator != nullptr) {
                    if(req != nullptr && iterator->trx_id_ == req->trx_id_) {
                        iterator = iterator->next_;
                        continue;
                    }

                    if(iterator->is_gap() && (iterator->get_record_lock_mode(RECORD_LOCK_GAP) & (1 << LOCK_X))) {
                        return iterator;
                    }
                    else if(iterator->is_next_key_lock() && (iterator->get_record_lock_mode(RECORD_LOCK_ORDINARY) & (1 << LOCK_X))) {
                        return iterator;
                    }
                    else if(iterator->is_insert_intention()) {
                        return iterator;
                    }
                    iterator = iterator->next_;
                }
            }
            else {
                // if there are other gap/next_key/insert_intention locks, cannot upgrade lock
                iterator = request_queue->request_queue_->next_;
                while(iterator != nullptr) {
                    if(req != nullptr && iterator->trx_id_ == req->trx_id_) {
                        iterator = iterator->next_;
                        continue;
                    }

                    if(iterator->is_gap() || iterator->is_next_key_lock() || iterator->is_insert_intention()) {
                        return iterator;
                    }
                    iterator = iterator->next_;
                }
            }
        } break;
        case RECORD_LOCK_REC_NOT_GAP: {
            if(lock_mode == LOCK_S) {
                // if there are other write rec_not_gap/write next_key locks, cannot upgrade lock
                iterator = request_queue->request_queue_->next_;
                while(iterator != nullptr) {
                    if(req != nullptr && iterator->trx_id_ == req->trx_id_) {
                        iterator = iterator->next_;
                        continue;
                    }

                    if(iterator->is_record_not_gap() && (iterator->get_record_lock_mode(RECORD_LOCK_REC_NOT_GAP) & (1 << LOCK_X))) {
                        return iterator;
                    }
                    else if(iterator->is_next_key_lock() && (iterator->get_record_lock_mode(RECORD_LOCK_ORDINARY) & (1<< LOCK_X))) {
                        return iterator;
                    }
                    else if(iterator->is_insert_intention()) {
                        return iterator;
                    }
                    iterator = iterator->next_;
                }
            }
            else {
                // if there are other rec_not_gap/next_key locks, cannot upgrade lock
                iterator = request_queue->request_queue_->next_;
                while(iterator != nullptr) {
                    if(req != nullptr && iterator->trx_id_ == req->trx_id_) {
                        iterator = iterator->next_;
                        continue;
                    }

                    if(iterator->is_next_key_lock() || iterator->is_record_not_gap()) {
                        return iterator;
                    }
                    iterator = iterator->next_;
                }
            }
        } break;
        case RECORD_LOCK_INSERT_INTENTION: {
            // if there are gap/next_key locks
            iterator = request_queue->request_queue_->next_;
            while(iterator != nullptr) {
                if(req != nullptr && iterator->trx_id_ == req->trx_id_) {
                        iterator = iterator->next_;
                        continue;
                    }

                if(iterator->is_gap() || iterator->is_next_key_lock() ) {
                    return iterator;
                }
                iterator = iterator->next_;
            }
        }
        default: break;
    }
    return nullptr;
}

Lock* LockManager::upgrade_record_lock_type_mode(RecordLockType lock_type, LockMode lock_mode, LockRequestQueue* request_queue, Lock* req, int thread_index) {
    std::unique_lock<std::mutex> lock(latch_);

    Lock* tmp = check_record_lock_conflic(lock_type, lock_mode, request_queue, req);
    if(tmp != nullptr) {
        // std::cout << "transaction " << req->trx_id_ << " failed request record lock on {table_id=" << req->table_id_ << ", record_no=" << req->record_no_ << \
        //  ", because transasction " << tmp->trx_id_ << " hold the lock\n";
        throw TransactionAbortException(req->trx_id_, AbortReason::UPGRADE_CONFLICT);
    }
    switch(lock_type) {
        case RECORD_LOCK_ORDINARY: {
            req->type_mode_ |= LOCK_ORDINARY;
            req->set_record_lock_mode(lock_type, lock_mode);
        } break;
        case RECORD_LOCK_GAP: {
            req->type_mode_ |= LOCK_GAP;
            req->set_record_lock_mode(lock_type, lock_mode);
        } break;
        case RECORD_LOCK_REC_NOT_GAP: {
            req->type_mode_ |= LOCK_REC_NOT_GAP;
            req->set_record_lock_mode(lock_type, lock_mode);
        } break;
        case RECORD_LOCK_INSERT_INTENTION: {
            req->type_mode_ |= LOCK_INSERT_INTENTION;
        }
        default: break;
    }

    // @STATE:
    if(state_open_)
        ContextManager::get_instance()->append_lock_state(req, thread_index);

    return req;
}

Lock* LockManager::request_record_lock(int table_id, const Rid& rid, Transaction* txn, RecordLockType lock_type, LockMode lock_mode, int thread_index) {
    // std::cout << "request_record_lock: table_id: " << table_id << ", rid.record_no = " << rid.record_no << ", lock_type: " << LockTypeStr[lock_type] << ", lock_mode: " << LockModeStr[lock_mode] << "trx_id: " << txn->get_transaction_id() << "\n";
    if(node_type_ == 1) {
        return nullptr;
    }

    std::unique_lock<std::mutex> lock(latch_);
    
    if(LockCheckState(txn) == false) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    LockDataId lock_data_id(table_id, rid.record_no, LockDataType::RECORD);
    Lock* lock_request = new Lock(txn->get_transaction_id());
    lock_request->table_id_ = table_id;
    lock_request->record_no_ = rid.record_no;
    // initiate lock's type_mode_
    switch(lock_type) {
        case RECORD_LOCK_ORDINARY: {
            lock_request->type_mode_ = (LOCK_ORDINARY | (1 << (lock_mode + BIT_LOCK_MODE_ORDINARY)));
        } break;
        case RECORD_LOCK_GAP: {
            lock_request->type_mode_ = (LOCK_GAP | (1 << (lock_mode + BIT_LOCK_MODE_GAP)));
        } break;
        case RECORD_LOCK_REC_NOT_GAP: {
            lock_request->type_mode_ = (LOCK_REC_NOT_GAP | (1 << (lock_mode + BIT_LOCK_MODE_REC_NOT_GAP)));
        } break;
        case RECORD_LOCK_INSERT_INTENTION: {
            lock_request->type_mode_ = (LOCK_INSERT_INTENTION);
        } break;
        default: break;
    }

    // LockRequestQueue* request_queue = &lock_table_.find(lock_data_id)->second;
    if(lock_table_.count(lock_data_id) == 0) {
        // no locks on the current page, intiate it in lock table and append lock directly
        LockRequestQueue* request_queue = new LockRequestQueue(rid.record_no, lock_request);
        lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lock_data_id), 
                            std::forward_as_tuple());
        LockListInBucket* lock_list_in_page = &lock_table_.find(lock_data_id)->second;
        lock_list_in_page->bucket_id_ = rid.record_no / BUCKET_SIZE;
        lock_list_in_page->first_lock_queue_ = request_queue;

        // @STATE
        if(state_open_)
            ContextManager::get_instance()->append_lock_state(lock_request, thread_index);

        return lock_request;
    }

    LockListInBucket* lock_list_in_page = &lock_table_.find(lock_data_id)->second;
    LockRequestQueue* request_queue = get_record_request_queue_(rid, lock_list_in_page);

    if(request_queue == nullptr) {
        // there are locks on the current page, but no locks on the current record, append lock directly
        request_queue = new LockRequestQueue(rid.record_no, lock_request);
        request_queue->next_ = lock_list_in_page->first_lock_queue_;
        lock_list_in_page->first_lock_queue_ = request_queue;

        //@STATE:
        if(state_open_)
            ContextManager::get_instance()->append_lock_state(lock_request, thread_index);  
        return lock_request;
    }

    Lock* req = request_queue->request_queue_->next_;
    while(req != nullptr) {
        if(req->trx_id_ == txn->get_transaction_id()) {
            if(lock_type == RECORD_LOCK_INSERT_INTENTION && req->is_insert_intention()) {
                delete lock_request;
                return req;
            }
            else if(req->is_contained_in_current_type(lock_type, 1 << lock_mode)) {
                delete lock_request;
                return req;
            }
            else {
                delete lock_request;
                lock.unlock();
                return upgrade_record_lock_type_mode(lock_type, lock_mode, request_queue, req, thread_index);
            }
        }
        req = req->next_;
    }

    // no acuiqred locks of the txn, try to request lock
    Lock* tmp = check_record_lock_conflic(lock_type, lock_mode, request_queue, nullptr);
    if(tmp != nullptr) {
        // std::cout << "transaction " << lock_request->trx_id_ << " failed request record lock on {table_id=" << lock_request->table_id_ << ", record_no=" << lock_request->record_no_ << \
        //  ", because transasction " << tmp->trx_id_ << " hold the lock, record_no=" << tmp->record_no_ << "\n";
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    // no conflicts, add lock request into queue
    lock_request->next_ = request_queue->request_queue_->next_;
    lock_request->prev_ = request_queue->request_queue_;
    if(request_queue->request_queue_->next_ != nullptr) {
        request_queue->request_queue_->next_->prev_ = lock_request;
    }
    request_queue->request_queue_->next_ = lock_request;

    // @STATE:
    if(state_open_)
        ContextManager::get_instance()->append_lock_state(lock_request, thread_index);

    return lock_request;
}

Lock* LockManager::upgrade_table_lock_mode(LockMode lock_mode, LockRequestQueue* request_queue, Lock* req, int thread_index) {
    std::unique_lock<std::mutex> lock(latch_);

    Lock* iterator = request_queue->request_queue_->next_;
    while(iterator != nullptr) {
        if(iterator->trx_id_ == req->trx_id_) {
            iterator = iterator->next_;
            continue;
        }

        if(iterator->is_conflict_table_mode(lock_mode)) {
            throw TransactionAbortException(req->trx_id_, AbortReason::DEADLOCK_PREVENTION);
        }

        iterator = iterator->next_;
    }

    req->type_mode_ |= 1 << lock_mode;

    // @STATE:
    if(state_open_)
        ContextManager::get_instance()->append_lock_state(req, thread_index);

    return req;
}

Lock* LockManager::request_table_lock(int table_id, Transaction* txn, LockMode lock_mode, int thread_index) {
    // std::cout << "request_table_lock: table_id: " << table_id << ", lock_mode: " << LockModeStr[lock_mode] << "trx_id: " << txn->get_transaction_id() << "\n";
    std::unique_lock<std::mutex> lock(latch_);

    if(LockCheckState(txn) == false) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    LockDataId lock_data_id(table_id, LockDataType::TABLE);
    Lock* lock_request = new Lock(txn->get_transaction_id());
    lock_request->table_id_ = table_id;
    lock_request->record_no_ = -1;
    lock_request->type_mode_ = (LOCK_TABLE | (1U << (lock_mode + BIT_LOCK_MODE_TABLE)));

    if(lock_table_.count(lock_data_id) == 0) {
        LockRequestQueue* request_queue = new LockRequestQueue();
        request_queue->request_queue_->next_ = lock_request;
        lock_request->prev_ = request_queue->request_queue_;
        lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lock_data_id), 
                            std::forward_as_tuple());
        LockListInBucket* lock_list_in_page = &lock_table_.find(lock_data_id)->second;
        lock_list_in_page->bucket_id_ = -1;
        lock_list_in_page->first_lock_queue_ = request_queue;

        //@STATE:
        if(state_open_)
            ContextManager::get_instance()->append_lock_state(lock_request, thread_index);
        return lock_request;
    }

    LockListInBucket* lock_list_in_page = &lock_table_.find(lock_data_id)->second;
    LockRequestQueue* request_queue = lock_list_in_page->first_lock_queue_;

    if(request_queue == nullptr) {
        LockRequestQueue* request_queue = new LockRequestQueue();
        request_queue->request_queue_->next_ = lock_request;
        lock_request->prev_ = request_queue->request_queue_;
        lock_list_in_page->first_lock_queue_ = request_queue;

        // @STATE:
        if(state_open_)
            ContextManager::get_instance()->append_lock_state(lock_request, thread_index);
        return lock_request;
    }

    Lock* req = request_queue->request_queue_->next_;
    bool conflict = false;
    while(req != nullptr) {
        // std::cout << "req.trx_id: " << req->trx_id_ << ", request_trx_id: " << txn->get_transaction_id() << "\n";
        if(req->trx_id_ == txn->get_transaction_id()) {
            if(req->get_table_lock_mode() >= (1U << lock_mode)) {
                // std::cout << "current mode contains request mode\n";
                delete lock_request;
                return req;
            }
            else {
                // std::cout << "current mode need to upgrade\n";
                delete lock_request;
                lock.unlock();
                return upgrade_table_lock_mode(lock_mode, request_queue, req, thread_index);
            }
        }
        else {
            if(req->is_conflict_table_mode(lock_mode)) {
                // std::cout << "conflict: req.trx_id=" << req->trx_id_ << ", req.table_mode:" << req->get_table_lock_mode() << "\n";
                conflict = true;
                break;
            }
        }
        req = req->next_;
    }

    if(conflict == true) {
        delete lock_request;
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    lock_request->next_ = request_queue->request_queue_->next_;
    lock_request->prev_ = request_queue->request_queue_;
    if(request_queue->request_queue_->next_ != nullptr) {
        request_queue->request_queue_->next_->prev_ = lock_request;
    }
    request_queue->request_queue_->next_ = lock_request;

    //@STATE:
    if(state_open_)
        ContextManager::get_instance()->append_lock_state(lock_request, thread_index);
    
    return lock_request;
}

bool LockManager::unlock(Transaction* txn, Lock* lock) {
    std::unique_lock<std::mutex> latch(latch_);

    if(UnlockCheckState(txn) == false) {
        return false;
    }

    // if(lock->is_record_lock()) {
        // std::cout << "release_record_lock: table_id: " << lock->table_id_ << ", rid.record_no = " << lock->record_no_ << ", lock_type: " << LockTypeStr[lock->get_record_lock_type()] << ", lock_mode: " << lock->get_record_lock_mode_str(lock->get_record_lock_type()) << "trx_id: " << txn->get_transaction_id() << "\n";
    // }   
    // else {
        // std::cout << "release_table_lock: table_id: " << lock->table_id_ << ", lock_mode: " << lock->get_table_lock_mode_str() << "trx_id: " << txn->get_transaction_id() << "\n";
    // } 

    lock->prev_->next_ = lock->next_;
    if(lock->next_ != nullptr)
        lock->next_->prev_ = lock->prev_;
    
    // @STATE:
    // TODO:
    if(state_open_)
        ContextManager::get_instance()->erase_lock_state(lock);

    delete lock;

    return true;
}

void LockManager::recover_lock_table(Transaction** active_txn_list, int thread_num) {
    ContextManager::get_instance()->fetch_lock_states(&lock_table_, active_txn_list, thread_num);
}