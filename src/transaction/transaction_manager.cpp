#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"
#include "state/state_manager.h"

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    txn->clear();
    txn->set_transaction_id(next_txn_id_ ++);
    txn->set_state(TransactionState::DEFAULT);
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, Context* context) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    auto lock_set = txn->get_lock_set();

    for(auto iter = lock_set->begin(); iter != lock_set->end(); iter ++) {
        lock_manager_->unlock(txn, *iter);
    }

    txn->set_state(TransactionState::COMMITTED);

    context->log_mgr_->add_log_to_buffer(std::move(std::make_unique<CommitLogRecord>(txn->get_transaction_id())));

    context->log_mgr_->write_log_to_storage();

    // commit之后，将事务移出active_txns，txn->get_transaction_id()
    // auto it = std::find(active_txns_.begin(), active_txns_.end(), txn->get_transaction_id());
    // if (it != active_txns_.end()) {
    //     active_txns_.erase(it);
    // }

}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, Context* context) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    auto write_set = txn->get_write_set();

    while(!write_set->empty()) {
        auto& item = write_set->back();
        IxIndexHandle* pindex_handle = sm_manager_->primary_index_.at(item->table_name_).get();
        switch(item->wtype_) {
            case WType::INSERT_TUPLE: {
                auto rid = pindex_handle->lower_bound(item->pkey_);
                pindex_handle->delete_record(rid, context);
            } break;
            case WType::DELETE_TUPLE: {
                auto rid = pindex_handle->lower_bound(item->pkey_);
                pindex_handle->rollback_delete_record(rid, context);
            } break;
            case WType::UPDATE_TUPLE: {
                auto rid = pindex_handle->lower_bound(item->pkey_);
                pindex_handle->update_record(rid, item->record_.raw_data_, context);
            } break;
            default:
            break;
        }
        write_set->pop_back();
    }

    auto lock_set = txn->get_lock_set();

    for(auto iter = lock_set->begin(); iter != lock_set->end(); iter ++) {
        lock_manager_->unlock(txn, *iter);
    }

    txn->set_state(TransactionState::ABORTED);

    context->log_mgr_->add_log_to_buffer(std::move(std::make_unique<AbortLogRecord>(txn->get_transaction_id())));
    context->log_mgr_->write_log_to_storage();

    // abort之后，将事务移出active_txns，txn->get_transaction_id()
    // auto it = std::find(active_txns_.begin(), active_txns_.end(), txn->get_transaction_id());
    // if (it != active_txns_.end()) {
    //     active_txns_.erase(it);
    // }
}

void TransactionManager::recover_active_txn_lists(Context* context) {
    ContextManager::get_instance()->fetch_active_txns(active_transactions_, thread_num_);
}