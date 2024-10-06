#include <cstring>
#include "log_manager.h"
#include "state/state_manager.h"
#include "storage/storage_service.pb.h"

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
lsn_t LogManager::add_log_to_buffer(std::unique_ptr<RedoLogRecord> redo_log) {
    std::unique_lock<std::mutex> lock(latch_);
    
    // if(log_buffer_->is_full(redo_log->log_tot_len_)) {
    //     // 如果剩余空间无法写入新增log，则需要把日志缓冲区刷入磁盘中
    //     lock.unlock();
    //     write_log_to_storage();
    //     lock.lock();
    // }

    redo_log->lsn_ = global_lsn_ ++;
    // redo_log->serialize(log_buffer_->buffer_ + log_buffer_->offset_);
    log_buffer_->write_log(redo_log.get());
    // log_buffer_->offset_ += redo_log->log_tot_len_;

    return redo_log->lsn_;    
}


void LogManager::write_log_to_storage() {
    std::unique_lock<std::mutex> lock(latch_);
    // init brpc
    storage_service::StorageService_Stub stub(log_channel_);
    storage_service::LogWriteRequest request;
    storage_service::LogWriteResponse* response = new storage_service::LogWriteResponse;
    brpc::Controller* cntl = new brpc::Controller;
    brpc::CallId cid = cntl->call_id();

    // char *log_data = new char[log_buffer_->offset_];
    int64_t curr_head;
    int64_t curr_tail;
    log_buffer_->get_curr_head_tail(curr_head, curr_tail);
    int64_t curr_size = (curr_tail - curr_head + log_buffer_->buf_size_) % log_buffer_->buf_size_;
    if(curr_size == 0) return;
    request.set_log(std::move(log_buffer_->get_range_string(curr_head, curr_tail, curr_size)));
    // switch (redo_log->log_type_)
    // {
    //     case RedoLogType::UPDATE: {
    //         auto update_log = reinterpret_cast<UpdateRedoLogRecord *>(redo_log.get());
    //         update_log->serialize(log_data);
    //         break;
    //     }
    //     case RedoLogType::DELETE: {
    //         auto delete_log = reinterpret_cast<DeleteRedoLogRecord *>(redo_log.get());
    //         delete_log->serialize(log_data);
    //         break;
    //     }
    //     case RedoLogType::INSERT: {
    //         auto insert_log = reinterpret_cast<InsertRedoLogRecord *>(redo_log.get());
    //         insert_log->serialize(log_data);
    //         break;
    //     }
    //     default: {
    //         std::cout << "Haven't implemented!\n";
    //         break;
    //     }
    // }
    // request.set_log(std::string(log_data, redo_log->log_tot_len_));

    // std::cout << "try to send log to storage by brpc\n";
    stub.LogWrite(cntl, &request, response, NULL);
    log_buffer_->release(curr_head, curr_tail, curr_size);

    // StateManager::get_instance()->update_log_state_head();
    // std::cout << "successfully send log to storage\n";
}


void LogManager::make_update_redolog(txn_id_t txn_id, RmRecord &old_record, RmRecord &new_record, Rid rid, std::string tab_name, bool is_persist) {
    auto update_redolog = std::make_unique<UpdateRedoLogRecord>(txn_id, old_record, new_record, rid, tab_name);
    // update_redolog->lsn_ = global_lsn_++;
    update_redolog->is_persisit_ = is_persist;

    // sent to storage node
    add_log_to_buffer(std::move(update_redolog));
}

void LogManager::make_delete_redolog(txn_id_t txn_id, RmRecord &delete_record, Rid rid, std::string table_name, bool is_persist) {
    auto delete_redolog = std::make_unique<DeleteRedoLogRecord>(txn_id, delete_record, rid, table_name);
    // delete_redolog->lsn_ = global_lsn_++;
    delete_redolog->is_persisit_ = is_persist;

    add_log_to_buffer(std::move(delete_redolog));
}

void LogManager::make_insert_redolog(txn_id_t txn_id, char *key, int key_size, RmRecord &insert_record ,Rid rid, std::string tab_name, bool is_persist) {
    auto insert_redolog = std::make_unique<InsertRedoLogRecord>(txn_id, key, key_size, insert_record, rid, tab_name);
    // insert_redolog->lsn_ = global_lsn_++;
    insert_redolog->is_persisit_ = is_persist;

    add_log_to_buffer(std::move(insert_redolog));
}
