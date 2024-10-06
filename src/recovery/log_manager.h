#pragma once

#include <brpc/channel.h>

#include <mutex>
#include <vector>

#include "state/allocator/buffer_allocator.h"
#include "redo_log/redo_log.h"

/* 日志缓冲区，只有一个buffer，因此需要阻塞地去把日志写入缓冲区中 */
// class LogBuffer {
// public:
//     LogBuffer() { 
//         offset_ = 0; 
//         memset(buffer_, 0, sizeof(buffer_));
//     }

//     bool is_full(int append_size) {
//         if(offset_ + append_size > LOG_BUFFER_SIZE)
//             return true;
//         return false;
//     }

//     void clear() {
//         offset_ = 0;
//     }

//     char buffer_[LOG_BUFFER_SIZE+1];
//     int offset_;    // 写入log的offset
// };

/* 日志管理器，负责把日志写入日志缓冲区，以及把日志缓冲区中的内容写入磁盘中 */
class LogManager {
public:
    LogManager(brpc::Channel* log_channel, RDMACircularBuffer* log_buffer): log_channel_(log_channel) {
        log_buffer_ = log_buffer;
    }
    
    lsn_t add_log_to_buffer(std::unique_ptr<RedoLogRecord> redo_log);

    RDMACircularBuffer* get_log_buffer() { return log_buffer_; }

    // make redo log
    void make_update_redolog(txn_id_t txn_id, RmRecord &old_record, RmRecord &new_record, Rid rid, std::string tab_name, bool is_persist = false);

    void make_delete_redolog(txn_id_t txn_id, RmRecord &delete_record, Rid rid, std::string tab_name, bool is_persist = false);

    void make_insert_redolog(txn_id_t txn_id, char *key, int key_size, RmRecord &insert_record ,Rid rid, std::string tab_name, bool is_persist = false);

    void write_log_to_storage();

private:    
    std::atomic<lsn_t>      global_lsn_{0};  // 全局lsn，递增，用于为每条记录分发lsn
    std::mutex              latch_;          // 用于对log_buffer_的互斥访问
    RDMACircularBuffer*     log_buffer_;     // 日志缓冲区
    lsn_t                   persist_lsn_;    // 记录已经持久化到磁盘中的最后一条日志的日志号
    brpc::Channel*          log_channel_;
    DiskManager*            disk_manager_;
}; 
