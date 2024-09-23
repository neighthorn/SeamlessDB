#pragma once

#include <mutex>
#include "common/config.h"
#include "common/macro.h"
#include "transaction/concurrency/lock.h"
#include "recovery/redo_log/redo_log.h"
#include "thirdparty/rlib/logging.hpp"

/**
 * RDMABufferAllocator is used to cache temporary data structures that is writing into remote StateNode
 * Different from the RDMARegionAllocator that is used to manage global_data_buffer, 
 * the RDMABufferAllocator is to manager each thread's local data_buffer.
 * Each thread has a __thread_local RDMABufferAllocator.
*/
class RDMABufferAllocator {
public:
    RDMABufferAllocator(char* _start, char* _end)
        : start(_start), end(_end), curr_offset(0) {}
    
    ALWAYS_INLINE
    char* Alloc(size_t size) {
        if(unlikely(start + curr_offset + size > end)) {
            curr_offset = 0;
        }

        char* res = start + curr_offset;
        curr_offset += size;
        return res;
    }

    ALWAYS_INLINE
    void Free(char*) {
        
    }

private:
    char* start;
    char* end;
    uint64_t curr_offset;
};

/*
    实现free
*/
class RDMABufferAlloc {
public:
    RDMABufferAlloc(char *_start, char *_end) :
        start_(_start), end_(_end), max_size_(end_ - start_), 
        curr_write_offset_(0), curr_read_offset_(0), free_space_(max_size_){}

    /*

    */
    ALWAYS_INLINE
    std::pair<bool, char*> Alloc(size_t size) {
        std::scoped_lock<std::mutex> lock(latch_);
        /*
            size == 0, return false;
        */
        if(size == 0) {
            return {false, nullptr};
        }

        if(size > max_size_) {
            std::cerr << "[Error]: exceed max size! size: " << size << ", max_size: " << max_size_ << "\n";
            return {false, nullptr};
        }

        /*
            back curr_write_offset and free space
        */
        size_t curr_write_offset_bak = curr_write_offset_;
        size_t free_space_bak = free_space_;

        /*
            找到起始位置
        */ 
        if(curr_write_offset_ + size > max_size_) {
            if(max_size_ - curr_write_offset_ > free_space_) {
                return {false, nullptr};
            }
            free_space_ -= (max_size_ - curr_write_offset_);
            curr_write_offset_ = 0;
        }

        /*
            free space < size, return false, no enough space
        */
        if(free_space_ < size) {
            curr_write_offset_  = curr_write_offset_bak;
            free_space_         = free_space_bak;
            return {false, nullptr};
        }

        /*
            当前写入位置合适
        */
        size_t write_offset = curr_write_offset_;
        free_space_ -= size;
        curr_write_offset_ = (curr_write_offset_ + size) % max_size_;  

        return std::make_pair(true, start_ + write_offset);
    }

    ALWAYS_INLINE
    void Free(size_t size) {
        std::scoped_lock<std::mutex> lock(latch_);
        /*
            检查异常情况
        */
        if(size == 0) {
            return ;
        }
        if(size > max_size_) {
            std::cerr << "[Error]: exceed max size! [Location]: " << __FILE__  << ":" << __LINE__ << std::endl;
            return ;
        }

        /*
            备份
        */
        size_t curr_read_offset_bak = curr_read_offset_;
        size_t free_space_bak       = free_space_;

        /*
            获取当前读取位置
        */
        if(curr_read_offset_ + size > max_size_) {
            free_space_ += (max_size_ - curr_read_offset_);
            curr_read_offset_ = 0;
        }

        /*
            free失败
        */
        if(free_space_ + size > max_size_) {
            curr_read_offset_ = curr_read_offset_bak;
            free_space_ = free_space_bak;
            return ;
        }
        
        /*
            free成功
        */
        free_space_ += size;
        curr_read_offset_ = (curr_read_offset_ + size) % max_size_;

        if(curr_read_offset_ == curr_write_offset_) {
            assert(free_space_ == max_size_);
            curr_read_offset_ = 0;
            curr_write_offset_ = 0;
        }

        return ;
    }

    inline size_t getFreeSpace() {
        return free_space_;
    }

    inline size_t getUsedSpace() {
        return max_size_ - free_space_;
    }

    inline size_t get_curr_write_offset() {
        return curr_write_offset_;
    }
    
    inline size_t get_curr_read_offset() {
        return curr_read_offset_;
    }

private:
    char    *start_;
    char    *end_;

    size_t  max_size_;
    size_t  curr_write_offset_;
    size_t  curr_read_offset_;

    size_t  free_space_;

    std::mutex latch_;
};

class LockState{
public:
    txn_id_t trx_id_;   // transaction id
    uint32_t type_mode_;    // the lock type and mode bit flags
    int table_id_;
    int32_t record_no_;
    int offset_;
};

class RDMACircularBuffer {
public:
    RDMACircularBuffer(char* buf, int64_t buffer_size) {
        buffer_ = buf;
        buf_size_ = buffer_size;
        head_ = tail_ = 0;
        free_size_ = buf_size_;
    }

    void write(char* data, int64_t len) {
        // std::cout << "len: " << len << ", free_size: " << free_size_ << ", tail: " << tail_ << ", head: " << head_ << "\n";
        assert(len <= free_size_);
        int curr_tail;
        {
            std::lock_guard<std::mutex> latch(latch_);
            assert(len <= free_size_);
            curr_tail = tail_;
            tail_ = (tail_ + len) % buf_size_;
            free_size_ -= len;
        }

        if(curr_tail + len > buf_size_) {
            memcpy(buffer_ + curr_tail, data, buf_size_ - curr_tail);
            int64_t first_write_size = buf_size_ - curr_tail;
            memcpy(buffer_, data + first_write_size, len - first_write_size);
        }
        else {
            memcpy(buffer_ + curr_tail, data, len);
        }
    }

    void write_lock(Lock* lock) {
        char* src = reinterpret_cast<char*>(lock);
        write(src, LOCK_STATE_SIZE_LOCAL);
    }

    void write_log(RedoLogRecord* redolog) {
        int curr_tail;
        {
            std::lock_guard<std::mutex> latch(latch_);
            curr_tail = tail_;
            tail_ = (tail_ + redolog->log_tot_len_) % buf_size_;
            free_size_ -= redolog->log_tot_len_;
            // std::cout << "write log: begin = " << curr_tail << ", end = " << tail_ << "\n";
        }

        if(curr_tail + redolog->log_tot_len_ > buf_size_) {
            char* dest = new char[redolog->log_tot_len_];
            redolog->serialize(dest);
            int64_t first_write_size = buf_size_ - curr_tail;
            memcpy(buffer_ + curr_tail, dest, first_write_size);
            memcpy(buffer_, dest + first_write_size, redolog->log_tot_len_ - first_write_size);
            delete[] dest;
        }
        else {
            redolog->serialize(buffer_ + curr_tail);
            // char* tmp = buffer_ + curr_tail;
            // print_char_array(tmp, redolog->log_tot_len_);
        }
    }

    void read_lock(LockState* lock_state, int offset) {
        if(offset + LOCK_STATE_SIZE_LOCAL > buf_size_) {
            int64_t first_read_size = buf_size_ - offset;
            memcpy(lock_state, buffer_ + offset, first_read_size);
            memcpy(reinterpret_cast<char*>(lock_state) + first_read_size, buffer_, LOCK_STATE_SIZE_LOCAL - first_read_size);
        }
        else {
            memcpy(lock_state, buffer_ + offset, LOCK_STATE_SIZE_LOCAL);
        }
    }

    void get_curr_tail_free_size_(int64_t& get_tail, int64_t& get_free_size) {
        std::lock_guard<std::mutex> latch(latch_);
        // int curr_tail = tail_;
        // return curr_tail;
        get_tail = tail_;
        get_free_size = free_size_;
    }

    void get_curr_head_tail(int64_t& get_head, int64_t& get_tail) {
        std::lock_guard<std::mutex> latch(latch_);
        get_head = head_;
        get_tail = tail_;
    }

    void release(int64_t head, int64_t tail, int64_t size) {
        // std::cout << "head: " << head << ", head_: " << head_ << ", tail: " << tail << ", tail_: " << tail_ << "\n";
        assert(head == head_);
        std::lock_guard<std::mutex> latch(latch_);
        head_ = tail;
        free_size_ += size;
    }

    std::string get_range_string(const int64_t& head, const int64_t& tail, const int64_t& size) {
        if(head < tail) {
            return std::move(std::string(buffer_ + head, size));
        }
        else {
            int size1 = buf_size_ - head_;
            std::string str1(buffer_ + head_, size1);
            std::string str2(buffer_, size - size1);
            return std::move(str1 + str2);
        }
    }

    RedoLogRecord* read_log(int64_t& head, int persist_lsn) {
        RedoLogRecord* redo_log_hdr = new RedoLogRecord();
        int hdr_tail = (head + REDO_LOG_HEADER_SIZE) % buf_size_;

        // std::cout << "read log, head = " << head << "\n";
        // char* tmp = buffer_ + head;
        // print_char_array(tmp, REDO_LOG_HEADER_SIZE);

        if(head > tail_) assert(0);

        if(head < hdr_tail) {
            // memcpy(redo_log_hdr, buffer_ + head, REDO_LOG_HEADER_SIZE);
            redo_log_hdr->deserialize(buffer_ + head);
        }
        else {
            int size1 = buf_size_ - head;
            char* range_string = new char[REDO_LOG_HEADER_SIZE];
            memcpy(range_string, buffer_ + head, size1);
            memcpy(range_string + size1, buffer_, REDO_LOG_BUFFER_SIZE - size1);
            redo_log_hdr->deserialize(range_string);
        }

        if (persist_lsn >= redo_log_hdr->lsn_) {
            head = (redo_log_hdr->log_tot_len_ + head) % buf_size_;
            delete redo_log_hdr;
            return nullptr;
        }

        int log_tail = head + redo_log_hdr->log_tot_len_;
        switch(redo_log_hdr->log_type_) {
            case RedoLogType::UPDATE: {
                UpdateRedoLogRecord* update_redo_log = new UpdateRedoLogRecord();
                if(log_tail < head) {
                    std::string tmp = std::move(get_range_string(head, log_tail, redo_log_hdr->log_tot_len_));
                    update_redo_log->deserialize(tmp.c_str());
                }
                else {
                    update_redo_log->deserialize(buffer_ + head);
                }
                head += redo_log_hdr->log_tot_len_;
                delete redo_log_hdr;
                return update_redo_log;
            } break;
            case RedoLogType::DELETE: {
                DeleteRedoLogRecord* delete_redo_log = new DeleteRedoLogRecord();
                if(log_tail < head) {
                    std::string tmp = std::move(get_range_string(head, log_tail, redo_log_hdr->log_tot_len_));
                    delete_redo_log->deserialize(tmp.c_str());
                }
                else {
                    delete_redo_log->deserialize(buffer_ + head);
                }
                head += redo_log_hdr->log_tot_len_;
                delete redo_log_hdr;
                return delete_redo_log;
            } break;
            case RedoLogType::INSERT: {
                InsertRedoLogRecord* insert_redo_log = new InsertRedoLogRecord();
                if(log_tail < head) {
                    std::string tmp = std::move(get_range_string(head, log_tail, redo_log_hdr->log_tot_len_));
                    insert_redo_log->deserialize(tmp.c_str());
                }
                else {
                    insert_redo_log->deserialize(buffer_ + head);
                }
                head += redo_log_hdr->log_tot_len_;
                delete redo_log_hdr;
                return insert_redo_log;
            } break;
            default:
            std::cout << "Invalid log type\n";
            return nullptr;
            break;
        }
    }

    char* buffer_;
    int64_t buf_size_;
    int64_t head_;
    int64_t tail_;
    int64_t free_size_;
    std::mutex latch_;
};