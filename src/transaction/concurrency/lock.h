#pragma once

#ifndef LOCK_H
#define LOCK_H

#include <condition_variable>

#include "record/record.h"
#include "common/config.h"
#include "common/common.h"

/**
 * 0-3 LOCK_MODE(LOCK_IS, LOCK_IX, LOCK_S, LOCK_X) 
 * 4-7 LOCK_TYPE(4: 1 TABLE_LOCK 0 REC_LOC)
 * 8   LOCK_WAIT
 * 9-31 REC_LOCK_TYPE
*/

/**
 * 0     table/rec lock type (1: table lock, 0 record lock)
 * 1-4   table lock mode (LockMode)
 * 5     lock wait
 * 6     next_key lock
 * 7-10  next_key lock mode
 * 11    gap lock
 * 12-15 gap lock mode
 * 16    rec_not_gap lock
 * 17-20 rec_not_gap lock mode
 * 21    insert_intention lock
*/

#define LOCK_TABLE 1
// (type_mode >> BIT_LOCK_MODE_TABLE) & (1 << LockMode)
#define BIT_LOCK_MODE_TABLE 1
#define LOCK_WAIT 32                    // 1<<5
#define LOCK_ORDINARY 64                // 1<<6
#define BIT_LOCK_MODE_ORDINARY 7
#define LOCK_GAP 2048                   // 1<<11
#define BIT_LOCK_MODE_GAP 12
#define LOCK_REC_NOT_GAP 65536          // 1<<16
#define BIT_LOCK_MODE_REC_NOT_GAP 17
#define LOCK_INSERT_INTENTION 2097152   // 1<<21
#define LOCK_MODE_MASK 0xFUL

// #define LOCK_MODE_MASK	0xFUL
// #define LOCK_TABLE 16
// #define LOCK_REC 32
// #define LOCK_TYPE_MASK 0xF0UL
// #define LOCK_WAIT 256

// #define LOCK_ORDINARY 0
// #define LOCK_GAP 512
// #define LOCK_REC_NOT_GAP 1024
// #define LOCK_INSERT_INTENTION 2048

enum RecordLockType: int {
    RECORD_LOCK_ORDINARY = 0,
    RECORD_LOCK_GAP,
    RECORD_LOCK_REC_NOT_GAP,
    RECORD_LOCK_INSERT_INTENTION,
    RECORD_INVALID_LOCK
};

enum LockMode: int {
    LOCK_IS = 0,
    LOCK_IX,
    LOCK_S,
    LOCK_X,
    NON_LOCK
};

static const std::string LockModeStr[5] = {
    "LOCK_IS",
    "LOCK_IX",
    "LOCK_S",
    "LOCK_X",
    "NON_LOCK"
};

static const std::string LockTypeStr[5] = {
    "RECORD_LOCK_ORDINARY",
    "RECORD_LOCK_GAP",
    "RECORD_LOCK_REC_NOT_GAP",
    "RECORD_LOCK_INSERT_INTENTION",
    "RECORD_INVALID_LOCK"
};

static const byte lock_compatibility_matrix[5][5] = {
    /**         IS     IX       S     X       AI */
    /* IS */ {true, true, true, false, true},
    /* IX */ {true, true, false, false, true},
    /* S  */ {true, false, true, false, false},
    /* X  */ {false, false, false, false, false},
    /* AI */ {true, true, false, false, false}};

class Lock {
public:
    txn_id_t trx_id_;   // transaction id
    uint32_t type_mode_;    // the lock type and mode bit flags
    int table_id_;
    int32_t record_no_;
    int offset_;
    Lock* next_ = nullptr;
    Lock* prev_ = nullptr;  // if the prev_ is nullptr, it is a head in the list

    Lock() {
        offset_ = -1;
    }

    Lock(txn_id_t trx_id) : trx_id_(trx_id) {
        offset_ = -1;
    }

    // uint32_t mode() {return (type_mode_ & LOCK_MODE_MASK); }
    // uint32_t type() { return (type_mode_ & LOCK_TYPE_MASK); }
    bool is_waiting() { return (type_mode_ & LOCK_WAIT); }
    bool is_table_lock() { return (type_mode_ & LOCK_TABLE); }
    uint32_t get_table_lock_mode() {
        return (type_mode_ >> BIT_LOCK_MODE_TABLE) & LOCK_MODE_MASK;
    }

    // for debug
    std::string get_table_lock_mode_str() {
        uint32_t current_mode = get_table_lock_mode();
        for(int i = LOCK_IS; i < NON_LOCK; ++i) {
            if(current_mode & (1 << i)) return LockModeStr[i];
        }
    }
    // for debug
    std::string get_record_lock_mode_str(RecordLockType lock_type) {
        uint32_t current_mode = get_record_lock_mode(lock_type);
        for(int i = LOCK_IS; i < NON_LOCK; ++i) {
            if(current_mode & (1 << i)) return LockModeStr[i];
        }
    }

    bool is_conflict_table_mode(LockMode lock_mode) {
        uint32_t current_mode = get_table_lock_mode();
        // std::cout << "current_table_lock_mode: " << LockModeStr[current_mode] << ", request_table_lock_mode: " << LockModeStr[lock_mode] << "\n";
        for(int i = LOCK_IS; i < NON_LOCK; ++i) {
            // LockMode mode = static_cast<LockMode>(i);
            // there is no mode in current table lock;
            if((current_mode & (1 << i)) && lock_compatibility_matrix[i][lock_mode] == false) return true;
        }
        return false;
    }
    bool is_record_lock() { return !(type_mode_ & LOCK_TABLE); }
    // bool is_record_lock() { return (type() == LOCK_REC); }
    bool is_gap() { return (type_mode_ & LOCK_GAP); }
    bool is_record_not_gap() { return (type_mode_ & LOCK_REC_NOT_GAP); }
    bool is_next_key_lock() { return (type_mode_ & LOCK_ORDINARY); }
    bool is_insert_intention() { return (type_mode_ & LOCK_INSERT_INTENTION); }
    RecordLockType get_record_lock_type() {
        if(is_next_key_lock()) return RECORD_LOCK_ORDINARY;
        if(is_gap()) return RECORD_LOCK_GAP;
        if(is_record_not_gap()) return RECORD_LOCK_REC_NOT_GAP;
        if(is_insert_intention()) return RECORD_LOCK_INSERT_INTENTION;
        return RECORD_INVALID_LOCK;
    }
    uint32_t get_record_lock_mode(RecordLockType lock_type) {
        switch(lock_type) {
            case RECORD_LOCK_ORDINARY: {
                return (type_mode_ >> BIT_LOCK_MODE_ORDINARY) & LOCK_MODE_MASK;
            } break;
            case RECORD_LOCK_GAP: {
                return (type_mode_ >> BIT_LOCK_MODE_GAP) & LOCK_MODE_MASK;
            } break;
            case RECORD_LOCK_REC_NOT_GAP: {
                return (type_mode_ >> BIT_LOCK_MODE_REC_NOT_GAP) & LOCK_MODE_MASK;
            }
            default:
                std::cout << "Invalid record type to get record mode.\n";
                break;
        }
        return 0;
    }
    void set_record_lock_mode(RecordLockType lock_type, LockMode lock_mode) {
        switch(lock_type) {
            case RECORD_LOCK_ORDINARY: {
                if(lock_mode == LOCK_X && ((type_mode_ >> (BIT_LOCK_MODE_ORDINARY + LOCK_S)) & 1)) {
                    type_mode_ ^= 1 << (BIT_LOCK_MODE_ORDINARY + LOCK_S);
                    type_mode_ |= 1 << (BIT_LOCK_MODE_ORDINARY + LOCK_X);
                }
                else {
                    type_mode_ |= 1 << (BIT_LOCK_MODE_ORDINARY + lock_mode);
                }
            } break;
            case RECORD_LOCK_GAP: {
                if(lock_mode == LOCK_X && ((type_mode_ >> (BIT_LOCK_MODE_GAP + LOCK_S)) & 1)) {
                    type_mode_ ^= 1 << (BIT_LOCK_MODE_GAP + LOCK_S);
                    type_mode_ |= 1 << (BIT_LOCK_MODE_GAP + LOCK_X);
                }
                else {
                    type_mode_ |= 1 << (BIT_LOCK_MODE_GAP + lock_mode);
                }
            } break;
            case RECORD_LOCK_REC_NOT_GAP: {
                if(lock_mode == LOCK_X && ((type_mode_ >> (BIT_LOCK_MODE_REC_NOT_GAP + LOCK_S)) & 1)) {
                    type_mode_ ^= 1 << (BIT_LOCK_MODE_REC_NOT_GAP + LOCK_S);
                    type_mode_ |= 1 << (BIT_LOCK_MODE_REC_NOT_GAP + LOCK_X);
                }
                else {
                    type_mode_ |= 1 << (BIT_LOCK_MODE_REC_NOT_GAP + lock_mode);
                }
            } break;
            default:
                std::cout << "Invalid record type to set record lock mode.\n";
                break;
        }
    }
    bool is_contained_in_current_type(RecordLockType aim_type, uint32_t aim_mode) {
        if(aim_type == RECORD_LOCK_ORDINARY) {
            if(is_next_key_lock() && get_record_lock_mode(RECORD_LOCK_ORDINARY) >= aim_mode) 
                return true;
        }
        if(aim_type == RECORD_LOCK_GAP) {
            if(is_next_key_lock() && get_record_lock_mode(RECORD_LOCK_ORDINARY) >= aim_mode)
                return true;
            else if(is_gap() && get_record_lock_mode(RECORD_LOCK_GAP) >= aim_mode)
                return true;
        }
        if(aim_type == RECORD_LOCK_REC_NOT_GAP) {
            if(is_next_key_lock() && get_record_lock_mode(RECORD_LOCK_ORDINARY) >= aim_mode)
                return true;
            else if(is_record_not_gap() && get_record_lock_mode(RECORD_LOCK_REC_NOT_GAP) >= aim_mode)
                return true;
        }
        // if(aim_type == RECORD_LOCK_INSERT_INTENTION) return is_insert_intention();
        return false;
    }

    void unlock_gap_lock() {
        assert(!is_gap());
        assert(!is_insert_intention());
        assert(is_next_key_lock());

        type_mode_ |= LOCK_REC_NOT_GAP;
    }
};

/* 多粒度锁，加锁对象的类型，包括记录和表 */
enum class LockDataType { TABLE = 0, RECORD = 1 };

/**
 * @description: 加锁对象的唯一标识
 */
class LockDataId {
   public:
    /* 表级锁 */
    LockDataId(int table_id, LockDataType type) {
        assert(type == LockDataType::TABLE);
        table_id_ = table_id;
        type_ = type;
        bucket_id_ = -1;
    }

    /* 行级锁 */
    LockDataId(int table_id, int32_t record_no_, LockDataType type) {
        assert(type == LockDataType::RECORD);
        table_id_ = table_id;
        bucket_id_ = record_no_ / BUCKET_SIZE;
        type_ = type;
    }

    inline int64_t Get() const {
        if (type_ == LockDataType::TABLE) {
            // fd_
            return static_cast<int64_t>(table_id_);
        } else {
            // fd_, rid_.page_no, rid.slot_no
            return ((static_cast<int64_t>(type_)) << 63) | ((static_cast<int64_t>(table_id_)) << 31) |
                   ((static_cast<int64_t>(bucket_id_)) << 16);
        }
    }

    bool operator==(const LockDataId &other) const {
        if (type_ != other.type_) return false;
        if (table_id_ != other.table_id_) return false;
        return bucket_id_ == other.bucket_id_;
    }
    int table_id_;
    // page_id_t page_id_;
    int32_t bucket_id_;
    LockDataType type_;
};

template <>
struct std::hash<LockDataId> {
    size_t operator()(const LockDataId &obj) const { return std::hash<int64_t>()(obj.Get()); }
};

class LockRequestQueue {
public:
    LockRequestQueue() {
        record_no_ = -1;
        Lock* head = new Lock(INVALID_TXN_ID);
        request_queue_ = head;
    }
    LockRequestQueue(int32_t record_no, Lock* first_request)
        : record_no_(record_no) {
        Lock* head = new Lock(INVALID_TXN_ID);
        request_queue_ = head;
        head->next_ = first_request;
        first_request->prev_ = head;
    }

    // Rid lock_rid_;
    int32_t record_no_;
    Lock* request_queue_ = nullptr;           // list of lock requests
    std::condition_variable cv_;    // condition variable is used to awake waiting requests, no used in no-wait
    LockRequestQueue* next_ = nullptr;
};

class LockListInBucket {
public:
    LockListInBucket() {}
    LockListInBucket(bucket_id_t bucket_id) : bucket_id_(bucket_id) {}
    LockListInBucket(bucket_id_t bucket_id, LockRequestQueue* first_lock_queue)
        : bucket_id_(bucket_id), first_lock_queue_(first_lock_queue) {}

    bucket_id_t bucket_id_;                     // -1 if this is table lock lists
    LockRequestQueue* first_lock_queue_ = nullptr;
};

#endif