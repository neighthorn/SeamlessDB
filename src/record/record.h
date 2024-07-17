#pragma once

#include "common/config.h"

/**
 * Record Format:
 * | next_record_offset | trx_id | is_deleted | rollback_file_id | page_no | slot_no | record_no | raw data |
*/

class RecordHdr {
public:
    int32_t next_record_offset_;    // 这是干嘛的？
    txn_id_t trx_id_;
    bool is_deleted_;
    file_id_t rollback_file_id_;
    page_id_t rollback_page_no_;
    int32_t rollback_slot_no_;
    int32_t record_no_;                 // the identifier of the record in this table
};

class Rid {
public:
    page_id_t page_no;
    int slot_no;
    int record_no;

    friend bool operator==(const Rid &x, const Rid &y) { return x.page_no == y.page_no && x.slot_no == y.slot_no && x.record_no == y.record_no; }

    friend bool operator!=(const Rid &x, const Rid &y) { return !(x == y); }
};

class Record {
public:
    char* record_;
    char* raw_data_;
    int32_t data_length_;
    bool allocated_ = false;

    Record() = default;

    Record(const Record& other) {
        data_length_ = other.data_length_;
        record_ = new char[sizeof(RecordHdr) + data_length_];
        raw_data_ = record_ + sizeof(RecordHdr);
        allocated_ = true;
        memcpy(record_, other.record_, data_length_ + sizeof(RecordHdr));
    }

    Record(char* record_slot, int record_tot_len) {
        data_length_ = record_tot_len - sizeof(RecordHdr);
        record_ = new char[record_tot_len];
        raw_data_ = record_ + sizeof(RecordHdr);
        allocated_ = true;

        memcpy(record_, record_slot, record_tot_len);
    }

    Record(int record_data_len) {
        data_length_ = record_data_len;
        record_ = new char[sizeof(RecordHdr) + data_length_];
        raw_data_ = record_ + sizeof(RecordHdr);
        allocated_ = true;
        init_hdr();
    }

    void init_hdr() {
        RecordHdr* record_hdr_ = (RecordHdr*)record_;
        record_hdr_->next_record_offset_ = INVALID_OFFSET;
        record_hdr_->trx_id_ = INVALID_TXN_ID;
        record_hdr_->is_deleted_ = false;
        record_hdr_->rollback_file_id_ = INVALID_FILE_ID;
        record_hdr_->rollback_page_no_ = INVALID_PAGE_ID;
        // record_hdr_->slot_no_ = INVALID_OFFSET;
        record_hdr_->rollback_slot_no_ = INVALID_OFFSET;
        record_hdr_->record_no_ = INVALID_SLOT_NO;
    }

    bool is_deleted() const {
        return *(bool*)(record_ + RECHDR_OFF_DELETE_MARK);
    }

    ~Record() {
        if(allocated_) {
            delete[] record_;
            allocated_ = false;
        }
    }
};