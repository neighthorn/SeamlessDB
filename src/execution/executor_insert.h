#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class InsertExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Value> values_;     // 需要插入的数据
    // RmFileHandle *fh_;              // 表的数据文件句柄
    IxIndexHandle* pindex_handle_;  // cluster index handle
    MultiVersionFileHandle *old_version_handle_; // old_version handle
    std::string tab_name_;          // 表名称
    Rid rid_;                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

   public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols_.size()) {
            throw InvalidValueCountError();
        }
        // fh_ = sm_manager_->fhs_.at(tab_name).get();
        pindex_handle_ = sm_manager->primary_index_.at(tab_name).get();
        old_version_handle_ = sm_manager->old_versions_.at(tab_name).get();
        context_ = context;

        // if(context != nullptr) {
        //     context->lock_mgr_->lock_IX_on_table(context->txn_, fh_->GetFd());
        // }
    };

    std::unique_ptr<Record> Next() override {
        // Make record buffer
        // RmRecord rec(fh_->get_file_hdr().record_size);
        Record record(tab_.record_length_);

        // 填充RecordHdr字段
        RecordHdr* record_hdr_ = (RecordHdr*)record.record_;
        record_hdr_->next_record_offset_ = INVALID_OFFSET;
        record_hdr_->trx_id_ = context_->txn_->get_transaction_id();
        record_hdr_->is_deleted_ = false;
        record_hdr_->rollback_file_id_ = INVALID_FILE_ID;  // 其实用不到seg_id，因为我们没有使用segment，直接用的page
        record_hdr_->rollback_page_no_ = INVALID_PAGE_ID;
        record_hdr_->rollback_slot_no_ = INVALID_OFFSET;

        // 填充record->raw_data字段
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols_[i];
            auto &val = values_[i];
            if (col.type != val.type) {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }
            val.init_raw(col.len);
            memcpy(record.raw_data_ + col.offset, val.raw->data, col.len);
        }

        // make primary key
        auto& pindex = tab_.indexes_[0];
        char* pkey = new char[pindex.col_tot_len];
        int poffset = 0;
        for(int i = 0; i < pindex.col_num; ++i) {
            memcpy(pkey + poffset, record.raw_data_ + pindex.cols[i].offset, pindex.cols[i].len);
            poffset += pindex.cols[i].len;
        }
        
        if(context_ != nullptr) {
            auto lower_rid_ = pindex_handle_->lower_bound(pkey);
            auto upper_rid_ = pindex_handle_->upper_bound(pkey);
            if(lower_rid_ != upper_rid_) {
                throw PrimaryKeyRepeatError();
            }
            Lock* lock = context_->lock_mgr_->request_record_lock(tab_.table_id_, lower_rid_, context_->txn_, RECORD_LOCK_INSERT_INTENTION, NON_LOCK, context_->coro_sched_->t_id_);
            assert(lock != nullptr);
            context_->txn_->append_lock(lock);
        }

        // Insert into record file
        rid_ = pindex_handle_->insert_record(pkey, record.record_, context_);

        // InsertLogRecord* insert_log = new InsertLogRecord(context_->txn_->get_transaction_id(),
                    // rec, rid_, tab_name_);
        // insert_log->prev_lsn_ = context_->txn_->get_prev_lsn();
        // context_->log_mgr_->add_log_to_buffer(insert_log);
        // context_->txn_->set_prev_lsn(insert_log->lsn_);

        if(context_ != nullptr) {
            WriteRecord *write_record = new WriteRecord(WType::INSERT_TUPLE, tab_name_, pkey, pindex.col_tot_len);
            context_->txn_->append_write_record(write_record);

            // make insert redo log
            RmRecord insert_record(record.data_length_ + sizeof(RecordHdr), record.record_);
            context_->log_mgr_->make_insert_redolog(context_->txn_->get_transaction_id(), pkey, pindex.col_tot_len, insert_record, rid_, tab_name_, true);
            
        }

        delete pkey;
        
        // Insert into index
        // for(size_t i = 1; i < tab_.indexes_.size(); ++i) {
        //     auto& index = tab_.indexes_[i];
        //     auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
        //     char* key = new char[index.col_tot_len];
        //     int offset = 0;
        //     for(size_t i = 0; i < index.col_num; ++i) {
        //         memcpy(key + offset, insert_record.raw_data_ + index.cols[i].offset, index.cols[i].len);
        //         offset += index.cols[i].len;
        //     }
        //     ih->insert_entry(key, rid_, context_->txn_);
        //     delete key;
        // }
        return nullptr;
    }
    Rid &rid() override { return rid_; }

    int checkpoint(char* dest) override {
        return 0;
    }

    std::chrono::time_point<std::chrono::system_clock> get_latest_ckpt_time() override {
        assert(0);
        return std::chrono::high_resolution_clock::now();
    }

    double get_curr_suspend_cost() override {
        assert(0);
        return 0;
    }
};