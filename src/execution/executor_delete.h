#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    // RmFileHandle *fh_;              // 表的数据文件句柄
    IxIndexHandle* pindex_handle_;  // cluster index handle
    MultiVersionFileHandle *old_version_handle_; // old_version_handle
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;
    int rid_index_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        // fh_ = sm_manager_->fhs_.at(tab_name).get();
        pindex_handle_ = sm_manager->primary_index_.at(tab_name).get();
        old_version_handle_ = sm_manager->old_versions_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
        rid_index_ = 0;

        // if(context != nullptr) {
        //     context->lock_mgr_->lock_IX_on_table(context->txn_, fh_->GetFd());
        // }
    }
    void set_rids(std::vector<Rid> rids){rids_ = rids;}
    std::unique_ptr<Record> Next() override {
        // Delete each rid from record file and index file
        int rid_tot_size = rids_.size();
        for(; rid_index_ < rid_tot_size; ++ rid_index_) {
            // auto rec = fh_->get_record(rid, context_);
            auto& rid = rids_[rid_index_];
            auto record = pindex_handle_->get_record(rid, context_);
            // Delete from index file
            // for(auto& index: tab_.indexes_) {
            //     char* key = new char[index.col_tot_len];
            //     int offset = 0;
            //     auto& index_cols = index.cols;
            //     auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_cols)).get();
            //     for(size_t i = 0; i < index_cols.size(); ++ i) {
            //         memcpy(key + offset, record->raw_data_ + index_cols[i].offset, index_cols[i].len);
            //         offset += index_cols[i].len;
            //     }
            //     ih->delete_entry(key, rid, context_->txn_);
            // }

            // Delete from record file
            // fh_->delete_record(rid, context_);

            // 将旧版本保存到old_version
            // Rid old_version_rid = old_version_handle_->insert_record(record->record_, context_);

            // 将delete标志置位，然后写回
            RecordHdr *record_hdr = (RecordHdr*)(record->record_);
            record_hdr->is_deleted_ = true;
            record_hdr->trx_id_ = context_->txn_->get_transaction_id();
            record_hdr->rollback_file_id_ = old_version_handle_->GetFd();
            // record_hdr->rollback_page_no_ = old_version_rid.page_no;
            // record_hdr->rollback_slot_no_ = old_version_rid.slot_no;
            
            pindex_handle_->update_record(rid, record->record_, context_);
            // pindex_handle_->delete_record(rid, context_);

            if(context_ != nullptr) {
                WriteRecord* write_record = new WriteRecord(WType::DELETE_TUPLE, tab_name_, record->raw_data_, tab_.get_primary_index_meta()->col_tot_len);
                context_->txn_->append_write_record(write_record);

                // make delete redo log
                RmRecord delete_record(record->data_length_ + sizeof(RecordHdr), record->record_);
                context_->log_mgr_->make_delete_redolog(context_->txn_->get_transaction_id(), delete_record, rid, tab_name_, true);
            }

            // record a delete operation into the transaction
            // RmRecord delete_record{rec->size};
            // memcpy(delete_record.data, rec->data, rec->size);

            // DeleteLogRecord* delete_log = new DeleteLogRecord(context_->txn_->get_transaction_id(),
            //             delete_record, rid, tab_name_);
            // delete_log->prev_lsn_ = context_->txn_->get_prev_lsn();
            // context_->log_mgr_->add_log_to_buffer(delete_log);
            // context_->txn_->set_prev_lsn(delete_log->lsn_);

            // WriteRecord *write_record = new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, delete_record);
            // context_->txn_->append_write_record(write_record);
        }
        return nullptr;
    }
    Rid &rid() override { return _abstract_rid; }

    int checkpoint(char* dest) override {
        int offset = 0; 
        memcpy(dest + offset, &rid_index_, sizeof(int));
        offset += sizeof(int);

        return offset;
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