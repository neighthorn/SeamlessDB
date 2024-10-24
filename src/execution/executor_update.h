#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include "recovery/redo_log/redo_log.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    IxIndexHandle* pindex_handle_;
    MultiVersionFileHandle *old_version_handle_;    // old_version_handle
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;
    int rid_index_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
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
    std::unique_ptr<Record> Next() override {
        auto pindex = tab_.get_primary_index_meta();
        int rid_tot_size = rids_.size();
        for (; rid_index_ < rid_tot_size; ++rid_index_) {
            auto& rid = rids_[rid_index_];
            // auto rec = fh_->get_record(rid, context_);
            auto record = pindex_handle_->get_record(rid, context_);
            // record a update operation into the transaction
            Record origin_record(record->record_, record->data_length_ + sizeof(RecordHdr));

            // store old version data
            // Rid old_version_rid = old_version_handle_->insert_record(origin_record.record_, context_);

            // Update record in record file
            for (auto &set_clause : set_clauses_) {
                auto lhs_col = tab_.get_col(set_clause.lhs.col_name);
                memcpy(record->raw_data_ + lhs_col->offset, set_clause.rhs.raw->data, lhs_col->len);
            }
            // update record header
            RecordHdr *record_hdr = (RecordHdr*)(record->record_);
            // record_hdr->trx_id_ = context_->txn_->get_transaction_id();
            // record_hdr->rollback_file_id_ = old_version_handle_->GetFd();
            // record_hdr->rollback_page_no_ = old_version_rid.page_no;
            // record_hdr->rollback_slot_no_ = old_version_rid.slot_no;
            
            pindex_handle_->update_record(rid, record->raw_data_, context_);
            
            if(context_ != nullptr) {
                WriteRecord* write_record = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, record->raw_data_, pindex->col_tot_len, origin_record);
                context_->txn_->append_write_record(write_record);
            }

            // make redo log and sent to storage node
            if(context_ != nullptr) {
                RmRecord old_record(origin_record.data_length_ + sizeof(RecordHdr), origin_record.record_);
                RmRecord new_record(record->data_length_ + sizeof(RecordHdr), record->record_);
                // std::unique_ptr<UpdateRedoLogRecord> update_log = std::make_unique<UpdateRedoLogRecord>(context_->txn_->get_transaction_id(), old_record, new_record, rid, tab_name_);
                // use rpc to sent to storage node
                // context_->log_mgr_->write_log_to_storage(std::move(update_log));
                context_->log_mgr_->make_update_redolog(context_->txn_->get_transaction_id(), old_record, new_record, rid, tab_name_, true);
            } 
        }
        return nullptr;
    }
    Rid &rid() override { return _abstract_rid; }

    // the checkpoint only stores the cursor and intermediate results, the meta info is not included
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