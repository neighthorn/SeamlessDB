#include <thread>

#include "log_replay.h"
#include "debug_log.h"

LogReplay::LogReplay(LogStore *log_store, DiskManager *disk_manager, IxManager *ix_manager, SmManager *sm_manager, ShareStatus *share_status) 
    : log_store_(log_store), disk_manager_(disk_manager), ix_manager_(ix_manager), sm_manager_(sm_manager), share_status_(share_status) {
        std::cout << "try to start log replay thread! \n";
        /*
            start replay thread
        */
        replay_thread_ = std::thread(&LogReplay::replay_log, this);
        std::cout << "replay thread starts!\n";
    }

void LogReplay::replay_log() {
    while(true) {
        // std::cout << "LogReplayThread: current_replay_lsn = " << share_status_->current_replay_lsn_ << ", need_replay_lsn_ = " << share_status_->need_replay_lsn_ << "\n";
        if(share_status_->current_replay_lsn_ < share_status_->need_replay_lsn_) {
            // do replay
            lsn_t replay_log = share_status_->current_replay_lsn_ + 1;
            // retrieve corresponding log
            std::string redo_log_string = log_store_->read_log(replay_log);
            replay_single_log(redo_log_string);
            share_status_->current_replay_lsn_ = replay_log;
        } else {
            // don't need redo, sleep and wait
            std::this_thread::sleep_for(std::chrono::milliseconds(50)); //sleep 50 ms
            continue;
        }
    }
}

void LogReplay::replay_single_log(const std::string &redo_log_str) {
    // deserilize
    RedoLogRecord redo_log_hdr;
    redo_log_hdr.deserialize(redo_log_str.c_str());

    // DEBUG_REPALY_LOG(redo_log_hdr);

    switch (redo_log_hdr.log_type_)
    {
        case RedoLogType::UPDATE: {
            UpdateRedoLogRecord update_redo_log;
            update_redo_log.deserialize(redo_log_str.c_str());

            // update_redo_log.format_print();
            // puts("");

            // do update 
            std::string table_name = std::string(update_redo_log.table_name_, update_redo_log.table_name_size_);
            auto index_handle = sm_manager_->primary_index_[table_name].get();
            if(index_handle == nullptr) {
                throw RMDBError("table name " + std::string(update_redo_log.table_name_) + " not found!");
            }

            index_handle->update_record(update_redo_log.rid_, update_redo_log.new_value_.data, nullptr);
            
            break;
        }
        case RedoLogType::DELETE: {
            DeleteRedoLogRecord delete_redo_log;
            delete_redo_log.deserialize(redo_log_str.c_str());

            // delete_redo_log.format_print();
            // puts("");

            // do delete
            std::string table_name = std::string(delete_redo_log.table_name_, delete_redo_log.table_name_size_);
            auto index_handle = sm_manager_->primary_index_[table_name].get();

            index_handle->update_record(delete_redo_log.rid_, delete_redo_log.delete_value_.data, nullptr);

            break;
        }
        
        case RedoLogType::INSERT: {
            InsertRedoLogRecord insert_redo_log;
            insert_redo_log.deserialize(redo_log_str.c_str());

            // insert_redo_log.format_print();
            // puts("");
            std::string table_name = std::string(insert_redo_log.table_name_, insert_redo_log.table_name_size_);
            auto index_handle = sm_manager_->primary_index_[table_name].get();

            index_handle->replay_insert_record(insert_redo_log.rid_, insert_redo_log.key_, insert_redo_log.insert_value_.data);
        }
    
        default:
            break;
    }
}
