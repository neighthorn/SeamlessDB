// #include "log_recovery.h"

// /**
//  * 把buffer中的字符反序列化成LogRecord
//  * buf: deserialize起始位置
//  * buf_offset: buffer中剩余字节数
// */
// LogRecord* RecoveryManager::deserialize(char* buf, int buf_offset){
//     int len = *reinterpret_cast<const uint32_t*>(buf + OFFSET_LOG_TOT_LEN);
//     if(len > buf_offset) return nullptr;

//     switch ((LogType)*buf) {
//         case LogType::UPDATE:{
//             UpdateLogRecord* update_log = new UpdateLogRecord();
//             update_log->deserialize(buf);
//             return update_log;
//         }
//         case LogType::INSERT: {
//             InsertLogRecord* insert_log = new InsertLogRecord();
//             insert_log->deserialize(buf);
//             return insert_log;
//         }
//         case LogType::DELETE: {
//             DeleteLogRecord* delete_log = new DeleteLogRecord();
//             delete_log->deserialize(buf);
//             return delete_log;
//         }
//         case LogType::begin: {
//             BeginLogRecord* begin_log = new BeginLogRecord();
//             begin_log->deserialize(buf);
//             return begin_log;
//         }
//         case LogType::commit: {
//             CommitLogRecord* commit_log = new CommitLogRecord();
//             commit_log->deserialize(buf);
//             return commit_log;
//         }
//         case LogType::ABORT: {
//             AbortLogRecord* abort_log = new AbortLogRecord();
//             abort_log->deserialize(buf);
//             return abort_log;
//         }
//         default:
//             return nullptr;
//             break;
//     }
// }

// void RecoveryManager::append_redo_log_into_page_map(RmFileHandle* table_file, page_id_t page_no, lsn_t log_lsn) {
//     try {
//         // 这个判断只有insert操作需要做，因为delete和update操作之前一定至少有一个相关的insert操作，一定执行过相关的try-catch
//         // 这里需要判断该页是否存在，如果不存在需要创建新的page
//         table_file->fetch_page_handle(page_no);
//     } catch(PageNotExistError& error) {
//         // 这样处理是否能够保证新的page的页号一定和log里面的页号相同
//         page_id_t new_page_no = table_file->create_new_page_handle().page->get_page_id().page_no;
//         assert(new_page_no != page_no);
//     }

//     int page_fd = table_file->GetFd();
//     PageId page_id = PageId{.fd = page_fd, .page_no = page_no};
//     lsn_t page_lsn = table_file->fetch_page_handle(page_no).page->get_page_lsn();
//     if(page_lsn < log_lsn) {
//         if(page_redo_map_.find(page_id) == page_redo_map_.end()) {
//             // page_redo_map_.insert(page_id, RedoLogsInPage());
//             page_redo_map_[page_id] = RedoLogsInPage();
//             assert(page_redo_map_.find(page_id) != page_redo_map_.end());
//         }
//         RedoLogsInPage* redo_logs_vector = &page_redo_map_.find(page_id)->second;
//         assert(redo_logs_vector != nullptr);
//         redo_logs_vector->redo_logs_.emplace_back(log_lsn);
//         redo_logs_vector->table_file_ = table_file;
//     }
// }

// /**
//  * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
//  */
// void RecoveryManager::analyze() {
//     int file_offset = 0;
//     while(true) {
//         memset(buffer_.buffer_, 0, sizeof(LOG_BUFFER_SIZE));
//         int read_bytes = disk_manager_->read_log(buffer_.buffer_, LOG_BUFFER_SIZE, file_offset);
//         assert(read_bytes != -1);

//         buffer_.offset_ = 0;
//         while(buffer_.offset_ < read_bytes && buffer_.offset_ + OFFSET_LOG_DATA <= read_bytes) {
//             // std::cout << buffer_.offset_ << " " << read_bytes << std::endl;
//             LogRecord* log = deserialize(buffer_.buffer_ + buffer_.offset_, read_bytes - buffer_.offset_);
//             if(log == nullptr) break;
//             // std::cout << LogTypeStr[log->log_type_] << std::endl;

//             switch(log->log_type_) {
//                 std::cout << "LSN: " << log->lsn_ << "\n";
//                 case LogType::begin: {
//                     undo_txn_last_log_[log->log_tid_] = log->lsn_;
//                     std::cout << "begin log: txn_id: " << log->log_tid_ << ", lsn: " << log->lsn_ << "\n";
//                 } break;
//                 case LogType::ABORT:
//                 case LogType::commit: {
//                     undo_txn_last_log_.erase(log->log_tid_);
//                     std::cout << "commit/abort log: txn_id: " << log->log_tid_ << ", lsn: " << log->lsn_ << "\n";
//                 } break;
//                 case LogType::INSERT: {
//                     std::cout << "insert log: txn_id: " << log->log_tid_ << ", lsn: " << log->lsn_ << "\n";
//                     InsertLogRecord* insert_log = dynamic_cast<InsertLogRecord*>(log);
//                     std::string table_name(insert_log->table_name_, insert_log->table_name_ + insert_log->table_name_size_);
//                     RmFileHandle* table_file = sm_manager_->fhs_[table_name].get();
//                     page_id_t page_no = insert_log->rid_.page_no;
//                     assert(table_file != nullptr);

//                     append_redo_log_into_page_map(table_file, page_no, insert_log->lsn_);
//                     undo_txn_last_log_[log->log_tid_] = log->lsn_;
//                 } break;
//                 case LogType::DELETE: {
//                     std::cout << "delete log: txn_id: " << log->log_tid_ << ", lsn: " << log->lsn_ << "\n";
//                     DeleteLogRecord* delete_log = dynamic_cast<DeleteLogRecord*>(log);
//                     std::string table_name(delete_log->table_name_, delete_log->table_name_ + delete_log->table_name_size_);
//                     RmFileHandle* table_file = sm_manager_->fhs_[table_name].get();
//                     page_id_t page_no = delete_log->rid_.page_no;
//                     assert(table_file != nullptr);

//                     append_redo_log_into_page_map(table_file, page_no, delete_log->lsn_);
//                     undo_txn_last_log_[log->log_tid_] = log->lsn_;
//                 } break;
//                 case LogType::UPDATE: {
//                     std::cout << "update log: txn_id: " << log->log_tid_ << ", lsn: " << log->lsn_ << "\n";
//                     UpdateLogRecord* update_log = dynamic_cast<UpdateLogRecord*>(log);
//                     std::string table_name(update_log->table_name_, update_log->table_name_ + update_log->table_name_size_);
//                     RmFileHandle* table_file = sm_manager_->fhs_[table_name].get();
//                     page_id_t page_no = update_log->rid_.page_no;
//                     assert(table_file != nullptr);

//                     append_redo_log_into_page_map(table_file, page_no, update_log->lsn_);
//                     undo_txn_last_log_[log->log_tid_] = log->lsn_;
//                 } break;
//                 default: 
//                 break;
//             }
            
//             lsn_to_offset_[log->lsn_] = file_offset;
//             lsn_to_len_[log->lsn_] = log->log_tot_len_;
//             buffer_.offset_ += log->log_tot_len_;
//             file_offset += log->log_tot_len_;
//         }

//         if(read_bytes < LOG_BUFFER_SIZE) break;
//     }
// }

// /**
//  * @description: 重做所有未落盘的操作
//  */
// void RecoveryManager::redo() {
//     // 在redo阶段需要遍历page_redo_map_，对每个page进行redo
//     for(auto iter = page_redo_map_.begin(); iter != page_redo_map_.end(); ++iter) {
//         PageId page_id = iter->first;
//         RedoLogsInPage* redo_logs = &iter->second;
//         RmFileHandle* table_file = redo_logs->table_file_;
//         int log_size = redo_logs->redo_logs_.size();

//         std::cout << "page_id: " << page_id.toString() << ",   redo_log_size: " << log_size << "\n";

//         for(int i = 0; i < log_size; ++i) {
//             lsn_t lsn = redo_logs->redo_logs_[i];
//             int len = lsn_to_len_[lsn];
//             char* buf = new char[len];
//             memset(buf, 0, len);
//             disk_manager_->read_log(buf, len, lsn_to_offset_[lsn]);
//             switch((LogType)*buf) {
//                 case LogType::INSERT: {
//                     std::cout << "Redo operation: insert\n";
//                     InsertLogRecord* insert_log = new InsertLogRecord();
//                     insert_log->deserialize(buf);
//                     table_file->insert_record(insert_log->rid_, insert_log->insert_value_.data);
//                 } break;
//                 case LogType::DELETE: {
//                     std::cout << "Redo operation: delete\n";
//                     DeleteLogRecord* delete_log = new DeleteLogRecord();
//                     delete_log->deserialize(buf);
//                     table_file->delete_record(delete_log->rid_, nullptr);
//                 } break;
//                 case LogType::UPDATE: {
//                     std::cout << "Redo operation: update\n";
//                     UpdateLogRecord* update_log = new UpdateLogRecord();
//                     update_log->deserialize(buf);
//                     table_file->update_record(update_log->rid_, update_log->new_value_.data, nullptr);
//                 } break;
//                 default:
//                 break;
//             }
//         }
//     }
// }

// /*
//  * _type: log type, Insert/Delete/Update...
//  * _log_name: variable name
//  * _handle_name: 
//  */
// #define GET_META(_type, _log_name, _handle_name) _type##LogRecord *_log_name = new _type##LogRecord(); \
//                     _log_name->deserialize(buf); \
//                     std::string table_name(_log_name->table_name_, _log_name->table_name_ + _log_name->table_name_size_);  \
//                     RmFileHandle* _handle_name = sm_manager_->fhs_[table_name].get();

// /**
//  * @description: 回滚未完成的事务
//  */
// void RecoveryManager::undo() {
//     // 在undo阶段需要遍历undo_txns，对每个需要undo的transaction去做undo
//     std::cout << "undo transaction count: " << undo_txn_last_log_.size() << "\n";
//     for(auto iter = undo_txn_last_log_.begin(); iter != undo_txn_last_log_.end(); ++iter) {
//         lsn_t lsn = iter->second;
//         while(lsn != INVALID_LSN) {
//             int len = lsn_to_len_[lsn];
//             char* buf = new char[len];
//             memset(buf, 0, len);
//             disk_manager_->read_log(buf, len, lsn_to_offset_[lsn]);
//             switch((LogType)*buf) {
//                 case LogType::INSERT: {
//                     GET_META(Insert, insert_log, table_file);
//                     table_file->delete_record(insert_log->rid_, nullptr);
//                     std::cout << "Undo operation: insert,  lsn: " << lsn << "\n";
//                     lsn = insert_log->prev_lsn_;
//                 } break;
//                 case LogType::DELETE: {
//                     GET_META(Delete, delete_log, table_file);
//                     table_file->insert_record(delete_log->rid_, delete_log->delete_value_.data);
//                     lsn = delete_log->prev_lsn_;
//                 } break;
//                 case LogType::UPDATE: {
//                     GET_META(Update, update_log, table_file);
//                     table_file->update_record(update_log->rid_, update_log->old_value_.data, nullptr);
//                     lsn = update_log->prev_lsn_;
//                 } break;
//                 case LogType::begin: {
//                     BeginLogRecord* begin_log = new BeginLogRecord();
//                     begin_log->deserialize(buf);
//                     lsn = begin_log->prev_lsn_;
//                 } break;
//                 default: {
//                     printf("Unexpected Log type: %s\n", LogTypeStr[(LogType)*buf].c_str());
//                 } break;
//             }
//         }
//     }
// }