// #pragma once

// #include <map>
// #include <unordered_map>
// #include "log_manager.h"
// #include "storage/disk_manager.h"
// #include "system/sm_manager.h"

// class RedoLogsInPage {
// public:
//     // RedoLogsInPage() { table_file_ = nullptr; }
//     // RmFileHandle* table_file_;
//     // std::vector<lsn_t> redo_logs_;   // 在该page上需要redo的操作的lsn
// };

// class RecoveryManager {
// public:
//     RecoveryManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, SmManager* sm_manager) {
//         disk_manager_ = disk_manager;
//         buffer_pool_manager_ = buffer_pool_manager;
//         sm_manager_ = sm_manager;
//     }
//     // LogRecord* deserialize(char* buf, int buf_offset);
//     // void append_redo_log_into_page_map(RmFileHandle* table_file, page_id_t page_no, lsn_t log_lsn);
//     // void analyze();
//     // void redo();
//     // void undo();
// private:
//     std::unordered_map<PageId, RedoLogsInPage> page_redo_map_;      // 记录每个page需要redo的log
//     std::map<txn_id_t, lsn_t> undo_txn_last_log_;                   // 记录每个需要undo的事务的最后一条日志
//     std::map<lsn_t, int> lsn_to_offset_;                            // 记录lsn和offset的映射
//     std::map<lsn_t, uint32_t> lsn_to_len_;                          // 记录lsn的长度
//     LogBuffer buffer_;                                              // 读入日志
//     DiskManager* disk_manager_;                                     // 用来读写文件
//     BufferPoolManager* buffer_pool_manager_;                        // 对页面进行读写
//     SmManager* sm_manager_;                                         // 访问数据库元数据
// };