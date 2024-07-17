#pragma once

#include <thread>

#include "storage/buffer_pool_manager.h"
#include "index/ix.h"
#include "storage_defs.h"
#include "storage/disk_manager.h"
#include "system/sm.h"
#include "log_store.h"
#include "recovery/redo_log/redo_log.h"

/* 
    LogReplay
*/
class LogReplay {

public:
    // Log Replay初始化时，启动一个线程运行replay_log()
    LogReplay(LogStore *log_store, DiskManager *disk_manager, IxManager *ix_manager, SmManager *sm_manager,ShareStatus *share_status);

    void replay_log();
    void replay_single_log(const std::string &redo_log_str);
private:
    // thread
    std::thread     replay_thread_;

    // disk manager and log buffer
    LogStore            *log_store_;
    DiskManager         *disk_manager_;
    IxManager           *ix_manager_;
    SmManager           *sm_manager_;
    ShareStatus         *share_status_;

};