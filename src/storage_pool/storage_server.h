#pragma once

#include <brpc/channel.h>

#include "storage/disk_manager.h"
#include "recovery/log_manager.h"
#include "system/sm_manager.h"
#include "storage/storage_rpc.h"
#include "log_replay.h"
#include "storage_defs.h"


class StorageServer {
public:
    StorageServer(int machine_id, int local_rpc_port, DiskManager* disk_manager, LogStore *log_store, ShareStatus *share_status, BufferPoolManager* buffer_pool_mgr)
        : disk_manager_(disk_manager), log_store_(log_store), share_status_(share_status), buffer_pool_mgr_(buffer_pool_mgr){
        brpc::Server server;
        storage_service::StoragePoolImpl storage_pool_rpc(disk_manager, log_store, share_status, buffer_pool_mgr_);
        if(server.AddService(&storage_pool_rpc, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(ERROR) << "Failed to add service.";
        }
        butil::EndPoint point;
        point = butil::EndPoint(butil::IP_ANY, local_rpc_port);

        brpc::ServerOptions options;

        if(server.Start(point, &options) != 0) {
            LOG(ERROR) << "Failed to start server.";
        }

        server.RunUntilAskedToQuit();
    }

    SmManager* sm_mgr_;
    IxManager* ix_mgr_;
    MultiVersionManager *mvcc_manager;
    BufferPoolManager* buffer_pool_mgr_;
    DiskManager* disk_manager_;
    LogStore *log_store_;
    ShareStatus *share_status_;
    std::string workload_;
};