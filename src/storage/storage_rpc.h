#pragma once

#include <butil/logging.h>
#include <brpc/server.h>
#include <gflags/gflags.h>

#include "storage_service.pb.h"
#include "storage/disk_manager.h"
#include "recovery/log_manager.h"
#include "storage/buffer_pool_manager.h"
#include "storage_pool/log_store.h"
#include "storage_pool/storage_defs.h"

namespace storage_service {
class StoragePoolImpl: public StorageService {
public:
    StoragePoolImpl(DiskManager* disk_manager, LogStore *log_store, ShareStatus *share_status, BufferPoolManager* buffer_pool_mgr);

    virtual ~StoragePoolImpl();

    virtual void LogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::LogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done);
    
    virtual void GetOldPage(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetOldPageRequest* request,
                       ::storage_service::GetOldPageResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void GetLatestPage(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetLatestPageRequest* request,
                       ::storage_service::GetLatestPageResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void GetPersistLsn(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetPersistLsnRequest* request,
                       ::storage_service::GetPersistLsnResponse* response,
                       ::google::protobuf::Closure* done);

private:
    DiskManager* disk_manager_;
    LogStore *log_store_;
    BufferPoolManager* buffer_pool_manager_;
    ShareStatus *share_status_;
};
}