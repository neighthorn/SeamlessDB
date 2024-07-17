#include <memory>
#include <brpc/channel.h>

#include "redo_log.h"
#include "storage/storage_service.pb.h"

class RedoLogService {
public:
    void write_log_with_rpc(std::unique_ptr<RedoLogRecord> redo_log) {
        // init brpc
        storage_service::StorageService_Stub stub(log_channel_);
        storage_service::LogWriteRequest request;
        storage_service::LogWriteResponse* response = new storage_service::LogWriteResponse;
        brpc::Controller* cntl = new brpc::Controller;
        brpc::CallId cid = cntl->call_id();

        char *log_data = new char[redo_log->log_tot_len_];
        switch (redo_log->log_type_)
        {
            case RedoLogType::UPDATE:
                UpdateRedoLogRecord *update_log = reinterpret_cast<UpdateRedoLogRecord *>(redo_log.get());
                update_log->serialize(log_data);
                break;
            default:
                break;
        }
        request.set_log(std::string(log_data, redo_log->log_tot_len_));

        stub.LogWrite(cntl, &request, response, NULL);
    }

private:
    brpc::Channel* log_channel_;
};