#include "storage_rpc.h"

namespace storage_service {
    StoragePoolImpl::StoragePoolImpl(DiskManager* disk_manager, LogStore *log_store, ShareStatus *share_status, BufferPoolManager* buffer_pool_mgr)
        : disk_manager_(disk_manager), log_store_(log_store), share_status_(share_status), buffer_pool_manager_(buffer_pool_mgr) {}

    StoragePoolImpl::~StoragePoolImpl(){}

    void StoragePoolImpl::LogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::LogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);

        // deserialize log
        // auto log_strs = request->log();
        // RedoLogRecord redo_log_hdr;
        // for(auto& str: log_strs) {
        //     redo_log_hdr.deserialize(str.c_str());
        //     redo_log_hdr.format_print();
        //     log_store_->write_log(redo_log_hdr.lsn_,str);
        //     if(redo_log_hdr.is_persisit_) {
        //         share_status_->need_replay_lsn_ = redo_log_hdr.lsn_;
        //     }
        // }
        auto log_buf = request->log();
        // std::cout << "receive log_write message from compute node, log_message is: " << request->log() << "\n";
        int len = log_buf.length();
        int off = 0;
        const char* src = log_buf.c_str();
        while(off < len) {
            RedoLogType log_type = *reinterpret_cast<const RedoLogType*>(src + off + REDO_LOG_TYPE_OFFSET);
            int lsn = *reinterpret_cast<const lsn_t*>(src + off + REDO_LOG_LSN_OFFSET);
            int log_tot_len = *reinterpret_cast<const uint32_t*>(src + off + REDO_LOG_TOTLEN_OFFSET);
            int is_persisit = *reinterpret_cast<const bool*>(src + off + REDO_LOG_IS_PERSIST_OFFSET);
            // std::cout << "lsn: " << lsn << ", log_tot_len: " << log_tot_len << ", is_persist: " << is_persisit << "\n";
            log_store_->write_log(lsn, log_buf.substr(off, log_tot_len));
            if(is_persisit) {
                share_status_->need_replay_lsn_ = lsn;
            }
            off += log_tot_len;
        }

        return;
    }

    void StoragePoolImpl::GetOldPage(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetOldPageRequest* request,
                       ::storage_service::GetOldPageResponse* response,
                       ::google::protobuf::Closure* done) {
        // std::cout << "receive get_old_page message from compute node.\n";
        brpc::ClosureGuard done_guard(done);

        /**
         * TODO: 优化：这里应该把最新的page放在storage节点的buffer_pool里面，从buffer里面读而不是从磁盘读
        */

        for(int i = 0; i < request->page_id().size(); ++i) {
            int table_id = request->page_id()[i].table_id();
            int fd = disk_manager_->get_table_fd(table_id);
            int page_no = request->page_id()[i].page_no();
            // std::cout << "table_id: " << table_id << ", page_id: " << request->page_id()[i].page_no();
            char data[PAGE_SIZE];
            try{
                disk_manager_->read_page(fd, page_no, data, PAGE_SIZE);
                response->add_data(std::move(std::string(data, PAGE_SIZE)));
            } catch(RMDBError& e) {
                std::cerr << "Error: " << e.what() << "\n";
            }
        }

        // response->set_data(return_pages);
        // std::cout << "success to get_old_pages.\n";
        return;
    }

    void StoragePoolImpl::GetLatestPage(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetLatestPageRequest* request,
                       ::storage_service::GetLatestPageResponse* response,
                       ::google::protobuf::Closure* done) {
        // std::cout << "receive get_latest_page message from compute node.\n";
        brpc::ClosureGuard done_guard(done);

        /**
         * TODO: 优化：这里应该把最新的page放在storage节点的buffer_pool里面，从buffer里面读而不是从磁盘读
        */
    //    std::cout << "page request: pageid={" << request->page_id()[0].table_id() << "," << request->page_id()[0].page_no() << "\n";

        for(int i = 0; i < request->page_id().size(); ++i) {
            int lsn = request->latest_lsn()[i];
            int table_id = request->page_id()[i].table_id();
            // int fd = disk_manager_->get_table_fd(table_id);
            int page_no = request->page_id()[i].page_no();
            // std::cout << "table_id: " << table_id << ", page_id: " << request->page_id()[i].page_no() << ", lsn: " << lsn << "\n";
            char data[PAGE_SIZE];
            // disk_manager_->read_page(fd, page_no, data, PAGE_SIZE);
            while(lsn > share_status_->current_replay_lsn_) {
                // wait
                // std::cout << "waitingforlogreplay: " << "lsn=" << lsn << "replaylsn=" << share_status_->current_replay_lsn_<<"\n";
            }
            Page* page = buffer_pool_manager_->fetch_page(PageId{table_id, page_no});
            memcpy(data, page->get_data(), PAGE_SIZE);
            buffer_pool_manager_->unpin_page(PageId{table_id, page_no}, false);
            response->add_data(std::move(std::string(data, PAGE_SIZE)));
        }
        // std::cout << "success to get_new_pages.\n";
        return;
    }

    void StoragePoolImpl::GetPersistLsn(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetPersistLsnRequest* request,
                       ::storage_service::GetPersistLsnResponse* response,
                       ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);

        response->set_persist_lsn(share_status_->need_replay_lsn_);
        return;
    }
}