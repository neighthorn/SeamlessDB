#pragma once

#include "record/record.h"
#include "common/context.h"
#include "ix_node_handle.h"
#include "transaction/transaction.h"
#include "multi_version/multi_version_manager.h"

enum class Operation { FIND = 0, INSERT, DELETE };  // 三种操作：查找、插入、删除

static const bool binary_search = false;


/**
 * the internal pages of the index are always in memory
 * */ 
class IxIndexHandle {
    friend class IxScan;
    // friend class My_IxScan;  // TEST
    friend class IxManager;

   private:
    DiskManager *disk_manager_;
    BufferPoolManager *buffer_pool_manager_;
    int fd_;                
    IxFileHdr* file_hdr_;  // 存了root_page，但其初始化为2（第0页存FILE_HDR_PAGE，第1页存LEAF_HEADER_PAGE）
    std::mutex root_latch_;
    TabMeta table_meta_;                       // used for table scan and record operations

   public:
    IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd, const TabMeta& table_meta);

    // for test
    int32_t get_next_record_no() { return file_hdr_->next_record_no_; }

    // used for scan
    std::unique_ptr<Record> get_record(const Rid& rid, Context* context);

    // used for insert
    Rid insert_record(const char* key, char* record, Context* context);

    // used for log replay 
    // 日志回放是串行执行，这里我就不加锁了context设置为nullptr
    void replay_insert_record(Rid rid, const char *key, char *record, Context *context = nullptr);

    void delete_record(const Rid& rid, Context* context);

    void rollback_delete_record(const Rid& rid, Context* context);

    void update_record(const Rid& rid, char* raw_data, Context* context);
    
    // for search
    std::pair<IxNodeHandle *, bool> find_leaf_page(const char *key, Operation operation, Transaction *transaction,
                                                 bool find_first = false);

    // for insert
    Rid insert_entry(const char* key, const char* record_value, Transaction *transaction);

    IxNodeHandle *split(IxNodeHandle *node);

    void insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node, Transaction *transaction);

    // for delete
    // void delete_entry(const char *key, const Rid &value, Transaction *transaction);

    // bool coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction = nullptr,
    //                             bool *root_is_latched = nullptr);
    // bool adjust_root(IxNodeHandle *old_root_node);

    // void redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index);

    // bool coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
    //               Transaction *transaction, bool *root_is_latched);

    // used for execution
    Rid lower_bound(const char *key);

    Rid upper_bound(const char *key);

    Rid leaf_end() const;

    Rid leaf_begin() const;

   private:
    // 辅助函数
    void update_root_page_no(page_id_t root) { file_hdr_->root_page_ = root; }

    bool is_empty() const { return file_hdr_->root_page_ == IX_NO_PAGE; }

    // bool is_empty() const {
    //     IxNodeHandle *root = fetch_node(file_hdr_.root_page);
    //     int sz = root->get_size();
    //     buffer_pool_manager_->unpin_page(root->get_page_id(), false);
    //     return sz == 0;
    //     // return file_hdr_.root_page == IX_NO_PAGE;
    // }

    // for concurrent index, using crabbing protocol
    void unlock_pages(Transaction *transaction);

    void unlock_unpin_pages(Transaction *transaction);

    bool is_safe(IxNodeHandle *node, Operation op);

    // for get/create node
    IxNodeHandle *fetch_node(int page_no) const;

    IxNodeHandle *create_node();

    // for maintain data structure
    void maintain_parent(IxNodeHandle *node);

    // void erase_leaf(IxNodeHandle *leaf);

    void release_node_handle(IxNodeHandle &node);

    void maintain_child(IxNodeHandle *node, int child_idx);

    // for index test
    // Rid get_rid(const Iid &iid) const;
};