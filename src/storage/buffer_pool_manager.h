#pragma once
#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <list>
#include <unordered_map>
#include <vector>
#include <brpc/channel.h>

#include "slice_manager.h"
#include "disk_manager.h"
#include "errors.h"
#include "page.h"
#include "replacer/clock_replacer.h"
#include "replacer/lru_replacer.h"
#include "replacer/replacer.h"
#include "common/common.h"

class BufferPool {
   private:
    size_t pool_size_;      // buffer_pool中可容纳页面的个数，即帧的个数
    Page *pages_;           // buffer_pool中的Page对象数组，在构造空间中申请内存空间，在析构函数中释放，大小为BUFFER_POOL_SIZE
    std::unordered_map<PageId, frame_id_t, PageIdHash> page_table_; // 帧号和页面号的映射哈希表，用于根据页面的PageId定位该页面的帧编号
    std::list<frame_id_t> free_list_;   // 空闲帧编号的链表
    DiskManager *disk_manager_;     // used for storage_pool, no use in compute_pool
    Replacer *replacer_;    // buffer_pool的置换策略，当前赛题中为LRU置换策略
    std::mutex latch_;      // 用于共享数据结构的并发控制
    brpc::Channel* page_channel_;
    SliceMetaManager* slice_mgr_;
    NodeType node_type_;

   public:
    BufferPool(NodeType node_type, size_t pool_size, brpc::Channel* page_channel, DiskManager* disk_manager = nullptr, SliceMetaManager* slice_mgr = nullptr)
        : node_type_(node_type), pool_size_(pool_size), page_channel_(page_channel), disk_manager_(disk_manager), slice_mgr_(slice_mgr) {
        // 为buffer pool分配一块连续的内存空间
        pages_ = new Page[pool_size_];
        // 可以被Replacer改变
        if (REPLACER_TYPE.compare("LRU"))
            replacer_ = new LRUReplacer(pool_size_);
        else if (REPLACER_TYPE.compare("CLOCK"))
            replacer_ = new LRUReplacer(pool_size_);
        else {
            replacer_ = new LRUReplacer(pool_size_);
        }
        // 初始化时，所有的page都在free_list_中
        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_.emplace_back(static_cast<frame_id_t>(i));  // static_cast转换数据类型
        }
    }

    ~BufferPool() {
        delete[] pages_;
        delete replacer_;
    }

    /**
     * @description: 将目标页面标记为脏页
     * @param {Page*} page 脏页
     */
    static void mark_dirty(Page* page) { page->is_dirty_ = true; }

   public: 
    Page* fetch_page(PageId page_id);

    // used for storage_node
    void fetch_page_from_disk(Page* page, PageId page_id, frame_id_t frame_id);

    // used for compute_node
    void fetch_page_from_rpc(Page* page, PageId page_id, frame_id_t frame_id);

    bool unpin_page(PageId page_id, bool is_dirty);

    Page* new_page(PageId* page_id);

    bool delete_page(PageId page_id);

    bool flush_page(PageId page_id);

    void flush_all_pages(int table_id);

    void print_buffer_info() {
        std::cout << "Pool Size: " << pool_size_ << ", Free size: " << free_list_.size() << ", Unpin size: " << replacer_->Size() << std::endl;
    }

   private:
    bool find_victim_page(frame_id_t* frame_id);

    void update_page(Page* page, PageId new_page_id, frame_id_t new_frame_id);
};


class BufferPoolManager {
    public:
    BufferPoolManager(NodeType node_type, size_t pool_size, brpc::Channel* page_channel, DiskManager* disk_manager = nullptr, SliceMetaManager* slice_mgr = nullptr){
        disk_manager_ = disk_manager;
        int size_per_pool = pool_size / BUFFER_POOL_NUM;
        for(size_t i = 0; i < BUFFER_POOL_NUM; ++i)
            buffer_pools_[i] = new BufferPool(node_type, size_per_pool, page_channel, disk_manager, slice_mgr);
    }

    ~BufferPoolManager() {
        for(size_t i = 0; i < BUFFER_POOL_NUM; ++i) delete buffer_pools_[i];
    }

    Page* fetch_page(PageId page_id) {
        return buffer_pools_[page_id.page_no % BUFFER_POOL_NUM]->fetch_page(page_id);
    }

    Page* new_page(PageId* page_id) {
        int page_no = disk_manager_->get_fd2pageno(disk_manager_->get_table_fd((*page_id).table_id));
        return buffer_pools_[page_no % BUFFER_POOL_NUM]->new_page(page_id);
    }

    bool delete_page(PageId page_id) {
        return buffer_pools_[page_id.page_no % BUFFER_POOL_NUM]->delete_page(page_id);
    }

    bool unpin_page(PageId page_id, bool is_dirty) {
        return buffer_pools_[page_id.page_no % BUFFER_POOL_NUM]->unpin_page(page_id, is_dirty);
    }

    void flush_all_pages(int table_id) {
        for(size_t i = 0; i < BUFFER_POOL_NUM; ++i) {
            buffer_pools_[i]->flush_all_pages(table_id);
        }
    }

    /*
        打印每个buffer的页面信息
    */
    void print_buffer_info() {
        for(int i = 0; i < BUFFER_POOL_NUM; ++i) {
            buffer_pools_[i]->print_buffer_info();
        }
    }

    BufferPool* buffer_pools_[BUFFER_POOL_NUM];
    DiskManager* disk_manager_;
};