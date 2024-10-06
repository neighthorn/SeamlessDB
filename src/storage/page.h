/*
 * @Description: 
 */
#pragma once

#include "common/config.h"
#include "common/rwlatch.h"

/**
 * @description: 存储层每个Page的id的声明
 */
struct PageId {
    int table_id;  //  Page所在的磁盘文件开启后的文件描述符, 来定位打开的文件在内存中的位置
    page_id_t page_no = INVALID_PAGE_ID;

    friend bool operator==(const PageId &x, const PageId &y) { return x.table_id == y.table_id && x.page_no == y.page_no; }
    bool operator<(const PageId& x) const {
        if(table_id < x.table_id) return true;
        return page_no < x.page_no;
    }

    std::string toString() {
        return "{table_id: " + std::to_string(table_id) + " page_no: " + std::to_string(page_no) + "}"; 
    }

    inline int64_t Get() const {
        return ((static_cast<int64_t>(table_id) << 32) | page_no);
    }
};

// PageId的自定义哈希算法, 用于构建unordered_map<PageId, frame_id_t, PageIdHash>
struct PageIdHash {
    size_t operator()(const PageId &x) const { return (x.table_id << 16) | x.page_no; }
};

template <>
struct std::hash<PageId> {
    size_t operator()(const PageId &obj) const { return std::hash<int64_t>()(obj.Get()); }
};


class Page {
    friend class BufferPool;

   public:
    
    Page() { reset_memory(); }

    ~Page() = default;

    PageId get_page_id() const { return id_; }

    inline char *get_data() { return data_; }

    bool is_dirty() const { return is_dirty_; }

    /**
     * @description: 获取page的写锁
     */
    inline void WLatch() { rwlatch_.WLock(); }

    /**
     * @description: 释放page的写锁 
     */
    inline void WUnlatch() { rwlatch_.WUnlock(); }

    /**
     * @description: 获取page的读锁
     */
    inline void RLatch() { rwlatch_.RLock(); }

    /**
     * @description: 释放page的读锁
     */
    inline void RUnlatch() { rwlatch_.RUnlock(); }

    static constexpr size_t OFFSET_PAGE_START = 0;
    static constexpr size_t OFFSET_LSN = 0;
    static constexpr size_t OFFSET_PAGE_HDR = 4;

    inline lsn_t get_page_lsn() { return *reinterpret_cast<lsn_t *>(get_data() + OFFSET_LSN) ; }

    inline void set_page_lsn(lsn_t page_lsn) { memcpy(get_data() + OFFSET_LSN, &page_lsn, sizeof(lsn_t)); }

   private:
    void reset_memory() { memset(data_, 0, PAGE_SIZE); }  // 将data_的PAGE_SIZE个字节填充为0

    /** page的唯一标识符 */
    PageId id_;

    /** The actual data that is stored within a page.
     *  该页面在bufferPool中的偏移地址
     */
    char data_[PAGE_SIZE] = {};

    /** 脏页判断 */
    bool is_dirty_ = false;

    /** The pin count of this page. */
    int pin_count_ = 0;

    /** Page latch. */
    ReaderWriterLatch rwlatch_;
};