#pragma once

// #include "common/macros.h"
#include "storage/buffer_pool_manager.h"


// multi version record

constexpr int MULTI_NO_PAGE = -1;
constexpr int MULTI_FILE_HDR_PAGE = 0;
constexpr int MULTI_FIRST_RECORD_PAGE = 1;
constexpr int MULTI_MAX_RECORD_SIZE = 512;

/* 文件头，记录表数据文件的元信息，写入磁盘中文件的第0号页面 */
struct MultiVersionFileHdr {
    int record_size;            // 表中每条记录的大小，由于不包含变长字段，因此当前字段初始化后保持不变
    int num_pages;              // 文件中分配的页面个数（初始化为1）
    int num_records_per_page;   // 每个页面最多能存储的元组个数
    int first_free_page_no;     // 文件中当前第一个包含空闲空间的页面号（初始化为-1）
    int bitmap_size;            // 每个页面bitmap大小
};

/* 表数据文件中每个页面的页头，记录每个页面的元信息 */
struct MultiVersionPageHdr {
    int next_free_page_no;  // 当前页面满了之后，下一个包含空闲空间的页面号（初始化为-1）
    int num_records;        // 当前页面中当前已经存储的记录个数（初始化为0）
};

/* 表中的记录 */
struct MultiVersionRecord {
    char* data;  // 记录的数据
    int size;    // 记录的大小
    bool allocated_ = false;    // 是否已经为数据分配空间

    MultiVersionRecord() = default;

    MultiVersionRecord(const MultiVersionRecord& other) {
        size = other.size;
        data = new char[size];
        memcpy(data, other.data, size);
        allocated_ = true;
    };

    MultiVersionRecord &operator=(const MultiVersionRecord& other) {
        size = other.size;
        data = new char[size];
        memcpy(data, other.data, size);
        allocated_ = true;
        return *this;
    };

    MultiVersionRecord(int size_) {
        size = size_;
        data = new char[size_];
        allocated_ = true;
    }

    MultiVersionRecord(int size_, char* data_) {
        size = size_;
        data = new char[size_];
        memcpy(data, data_, size_);
        allocated_ = true;
    }

    void SetData(char* data_) {
        memcpy(data, data_, size);
    }

    void Deserialize(const char* data_) {
        size = *reinterpret_cast<const int*>(data_);
        if(allocated_) {
            delete[] data;
        }
        data = new char[size];
        memcpy(data, data_ + sizeof(int), size);
    }

    ~MultiVersionRecord() {
        if(allocated_) {
            delete[] data;
        }
        allocated_ = false;
        data = nullptr;
    }
};
