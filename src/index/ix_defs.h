#pragma once

#include <vector>

#include "system/sm_meta.h"
#include "storage/buffer_pool_manager.h"

constexpr int IX_NO_PAGE = -1;
constexpr int IX_FILE_HDR_PAGE = 0;
// constexpr int IX_LEAF_HEADER_PAGE = 1; // unused
constexpr int IX_INIT_ROOT_PAGE = 1;
constexpr int IX_INIT_NUM_PAGES = 2;
constexpr int IX_MAX_COL_LEN = 512;

class IxFileHdr {
public: 
    int num_pages_;                     // 磁盘文件中页面的数量
    page_id_t root_page_;               // B+树根节点对应的页面号
    int col_num_;                       // 索引包含的字段数量
    std::vector<ColType> col_types_;    // 字段的类型
    std::vector<int> col_lens_;         // 字段的长度
    int col_tot_len_;                   // 索引包含的字段的总长度
    int btree_order_;                   // max key number per internal page, max children number = btree_order+1
    int keys_size_;                     // keys_size = (btree_order + 1) * col_tot_len
    int record_len_;                    // the length of record (fixed), including the record header
    int max_number_of_records_;         // records per leaf page
    page_id_t first_leaf_;              // 首叶节点对应的页号，在上层IxManager的open函数进行初始化，初始化为root page_no
    page_id_t last_leaf_;               // 尾叶节点对应的页号
    int32_t next_record_no_;            // the unique identifier of the record in this table
    int tot_len_;                       // 记录结构体的整体长度

    IxFileHdr() {
        tot_len_ = col_num_ = 0;
        std::cout << "first\n";
    }

    IxFileHdr(int num_pages, page_id_t root_page, int col_num, int col_tot_len, int btree_order, 
                int keys_size, int record_len, int max_number_of_records, page_id_t first_leaf, page_id_t last_leaf, int32_t next_record_no)
                : num_pages_(num_pages), root_page_(root_page), col_num_(col_num), col_tot_len_(col_tot_len), 
                btree_order_(btree_order), keys_size_(keys_size), record_len_(record_len),
                max_number_of_records_(max_number_of_records), first_leaf_(first_leaf), last_leaf_(last_leaf), next_record_no_(next_record_no) {
                    tot_len_ = 0;
                    std::cout << "second\n";
                } 

    void update_tot_len() {
        tot_len_ = 0;
        tot_len_ += sizeof(page_id_t) * 3 + sizeof(int) * 8 + sizeof(int32_t);
        tot_len_ += sizeof(ColType) * col_num_ + sizeof(int) * col_num_;
    }

    void serialize(char* dest) {
        std::cout << "serialize ix_file_hdr\n";
        int offset = 0;
        memcpy(dest + offset, &tot_len_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &num_pages_, sizeof(int));
        std::cout << "num_pages: " << num_pages_ << "\n";
        offset += sizeof(int);
        memcpy(dest + offset, &root_page_, sizeof(page_id_t));
        offset += sizeof(page_id_t);
        memcpy(dest + offset, &col_num_, sizeof(int));
        offset += sizeof(int);
        for(int i = 0; i < col_num_; ++i) {
            memcpy(dest + offset, &col_types_[i], sizeof(ColType));
            offset += sizeof(ColType);
        }
        for(int i = 0; i < col_num_; ++i) {
            memcpy(dest + offset, &col_lens_[i], sizeof(int));
            offset += sizeof(int);
        }
        memcpy(dest + offset, &col_tot_len_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &btree_order_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &keys_size_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &record_len_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &max_number_of_records_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &first_leaf_, sizeof(page_id_t));
        std::cout << "first leaf: " << first_leaf_ << "\n";
        offset += sizeof(page_id_t);
        memcpy(dest + offset, &last_leaf_, sizeof(page_id_t));
        std::cout << "last_leaf: " << last_leaf_ << "\n";
        offset += sizeof(page_id_t);
        memcpy(dest + offset, &next_record_no_, sizeof(int32_t));
        std::cout << "next_record_no: " << next_record_no_ << "\n";
        offset += sizeof(int32_t);
        assert(offset == tot_len_);
    }

    void deserialize(char* src) {
        int offset = 0;
        tot_len_ = *reinterpret_cast<const int*>(src + offset);
        std::cout << "tot_len: " << tot_len_ << "\n"; 
        offset += sizeof(int);
        num_pages_ = *reinterpret_cast<const int*>(src + offset);
        std::cout << "num_pages: " << num_pages_ << "\n";
        offset += sizeof(int);
        root_page_ = *reinterpret_cast<const page_id_t*>(src + offset);
        std::cout << "root_page: " << root_page_ << "\n";
        offset += sizeof(page_id_t);
        col_num_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        std::cout << col_num_ << "\n";
        for(int i = 0; i < col_num_; ++i) {
            // col_types_[i] = *reinterpret_cast<const ColType*>(src + offset);
            ColType type = *reinterpret_cast<const ColType*>(src + offset);
            offset += sizeof(ColType);
            col_types_.push_back(type);
        }
        for(int i = 0; i < col_num_; ++i) {
            // col_lens_[i] = *reinterpret_cast<const int*>(src + offset);
            int len = *reinterpret_cast<const int*>(src + offset);
            offset += sizeof(int);
            col_lens_.push_back(len);
        }
        col_tot_len_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        btree_order_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        keys_size_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        record_len_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        max_number_of_records_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        first_leaf_ = *reinterpret_cast<const page_id_t*>(src+ offset);
        std::cout << "first_leaf: " << first_leaf_ << "\n";
        offset += sizeof(page_id_t);
        last_leaf_ = *reinterpret_cast<const page_id_t*>(src + offset);
        std::cout << "last_leaf: " << last_leaf_ << "\n";
        offset += sizeof(page_id_t);
        next_record_no_ = *reinterpret_cast<const int32_t*>(src + offset);
        std::cout << "next_record_no: " << next_record_no_ << "\n";
        offset += sizeof(int32_t);
        assert(offset == tot_len_);
    }
};

// both LeafPage and InternalPage use the IxPageHdr
class IxPageHdr {
public:
    page_id_t parent_;              // the page_no of the parent
    // For a internal node, num_key represents current keys (always equals to #child - 1) the number of keys，key_idx∈[0,num_key)
    // For leaf node, it is not used
    int num_key_;                    
    int num_records_;               // used for leaf page, number of records that are not deleted
    int tot_num_records_;           // used for leaf page, number of all records, including deleted records
    int32_t free_space_offset_;     // used for leaf page, the offset of the free space, starting from the UserRecord space
    int32_t first_deleted_offset_;  // used for leaf page, records are inserted to the first_deleted slot
    bool is_leaf_;                  // a leaf node or an internal node
    page_id_t prev_page_;           // pointer to the previous page at the same level
    page_id_t next_page_;           // pointer to the next page at the same level
};

// used for leaf page, located in the end of the page
// contains an array of <key, offset>, which are storaged in an ascending order
// class IxPageDirectory {
//     char* record_key_;              // the primariy key of the record
//     int32_t record_offset_;         // the offset in the page of the record data
// };

// 这个其实和Rid结构一样，上层调用过的Iid可以都直接改成Rid？
// class Iid {
// public:
//     page_id_t page_no;
//     int slot_no;

//     friend bool operator==(const Iid &x, const Iid &y) { return x.page_no == y.page_no && x.slot_no == y.slot_no; }

//     friend bool operator!=(const Iid &x, const Iid &y) { return !(x == y); }
// };