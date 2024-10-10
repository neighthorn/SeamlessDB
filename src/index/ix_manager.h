#pragma once

#include <memory>
#include <string>

#include "system/sm_meta.h"
#include "ix_defs.h"
#include "ix_index_handle.h"

class IxManager {
   private:
    DiskManager *disk_manager_;
    BufferPoolManager *buffer_pool_manager_;

   public:
    IxManager(BufferPoolManager *buffer_pool_manager, DiskManager* disk_manager = nullptr)
        : buffer_pool_manager_(buffer_pool_manager), disk_manager_(disk_manager) {}

    // std::string get_index_name(const std::string &filename, int index_no) {
    //     return filename + '.' + std::to_string(index_no) + ".idx";
    // }
    std::string get_index_name(const std::string &filename, const std::vector<std::string>& index_cols) {
        std::string index_name = filename;
        for(size_t i = 0; i < index_cols.size(); ++i) 
            index_name += "_" + index_cols[i];
        index_name += ".idx";

        return index_name;
    }

    std::string get_index_name(const std::string &filename, const std::vector<ColMeta>& index_cols) {
        std::string index_name = filename;
        for(size_t i = 0; i < index_cols.size(); ++i) 
            index_name += "_" + index_cols[i].name;
        index_name += ".idx";

        return index_name;
    }

    bool exists(const std::string &filename, const std::vector<ColMeta>& index_cols) {
        auto ix_name = get_index_name(filename, index_cols);
        return disk_manager_->is_file(ix_name);
    }

    bool exists(const std::string &filename, const std::vector<std::string>& index_cols) {
        auto ix_name = get_index_name(filename, index_cols);
        return disk_manager_->is_file(ix_name);
    }

    void create_index(const std::string &filename, const std::vector<ColMeta>& index_cols, const TabMeta& table_meta) {
        std::string ix_name = get_index_name(filename, index_cols);
        // Create index file
        disk_manager_->create_file(ix_name);
        // Open index file
        int fd = disk_manager_->open_file(ix_name);
        int record_len = table_meta.record_length_ + sizeof(RecordHdr); // the length of record in index, including record header

        // Create file header and write to file
        // Theoretically we have: |page_hdr| + (|attr| + |rid|) * n <= PAGE_SIZE
        // but we reserve one slot for convenient inserting and deleting, i.e.
        // |page_hdr| + (|attr| + |rid|) * (n + 1) <= PAGE_SIZE
        int col_tot_len = 0;
        int col_num = index_cols.size();
        for(auto& col: index_cols) {
            col_tot_len += col.len;
        }
        if (col_tot_len > IX_MAX_COL_LEN) {
            throw InvalidColLengthError(col_tot_len);
        }
        // btree_order_ represents the max number of <key, child point> pairs in Internal Page
        // 根据 |page_hdr| + (|attr| + |page_id_t|) * (n + 1) <= PAGE_SIZE 求得n的最大值btree_order
        // 即 n <= btree_order，那么btree_order就是每个结点最多可插入的键值对数量（实际还多留了一个空位，但其不可插入）
        int btree_order = static_cast<int>((PAGE_SIZE - sizeof(IxPageHdr)) / (col_tot_len + sizeof(page_id_t)) - 1);
        assert(btree_order > 2);

        // max_number_of_records_ represents the max number of records in leaf page
        // |IxPageHdr| + record_len * (n+1) + (col_tot_len + |int32_t|) * (n + 1) <= PAGE_SIZE
        // we set the page_directory to n+1 in order to finish the split operation for LeafPage easier
        int max_number_of_records = static_cast<int>((PAGE_SIZE - sizeof(IxPageHdr)) / (record_len + col_tot_len + sizeof(int)) - 1);

        IxFileHdr* fhdr = new IxFileHdr(IX_INIT_NUM_PAGES, IX_INIT_ROOT_PAGE,
                                col_num, col_tot_len, btree_order, (btree_order + 1) * col_tot_len, record_len,
                                max_number_of_records, IX_INIT_ROOT_PAGE, IX_INIT_ROOT_PAGE, 0);
        for(int i = 0; i < col_num; ++i) {
            fhdr->col_types_.push_back(index_cols[i].type);
            fhdr->col_lens_.push_back(index_cols[i].len);
        }
        fhdr->update_tot_len();
        
        char* data = new char[PAGE_SIZE];
        memset(data, 0, PAGE_SIZE);
        fhdr->serialize(data);

        disk_manager_->write_page(fd, IX_FILE_HDR_PAGE, data, PAGE_SIZE);

        char page_buf[PAGE_SIZE];  // 在内存中初始化page_buf中的内容，然后将其写入磁盘
        memset(page_buf, 0, PAGE_SIZE);
        // Create root node and write to file
        {
            memset(page_buf, 0, PAGE_SIZE);
            auto phdr = reinterpret_cast<IxPageHdr *>(page_buf);
            *phdr = {
                .parent_ = IX_NO_PAGE,
                .num_key_ = 0,
                .num_records_ = 0,
                .tot_num_records_ = 0,
                .free_space_offset_ = 0,                // starting from the UserRecord space
                .first_deleted_offset_ = INVALID_OFFSET,
                .is_leaf_ = true,
                .prev_page_ = IX_NO_PAGE,
                .next_page_ = IX_NO_PAGE,
            };
            // Must write PAGE_SIZE here in case of future fetch_node()
            disk_manager_->write_page(fd, IX_INIT_ROOT_PAGE, page_buf, PAGE_SIZE);
        }

        disk_manager_->set_fd2pageno(fd, IX_INIT_NUM_PAGES);  // DEBUG

        // Close index file
        disk_manager_->close_file(fd);
    }

    void destroy_index(const std::string &filename, const std::vector<ColMeta>& index_cols) {
        std::string ix_name = get_index_name(filename, index_cols);
        disk_manager_->destroy_file(ix_name);
    }

    void destroy_index(const std::string &filename, const std::vector<std::string>& index_cols) {
        std::string ix_name = get_index_name(filename, index_cols);
        disk_manager_->destroy_file(ix_name);
    }

    // 注意这里打开文件，创建并返回了index file handle的指针
    std::unique_ptr<IxIndexHandle> open_index(const std::string &filename, const std::vector<ColMeta>& index_cols, const TabMeta& table_meta) {
        std::string ix_name = get_index_name(filename, index_cols);
        
        int fd;
        
        fd = disk_manager_->open_file(ix_name);
        std::cout << "open index, ix_name: " << ix_name << ", fd: " << fd << "\n";
        disk_manager_->set_table_fd(table_meta.table_id_, fd);
        
        // std::cout << "set table " << table_meta.table_id_ << "'s fd as " << fd << "\n";
        return std::make_unique<IxIndexHandle>(disk_manager_, buffer_pool_manager_, fd, table_meta);
    }

    std::unique_ptr<IxIndexHandle> open_index(const std::string &filename, const std::vector<std::string>& index_cols, const TabMeta& table_meta) {
        std::string ix_name = get_index_name(filename, index_cols);
        int fd = disk_manager_->open_file(ix_name);
        disk_manager_->set_table_fd(table_meta.table_id_, fd);
        // std::cout << "set table " << table_meta.table_id_ << "'s fd as " << fd << "\n";
        return std::make_unique<IxIndexHandle>(disk_manager_, buffer_pool_manager_, fd, table_meta);
    }

    // no use
    void close_index(const IxIndexHandle *ih) {
        // 由于在更新file_hdr的时候直接更新的数据结构，没有更新页面，所以要最后把数据结构写到页面中
        Page* page = buffer_pool_manager_->fetch_page(PageId{.table_id = ih->table_meta_.table_id_, .page_no = IX_FILE_HDR_PAGE});
        ih->file_hdr_->serialize(page->get_data());
        buffer_pool_manager_->unpin_page(PageId{.table_id = ih->table_meta_.table_id_, .page_no = IX_FILE_HDR_PAGE}, true);

        // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
        buffer_pool_manager_->flush_all_pages(ih->table_meta_.table_id_);

        
        // char* data = new char[ih->file_hdr_->tot_len_];
        // ih->file_hdr_->serialize(data);
        // // std::cout << "ih->fd_: " << ih->fd_ << ", disk_manager.get_fd: " << disk_manager_->get_table_fd(ih->table_meta_.table_id_) << "\n";
        // disk_manager_->write_page(ih->fd_, IX_FILE_HDR_PAGE, data, ih->file_hdr_->tot_len_);
        
        // char* read_data = new char[ih->file_hdr_->tot_len_];
        // disk_manager_->read_page(ih->fd_, IX_FILE_HDR_PAGE, read_data, ih->file_hdr_->tot_len_);
        // IxFileHdr* file_hdr = new IxFileHdr();
        // std::cout << "****************************\n";
        // std::cout << "read from disk:\n";
        // file_hdr->deserialize(read_data);
        // std::cout << "\nread from buffer:\n";
        
        // file_hdr->deserialize(page->get_data());
        // std::cout << "****************************\n";
        
        disk_manager_->close_file(ih->fd_);
    }
};