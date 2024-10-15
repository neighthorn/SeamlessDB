#include "ix_node_handle.h"

IxNodeHandle::IxNodeHandle(IxFileHdr* file_hdr, Page* page) : file_hdr_(file_hdr), page_(page) {
    page_hdr_ = reinterpret_cast<IxPageHdr*>(page->get_data());
    records_ = page_->get_data() + sizeof(IxPageHdr);
    // std::cout << "the records_ offset is: " << sizeof(IxPageHdr);
    page_directory_ = records_ + (file_hdr->max_number_of_records_ + 1) * file_hdr->record_len_;
    keys_ = page_->get_data() + sizeof(IxPageHdr);
    child_nodes_ = keys_ + file_hdr_->keys_size_;
}

int IxNodeHandle::internal_upper_bound(const char *target) {
    int low = 0, high = page_hdr_->num_key_;
    while (low < high) {
        int mid = (low + high) / 2;
        char *key_addr = internal_key_at(mid);
        if (ix_compare(target, key_addr, file_hdr_->col_types_, file_hdr_->col_lens_) < 0) {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    return low;
}

int IxNodeHandle::internal_lower_bound(const char *target) {
    int low = 0, high = page_hdr_->num_key_;
    while (low < high) {
        int mid = (low + high) / 2;
        char *key_addr = internal_key_at(mid);
        if (ix_compare(target, key_addr, file_hdr_->col_types_, file_hdr_->col_lens_) <= 0) {
            // target<=key_addr，说明key_addr还需要变小
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    return low;
}

/**
 * @brief 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::internal_insert_pairs(int pos, const char* key, const char* children, int n) {
    assert(pos <= page_hdr_->num_key_);

    // 1 insert keys
    char *key_slot = internal_key_at(pos);  // pos位置的首地址为key_slot
    // key_slot的前(num_key-pos)位向后移动n位（这里的每位长度为col_len）
    memmove(key_slot + n * file_hdr_->col_tot_len_, key_slot, (page_hdr_->num_key_ - pos) * file_hdr_->col_tot_len_);
    // key的前n位插入到key_slot的前n位
    memcpy(key_slot, key, n * file_hdr_->col_tot_len_);

    // 2 insert rids
    // Rid *rid_slot = get_rid(pos);
    char* child_slot = internal_value_at(pos);
    memmove(child_slot + n * sizeof(page_id_t), child_slot, (page_hdr_->num_key_ - pos) * sizeof(page_id_t));
    memcpy(child_slot, children, n * sizeof(page_id_t));

    // 3 update num_key
    page_hdr_->num_key_ += n;
}

// 此函数由parent调用，寻找child，返回child在parent中的rid_idx∈[0,page_hdr->num_key)
int IxNodeHandle::internal_find_child(IxNodeHandle *child) {
    // printf("node=%d  child=%d\n", get_page_no(), child->get_page_no());
    assert(get_page_no() != child->get_page_no());
    int rid_idx;
    for (rid_idx = 0; rid_idx < page_hdr_->num_key_; rid_idx++) {
        // printf("page1=%d page2=%d\n", (*get_rid(rid_idx)).page_no, child->get_page_no());
        if (internal_child_page_at(rid_idx) == child->get_page_no()) {
            break;
        }
    }
    assert(rid_idx < page_hdr_->num_key_);
    return rid_idx;
}

/**
 * Used for the split operation of leaf page.
 * Insert record_number records into the leaf page, the records are continuous
 * keys are inserted into the pos slot of the page_directory
 * For example, we have to insert two records into the second and third slot in the page,
 * the record_number is 2, and the pos equals to 1
 * origin:
 * |Record|Record||Record|
 * |DirEntry0|DirEntry1|DirEntry2|
 * after insert:
 * |Record|Record|Record|Record|Record|
 * |DirEntry0|DirInsert1|DirInsert2|DirEntry1|DirEntry2|
 * Despite the movement of directory entries, we have to copy the records' data at the end of the last record,
 * and update the next_record_offset pointer of corresponding records at the same time.
*/
void IxNodeHandle::leaf_insert_continous_records(int pos, const char* extern_page_record, const char* begin_directory, int record_number) {
    assert(record_number + page_hdr_->tot_num_records_ <= file_hdr_->max_number_of_records_);

    // the pos slot in page_directory
    int directory_size = file_hdr_->col_tot_len_ + sizeof(int32_t);
    char* directory_slot = page_directory_ + pos * directory_size;
    int move_bytes = (page_hdr_->tot_num_records_ - pos) * directory_size;
    // move the original [pos, tot_num_records) record_number slots back
    memmove(directory_slot + record_number * directory_size, directory_slot, move_bytes);
    // copy the record_number directory entries to the [pos, pos + record_number) slots
    memcpy(directory_slot, begin_directory, record_number * directory_size);

    // find the next insert position
    // this function is only used for split function, therefore, the new records
    // can be inserted to the free space directly without considering the deleted space
    char* record_insert_slot = records_ + page_hdr_->free_space_offset_;
    // find the record location whose order is pos-1
    int32_t prev_record_offset;
    // prev_record represents the record whose next_record_offset has to be updated
    char* prev_record;
    if(pos > 0) {
        prev_record_offset = *(int32_t*) (directory_slot - sizeof(int32_t));
        prev_record = records_ + prev_record_offset;
    }
    // to_insert_record represents the record's location which is going to be inserted
    int begin_record_offset = *(int32_t*)(begin_directory + file_hdr_->col_tot_len_);
    const char* to_insert_record = extern_page_record + begin_record_offset;
    for(int i = 0; i < record_number; ++i) {
        // copy record's data
        memcpy(record_insert_slot, to_insert_record, file_hdr_->record_len_);
        // update the corresponding directory entry
        *(int32_t*)(directory_slot + file_hdr_->col_tot_len_) = page_hdr_->free_space_offset_;
        // update previous record's next_record_offset
        if(!(pos == 0 && i ==0))
            *(int32_t*)prev_record = page_hdr_->free_space_offset_;
        prev_record = record_insert_slot;

        if(i == record_number - 1) break;

        // update the next_insert_offset, next insert slot location and the next page_directory entry location
        record_insert_slot += file_hdr_->record_len_;
        page_hdr_->free_space_offset_ += file_hdr_->record_len_;
        directory_slot += directory_size;
        // update the to_insert_record's location
        /**
         * TODO: bug?
        */
        to_insert_record = extern_page_record + *(int32_t*) to_insert_record;
    }

    if(page_hdr_->tot_num_records_ - pos > 0) {
        // update the next_record_offset of the last inserted record
        *(int32_t*)prev_record = *(int32_t*)(directory_slot + directory_size + file_hdr_->col_tot_len_);
    }
    else {
        // the last record, set the next_record_offset to -1
        *(int32_t*)prev_record = INVALID_OFFSET;
    }

    page_hdr_->tot_num_records_ += record_number;
}

int IxNodeHandle::leaf_directory_lower_bound(const char* target) {
    int low = 0, high = page_hdr_->tot_num_records_;
    while(low < high) {
        int mid = (low + high) >> 1;
        char* dir_entry_addr = leaf_get_directory_entry_at(mid);
        if(ix_compare(target, dir_entry_addr, file_hdr_->col_types_, file_hdr_->col_lens_) <= 0) {
            high = mid;
        }
        else {
            low = mid + 1;
        }
    }
    return low;
}

int IxNodeHandle::leaf_directory_upper_bound(const char* target) {
    int low = 0, high = page_hdr_->tot_num_records_;
    while(low < high) {
        int mid = (low + high) >> 1;
        char* dir_entry_addr = leaf_get_directory_entry_at(mid);
        if(ix_compare(target, dir_entry_addr, file_hdr_->col_types_, file_hdr_->col_lens_) < 0) {
            high = mid;
        }
        else {
            low = mid + 1;
        }
    }
    return low;
}

// insert the primary key of the record into the page_directory, 
// and return the index of the inserted directory entry
int IxNodeHandle::leaf_insert_key_record(const char* key, const char* record_value) {
    // get the first dir >= key
    int insert_index = leaf_directory_lower_bound(key);
    // std::cout << "insert index: " << insert_index << std::endl;
    int dir_entry_len = file_hdr_->col_tot_len_ + sizeof(int32_t);  // key + offset
    char* insert_dir_entry_slot = leaf_get_directory_entry_at(insert_index);
    
    /**
     * TODO: 如果已经被删除了，是可以插入进去的
    */
    // 插入的key已经存在
    if(insert_index < page_hdr_->tot_num_records_ &&
        ix_compare(insert_dir_entry_slot, key, file_hdr_->col_types_, file_hdr_->col_lens_) == 0) {
        // if()
        std::cout << "Error: the key has already existed in the leaf node!\n";
        assert(0);
        return page_hdr_->tot_num_records_;
    }

    // insert a dir_entry
    // memmove(insert_dir_entry_slot + dir_entry_len, insert_dir_entry_slot, dir_entry_len);
    memmove(insert_dir_entry_slot + dir_entry_len, insert_dir_entry_slot, dir_entry_len * (page_hdr_->tot_num_records_ - insert_index));
    memcpy(insert_dir_entry_slot, key, file_hdr_->col_tot_len_);

    // insert the record data
    // get insert location
    int insert_offset = leaf_get_insert_offset();
    assert(insert_offset != INVALID_OFFSET);
    // update free_space_offset and first_deleted_offset in page_hdr
    leaf_update_insert_offset();
    char* record_insert_slot = records_ + insert_offset;
    memcpy(record_insert_slot, record_value, file_hdr_->record_len_);
    // std::cout << "record insert offset: " << insert_offset << "\n";
    // std::cout << "insert record's values: " << (*(int32_t*)record_value) << "\n";

    // update the record_offset in dir_entry
    *(int32_t*)(insert_dir_entry_slot + file_hdr_->col_tot_len_) = insert_offset;
    
    // update the previous record's next_record_offset
    if(insert_index > 0) {
        int32_t previous_record_offset = *(int32_t*)(leaf_get_directory_entry_at(insert_index - 1) + file_hdr_->col_tot_len_);
        *(int32_t*)(records_ + previous_record_offset) = insert_offset;
    }

    // update current record's next_record_offset
    if(insert_index < page_hdr_->tot_num_records_) {
        int32_t next_record_offset = *(int32_t*)(leaf_get_directory_entry_at(insert_index + 1) + file_hdr_->col_tot_len_);
        *(int32_t*)record_insert_slot = next_record_offset;
    }

    // update current record's record_no_
    *(int32_t*)(record_insert_slot + RECHDR_RECORD_NO_LOCATION) = file_hdr_->next_record_no_ ++;

    // update page_hdr's tot_num_records
    page_hdr_->tot_num_records_ ++;

    return insert_index;
}