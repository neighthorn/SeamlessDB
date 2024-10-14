#pragma once

#include "ix_defs.h"

static int ix_compare(const char *a, const char *b, ColType type, int col_len) {
    switch (type) {
        case TYPE_INT: {
            int ia = *(int *)a;
            int ib = *(int *)b;
            return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
        }
        case TYPE_FLOAT: {
            float fa = *(float *)a;
            float fb = *(float *)b;
            return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
        }
        case TYPE_STRING: {
            std::string a_str(a, col_len);
            std::string b_str(b, col_len);
            // std::cout << "ix_compare: a_str: " << a_str << ", b_str: "  << b_str << "\n";
            return memcmp(a, b, col_len);
        }
        default:
            throw InternalError("Unexpected data type");
    }
}

[[maybe_unused]]static int ix_compare(const char* a, const char* b, const std::vector<ColType>& col_types, const std::vector<int>& col_lens) {
    int offset = 0;
    for(size_t i = 0; i < col_types.size(); ++i) {
        int res = ix_compare(a + offset, b + offset, col_types[i], col_lens[i]);
        // std::cout << "ix_compare_result: " << res << "\n";
        if(res != 0) return res;
        offset += col_lens[i];
    }
    return 0;
}

/**
 * Both leaf page and internal page use the IxNodeHandle.
 * LeafPage:
 * |----------------|
 * | IxPageHdr      |  fixed space
 * |----------------|
 * | Records        |  space grows from top to the bottom
 * |----------------|
 * | Page Directory |  fixed space
 * |----------------|
 * To better split, the number of page directory entries is one more than the number of records.
 * InternalPage:
 * |----------------|
 * | IxPageHdr      |  fixed space
 * |----------------|
 * | Keys           |  fixed space
 * |----------------|
 * | Child nodes    |  fixed space
 * |----------------|
*/
class IxNodeHandle {
    friend class IxIndexHandle;
    friend class IxScan;

   private:
    IxFileHdr* file_hdr_;         // the pointer to the index file's header, which is always in cache
    Page* page_;                        // the pointer to the node's page
    IxPageHdr* page_hdr_;               // pointer to the index page header, the size if sizeof(IxPageHdr)
    char* keys_ = nullptr;              // pointer to the keys, store the primary keys
    char* child_nodes_ = nullptr;       // pointer to the child nodes, store the page_no of the child node
    char* records_ = nullptr;           // pointer to the records, store the table records
    char* page_directory_ = nullptr;    // pointer to the page directory, store the offset of the records in the page

public:

    IxNodeHandle() = default;
    IxNodeHandle(IxFileHdr *file_hdr, Page *page);

    /**
     * The following functions can be used for both LeafPage and InternalPage
    */

    page_id_t get_page_no() { return page_->get_page_id().page_no; }

    PageId get_page_id() { return page_->get_page_id(); }

    page_id_t get_parent_page_no() { return page_hdr_->parent_; }

    void set_parent_page_no(page_id_t parent) { page_hdr_->parent_ = parent; }

    bool is_leaf_page() { return page_hdr_->is_leaf_; }

    bool is_root_page() { return get_parent_page_no() == INVALID_PAGE_ID; }

    page_id_t get_next_page() { return page_hdr_->next_page_; }

    page_id_t get_prev_page() { return page_hdr_->prev_page_; }

    void set_next_page(page_id_t page_no) { page_hdr_->next_page_ = page_no; }

    void set_prev_page(page_id_t page_no) { page_hdr_->prev_page_ = page_no; }

    char* get_key_at(int i) {
        if(page_hdr_->is_leaf_) {
            return leaf_get_directory_entry_at(i);
        }
        else {
            return internal_key_at(i);
        }
    }

    int get_size() {
        if(page_hdr_->is_leaf_) {
            return leaf_get_tot_record_num();
        }
        else {
            return internal_get_key_num();
        }
    }

    int get_max_size() {
        if(page_hdr_->is_leaf_) {
            return leaf_get_max_size();
        }
        else {
            return internal_get_max_size();
        }
    }

    int get_min_size() {
        if(page_hdr_->is_leaf_) {
            return leaf_get_min_size();
        }
        else {
            return internal_get_min_size();
        }
    }

    /**
     * The following functions are used for InternalPage
    */
    int internal_get_key_num() { return page_hdr_->num_key_; }

    int internal_get_max_size() { return file_hdr_->btree_order_ + 1; }

    int internal_get_min_size() { return (file_hdr_->btree_order_ + 1) / 2; }

    // find the ith key's value
    char* internal_key_at(int i) { return keys_ + i * file_hdr_->col_tot_len_; }

    char* internal_value_at(int i) { 
        return child_nodes_ + sizeof(page_id_t) * i;
    }

    void internal_set_key_at(int i, char* key) {
        memcpy(internal_key_at(i), key, file_hdr_->col_tot_len_);
    }

    void internal_set_child_at(int i, page_id_t page_no) {
        *(page_id_t*)internal_value_at(i) = page_no;
    }

    // find the ith child node's page_no
    page_id_t internal_child_page_at(int i) {
        // std::cout << "internal_child_page_at: i=" << i << ", child_nodes at 0=" <<  *(page_id_t*)child_nodes_ << "\n";
        return *(page_id_t*)(child_nodes_ + sizeof(page_id_t) * i);
    }

    // used in internal node to find the page which store the target key
    page_id_t internal_lookup(const char *key) {
        // std::cout << "internal_lookup, key=" << *(int*)key;
        int key_idx = internal_upper_bound(key);
        // std::cout << ", key_idx: " << key_idx << "\n";
        return internal_child_page_at(key_idx == 0 ? 0 : key_idx - 1);
    }

    int internal_upper_bound(const char *target);

    int internal_lower_bound(const char *target);

    void internal_insert_pairs(int pos, const char* key, const char* children, int n);

    int internal_find_child(IxNodeHandle *child);

    /**
     * The following functions are used for LeafPage
    */

    int leaf_get_max_size() { return file_hdr_->max_number_of_records_ + 1; }

    int leaf_get_min_size() { return (file_hdr_->max_number_of_records_ + 1) / 2; }

    int leaf_get_tot_record_num() { return page_hdr_->tot_num_records_; }

    // the record is first inserted to the deleted slot in UserRecords, if there are not deleted slots,
    // the record will be insert into the first slot in free space.
    // Only the split operation can generate deleted slot
    int32_t leaf_get_insert_offset() {
        if(page_hdr_->first_deleted_offset_ == INVALID_OFFSET) {
            return page_hdr_->free_space_offset_;
        }
        return page_hdr_->first_deleted_offset_;
    }

    /**
     * The next_record_offset in record header is initiated as INVALID_OFFSET.
     * When a record is inserted to a page, its previous record's next_record_offset will point
     * to the current record's location, the current record's next_record_offset will point to the next
     * record's location.
     * The free_space_offset in page header is initiated as sizeof(IxPageHdr).
     * The first_deleted_offset in page header is initiated as INVALID_OFFSET.
     * When a split operation is required, the first_deleted_offset will point to the first
     * record that moves to another page. When a record is inserted to the first_deleted_offset, then
     * the first_deleted_offset will point to the next_record_offset of the first deleted record.
    */
    void leaf_update_insert_offset() {
        if(page_hdr_->first_deleted_offset_ == INVALID_OFFSET) {
            page_hdr_->free_space_offset_ += file_hdr_->record_len_;
            if(page_hdr_->free_space_offset_ + sizeof(IxPageHdr) >= PAGE_SIZE) page_hdr_->free_space_offset_ = INVALID_OFFSET;
            // puts("get insert offset from free space");
        }
        else {
            int32_t next_delete_offset = page_hdr_->first_deleted_offset_;
            page_hdr_->first_deleted_offset_ = next_delete_offset;
            // puts("get insert offset from deleted space");
        }
    }

    // used for LeafPage's split operation, insert multiple continous records
    void leaf_insert_continous_records(int pos, const char* extern_page_record, const char* begin_directory, int record_number);

    // used for leaf page to find the specific record
    char* leaf_get_record_at(int i) {
        // std::cout << "leaf_get_record_at: " << i << "\n";
        char* directory = page_directory_ + (file_hdr_->col_tot_len_ + sizeof(int32_t)) * i;
        int offset = *(int32_t*)(directory + file_hdr_->col_tot_len_);
        // std::cout << "get record offset: " << offset << "\n";
        return records_ + offset;
    }

    int32_t leaf_get_record_no_at(int i) {
        if(i >= page_hdr_->tot_num_records_) return -1;
        char* directory = page_directory_ + (file_hdr_->col_tot_len_ + sizeof(int32_t)) * i;
        int offset = *(int32_t*)(directory + file_hdr_->col_tot_len_);
        return *(int32_t*)(records_ + offset + RECHDR_RECORD_NO_LOCATION);
    }

    // used for leaf page to find the specific directory entry
    char* leaf_get_directory_entry_at(int i) {
        assert(i <= file_hdr_->max_number_of_records_);

        char* directory = page_directory_ + (file_hdr_->col_tot_len_ + sizeof(int32_t)) * i;
        return directory;
    }

    int leaf_directory_lower_bound(const char* target);

    int leaf_directory_upper_bound(const char* target);

    // insert the primary key of the record into the page_directory, 
    // and return the index of the inserted directory entry
    int leaf_insert_key_record(const char* key, const char* record_value);
    
};