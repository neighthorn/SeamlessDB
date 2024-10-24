#include "ix_scan.h"

#include <cstdlib> // for abort


#define ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            std::cerr << "Assertion failed: (" << #condition << "), " \
                      << "message: " << message << ", " \
                      << "function " << __FUNCTION__ << ", " \
                      << "file " << __FILE__ << ", " \
                      << "line " << __LINE__ << "." << std::endl; \
            std::abort(); \
        } \
    } while (false)

/**
 * @brief 
 * @todo 加上读锁（需要使用缓冲池得到page）
 */
void IxScan::next() {
    assert(!is_end());
    IxNodeHandle *node = ih_->fetch_node(rid_.page_no);
    assert(node->is_leaf_page());
    // assert(rid_.slot_no < node->get_size());
    ASSERT(rid_.slot_no < node->get_size(), "rid_.slot_no: " + std::to_string(rid_.slot_no) + ", node->get_size(): " + std::to_string(node->get_size()) + ", node->page_id: " << std::to_string(node->get_page_id().page_no));
    // increment slot no
    rid_.slot_no++;
    if (rid_.page_no != ih_->file_hdr_->last_leaf_ && rid_.slot_no == node->get_size()) {
        // go to next leaf
        rid_.slot_no = 0;
        rid_.page_no = node->get_next_page();
    }
    ih_->buffer_pool_manager_->unpin_page(node->get_page_id(), false);
    rid_.record_no ++;
}
