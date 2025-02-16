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
    // assert(!is_end());
    IxNodeHandle *node = ih_->fetch_node(rid_.page_no);
    // if(node->get_next_page() - node->get_page_no() <= 0) {
    //     if(!(node->get_page_no() == node->file_hdr_->last_leaf_ && node->get_next_page() == IX_NO_PAGE)) {
    //         std::string str = "Invalid next_page value: node->get_page_no(): " + std::to_string(node->get_page_no()) + ", node->get_next_page(): " + std::to_string(node->get_next_page()) + '\n';
    //         std::cout << str;
    //         assert(0);
    //     }
    // }
    // if(node->get_prev_page() - node->get_page_no() >= 0) {
    //     std::string str = "Invalid prev_page value: node->get_page_no(): " + std::to_string(node->get_page_no()) + ", node->get_prev_page(): " + std::to_string(node->get_prev_page()) + '\n';
    //     std::cout << str;
    //     assert(0);
    // }
    // assert(node->get_page_no() == rid_.page_no);
    // assert(node->is_leaf_page());
    // used for test
    // assert(rid_.slot_no < node->get_size());
    // ASSERT(rid_.slot_no < node->get_size(), "rid_.slot_no: " + std::to_string(rid_.slot_no) + ", node->get_size(): " + std::to_string(node->get_size()) + ", node->page_id: " << std::to_string(node->get_page_id().page_no));
    // increment slot no
    rid_.slot_no++;
    if (rid_.page_no != ih_->file_hdr_->last_leaf_ && rid_.slot_no == node->get_size()) {
        // go to next leaf
        rid_.slot_no = 0;
        // std::cout << "Page1 [" << rid_.page_no<< "] is full, go to next page [" << node->get_page_no() << "].\n";
        rid_.page_no = node->get_next_page();
        // std::cout << "Page2 [" << node->get_page_no() << "] is full, go to next page [" << rid_.page_no << "].\n";
    }
    ih_->buffer_pool_manager_->unpin_page(node->get_page_id(), false);
    delete node;
    node = nullptr;
    rid_.record_no ++;

    /**
     * @TEST: 判断rid_.record_no是否和对应位置上的record_no相等
     */
    // node = ih_->fetch_node(rid_.page_no);
    // if(rid_.record_no != node->leaf_get_record_no_at(rid_.slot_no)) {
        // std::cerr << "rid_.record_no: " << rid_.record_no << ", node->leaf_get_record_no_at(rid_.slot_no): " << node->leaf_get_record_no_at(rid_.slot_no) << std::endl;
    // }
}
