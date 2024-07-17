#include "rm_scan.h"
#include "rm_file_handle.h"

RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    rid_ = {.page_no = RM_FIRST_RECORD_PAGE, .slot_no = -1};
    next();
}

void RmScan::next() {
    assert(!is_end());
    while (rid_.page_no < file_handle_->file_hdr_.num_pages) {
        RmPageHandle page_handle = file_handle_->fetch_page_handle(rid_.page_no);
        // 用bitmap找下一个为1的位（即由当前的slot_no得到下一个有record的slot_no）
        rid_.slot_no = Bitmap::next_bit(true, page_handle.bitmap, file_handle_->file_hdr_.num_records_per_page, rid_.slot_no);
        if (rid_.slot_no < file_handle_->file_hdr_.num_records_per_page) {
            // 在这个page中找到了有record的slot_no
            return;
        }
        // next record not found in this page 没找到，找下一个page
        rid_.slot_no = -1;
        rid_.page_no++;
    }
    // 所有的page都没找到
    rid_.page_no = RM_NO_PAGE;
}
