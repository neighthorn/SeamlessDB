#pragma once

#include "ix_defs.h"
#include "ix_index_handle.h"

// class IxIndexHandle;

// 用于遍历叶子结点
// 用于直接遍历叶子结点，而不用findleafpage来得到叶子结点
// TODO：对page遍历时，要加上对读锁
class IxScan {
    const IxIndexHandle *ih_;
    Rid rid_;  // 初始为lower（用于遍历的指针）
    Rid end_;  // 初始为upper
    // BufferPoolManager *bpm_;

public:
    // used for sequential scan, the iid_ is initiated as leaf_begin, and the end_ is initiated as leaf_end
    IxScan(const IxIndexHandle* ih) : ih_(ih) {
        rid_ = ih->leaf_begin();
        end_ = ih->leaf_end();
    }
    IxScan(const IxIndexHandle *ih, const Rid &lower, const Rid &upper)
        : ih_(ih), rid_(lower), end_(upper) {}

    void next();

    bool is_end() { 
        if(rid_.page_no == end_.page_no && rid_.slot_no == end_.slot_no) return true;
        return false;
     }

    // Rid rid() const override;

    const Rid &rid() const { return rid_; }
    const Rid& end() const { return end_; }
};