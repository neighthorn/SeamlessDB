#pragma once

#include "rm_defs.h"
#include "record.h"

class RmFileHandle;

class RmScan {
    const RmFileHandle *file_handle_;
    Rid rid_;
public:
    RmScan(const RmFileHandle *file_handle);

    void next();

    bool is_end() const { return rid_.page_no == RM_NO_PAGE; }

    Rid rid() const { return rid_; }
};
