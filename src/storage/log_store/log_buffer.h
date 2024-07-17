#pragma once

#include <unistd.h>

#include "redolog_defs.h"

class LogBuffer {
public:
    LogBuffer() { 
        offset_ = 0; 
        memset(buffer_, 0, sizeof(buffer_));
    }

    char buffer_[REDO_LOG_BUFFER_SIZE + 1];
    int offset_;    // 写入log的offset
};