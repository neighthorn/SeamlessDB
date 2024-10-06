#pragma once

#include "errors.h"

enum class ExecutionType {
    SEQUENTIAL_SCAN,
    INDEX_SCAN,
    BLOCK_JOIN,
    HASH_JOIN,
    PROJECTION,
    SORT,
    NOT_DEFINED
};