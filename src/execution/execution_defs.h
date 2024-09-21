#pragma once

#include "errors.h"

enum class ExecutionType {
    SEQUENTIAL_SCAN,
    INDEX_SCAN,
    BLOCK_JOIN,
    HASH_JOIN,
    NOT_DEFINED
};