#pragma once

#include "common/config.h"
#include "record/rm_defs.h"

// redo log buffer size
static constexpr int REDO_LOG_BUFFER_SIZE = (10 * PAGE_SIZE); 

// redo log file name
static constexpr char REDO_LOG_FILE_NAME[] = "redo_log.log";

// static constexpr std::chrono::duration<int64_t> FLUSH_TIMEOUT = std::chrono::seconds(3);
// LOG TYPE
static constexpr int REDO_LOG_TYPE_OFFSET = 0;
// LSN
static constexpr int REDO_LOG_LSN_OFFSET = sizeof(int);
// LOG TOTLAL LEN
static constexpr int REDO_LOG_TOTLEN_OFFSET = REDO_LOG_LSN_OFFSET + sizeof(lsn_t);
// TID
static constexpr int REDO_LOG_TID_OFFSET = REDO_LOG_TOTLEN_OFFSET + sizeof(uint32_t);
// PREV LSN
static constexpr int REDO_LOG_PREV_LSN_OFFSET = REDO_LOG_TID_OFFSET + sizeof(txn_id_t);
// bool is persist
static constexpr int REDO_LOG_IS_PERSIST_OFFSET = REDO_LOG_PREV_LSN_OFFSET + sizeof(lsn_t);
// LOG DATA
static constexpr int REDO_LOG_DATA_OFFSET = REDO_LOG_IS_PERSIST_OFFSET + sizeof(bool);
// size of log header
static constexpr int REDO_LOG_HEADER_SIZE = REDO_LOG_DATA_OFFSET;