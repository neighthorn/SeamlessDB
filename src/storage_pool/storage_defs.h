#pragma once

#include "recovery/redo_log/redolog_defs.h"

struct ShareStatus
{
    // std::mutex replay_lock_;    // 
    std::atomic<lsn_t> current_replay_lsn_;
    std::atomic<lsn_t> need_replay_lsn_;
};