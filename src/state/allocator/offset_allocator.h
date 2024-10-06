#pragma once

#include "common/config.h"

const offset_t THREAD_LOCAL_LOG_BUFFER_SIZE = 1024 * 1024 * 1024;

/**
 * Every thread has a LogOffsetAllocator to manage the LogOffset in remote StateNode.
*/
class OffsetAllocator {
public:
    OffsetAllocator(t_id_t thread_id, int local_size) {
        // auto per_thread_remote_log_buffer_size = LOG_BUFFER_SIZE / num_thread;
        start_offset = thread_id * local_size;
        end_offset = (thread_id + 1) * local_size;
        current_offset = 0;
    }

    offset_t GetNextOffset(node_id_t node_id, size_t entry_size) {
        if(unlikely(start_offset + current_offset + entry_size > (offset_t)end_offset)) {
            current_offset = 0;
            // @StateReplicateTODO: 当一块region满了之后应该分配一个新的region
        }
        offset_t offset = start_offset + current_offset;
        current_offset += entry_size;
        return offset;
    }

    void free() {
        current_offset = 0;
    }

private:
    offset_t start_offset;
    int64_t end_offset;
    int64_t current_offset;
};