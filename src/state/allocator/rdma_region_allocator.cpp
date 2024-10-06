#include "rdma_region_allocator.h"

RDMARegionAllocator* RDMARegionAllocator::rdma_region_allocator = nullptr;

/* 
RDMA Region
    +---lock bitmap---+---lock buf---+---log buf---+---thread local buf---+---join_plan---+---join block---+
global region:
    Lock bimap  : 
    lock buf    :
    log buf     :
thread local region:
    thread local buf    :
    join plan           :
    join block          :    
*/

#define PER_THREAD_ALLOC_SIZE 1024 * 1024

RDMARegionAllocator::RDMARegionAllocator(MetaManager* global_meta_man, t_id_t thread_num_per_machine) {
    lock_buf_size = LOCK_REGION_SIZE; 
    thread_num = thread_num_per_machine;
    log_buf_size = LOG_REGION_SIZE;
    std::cout << "thread_num: " << thread_num_per_machine << "\n";

    size_t global_mr_size = lock_buf_size;  // lock states
    global_mr_size += LOCK_MAX_COUNT / 8;   // lock bitmap
    global_mr_size += log_buf_size + sizeof(int64_t) * 3;         // log + log_mr
    global_mr_size += (size_t)thread_num_per_machine * PER_THREAD_ALLOC_SIZE;   // thread_local
    global_mr_size += (size_t)thread_num_per_machine * PER_THREAD_SQL_SIZE;     // sql
    global_mr_size += (size_t) thread_num_per_machine * PER_THREAD_JOIN_PLAN_SIZE;  // join plan
    global_mr_size += (size_t) thread_num_per_machine * PER_THREAD_JOIN_BLOCK_SIZE; // join block
    global_mr_size += (size_t) thread_num_per_machine * PER_THREAD_OP_CK_READ_CACHE_SIZE;   // read cache for op checkpoint

    std::cout << "global_mr_size: " << global_mr_size << "\n";

    // Register a buffer to the previous opened device. It's DRAM in compute pools
    global_mr = (char*)malloc(global_mr_size);
    lock_bitmap_mr = global_mr;
    lock_mr = lock_bitmap_mr + LOCK_MAX_COUNT / 8;
    log_mr = lock_mr + lock_buf_size; 
    log_meta_mr = log_mr + log_buf_size;
    thread_local_mr = log_meta_mr + sizeof(int64_t) * 3;
    /*
        compute global mr size 
    */
    // size_t global_mr_size = (size_t)thread_num_per_machine * PER_THREAD_SQL_SIZE;
    
    // global_mr_size += lock_buf_size;
    // global_mr_size += LOCK_MAX_COUNT / 8;
    // global_mr_size += LOG_REGION_SIZE;

    // Register a buffer to the previous opened device. It's DRAM in compute pools
    // global_mr                   =   (char*)malloc(global_mr_size);
    // lock_bitmap_mr              =   global_mr;
    // lock_mr                     =   global_mr + LOCK_MAX_COUNT / 8;
    // log_mr                      =   lock_mr + lock_buf_size; 
    // thread_local_mr             =   log_mr + log_buf_size;
    thread_local_sql_mr         =   thread_local_mr + thread_num_per_machine * PER_THREAD_ALLOC_SIZE;
    thread_local_join_plan_mr   =   thread_local_sql_mr + thread_num_per_machine * PER_THREAD_SQL_SIZE;
    thread_local_join_block_mr  =   thread_local_join_plan_mr + thread_num_per_machine * PER_THREAD_JOIN_PLAN_SIZE;
    thread_local_op_ck_read_cache_mr = thread_local_join_block_mr + thread_num_per_machine * PER_THREAD_JOIN_BLOCK_SIZE;

    assert(global_mr != nullptr);
    memset(global_mr, 0, global_mr_size);

    RDMA_ASSERT(global_meta_man->global_rdma_ctrl->register_memory(MASTER_LOCAL_ID, global_mr, global_mr_size, global_meta_man->opened_rnic));
}

bool RDMARegionAllocator::create_instance(MetaManager* global_meta_mgr, t_id_t thread_num_per_machine) {
    if(rdma_region_allocator == nullptr) {
        rdma_region_allocator = new (std::nothrow) RDMARegionAllocator(global_meta_mgr, thread_num_per_machine);
    }
    return (rdma_region_allocator == nullptr);
}

void RDMARegionAllocator::destroy_instance() {
    delete rdma_region_allocator;
    rdma_region_allocator = nullptr;
}