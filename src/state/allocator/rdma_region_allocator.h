#pragma once

#include <assert.h>

#include "state/meta_manager.h"

// const uint64_t PER_THREAD_SQL_SIZE        = (size_t)  100 * 1024 * 1024;  // 100 MB
// const uint64_t PER_THREAD_JOIN_PLAN_SIZE  = (size_t)  16 * 1024 * 1024;   // 16 MB
// const uint64_t PER_THREAD_JOIN_BLOCK_SIZE = (size_t)  100 * 1024 * 1024;  // 100 MB

// This allocator is a global one which manages all the RDMA regions in this machine

// |                   | <- t1 start
// |                   |
// |                   |
// |                   |
// |                   | <- t1 end. t2 start
// |                   |
// |                   |
// |                   |
// |                   | <- t2 end. t3 start

class RDMARegionAllocator {
public:

  /**
   * Intializes the RDMA Region Allocator used for MasterNode
   * Must be called before get_instance() can be used.
   * 
   * @return true if initialization failed, false otherwise
  */
  static bool create_instance(MetaManager* global_meta_mgr, t_id_t thread_num_per_machine);

  /**
   * Destroys the singleton instance.
  */
  static void destroy_instance();

  /**
   * Retrieves singleton instance
  */
  static RDMARegionAllocator* get_instance() {
    assert(rdma_region_allocator != nullptr);
    return rdma_region_allocator;
  }

  ALWAYS_INLINE
  std::pair<char*, char*> GetThreadLocalRegion(t_id_t tid) {
    std::cout << "tid: " << tid << ", thread_num: " << thread_num << std::endl;
    assert(tid < thread_num);
    return std::make_pair(thread_local_mr + tid * PER_THREAD_SQL_SIZE, thread_local_mr + (tid + 1) * PER_THREAD_SQL_SIZE);
  }

  /*
    sql region
  */
  ALWAYS_INLINE
  std::pair<char*, char*> GetThreadLocalSQLRegion(t_id_t tid) {
    assert(tid < thread_num);
    return std::make_pair(thread_local_sql_mr + tid * PER_THREAD_SQL_SIZE, thread_local_sql_mr + (tid + 1) * PER_THREAD_SQL_SIZE);
  }

  /*
    join plan && join block
  */
  ALWAYS_INLINE
  std::pair<char *, char *> GetThreadLocalJoinPlanRegion(t_id_t tid) {
    assert(tid < thread_num);
    return std::make_pair(thread_local_join_plan_mr + tid * PER_THREAD_JOIN_PLAN_SIZE, thread_local_join_plan_mr + (tid + 1) * PER_THREAD_JOIN_PLAN_SIZE);
  }

  ALWAYS_INLINE
  std::pair<char *, char *> GetThreadLocalJoinBlockRegion(t_id_t tid) {
    assert(tid < thread_num);
    std::cout << "tid: " << tid << "\n";
    return std::make_pair(thread_local_join_block_mr + tid * PER_THREAD_JOIN_BLOCK_SIZE, thread_local_join_block_mr + (tid + 1) * PER_THREAD_JOIN_BLOCK_SIZE);
  }

  std::pair<char *, char *> GetThreadLocalOPCheckpointReadCacheRegion(t_id_t tid) {
    assert(tid < thread_num);
    return std::make_pair(thread_local_op_ck_read_cache_mr + tid * PER_THREAD_OP_CK_READ_CACHE_SIZE, thread_local_op_ck_read_cache_mr + (tid + 1) * PER_THREAD_OP_CK_READ_CACHE_SIZE);
  }


  std::pair<char*, char*> GetLockRegion() {
    return std::make_pair(lock_mr, lock_mr + lock_buf_size);
  }
  char* GetLockBitmapRegion() {
    return lock_bitmap_mr;
  }

  char* GetLogRegion() {
    return log_mr;
  }

  char* GetLogMetaRegion() {
    return log_meta_mr;
  }

private:
  RDMARegionAllocator(MetaManager* global_meta_man, t_id_t thread_num_per_machine);

  ~RDMARegionAllocator() {
    if (global_mr) free(global_mr);
  }
  
private:
  static RDMARegionAllocator* rdma_region_allocator;
  char* global_mr;  // memory region
  char* lock_mr;
  char* log_mr;
  char* log_meta_mr;
  char* lock_bitmap_mr;
  char* thread_local_mr;
  char* thread_local_sql_mr;        // sql
  char *thread_local_join_plan_mr;  // join plan  
  char *thread_local_join_block_mr; // join block
  char *thread_local_op_ck_read_cache_mr; // read cache for op checkpoint
public:
  size_t lock_buf_size;
  t_id_t thread_num;
  size_t log_buf_size;
};
