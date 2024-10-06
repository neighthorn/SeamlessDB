set(RDMA_SRC 
    coroutine/coroutine_scheduler.cpp
    coroutine/doorbell.cpp
    meta_manager.cpp
    qp_manager.cpp
    allocator/rdma_region_allocator.cpp
    state_item/op_state.cpp
    op_state_manager.cpp
    state_manager.cpp
    resume_util.cpp
)

add_library(rdma_util STATIC ${RDMA_SRC})
target_link_libraries(rdma_util rlib execution execution_op parser planner analyze)

# test buffer allocator
# add_executable(test_rdma_region_allocator allocator/buffer_allocator_test.cpp)
# target_link_libraries(test_rdma_region_allocator gtest_main)

# test op_state_manager
# add_executable(operator_state_test operator_state_test.cpp op_state_manager.cpp)
# target_link_libraries(operator_state_test gtest_main transaction system rdma_util cjson -lfmt)