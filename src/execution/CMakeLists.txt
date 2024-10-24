set(SOURCES 
    execution_manager.cpp
    executor_block_join.cpp
    executor_hash_join.cpp
    executor_projection.cpp
    execution_sort.cpp
    comp_ckpt_mgr.cpp
)
add_library(execution STATIC ${SOURCES})

target_link_libraries(execution system index transaction multi_version_record)

set(OP_SOURCES
    executor_block_join.cpp
    executor_hash_join.cpp
    executor_projection.cpp
    execution_sort.cpp
    comp_ckpt_mgr.cpp
)
add_library(execution_op STATIC ${OP_SOURCES})
target_link_libraries(execution_op system index transaction multi_version_record)

#add_executable(test_ql test_execution.cpp)
#target_link_libraries(test_ql execution parser)
#add_test(NAME test_ql COMMAND test_ql
#     WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

## ql_gtest
#add_executable(ql_gtest ql_gtest.cpp)
#target_link_libraries(ql_gtest execution parser gtest_main)