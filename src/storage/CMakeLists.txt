set(SOURCES 
        disk_manager.cpp 
        buffer_pool_manager.cpp 
        ../replacer/replacer.h 
        ../replacer/lru_replacer.cpp 
        ../replacer/clock_replacer.cpp
        storage_rpc.cc
        storage_service.pb.cc
)
add_library(storage STATIC ${SOURCES})
target_link_libraries(storage ${BRPC_LIB} ${DYNAMIC_LIB})

# add_executable(test_storage test_storage.cpp)
# target_link_libraries(test_storage storage)

# add_test(NAME test_storage COMMAND test_storage
#          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

#  # storage_gtest
#  add_executable(storage_gtest storage_gtest.cpp)
#  target_link_libraries(storage_gtest storage gtest_main stdc++fs)  # add gtest; stdc++fs for "experimental/filesystem"

#  # buffer_pool_manager_test
#  add_executable(grading_buffer_pool_manager_test grading_buffer_pool_manager_test.cpp)
#  target_link_libraries(grading_buffer_pool_manager_test storage gtest_main)  # add gtest

#  # buffer_pool_manager_concurrency_test
#  add_executable(grading_buffer_pool_manager_concurrency_test grading_buffer_pool_manager_concurrency_test.cpp)
#  target_link_libraries(grading_buffer_pool_manager_concurrency_test storage gtest_main)  # add gtest

#  # leaderboard_test
#  add_executable(grading_leaderboard_test grading_leaderboard_test.cpp)
#  target_link_libraries(grading_leaderboard_test storage gtest_main)  # add gtest

 add_subdirectory(log_store)
