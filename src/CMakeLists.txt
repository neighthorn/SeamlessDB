include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty)

add_subdirectory(thirdparty)
add_subdirectory(state)
add_subdirectory(analyze)
add_subdirectory(record)
add_subdirectory(index)
add_subdirectory(system)
add_subdirectory(multi_version)
add_subdirectory(execution)
add_subdirectory(parser)
add_subdirectory(optimizer)
add_subdirectory(storage)
add_subdirectory(common)
add_subdirectory(replacer)
add_subdirectory(transaction)
add_subdirectory(recovery)
# add_subdirectory(test)
add_subdirectory(storage_pool)
add_subdirectory(compute_pool)
add_subdirectory(state_pool)
add_subdirectory(benchmark)
add_subdirectory(proxy)


target_link_libraries(parser execution pthread)

# add_executable(rmdb rmdb.cpp)
# target_link_libraries(rmdb parser execution readline pthread planner analyze)

# # unit_test
# add_executable(unit_test unit_test.cpp)
# target_link_libraries(unit_test storage lru_replacer gtest_main)  # add gtest
