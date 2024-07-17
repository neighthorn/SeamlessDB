# set(SOURCES rm_file_handle.cpp rm_scan.cpp)
# add_library(record STATIC ${SOURCES})
# add_library(records SHARED ${SOURCES})
# target_link_libraries(record system transaction system storage)

# add_executable(test_bitmap test_bitmap.cpp)
# add_test(NAME test_bitmap COMMAND test_bitmap
#      WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

# add_executable(test_rm test_rm.cpp)
# target_link_libraries(test_rm record)
# add_test(NAME test_rm COMMAND test_rm
#      WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

# # rm_gtest
# add_executable(rm_gtest rm_gtest.cpp)
# target_link_libraries(rm_gtest record gtest_main)