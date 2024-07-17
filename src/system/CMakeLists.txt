set(SOURCES sm_manager.cpp)
add_library(system STATIC ${SOURCES})
target_link_libraries(system index)

# add_executable(test_sm test_sm.cpp)
# target_link_libraries(test_sm system)
# add_test(NAME test_sm COMMAND test_sm
#         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

# # sm_gtest
# add_executable(sm_gtest sm_gtest.cpp)
# target_link_libraries(sm_gtest system gtest_main)
