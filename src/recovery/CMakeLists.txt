set(SOURCES log_manager.cpp)
add_library(recovery STATIC ${SOURCES})
add_library(recoverys SHARED ${SOURCES})
target_link_libraries(recovery system pthread)

add_subdirectory(redo_log)
# add_executable(log_manager_test log_manager_test.cpp)
# target_link_libraries(log_manager_test recovery gtest_main)