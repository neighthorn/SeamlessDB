set(SOURCES planner.cpp)
add_library(planner STATIC ${SOURCES})

# plan serialize test
add_executable(plan_serialize_test plan_serialize_test.cpp)
target_link_libraries(plan_serialize_test gtest planner system rdma_util)