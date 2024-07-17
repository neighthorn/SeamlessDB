set(WORKLOAD_SRC
    test/test_wk.cpp
    tpcc/tpcc_wk.cpp
    tpch/tpch_wk.cpp
    util/clock.cpp
    util/random.cpp
)

add_library(workload ${WORKLOAD_SRC})
target_link_libraries(workload index system)