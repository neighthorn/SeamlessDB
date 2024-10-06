set(WORKLOAD_SRC
    test/test_wk.cpp
    tpcc/tpcc_wk.cpp
    tpch/tpch_wk.cpp
    util/clock.cpp
    util/random.cpp
)

add_library(workload ${WORKLOAD_SRC})
target_link_libraries(workload index system)

set(COMP_EXP_SRC
    tpch/comparative_exp.cpp
)

add_executable(comp_exp ${COMP_EXP_SRC})
target_link_libraries(comp_exp execution pthread planner cjson workload rdma_util multi_version_record -lfmt)