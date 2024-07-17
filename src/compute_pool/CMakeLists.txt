set(RW_SRC
    rw_server.cpp
)

add_executable(rw_server ${RW_SRC})
target_link_libraries(rw_server parser execution readline pthread planner analyze cjson workload rdma_util multi_version_record -lfmt)

# set(RO_SRC
#     ro_server.cpp
# )

# add_executable(ro_server ${RO_SRC})

# target_link_libraries(ro_server parser execution readline pthread planner analyze multi_version_record)