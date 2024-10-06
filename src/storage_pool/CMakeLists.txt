set(STORAGE_SRC
    storage_server.cpp
    log_replay.cpp
)

# add_library(log_replay log_replay.cpp)
# target_link_libraries(log_replay log_store)

find_library(LZ4_LIB lz4)
message(STATUS "finding ${LZ4_LIB}")

add_library(log_store log_store.cpp)
target_link_libraries(log_store PUBLIC rocksdb ${LZ4_LIB} -lpthread -lz -lsnappy -lbz2 -lzstd -ldl)

add_executable(storage_pool ${STORAGE_SRC})
target_link_libraries(storage_pool log_store storage cjson workload multi_version_record  -lfmt)