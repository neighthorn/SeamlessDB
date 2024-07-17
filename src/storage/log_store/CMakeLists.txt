# find_library(LZ4_LIB lz4)
# message(STATUS "finding ${LZ4_LIB}")

# # log store
# add_library(log_store log_store.cpp)
# target_link_libraries(log_store rocksdb ${LZ4_LIB} -lpthread -lz -lsnappy -lbz2 -lzstd -ldl)


# # rocksdb test
# add_executable(test_rocksdb test_rocksdb.cpp)
# target_include_directories(test_rocksdb PRIVATE ${ROCKSDB_DIR}/include)

# target_link_libraries(test_rocksdb rocksdb ${LZ4_LIB} -lpthread -lz -lsnappy -lbz2 -lzstd -ldl )