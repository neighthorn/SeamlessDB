ADD_SUBDIRECTORY(googletest)


set(ROCKSDB_DIR ${CMAKE_SOURCE_DIR}/deps/rocksdb)
add_library(rocksdb STATIC IMPORTED)

set_target_properties(rocksdb PROPERTIES IMPORTED_LOCATION ${ROCKSDB_DIR}/librocksdb.a)
include_directories(${ROCKSDB_DIR}/include)