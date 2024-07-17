set(SOURCES multi_version_file_handle.cpp)
add_library(multi_version_record STATIC ${SOURCES})
add_library(multi_version_records SHARED ${SOURCES})
target_link_libraries(multi_version_record system transaction storage)