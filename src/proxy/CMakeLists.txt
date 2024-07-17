set(PROXY_SRC
    proxy.cpp
)

add_executable(proxy ${PROXY_SRC})
target_link_libraries(proxy workload pthread cjson -lfmt)