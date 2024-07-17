set(STATE_NODE_SRC state_server.cpp)

add_executable(state_pool ${STATE_NODE_SRC})
target_link_libraries(state_pool rlib cjson -lfmt)