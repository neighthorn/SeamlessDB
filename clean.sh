rm -rf ./build/db_test/
rm -rf ./build/db_tpcc
rm -rf ./build/db_tpch
rm -rf ./build/storage_node_log_storage
rm -rf ./build_debug/db_test/
rm -rf ./build_debug/db_tpcc
rm -rf ./build_debug/db_tpch
rm -rf ./build_debug/storage_node_log_storage
ps -ef | grep storage_pool  | grep -v grep | awk '{print $2}' | xargs kill -9
ps -ef | grep rw_server     | grep -v grep | awk '{print $2}' | xargs kill -9
ps -ef | grep ro_server     | grep -v grep | awk '{print $2}' | xargs kill -9
ps -ef | grep state_pool    | grep -v grep | awk '{print $2}' | xargs kill -9