#include <assert.h>

#include "util/json_util.h"
#include "storage_server.h"
#include "log_replay.h"
#include "benchmark/test/test_wk.h"
#include "benchmark/tpcc/tpcc_wk.h"
#include "benchmark/tpch/tpch_wk.h"
#include "benchmark/TAW/hat_wk.h"

void load_data(std::string workload, int record_num, SmManager* sm_mgr, IxManager* ix_mgr, MultiVersionManager *mvcc_mgr) {
    if(workload.compare("test") == 0) {
        TestWK* test_wk = new TestWK(sm_mgr, ix_mgr, record_num, mvcc_mgr);
        if(test_wk->create_table())
            test_wk->load_data();
    }
    else if(workload.compare("tpcc") == 0) {
        TPCCWK* tpcc_wk = new TPCCWK(sm_mgr, ix_mgr, record_num, mvcc_mgr);
        if(tpcc_wk->create_table())
            tpcc_wk->load_data();
    }
    else if(workload.compare("tpch") == 0) {
        TPCHWK *tpch_wk = new TPCHWK(sm_mgr, ix_mgr, record_num, mvcc_mgr);
        if(tpch_wk->create_table())
            tpch_wk->load_data();
    } 
    else if(workload.compare("TAW") == 0) {
        HATtrickWK* hattrick_wk = new HATtrickWK(sm_mgr, ix_mgr, record_num, mvcc_mgr);
        if(hattrick_wk->create_table())
            hattrick_wk->load_data();
    }
    else {
        std::cerr << "workload not supported!\n";
    }
}

void test_leaf_pages(SmManager* sm_mgr_) {
    // 在完成数据导入之后遍历orders表中所有的leaf page，打印每个leaf page的prev和next指针
    // std::cout << "********TEST ORDERS LEAF PAGES**********\n";
    // IxIndexHandle* index_handle = sm_mgr_->primary_index_.at("orders").get();
    // auto lower = index_handle->leaf_begin();
    // auto upper = index_handle->leaf_end();
    // std::cout << "lower: " << lower.page_no << " " << lower.slot_no << " " << lower.record_no << "\n";
    // std::cout << "upper: " << upper.page_no << " " << upper.slot_no << " " << upper.record_no << "\n";
    // auto ix_scan = IxScan(index_handle, lower, upper);
    // while(!ix_scan.is_end()) {
    //     ix_scan.next();
    // }
    // delete index_handle;
    // index_handle = nullptr;
    // std::cout << "********TEST ORDERS LEAF PAGES**********\n\n";

    // std::cout << "********TEST LINEITEM LEAF PAGES**********\n";
    // auto index_handle = sm_mgr_->primary_index_.at("lineitem").get();
    // auto lower = index_handle->leaf_begin();
    // auto upper = index_handle->leaf_end();
    // std::cout << "lower: " << lower.page_no << " " << lower.slot_no << " " << lower.record_no << "\n";
    // std::cout << "upper: " << upper.page_no << " " << upper.slot_no << " " << upper.record_no << "\n";
    // auto ix_scan = IxScan(index_handle, lower, upper);
    // while(!ix_scan.is_end()) {
    //     ix_scan.next();
    // }
    // delete index_handle;
    // index_handle = nullptr;
    // std::cout << "********TEST LINEITEM LEAF PAGES**********\n";


    std::cout << "********TEST LINEITEM LEAF PAGES -- PARALLEL**********\n";
    auto index_handle = sm_mgr_->primary_index_.at("lineitem").get();
    auto lower = index_handle->leaf_begin();
    char key[14];
    int maxint = INT32_MAX;
    memcpy(key, "1995-06-01", 10);
    memcpy(key + 10, reinterpret_cast<char*>(&maxint), 4);
    auto upper = index_handle->lower_bound(key);
    std::cout << "lower: " << lower.page_no << " " << lower.slot_no << " " << lower.record_no << "\n";
    std::cout << "upper: " << upper.page_no << " " << upper.slot_no << " " << upper.record_no << "\n";

    auto lower2 = index_handle->upper_bound(key);
    auto upper2 = index_handle->leaf_end();
    std::cout << "lower2: " << lower2.page_no << " " << lower2.slot_no << " " << lower2.record_no << "\n";
    std::cout << "upper2: " << upper2.page_no << " " << upper2.slot_no << " " << upper2.record_no << "\n";

    auto ix_scan = IxScan(index_handle, lower, upper);
    auto ix_scan2 = IxScan(index_handle, lower2, upper2);

    std::thread t1([&ix_scan](){
        std::cout << "thread1\n";
        while(!ix_scan.is_end()) {
            ix_scan.next();
        }
    });

    std::thread t2([&ix_scan2](){
        std::cout << "thread2\n";
        while(!ix_scan2.is_end()) {
            ix_scan2.next();
        }
    });

    t1.join();
    t2.join();
    std::cout << "********TEST LINEITEM LEAF PAGES -- PARALLEL**********\n";
}

int main(int argc, char* argv[]) {
    std::string config_path = "../src/config/storage_server_config.json";

    cJSON* cjson = parse_json_file(config_path);
    cJSON* storage_node = cJSON_GetObjectItem(cjson, "storage_node");
    int node_id = cJSON_GetObjectItem(storage_node, "machine_id")->valueint;
    int local_rpc_port = cJSON_GetObjectItem(storage_node, "local_rpc_port")->valueint;
    std::string workload = cJSON_GetObjectItem(storage_node, "workload")->valuestring;
    int record_num = cJSON_GetObjectItem(storage_node, "record_num")->valueint;
    int buffer_pool_size = cJSON_GetObjectItem(storage_node, "buffer_pool_size")->valueint;

    std::cout << "finish resolving storage_node config\n";

    // cJSON* remote_compute_node = cJSON_GetObjectItem(cjson, "remote_compute_node");
    // cJSON* remote_compute_node_ips = cJSON_GetObjectItem(remote_compute_node, "compute_node_ips");
    // cJSON* remote_compute_node_ports = cJSON_GetObjectItem(remote_compute_node, "compute_node_ports");
    // int compute_node_num = cJSON_GetArraySize(remote_compute_node_ips);
    // std::cout << "compute_node_num: " << compute_node_num << "\n";
    // std::vector<std::string> compute_node_ips;
    // std::vector<int> compute_node_ports;
    // for(int i = 0; i < compute_node_num; ++i) {
    //     compute_node_ips.emplace_back(cJSON_GetArrayItem(remote_compute_node_ips, i)->valuestring);
    //     compute_node_ports.emplace_back(cJSON_GetArrayItem(remote_compute_node_ports, i)->valueint);
    // }

    std::cout << "finish resolving config.json\n";

    auto disk_manager = std::make_shared<DiskManager>();
    auto buffer_pool_manager = std::make_shared<BufferPoolManager>(NodeType::STORAGE_NODE, buffer_pool_size, nullptr, disk_manager.get());
    auto ix_manager = std::make_shared<IxManager>(buffer_pool_manager.get(), disk_manager.get());
    auto mvcc_manager = std::make_shared<MultiVersionManager>(disk_manager.get(), buffer_pool_manager.get());
    auto sm_manager = std::make_shared<SmManager>(disk_manager.get(), buffer_pool_manager.get(), ix_manager.get(), mvcc_manager.get());

    std::cout << "begin load data\n";
    load_data(workload, record_num, sm_manager.get(), ix_manager.get(), mvcc_manager.get());

    // test_leaf_pages(sm_manager.get());
    // auto log_manager = std::make_shared<LogManager>();

// #ifdef ENABLE_LOG_STORE
    auto log_store = std::make_shared<LogStore>("./storage_node_log_storage");

    // clear all log storage data
    // log_store->clear_all();

    // init ShareStatus
    ShareStatus share_status{.current_replay_lsn_ = -1, .need_replay_lsn_ = -1};

    /*
        log replay thread
    */
    auto log_replay = std::make_shared<LogReplay>(log_store.get(), disk_manager.get(), ix_manager.get(), sm_manager.get(), &share_status);
// #endif

    std::cout << "try to start server\n";

// #ifdef ENABLE_LOG_STORE
    auto server = std::make_shared<StorageServer>(node_id, local_rpc_port, disk_manager.get(), log_store.get(), &share_status, buffer_pool_manager.get());
// #else
    // auto server = std::make_shared<StorageServer>(node_id, local_rpc_port, disk_manager.get(), nullptr, nullptr, buffer_pool_manager.get());
// #endif

    return 0;
}