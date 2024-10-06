#include "meta_manager.h"

#include "util/json_util.h"
#include "common/config.h"
#include <fstream>

MetaManager* MetaManager::global_meta_mgr = nullptr;

/**
 * In MetaManager(), we need to init the remote node meta_info
*/
MetaManager::MetaManager(const std::string& config_path) {
  std::cout << "begin\n";
  // std::string config_path = "../src/config/compute_server_config.json";
  cJSON* cjson = parse_json_file(config_path);
  std::cout << "get json file\n";
  cJSON* local_node;

  if(node_type_ == 0) {
    local_node = cJSON_GetObjectItem(cjson, "rw_node");
  }
  else if(node_type_ == 1){
    local_node = cJSON_GetObjectItem(cjson, "ro_node");
  }
  else if(node_type_ == 2) {
    local_node = cJSON_GetObjectItem(cjson, "comparative_exp");
  }
  else {
    throw RMDBError("Invalid node type.\n");
  }
  
  local_machine_id = (node_id_t)cJSON_GetObjectItem(local_node, "machine_id")->valueint;
  int local_port = cJSON_GetObjectItem(local_node, "local_rdma_port")->valueint;
  std::cout << "local port: " << local_port << "\n";

  cJSON* state_nodes = cJSON_GetObjectItem(cjson, "remote_state_nodes");
  cJSON* remote_ip_array = cJSON_GetObjectItem(state_nodes, "remote_ips");
  std::cout << "get remote ip array\n";
  cJSON* remote_port_array = cJSON_GetObjectItem(state_nodes, "remote_ports");
  std::cout << "get remote port array\n";
  int remote_node_cnt = cJSON_GetArraySize(remote_ip_array);
  std::cout << "MetaManager: finish parse config file\n";

  // txn_list_bitmap_ = new TxnListBitmap(MAX_THREAD_NUM);
  // txn_list_bitmap_ = new RegionBitmap(MAX_THREAD_NUM, thread_num);
  std::cout << "MetaManager: finish create txn_list bitmap\n";
  // txn_list_bitmap_addr = 0;
  txn_list_base_addr = 0;
  txn_size = sizeof(TxnItem);
  
  cJSON* ip = NULL;
  cJSON* port = NULL;
  ip = cJSON_GetArrayItem(remote_ip_array, node_type_);
  port = cJSON_GetArrayItem(remote_port_array, node_type_);
  remote_nodes.push_back(RemoteNode{.node_id = 0, .ip = ip->valuestring, .port = port->valueint});

  cJSON_Delete(cjson);
  // cJSON* remote_ips = 
    // // Read config json file
    // std::string config_filepath = "../config/compute_node_config.json";
    // auto json_config = JsonConfig::load_file(config_filepath);
    // // Get local node info
    // auto local_node = json_config.get("local_compute_node");
    // local_machine_id = (node_id_t)local_node.get("machine_id").get_int64();

    // get remote StateNode ip_info
    // auto state_nodes = json_config.get("remote_state_nodes");
    // auto remote_ips = state_nodes.get("remote_ips");                // Array
    // auto remote_ports = state_nodes.get("remote_ports");            // Array Used for RDMA exchanges
    // auto remote_meta_ports = state_nodes.get("remote_meta_ports");  // Array Used for transferring datastore metas

    // // Get remote machine's memory store meta via TCP
    // for (size_t index = 0; index < remote_ips.size(); index++) {
    //     std::string remote_ip = remote_ips.get(index).get_str();
    //     int remote_meta_port = (int)remote_meta_ports.get(index).get_int64();
    //     node_id_t remote_machine_id = GetMemStoreMeta(remote_ip, remote_meta_port);
    //     if (remote_machine_id == -1) {
    //     std::cerr << "Thread " << std::this_thread::get_id() << " GetMemStoreMeta() failed!, remote_machine_id = -1" << std::endl;
    //     }
    //     int remote_port = (int)remote_ports.get(index).get_int64();
    //     remote_nodes.push_back(RemoteNode{.node_id = remote_machine_id, .ip = remote_ip, .port = remote_port});
    // }
    // RDMA_LOG(INFO) << "All hash meta received";

    // RDMA setup
    global_rdma_ctrl = std::make_shared<RdmaCtrl>(local_machine_id, local_port);

    // Using the first RNIC's first port
    RdmaCtrl::DevIdx idx;
    idx.dev_id = 0;
    idx.port_id = 1;

    // Open device
    opened_rnic = global_rdma_ctrl->open_device(idx);
    std::fstream f;
    f.open("/usr/local/mysql/myerror.log", std::ios::out|std::ios::app);
    f << "open device in meta manager\n";
    f.close();

    for (auto& remote_node : remote_nodes) {
        GetMRMeta(remote_node);
    }
  // RDMA_LOG(INFO) << "client: All remote mr meta received!";
}

/**
 * GetMemStoreMeta() function is used to get StateNode meta_info via TCP/IP
*/
node_id_t MetaManager::GetMemStoreMeta(std::string& remote_ip, int remote_port) {
  // // Get remote memory store metadata for remote accesses, via TCP
  // /* ---------------Initialize socket---------------- */
  // struct sockaddr_in server_addr;
  // server_addr.sin_family = AF_INET;
  // if (inet_pton(AF_INET, remote_ip.c_str(), &server_addr.sin_addr) <= 0) {
  //   RDMA_LOG(ERROR) << "MetaManager inet_pton error: " << strerror(errno);
  //   return -1;
  // }
  // server_addr.sin_port = htons(remote_port);
  // int client_socket = socket(AF_INET, SOCK_STREAM, 0);

  // // The port can be used immediately after restart
  // int on = 1;
  // setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  // if (client_socket < 0) {
  //   RDMA_LOG(ERROR) << "MetaManager creates socket error: " << strerror(errno);
  //   close(client_socket);
  //   return -1;
  // }
  // if (connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
  //   RDMA_LOG(ERROR) << "MetaManager connect error: " << strerror(errno);
  //   close(client_socket);
  //   return -1;
  // }

  // /* --------------- Receiving hash metadata ----------------- */
  // size_t hash_meta_size = (size_t)1024 * 1024 * 1024;
  // char* recv_buf = (char*)malloc(hash_meta_size);
  // auto retlen = recv(client_socket, recv_buf, hash_meta_size, 0);
  // if (retlen < 0) {
  //   RDMA_LOG(ERROR) << "MetaManager receives hash meta error: " << strerror(errno);
  //   free(recv_buf);
  //   close(client_socket);
  //   return -1;
  // }
  // char ack[] = "[ACK]hash_meta_received_from_client";
  // send(client_socket, ack, strlen(ack) + 1, 0);
  // close(client_socket);
  // char* snooper = recv_buf;
  // // Get number of meta
  // size_t primary_meta_num = *((size_t*)snooper);
  // snooper += sizeof(primary_meta_num);
  // size_t backup_meta_num = *((size_t*)snooper);
  // snooper += sizeof(backup_meta_num);
  // node_id_t remote_machine_id = *((node_id_t*)snooper);
  // if (remote_machine_id >= MAX_REMOTE_NODE_NUM) {
  //   RDMA_LOG(FATAL) << "remote machine id " << remote_machine_id << " exceeds the max machine number";
  // }
  // snooper += sizeof(remote_machine_id);
  // // Get the `end of file' indicator: finish transmitting
  // char* eof = snooper + sizeof(HashMeta) * (primary_meta_num + backup_meta_num);
  // if ((*((uint64_t*)eof)) == MEM_STORE_META_END) {
  //   for (size_t i = 0; i < primary_meta_num; i++) {
  //     HashMeta meta;
  //     memcpy(&meta, (HashMeta*)(snooper + i * sizeof(HashMeta)), sizeof(HashMeta));
  //     primary_hash_metas[meta.table_id] = meta;
  //     primary_table_nodes[meta.table_id] = remote_machine_id;
  //     // RDMA_LOG(INFO) << "primary_node_ip: " << remote_ip << " table id: " << meta.table_id << " data_ptr: 0x" << std::hex << meta.data_ptr << " base_off: 0x" << meta.base_off << " bucket_num: " << std::dec << meta.bucket_num << " node_size: " << meta.node_size << " B";
  //   }
  //   snooper += sizeof(HashMeta) * primary_meta_num;
  //   for (size_t i = 0; i < backup_meta_num; i++) {
  //     HashMeta meta;
  //     memcpy(&meta, (HashMeta*)(snooper + i * sizeof(HashMeta)), sizeof(HashMeta));

  //     // if (backup_hash_metas.find(meta.table_id) == backup_hash_metas.end()) {
  //     //   backup_hash_metas[meta.table_id] = std::vector<HashMeta>();
  //     // }
  //     // if (backup_table_nodes.find(meta.table_id) == backup_table_nodes.end()) {
  //     //   backup_table_nodes[meta.table_id] = std::vector<node_id_t>();
  //     // }
  //     // backup_hash_metas[meta.table_id].push_back(meta);
  //     // backup_table_nodes[meta.table_id].push_back(remote_machine_id);
  //     // RDMA_LOG(INFO) << "backup_node_ip: " << remote_ip << " table id: " << meta.table_id << " data_ptr: " << std::hex << meta.data_ptr << " base_off: " << meta.base_off << " bucket_num: " << std::dec << meta.bucket_num << " node_size: " << meta.node_size;

  //     backup_hash_metas[meta.table_id].push_back(meta);
  //     backup_table_nodes[meta.table_id].push_back(remote_machine_id);

  //   }
  // } else {
  //   free(recv_buf);
  //   return -1;
  // }
  // free(recv_buf);
  // return remote_machine_id;
}

void MetaManager::GetMRMeta(const RemoteNode& node) {
  // Get remote node's memory region information via TCP
  std::cout <<"GetMRMeta: " << node.ip << ":" << node.port << "\n";
  MemoryAttr remote_txn_list_mr{}, remote_lock_buf_mr{}, remote_log_buf_mr{};
  MemoryAttr remote_sql_buf_mr{}, remote_plan_buf_mr{};
  MemoryAttr remote_join_plan_buf_mr{}, remote_join_block_buf_mr{};

  while (QP::get_remote_mr(node.ip, node.port, STATE_TXN_LIST_ID, &remote_txn_list_mr) != SUCC) {
    usleep(2000);
  }
  while (QP::get_remote_mr(node.ip, node.port, STATE_LOCK_BUF_ID, &remote_lock_buf_mr) != SUCC) {
    usleep(2000);
  }
  while (QP::get_remote_mr(node.ip, node.port, STATE_LOG_BUF_ID, &remote_log_buf_mr) != SUCC) {
    usleep(2000);
  }
  while(QP::get_remote_mr(node.ip, node.port, STATE_SQL_BUF_ID, &remote_sql_buf_mr) != SUCC) {
    usleep(2000);
  }
  while(QP::get_remote_mr(node.ip, node.port, STATE_PLAN_BUF_ID, &remote_plan_buf_mr) != SUCC) {
    usleep(2000);
  }
  // join plan & join block
  while(QP::get_remote_mr(node.ip, node.port, STATE_JOIN_PLAN_BUF_ID, &remote_join_plan_buf_mr) != SUCC) {
    usleep(2000);
  }
  while(QP::get_remote_mr(node.ip, node.port, STATE_JOIN_BLOCK_BUF_ID, &remote_join_block_buf_mr) != SUCC) {
    usleep(2000);
  }
  remote_txn_list_mrs[node.node_id] = remote_txn_list_mr;
  remote_lock_buf_mrs[node.node_id] = remote_lock_buf_mr;
  remote_log_buf_mrs[node.node_id] = remote_log_buf_mr;
  remote_sql_buf_mrs[node.node_id] = remote_sql_buf_mr;
  remote_plan_buf_mrs[node.node_id] = remote_plan_buf_mr;
  /*

  */
  remote_join_plan_buf_mrs[node.node_id]  = remote_join_plan_buf_mr;
  remote_join_block_buf_mrs[node.node_id] = remote_join_block_buf_mr;
  std::cout << "GetMRMeta Success\n";
}

bool MetaManager::create_instance(const std::string& config_path) {
  if(global_meta_mgr == nullptr)
      global_meta_mgr = new MetaManager(config_path);
  return (global_meta_mgr == nullptr);
}

void MetaManager::destroy_instance() {
  delete global_meta_mgr;
  global_meta_mgr = nullptr;
  std::cout << "destroy meta manager instance\n";
}