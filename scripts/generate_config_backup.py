import json
import os

# Directory to save the configuration files
output_dir = "config_files"
os.makedirs(output_dir, exist_ok=True)

# Loop over the range of state_theta and block_size values
for theta in [round(i * 0.1, 1) for i in range(11)]:
    for block_size in range(100, 1100, 200):
        # Configuration dictionary
        config = {
                "rw_node": {
                    "machine_id": 1,
                    "local_rpc_port": 12194,
                    "local_rdma_port": 12195,
                    "thread_num": 1,
                    "workload": "tpch",
                    "record_num": 1,
                    "state_open": 1,
                    "state_theta": theta,
                    "buffer_pool_size": 1310720,
                    "src_scale_factor": 1.0,
                    "block_size": block_size
                },
                "remote_state_nodes": {
                    "remote_ips": ["127.0.0.1"],
                    "remote_ports": [12193],
                    "txn_list_size_GB": 1,
                    "log_buf_size_GB": 1,
                    "lock_buf_size_GB": 1,
                    "sql_buf_size_GB": 1,
                    "plan_buf_size_GB": 1,
                    "cursor_buf_size_GB": 1
                }
        }

        # Generate file name
        file_name = f"compute_back_config_{theta}_{block_size}.json"
        file_path = os.path.join(output_dir, file_name)

        # Save configuration to file
        with open(file_path, 'w') as f:
            json.dump(config, f, indent=4)

        print(f"Generated {file_path}")
