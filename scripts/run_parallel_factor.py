import os
import time
import subprocess

# 实验参数: <Query, parallel_factor, state_open, cnt> 
# param_sets = [(10, 1), (10, 2), (10, 3), (30, 1), (30, 2), (30, 3), (50, 1), (50, 2), (50, 3), (70, 1), (70, 2), (70, 3), (90, 1), (90, 2), (90, 3)]
param_sets = [
            # ("Q3", 1, 0, 1), ("Q3", 1, 0, 2),
            # ("Q3", 1, 1, 1), ("Q3", 1, 1, 2), 
            # ("Q3", 2, 0, 1), ("Q3", 2, 0, 2),
            # ("Q3", 2, 1, 1), ("Q3", 2, 1, 2),
            # ("Q3", 3, 0, 1), ("Q3", 3, 0, 2),
            # ("Q3", 3, 1, 1), ("Q3", 3, 1, 2),
            # ("Q3", 4, 0, 1), ("Q3", 4, 0, 2),
            # ("Q3", 4, 1, 1), ("Q3", 4, 1, 2),
            # ("Q3", 8, 0, 1), ("Q3", 8, 0, 2),
            # ("Q3", 8, 1, 1), ("Q3", 8, 1, 2),

            ("Q5", 1, 0, 1), ("Q5", 1, 0, 2),
            ("Q5", 1, 1, 1), ("Q5", 1, 1, 2), 
            ("Q5", 2, 0, 1), ("Q5", 2, 0, 2),
            ("Q5", 2, 1, 1), ("Q5", 2, 1, 2),
            ("Q5", 3, 0, 1), ("Q5", 3, 0, 2),
            ("Q5", 3, 1, 1), ("Q5", 3, 1, 2),
            ("Q5", 4, 0, 1), ("Q5", 4, 0, 2),
            ("Q5", 4, 1, 1), ("Q5", 4, 1, 2),
            ("Q5", 8, 0, 1), ("Q5", 8, 0, 2),
            ("Q5", 8, 1, 1), ("Q5", 8, 1, 2),

            ("Q6", 1, 0, 1), ("Q6", 1, 0, 2),
            ("Q6", 1, 1, 1), ("Q6", 1, 1, 2), 
            ("Q6", 2, 0, 1), ("Q6", 2, 0, 2),
            ("Q6", 2, 1, 1), ("Q6", 2, 1, 2),
            ("Q6", 3, 0, 1), ("Q6", 3, 0, 2),
            ("Q6", 3, 1, 1), ("Q6", 3, 1, 2), 
            ("Q6", 4, 0, 1), ("Q6", 4, 0, 2),
            ("Q6", 4, 1, 1), ("Q6", 4, 1, 2),
            ("Q6", 8, 0, 1), ("Q6", 8, 0, 2),
            ("Q6", 8, 1, 1), ("Q6", 8, 1, 2),

            ("Q10", 1, 0, 1), ("Q10", 1, 0, 2),
            ("Q10", 1, 1, 1), ("Q10", 1, 1, 2), 
            ("Q10", 2, 0, 1), ("Q10", 2, 0, 2),
            ("Q10", 2, 1, 1), ("Q10", 2, 1, 2),
            ("Q10", 3, 0, 1), ("Q10", 3, 0, 2),
            ("Q10", 3, 1, 1), ("Q10", 3, 1, 2), 
            ("Q10", 4, 0, 1), ("Q10", 4, 0, 2),
            ("Q10", 4, 1, 1), ("Q10", 4, 1, 2),
            ("Q10", 8, 0, 1), ("Q10", 8, 0, 2),
            ("Q10", 8, 1, 1), ("Q10", 8, 1, 2),
]

# 进入../build文件夹
os.chdir("../build")

for query, parallel_factor, state_open, cnt in param_sets:
    print(f"Running experiment with query={query}, parallel_factor={parallel_factor}, state_open={state_open}, cnt={cnt}")

    proxy_config_file_path = f"/root/SeamlessDB/src/config/proxy/proxy_config_{query}.json"
    compute_server_config_file_path = ""
    if state_open == 0:
        compute_server_config_file_path = f"/root/SeamlessDB/src/config/state_close/compute_server_config_p{parallel_factor}_no_state.json"
    else:
        compute_server_config_file_path = f"/root/SeamlessDB/src/config/state_open/compute_server_config_p{parallel_factor}_state.json"

    print(f"cp {compute_server_config_file_path} /root/SeamlessDB/src/config/compute_server_config.json")

    os.system(f"cp {proxy_config_file_path} /root/SeamlessDB/src/config/proxy_config.json")
    os.system(f"cp {compute_server_config_file_path} /root/SeamlessDB/src/config/compute_server_config.json")

    # 运行 active ro 并保存输出
    with open(f"{query}_active_{parallel_factor}_{state_open}_{cnt}.txt", "w") as active_file:
        active_proc = subprocess.Popen(["./bin/rw_server", "active", "ro"], stdout=active_file)

    # 运行 backup ro 并保存输出
    with open(f"{query}_backup_{parallel_factor}_{state_open}_{cnt}.txt", "w") as backup_file:
        backup_proc = subprocess.Popen(["./bin/rw_server", "backup", "ro"], stdout=backup_file)
    
    # sleep 5 秒
    time.sleep(15)

    output_file = f"{query}_proxy_{parallel_factor}_{state_open}_{cnt}.txt"
    os.system(f"./bin/proxy ro > {output_file} 2>&1")

    # # 终止 rw_server 进程
    subprocess.run("ps -ef | grep rw_server | grep -v grep | awk '{print $2}' | xargs kill -9", shell=True)
    
    time.sleep(5)

