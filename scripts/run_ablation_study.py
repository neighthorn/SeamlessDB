import os
import time
import subprocess

# 生成新的参数集: <query, parallel_factor, interval, cnt>
# queries = [ "Q6", "Q10"]
queries = ["Q3"]
parallel_factors = [1,2,3,4,8]
# intervals = [3, 5, 10]
cnts = [1, 2, 3]

param_sets = [(q, p, c) for q in queries for p in parallel_factors for c in cnts]

# 进入 ../build 文件夹
os.chdir("../build")

for query, parallel_factor, cnt in param_sets:
    print(f"Running experiment with query={query}, parallel_factor={parallel_factor}, cnt={cnt}")

    proxy_config_file_path = f"/root/SeamlessDB/src/config/proxy/proxy_config_{query}.json"
    compute_server_config_file_path = f"/root/SeamlessDB/src/config/ablation_study/compute_server_config_p{parallel_factor}_state.json"

    print(f"cp {compute_server_config_file_path} /root/SeamlessDB/src/config/compute_server_config.json")
    print(f"cp {proxy_config_file_path} /root/SeamlessDB/src/config/proxy_config.json")

    os.system(f"cp {proxy_config_file_path} /root/SeamlessDB/src/config/proxy_config.json")
    os.system(f"cp {compute_server_config_file_path} /root/SeamlessDB/src/config/compute_server_config.json")

    # 运行 active ro 并保存输出
    with open(f"{query}_active_{parallel_factor}_{cnt}.txt", "w") as active_file:
        active_proc = subprocess.Popen(["./bin/rw_server", "active", "ro"], stdout=active_file)

    # 运行 backup ro 并保存输出
    with open(f"{query}_backup_{parallel_factor}_{cnt}.txt", "w") as backup_file:
        backup_proc = subprocess.Popen(["./bin/rw_server", "backup", "ro"], stdout=backup_file)
    
    # sleep 15 秒
    time.sleep(15)

    output_file = f"{query}_proxy_{parallel_factor}_{cnt}.txt"
    os.system(f"./bin/proxy ro > {output_file} 2>&1")

    # 终止 rw_server 进程
    subprocess.run("ps -ef | grep rw_server | grep -v grep | awk '{print $2}' | xargs kill -9", shell=True)
    
    time.sleep(5)
