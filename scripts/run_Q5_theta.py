import os
import time
import subprocess

# 实验参数
# param_sets = [(10, 1), (10, 2), (10, 3), (30, 1), (30, 2), (30, 3), (50, 1), (50, 2), (50, 3), (70, 1), (70, 2), (70, 3), (90, 1), (90, 2), (90, 3)]
queries = ["Q5"]
parallel_factors = [4]
theta_values = [0.3, 1, 10, 20, 100, 500]
cnts = [1, 2]
failover_points = [10, 30, 50, 70, 90]

param_sets = [(query, theta, failover_point, cnt) for query in queries for theta in theta_values for failover_point in failover_points for cnt in cnts]

# 进入../build文件夹
os.chdir("../build")

# 运行12组实验
for query, theta, failover_point, cnt in param_sets:
    print(f"Running experiment with query={query}, theta={theta}, failover_point={failover_point}, cnt={cnt}")

    proxy_config_file_path = f"/root/SeamlessDB/src/config/proxy/proxy_config_{query}.json"
    compute_server_config_file_path = f"/root/SeamlessDB/src/config/ablation_study_theta/compute_server_config_{theta}.json"
    compute_back_config_file_path = f"/root/SeamlessDB/src/config/ablation_study_theta/compute_back_config_{theta}.json"

    print(f"cp {compute_server_config_file_path} /root/SeamlessDB/src/config/compute_server_config.json")
    print(f"cp {compute_back_config_file_path} /root/SeamlessDB/src/config/compute_back_config.json")
    print(f"cp {proxy_config_file_path} /root/SeamlessDB/src/config/proxy_config.json")

    os.system(f"cp {proxy_config_file_path} /root/SeamlessDB/src/config/proxy_config.json")
    os.system(f"cp {compute_server_config_file_path} /root/SeamlessDB/src/config/compute_server_config.json")
    os.system(f"cp {compute_back_config_file_path} /root/SeamlessDB/src/config/compute_back_config.json")
    
    # 运行 active ro 并保存输出
    with open(f"{query}_active_{theta}_{failover_point}_{cnt}.txt", "w") as active_file:
        active_proc = subprocess.Popen(["./bin/rw_server", "active", "ro"], stdout=active_file)
    
    # 运行 backup ro 并保存输出
    with open(f"{query}_backup_{theta}_{failover_point}_{cnt}.txt", "w") as backup_file:
        backup_proc = subprocess.Popen(["./bin/rw_server", "backup", "ro"], stdout=backup_file)
    
    # sleep 15 秒
    time.sleep(15)

    # 运行 bash /root/SeamlessDB/scripts/run_test.sh 并保存输出
    with open(f"{query}_proxy_{theta}_{failover_point}_{cnt}.txt", "w") as proxy_file:
        subprocess.run(["bash", "/root/SeamlessDB/scripts/run_test.sh", str(failover_point)], stdout=proxy_file)
    
    # sleep 1000 秒
    sleep_time = 400

    if theta > 20:
        sleep_time = 700
    time.sleep(sleep_time)

    # if(hasattr(os, 'fsync')):
    #     active_file.flush()
    #     os.fsync(active_file.fileno())
    #     active_file.close()
    #     backup_file.flush()
    #     os.fsync(backup_file.fileno())
    #     backup_file.close()
    #     proxy_file.flush()
    #     os.fsync(proxy_file.fileno())
    #     proxy_file.close()

    # 终止 rw_server 进程
    subprocess.run("ps -ef | grep rw_server | grep -v grep | awk '{print $2}' | xargs kill -9", shell=True)
    subprocess.run("ps -ef | grep proxy | grep -v grep | awk '{print $2}' | xargs kill -9", shell=True)

    time.sleep(5)
