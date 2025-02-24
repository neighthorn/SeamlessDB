import os
import time
import subprocess

# 正常运行时间500s

# 实验参数
param_sets = [(10, 1), (10, 2), (10, 3), (30, 1), (30, 2), (30, 3), (50, 1), (50, 2), (50, 3), (70, 1), (70, 2), (70, 3), (90, 1), (90, 2), (90, 3)]

# 进入../build文件夹
os.chdir("../build")

# 运行12组实验
for P, cnt in param_sets:
    print(f"Running experiment with P={P}, cnt={cnt}")

    # 运行 active ro 并保存输出
    with open(f"active_{P}_{cnt}.txt", "w") as active_file:
        active_proc = subprocess.Popen(["./bin/rw_server", "active", "ro"], stdout=active_file)
    
    # 运行 backup ro 并保存输出
    with open(f"backup_{P}_{cnt}.txt", "w") as backup_file:
        backup_proc = subprocess.Popen(["./bin/rw_server", "backup", "ro"], stdout=backup_file)
    
    # sleep 15 秒
    time.sleep(15)

    # 运行 bash /root/SeamlessDB/scripts/run_test.sh 并保存输出
    with open(f"proxy_{P}_{cnt}.txt", "w") as proxy_file:
        subprocess.run(["bash", "/root/SeamlessDB/scripts/run_test.sh", str(P)], stdout=proxy_file)
    
    # sleep 1000 秒
    time.sleep(600)

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


for cnt in range(1, 4):
    with open(f"active_0_{cnt}.txt", "w") as active_file:
        active_proc = subprocess.Popen(["./bin/rw_server", "active", "ro"], stdout=active_file)

    with open(f"backup_0_{cnt}.txt", "w") as backup_file:
        backup_proc = subprocess.Popen(["./bin/rw_server", "backup", "ro"], stdout=backup_file)

    time.sleep(15)

    with open(f"proxy_0_{cnt}.txt", "w") as proxy_file:
        subprocess.run(["./bin/proxy", "ro"], stdout=proxy_file)
    
    time.sleep(600)

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

    subprocess.run("ps -ef | grep rw_server | grep -v grep | awk '{print $2}' | xargs kill -9", shell=True)
    subprocess.run("ps -ef | grep proxy | grep -v grep | awk '{print $2}' | xargs kill -9", shell=True)

    time.sleep(5)



subprocess.call("cp /root/SeamlessDB/src/config/compute_server_config_nockpt.json /root/SeamlessDB/src/config/compute_server_config.json", shell=True)

for cnt in range(1, 4):
    with open(f"no_ckpt_active_{cnt}.txt", "w") as active_file:
        active_proc = subprocess.Popen(["./bin/rw_server", "active", "ro"], stdout=active_file)

    with open(f"no_ckpt_backup_{cnt}.txt", "w") as backup_file:
        backup_proc = subprocess.Popen(["./bin/rw_server", "backup", "ro"], stdout=backup_file)

    time.sleep(15)

    with open(f"no_ckpt_proxy_{cnt}.txt", "w") as proxy_file:
        subprocess.run(["./bin/proxy", "ro"], stdout=proxy_file)
    
    time.sleep(600)

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

    subprocess.run("ps -ef | grep rw_server | grep -v grep | awk '{print $2}' | xargs kill -9", shell=True)
    subprocess.run("ps -ef | grep proxy | grep -v grep | awk '{print $2}' | xargs kill -9", shell=True)

    time.sleep(5)
