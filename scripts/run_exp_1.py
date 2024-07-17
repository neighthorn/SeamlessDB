import os
import subprocess
import time
import subprocess

param = 30
for i in range(1, 4):
    # Change directory to ../build/
    os.chdir("../build/")

    # Start rw_server active and redirect output to rw_server_active.txt
    active_output_file = open("rw_server_active_" + str(param) + ".txt", "w")
    active_process = subprocess.Popen(["./bin/rw_server", "active"], stdout=active_output_file, stderr=active_output_file)

    # Start rw_server backup and redirect output to rw_server_backup.txt
    backup_output_file = open("rw_server_backup_" + str(param) + ".txt", "w")
    backup_process = subprocess.Popen(["./bin/rw_server", "backup"], stdout=backup_output_file, stderr=backup_output_file)

    # sleep 
    time.sleep(10)
    
    # Start run_test.sh program
    test_process = subprocess.Popen(['bash', "/home/ysm/projects/cloud-rucbase/scripts/run_test.sh", str(param)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Wait for 30 minutes
    time.sleep(3000)

    # 刷新文件缓冲区
    if hasattr(os, 'fsync'):
        active_output_file.flush()
        os.fsync(active_output_file.fileno())
        active_output_file.close()
        backup_output_file.flush()
        os.fsync(backup_output_file.fileno())
        backup_output_file.close()
    # Kill all processes
    # active_process.kill()
    # backup_process.kill()
    test_process.kill()
    
    os.system("ps -ef | grep rw_server | grep -v grep | awk '{print $2}' | xargs kill -9")
    
    time.sleep(5)
    
    param += 20
