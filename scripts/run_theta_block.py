import os
import shutil
import time
import subprocess

# Variable for the normal time
normal_time = 280  # Replace with your desired normal time

# Function to run the experiment
def run_experiment(block, theta, progress):
    # Define source and destination paths
    src_dir = "/home/ysm/projects/cloud-rucbase/scripts/config_files"
    dest_dir = "/home/ysm/projects/cloud-rucbase/src/config"
    build_dir = "/home/ysm/projects/cloud-rucbase/build"
    tpch_result_dir = "../tpch_result"
    
    # Ensure the destination and result directories exist
    os.makedirs(dest_dir, exist_ok=True)
    os.makedirs(tpch_result_dir, exist_ok=True)

    # Copy and rename configuration files
    src_config_file = f"{src_dir}/compute_server_config_{theta}_{block}.json"
    dest_config_file = f"{dest_dir}/compute_server_config.json"
    print(f"Copying {src_config_file} to {dest_config_file}")
    shutil.copyfile(src_config_file, dest_config_file)

    src_back_config_file = f"{src_dir}/compute_back_config_{theta}_{block}.json"
    dest_back_config_file = f"{dest_dir}/compute_back_config.json"
    shutil.copyfile(src_back_config_file, dest_back_config_file)

    # Navigate to the build directory
    os.chdir(build_dir)

    # Run state_pool
    # state_pool_process = subprocess.Popen(["./bin/state_pool"])
    # time.sleep(10)
    # print("finished run state pool")

    # Run rw_server active and redirect output
    active_output_file = f"{tpch_result_dir}/active_{theta}_{block}.output"
    with open(active_output_file, 'w') as active_output:
        active_process = subprocess.Popen(["./bin/rw_server", "active", "ro"], stdout=active_output)
    
    # Run rw_server backup and redirect output
    back_output_file = f"{tpch_result_dir}/back_{theta}_{block}.output"
    with open(back_output_file, 'w') as back_output:
        backup_process = subprocess.Popen(["./bin/rw_server", "backup", "ro"], stdout=back_output)

    time.sleep(10)

    # Run proxy
    proxy_process = subprocess.Popen(["./bin/proxy", "ro"])

    # Wait for normal_time * 80% and then terminate active server
    time.sleep(normal_time * progress)
    # active_process.terminate()
    active_process.kill()
    # time.sleep(2)  # Wait to allow buffer flushing
    # active_process.wait()

    # Wait for normal_time * 50% and then terminate backup server
    time.sleep(normal_time * (1.3 - progress))
    backup_process.kill()
    # backup_process.terminate()
    # time.sleep(2)  # Wait to allow buffer flushing
    # backup_process.wait()

    # Kill proxy and state_pool processes
    proxy_process.kill()
    
    os.system("ps -ef | grep rw_server | grep -v grep | awk '{print $2}' | xargs kill -9")
    # os.system("ps -ef | grep rw_server | grep -v grep | awk '{print $3}' | xargs kill -9")
    
    time.sleep(5)
    # state_pool_process.kill()

# Example call to run_experiment function
# theta_range = [0.0, 0.1, 0.5]
# block_range = [100, 500, 900]
# progress_range = [0.4, 0.5, 0.6, 0.7, 0.8]
# for theta in theta_range:
#     for block_size in block_range:
#         for progress in progress_range:
#             run_experiment(block_size, theta, progress)

configs = [
    [0.0, 500, 0.4], 
    [0.1, 500, 0.4], 
    [0.5, 500, 0.4]
]

for config in configs:
    run_experiment(config[1], config[0], config[2])
