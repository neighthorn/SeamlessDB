import re
import csv

# 定义文件路径和待处理的theta和block size值
input_file = 'extract_results.txt'
theta_values = [0.0, 0.1, 0.5]
block_sizes = [100, 500, 900]
suspend_percentages = [40, 50, 60, 70, 80]

# 定义正则表达式来匹配所需的内容
theta_block_pattern = re.compile(r'Theta:\s*([\d\.]+),\s*Block Size:\s*(\d+)')
read_op_meta_pattern = re.compile(r'\[READ OP META FROM STATE\]')
reconnect_pattern = re.compile(r'\[time for reconnect:\s*(\d+)\]')

# 初始化数据结构
data = {theta: {block_size: {suspend: 0 for suspend in suspend_percentages} for block_size in block_sizes} for theta in theta_values}

# 解析文件并提取数据
with open(input_file, 'r') as file:
    lines = file.readlines()
    current_theta = None
    current_block_size = None
    current_suspend = None
    suspend_index = 0

    for line in lines:
        theta_block_match = theta_block_pattern.search(line)
        if theta_block_match:
            current_theta = float(theta_block_match.group(1))
            current_block_size = int(theta_block_match.group(2))
            suspend_index = 0

        if current_theta in theta_values and current_block_size in block_sizes:
            if read_op_meta_pattern.search(line):
                if suspend_index < len(suspend_percentages):
                    current_suspend = suspend_percentages[suspend_index]
                    suspend_index += 1

            reconnect_match = reconnect_pattern.search(line)
            if reconnect_match:
                reconnect_time = int(reconnect_match.group(1))
                data[current_theta][current_block_size][current_suspend] = reconnect_time / 1e6  # 转换为秒


# 排序
for theta in theta_values:
    for block_size in block_sizes:
        # data[theta][block_size]按从大到小的顺序排序
        reconnect_times = list(data[theta][block_size].values())
        reconnect_times.sort(reverse=True)
        i = 0
        for suspend in suspend_percentages:
            data[theta][block_size][suspend] = reconnect_times[i]
            i += 1
# 生成CSV文件
for theta in theta_values:
    for block_size in block_sizes:
        csv_file = f'theta_{theta}_blocksize_{block_size}.csv'
        with open(csv_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['theta', theta])
            writer.writerow(['block_size', block_size])
            writer.writerow(['suspend'] + [f'{suspend}%' for suspend in suspend_percentages])
            writer.writerow(['reconnect'] + [data[theta][block_size][suspend] for suspend in suspend_percentages])

print("CSV文件已生成。")
