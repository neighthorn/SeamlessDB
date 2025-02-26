import os
import re
import csv

# 参数列表
param_sets = [
            ("Q3", 1, 0, 1), ("Q3", 1, 0, 2),
            ("Q3", 1, 1, 1), ("Q3", 1, 1, 2), 
            ("Q3", 2, 0, 1), ("Q3", 2, 0, 2),
            ("Q3", 2, 1, 1), ("Q3", 2, 1, 2),
            ("Q3", 3, 0, 1), ("Q3", 3, 0, 2),
            ("Q3", 3, 1, 1), ("Q3", 3, 1, 2),
            ("Q3", 4, 0, 1), ("Q3", 4, 0, 2),
            ("Q3", 4, 1, 1), ("Q3", 4, 1, 2),
            ("Q3", 8, 0, 1), ("Q3", 8, 0, 2),
            ("Q3", 8, 1, 1), ("Q3", 8, 1, 2),

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
            ("Q10", 8, 1, 1), ("Q10", 8, 1, 2)
]

# 定义 CSV 文件的路径
csv_file = 'parallel_factor_latency_results.csv'

# CSV 文件头
header = ['query', 'parallel_factor', 'state_open', 'cnt', 'latency_seconds']

os.chdir('../build/')  # 切换到上级目录

# 创建一个 CSV 文件并写入头
with open(csv_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(header)

    # 遍历每组参数
    for query, parallel_factor, state_open, cnt in param_sets:
        # 构造对应的输出文件名
        output_file = f"{query}_proxy_{parallel_factor}_{state_open}_{cnt}.txt"
        latency = -1  # 默认值

        # 检查文件是否存在
        if os.path.exists(output_file):
            try:
                # 尝试用 utf-8 解码
                with open(output_file, 'r', encoding='utf-8') as file:
                    content = file.read()
            except UnicodeDecodeError:
                # 如果 utf-8 解码失败，尝试其他编码
                try:
                    with open(output_file, 'r', encoding='ISO-8859-1') as file:
                        content = file.read()
                except UnicodeDecodeError:
                    try:
                        with open(output_file, 'r', encoding='windows-1252') as file:
                            content = file.read()
                    except Exception as e:
                        print(f"Error opening {output_file} with different encodings: {e}")
                        content = None

            # 如果读取内容成功，查找 ro_latency
            if content:
                match = re.search(r'ro latency: (\d+)', content)
                if match:
                    # 获取 latency 值并转换为秒
                    latency_ms = int(match.group(1))
                    latency = latency_ms / 1000.0  # 转换为秒
        # 将结果写入 CSV 文件
        writer.writerow([query, parallel_factor, state_open, cnt, latency])

print("结果已保存到", csv_file)
