import re
from collections import defaultdict

# 正则表达式匹配 s_i_id 和 s_w_id 的值
pattern = re.compile(r's_i_id=(\d+)\s+and\s+s_w_id=(\d+)')

# 初始化一个默认字典来记录键值对出现次数
counter = defaultdict(int)

# 读取输入文件
with open('build/proxy.log', 'r') as file:
    for line in file:
        match = pattern.search(line)
        if match:
            s_i_id = match.group(1)
            s_w_id = match.group(2)
            key = (s_i_id, s_w_id)
            counter[key] += 1

# 输出每个键值对及其出现次数
for key, count in counter.items():
    if count > 1:
        print(f'Key (s_i_id={key[0]}, s_w_id={key[1]}) appears {count} times.')
