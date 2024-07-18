import matplotlib.pyplot as plt
import numpy as np
import time

# key_seq = ["RC+E", "RC+P", "SI+E", "SI+P", "SER", "RC+TV", "SI+TV", "TriStar"]
key_seq = ["RC+E", "RC+P", "SI+E", "SI+P", "SER", "TriStar"]
key_seq_tpcc = ["RC+E", "RC+P", "SI", "SER", "TriStar"]

# key_to_colors = {
#     "RC+E": "#BB5441",
#     "RC+P": "#DFCFC4",
#     "SI+E": "#3570A8",
#     "SI+P": "#ADC2C5",
#     "SER": "#3E3554",
#     # "RC+TV": "BB5441",
#     # "SI+TV": "#3570A8",
#     "TriStar": "#C60035",
# }

key_to_colors = {
    "RC+E": "#BC8453",
    "RC+P": "#34553A",
    "SI+E": "#ABD073",
    "SI+P": "#78BB9C",
    "SI": "#3570A8",
    "SER": "#683836",
    # "RC+TV": "BB5441",
    # "SI+TV": "#3570A8",
    "TriStar": "#E1585F",
}

key_to_hatch = {
    "RC+E": "////",
    "RC+P": "\\\\",
    "SI+E": "--||",
    "SI+P": "-//",
    "SI": "O|",
    "SER": "///\\\\\\",
    # "RC+TV": "BB5441",
    # "SI+TV": "#3570A8",
    "TriStar": "..",
}

# 生成时间序列（0到20秒，每秒一个数据点）
time_points = np.linspace(0, 20, num=20)

# 第一组数据（假设这是你的第一组数据，随机生成的示例）
data1 = [1414,1579,1628,1606,1591,1590,1529,1501,1573,1581,854,1616,1522,1643,1615,1510,1623,1559,1559,1569]

# 第二组数据（假设这是你的第二组数据，随机生成的示例）
data2 = [1505,1664,1583,1555,1512,1481,1597,1608,1633,1617,669,1620,1610,1576,1617,1528,1508,1536,1567,1528]

# 绘图
fig, ax = plt.subplots(figsize=(12, 6))  # 设置整体图像的大小
fig.subplots_adjust(left=0.1, right=0.95, top=0.9, bottom=0.1)

ax.plot(time_points, data1, marker='o', linestyle='-', color='#3570A8', label='SeamlessDB')
ax.plot(time_points, data2, marker='s', linestyle='-', color='#BC8453', label='Baseline')

# 添加标签和标题
ax.set_xlabel('Time (seconds)', fontsize=16)
ax.set_ylabel('Transaction Throughput', fontsize=16)
ax.set_title('Transaction Throughput Over Time', fontsize=18)
ax.legend(fontsize=14)

# 设置横坐标刻度为每秒
ax.set_xticks(np.arange(0, 21, step=1))  # 每秒标记一次
ax.set_xlim(0, 20)  # 设置横坐标范围

# 显示网格
ax.yaxis.grid(True)  # 显示水平网格线
ax.xaxis.grid(False)  # 不显示竖直网格线

# 显示图形
# plt.tight_layout()
# plt.show()

plt.savefig('transaction_throughput.png', dpi=300)

plt.close()
