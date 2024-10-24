'''
Author: ZhaoyangZhang
Date: 2024-07-11 14:11:36
LastEditors: Do not edit
LastEditTime: 2024-07-17 20:12:14
FilePath: /draws/seamlessdb/tpch/tpch_queryprocess_overhead.py
'''
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams, font_manager

font_dirs = ['/usr/share/fonts/truetype/msttcorefonts/']  # 替换为你的字体路径
font_files = font_manager.findSystemFonts(fontpaths=font_dirs)
for font_file in font_files:
    font_manager.fontManager.addfont(font_file)

rcParams['font.family'] = 'serif'
rcParams['font.serif'] = ['Times New Roman', 'Liberation Serif', 'serif']

# 提供的数据
query_progress = [10, 30, 50, 70, 90]  # 查询进度从0%到90%

# Q6
# Baseline执行时间（无状态恢复的总时间），转换为秒
baseline_execution_time = np.array([
    154.775,
    186.734,
    217.046,
    244.436,
    271.374,
])

# SeamlessDB执行时间（有状态恢复的总时间），转换为秒
seamlessdb_execution_time = np.array([
    148.16,
    145.719,
    146.411,
    143.604,
    144.544,
])

# 创建图表
fig, ax = plt.subplots(figsize=(7, 6))  # 调整图表大小
fig.subplots_adjust(left=0.1, right=0.95, top=0.9, bottom=0.1)

bar_width = 4  # 设置柱状图的宽度
index = np.arange(len(query_progress))  # 横坐标

# 设置柱状图，并添加不同的hatch
bar1 = ax.bar(index - bar_width/2, baseline_execution_time, bar_width, label='Baseline', color='#85C3DC', hatch='x')
bar2 = ax.bar(index + bar_width/2, seamlessdb_execution_time, bar_width, label='SeamlessDB', color='#E2C098', hatch='/')

# 添加图例到图表顶部居中
legend = ax.legend(fontsize=22, loc='upper center', bbox_to_anchor=(0.5, 1.2), ncol=2, frameon=False)
ax.grid(axis='y', linestyle='-')

# 添加标题和标签
ax.set_xlabel('Instance Transition Point (%)', fontsize=28)  # 调大字体
ax.set_ylabel('Latency (s)', fontsize=28)  # 调大字体

# 设置x轴范围和标签
ax.set_xlim(-1, len(query_progress))
ax.set_xticks(index)
ax.set_xticklabels([str(val) for val in query_progress], fontsize=22)

# 设置y轴范围
ax.set_ylim(0, 4500)
ax.set_yticks([1000, 2000, 3000, 4000])

# 美化图表
ax.tick_params(axis='x', labelsize=22)  # 调大字体
ax.tick_params(axis='y', labelsize=22)  # 调大字体

fig.tight_layout()

# 保存图表
fig.savefig('tpch_queryprogress_execution_time.pdf', dpi=300)
plt.close(fig)
