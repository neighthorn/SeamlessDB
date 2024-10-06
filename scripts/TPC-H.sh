#!/bin/bash

# 定义常量 normal_time, SQL正常执行时间(s)
normal_time=10

# 检查是否传入 percent 参数
if [ -z "$1" ]; then
  echo "Usage: $0 <percent>"
  exit 1
fi

# 获取 percent 参数，范围为 0-100
percent=$1
if ! [[ "$percent" =~ ^[0-9]+$ ]] || [ "$percent" -lt 0 ] || [ "$percent" -gt 100 ]; then
  echo "Percent must be an integer between 0 and 100."
  exit 1
fi

# 计算第一次 sleep 的时间
sleep_time=$(echo "$normal_time * $percent / 100" | bc)

# 进入 build 文件夹
cd ../build || { echo "Failed to enter build directory"; exit 1; }

# 启动 ./bin/proxy
./bin/proxy &

# 第一次 sleep
sleep "$sleep_time"

# 查找并杀死名为 'rw_server active' 的进程
ps aux | grep 'rw_server active' | grep -v grep | awk '{print $2}' | xargs -r kill

# 再等待 10 秒
# sleep 10

# 查找并杀死名为 'rw_server backup' 的进程
# ps aux | grep 'rw_server backup' | grep -v grep | awk '{print $2}' | xargs -r kill
