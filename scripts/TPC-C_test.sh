#!/bin/bash

# 进入 build 文件夹
cd ../build || { echo "Failed to enter build directory"; exit 1; }

# 启动 ./bin/proxy
./bin/proxy rw &

# 等待 10 秒
sleep 10

# 查找并杀死名为 'rw_server active' 的进程
ps aux | grep 'rw_server active' | grep -v grep | awk '{print $2}' | xargs -r kill

# 再等待 10 秒
sleep 10

# 查找并杀死名为 'rw_server backup' 的进程
ps aux | grep 'rw_server backup' | grep -v grep | awk '{print $2}' | xargs -r kill
