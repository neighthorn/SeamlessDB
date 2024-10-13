#!/bin/bash

# 获取测试目录的路径作为输入参数
if [ -z "$1" ]; then
    echo "Usage: $0 <directory_path>"
    exit 1
fi

TEST_DIR=$1

# 创建临时文件进行读写测试
TEST_FILE="$TEST_DIR/testfile_io.tmp"

# 测试写入速度（512MB）
echo "Testing write speed..."
WRITE_SPEED=$(dd if=/dev/zero of=$TEST_FILE bs=1M count=512 conv=fdatasync 2>&1 | grep 'copied' | awk '{print $(NF-1), $NF}')

# 测试读取速度（512MB）
echo "Testing read speed..."
READ_SPEED=$(dd if=$TEST_FILE of=/dev/null bs=1M count=512 2>&1 | grep 'copied' | awk '{print $(NF-1), $NF}')

# 输出测试结果
echo "Write speed: $WRITE_SPEED"
echo "Read speed: $READ_SPEED"

# 删除测试文件
rm -f $TEST_FILE

echo "Disk I/O test completed."
