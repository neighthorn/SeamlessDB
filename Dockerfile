# 使用基于Ubuntu 22.04的镜像作为基础
FROM ubuntu:22.04

# 设置环境变量
ENV DEBIAN_FRONTEND=noninteractive

# 安装基本开发工具和调试工具
RUN apt-get update && apt-get install -y \
    build-essential \
    gdb \
    cmake \
    rsync \
    && rm -rf /var/lib/apt/lists/*

# 创建一个工作目录
WORKDIR /ysm

# 将本地的/home/ysm目录映射到容器中的/app目录
VOLUME /home/ysm:/ysm

# 设置入口点
CMD ["/bin/bash"]
