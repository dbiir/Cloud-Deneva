#!/bin/bash

# 使用 pgrep 查找所有属于用户 wyl 且命令以 run 开头的进程
echo "正在查找所有符合条件的进程..."

processes=$(ps -u wyl -o pid,cmd | grep './run' | grep -v grep)

if [ -z "$processes" ]; then
    echo "没有找到任何符合条件的进程。"
else
    echo "找到的进程如下："
    echo "$processes"

    # 读取进程ID并杀死这些进程
    echo "正在杀死这些进程..."
    echo "$processes" | awk '{print $1}' | while read pid; do
        kill -9 $pid
        if [ $? -eq 0 ]; then
            echo "进程 $pid 已被杀死。"
        else
            echo "进程 $pid 杀死失败，请检查权限或使用sudo。"
        fi
    done
fi