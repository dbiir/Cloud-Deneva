#!/bin/bash
ssh $1@$2 "ps -aux | grep rundb | grep -v ssh | grep -v grep | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
ssh $1@$2 "ps -aux | grep runcl | grep -v ssh | grep -v grep | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
ssh $1@$2 "ps -aux | grep runst | grep -v ssh | grep -v grep | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
