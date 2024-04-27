#!/bin/bash

HOSTS="$1"
PATHE="$2"
NODE_CNT="$3"
CL_NODE_CNT="$4"
ST_NODE_CNT="$5"
USERNAME="$6"
count=0

for HOSTNAME in ${HOSTS}; do
    #SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 10m 10m gdb -batch -ex \"run\" -ex \"bt\" --args ./rundb -nid${count} >> results.out 2>&1 | grep -v ^\"No stack.\"$"
    # count < $NODE_CNT, run db
    if [ $count -lt $NODE_CNT ]; then
        SCRIPT="cd ${PATHE};env SCHEMA_PATH=\"$2\" timeout -k 9 3m ${PATHE}rundb -nid${count} > ${PATHE}dbresults.out 2>&1"
        echo "${HOSTNAME}: rundb ${count}"
    # count >= $NODE_CNT & count < $NODE_CNT + $CL_NODE_CNT, run cl
    elif [ $count -lt `expr $NODE_CNT + $CL_NODE_CNT` ]; then
        SCRIPT="cd ${PATHE};env SCHEMA_PATH=\"$2\" timeout -k 9 3m ${PATHE}runcl -nid${count} > ${PATHE}clresults.out 2>&1"
        echo "${HOSTNAME}: runcl ${count}"
    # count >= $NODE_CNT + $CL_NODE_CNT, run st
    else
        SCRIPT="cd ${PATHE};env SCHEMA_PATH=\"$2\" timeout -k 9 3m ${PATHE}runst -nid${count} > ${PATHE}stresults.out 2>&1"
        echo "${HOSTNAME}: runst ${count}"
    fi
    ssh -n -o BatchMode=yes -o StrictHostKeyChecking=no ${USERNAME}@${HOSTNAME} "${SCRIPT}" &
    count=`expr $count + 1`
done

if [ $7 -ne 0 ]
then
    sleep 60
    OLD_IFS="$IFS"
    IFS=" "
    HOSTLIST=($HOSTS)
    IFS="$OLD_IFS"
    scp run_perf.sh ${USERNAME}@${HOSTLIST[0]}:${PATHE}
    ssh ${USERNAME}@${HOSTLIST[0]} "bash ${PATHE}/run_perf.sh $7 $8"
fi

while [ $count -gt 0 ]
do
    wait $pids
    count=`expr $count - 1`
done
