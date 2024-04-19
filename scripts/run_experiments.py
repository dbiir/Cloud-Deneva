#!/usr/bin/python3

import os,sys,datetime,re
import shlex
import subprocess
from experiments import *
from helper import *
from run_config import *
import time
import signal

def custom_signal_handler(signum, frame):
    print('Signal handler called with signal', signum)
    print('Exiting...')
    sys.exit(0)

signal_handler = signal.signal(signal.SIGINT, custom_signal_handler)

now = datetime.datetime.now()
strnow=now.strftime("%Y%m%d-%H%M%S")

os.chdir('..')

PATH=os.getcwd()

result_dir = PATH + "/results/" + strnow + '/'

execute = True
remote = True
cluster = 'vcloud'
skip = False

exps=[]
perf_checked = False
has_perf = False
perfTime = 60
fromtimelist=[]
totimelist=[]

keywords = ['tput', 'local_txn_abort_cnt', 'hdcc_calvin_cnt', 'hdcc_calvin_local_cnt', 'hdcc_silo_cnt', 'hdcc_silo_local_cnt']
keywords_cal_type = ['sum', 'sum', 'sum', 'sum', 'sum', 'sum']
draw_keywords = ['tput']

if len(sys.argv) < 2:
     sys.exit("Usage: %s [-exec/-e/-noexec/-ne] [-c cluster] experiments\n \
            -exec/-e: compile and execute locally\n \
            -noexec/-ne: compile first target only \
            -c: run remote on cluster defined in run_config.py(default)\n \
            " % sys.argv[0])

for arg in sys.argv[1:]:
    if arg == "--help" or arg == "-h":
        sys.exit("Usage: %s [-exec/-e/-noexec/-ne] [-skip] [-c cluster] experiments\n \
                -exec/-e: compile and execute locally\n \
                -noexec/-ne: compile first target only \
                -skip: skip any experiments already in results folder\n \
                -c: run remote on cluster defined in run_config.py(default)\n \
                " % sys.argv[0])
    if arg == "--exec" or arg == "-e":
        execute = True
        remote = False
    elif arg == "--noexec" or arg == "-ne":
        execute = False
    elif arg == "--skip":
        skip = True
    elif arg == "-c":
        execute = True
        remote = True
    else:
        exps.append(arg)

for exp in exps:
    fmt,experiments = experiment_map[exp]()

    experiment_dir = result_dir + exp + '/'
    perf_dir = experiment_dir + 'perf/'
    cmd = "mkdir -p {}".format(perf_dir)
    print(cmd)
    os.system(cmd)

    calculate_sets = []
    variable1 = []
    variable2 = []

    for e in experiments:
        cfgs = get_cfgs(fmt,e)
        if remote:
            cfgs["TPORT_TYPE"], cfgs["TPORT_PORT"] = "tcp", 7000
        output_f = get_outfile_name(cfgs, fmt)
        output_dir = output_f + "/"
        output_f += strnow

        f = open("config.h", 'r')
        lines = f.readlines()
        f.close()
        with open("config.h", 'w') as f_cfg:
            for line in lines:
                found_cfg = False
                for c in cfgs:
                    found_cfg = re.search("#define " + c + "\t", line) or re.search("#define " + c + " ", line)
                    if found_cfg:
                        f_cfg.write("#define " + c + " " + str(cfgs[c]) + "\n")
                        break
                if not found_cfg:
                    f_cfg.write(line)

        cmd = "make clean; make -j32"
        print(cmd)
        res = os.system(cmd)
        if res != 0:
            print('\033[91m' + 'fail to compile\n' + '\033[0m', file=sys.stderr)
            continue
        if not execute:
            continue

        cmd = "cp config.h {}{}.cfg".format(experiment_dir,output_f)
        print(cmd)
        os.system(cmd)

        nnodes = cfgs["NODE_CNT"]
        nclnodes = cfgs["CLIENT_NODE_CNT"]
        nstnodes = cfgs["STORAGE_NODE_CNT"]
        if nclnodes == "NODE_CNT":
            nclnodes = nnodes
        if nstnodes == "NODE_CNT":
            nstnodes = nnodes

        if remote:
            machines_ = vcloud_machines
            location = deploy_location
            uname = username

            machines = machines_[:(cfgs["NODE_CNT"])]+machines_[len(machines_)//3:(len(machines_)//3+nclnodes)]+machines_[2*len(machines_)//3:(2*len(machines_)//3+nstnodes)]
            with open("ifconfig.txt", 'w') as f_ifcfg:
                for m in machines:
                    f_ifcfg.write(m + "\n")

            distinct_machines = []
            for m in machines:
                if m not in distinct_machines:
                    distinct_machines.append(m)

            if cfgs["WORKLOAD"] == "TPCC":
                files = ["rundb", "runcl", "runst", "ifconfig.txt", "./benchmarks/TPCC_short_schema.txt", "./benchmarks/TPCC_full_schema.txt"]
            elif cfgs["WORKLOAD"] == "YCSB":
                files = ["rundb", "runcl", "runst", "ifconfig.txt", "benchmarks/YCSB_schema.txt"]
            for m in distinct_machines:
                cmd = './scripts/kill.sh {} {}'.format(uname, m)
                print(cmd)
                os.system(cmd)
            for m, f in itertools.product(distinct_machines, files):
                cmd = 'scp {}/{} {}@{}:/{}'.format(PATH, f, uname, m, location)
                print(cmd)
                os.system(cmd)

            if perf_checked == False:
                cmd = 'ssh {}@{} "which perf"'.format(uname, machines[0])
                print(cmd)
                res = os.system(cmd)
                perf_checked = True
                if res == 0:
                    has_perf = True
                else:
                    perfTime = 0

            print("Deploying: {}".format(output_f))
            os.chdir('./scripts')
            cmd = './vcloud_deploy.sh \'{}\' /{}/ {} {} {} {} {} {}'.format(' '.join(machines), location, nnodes, nclnodes, nstnodes, uname, perfTime, deploy_location)
            print(cmd)
            fromtimelist.append(str(int(time.time())) + "000")
            os.system(cmd)
            totimelist.append(str(int(time.time())) + "000")

            if has_perf:
                perf_machine = machines[0]
                cmd = "scp {}@{}:/{}/perf.data {}{}.data".format(uname, perf_machine, location, perf_dir, output_f)
                print(cmd)
                os.system(cmd)

            os.chdir('..')
            for m, n in zip(machines, range(cfgs["NODE_CNT"])):
                cmd = 'scp {}@{}:/{}/dbresults.out {}/{}_{}.out'.format(uname, m, location, experiment_dir, n, output_f)
                print(cmd)
                os.system(cmd)
            for m,n in zip(machines[len(machines)//3:], range(cfgs["NODE_CNT"])):
                cmd = 'scp {}@{}:/{}/clresults.out {}/{}_{}.out'.format(uname, m, location, experiment_dir, n+cfgs["NODE_CNT"], output_f)
                print(cmd)
                os.system(cmd)
            for m,n in zip(machines[2*len(machines)//3:], range(cfgs["NODE_CNT"])):
                cmd = 'scp {}@{}:/{}/stresults.out {}/{}_{}.out'.format(uname, m, location, experiment_dir, n+2*cfgs["NODE_CNT"], output_f)
                print(cmd)
                os.system(cmd)

        else:
            pids = []
            print("Deploying: {}".format(output_f))
            for n in range(nnodes+nclnodes+nstnodes):
                if n < nnodes:
                    cmd = "./rundb -nid{}".format(n)
                elif n < nnodes + nclnodes:
                    cmd = "./runcl -nid{}".format(n)
                else:
                    cmd = "./runst -nid{}".format(n)
                print(cmd)
                cmd = shlex.split(cmd)
                ofile_n = "{}{}_{}.out".format(experiment_dir,n,output_f)
                ofile = open(ofile_n,'w')
                p = subprocess.Popen(cmd,stdout=ofile,stderr=ofile)
                pids.insert(0,p)
            for n in range(nnodes + nclnodes):
                pids[n].wait()

        tmp_path = os.getcwd()
        os.chdir(experiment_dir)
        simple_f = open('simple_summary.txt', 'a')
        simple_f.write('nodes: ' + str(machines[:cfgs['NODE_CNT']]) + '\n')
        simple_f.write(str(fmt) + '\n')
        simple_f.write(str(e) + '\n')
        calculate = {}
        for i in range(cfgs['NODE_CNT']):
            searchfile = str(i) + '_' + output_f + '.out'
            with open(searchfile, 'r') as f:
                content = f.read()
            simple_f.write('node ' + str(i) + ':\n')

            results = {}
            for keyword in keywords:
                pattern = r"\b" + keyword + r"\b\s*=\s*([\d\.]+)"
                match = re.search(pattern, content)
                if match:
                    results[keyword] = match.group(1)
                    if keyword in calculate:
                        calculate[keyword] += float(match.group(1))
                    else:
                        calculate[keyword] = float(match.group(1))
            
            for keyword in results:
                simple_f.write(keyword + ' = ' + results[keyword] + '\n')

        simple_f.write('\n' + 'calculated metrics:\n')
        for keyword, cal_type in zip(keywords, keywords_cal_type):
            if keyword in calculate:
                if cal_type == 'sum':
                    simple_f.write(keyword + '_sum = ' + str(calculate[keyword]) + '\n')
                if cal_type == 'avg':
                    simple_f.write(keyword + '_avg = ' + str(calculate[keyword] / cfgs['NODE_CNT']) + '\n')

        simple_f.write('\n')
        simple_f.close()
        calculate_sets.append(calculate)
        os.chdir(tmp_path)

        # For drawing plots, any experiment need to only have one or two varibles
        # The first varible is always the second place in e and the last iteration of e (write in experiments.py)
        # The code below does not support more than 3 variables
        if e[1] not in variable1:
            variable1.append(e[1])
        if e[2] not in variable2:
            variable2.append(e[2])

    variable1_cnt = len(variable1)

    if variable1_cnt == 0 or len(calculate_sets) / variable1_cnt != len(variable2):
        print('Some experiments are missing, cannot draw plots')
        continue

    tmp_path = os.getcwd()
    os.chdir(experiment_dir)
    for keyword in keywords:
        if keyword not in calculate_sets[0]:
            continue
        plot_data = [calculate_sets[i][keyword] for i in range(len(calculate_sets))]
        i = 0
        plot_f = open(keyword + '_plot.txt', 'a')

        plot_f.write('#\t')
        plot_f.write('\t'.join(map(str, variable1)) + '\n')
        for variable_ in variable2:
            if type(variable_) == int or type(variable_) == float:
                plot_f.write(str(variable_) + '\t')
            else:
                plot_f.write(str(i // variable1_cnt) + '\t')
            for c in range(variable1_cnt):
                plot_f.write(str(plot_data[i]) + '\t')
                i += 1
            plot_f.write('\n')
        plot_f.close()

    os.chdir(tmp_path)
    os.chdir('./scripts')
    cmd = "which gnuplot"
    res = os.system(cmd)
    if res == 0:
        for keyword in draw_keywords:
            if keyword not in calculate_sets[0]:
                continue
            cmd = "gnuplot -c simple_plot.gp {} {} {} {}".format(experiment_dir + keyword + '_plot.txt', (exp + '_' + keyword).upper(), experiment_dir + keyword + '_plot.pdf', ' '.join(map(str, variable1)))
            print(cmd)
            os.system(cmd)
    os.chdir('..')
