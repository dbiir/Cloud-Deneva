#!/usr/bin/python

#Configuration file for run_experiments.py

username = "hyh"
deploy_location = 'home/hyh/test'

# 1/3 of the machines are used for server, 1/3 for client, 1/3 for storage
# Change run_experiments.py if you want to use different number of machines
vcloud_machines = [
#Server
"10.77.110.147",
"10.77.110.148",
"10.77.110.144",
"10.77.110.145",
"10.77.110.146",
#Client
"10.77.110.147",
"10.77.110.148",
"10.77.110.144",
"10.77.110.145",
"10.77.110.146",
#Storage
"10.77.110.147",
"10.77.110.148",
"10.77.110.144",
"10.77.110.145",
"10.77.110.146",
]
