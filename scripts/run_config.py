#!/usr/bin/python

#Configuration file for run_experiments.py

username = "wyl"
deploy_location = 'home/wyl/test'

# 1/3 of the machines are used for server, 1/3 for client, 1/3 for storage
# Change run_experiments.py if you want to use different number of machines
vcloud_machines = [
#Server
"10.77.110.147",
"10.77.110.148",
#Client
"10.77.110.147",
"10.77.110.148",
#Storage
"10.77.110.147",
"10.77.110.148"
]
