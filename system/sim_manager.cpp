/*
	 Copyright 2016 Massachusetts Institute of Technology

	 Licensed under the Apache License, Version 2.0 (the "License");
	 you may not use this file except in compliance with the License.
	 You may obtain a copy of the License at

			 http://www.apache.org/licenses/LICENSE-2.0

	 Unless required by applicable law or agreed to in writing, software
	 distributed under the License is distributed on an "AS IS" BASIS,
	 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	 See the License for the specific language governing permissions and
	 limitations under the License.
*/

#include "global.h"
#include "helper.h"
#include "sim_manager.h"
#include "mem_alloc.h"

void SimManager::init() {
	sim_done = false;
	warmup = false;
	warmup_end_time = 0;
	start_set = false;
	sim_init_done = false;
	txn_cnt = 0;
	inflight_cnt = 0;
	epoch_txn_cnt = 0;
#if CC_ALG == CALVIN && CALVIN_W
	workers_epoch = (uint64_t*)std::malloc(sizeof(uint64_t)*g_sched_thread_cnt);
	for(UInt32 i = 0 ; i < g_sched_thread_cnt ; i++){workers_epoch[i]=1;}
#else
	worker_epoch = 1;
#endif
	seq_epoch = 0;
	rsp_cnt = g_total_node_cnt - 1;
	flushed_batch = 0;
	now_batch = 1;
	now_batch_txn_cnt = 0;
	aria_phase = ARIA_INIT;
	batch_process_count = 0;
	barrier_count = 0;
	barriers = (bool *) mem_allocator.alloc(sizeof(bool) * g_node_cnt);
	memset(barriers, 0, sizeof(bool) * g_node_cnt);

#if TIME_ENABLE
	run_starttime = get_sys_clock();
	last_da_query_time = get_sys_clock();
#else
	run_starttime = get_wall_clock();
#endif
	last_worker_epoch_time = run_starttime;
	last_seq_epoch_time = get_wall_clock();
}

void SimManager::set_starttime(uint64_t starttime) {
	if(ATOM_CAS(start_set, false, true)) {
		run_starttime = starttime;
		last_da_query_time = starttime;
		last_worker_epoch_time = starttime;
		sim_done = false;
		printf("Starttime set to %ld\n",run_starttime);
	} 
}
bool SimManager::timeout() {
#if TIME_ENABLE
	#if WORKLOAD == DA
		uint64_t t=last_da_query_time;
		uint64_t now=get_sys_clock();
		if(now<t)
		{
			now=t;
		}
		bool res =  ((get_sys_clock() - run_starttime) >= (g_done_timer + g_warmup_timer)/12)
		&&((now - t) >= (g_done_timer + g_warmup_timer)/6);
		if (res) {
			printf("123\n");
		}
		return res;
	#else
	return (get_sys_clock() - run_starttime) >= g_done_timer + g_warmup_timer;
	#endif
#else
	return (get_wall_clock() - run_starttime) >= g_done_timer + g_warmup_timer;
#endif
}

bool SimManager::is_done() {
	bool done = sim_done || timeout();
	if(done && !sim_done) {
		set_done();
	}
	return done;
}

bool SimManager::is_warmup_done() {
	#if WORKLOAD == DA
		return true;
	#endif
	if(warmup)
		return true;
	bool done = ((get_sys_clock() - run_starttime) >= g_warmup_timer);
	if(done) {
		ATOM_CAS(warmup_end_time,0,get_sys_clock());
		ATOM_CAS(warmup,false,true);
	}
	return done;
}
bool SimManager::is_setup_done() { return sim_init_done; }

void SimManager::set_setup_done() { ATOM_CAS(sim_init_done, false, true); }

void SimManager::set_done() {
	if(ATOM_CAS(sim_done, false, true)) {
		if (warmup_end_time == 0) warmup_end_time = run_starttime;
		SET_STATS(0, total_runtime, get_sys_clock() - warmup_end_time); 
	}
	printf("set done\n");
	fflush(stdout);
}

void SimManager::process_setup_msg() {
	uint64_t rsp_left = ATOM_SUB_FETCH(rsp_cnt,1);
	if(rsp_left == 0) {
		set_setup_done();
	}
}

void SimManager::inc_txn_cnt() { ATOM_ADD(txn_cnt, 1); }

void SimManager::inc_inflight_cnt() { ATOM_ADD(inflight_cnt, 1); }

void SimManager::dec_inflight_cnt() { ATOM_SUB(inflight_cnt, 1); }

void SimManager::inc_epoch_txn_cnt() { ATOM_ADD(epoch_txn_cnt, 1); }

void SimManager::decr_epoch_txn_cnt() { ATOM_SUB(epoch_txn_cnt, 1); }

uint64_t SimManager::get_seq_epoch() { return seq_epoch; }

void SimManager::advance_seq_epoch() {
	ATOM_ADD(seq_epoch,1);
	last_seq_epoch_time += g_seq_batch_time_limit;
}

#if CC_ALG != CALVIN || (CC_ALG == CALVIN && !CALVIN_W)
uint64_t SimManager::get_worker_epoch() { return worker_epoch; }

void SimManager::next_worker_epoch() {
	last_worker_epoch_time = get_sys_clock();
	ATOM_ADD(worker_epoch,1);
}
#else
uint64_t SimManager::get_worker_epoch(uint32_t locker_id) { return workers_epoch[locker_id]; }

void SimManager::next_worker_epoch(uint32_t locker_id) {
	last_worker_epoch_time = get_sys_clock();
	ATOM_ADD(workers_epoch[locker_id],1);
}	
#endif

double SimManager::seconds_from_start(uint64_t time) {
	return (double)(time - run_starttime) / BILLION;
}

void SimManager::next_aria_phase() {
	aria_phase = (ARIA_PHASE)((aria_phase + 1) % 5);
}
