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
#include "thread.h"
#include "client_thread.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "client_query.h"
#include "transport.h"
#include "client_txn.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "wl.h"
#include "message.h"
#include <random>

void ClientThread::setup() {
	if( _thd_id == 0) {
		send_init_done_to_all_nodes();
	}
#if LOAD_METHOD == LOAD_RATE
	assert(g_load_per_server > 0);
	// send ~twice as frequently due to delays in context switching
	send_interval = (g_client_thread_cnt * BILLION) / g_load_per_server / 1.8;
	printf("Client interval: %ld\n",send_interval);
#endif

}

RC ClientThread::run() {

	tsetup();
	printf("Running ClientThread %ld\n",_thd_id);
	BaseQuery * m_query;
	uint64_t iters = 0;
	uint32_t num_txns_sent = 0;
	int txns_sent[g_servers_per_client];
#if WORKLOAD == TPCC
	vector<uint64_t> order_ids;
	for (uint64_t i = 0; i < g_num_wh; i++) {
		for (uint64_t j = 0; j < g_dist_per_wh; j++) {
#if SINGLE_WRITE_NODE
			order_ids.push_back(g_client_thread_cnt * (g_node_id - g_node_cnt) + _thd_id + 2101);
#else
			order_ids.push_back(_thd_id + 2101);
#endif
		}
	}
#endif
	for (uint32_t i = 0; i < g_servers_per_client; ++i) txns_sent[i] = 0;

	run_starttime = get_sys_clock();
	while(!simulation->is_done()) {
		heartbeat();
#if SERVER_GENERATE_QUERIES
		break;
#endif
		//uint32_t next_node = iters++ % g_node_cnt;
		progress_stats();
		int32_t inf_cnt;
	#if CC_ALG == BOCC || CC_ALG == FOCC || ONE_NODE_RECIEVE == 1
		uint32_t next_node = 0;
		uint32_t next_node_id = next_node;
	#else
		uint32_t next_node = (((iters++) * g_client_thread_cnt) + _thd_id )% g_servers_per_client;
		uint32_t next_node_id = next_node + g_server_start_node;
	#endif
		// uint32_t next_node_id = next_node + g_server_start_node;
		// Just in case...
		if (iters == UINT64_MAX)
			iters = 0;
#if LOAD_METHOD == LOAD_MAX
	#if WORKLOAD != DA
		if ((inf_cnt = client_man.inc_inflight(next_node)) < 0)
			continue;
	#endif
		m_query = client_query_queue.get_next_query(next_node,_thd_id);
		if(last_send_time > 0) {
			INC_STATS(get_thd_id(),cl_send_intv,get_sys_clock() - last_send_time);
		}
		last_send_time = get_sys_clock();
		simulation->last_da_query_time = get_sys_clock();
#elif LOAD_METHOD == LOAD_RATE
		if ((inf_cnt = client_man.inc_inflight(next_node)) < 0)
			continue;
		uint64_t gate_time;
		while((gate_time = get_sys_clock()) - last_send_time < send_interval) { }
		if(last_send_time > 0) {
			INC_STATS(get_thd_id(),cl_send_intv,gate_time - last_send_time);
		}
		last_send_time = gate_time;
		m_query = client_query_queue.get_next_query(next_node,_thd_id);
#else
		assert(false);
#endif
		assert(m_query);

		DEBUG("Client: thread %lu sending query to node: %u, %d, %f\n",
				_thd_id, next_node_id,inf_cnt,simulation->seconds_from_start(get_sys_clock()));
#if WORKLOAD == TPCC
		TPCCQuery * query = (TPCCQuery *) m_query;
		if (query->txn_type == TPCC_DELIVERY) {
			query->o_id = order_ids.at((query->w_id - 1) * g_dist_per_wh + (query->d_id - 1));
#if SINGLE_WRITE_NODE
			order_ids[(query->w_id - 1) * g_dist_per_wh + (query->d_id - 1)] += g_client_thread_cnt * g_client_node_cnt;
#else
			order_ids[(query->w_id - 1) * g_dist_per_wh + (query->d_id - 1)] += g_client_thread_cnt;
#endif
		}
#endif
#if ONE_NODE_RECIEVE == 1 && defined(NO_REMOTE) && LESS_DIS_NUM == 10
		Message * msg = Message::create_message((BaseQuery*)m_query,CL_QRY_O);
#else
		Message * msg = Message::create_message((BaseQuery*)m_query,CL_QRY);
#endif
		((ClientQueryMessage*)msg)->client_startts = get_sys_clock();
#if SINGLE_WRITE_NODE
		if (m_query->readonly() && g_node_cnt != 1) {
			uint64_t random_node;
			if (readonly_perc() <= (g_node_cnt - 1.0) / g_node_cnt) {
				random_node = rand() % (g_node_cnt - 1) + 1;
			} else {
				double r = (double)(rand() % 10000) / 10000;
				if (r < (readonly_perc() - (g_node_cnt - 1.0) / g_node_cnt) / (g_node_cnt - 1.0) / g_node_cnt) {
					random_node = 0;
				} else {
					random_node = rand() % (g_node_cnt - 1) + 1;
				}
			}
			msg_queue.enqueue(get_thd_id(),msg,random_node);
		} else {
			msg_queue.enqueue(get_thd_id(),msg,0);
		}
#else
		msg_queue.enqueue(get_thd_id(),msg,next_node_id);
#endif
		num_txns_sent++;
		txns_sent[next_node]++;
		INC_STATS(get_thd_id(),txn_sent_cnt,1);
		#if WORKLOAD==DA
			delete m_query;
		#endif
	}

	for (uint64_t l = 0; l < g_servers_per_client; ++l)
		printf("Txns sent to node %lu: %d\n", l+g_server_start_node, txns_sent[l]);

	//SET_STATS(get_thd_id(), total_runtime, get_sys_clock() - simulation->run_starttime);

	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}

RC DynamicThread::run() {
	tsetup();
	printf("Running DynamicThread %ld\n",_thd_id);

	default_random_engine generator;
	//	produces pseudo-random number in closed interval [1, g_dy_Nbatch - 1]
	//	so that we can assure that g_dy_batch_id gets a different value in every epoch
	//	for case g_dy_Nbatch == 1, this works fine, but surely, g_dy_batch_id keeps at zero
	uniform_int_distribution<uint32_t> distrib(1, g_dy_Nbatch - 1);
	uint64_t start, end;
	while(!simulation->is_done()) {
		start = get_sys_clock();
		g_dy_batch_id = (g_dy_batch_id + distrib(generator)) % g_dy_Nbatch;
		uint32_t dy_wr_index = g_dy_batch_id % dy_write.size(), dy_skew_index = g_dy_batch_id / dy_write.size();
		printf("write perc: %lf\tskew: %lf\n", dy_write[dy_wr_index], dy_skew[dy_skew_index]);
		fflush(stdout);
		for(end = get_sys_clock();(end - start) < SWITCH_INTERVAL;end = get_sys_clock()){}
	}

	return FINISH;
}

void DynamicThread::setup() {

}