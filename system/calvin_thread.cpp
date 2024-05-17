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
#include "manager.h"
#include "thread.h"
#include "calvin_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "sequencer.h"
#include "logger.h"
#include "message.h"
#include "work_queue.h"
#if CC_ALG == SNAPPER
#include <unordered_map>
#include <utility>
#include <vector>
#include "row.h"
#endif
#if CC_ALG == CALVIN || CC_ALG == CALVIN_W
#include <map> //used for sequencer thread temporary store txn msg
#endif

void CalvinLockThread::setup() {}

#if CC_ALG != SNAPPER
RC CalvinLockThread::run() {
	tsetup();

	RC rc = RCOK;
	TxnManager * txn_man;
	uint64_t prof_starttime = get_sys_clock();
	uint64_t idle_starttime = 0;

	while(!simulation->is_done()) {
		txn_man = NULL;

		Message * msg = work_queue.sched_dequeue(_thd_id);

		if(!msg) {
			if (idle_starttime == 0) idle_starttime = get_sys_clock();
			continue;
		}
		if(idle_starttime > 0) {
				INC_STATS(_thd_id,sched_idle_time,get_sys_clock() - idle_starttime);
				idle_starttime = 0;
		}

		prof_starttime = get_sys_clock();
		assert(msg->get_txn_id() != UINT64_MAX);
		txn_man =
				txn_table.get_transaction_manager(get_thd_id(), msg->get_txn_id(), msg->get_batch_id());
#if CC_ALG != HDCC
		assert(msg->get_rtype() == CL_QRY || msg->get_rtype() == CL_QRY_O);
		
#else
		txn_man->algo = msg->algo;
#endif
		while (!txn_man->unset_ready()) {
		}
#if CC_ALG == CALVIN_W
		if(txn_man->worker_has_dealed)
		{
			txn_table.release_transaction_manager(get_thd_id(), txn_man->get_txn_id(),
                                        txn_man->get_batch_id());
			continue;
		}
#endif
		assert(ISSERVERN(msg->get_return_id()));
		txn_man->txn_stats.starttime = get_sys_clock();

		txn_man->txn_stats.lat_network_time_start = msg->lat_network_time;
		txn_man->txn_stats.lat_other_time_start = msg->lat_other_time;
#if CC_ALG == CALVIN_W
		if(ATOM_CAS(txn_man->has_ready,false,true))
		{
			msg->copy_to_txn(txn_man);
		}
#else
		msg->copy_to_txn(txn_man);
#endif
		txn_man->register_thread(this);
		assert(ISSERVERN(txn_man->return_id));

		INC_STATS(get_thd_id(),sched_txn_table_time,get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();

		rc = RCOK;
		// Acquire locks
		if (!txn_man->isRecon()) {	
				//判断条件没有实际意义
				rc = txn_man->acquire_locks();
		}

		if(rc == RCOK) {
				// WhiteBear: 要避免多线程加锁一个事务不进队列或者进多次队列
				work_queue.enqueue(_thd_id,msg,false);	
		}
		txn_man->set_ready();	
		//原本的作用是避免多个线程操作同一个事务

		INC_STATS(_thd_id,mtx[33],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();
	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}
#else
RC CalvinLockThread::run() {
	tsetup();

	RC rc = RCOK;
	TxnManager * txn_man;
	uint64_t prof_starttime = get_sys_clock();
	uint64_t idle_starttime = 0;
	uint64_t last_batch_id = 0;
	unordered_map<row_t *, vector<pair<TxnManager *, access_t>>> read_write_sets;

	while(!simulation->is_done()) {
		txn_man = NULL;

		Message * msg = work_queue.sched_dequeue(_thd_id);

		if(!msg) {
			if (idle_starttime == 0) idle_starttime = get_sys_clock();
			continue;
		}
		if(idle_starttime > 0) {
				INC_STATS(_thd_id,sched_idle_time,get_sys_clock() - idle_starttime);
				idle_starttime = 0;
		}

		prof_starttime = get_sys_clock();
		assert(msg->get_txn_id() != UINT64_MAX || msg->rtype == RDONE);

		if(msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
			txn_man =
					txn_table.get_transaction_manager(get_thd_id(), msg->get_txn_id(), msg->get_batch_id());
			txn_man->algo = msg->algo;

			while (!txn_man->unset_ready()) {
			}
		#if CC_ALG == SNAPPER
			txn_man->txn->timestamp = UINT64_MAX;
		#endif
			assert(ISSERVERN(msg->get_return_id()));
			txn_man->txn_stats.starttime = get_sys_clock();

			txn_man->txn_stats.lat_network_time_start = msg->lat_network_time;
			txn_man->txn_stats.lat_other_time_start = msg->lat_other_time;

			msg->copy_to_txn(txn_man);
			txn_man->register_thread(this);
			assert(ISSERVERN(txn_man->return_id));

			INC_STATS(get_thd_id(),sched_txn_table_time,get_sys_clock() - prof_starttime);
			prof_starttime = get_sys_clock();

			rc = RCOK;

			//get all rows first
			txn_man->get_read_write_set();

			for (auto i = txn_man->read_write_set.begin(); i != txn_man->read_write_set.end(); i++) {
				auto record = read_write_sets.find(i->first);
				if(record == read_write_sets.end()) {
					read_write_sets.emplace(i->first, vector<pair<TxnManager *, access_t>>(1, pair<TxnManager *, access_t>(txn_man, i->second)));
				} else {
					record->second.push_back(pair<TxnManager *, access_t>(txn_man, i->second));
				}
			}

			txn_man->set_ready();
		} else {
			msg->release();
			msg = NULL;
			for (auto i = read_write_sets.begin(); i != read_write_sets.end(); i++) {
				row_t * row = i->first;
				auto vector = i->second;
				row->enter_critical_section();
				for (auto j = vector.begin(); j != vector.end(); j++) {
					TxnManager * txn = j->first;
					RC rc = txn->acquire_lock(row, j->second);
					if (rc == RCOK) {
						Message* msg = Message::create_message(txn,RTXN);
        				msg->algo = CALVIN;
						work_queue.enqueue(_thd_id,msg,false);
						INC_STATS(_thd_id,mtx[33],get_sys_clock() - prof_starttime);
						prof_starttime = get_sys_clock();
					}
				}
				row->leave_critical_section();
				vector.clear();
			}
			read_write_sets.clear();
		}
	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}
#endif

void CalvinSequencerThread::setup() {}

bool CalvinSequencerThread::is_batch_ready() {
	bool ready = get_wall_clock() - simulation->last_seq_epoch_time >= g_seq_batch_time_limit;
	return ready;
}

RC CalvinSequencerThread::run() {
	tsetup();

	Message * msg;
	uint64_t idle_starttime = 0;
	uint64_t prof_starttime = 0;

	uint32_t cloud_log_id = 0 ;
	map<uint32_t,Message*> map_cloudlogid2msg;
	map<uint32_t,uint32_t> map_cloudlogid2counter;

	while(!simulation->is_done()) {

		prof_starttime = get_sys_clock();

		if(is_batch_ready()) {
			simulation->advance_seq_epoch();
			//last_batchtime = get_wall_clock();
			seq_man.send_next_batch(_thd_id);
		}

		INC_STATS(_thd_id,mtx[30],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();

		msg = work_queue.sequencer_dequeue(_thd_id);

		INC_STATS(_thd_id,mtx[31],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();

		if(!msg) {
			if (idle_starttime == 0) idle_starttime = get_sys_clock();
				continue;
		}
		if(idle_starttime > 0) {
			INC_STATS(_thd_id,seq_idle_time,get_sys_clock() - idle_starttime);
			idle_starttime = 0;
		}
		auto rtype = msg->get_rtype();
		if(rtype == CL_QRY || rtype == CL_QRY_O)
		{
			Message * log_txn_msg = Message::create_message(CLOUD_LOG_TXN);
			LogCloudTxnMessage * log_msg = (LogCloudTxnMessage*)(log_txn_msg);
			uint64_t temp_txn_id= cloud_log_id * g_node_cnt + g_node_id;
			cloud_log_id++;
			log_msg->txn_id = temp_txn_id;
			log_msg->copy_from_msg(msg);
			for (uint64_t j = 0; j < g_storage_log_node_cnt; j++) { //这个for循环是向每个storage发消息
				Message * msg_copy = Message::create_message(CLOUD_LOG_TXN);
				LogCloudTxnMessage*log_msg_copy=(LogCloudTxnMessage*)msg_copy;
				memcpy(log_msg_copy,log_msg,sizeof(LogCloudTxnMessage));
				msg_queue.enqueue(get_thd_id(), msg_copy, g_node_cnt + g_client_node_cnt + j);
			}
			log_txn_msg->release();
			map_cloudlogid2counter[temp_txn_id] = 0;
			map_cloudlogid2msg[temp_txn_id] = msg;
		}else if(msg->get_rtype() == CLOUD_LOG_TXN_ACK)
		{
			LogFlushedMessage * log_flush_msg = (LogFlushedMessage*)(msg);
			map_cloudlogid2counter[log_flush_msg->txn_id]++;
			if(map_cloudlogid2counter[log_flush_msg->txn_id] == g_storage_log_node_cnt)
			{
				Message * client_msg = map_cloudlogid2msg[log_flush_msg->txn_id];
				assert(client_msg);
				seq_man.process_txn(client_msg,get_thd_id(),0,0,0,0);
				map_cloudlogid2counter.erase(log_flush_msg->txn_id);
				map_cloudlogid2msg.erase(log_flush_msg->txn_id);
			}
			msg->release();			
		}else{
			switch (rtype) {
				case CALVIN_ACK:
					// Ack from server
					DEBUG("SEQ process_ack (%ld,%ld) from %ld\n", msg->get_txn_id(), msg->get_batch_id(),
								msg->get_return_id());
					seq_man.process_ack(msg,get_thd_id());
					// Free message here
					msg->release();
					break;
				case CALVIN_ABORT:
					seq_man.process_abort(msg, get_thd_id());
					// Don't free message yet
					break;
				default:
					assert(false);
			}
		}
		INC_STATS(_thd_id,mtx[32],get_sys_clock() - prof_starttime);
		prof_starttime = get_sys_clock();
	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;

}
