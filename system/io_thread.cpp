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
#include "manager.h"
#include "thread.h"
#include "io_thread.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "message.h"
#include "client_txn.h"
#include "work_queue.h"
#include "txn.h"
#include "ycsb.h"

void init_log_record_by_msg(LogRecord * rec,LogCloudTxnMessage * cmsg)
{
	AriesLogRecord & rcd = rec->rcd;
	rcd.iud = L_CLOUD_TXN;
	rcd.txn_id = cmsg->txn_id;
	rcd.image_size = 1;
	memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->txn_id),sizeof(cmsg->txn_id));
	rcd.image_size += sizeof(cmsg->txn_id);
	memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->array_size2),sizeof(uint64_t));
	rcd.image_size += sizeof(uint64_t);
	if(WORKLOAD == YCSB)
	{
		for(uint64_t i = 0 ; i < cmsg->requests.get_count();i++)
		{
			ycsb_request*req = cmsg->requests.get(i);
			memcpy((rcd.before_and_after_image)+rcd.image_size,req,sizeof(ycsb_request));
			rcd.image_size += sizeof(ycsb_request);
		}
	}
	else if(WORKLOAD == TPCC)
	{
		assert(cmsg);
		memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->array_size1),sizeof(uint64_t));
		rcd.image_size += sizeof(uint64_t);
		memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->txn_type),sizeof(uint64_t));
		rcd.image_size += sizeof(uint64_t);
		memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->w_id),sizeof(uint64_t));
		rcd.image_size += sizeof(uint64_t);
		memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->d_id),sizeof(uint64_t));
		rcd.image_size += sizeof(uint64_t);
		switch (cmsg->txn_type)
		{
		case TPCC_PAYMENT:
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->c_id),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->d_w_id),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->c_w_id),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->c_d_id),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->c_last),LASTNAME_LEN);
			rcd.image_size += LASTNAME_LEN;
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->h_amount),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->by_last_name),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			break;
		case TPCC_NEW_ORDER:
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->c_id),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			for(uint64_t i = 0 ; i < cmsg->items.get_count();i++)
			{
				Item_no*item = cmsg->items.get(i);
				memcpy((rcd.before_and_after_image)+rcd.image_size,item,sizeof(Item_no));
				rcd.image_size += sizeof(Item_no);
			}
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->rbk),sizeof(bool));
			rcd.image_size += sizeof(bool);
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->remote),sizeof(bool));
			rcd.image_size += sizeof(bool);
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->ol_cnt),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->o_entry_d),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			break;
		case TPCC_ORDER_STATUS:
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->o_id),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			break;
		case TPCC_DELIVERY:
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->o_id),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->o_carrier_id),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->ol_delivery_d),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			break;
		case TPCC_STOCK_LEVEL:
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->o_id),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			memcpy((rcd.before_and_after_image)+rcd.image_size,&(cmsg->threshold),sizeof(uint64_t));
			rcd.image_size += sizeof(uint64_t);
			break;
		default:
			assert(false);
			break;
		}
	}
	else
	{
		assert(false);
	}
	
}

void InputThread::setup() {

	std::vector<Message*> * msgs;
	size_t cloud_log_record_size = 0;
	if(WORKLOAD == YCSB)
	{
		// cloud_log_record_size = sizeof(ycsb_request) * g_req_per_query;
		cloud_log_record_size = 240;
	}
	else if (WORKLOAD == TPCC)
	{
		// cloud_log_record_size = sizeof(uint64_t) * 14 + sizeof(bool) * 3 + sizeof(Item_no) *  g_max_items_per_txn + LASTNAME_LEN * sizeof(char);
		cloud_log_record_size = 491;
	}
	assert(cloud_log_record_size != 0);
	fflush(stdout);
	while(!simulation->is_setup_done()) {
		msgs = tport_man.recv_msg(get_thd_id());
		if (msgs == NULL) continue;
		while(!msgs->empty()) {
			Message * msg = msgs->front();
			if(msg->rtype == INIT_DONE) {
				printf("Received INIT_DONE from node %ld\n",msg->return_node_id);
				fflush(stdout);
				simulation->process_setup_msg();
			} else {
				if (ISSERVER || ISREPLICA) {
					//printf("Received Msg %d from node %ld\n",msg->rtype,msg->return_node_id);
#if CC_ALG == CALVIN || CC_ALG == HDCC || CC_ALG == SNAPPER
					if(msg->rtype == CALVIN_ACK ||(msg->rtype == CL_QRY && ISCLIENTN(msg->get_return_id())) ||
						(msg->rtype == CL_QRY_O && ISCLIENTN(msg->get_return_id()))) {
						work_queue.sequencer_enqueue(get_thd_id(),msg);
						msgs->erase(msgs->begin());
						continue;
					}
					if(msg->rtype == CLOUD_LOG_TXN_ACK)
					{
						work_queue.sequencer_enqueue(get_thd_id(),msg);
						msgs->erase(msgs->begin());
						continue;
					}
					if( msg->rtype == RDONE || msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
						assert(ISSERVERN(msg->get_return_id()));
						work_queue.sched_enqueue(get_thd_id(),msg);
						msgs->erase(msgs->begin());
						continue;
					}
#endif
					work_queue.enqueue(get_thd_id(),msg,false);
				} else {					
					if (msg->rtype == LOG_MSG) {
						LogMessage * log_msg = (LogMessage*)msg;
						uint64_t record_cnt = log_msg->record_cnt;
						for (uint64_t i = 0; i < record_cnt; i++) {
							LogRecord * record = (LogRecord*)mem_allocator.alloc(sizeof(LogRecord) + log_msg->records[i]->rcd.image_size * 2 - 1);
							memcpy(record, log_msg->records[i], sizeof(LogRecord) + log_msg->records[i]->rcd.image_size * 2 - 1);
							replay.replay_enqueue(_thd_id, record);
							logger.enqueueRecord(log_msg->records[i]);
						}
						msg->release();
						delete msg;
					}else if(msg->rtype == CLOUD_LOG_TXN){
						LogRecord * record = (LogRecord*)mem_allocator.alloc(sizeof(LogRecord) + cloud_log_record_size + 2 * sizeof(uint64_t));
						LogCloudTxnMessage * cmsg = (LogCloudTxnMessage*)msg;
						init_log_record_by_msg(record,cmsg);
						logger.enqueueRecord(record);
						delete msg;
					}else if(msg->rtype == RSTO) {
						replay.request_enqueue(_thd_id, msg);
					}else {
						assert(false);
					}
				}
			}
			msgs->erase(msgs->begin());
		}
		delete msgs;
	}
	// if (!ISCLIENT) {
	// 	txn_man = (YCSBTxnManager *)
	// 		mem_allocator.align_alloc( sizeof(YCSBTxnManager));
	// 	new(txn_man) YCSBTxnManager();
	// 	// txn_man = (TxnManager*) malloc(sizeof(TxnManager));
	// 	uint64_t thd_id = get_thd_id();
	// 	txn_man->init(thd_id, NULL);
	// }
}

RC InputThread::run() {
	tsetup();
	printf("Running InputThread %ld\n",_thd_id);

	if(ISCLIENT) {
		client_recv_loop();
	} else if (ISSTORAGE) {
		storage_recv_loop();
	} else {
		server_recv_loop();
	}

	return FINISH;

}

RC InputThread::client_recv_loop() {
	int rsp_cnts[g_servers_per_client];
	memset(rsp_cnts, 0, g_servers_per_client * sizeof(int));

	run_starttime = get_sys_clock();
	uint64_t return_node_offset;
	uint64_t inf;

	std::vector<Message*> * msgs;

	while (!simulation->is_done()) {
		heartbeat();
		uint64_t starttime = get_sys_clock();
		msgs = tport_man.recv_msg(get_thd_id());
		INC_STATS(_thd_id,mtx[28], get_sys_clock() - starttime);
		starttime = get_sys_clock();
		//while((m_query = work_queue.get_next_query(get_thd_id())) != NULL) {
		//Message * msg = work_queue.dequeue();
		if (msgs == NULL) continue;
		while(!msgs->empty()) {
			Message * msg = msgs->front();
			assert(msg->rtype == CL_RSP);
		#if CC_ALG == BOCC || CC_ALG == FOCC || ONE_NODE_RECIEVE == 1
			return_node_offset = msg->return_node_id;
		#else
		#if SINGLE_WRITE_NODE
			// HACK! We assumed the number of servers and clients are the same here
			assert(g_servers_per_client == 1);
			return_node_offset = 0;
		#else
			return_node_offset = msg->return_node_id - g_server_start_node;
		#endif
		#endif
			assert(return_node_offset < g_servers_per_client);
			rsp_cnts[return_node_offset]++;
			INC_STATS(get_thd_id(),txn_cnt,1);
			uint64_t timespan = get_sys_clock() - ((ClientResponseMessage*)msg)->client_startts;
			INC_STATS(get_thd_id(),txn_run_time, timespan);
			if (warmup_done) {
				INC_STATS_ARR(get_thd_id(),client_client_latency, timespan);
			}
			//INC_STATS_ARR(get_thd_id(),all_lat,timespan);
			inf = client_man.dec_inflight(return_node_offset);
			DEBUG("Recv %ld from %ld, %ld -- %f\n", ((ClientResponseMessage *)msg)->txn_id,
						msg->return_node_id, inf, float(timespan) / BILLION);
			assert(inf >=0);
			// delete message here
			msgs->erase(msgs->begin());
		}
		delete msgs;
		INC_STATS(_thd_id,mtx[29], get_sys_clock() - starttime);

	}

	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}


bool InputThread::fakeprocess(Message * msg) {
	RC rc __attribute__ ((unused));
	bool eq = false;

	txn_man->set_txn_id(msg->get_txn_id());
	// txn_table.get_transaction_manager(get_thd_id(),msg->get_txn_id(),0);
	// txn_man->txn_stats.clear_short();
	// if (CC_ALG != CALVIN) {
	//   txn_man->txn_stats.lat_network_time_start = msg->lat_network_time;
	//   txn_man->txn_stats.lat_other_time_start = msg->lat_other_time;
	// }
	// txn_man->txn_stats.msg_queue_time += msg->mq_time;
	// txn_man->txn_stats.msg_queue_time_short += msg->mq_time;
	// msg->mq_time = 0;
	// txn_man->txn_stats.work_queue_time += msg->wq_time;
	// txn_man->txn_stats.work_queue_time_short += msg->wq_time;
	// //txn_man->txn_stats.network_time += msg->ntwk_time;
	// msg->wq_time = 0;
	// txn_man->txn_stats.work_queue_cnt += 1;

	// DEBUG("%ld Processing %ld %d\n",get_thd_id(),msg->get_txn_id(),msg->get_rtype());
	// assert(msg->get_rtype() == CL_QRY || msg->get_txn_id() != UINT64_MAX);
	// uint64_t starttime = get_sys_clock();
		switch(msg->get_rtype()) {
			case RPREPARE:
				rc = RCOK;
				txn_man->set_rc(rc);
				msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RACK_PREP),msg->return_node_id);
				break;
			case RQRY:
				rc = RCOK;
				txn_man->set_rc(rc);
				msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RQRY_RSP),msg->return_node_id);
				break;
			case RQRY_CONT:
				rc = RCOK;
				txn_man->set_rc(rc);
				msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RQRY_RSP),msg->return_node_id);
				break;
			case RFIN:
				rc = RCOK;
				txn_man->set_rc(rc);
				if(!((FinishMessage*)msg)->readonly || CC_ALG == MAAT || CC_ALG == OCC || CC_ALG == TICTOC || CC_ALG == BOCC || CC_ALG == SSI)
				// if(!((FinishMessage*)msg)->readonly || CC_ALG == MAAT || CC_ALG == OCC)
					msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RACK_FIN),GET_NODE_ID(msg->get_txn_id()));
				// rc = process_rfin(msg);
				break;
			default:
				eq = true;
				break;
		}
		return eq;
	// uint64_t timespan = get_sys_clock() - starttime;
	// INC_STATS(get_thd_id(),worker_process_cnt,1);
	// INC_STATS(get_thd_id(),worker_process_time,timespan);
	// INC_STATS(get_thd_id(),worker_process_cnt_by_type[msg->rtype],1);
	// INC_STATS(get_thd_id(),worker_process_time_by_type[msg->rtype],timespan);
	// DEBUG("%ld EndProcessing %d %ld\n",get_thd_id(),msg->get_rtype(),msg->get_txn_id());
}

RC InputThread::server_recv_loop() {

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	assert (rc == RCOK);
	uint64_t starttime;

	std::vector<Message*> * msgs;
	while (!simulation->is_done()) {
		heartbeat();
		starttime = get_sys_clock();

		msgs = tport_man.recv_msg(get_thd_id());

		INC_STATS(_thd_id,mtx[28], get_sys_clock() - starttime);
		starttime = get_sys_clock();

		if (msgs == NULL) continue;
		while(!msgs->empty()) {
			Message * msg = msgs->front();
			if(msg->rtype == INIT_DONE) {
				msgs->erase(msgs->begin());
				continue;
			}
#if CC_ALG == CALVIN || CC_ALG==HDCC || CC_ALG == SNAPPER
			if(msg->rtype==CONF_STAT){
				assert(CC_ALG==HDCC);
				g_conflict_queue.push((ConflictStaticsMessage*)msg);
				msgs->erase(msgs->begin());
				continue;
			}
			if(msg->rtype == CALVIN_ACK ||(msg->rtype == CL_QRY && ISCLIENTN(msg->get_return_id())) ||
			(msg->rtype == CL_QRY_O && ISCLIENTN(msg->get_return_id()))) {
				work_queue.sequencer_enqueue(get_thd_id(),msg);
				msgs->erase(msgs->begin());
				continue;
			}
			if( msg->rtype == CLOUD_LOG_TXN_ACK )
			{
				work_queue.sequencer_enqueue(get_thd_id(),msg);
				msgs->erase(msgs->begin());
				continue;
			}
			if( msg->rtype == RDONE || msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
				assert(ISSERVERN(msg->get_return_id()));
				work_queue.sched_enqueue(get_thd_id(),msg);
				msgs->erase(msgs->begin());
				continue;
			}
#endif
			if (msg->rtype == RPDONE) {
				simulation->flushed_batch = ((DoneMessage *)msg)->batch_id;
				msgs->erase(msgs->begin());
				continue;
			}
#ifdef FAKE_PROCESS
			if (fakeprocess(msg))
#endif
				work_queue.enqueue(get_thd_id(),msg,false);
			msgs->erase(msgs->begin());
		}
		delete msgs;
		INC_STATS(_thd_id,mtx[29], get_sys_clock() - starttime);

	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}

RC InputThread::storage_recv_loop() {
	std::vector<Message*> * msgs;
	size_t cloud_log_record_size = 0;
	if(WORKLOAD == YCSB)
	{
		// cloud_log_record_size = sizeof(ycsb_request) * g_req_per_query;
		cloud_log_record_size = 240;
	}
	else if (WORKLOAD == TPCC)
	{
		// cloud_log_record_size = sizeof(uint64_t) * 14 + sizeof(bool) * 3 + sizeof(Item_no) *  g_max_items_per_txn + LASTNAME_LEN * sizeof(char);
		cloud_log_record_size = 491;
	}
	assert(cloud_log_record_size != 0);
	while (!simulation->is_done()) {
		heartbeat();
		msgs = tport_man.recv_msg(get_thd_id());
		
		if (msgs == NULL) continue;
		while(!msgs->empty()) {
			Message * msg = msgs->front();
			if (msg->rtype == INIT_DONE) {
					msgs->erase(msgs->begin());
					continue;
			} else if (msg->rtype == LOG_MSG) {
				LogMessage * log_msg = (LogMessage*)msg;
				uint64_t record_cnt = log_msg->record_cnt;
				for (uint64_t i = 0; i < record_cnt; i++) {
					LogRecord * record = (LogRecord*)mem_allocator.alloc(sizeof(LogRecord) + log_msg->records[i]->rcd.image_size * 2 - 1);
					memcpy(record, log_msg->records[i], sizeof(LogRecord) + log_msg->records[i]->rcd.image_size * 2 - 1);
					replay.replay_enqueue(_thd_id, record);
					logger.enqueueRecord(log_msg->records[i]);
				}
				msg->release();
				delete msg;
			}else if (msg->rtype == RSTO) {
				replay.request_enqueue(_thd_id, msg);
			}else if(msg->rtype == CLOUD_LOG_TXN) {
				LogRecord * record = (LogRecord*)mem_allocator.alloc(sizeof(LogRecord) + cloud_log_record_size + 2 * sizeof(uint64_t));
				LogCloudTxnMessage * cmsg = (LogCloudTxnMessage*)msg;
				init_log_record_by_msg(record,cmsg);
				logger.enqueueRecord(record);
				delete msg;
			}else {
					assert(false);
			}
			msgs->erase(msgs->begin());
		}
		delete msgs;
	}
	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}

void OutputThread::setup() {
	DEBUG_M("OutputThread::setup MessageThread alloc\n");
	messager = (MessageThread *) mem_allocator.alloc(sizeof(MessageThread));
	messager->init(_thd_id);
	while (!simulation->is_setup_done()) {
		messager->run();
	}
}

RC OutputThread::run() {

	tsetup();
	printf("Running OutputThread %ld\n",_thd_id);

	while (!simulation->is_done()) {
		heartbeat();
		messager->run();
	}

	printf("FINISH %ld:%ld\n",_node_id,_thd_id);
	fflush(stdout);
	return FINISH;
}


