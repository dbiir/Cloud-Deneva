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

#include "helper.h"
#include "txn.h"
#include "row.h"
#include "wl.h"
#include "query.h"
#include "thread.h"
#include "mem_alloc.h"
#include "occ.h"
#include "focc.h"
#include "bocc.h"
#include "row_occ.h"
#include "table.h"
#include "catalog.h"
#include "dli.h"
#include "dta.h"
#include "index_btree.h"
#include "index_hash.h"
#include "maat.h"
#include "manager.h"
#include "mem_alloc.h"
#include "message.h"
#include "msg_queue.h"
#include "occ.h"
#include "pool.h"
#include "message.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "pps_query.h"
#include "array.h"
#include "maat.h"
#include "wkdb.h"
#include "tictoc.h"
#include "ssi.h"
#include "wsi.h"
#include "manager.h"
#include "work_queue.h"
#include "cache_manager.h"
#if CC_ALG == SNAPPER
#include "row_snapper.h"
#endif

void TxnStats::init() {
	starttime=0;
	wait_starttime=get_sys_clock();
	total_process_time=0;
	process_time=0;
	total_local_wait_time=0;
	local_wait_time=0;
	total_remote_wait_time=0;
	remote_wait_time=0;
	total_twopc_time=0;
	twopc_time=0;
	write_cnt = 0;
	abort_cnt = 0;

	 total_work_queue_time = 0;
	 work_queue_time = 0;
	 total_cc_block_time = 0;
	 cc_block_time = 0;
	 total_cc_time = 0;
	 cc_time = 0;
	 total_work_queue_cnt = 0;
	 work_queue_cnt = 0;
	 total_msg_queue_time = 0;
	 msg_queue_time = 0;
	 total_abort_time = 0;

	 clear_short();
}

void TxnStats::clear_short() {

	 work_queue_time_short = 0;
	 cc_block_time_short = 0;
	 cc_time_short = 0;
	 msg_queue_time_short = 0;
	 process_time_short = 0;
	 network_time_short = 0;
}

void TxnStats::reset() {
	wait_starttime=get_sys_clock();
	total_process_time += process_time;
	process_time = 0;
	total_local_wait_time += local_wait_time;
	local_wait_time = 0;
	total_remote_wait_time += remote_wait_time;
	remote_wait_time = 0;
	total_twopc_time += twopc_time;
	twopc_time = 0;
	write_cnt = 0;

	total_work_queue_time += work_queue_time;
	work_queue_time = 0;
	total_cc_block_time += cc_block_time;
	cc_block_time = 0;
	total_cc_time += cc_time;
	cc_time = 0;
	total_work_queue_cnt += work_queue_cnt;
	work_queue_cnt = 0;
	total_msg_queue_time += msg_queue_time;
	msg_queue_time = 0;

	clear_short();

}

void TxnStats::abort_stats(uint64_t thd_id) {
	total_process_time += process_time;
	total_local_wait_time += local_wait_time;
	total_remote_wait_time += remote_wait_time;
	total_twopc_time += twopc_time;
	total_work_queue_time += work_queue_time;
	total_msg_queue_time += msg_queue_time;
	total_cc_block_time += cc_block_time;
	total_cc_time += cc_time;
	total_work_queue_cnt += work_queue_cnt;
	assert(total_process_time >= process_time);

	INC_STATS(thd_id,lat_s_rem_work_queue_time,total_work_queue_time);
	INC_STATS(thd_id,lat_s_rem_msg_queue_time,total_msg_queue_time);
	INC_STATS(thd_id,lat_s_rem_cc_block_time,total_cc_block_time);
	INC_STATS(thd_id,lat_s_rem_cc_time,total_cc_time);
	INC_STATS(thd_id,lat_s_rem_process_time,total_process_time);
}

void TxnStats::commit_stats(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id,
														uint64_t timespan_long, uint64_t timespan_short) {
	total_process_time += process_time;
	total_local_wait_time += local_wait_time;
	total_remote_wait_time += remote_wait_time;
	total_twopc_time += twopc_time;
	total_work_queue_time += work_queue_time;
	total_msg_queue_time += msg_queue_time;
	total_cc_block_time += cc_block_time;
	total_cc_time += cc_time;
	total_work_queue_cnt += work_queue_cnt;
	assert(total_process_time >= process_time);

#if CC_ALG == CALVIN
	INC_STATS(thd_id,lat_s_loc_work_queue_time,work_queue_time);
	INC_STATS(thd_id,lat_s_loc_msg_queue_time,msg_queue_time);
	INC_STATS(thd_id,lat_s_loc_cc_block_time,cc_block_time);
	INC_STATS(thd_id,lat_s_loc_cc_time,cc_time);
	INC_STATS(thd_id,lat_s_loc_process_time,process_time);
	// latency from start of transaction at this node
	PRINT_LATENCY("lat_l %ld %ld %ld %f %f %f %f %f %f\n", txn_id, batch_id, total_work_queue_cnt,
								(double)timespan_long / BILLION, (double)total_work_queue_time / BILLION,
								(double)total_msg_queue_time / BILLION, (double)total_cc_block_time / BILLION,
								(double)total_cc_time / BILLION, (double)total_process_time / BILLION);
#else
	// latency from start of transaction
	if (IS_LOCAL(txn_id)) {
	INC_STATS(thd_id,lat_l_loc_work_queue_time,total_work_queue_time);
	INC_STATS(thd_id,lat_l_loc_msg_queue_time,total_msg_queue_time);
	INC_STATS(thd_id,lat_l_loc_cc_block_time,total_cc_block_time);
	INC_STATS(thd_id,lat_l_loc_cc_time,total_cc_time);
	INC_STATS(thd_id,lat_l_loc_process_time,total_process_time);
	INC_STATS(thd_id,lat_l_loc_abort_time,total_abort_time);

	INC_STATS(thd_id,lat_s_loc_work_queue_time,work_queue_time);
	INC_STATS(thd_id,lat_s_loc_msg_queue_time,msg_queue_time);
	INC_STATS(thd_id,lat_s_loc_cc_block_time,cc_block_time);
	INC_STATS(thd_id,lat_s_loc_cc_time,cc_time);
	INC_STATS(thd_id,lat_s_loc_process_time,process_time);

	INC_STATS(thd_id,lat_short_work_queue_time,work_queue_time_short);
	INC_STATS(thd_id,lat_short_msg_queue_time,msg_queue_time_short);
	INC_STATS(thd_id,lat_short_cc_block_time,cc_block_time_short);
	INC_STATS(thd_id,lat_short_cc_time,cc_time_short);
	INC_STATS(thd_id,lat_short_process_time,process_time_short);
	INC_STATS(thd_id,lat_short_network_time,network_time_short);
	} else {
	INC_STATS(thd_id,lat_l_rem_work_queue_time,total_work_queue_time);
	INC_STATS(thd_id,lat_l_rem_msg_queue_time,total_msg_queue_time);
	INC_STATS(thd_id,lat_l_rem_cc_block_time,total_cc_block_time);
	INC_STATS(thd_id,lat_l_rem_cc_time,total_cc_time);
	INC_STATS(thd_id,lat_l_rem_process_time,total_process_time);
	}
	if (IS_LOCAL(txn_id)) {
		PRINT_LATENCY("lat_s %ld %ld %f %f %f %f %f %f\n", txn_id, work_queue_cnt,
									(double)timespan_short / BILLION, (double)work_queue_time / BILLION,
									(double)msg_queue_time / BILLION, (double)cc_block_time / BILLION,
									(double)cc_time / BILLION, (double)process_time / BILLION);
	 /*
	PRINT_LATENCY("lat_l %ld %ld %ld %f %f %f %f %f %f %f\n"
			, txn_id
			, total_work_queue_cnt
			, abort_cnt
			, (double) timespan_long / BILLION
			, (double) total_work_queue_time / BILLION
			, (double) total_msg_queue_time / BILLION
			, (double) total_cc_block_time / BILLION
			, (double) total_cc_time / BILLION
			, (double) total_process_time / BILLION
			, (double) total_abort_time / BILLION
			);
			*/
	} else {
		PRINT_LATENCY("lat_rs %ld %ld %f %f %f %f %f %f\n", txn_id, work_queue_cnt,
									(double)timespan_short / BILLION, (double)total_work_queue_time / BILLION,
									(double)total_msg_queue_time / BILLION, (double)total_cc_block_time / BILLION,
									(double)total_cc_time / BILLION, (double)total_process_time / BILLION);
	}
	/*
	if (!IS_LOCAL(txn_id) || timespan_short < timespan_long) {
	// latency from most recent start or restart of transaction
	PRINT_LATENCY("lat_s %ld %ld %f %f %f %f %f %f\n"
			, txn_id
			, work_queue_cnt
			, (double) timespan_short / BILLION
			, (double) work_queue_time / BILLION
			, (double) msg_queue_time / BILLION
			, (double) cc_block_time / BILLION
			, (double) cc_time / BILLION
			, (double) process_time / BILLION
			);
	}
	*/
#endif

	if (!IS_LOCAL(txn_id)) {
		return;
	}

	INC_STATS(thd_id,txn_total_process_time,total_process_time);
	INC_STATS(thd_id,txn_process_time,process_time);
	INC_STATS(thd_id,txn_total_local_wait_time,total_local_wait_time);
	INC_STATS(thd_id,txn_local_wait_time,local_wait_time);
	INC_STATS(thd_id,txn_total_remote_wait_time,total_remote_wait_time);
	INC_STATS(thd_id,txn_remote_wait_time,remote_wait_time);
	INC_STATS(thd_id,txn_total_twopc_time,total_twopc_time);
	INC_STATS(thd_id,txn_twopc_time,twopc_time);
	if(write_cnt > 0) {
	INC_STATS(thd_id,txn_write_cnt,1);
	}
	if(abort_cnt > 0) {
	INC_STATS(thd_id,unique_txn_abort_cnt,1);
	}

}


void Transaction::init() {
	timestamp = UINT64_MAX;
	start_timestamp = UINT64_MAX;
	end_timestamp = UINT64_MAX;
	txn_id = UINT64_MAX;
	batch_id = UINT64_MAX;
	DEBUG_M("Transaction::init array insert_rows\n");
	insert_rows.init(g_max_items_per_txn + 10);
	delete_rows.init(g_max_items_per_txn + 10);
	DEBUG_M("Transaction::reset array accesses\n");
	accesses.init(MAX_ROW_PER_TXN);

	reset(0);
}

void Transaction::reset(uint64_t thd_id) {
	release_accesses(thd_id);
	accesses.clear();
	//release_inserts(thd_id);
	insert_rows.clear();
	delete_rows.clear();
	write_cnt = 0;
	row_cnt = 0;
	twopc_state = START;
	rc = RCOK;
}

void Transaction::release_accesses(uint64_t thd_id) {
	for(uint64_t i = 0; i < accesses.size(); i++) {
	access_pool.put(thd_id,accesses[i]);
	}
}

void Transaction::release_inserts(uint64_t thd_id) {
	for(uint64_t i = 0; i < insert_rows.size(); i++) {
		row_t * row = insert_rows[i].first;
		row->free_manager();
		row->free_row();
		mem_allocator.free(row,sizeof(row_t));
		DEBUG_M("Transaction::release insert_rows free\n");
	}
}

void TxnManager::do_insert() {
	for (uint64_t i = 0; i < txn->insert_rows.size(); i++) {
		row_t * row = txn->insert_rows[i].first;
		itemid_t * m_item = (itemid_t *) mem_allocator.alloc(sizeof(itemid_t));
		m_item->type = DT_row;
		m_item->location = txn->insert_rows[i].first;
		m_item->valid = true;
		row_cache.insert_to_tail(this, row, hash_key_or_wh_to_cache(row));
#if CC_ALG == CALVIN
		row->cache_node->dirty_batch = txn->batch_id;
#else
		row->cache_node->dirty_batch = txn->batch_id;
#endif
		txn->insert_rows[i].second->index_insert(row->get_primary_key(), m_item, row->get_part_id());
	}
}

void TxnManager::do_delete() {
	for (uint64_t i = 0; i < txn->delete_rows.size(); i++) {
		row_t * row = txn->delete_rows[i].first;
		itemid_t * m_item;
		txn->delete_rows[i].second->index_read(row->get_primary_key(), m_item, row->get_part_id(), 0);
		assert(m_item != NULL);
		// m_item->valid = false;
		row_cache.remove(this, row, hash_key_or_wh_to_cache(row));
		txn->delete_rows[i].second->index_remove(row->get_primary_key(), row->get_part_id());
	}
}

//[ ]: leave it to see if it can run fast under TPCC with few wharehouses.
uint64_t TxnManager::hash_key_or_wh_to_cache(row_t * row) {
#if WORKLOAD == YCSB
	return row->get_primary_key() % g_cache_list_num;
#elif WORKLOAD == TPCC
	if (g_num_wh >= g_cache_list_num) {
		return (row->get_part_id() - 1) % g_cache_list_num;
	} else {
		return row->get_primary_key() % g_cache_list_num;
	}
#endif
}

void Transaction::release(uint64_t thd_id) {
	DEBUG("Transaction release\n");
	release_accesses(thd_id);
	DEBUG_M("Transaction::release array accesses free\n")
	accesses.release();
	release_inserts(thd_id);
	DEBUG_M("Transaction::release array insert_rows free\n")
	insert_rows.release();
	delete_rows.release();
}

void TxnManager::init(uint64_t thd_id, Workload * h_wl) {
#if CC_ALG == CALVIN && CALVIN_W
	lockers_has_watched = (bool*)malloc(sizeof(bool)*g_sched_thread_cnt);
	for(UInt32 i = 0 ; i < g_sched_thread_cnt ; i++)
	{
		lockers_has_watched[i] = false;
	}
	worker_has_dealed = false;
	acquired_lock_num = 0;
	sum_lock_num = 0;
	has_ready = false;
#endif
	cache_not_ready = false;
	uint64_t prof_starttime = get_sys_clock();
	if(!txn)  {
	DEBUG_M("Transaction alloc\n");
	txn_pool.get(thd_id,txn);

	}
	INC_STATS(get_thd_id(),mtx[15],get_sys_clock()-prof_starttime);
	prof_starttime = get_sys_clock();
	//txn->init();
	if(!query) {
	DEBUG_M("TxnManager::init Query alloc\n");
	qry_pool.get(thd_id,query);
	}
	INC_STATS(get_thd_id(),mtx[16],get_sys_clock()-prof_starttime);
	//query->init();
	//reset();
	sem_init(&rsp_mutex, 0, 1);
	return_id = UINT64_MAX;

	this->h_wl = h_wl;
#if CC_ALG == MAAT
	uncommitted_writes = new std::set<uint64_t>();
	uncommitted_writes_y = new std::set<uint64_t>();
	uncommitted_reads = new std::set<uint64_t>();
#endif
#if CC_ALG == TICTOC
	_is_sub_txn = true;
	_min_commit_ts = glob_manager.get_max_cts();;
	_num_lock_waits = 0;
	_signal_abort = false;
	_timestamp = glob_manager.get_ts(get_thd_id());
#endif
#if CC_ALG == CALVIN
	phase = CALVIN_RW_ANALYSIS;
	locking_done = false;
	calvin_locked_rows.init(MAX_ROW_PER_TXN);
	need_require_cache_num = 0;
	row_wait_for_cache = new vector<pair<row_t*, bool>>();
	cache_ready = false;
	rtxn_but_wait = false;
#endif
#if CC_ALG == DLI_MVCC || CC_ALG == DLI_MVCC_OCC
	is_abort = nullptr;
#endif
#if CC_ALG == SILO
	_pre_abort = (g_params["pre_abort"] == "true");
	if (g_params["validation_lock"] == "no-wait")
		_validation_no_wait = true;
	else if (g_params["validation_lock"] == "waiting")
		_validation_no_wait = false;
	else
		assert(false);
  _cur_tid = 0;
  num_locks = 0;
  memset(write_set, 0, sizeof(write_set));
  // write_set = (int *) mem_allocator.alloc(sizeof(int) * 100);
#endif
#if CC_ALG == HDCC
	_pre_abort = (g_params["pre_abort"] == "true");
	_pre_abort = false;	//set to false for now
	phase = CALVIN_RW_ANALYSIS;
	locking_done = false;
	calvin_locked_rows.init(MAX_ROW_PER_TXN);

	num_locks = 0;
	memset(write_set, 0, sizeof(write_set));
	max_calvin_tid = 0;
	max_calvin_bid = 0;
#endif
#if CC_ALG == SNAPPER
	phase = CALVIN_RW_ANALYSIS;
	calvin_locked_rows.init(MAX_ROW_PER_TXN);
	wait_for_locks = set<row_t *>();
	read_write_set = vector<pair<row_t *, access_t>>();
	wait_for_locks_ready = true;
	last_lock_ts = 0;
	algo = -1;
#endif
#if CC_ALG == ARIA
	read_set.resize(g_node_cnt);
	write_set.resize(g_node_cnt);
#endif

	registed_ = false;
	txn_ready = true;
	twopl_wait_start = 0;

#if SINGLE_WRITE_NODE
	log_records = std::vector<LogRecord *>();
#endif

	txn_stats.init();
}

// reset after abort
void TxnManager::reset() {
#if CC_ALG == SNAPPER
	lock_ready = (algo == WAIT_DIE)? true: false;
#else
	lock_ready = false;
#endif
#if CC_ALG == CALVIN && CALVIN_W
	for(UInt32 i = 0 ; i < g_sched_thread_cnt ; i++)
	{
		lockers_has_watched[i] = false;
	}
	worker_has_dealed = false;
	has_ready = false;
	acquired_lock_num = 0;
	sum_lock_num = 0;
#endif
	cache_not_ready = false;
	lock_ready_cnt = 0;
	locking_done = true;
#if CC_ALG == DLI_MVCC || CC_ALG == DLI_MVCC_OCC
	is_abort = nullptr;
#endif
	ready_part = 0;
	rsp_cnt = 0;
	aborted = false;
	return_id = UINT64_MAX;
	twopl_wait_start = 0;
	log_flushed = false;
	log_flushed_cnt = 0;

	//ready = true;

	// MaaT & DTA & WKDB
	greatest_write_timestamp = 0;
	greatest_read_timestamp = 0;
	commit_timestamp = 0;
#if CC_ALG == MAAT
	uncommitted_writes->clear();
	uncommitted_writes_y->clear();
	uncommitted_reads->clear();
#endif

#if CC_ALG == CALVIN || CC_ALG == HDCC
	phase = CALVIN_RW_ANALYSIS;
	locking_done = false;
	calvin_locked_rows.clear();
	need_require_cache_num = 0;
	row_wait_for_cache->clear();
	cache_ready = false;
	rtxn_but_wait = false;
#endif
#if CC_ALG == SNAPPER
	phase = CALVIN_RW_ANALYSIS;
	locking_done = false;
	calvin_locked_rows.clear();
	isTimeout = false;
	read_write_set.clear();
	wait_for_locks.clear();
	wait_for_locks_ready = true;
	wait_row = NULL;
	dependOn.clear();
	dependBy.clear();
	last_lock_ts = 0;
#endif
#if CC_ALG == ARIA
	for (uint64_t i = 0; i < g_node_cnt; i++) {
		read_set[i].clear();
		write_set[i].clear();
	}
	w_loc = true;
	c_w_loc = true;
	ol_supply_w_all_loc = true;
	participants_cnt = 0;
	raw = false;
	war = false;
	aria_phase = ARIA_READ;
#endif

	assert(txn);
	assert(query);
	txn->reset(get_thd_id());

	// Stats
	txn_stats.reset();

#if SINGLE_WRITE_NODE
	log_records.clear();
#endif
}

void TxnManager::release() {
	uint64_t prof_starttime = get_sys_clock();
#if CC_ALG == HDCC || CC_ALG == SNAPPER
	qry_pool.put(get_thd_id(),query, algo);
#else
	qry_pool.put(get_thd_id(),query);
#endif
	INC_STATS(get_thd_id(),mtx[0],get_sys_clock()-prof_starttime);
	query = NULL;
	prof_starttime = get_sys_clock();
	txn_pool.put(get_thd_id(),txn);
	INC_STATS(get_thd_id(),mtx[1],get_sys_clock()-prof_starttime);
	txn = NULL;

#if CC_ALG == MAAT
	delete uncommitted_writes;
	delete uncommitted_writes_y;
	delete uncommitted_reads;
#endif

#if CC_ALG == CALVIN
	calvin_locked_rows.release();
	delete row_wait_for_cache;
#endif
#if CC_ALG == SILO
  num_locks = 0;
  memset(write_set, 0, sizeof(write_set));
  // mem_allocator.free(write_set, sizeof(int) * 100);
#endif
#if CC_ALG == HDCC
	calvin_locked_rows.release();
	num_locks = 0;
	memset(write_set, 0, sizeof(write_set));
	max_calvin_tid = 0;
	max_calvin_bid = 0;
#endif
#if CC_ALG == SNAPPER
	calvin_locked_rows.release();
	read_write_set.clear();
	wait_for_locks.clear();
	wait_for_locks_ready = true;
	wait_row = NULL;
	dependOn.clear();
	dependBy.clear();
	last_lock_ts = 0;
#endif
	txn_ready = true;

#if SINGLE_WRITE_NODE
	//free memory in logRecords
	for (uint64_t i = 0; i < log_records.size(); i++) {
		mem_allocator.free(log_records[i], sizeof(LogRecord) + 2 * log_records[i]->rcd.image_size - 1);
	}
#endif
}

void TxnManager::reset_query() {
#if WORKLOAD == YCSB
#if CC_ALG == HDCC || CC_ALG == SNAPPER
	((YCSBQuery*)query)->reset(algo);
#else
	((YCSBQuery*)query)->reset();
#endif
#elif WORKLOAD == TPCC
#if CC_ALG == HDCC || CC_ALG == SNAPPER
	((TPCCQuery*)query)->reset(algo);
#else
	((TPCCQuery*)query)->reset();
#endif
#elif WORKLOAD == PPS
	((PPSQuery*)query)->reset();
#endif
}

RC TxnManager::commit() {
	DEBUG("Commit %ld\n",get_txn_id());
	while(true) {
		uint64_t txn_cnt = simulation->now_batch_txn_cnt;
		uint64_t now_batch = simulation->now_batch;
		if (txn_cnt + 1 < g_replay_batch_size) {
			if (ATOM_CAS(simulation->now_batch_txn_cnt, txn_cnt, txn_cnt + 1)) {
				set_batch_id(now_batch);
				break;
			}
		} else if (txn_cnt + 1 == g_replay_batch_size) {
			if (ATOM_CAS(simulation->now_batch_txn_cnt, txn_cnt, 0)) {
				set_batch_id(ATOM_ADD_FETCH(simulation->now_batch, 1));
				break;
			}
		} else {
			assert(false);
		}
	}
	release_locks(RCOK);
#if CC_ALG == MAAT
	time_table.release(get_thd_id(),get_txn_id());
#endif
#if CC_ALG == WOOKONG
	wkdb_time_table.release(get_thd_id(),get_txn_id());
#endif
#if CC_ALG == TICTOC
	tictoc_man.cleanup(RCOK, this);
#endif
#if CC_ALG == SSI
	inout_table.set_commit_ts(get_thd_id(), get_txn_id(), get_commit_timestamp());
	inout_table.set_state(get_thd_id(), get_txn_id(), SSI_COMMITTED);
#endif
	commit_stats();
#if LOGGING && CC_ALG != CALVIN
#if CC_ALG == HDCC
	if (algo != CALVIN) {
		LogRecord * record = logger.createRecord(get_txn_id(),L_COMMIT,0,0,max_calvin_tid);
#else
		LogRecord * record = logger.createRecord(get_txn_id(),L_COMMIT,0,0);
#endif
		if(g_repl_cnt > 0) {
			msg_queue.enqueue(get_thd_id(), Message::create_message(record, LOG_MSG),
												g_node_id + g_node_cnt + g_client_node_cnt);
		}
		logger.enqueueRecord(record);
		if (IS_LOCAL(get_txn_id())) {
			return WAIT;
		}
#if CC_ALG == HDCC
	}
#endif
#endif
	return Commit;
}

RC TxnManager::abort() {
	if (aborted) return Abort;
#if CC_ALG == SSI
	inout_table.set_state(get_thd_id(), get_txn_id(), SSI_ABORTED);
	inout_table.clear_Conflict(get_thd_id(), get_txn_id());
#endif
	DEBUG("Abort %ld\n",get_txn_id());
	txn->rc = Abort;
	INC_STATS(get_thd_id(),total_txn_abort_cnt,1);
	txn_stats.abort_cnt++;
	if(IS_LOCAL(get_txn_id())) {
	INC_STATS(get_thd_id(), local_txn_abort_cnt, 1);
	} else {
	INC_STATS(get_thd_id(), remote_txn_abort_cnt, 1);
	txn_stats.abort_stats(get_thd_id());
	}

	aborted = true;
	release_locks(Abort);
#if CC_ALG == MAAT
	//assert(time_table.get_state(get_txn_id()) == MAAT_ABORTED);
	time_table.release(get_thd_id(),get_txn_id());
#endif
#if CC_ALG == WOOKONG
	wkdb_time_table.release(get_thd_id(),get_txn_id());
#endif
#if CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
	//assert(time_table.get_state(get_txn_id()) == MAAT_ABORTED);
	dta_time_table.release(get_thd_id(), get_txn_id());
#endif

	uint64_t timespan = get_sys_clock() - txn_stats.restart_starttime;
	if (IS_LOCAL(get_txn_id()) && warmup_done) {
		INC_STATS_ARR(get_thd_id(),start_abort_commit_latency, timespan);
	}
#if LOGGING && CC_ALG != CALVIN
#if CC_ALG == HDCC
	if (algo != CALVIN) {
#endif
		LogRecord * record = logger.createRecord(get_txn_id(),L_ABORT,0,0);
		if(g_repl_cnt > 0) {
			msg_queue.enqueue(get_thd_id(), Message::create_message(record, LOG_MSG),
												g_node_id + g_node_cnt + g_client_node_cnt);
		}
		logger.enqueueRecord(record);
#if CC_ALG == HDCC
	}
#endif
#endif
#if SINGLE_WRITE_NODE
	for (uint64_t i = 0; i < log_records.size(); i++) {
		mem_allocator.free(log_records[i], sizeof(LogRecord) + 2 * log_records[i]->rcd.image_size - 1);
	}
	log_records.clear();
#endif
	/*
	// latency from most recent start or restart of transaction
	PRINT_LATENCY("lat_s %ld %ld 0 %f %f %f %f %f %f 0.0\n"
			, get_txn_id()
			, txn_stats.work_queue_cnt
			, (double) timespan / BILLION
			, (double) txn_stats.work_queue_time / BILLION
			, (double) txn_stats.msg_queue_time / BILLION
			, (double) txn_stats.cc_block_time / BILLION
			, (double) txn_stats.cc_time / BILLION
			, (double) txn_stats.process_time / BILLION
			);
			*/
	//commit_stats();
	return Abort;
}

RC TxnManager::start_abort() {
	// ! trans process time
	uint64_t prepare_start_time = get_sys_clock();
	txn_stats.prepare_start_time = prepare_start_time;
	uint64_t process_time_span  = prepare_start_time - txn_stats.restart_starttime;
	txn_stats.trans_process_time = process_time_span;
	// INC_STATS(get_thd_id(), trans_process_time, process_time_span);
  	// INC_STATS(get_thd_id(), trans_process_count, 1);
	txn->rc = Abort;
	DEBUG("%ld start_abort\n",get_txn_id());

	uint64_t finish_start_time = get_sys_clock();
	txn_stats.finish_start_time = finish_start_time;
	// uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
	// INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
  	// INC_STATS(get_thd_id(), trans_prepare_count, 1);
	if(query->partitions_touched.size() > 1) {
		txn_stats.trans_abort_network_start_time = get_sys_clock();
		send_finish_messages();
		abort();
		return Abort;
	}
	return abort();
}

#ifdef NO_2PC
RC TxnManager::start_commit() {
	RC rc = RCOK;
	DEBUG("%ld start_commit RO?%d\n",get_txn_id(),query->readonly());
	_is_sub_txn = false;

	rc = validate();
	if(CC_ALG == SSI) {
		ssi_man.gene_finish_ts(this);
	}
	if(CC_ALG == WSI) {
		wsi_man.gene_finish_ts(this);
	}
	if(rc == RCOK)
		rc = commit();
	else
		start_abort();

		return rc;
}
#else
RC TxnManager::start_commit() {
	// ! trans process time
	uint64_t prepare_start_time = get_sys_clock();
	txn_stats.prepare_start_time = prepare_start_time;
	uint64_t process_time_span  = prepare_start_time - txn_stats.restart_starttime;
	txn_stats.trans_process_time = process_time_span;
	// INC_STATS(get_thd_id(), trans_process_time, process_time_span);
  	// INC_STATS(get_thd_id(), trans_process_count, 1);
	RC rc = RCOK;
	DEBUG("%ld start_commit RO?%d\n",get_txn_id(),query->readonly());
	if(is_multi_part()) {
#if LOGGING && CC_ALG != CALVIN
#if CC_ALG == HDCC
	if (algo != CALVIN) {
#endif
		LogRecord * record = logger.createRecord(get_txn_id(),L_FLUSH,0,0);
		if(g_repl_cnt > 0) {
			msg_queue.enqueue(get_thd_id(), Message::create_message(record, LOG_MSG),
												g_node_id + g_node_cnt + g_client_node_cnt);
		}
		logger.enqueueRecord(record);
#if CC_ALG == HDCC
	}
#endif
#endif
		if(CC_ALG == TICTOC) {
			rc = validate();
			if (rc != Abort) {
				txn_stats.trans_validate_network_start_time = get_sys_clock();
				send_prepare_messages();
				rc = WAIT_REM;
			}
		} else if (!query->readonly() || CC_ALG == OCC || CC_ALG == MAAT || CC_ALG == DLI_BASE ||
				CC_ALG == DLI_OCC || CC_ALG == SILO || CC_ALG == BOCC || CC_ALG == SSI || CC_ALG == HDCC || CC_ALG == SNAPPER) {
			// send prepare messages
			txn_stats.trans_validate_network_start_time = get_sys_clock();
			send_prepare_messages();
#if CC_ALG == HDCC
			txn->rc = validate();				
#endif
			rc = WAIT_REM;
		} else {
			if(CC_ALG == WOOKONG && IS_LOCAL(get_txn_id()) && rc == RCOK) {
				rc = wkdb_man.find_bound(this);
				if (g_ts_alloc == LTS_HLC_CLOCK) {
					// hlc_ts.update_with_cts(get_commit_timestamp());
				}
			}
			uint64_t finish_start_time = get_sys_clock();
			txn_stats.finish_start_time = finish_start_time;
			// uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
			// INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
      		// INC_STATS(get_thd_id(), trans_prepare_count, 1);
			if(CC_ALG == WSI) {
				wsi_man.gene_finish_ts(this);
			}
			txn_stats.trans_commit_network_start_time = get_sys_clock();
			send_finish_messages();
			rsp_cnt = 0;
			rc = commit();
		}
	} else { // is not multi-part
		rc = validate();
		uint64_t finish_start_time = get_sys_clock();
		txn_stats.finish_start_time = finish_start_time;
		// uint64_t prepare_timespan  = finish_start_time - txn_stats.prepare_start_time;
		// INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
    	// INC_STATS(get_thd_id(), trans_prepare_count, 1);
		if(CC_ALG == SSI) {
			ssi_man.gene_finish_ts(this);
		}
		if(CC_ALG == WSI) {
			wsi_man.gene_finish_ts(this);
		}
		if(rc == RCOK) {
#if SINGLE_WRITE_NODE
			if (!query->readonly()) {
				log_records.push_back(logger.createRecord(get_txn_id(),L_COMMIT,0,0,0,0));
				for (uint64_t j = 0; j < g_storage_log_node_cnt; j++) {
					Message * msg = Message::create_message(log_records, LOG_MSG);
					msg_queue.enqueue(get_thd_id(), msg, g_node_cnt + g_client_node_cnt + j);
				}
				rc = WAIT_REM;
			} else {
				rc = commit();
			}
#endif
			// rc = commit();
		} else {
			txn->rc = Abort;
			DEBUG("%ld start_abort\n",get_txn_id());
			//TODO: the size cannot bigger than 1
			if(query->partitions_touched.size() > 1) {
				txn_stats.trans_abort_network_start_time = get_sys_clock();
				send_finish_messages();
				abort();
				rc = Abort;
			}
			rc = abort();
		}
	}
	return rc;
}
#endif
void TxnManager::send_prepare_messages() {
	rsp_cnt = query->partitions_touched.size() - 1;
	DEBUG("%ld Send PREPARE messages to %d\n",get_txn_id(),rsp_cnt);
	for(uint64_t i = 0; i < query->partitions_touched.size(); i++) {
	if(GET_NODE_ID(query->partitions_touched[i]) == g_node_id) {
		continue;
	}
		msg_queue.enqueue(get_thd_id(), Message::create_message(this, RPREPARE),
											GET_NODE_ID(query->partitions_touched[i]));
#if CC_ALG == ARIA
		participants_cnt++;
#endif
	}
}

void TxnManager::send_validation_messages() {
	rsp_cnt = query->partitions_touched.size() - 1;
	DEBUG("%ld Send PREPARE messages to %d\n",get_txn_id(),rsp_cnt);
	for(uint64_t i = 0; i < query->partitions_touched.size(); i++) {
		if(GET_NODE_ID(query->partitions_touched[i]) == g_node_id) {
			continue;
		}
		msg_queue.enqueue(get_thd_id(), Message::create_message(this, VALID),
											GET_NODE_ID(query->partitions_touched[i]));
	}
}

void TxnManager::send_finish_messages() {
	rsp_cnt = query->partitions_touched.size() - 1;
	assert(IS_LOCAL(get_txn_id()));
	DEBUG("%ld Send FINISH messages to %d\n",get_txn_id(),rsp_cnt);
	for(uint64_t i = 0; i < query->partitions_touched.size(); i++) {
		if(GET_NODE_ID(query->partitions_touched[i]) == g_node_id) {
			continue;
    }
		msg_queue.enqueue(get_thd_id(), Message::create_message(this, RFIN),
											GET_NODE_ID(query->partitions_touched[i]));
	}
}

int TxnManager::received_response(RC rc) {
	assert(txn->rc == RCOK || txn->rc == Abort);
	if (txn->rc == RCOK) txn->rc = rc;
#if CC_ALG == CALVIN
	++rsp_cnt;
#elif CC_ALG == HDCC || CC_ALG == SNAPPER
	if (algo == CALVIN) {
		++rsp_cnt;
	} else if (rsp_cnt > 0) {
		--rsp_cnt;
	}
	return rsp_cnt;
#else
  if (rsp_cnt > 0)
	  --rsp_cnt;
#endif
	return rsp_cnt;
}

bool TxnManager::waiting_for_response() { return rsp_cnt > 0; }

bool TxnManager::is_multi_part() {
	return query->partitions_touched.size() > 1;
	//return query->partitions.size() > 1;
}

void TxnManager::commit_stats() {
	uint64_t commit_time = get_sys_clock();
	uint64_t timespan_short = commit_time - txn_stats.restart_starttime;
	uint64_t timespan_long  = commit_time - txn_stats.starttime;
	INC_STATS(get_thd_id(),total_txn_commit_cnt,1);

	uint64_t warmuptime = get_sys_clock() - simulation->run_starttime;
	DEBUG("Commit_stats execute_time %ld warmup_time %ld\n",warmuptime,g_warmup_timer);
	if (simulation->is_warmup_done())
		DEBUG("Commit_stats total_txn_commit_cnt %ld\n",stats._stats[get_thd_id()]->total_txn_commit_cnt);
	if(get_txn_id() % g_node_cnt != g_node_id) {
#if CC_ALG == HDCC
		if (algo == CALVIN) {
			// INC_STATS(get_thd_id(), txn_run_time, timespan_long);
			// INC_STATS(get_thd_id(),multi_part_txn_run_time,timespan_long);
			// INC_STATS(get_thd_id(),single_part_txn_run_time,timespan_long);
			INC_STATS(get_thd_id(), hdcc_calvin_cnt, 1);
		} else {
			INC_STATS(get_thd_id(), hdcc_silo_cnt, 1);
		}
		INC_STATS(get_thd_id(),remote_txn_commit_cnt,1);
		txn_stats.commit_stats(get_thd_id(), get_txn_id(), get_batch_id(), timespan_long,
													timespan_short);
		return;
#elif CC_ALG == SNAPPER
		if (algo == CALVIN) {
			// INC_STATS(get_thd_id(), txn_run_time, timespan_long);
			// INC_STATS(get_thd_id(),multi_part_txn_run_time,timespan_long);
			// INC_STATS(get_thd_id(),single_part_txn_run_time,timespan_long);
			INC_STATS(get_thd_id(), snapper_calvin_cnt, 1);
		} else {
			INC_STATS(get_thd_id(), snapper_lock_cnt, 1);
		}
		INC_STATS(get_thd_id(),remote_txn_commit_cnt,1);
		txn_stats.commit_stats(get_thd_id(), get_txn_id(), get_batch_id(), timespan_long,
													timespan_short);
		return;
#else
		INC_STATS(get_thd_id(),remote_txn_commit_cnt,1);
		txn_stats.commit_stats(get_thd_id(), get_txn_id(), get_batch_id(), timespan_long,
													 timespan_short);
		return;
#endif
	}


	INC_STATS(get_thd_id(),txn_cnt,1);
	INC_STATS(get_thd_id(),local_txn_commit_cnt,1);
#if CC_ALG ==HDCC
	if (algo == CALVIN) {
		INC_STATS(get_thd_id(), hdcc_calvin_cnt, 1);
		INC_STATS(get_thd_id(), hdcc_calvin_local_cnt, 1);
	} else {
		INC_STATS(get_thd_id(), hdcc_silo_cnt, 1);
		INC_STATS(get_thd_id(), hdcc_silo_local_cnt, 1);
	}
#endif
#if CC_ALG == SNAPPER
	if (algo == CALVIN) {
		INC_STATS(get_thd_id(), snapper_calvin_cnt, 1);
	} else {
		INC_STATS(get_thd_id(), snapper_lock_cnt, 1);

	}
#endif
	INC_STATS(get_thd_id(), txn_run_time, timespan_long);
	if(query->partitions_touched.size() > 1) {
		INC_STATS(get_thd_id(),multi_part_txn_cnt,1);
		INC_STATS(get_thd_id(),multi_part_txn_run_time,timespan_long);
	} else {
		INC_STATS(get_thd_id(),single_part_txn_cnt,1);
		INC_STATS(get_thd_id(),single_part_txn_run_time,timespan_long);
	}
	/*if(cflt) {
		INC_STATS(get_thd_id(),cflt_cnt_txn,1);
	}*/
	txn_stats.commit_stats(get_thd_id(),get_txn_id(),get_batch_id(),timespan_long, timespan_short);
	#if CC_ALG == CALVIN
	return;
	#endif

#if CC_ALG == HDCC || CC_ALG == SNAPPER
	if (algo == CALVIN)
	{
		return;
	}
#endif
	INC_STATS_ARR(get_thd_id(),start_abort_commit_latency, timespan_short);
	INC_STATS_ARR(get_thd_id(),last_start_commit_latency, timespan_short);
	INC_STATS_ARR(get_thd_id(),first_start_commit_latency, timespan_long);

	assert(query->partitions_touched.size() > 0);
	INC_STATS(get_thd_id(),parts_touched,query->partitions_touched.size());
	INC_STATS(get_thd_id(),part_cnt[query->partitions_touched.size()-1],1);
	for(uint64_t i = 0 ; i < query->partitions_touched.size(); i++) {
		INC_STATS(get_thd_id(),part_acc[query->partitions_touched[i]],1);
	}
}

void TxnManager::register_thread(Thread * h_thd) {
	this->h_thd = h_thd;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
	this->active_part = GET_PART_ID_FROM_IDX(get_thd_id());
#endif
}

void TxnManager::set_txn_id(txnid_t txn_id) { txn->txn_id = txn_id; }

txnid_t TxnManager::get_txn_id() { return txn->txn_id; }

Workload *TxnManager::get_wl() { return h_wl; }

uint64_t TxnManager::get_thd_id() {
	if(h_thd)
	return h_thd->get_thd_id();
	else
	return 0;
}

BaseQuery *TxnManager::get_query() { return query; }
void TxnManager::set_query(BaseQuery *qry) { query = qry; }

void TxnManager::set_timestamp(ts_t timestamp) { txn->timestamp = timestamp; }

ts_t TxnManager::get_timestamp() { return txn->timestamp; }

void TxnManager::set_start_timestamp(uint64_t start_timestamp) {
	txn->start_timestamp = start_timestamp;
}

ts_t TxnManager::get_start_timestamp() { return txn->start_timestamp; }

uint64_t TxnManager::incr_lr() {
	//ATOM_ADD(this->rsp_cnt,i);
	uint64_t result;
	sem_wait(&rsp_mutex);
	result = ++this->lock_ready_cnt;
	sem_post(&rsp_mutex);
	return result;
}

uint64_t TxnManager::decr_lr() {
	//ATOM_SUB(this->rsp_cnt,i);
	uint64_t result;
	sem_wait(&rsp_mutex);
	result = --this->lock_ready_cnt;
	sem_post(&rsp_mutex);
	return result;
}
uint64_t TxnManager::incr_rsp(int i) {
	//ATOM_ADD(this->rsp_cnt,i);
	uint64_t result;
	sem_wait(&rsp_mutex);
	result = ++this->rsp_cnt;
	sem_post(&rsp_mutex);
	return result;
}

uint64_t TxnManager::decr_rsp(int i) {
	//ATOM_SUB(this->rsp_cnt,i);
	uint64_t result;
	sem_wait(&rsp_mutex);
	result = --this->rsp_cnt;
	sem_post(&rsp_mutex);
	return result;
}

void TxnManager::release_last_row_lock() {
	assert(txn->row_cnt > 0);
	row_t * orig_r = txn->accesses[txn->row_cnt-1]->orig_row;
	access_t type = txn->accesses[txn->row_cnt-1]->type;
	orig_r->return_row(RCOK, type, this, NULL);
	//txn->accesses[txn->row_cnt-1]->orig_row = NULL;
}

void TxnManager::cleanup_row(RC rc, uint64_t rid) {
	access_t type = txn->accesses[rid]->type;
	if (type == WR && rc == Abort && CC_ALG != MAAT) {
		type = XP;
	}

	uint64_t version = 0;
	// Handle calvin elsewhere

#if CC_ALG != CALVIN
#if ISOLATION_LEVEL != READ_UNCOMMITTED
	row_t * orig_r = txn->accesses[rid]->orig_row;
#if CC_ALG == HDCC && DETERMINISTIC_ABORT_MODE
	if (algo == CALVIN && ROLL_BACK && type == XP) {
		orig_r->return_row(rc, type, this, txn->accesses[rid]->orig_data);
	} else {
#elif CC_ALG == SNAPPER
	if (algo == CALVIN) {
	} else if (ROLL_BACK && type == XP) {
			orig_r->return_row(rc,type, this, txn->accesses[rid]->orig_data);
	} else {
#else
	if (ROLL_BACK && type == XP &&
			(CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE ||
			 CC_ALG == HSTORE_SPEC)) {
		orig_r->return_row(rc,type, this, txn->accesses[rid]->orig_data);
	} else {
#endif
#if ISOLATION_LEVEL == READ_COMMITTED
		if(type == WR) {
			version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);
		}
#else
#if CC_ALG == HDCC
		if (algo == CALVIN) {
			orig_r->return_row(rc, type, this, txn->accesses[rid]->data);
		} else {
			version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);
		}
#else
	version = orig_r->return_row(rc, type, this, txn->accesses[rid]->data);
#endif
#endif
	}
#endif

#if ROLL_BACK && \
		(CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC)
	if (type == WR) {
		//printf("free 10 %ld\n",get_txn_id());
				txn->accesses[rid]->orig_data->free_row();
		DEBUG_M("TxnManager::cleanup row_t free\n");
		row_pool.put(get_thd_id(),txn->accesses[rid]->orig_data);
		if(rc == RCOK) {
			INC_STATS(get_thd_id(),record_write_cnt,1);
			++txn_stats.write_cnt;
		}
	}
#endif
#if ROLL_BACK && CC_ALG == SNAPPER
	if (algo == WAIT_DIE && type == WR) {
		txn->accesses[rid]->orig_data->free_row();
		DEBUG_M("TxnManager::cleanup row_t free\n");
		row_pool.put(get_thd_id(),txn->accesses[rid]->orig_data);
		if(rc == RCOK) {
			INC_STATS(get_thd_id(),record_write_cnt,1);
			++txn_stats.write_cnt;
		}
	}
#endif
#if ROLL_BACK && CC_ALG == HDCC && DETERMINISTIC_ABORT_MODE
	if (algo == CALVIN && type == WR) {
		txn->accesses[rid]->orig_data->free_row();
		DEBUG_M("TxnManager::cleanup row_t free\n");
		row_pool.put(get_thd_id(),txn->accesses[rid]->orig_data);
		if(rc == RCOK) {
			INC_STATS(get_thd_id(),record_write_cnt,1);
			++txn_stats.write_cnt;
		}
	}
#endif
#endif
	if (type == WR) txn->accesses[rid]->version = version;
#if CC_ALG == TICTOC
	if (_min_commit_ts > glob_manager.get_max_cts())
		glob_manager.set_max_cts(_min_commit_ts);
#endif

#if CC_ALG != SILO
#if CC_ALG == HDCC
	if (algo == SILO) {
		return;
	}
#endif
  txn->accesses[rid]->data = NULL;
#endif
}

void TxnManager::cleanup(RC rc) {
	if (rc == RCOK) {
		do_insert();
		do_delete();
	}
#if CC_ALG == SILO
  finish(rc);
#endif
#if CC_ALG == HDCC
	if (algo == SILO) {
		finish(rc);
	}
#endif
#if CC_ALG == ARIA
	finish(rc);
#endif
#if CC_ALG == OCC && MODE == NORMAL_MODE
	occ_man.finish(rc,this);
#endif
#if CC_ALG == BOCC && MODE == NORMAL_MODE
	bocc_man.finish(rc,this);
#endif
#if CC_ALG == FOCC && MODE == NORMAL_MODE
	focc_man.finish(rc,this);
#endif
#if (CC_ALG == WSI) && MODE == NORMAL_MODE
	wsi_man.finish(rc,this);
#endif
	ts_t starttime = get_sys_clock();
	uint64_t row_cnt = txn->accesses.get_count();
	assert(txn->accesses.get_count() == txn->row_cnt);
	// assert((WORKLOAD == YCSB && row_cnt <= g_req_per_query) || (WORKLOAD == TPCC && row_cnt <=
	// g_max_items_per_txn*2 + 3));

	DEBUG("Cleanup %ld %ld\n",get_txn_id(),row_cnt);
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		cleanup_row(rc,rid);
	}
#if CC_ALG == SNAPPER
	if (wait_row) {
		wait_row->manager->lock_release(this);
		wait_row = NULL;
	}
#endif
#if CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || \
		CC_ALG == DLI_MVCC_BASE
	dli_man.finish_trans(rc, this);
#endif
#if CC_ALG == CALVIN
	// cleanup locked rows
	std::string str_out = "batch_id = " + std::to_string(get_batch_id()) +
						" , txn_id = " + std::to_string(get_txn_id()) +" , unlock_keys:";
	set<uint64_t> keys;
	for (uint64_t i = 0; i < calvin_locked_rows.size(); i++) {
		row_t * row = calvin_locked_rows[i];
		keys.insert(row->get_primary_key());
		row->return_row(rc,RD,this,row);
	}
	// for(auto key : keys)
	// {
	// 	str_out += " " + std::to_string(key);
	// }
	// str_out += ".\n";
	// std::cout<<str_out<<std::flush;
#endif
#if CC_ALG == SNAPPER
	if (algo == CALVIN) {
		// cleanup locked rows
		for (uint64_t i = 0; i < calvin_locked_rows.size(); i++) {
			row_t * row = calvin_locked_rows[i];
			row->return_row(rc,RD,this,row);
		}
	}
#endif

#if CC_ALG == DTA
	dta_man.finish(rc, this);
#endif
	bool last_row_processed = false;
	for (uint64_t i = 0; i < txn->accesses.size(); i++) {
		if (rc == Abort || txn->accesses[i]->type != XP) {
			pthread_mutex_lock(txn->accesses[i]->orig_row->cache_node->locker);
			int64_t now_num = ATOM_SUB_FETCH(txn->accesses[i]->orig_row->cache_node->use_cache_num, 1);
			assert(now_num >= 0);
			pthread_mutex_unlock(txn->accesses[i]->orig_row->cache_node->locker);
			if (txn->accesses[i]->orig_row == last_row) {
				last_row_processed = true;
			}
		}
	}
	if (rc == Abort) {
		txn->release_inserts(get_thd_id());
		txn->insert_rows.clear();
		txn->delete_rows.clear();

		if (!last_row_processed) {
			pthread_mutex_lock(last_row->cache_node->locker);
			int64_t now_num = ATOM_SUB_FETCH(last_row->cache_node->use_cache_num, 1);
			assert(now_num >= 0);
			pthread_mutex_unlock(last_row->cache_node->locker);
		}

		INC_STATS(get_thd_id(), abort_time, get_sys_clock() - starttime);
	}
}

RC TxnManager::get_lock(row_t * row, access_t type) {
	if (calvin_locked_rows.contains(row)) {
		return RCOK;
	}
	if (type != XP) {
		calvin_locked_rows.add(row);
	}
	RC rc = row->get_lock(type, this);
	if(rc == WAIT) {
		INC_STATS(get_thd_id(), txn_wait_cnt, 1);
	}
	return rc;
}

RC TxnManager::get_row(row_t * row, access_t type, row_t *& row_rtn) {
	uint64_t starttime = get_sys_clock();
	uint64_t timespan;
	RC rc = RCOK;
	DEBUG_M("TxnManager::get_row access alloc\n");
	Access * access = NULL;
	this->last_row = row;
	this->last_type = type;
  uint64_t get_access_end_time = 0;
#if CC_ALG != CALVIN
	pthread_mutex_lock(row->cache_node->locker);
	if (row->cache_node->use_cache_num >= 0) {
		row->cache_node->use_cache_num++;
		row_cache.move_to_tail(this, row->cache_node, hash_key_or_wh_to_cache(row));
	} else {
		if (row->cache_node->is_cache_required == false) {
			row->cache_node->is_cache_required = true;
			pthread_mutex_unlock(row->cache_node->locker);
			Message * msg = Message::create_message(this,RSTO);
			RStorageMessage * rmsg = (RStorageMessage *) msg;
			rmsg->table_ids.init(1);
			rmsg->keys.init(1);
			rmsg->table_ids.add(row->get_table()->get_table_id());
			rmsg->keys.add(row->get_primary_key());
			msg_queue.enqueue(get_thd_id(), msg, g_node_id / g_servers_per_storage + g_node_cnt + g_client_node_cnt);
		} else {
			WaitTxnNode * node = (WaitTxnNode *)mem_allocator.alloc(sizeof(WaitTxnNode));
			node->txn_man = this;
			LIST_PUT_TAIL(row->cache_node->wait_list_head, row->cache_node->wait_list_tail, node);
			pthread_mutex_unlock(row->cache_node->locker);
		}
		return WAIT;
	}
	pthread_mutex_unlock(row->cache_node->locker);
#endif
#if CC_ALG == TICTOC
	bool isexist = false;
	uint64_t size = get_write_set_size();
	size += get_read_set_size();
	// UInt32 n = 0, m = 0;
	for (uint64_t i = 0; i < size; i++) {
		if (txn->accesses[i]->orig_row == row) {
			access = txn->accesses[i];
			access->orig_row->get_ts(access->orig_wts, access->orig_rts);
			isexist = true;
			// DEBUG("TxnManagerTictoc::find the exist access \n", access->orig_data, access->orig_row, access->data, access->orig_rts, access->orig_wts);
			break;
		}
	}
	if (!access) {
		access_pool.get(get_thd_id(),access);

    get_access_end_time = get_sys_clock();
    INC_STATS(get_thd_id(), trans_get_access_count, 1);
    INC_STATS(get_thd_id(), trans_get_access_time, get_access_end_time - starttime);

		rc = row->get_row(type, this, access->data, access->orig_wts, access->orig_rts);
		if (!OCC_WAW_LOCK || type == RD) {
			_min_commit_ts = _min_commit_ts > access->orig_wts ? _min_commit_ts : access->orig_wts;
		} else {
			if (rc == WAIT)
						ATOM_ADD_FETCH(_num_lock_waits, 1);
						if (rc == Abort || rc == WAIT)
								return rc;
		}
    INC_STATS(get_thd_id(), trans_get_row_time, get_sys_clock() - get_access_end_time);
    INC_STATS(get_thd_id(), trans_get_row_count, 1);
	} else {
    get_access_end_time = get_sys_clock();
    INC_STATS(get_thd_id(), trans_get_access_count, 1);
    INC_STATS(get_thd_id(), trans_get_access_time, get_access_end_time - starttime);
  }
	if (!OCC_WAW_LOCK || type == RD) {
		access->locked = false;
	} else {
		_min_commit_ts = _min_commit_ts > access->orig_rts + 1 ? _min_commit_ts : access->orig_rts + 1;
		access->locked = true;
	}
#else
	access_pool.get(get_thd_id(),access);
  get_access_end_time = get_sys_clock();
  INC_STATS(get_thd_id(), trans_get_access_time, get_access_end_time - starttime);
  INC_STATS(get_thd_id(), trans_get_access_count, 1);
#endif
	//uint64_t row_cnt = txn->row_cnt;
	//assert(txn->accesses.get_count() - 1 == row_cnt);
#if CC_ALG != TICTOC
  // uint64_t start_time = get_sys_clock();
	rc = row->get_row(type, this, access);
  INC_STATS(get_thd_id(), trans_get_row_time, get_sys_clock() - get_access_end_time);
  INC_STATS(get_thd_id(), trans_get_row_count, 1);
#endif
#if CC_ALG == FOCC
	focc_man.active_storage(type, this, access);
#endif
#if CC_ALG == SNAPPER
	if (rc == WAIT) {
		wait_row = row;
	}
#endif
  uint64_t middle_time = get_sys_clock();
	if (rc == Abort || rc == WAIT) {
		row_rtn = NULL;
		DEBUG_M("TxnManager::get_row(abort) access free\n");
		access_pool.put(get_thd_id(),access);
		timespan = get_sys_clock() - starttime;
    INC_STATS(get_thd_id(), trans_store_access_time, timespan + starttime - middle_time);
    INC_STATS(get_thd_id(), trans_store_access_count, 1);
		INC_STATS(get_thd_id(), txn_manager_time, timespan);
		INC_STATS(get_thd_id(), txn_conflict_cnt, 1);
		//cflt = true;
#if DEBUG_TIMELINE
		printf("CONFLICT %ld %ld\n",get_txn_id(),get_sys_clock());
#endif
		return rc;
	}
	access->type = type;
	access->orig_row = row;
#if CC_ALG == SILO
	access->tid = last_tid;
#endif
#if ROLL_BACK && CC_ALG == SNAPPER
	if (algo == WAIT_DIE) {
		if (type == WR) {
			uint64_t part_id = row->get_part_id();
			DEBUG_M("TxnManager::get_row row_t alloc\n")
			row_pool.get(get_thd_id(),access->orig_data);
			access->orig_data->init(row->get_table(), part_id, 0);
			access->orig_data->copy(row);
			assert(access->orig_data->get_schema() == row->get_schema());

			// ARIES-style physiological logging
	#if LOGGING
				// LogRecord * record =
				// logger.createRecord(LRT_UPDATE,L_UPDATE,get_txn_id(),part_id,row->get_table()->get_table_id(),row->get_primary_key());
				LogRecord *record = logger.createRecord(
						get_txn_id(), L_UPDATE, row->get_table()->get_table_id(), row->get_primary_key());
			if(g_repl_cnt > 0) {
					msg_queue.enqueue(get_thd_id(), Message::create_message(record, LOG_MSG),
														g_node_id + g_node_cnt + g_client_node_cnt);
			}
			logger.enqueueRecord(record);
	#endif
			}
	}
#endif
#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || \
									CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC)
	if (type == WR) {
	//printf("alloc 10 %ld\n",get_txn_id());
	uint64_t part_id = row->get_part_id();
	DEBUG_M("TxnManager::get_row row_t alloc\n")
	row_pool.get(get_thd_id(),access->orig_data);
	access->orig_data->init(row->get_table(), part_id, 0);
	access->orig_data->copy(row);
	assert(access->orig_data->get_schema() == row->get_schema());

	// ARIES-style physiological logging
#if LOGGING
		// LogRecord * record =
		// logger.createRecord(LRT_UPDATE,L_UPDATE,get_txn_id(),part_id,row->get_table()->get_table_id(),row->get_primary_key());
		LogRecord *record = logger.createRecord(
				get_txn_id(), L_UPDATE, row->get_table()->get_table_id(), row->get_primary_key());
	if(g_repl_cnt > 0) {
			msg_queue.enqueue(get_thd_id(), Message::create_message(record, LOG_MSG),
												g_node_id + g_node_cnt + g_client_node_cnt);
	}
	logger.enqueueRecord(record);
#endif
	}
#endif

#if ROLL_BACK && CC_ALG == HDCC && DETERMINISTIC_ABORT_MODE
	if (algo == CALVIN) {
		if (type == WR) {
			uint64_t part_id = row->get_part_id();
			DEBUG_M("TxnManager::get_row row_t alloc\n")
			row_pool.get(get_thd_id(),access->orig_data);
			access->orig_data->init(row->get_table(), part_id, 0);
			access->orig_data->copy(row);
			assert(access->orig_data->get_schema() == row->get_schema());
		}
	}

#endif

#if LOGGING && CC_ALG != CALVIN
	if (type == WR) {
#if CC_ALG == HDCC
		if (algo != CALVIN) {
#endif
			LogRecord *record = logger.createRecord(
					get_txn_id(), L_UPDATE, row->get_table()->get_table_id(), row->get_primary_key());
			if(g_repl_cnt > 0) {
				msg_queue.enqueue(get_thd_id(), Message::create_message(record, LOG_MSG),
														g_node_id + g_node_cnt + g_client_node_cnt);
			}
			logger.enqueueRecord(record);
#if CC_ALG == HDCC
		}
#endif
	}
#endif

#if CC_ALG == TICTOC
	if (!isexist) {
		++txn->row_cnt;
		if (type == WR) ++txn->write_cnt;
		txn->accesses.add(access);
	}
#else
	++txn->row_cnt;
	if (type == WR) {
		++txn->write_cnt;
		++txn_stats.write_cnt;
	}
	txn->accesses.add(access);
#endif

	timespan = get_sys_clock() - starttime;
  INC_STATS(get_thd_id(), trans_store_access_time, timespan + starttime - middle_time);
  INC_STATS(get_thd_id(), trans_store_access_count, 1);
	INC_STATS(get_thd_id(), txn_manager_time, timespan);
	row_rtn  = access->data;

	if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == CALVIN) assert(rc == RCOK);
	assert(rc == RCOK);
	return rc;
}

RC TxnManager::get_row_post_wait(row_t *& row_rtn) {
	assert(CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC);

	uint64_t starttime = get_sys_clock();
#if CC_ALG == SNAPPER
	wait_row = NULL;
#endif
	row_t * row = this->last_row;
	access_t type = this->last_type;
	assert(row != NULL);
	DEBUG_M("TxnManager::get_row_post_wait access alloc\n")
	Access * access;
	access_pool.get(get_thd_id(),access);

	row->get_row_post_wait(type,this,access->data);

	access->type = type;
	access->orig_row = row;
#if ROLL_BACK && CC_ALG == SNAPPER
	if (algo == WAIT_DIE) {
		if (type == WR) {
			uint64_t part_id = row->get_part_id();
			DEBUG_M("TxnManager::get_row_post_wait row_t alloc\n")
			row_pool.get(get_thd_id(),access->orig_data);
			access->orig_data->init(row->get_table(), part_id, 0);
			access->orig_data->copy(row);
		}
	}
#endif
#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
	if (type == WR) {
		uint64_t part_id = row->get_part_id();
	//printf("alloc 10 %ld\n",get_txn_id());
	DEBUG_M("TxnManager::get_row_post_wait row_t alloc\n")
	row_pool.get(get_thd_id(),access->orig_data);
		access->orig_data->init(row->get_table(), part_id, 0);
		access->orig_data->copy(row);
	}
#endif

	++txn->row_cnt;
	if (type == WR) ++txn->write_cnt;
	txn->accesses.add(access);
	uint64_t timespan = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_manager_time, timespan);
	this->last_row_rtn  = access->data;
	row_rtn  = access->data;
	return RCOK;
}

void TxnManager::process_cache(uint64_t thd_id, Message * msg) {
#if CC_ALG != CALVIN
	if (!ISSERVERN(msg->return_node_id)) {
		row_t * row = this->last_row;
		pthread_mutex_lock(row->cache_node->locker);
		row_cache.insert_to_tail(this, row, hash_key_or_wh_to_cache(row));
		bool valid = ATOM_CAS(row->cache_node->is_cache_required, true, false);
		assert(valid);
		WaitTxnNode * node = row->cache_node->wait_list_head;
		while (node) {
			work_queue.enqueue(thd_id,Message::create_message(node->txn_man,RSTO_RSP),false);
			LIST_REMOVE_HT(node, row->cache_node->wait_list_head, row->cache_node->wait_list_tail);
			WaitTxnNode * old_node = node;
			node = node->next;
			mem_allocator.free(old_node, sizeof(WaitTxnNode));
		}
		pthread_mutex_unlock(row->cache_node->locker);
	}
#else
	if (!ISSERVERN(msg->return_node_id)) {
		for (auto it = row_wait_for_cache->begin(); it != row_wait_for_cache->end(); ) {
			if (!it->second) {
				++it;
				continue;
			}
			row_t * row = it->first;
			pthread_mutex_lock(row->cache_node->locker);
			row_cache.insert_to_tail(this, row, hash_key_or_wh_to_cache(row));
			row->cache_node->use_cache_num++;
			// printf("batch=%ld, txn=%ld get row=%ld cache after require\n", get_batch_id(), get_txn_id(), row->get_primary_key());
			bool valid = ATOM_CAS(row->cache_node->is_cache_required, true, false);
			assert(valid);
			WaitTxnNode * node = row->cache_node->wait_list_head;
			while (node) {
				Message * msg = Message::create_message(node->txn_man, RSTO_RSP);
				RStorageResponseMessage * rsto_rsp_msg = (RStorageResponseMessage *) msg;
				rsto_rsp_msg->row = row;
				// printf("batch=%ld, txn=%ld send rsp to batch=%ld, txn=%ld\n", txn->batch_id, txn->txn_id, node->txn_man->txn->batch_id, node->txn_man->txn->txn_id);
				work_queue.enqueue(thd_id, msg, false);
				LIST_REMOVE_HT(node, row->cache_node->wait_list_head, row->cache_node->wait_list_tail);
				WaitTxnNode * old_node = node;
				node = node->next;
				mem_allocator.free(old_node, sizeof(WaitTxnNode));
				row->cache_node->use_cache_num++;
			}
			pthread_mutex_unlock(row->cache_node->locker);
			it = row_wait_for_cache->erase(it);
		}
	}
	else {
		RStorageResponseMessage * rsto_rsp_msg = (RStorageResponseMessage *) msg;
		for (auto it = row_wait_for_cache->begin(); it != row_wait_for_cache->end(); ) {
			row_t * row = it->first;
			if (rsto_rsp_msg->row == row) {
				pthread_mutex_lock(row->cache_node->locker);
				if (row->cache_node->use_cache_num >= 0) {
					// row->cache_node->use_cache_num++;
					row_cache.move_to_tail(this, row->cache_node, hash_key_or_wh_to_cache(row));
					it = row_wait_for_cache->erase(it);
					// printf("batch=%ld, txn=%ld get row=%ld cache after wait\n", get_batch_id(), get_txn_id(), row->get_primary_key());
				} else {
					++it;
				}
				pthread_mutex_unlock(row->cache_node->locker);
			} else {
				++it;
			}
		}
	}
	if (!cache_not_ready && row_wait_for_cache->size() == 0) {
		// printf("batch=%ld, txn=%ld cache_ready\n", get_batch_id(), get_txn_id());
		cache_ready = true;
	}
#endif
}

void TxnManager::get_cache(row_t *&row) {
  pthread_mutex_lock(row->cache_node->locker);
  if (row->cache_node->use_cache_num >= 0) {
    row->cache_node->use_cache_num++;
    row_cache.move_to_tail(this, row->cache_node, hash_key_or_wh_to_cache(row));
    pthread_mutex_unlock(row->cache_node->locker);
    // printf("batch=%ld, txn=%ld get row=%ld\n", get_batch_id(), get_txn_id(), row->get_primary_key());
  } else {
    if (row->cache_node->is_cache_required == false) {
      row->cache_node->is_cache_required = true;
      pthread_mutex_unlock(row->cache_node->locker);
      need_require_cache_num++;
      row_wait_for_cache->emplace_back(row, true);
      // printf("batch=%ld, txn=%ld require row=%ld cache\n", get_batch_id(), get_txn_id(), row->get_primary_key());
    } else {
      WaitTxnNode *node = (WaitTxnNode *)mem_allocator.alloc(sizeof(WaitTxnNode));
      node->txn_man = this;
      LIST_PUT_TAIL(row->cache_node->wait_list_head, row->cache_node->wait_list_tail, node);
      pthread_mutex_unlock(row->cache_node->locker);
      row_wait_for_cache->emplace_back(row, false);
      // printf("batch=%ld, txn=%ld wait row=%ld cache\n", get_batch_id(), get_txn_id(), row->get_primary_key());
    }
  }
}

void TxnManager::insert_row(row_t * row, INDEX * index) {
	if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) return;
	assert(txn->insert_rows.size() < MAX_ROW_PER_TXN);
	txn->insert_rows.add(std::pair<row_t*, INDEX*>(row, index));
}

void TxnManager::delete_row(row_t * row, INDEX * index) {
	if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) return;
	assert(txn->delete_rows.size() < MAX_ROW_PER_TXN);
	txn->delete_rows.add(std::pair<row_t*, INDEX*>(row, index));
}

itemid_t *TxnManager::index_read(INDEX *index, idx_key_t key, int part_id) {
	uint64_t starttime = get_sys_clock();

	itemid_t * item;
	index->index_read(key, item, part_id, get_thd_id());

	uint64_t t = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_index_time, t);
	//txn_time_idx += t;

	return item;
}

itemid_t *TxnManager::index_read(INDEX *index, idx_key_t key, int part_id, int count) {
	uint64_t starttime = get_sys_clock();

	itemid_t * item;
	index->index_read(key, count, item, part_id);

	uint64_t t = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_index_time, t);
	//txn_time_idx += t;

	return item;
}

itemid_t **TxnManager::index_read_all(INDEX * index, idx_key_t key, int part_id, int &count) {
	uint64_t starttime = get_sys_clock();

	itemid_t ** items;
	index->index_read_all(key, items, count, part_id);

	uint64_t t = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_index_time, t);

	return items;
}

RC TxnManager::validate_c() {
	RC rc = RCOK;
#if CC_ALG == HDCC
	rc = validate_cont();
    if(rc == RCOK) {
		  commit_timestamp = get_txn_id();
		  DEBUG("Validate success: %ld, cts: %ld \n", get_txn_id(), commit_timestamp);
    }
#endif
	return rc;
}


RC TxnManager::validate() {
#if MODE != NORMAL_MODE
	return RCOK;
#endif
	if (CC_ALG != OCC && CC_ALG != MAAT  && CC_ALG != WOOKONG &&
			CC_ALG != TICTOC && CC_ALG != BOCC && CC_ALG != FOCC && CC_ALG != WSI &&
			CC_ALG != SSI && CC_ALG != DLI_BASE && CC_ALG != DLI_OCC &&
			CC_ALG != DLI_MVCC_OCC && CC_ALG != DTA && CC_ALG != DLI_DTA &&
			CC_ALG != DLI_DTA2 && CC_ALG != DLI_DTA3 && CC_ALG != DLI_MVCC && CC_ALG != SILO &&
			CC_ALG != HDCC && CC_ALG != SNAPPER) {
		return RCOK;
	}
	RC rc = RCOK;
	uint64_t starttime = get_sys_clock();
#if CC_ALG == HDCC
	if (is_multi_part()) {
		rc = validate_lock();
	} else {
		rc = validate_once();
		if(rc == RCOK) {
			commit_timestamp = get_txn_id();
			DEBUG("Validate success: %ld, cts: %ld \n", get_txn_id(), commit_timestamp);
		}
	}
	
	// if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
	// 	commit_timestamp = get_txn_id();
	// 	DEBUG("Validate success: %ld, cts: %ld \n", get_txn_id(), commit_timestamp);
	// }
#elif CC_ALG == SNAPPER
	rc = validate_snapper();
#else
	if (CC_ALG == OCC && rc == RCOK) rc = occ_man.validate(this);
	if(CC_ALG == BOCC && rc == RCOK) rc = bocc_man.validate(this);
	if(CC_ALG == FOCC && rc == RCOK) rc = focc_man.validate(this);
	if(CC_ALG == SSI && rc == RCOK) rc = ssi_man.validate(this);
	if(CC_ALG == WSI && rc == RCOK) rc = wsi_man.validate(this);
	if(CC_ALG == MAAT  && rc == RCOK) {
		rc = maat_man.validate(this);
		// Note: home node must be last to validate
		if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
			rc = maat_man.find_bound(this);
		}
	}
	if(CC_ALG == TICTOC  && rc == RCOK) {
		rc = tictoc_man.validate(this);
		// Note: home node must be last to validate
		// if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
		//   rc = tictoc_man.find_bound(this);
		// }
	}
	if ((CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 ||
			 CC_ALG == DLI_MVCC) &&
			rc == RCOK) {
		rc = dli_man.validate(this);
		if (IS_LOCAL(get_txn_id()) && rc == RCOK) {
#if CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
			rc = dli_man.find_bound(this);
#else
			set_commit_timestamp(glob_manager.get_ts(get_thd_id()));
#endif
		}
	}
	if(CC_ALG == WOOKONG  && rc == RCOK) {
		rc = wkdb_man.validate(this);
		// Note: home node must be last to validate
		if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
			rc = wkdb_man.find_bound(this);
			if (g_ts_alloc == LTS_HLC_CLOCK) {
				// hlc_ts.update_with_cts(get_commit_timestamp());
			}
		}
	}
	if ((CC_ALG == DTA) && rc == RCOK) {
		rc = dta_man.validate(this);
		// Note: home node must be last to validate
		if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
				rc = dta_man.find_bound(this);
		}
	}
#if CC_ALG == SILO
  if(CC_ALG == SILO && rc == RCOK) {
    rc = validate_silo();
    if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
      _cur_tid ++;
      commit_timestamp = _cur_tid;
      DEBUG("Validate success: %ld, cts: %ld \n", get_txn_id(), commit_timestamp);
    }
  }
#endif
#endif
	INC_STATS(get_thd_id(),txn_validate_time,get_sys_clock() - starttime);
	INC_STATS(get_thd_id(),trans_validate_time,get_sys_clock() - starttime);
  INC_STATS(get_thd_id(),trans_validate_count, 1);
	return rc;
}

RC TxnManager::send_remote_reads() {
	assert(CC_ALG == CALVIN || CC_ALG == HDCC || CC_ALG == SNAPPER);
#if !YCSB_ABORT_MODE && WORKLOAD == YCSB
	return RCOK;
#endif
	assert(query->active_nodes.size() == g_node_cnt);
	for(uint64_t i = 0; i < query->active_nodes.size(); i++) {
		if (i == g_node_id) continue;
	if(query->active_nodes[i] == 1) {
		DEBUG("(%ld,%ld) send_remote_read to %ld\n",get_txn_id(),get_batch_id(),i);
		msg_queue.enqueue(get_thd_id(),Message::create_message(this,RFWD),i);
	}
	}
	return RCOK;

}

bool TxnManager::calvin_exec_phase_done() {
	bool ready =  (phase == CALVIN_DONE) && (get_rc() != WAIT);
	if(ready) {
	DEBUG("(%ld,%ld) calvin exec phase done!\n",txn->txn_id,txn->batch_id);
	}
	return ready;
}

bool TxnManager::calvin_collect_phase_done() {
	bool ready =  (phase == CALVIN_COLLECT_RD) && (get_rsp_cnt() == calvin_expected_rsp_cnt);
	if(ready) {
	DEBUG("(%ld,%ld) calvin collect phase done!\n",txn->txn_id,txn->batch_id);
	}
	return ready;
}

void TxnManager::release_locks(RC rc) {
	uint64_t starttime = get_sys_clock();

	cleanup(rc);

	uint64_t timespan = (get_sys_clock() - starttime);
	INC_STATS(get_thd_id(), txn_cleanup_time,  timespan);
}

void TxnManager::next_batch(row_t * row) {
	if (row->versions[row->cur_ver].batch_id != txn->batch_id) {
      row->versions[row->cur_ver].valid_until = txn->batch_id;
	  uint64_t old_ver = row->cur_ver;
      row->cur_ver = (row->cur_ver + 1) % g_version_cnt;
	  memcpy(row->versions[row->cur_ver].data, row->versions[old_ver].data, row->get_schema()->get_tuple_size());
      row->versions[row->cur_ver].batch_id = txn->batch_id;
      row->versions[row->cur_ver].valid_until = UINT64_MAX;
      row->data = row->versions[row->cur_ver].data;
    }
}
