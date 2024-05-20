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
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"
#include "msg_queue.h"
#include "message.h"
#if CC_ALG == HDCC
#include "row_hdcc.h"
#endif
// WhiteBear: 事务执行
void YCSBTxnManager::init(uint64_t thd_id, Workload * h_wl) {
	TxnManager::init(thd_id, h_wl);
	_wl = (YCSBWorkload *) h_wl;
  reset();
}

void YCSBTxnManager::reset() {
  state = YCSB_0;
  next_record_id = 0;
	TxnManager::reset();
}
// WhiteBear: 一定是需要改的，根据线程确定数据分区，对事务分区内的数据项加锁，
RC YCSBTxnManager::acquire_locks() {
  uint64_t starttime = get_sys_clock();
  assert(CC_ALG == CALVIN || CC_ALG == HDCC || CC_ALG == SNAPPER);
#if CALVIN_W
  uint64_t now_sched_id = get_thd_id() % g_sched_thread_cnt;
	lockers_has_watched[now_sched_id] = true;
	if(this->acquired_lock_num == this->sum_lock_num &&
		this->acquired_lock_num != 0)
	{
    printf("batch_id = %ld , txn_id = %ld , acquired_lock_num = %d , sum_lock_num = %d , rc = 3 .\n",
  get_batch_id(),get_txn_id(),acquired_lock_num,sum_lock_num);
		return RC::WAIT;
	}
#endif
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  locking_done = false;
  RC rc = WAIT;
  incr_lr();
  assert(ycsb_query->requests.size() == g_req_per_query);
  assert(phase == CALVIN_RW_ANALYSIS);
#if CALVIN_W
  // uint32_t now_sched_id = get_thd_id() % g_sched_thread_cnt;
  // lockers_has_watched[now_sched_id] = true;
  bool belong_this_sched = false;
  bool need_count_sum_lock_num = false;
  if(this->sum_lock_num == 0) need_count_sum_lock_num = true;
  assert(ycsb_query->requests.size() > 0);            
  //  后面没有对空事务进行处理的逻辑
#endif
	for (uint32_t rid = 0; rid < ycsb_query->requests.size(); rid ++) {
		ycsb_request * req = ycsb_query->requests[rid];
		uint64_t part_id = _wl->key_to_part( req->key );  
    //  确定当前数据项属于哪个节点，未修改
    DEBUG("LK Acquire (%ld,%ld) %d,%ld -> %ld\n", get_txn_id(), get_batch_id(), req->acctype,
          req->key, GET_NODE_ID(part_id));
    if (GET_NODE_ID(part_id) != g_node_id) continue;  
    //  如果当前数据项不属于该节点处理下一个数据项
#if CALVIN_W
    if(need_count_sum_lock_num) this->sum_lock_num++;
    uint32_t target_sched_id = belong_sched_id( req->key );
    if(target_sched_id != now_sched_id) continue;     
    //  如果当前数据项不属于当前scheduler(locker)则跳过
    belong_this_sched = true;
#endif
		INDEX * index = _wl->the_index;
		itemid_t * item;
		item = index_read(index, req->key, part_id);
		row_t * row = ((row_t *)item->location);
		RC rc2 = get_lock(row,req->acctype);
    if(rc2 != RCOK) {
      rc = rc2;
    }
#if CALVIN_W
    this->acquired_lock_num++;
#endif
	}
#if CALVIN_W
  if(!belong_this_sched && ycsb_query->requests.size() > 0)
  {
    rc = RC::WAIT;          
    // 该事务与当前scheduler无关，则返回WAIT
  }
  if(decr_lr() == 0 && this->acquired_lock_num == this->sum_lock_num)
  {                         
    //  当前事务在当前节点所有数据项都已经获得了锁
    if (ATOM_CAS(lock_ready, false, true)) rc = RCOK;
  }
  else
  {
    rc = RC::WAIT;
  }
#else
  if(decr_lr() == 0) {
    if (ATOM_CAS(lock_ready, false, true)) rc = RCOK;
  }
#endif
  txn_stats.wait_starttime = get_sys_clock();
  INC_STATS(get_thd_id(),calvin_sched_time,get_sys_clock() - starttime);
  locking_done = true;
#if CALVIN_W
  printf("batch_id = %ld , txn_id = %ld , acquired_lock_num = %d , sum_lock_num = %d , rc = %d .\n",
  get_batch_id(),get_txn_id(),acquired_lock_num,sum_lock_num,rc);
#else
  printf("batch_id = %ld , txn_id = %ld , rc = %d .\n",
  get_batch_id(),get_txn_id(),rc);
#endif
  return rc;  //对于CALVIN来说，要么返回WAIT，要么返回RCOK
}

#if CC_ALG == SNAPPER
void YCSBTxnManager::get_read_write_set() {
  uint64_t starttime = get_sys_clock();
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  assert(ycsb_query->requests.size() == g_req_per_query);
  assert(phase == CALVIN_RW_ANALYSIS);
	for (uint32_t rid = 0; rid < ycsb_query->requests.size(); rid ++) {
		ycsb_request * req = ycsb_query->requests[rid];
		uint64_t part_id = _wl->key_to_part( req->key );
    DEBUG("LK Acquire (%ld,%ld) %d,%ld -> %ld\n", get_txn_id(), get_batch_id(), req->acctype,
          req->key, GET_NODE_ID(part_id));
    if (GET_NODE_ID(part_id) != g_node_id) continue;
		INDEX * index = _wl->the_index;
		itemid_t * item;
		item = index_read(index, req->key, part_id);
		row_t * row = ((row_t *)item->location);
    read_write_set.emplace_back(row, req->acctype);
    wait_for_locks.insert(row);
  }
  INC_STATS(get_thd_id(),calvin_sched_time,get_sys_clock() - starttime);
}

RC YCSBTxnManager::acquire_lock(row_t * row, access_t acctype) {
  RC rc = WAIT;
  // uint64_t starttime = get_sys_clock();
  RC rc2 = get_lock(row,acctype);
  if (rc2 == RCOK) {
    if (wait_for_locks.empty()) {
        if (ATOM_CAS(lock_ready, false, true)) {
          rc = RCOK;
        }
    }
  }
  return rc;
}
#endif

RC YCSBTxnManager::run_txn() {
  RC rc = RCOK;
  assert(CC_ALG != CALVIN);

  if(IS_LOCAL(txn->txn_id) && state == YCSB_0 && next_record_id == 0) {
    DEBUG("Running txn %ld\n",txn->txn_id);
    //query->print();
    query->partitions_touched.add_unique(GET_PART_ID(0,g_node_id));
  }

  uint64_t starttime = get_sys_clock();

  while(rc == RCOK && !is_done()) {
    rc = run_txn_state();
  }

  uint64_t curr_time = get_sys_clock();
  txn_stats.process_time += curr_time - starttime;
  txn_stats.process_time_short += curr_time - starttime;
  txn_stats.wait_starttime = get_sys_clock();

  if(IS_LOCAL(get_txn_id())) {
  #if DETERMINISTIC_ABORT_MODE
    if (query->isDeterministicAbort && rc == RCOK && is_done()) {
      query->isDeterministicAbort = false;
      rc = Abort;
      INC_STATS(get_thd_id(), deterministic_abort_cnt_silo, 1);
    }
  #endif
    if(is_done() && rc == RCOK)
      rc = start_commit();
    else if(rc == Abort)
      rc = start_abort();
  } else if(rc == Abort){
    rc = abort();
  }

  return rc;

}

RC YCSBTxnManager::run_txn_post_wait() {
  uint64_t starttime = get_sys_clock();
  get_row_post_wait(row);
  uint64_t curr_time = get_sys_clock();
  txn_stats.process_time += curr_time - starttime;
  txn_stats.process_time_short += curr_time - starttime;
  next_ycsb_state();
  INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - curr_time);
  return RCOK;
}

bool YCSBTxnManager::is_done() { return next_record_id >= ((YCSBQuery*)query)->requests.size(); }

void YCSBTxnManager::next_ycsb_state() {
  switch(state) {
    case YCSB_0:
      state = YCSB_1;
      break;
    case YCSB_1:
      next_record_id++;
      if(send_RQRY_RSP || !IS_LOCAL(txn->txn_id) || !is_done()) {
        state = YCSB_0;
      } else {
        state = YCSB_FIN;
      }
      break;
    case YCSB_FIN:
      break;
    default:
      assert(false);
  }
}

bool YCSBTxnManager::is_local_request(uint64_t idx) {
  return GET_NODE_ID(_wl->key_to_part(((YCSBQuery*)query)->requests[idx]->key)) == g_node_id;
}

RC YCSBTxnManager::send_remote_request() {
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  uint64_t dest_node_id = GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
  ycsb_query->partitions_touched.add_unique(GET_PART_ID(0,dest_node_id));
  DEBUG("ycsb send remote request %ld, %ld\n",txn->txn_id,txn->batch_id);
  msg_queue.enqueue(get_thd_id(),Message::create_message(this,RQRY),dest_node_id);
  txn_stats.trans_process_network_start_time = get_sys_clock();
  return WAIT_REM;
}

#if CC_ALG == ARIA
RC YCSBTxnManager::send_remote_read_requests() {
  for (uint64_t i = 0; i < g_node_cnt; i++) {
    if (i == g_node_id) continue;
    if (read_set[i].size() == 0) continue;
    next_send_node = i;
    YCSBQueryMessage * msg = (YCSBQueryMessage*) Message::create_message(this,RQRY);
    // printf("txn: %ld send remote read to %ld\n", txn->txn_id, i);
    msg->aria_phase = ARIA_READ;
    msg_queue.enqueue(get_thd_id(),msg,i);
    participants_cnt++;
  }
  txn_stats.trans_process_network_start_time = get_sys_clock();
  return participants_cnt == 0? RCOK : WAIT_REM;
}

RC YCSBTxnManager::send_remote_write_requests() {
  for (uint64_t i = 0; i < g_node_cnt; i++) {
    if (i == g_node_id) continue;
    if (read_set[i].size() == 0 && write_set[i].size() == 0) continue;
    next_send_node = i;
    YCSBQueryMessage * msg = (YCSBQueryMessage*) Message::create_message(this,RQRY);
    // printf("txn: %ld send remote write to %ld\n", txn->txn_id, i);
    msg->aria_phase = ARIA_RESERVATION;
    msg_queue.enqueue(get_thd_id(),msg,i);
    participants_cnt++;
  }
  txn_stats.trans_process_network_start_time = get_sys_clock();
  return participants_cnt == 0? RCOK : WAIT_REM;
}

RC YCSBTxnManager::process_aria_remote(ARIA_PHASE aria_phase) {
  RC rc = RCOK;
  YCSBQuery * ycsb_query = (YCSBQuery*) query;
  switch (aria_phase)
  {
  case ARIA_READ:
    for (uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
      ycsb_request * request = ycsb_query->requests[i];
      rc = run_ycsb_0(request,row);
      assert(rc == RCOK);
      rc = run_ycsb_1(request->acctype,row);
      assert(rc == RCOK);
    }
    // printf("txn: %ld remote read rc: %d\n", txn->txn_id, rc);
    break;
  case ARIA_RESERVATION:
    for (uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
      if (ycsb_query->requests[i]->acctype == WR) {
        ycsb_request * request = ycsb_query->requests[i];
        rc = run_ycsb_0(request,row);
        assert(rc == RCOK);
        rc = run_ycsb_1(request->acctype,row);
        assert(rc == RCOK);
      }
    }
    rc = reserve();
    // printf("txn: %ld remote reserve rc: %d\n", txn->txn_id, rc);
    if (rc == Abort) {
      txn->rc = Abort;
    }
    break;
  case ARIA_CHECK:
    assert(txn->rc != Abort);
    rc = check();
    if (rc == Abort) {
      txn->rc = Abort;
    }
    // printf("txn: %ld remote check rc: %d\n", txn->txn_id, rc);
    break;
  default:
    assert(false);
    break;
  }
  return rc;
}
#endif

#if CC_ALG != ARIA
void YCSBTxnManager::copy_remote_requests(YCSBQueryMessage * msg) {
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  //msg->requests.init(ycsb_query->requests.size());
  uint64_t dest_node_id = GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
#if ONE_NODE_RECIEVE == 1 && defined(NO_REMOTE) && LESS_DIS_NUM == 10
  while (next_record_id < ycsb_query->requests.size() && GET_NODE_ID(ycsb_query->requests[next_record_id]->key) == dest_node_id) {
#else
  while (next_record_id < ycsb_query->requests.size() && !is_local_request(next_record_id) &&
         GET_NODE_ID(ycsb_query->requests[next_record_id]->key) == dest_node_id) {
#endif
    YCSBQuery::copy_request_to_msg(ycsb_query,msg,next_record_id++);
  }
}
#else
void YCSBTxnManager::copy_remote_requests(YCSBQueryMessage * msg) {
  if (simulation->aria_phase == ARIA_READ) {
    for (uint64_t i = 0; i < read_set[next_send_node].size(); i++) {
      msg->requests.add(read_set[next_send_node][i]);
    }
  } else {
    for (uint64_t i = 0; i < write_set[next_send_node].size(); i++) {
      msg->requests.add(write_set[next_send_node][i]);
    }
  }
}
#endif

RC YCSBTxnManager::run_txn_state() {
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
	ycsb_request * req = ycsb_query->requests[next_record_id];
	uint64_t part_id = _wl->key_to_part( req->key );
  bool loc = GET_NODE_ID(part_id) == g_node_id;

	RC rc = RCOK;

	switch (state) {
		case YCSB_0 :
      if(loc) {
        rc = run_ycsb_0(req,row);
      } else {
        rc = send_remote_request();

      }

      break;
		case YCSB_1 :
      rc = run_ycsb_1(req->acctype,row);
      break;
    case YCSB_FIN :
      state = YCSB_FIN;
      break;
    default:
			assert(false);
  }

  if (rc == RCOK) next_ycsb_state();

  return rc;
}

RC YCSBTxnManager::run_ycsb_0(ycsb_request * req,row_t *& row_local) {
  uint64_t starttime = get_sys_clock();
  RC rc = RCOK;
  int part_id = _wl->key_to_part( req->key );
  access_t type = req->acctype;
  itemid_t * m_item;
  INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
  m_item = index_read(_wl->the_index, req->key, part_id);
  starttime = get_sys_clock();
  row_t * row = ((row_t *)m_item->location);

  INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
  rc = get_row(row, type,row_local);
  return rc;

}

// [ ]:因为目前YCSB的写入并没有用req中的随机值而是写了固定值0，日志里也暂时写成固定值了
RC YCSBTxnManager::run_ycsb_1(access_t acctype, row_t * row_local) {
  uint64_t starttime = get_sys_clock();
  if (acctype == RD || acctype == SCAN) {
    int fid = 0;
		char * data = row_local->get_data();
		uint64_t fval __attribute__ ((unused));
    fval = *(uint64_t *)(&data[fid * 100]);
#if ISOLATION_LEVEL == READ_COMMITTED || ISOLATION_LEVEL == READ_UNCOMMITTED
    // Release lock after read
    release_last_row_lock();
#endif

  } else {
    assert(acctype == WR);
		int fid = 0;
	  char * data = row_local->get_data();
	  *(uint64_t *)(&data[fid * 100]) = 0;
#if CC_ALG == HDCC
    if (algo == CALVIN) {
      row_local->manager->_tid = txn->txn_id;
      row_local->manager->max_calvin_write_tid = txn->txn_id;
      row_local->manager->max_calvin_write_bid = txn->batch_id;
    }
#endif
#if YCSB_ABORT_MODE
    if (data[0] == 'a') return RCOK;
#endif

#if ISOLATION_LEVEL == READ_UNCOMMITTED
    // Release lock after write
    release_last_row_lock();
#endif
#if SINGLE_WRITE_NODE
#if YCSB_SHORT_LOG
    char for_log = 0;
    LogRecord * log_record = logger.createRecord(get_txn_id(), L_UPDATE, row_local->get_table()->get_table_id(), row_local->get_primary_key(), fid, 1, &for_log, &for_log);
#else
    LogRecord * log_record = logger.createRecord(get_txn_id(), L_UPDATE, row_local->get_table()->get_table_id(), row_local->get_primary_key(), fid, 100, data, data);
#endif
    log_records.push_back(log_record);
#endif
  }
  INC_STATS(get_thd_id(),trans_benchmark_compute_time,get_sys_clock() - starttime);
  return RCOK;
}
RC YCSBTxnManager::run_calvin_txn() {
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  DEBUG("(%ld,%ld) Run calvin txn\n",txn->txn_id,txn->batch_id);
  while(!calvin_exec_phase_done() && rc == RCOK) {
    DEBUG("(%ld,%ld) phase %d\n",txn->txn_id,txn->batch_id,this->phase);
    switch(this->phase) {
      case CALVIN_RW_ANALYSIS:

        // Phase 1: Read/write set analysis
        calvin_expected_rsp_cnt = ycsb_query->get_participants(_wl);
#if YCSB_ABORT_MODE
        if(query->participant_nodes[g_node_id] == 1) {
          calvin_expected_rsp_cnt--;
        }
#else
        calvin_expected_rsp_cnt = 0;
#endif
        DEBUG("(%ld,%ld) expects %d responses;\n", txn->txn_id, txn->batch_id,
              calvin_expected_rsp_cnt);

        this->phase = CALVIN_LOC_RD;
        break;
      case CALVIN_LOC_RD:
        // Phase 2: Perform local reads
        DEBUG("(%ld,%ld) local reads\n",txn->txn_id,txn->batch_id);
        rc = run_ycsb();
        //release_read_locks(query);

        this->phase = CALVIN_SERVE_RD;
        break;
      case CALVIN_SERVE_RD:
        // Phase 3: Serve remote reads
        // If there is any abort logic, relevant reads need to be sent to all active nodes...
        if(query->participant_nodes[g_node_id] == 1) {
          rc = send_remote_reads();
        }
        if(query->active_nodes[g_node_id] == 1) {
          this->phase = CALVIN_COLLECT_RD;
          if(calvin_collect_phase_done()) {
            rc = RCOK;
          } else {
            DEBUG("(%ld,%ld) wait in collect phase; %d / %d rfwds received\n", txn->txn_id,
                  txn->batch_id, rsp_cnt, calvin_expected_rsp_cnt);
            rc = WAIT;
          }
        } else { // Done
          rc = RCOK;
          this->phase = CALVIN_DONE;
        }

        break;
      case CALVIN_COLLECT_RD:
        // Phase 4: Collect remote reads
        this->phase = CALVIN_EXEC_WR;
        break;
      case CALVIN_EXEC_WR:
        // Phase 5: Execute transaction / perform local writes
        DEBUG("(%ld,%ld) execute writes\n",txn->txn_id,txn->batch_id);
        rc = run_ycsb();
        this->phase = CALVIN_DONE;
        break;
      default:
        assert(false);
    }
  }
  uint64_t curr_time = get_sys_clock();
  txn_stats.process_time += curr_time - starttime;
  txn_stats.process_time_short += curr_time - starttime;
  txn_stats.wait_starttime = get_sys_clock();
  return rc;
}

#if CC_ALG == ARIA
RC YCSBTxnManager::run_aria_txn() {
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  DEBUG("(%ld,%ld) Run aria txn\n",txn->txn_id,txn->batch_id);
  switch (simulation->aria_phase)
  {
  case ARIA_READ:
    //analyze read/write set, do local read if key is equal to g_node_id or send remote read to remote node
    assert(ycsb_query->requests.size() == g_req_per_query);
    for (uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
      ycsb_request * request = ycsb_query->requests[i];
      uint64_t target_node = GET_NODE_ID(_wl->key_to_part(request->key));
      if (request->acctype == RD) {
        if (target_node == g_node_id) {
          rc = run_ycsb_0(request,row);
          assert(rc == RCOK);
          rc = run_ycsb_1(request->acctype,row);
          assert(rc == RCOK);
          read_set[target_node].emplace_back(request);
        } else {
          read_set[target_node].emplace_back(request);
        }
      } else {
        write_set[target_node].emplace_back(request);
      }
      ycsb_query->partitions_touched.add_unique(target_node);
    }

    rc = send_remote_read_requests();
    // if (rc == WAIT_REM) {
    //   printf("txn: %ld wait for remote read\n", txn->txn_id);
    // }
    assert(rc == RCOK || rc == WAIT_REM);

    assert(aria_phase == ARIA_READ);
    aria_phase = (ARIA_PHASE) (aria_phase + 1);
    assert(simulation->aria_phase == ARIA_READ);
    // printf("txn: %ld read phase rc: %d\n", txn->txn_id, rc);

    break;
  case ARIA_RESERVATION:
    // write to row copy and do reservation
    for (uint64_t i = 0; i < write_set[g_node_id].size(); i++) {
      ycsb_request * request = write_set[g_node_id][i];
      rc = run_ycsb_0(request,row);
      assert(rc == RCOK);
      rc = run_ycsb_1(request->acctype,row);
      assert(rc == RCOK);
    }

    rc = reserve();
    if (rc == Abort) {
      txn->rc = Abort;
    }

    rc = send_remote_write_requests();
    // if (rc == WAIT_REM) {
    //   printf("txn: %ld wait for remote write\n", txn->txn_id);
    // }
    assert(rc == RCOK || rc == Abort || rc == WAIT_REM);

    assert(aria_phase == ARIA_RESERVATION);
    aria_phase = (ARIA_PHASE) (aria_phase + 1);
    assert(simulation->aria_phase == ARIA_RESERVATION);
    // printf("txn: %ld reserve phase rc: %d\n", txn->txn_id, rc);

// #if true
//     if (rc == Abort) {
//       abort();
//     } else if (rc == RCOK) {
//       commit();
//     }
// #endif

    break;
  case ARIA_CHECK:
    // If we already known that txn should be aborted, do nothing in this phase, otherwise, check if the txn should be aborted
    if (txn->rc == Abort) {
    } else {
      //send check request to all participants like 2PC
      send_prepare_messages();

      rc = check();
      if (rc == Abort) {
        txn->rc = Abort;
      }

      if (rsp_cnt != 0) {
        rc = WAIT_REM;
      }
    }

    assert(aria_phase == ARIA_CHECK);
    aria_phase = (ARIA_PHASE) (aria_phase + 1);
    assert(simulation->aria_phase == ARIA_CHECK);
    // printf("txn: %ld check phase rc: %d\n", txn->txn_id, rc);
    break;
  case ARIA_COMMIT:
    send_finish_messages();

    if (txn->rc == Abort) {
      abort();
    } else {
      commit();
    }

    if (rsp_cnt != 0) {
      rc = WAIT_REM;
    }

    assert(aria_phase == ARIA_COMMIT);
    aria_phase = (ARIA_PHASE) (aria_phase + 1);
    assert(simulation->aria_phase == ARIA_COMMIT);
    // printf("txn: %ld commit phase rc: %d\n", txn->txn_id, rc);
    break;
  default:
    break;
  }
  uint64_t curr_time = get_sys_clock();
  txn_stats.process_time += curr_time - starttime;
  txn_stats.process_time_short += curr_time - starttime;
  txn_stats.wait_starttime = get_sys_clock();
  INC_STATS(get_thd_id(),worker_activate_txn_time,curr_time - starttime);
  return rc;
}
#endif

RC YCSBTxnManager::run_ycsb() {
  RC rc = RCOK;
  assert(CC_ALG == CALVIN || CC_ALG == HDCC || CC_ALG == SNAPPER);
  YCSBQuery* ycsb_query = (YCSBQuery*) query;

  for (uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
	  ycsb_request * req = ycsb_query->requests[i];
    if (this->phase == CALVIN_LOC_RD && req->acctype == WR) continue;
    if (this->phase == CALVIN_EXEC_WR && req->acctype == RD) continue;

		uint64_t part_id = _wl->key_to_part( req->key );
    bool loc = GET_NODE_ID(part_id) == g_node_id;

    if (!loc) continue;

    rc = run_ycsb_0(req,row);
    assert(rc == RCOK);

    rc = run_ycsb_1(req->acctype,row);
    assert(rc == RCOK);
  }
  return rc;

}

