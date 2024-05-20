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

#include "mem_alloc.h"
#include "query.h"
#include "ycsb_query.h"
#include "ycsb.h"
#include "tpcc_query.h"
#include "tpcc.h"
#include "pps_query.h"
#include "pps.h"
#include "global.h"
#include "message.h"
#include "maat.h"
#include "dta.h"
#include "da.h"
#include "da_query.h"
#include "wkdb.h"
#include "tictoc.h"

std::vector<Message*> * Message::create_messages(char * buf) {
  std::vector<Message*> * all_msgs = new std::vector<Message*>;
  char * data = buf;
	uint64_t ptr = 0;
  uint64_t starttime = 0;
  uint32_t dest_id;
  uint32_t return_id;
  uint32_t txn_cnt;
  COPY_VAL(dest_id,data,ptr);
  COPY_VAL(return_id,data,ptr);
  COPY_VAL(txn_cnt,data,ptr);
  COPY_VAL(starttime,data,ptr);
  if (return_id < NODE_CNT) {
    INC_STATS(0,trans_network_send,starttime);
    INC_STATS(0,trans_network_recv,get_sys_clock());
    INC_STATS(0,trans_network_wait,get_sys_clock()-starttime);
  }
#if ONE_NODE_RECIEVE == 1 && defined(NO_REMOTE) && LESS_DIS_NUM == 10
#else
  assert(dest_id == g_node_id);
  assert(return_id != g_node_id);
#endif
  assert(ISCLIENTN(return_id) || ISSERVERN(return_id) || ISSTORAGEN(return_id));
  while(txn_cnt > 0) {
    Message * msg = create_message(&data[ptr]);
    msg->return_node_id = return_id;
    ptr += msg->get_size();
    all_msgs->push_back(msg);
    --txn_cnt;
  }
  return all_msgs;
}

Message * Message::create_message(char * buf) {
 RemReqType rtype = NO_MSG;
 uint64_t ptr = 0;
 COPY_VAL(rtype,buf,ptr);
 Message * msg = create_message(rtype);
  //printf("buffer is:%s\n",buf);
  //printf("msg:%lu:%lu %lu %lu\n",((DAQueryMessage*)msg)->seq_id,((DAQueryMessage*)msg)->state,((DAQueryMessage*)msg)->next_state,((DAQueryMessage*)msg)->last_state);
  fflush(stdout);
 msg->copy_from_buf(buf);
 return msg;
}

Message * Message::create_message(TxnManager * txn, RemReqType rtype) {
 Message * msg = create_message(rtype);
 msg->mcopy_from_txn(txn);
 msg->copy_from_txn(txn);

 // copy latency here
 msg->lat_work_queue_time = txn->txn_stats.work_queue_time_short;
 msg->lat_msg_queue_time = txn->txn_stats.msg_queue_time_short;
 msg->lat_cc_block_time = txn->txn_stats.cc_block_time_short;
 msg->lat_cc_time = txn->txn_stats.cc_time_short;
 msg->lat_process_time = txn->txn_stats.process_time_short;
 msg->lat_network_time = txn->txn_stats.lat_network_time_start;
 msg->lat_other_time = txn->txn_stats.lat_other_time_start;

 return msg;
}

Message * Message::create_message(std::vector<LogRecord *> records, RemReqType rtype) {
 Message * msg = create_message(rtype);
 ((LogMessage*)msg)->copy_from_records(records);
 msg->txn_id = records[0]->rcd.txn_id;
 return msg;
}


Message * Message::create_message(BaseQuery * query, RemReqType rtype) {
 assert(rtype == RQRY || rtype == CL_QRY || rtype == CL_QRY_O);
 Message * msg = create_message(rtype);
#if WORKLOAD == YCSB
 ((YCSBClientQueryMessage*)msg)->copy_from_query(query);
#elif WORKLOAD == TPCC
 ((TPCCClientQueryMessage*)msg)->copy_from_query(query);
#elif WORKLOAD == PPS
 ((PPSClientQueryMessage*)msg)->copy_from_query(query);
#elif  WORKLOAD == DA
  ((DAClientQueryMessage*)msg)->copy_from_query(query);
#endif
 return msg;
}

Message * Message::create_message(uint64_t txn_id, RemReqType rtype) {
 Message * msg = create_message(rtype);
 msg->txn_id = txn_id;
 return msg;
}

Message * Message::create_message(uint64_t txn_id, uint64_t batch_id, RemReqType rtype) {
 Message * msg = create_message(rtype);
 msg->txn_id = txn_id;
 msg->batch_id = batch_id;
 return msg;
}

Message * Message::create_message(RemReqType rtype) {
  Message * msg;
  switch(rtype) {
    case INIT_DONE:
      msg = new InitDoneMessage;
      break;
    case RQRY:
    case RQRY_CONT:
#if WORKLOAD == YCSB
      msg = new YCSBQueryMessage;
#elif WORKLOAD == TPCC
      msg = new TPCCQueryMessage;
#elif WORKLOAD == PPS
      msg = new PPSQueryMessage;
#elif WORKLOAD == DA
      msg = new DAQueryMessage;
#endif
      msg->init();
      break;
    case RFIN:
      msg = new FinishMessage;
      break;
    case RQRY_RSP:
      msg = new QueryResponseMessage;
      break;
    case LOG_MSG:
      msg = new LogMessage;
      break;
    case LOG_MSG_RSP:
      msg = new LogRspMessage;
      break;
    case LOG_FLUSHED:
      msg = new LogFlushedMessage;
      break;
    case RSTO:
      msg = new RStorageMessage;
      break;
    case RSTO_RSP:
      msg = new RStorageResponseMessage;
      break;
    case CLOUD_LOG_TXN:
      msg = new LogCloudTxnMessage;
      break;
    case CLOUD_LOG_TXN_ACK:
      msg = new LogFlushedMessage;
    case CALVIN_ACK:
    case ARIA_ACK:
    case RACK_PREP:
    case RACK_FIN:
      msg = new AckMessage;
      break;
    case CL_QRY:
    case CL_QRY_O:
    case RTXN:
    case RTXN_CONT:
    case CALVIN_ABORT:
#if WORKLOAD == YCSB
      msg = new YCSBClientQueryMessage;
#elif WORKLOAD == TPCC
      msg = new TPCCClientQueryMessage;
#elif WORKLOAD == PPS
      msg = new PPSClientQueryMessage;
#elif WORKLOAD == DA
      msg = new DAClientQueryMessage;
#endif
      msg->init();
      break;
    case RPREPARE:
      msg = new PrepareMessage;
      break;
    case REQ_VALID:
    case VALID:
      msg = new ValidationMessage;
      break;
    case RFWD:
      msg = new ForwardMessage;
      break;
    case RPDONE:
    case RDONE:
      msg = new DoneMessage;
      break;
    case CL_RSP:
      msg = new ClientResponseMessage;
      break;
    case CONF_STAT:
      msg = new ConflictStaticsMessage;
      msg->init();
      break;
    default:
      assert(false);
  }
  assert(msg);
  msg->rtype = rtype;
  msg->txn_id = UINT64_MAX;
  msg->batch_id = UINT64_MAX;
#if CC_ALG == HDCC || CC_ALG == SNAPPER
  msg->algo = 0;
#endif
  msg->return_node_id = g_node_id;
  msg->wq_time = 0;
  msg->mq_time = 0;
  msg->ntwk_time = 0;

  msg->lat_work_queue_time = 0;
  msg->lat_msg_queue_time = 0;
  msg->lat_cc_block_time = 0;
  msg->lat_cc_time = 0;
  msg->lat_process_time = 0;
  msg->lat_network_time = 0;
  msg->lat_other_time = 0;


  return msg;
}

uint64_t Message::mget_size() {
  uint64_t size = 0;
  size += sizeof(RemReqType);
  size += sizeof(uint64_t);
#if CC_ALG == CALVIN
  size += sizeof(uint64_t);
#endif
#if CC_ALG == HDCC
  size += sizeof(uint64_t);
  size += sizeof(uint64_t);
  size += sizeof(int);
#endif
#if CC_ALG == SNAPPER
  size += sizeof(uint64_t);
  size += sizeof(int);
#endif
  // for stats, send message queue time
  size += sizeof(uint64_t);

  // for stats, latency
  size += sizeof(uint64_t) * 7;
  return size;
}

void Message::mcopy_from_txn(TxnManager * txn) {
  //rtype = query->rtype;
  txn_id = txn->get_txn_id();
#if CC_ALG == CALVIN || CC_ALG == ARIA
  batch_id = txn->get_batch_id();
#elif CC_ALG == HDCC
  batch_id = txn->get_batch_id();
  original_return_node_id = txn->original_return_id;
  algo = txn->algo;
#elif CC_ALG == SNAPPER
  batch_id = txn->get_batch_id();
  algo = txn->algo;
#endif
}

void Message::mcopy_to_txn(TxnManager* txn) {
  txn->return_id = return_node_id;
#if CC_ALG == HDCC
  txn->original_return_id = original_return_node_id;
#endif
}

void Message::mcopy_from_buf(char * buf) {
  uint64_t ptr = 0;
  COPY_VAL(rtype,buf,ptr);
  COPY_VAL(txn_id,buf,ptr);
#if CC_ALG == CALVIN
  COPY_VAL(batch_id,buf,ptr);
#elif CC_ALG == HDCC
  COPY_VAL(batch_id,buf,ptr);
  COPY_VAL(original_return_node_id,buf,ptr);
  COPY_VAL(algo,buf,ptr);
#elif CC_ALG == SNAPPER
  COPY_VAL(batch_id,buf,ptr);
  COPY_VAL(algo,buf,ptr);
#endif
  COPY_VAL(mq_time,buf,ptr);

  COPY_VAL(lat_work_queue_time,buf,ptr);
  COPY_VAL(lat_msg_queue_time,buf,ptr);
  COPY_VAL(lat_cc_block_time,buf,ptr);
  COPY_VAL(lat_cc_time,buf,ptr);
  COPY_VAL(lat_process_time,buf,ptr);
  COPY_VAL(lat_network_time,buf,ptr);
  COPY_VAL(lat_other_time,buf,ptr);
  if ((CC_ALG == CALVIN && rtype == CALVIN_ACK && txn_id % g_node_cnt == g_node_id) ||
      (CC_ALG != CALVIN && IS_LOCAL(txn_id))) {
    lat_network_time = (get_sys_clock() - lat_network_time) - lat_other_time;
  } else {
    lat_other_time = get_sys_clock();
  }
  //printf("buftot %ld: %f, %f\n",txn_id,lat_network_time,lat_other_time);
}

void Message::mcopy_to_buf(char * buf) {
  uint64_t ptr = 0;
  COPY_BUF(buf,rtype,ptr);
  COPY_BUF(buf,txn_id,ptr);
#if CC_ALG == CALVIN
  COPY_BUF(buf,batch_id,ptr);
#elif CC_ALG == HDCC
  COPY_BUF(buf,batch_id,ptr);
  COPY_BUF(buf,original_return_node_id,ptr);
  COPY_BUF(buf,algo,ptr);
#elif CC_ALG == SNAPPER
  COPY_BUF(buf,batch_id,ptr);
  COPY_BUF(buf,algo,ptr);
#endif
  COPY_BUF(buf,mq_time,ptr);

  COPY_BUF(buf,lat_work_queue_time,ptr);
  COPY_BUF(buf,lat_msg_queue_time,ptr);
  COPY_BUF(buf,lat_cc_block_time,ptr);
  COPY_BUF(buf,lat_cc_time,ptr);
  COPY_BUF(buf,lat_process_time,ptr);
  if ((CC_ALG == CALVIN && (rtype == CL_QRY||rtype == CL_QRY_O) && txn_id % g_node_cnt == g_node_id) ||
      (CC_ALG != CALVIN && IS_LOCAL(txn_id))) {
    lat_network_time = get_sys_clock();
  } else {
    lat_other_time = get_sys_clock() - lat_other_time;
  }
  //printf("mtobuf %ld: %f, %f\n",txn_id,lat_network_time,lat_other_time);
  COPY_BUF(buf,lat_network_time,ptr);
  COPY_BUF(buf,lat_other_time,ptr);
}

void Message::release_message(Message * msg) {
  switch(msg->rtype) {
    case INIT_DONE: {
      InitDoneMessage * m_msg = (InitDoneMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                    }
    case RQRY:
    case RQRY_CONT: {
#if WORKLOAD == YCSB
      YCSBQueryMessage * m_msg = (YCSBQueryMessage*)msg;
#elif WORKLOAD == TPCC
      TPCCQueryMessage * m_msg = (TPCCQueryMessage*)msg;
#elif WORKLOAD == PPS
      PPSQueryMessage * m_msg = (PPSQueryMessage*)msg;
#elif WORKLOAD == DA
      DAQueryMessage* m_msg = (DAQueryMessage*)msg;
#endif
      m_msg->release();
      delete m_msg;
      break;
                    }
    case RFIN: {
      FinishMessage * m_msg = (FinishMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
               }
    case RQRY_RSP: {
      QueryResponseMessage * m_msg = (QueryResponseMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                   }
    case LOG_MSG: {
      LogMessage * m_msg = (LogMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                  }
    case LOG_MSG_RSP: {
      LogRspMessage * m_msg = (LogRspMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                      }
    case LOG_FLUSHED: {
      LogFlushedMessage * m_msg = (LogFlushedMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                      }
    case RSTO: {
      RStorageMessage * m_msg = (RStorageMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
    }
    case RSTO_RSP: {
      RStorageResponseMessage * m_msg = (RStorageResponseMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
    }
    case CALVIN_ACK:
    case ARIA_ACK:
    case RACK_PREP:
    case RACK_FIN: {
      AckMessage * m_msg = (AckMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                   }
    case CL_QRY:
    case CL_QRY_O:
    case RTXN:
    case RTXN_CONT: {
#if WORKLOAD == YCSB
      YCSBClientQueryMessage * m_msg = (YCSBClientQueryMessage*)msg;
#elif WORKLOAD == TPCC
      TPCCClientQueryMessage * m_msg = (TPCCClientQueryMessage*)msg;
#elif WORKLOAD == PPS
      PPSClientQueryMessage * m_msg = (PPSClientQueryMessage*)msg;
#elif WORKLOAD == DA
      DAClientQueryMessage* m_msg = (DAClientQueryMessage*)msg;
#endif
      m_msg->release();
      delete m_msg;
      break;
                    }
    case RPREPARE: {
      PrepareMessage * m_msg = (PrepareMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                   }
    case REQ_VALID:
    case VALID: {
      ValidationMessage * m_msg = (ValidationMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                  }
    case RFWD: {
      ForwardMessage * m_msg = (ForwardMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
               }
    case RPDONE:
    case RDONE: {
      DoneMessage * m_msg = (DoneMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                }
    case CL_RSP: {
      ClientResponseMessage * m_msg = (ClientResponseMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                 }
    case CONF_STAT: {
      ConflictStaticsMessage * m_msg = (ConflictStaticsMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                      }
    case CLOUD_LOG_TXN :{
      LogCloudTxnMessage * l_msg = (LogCloudTxnMessage*)msg;
      l_msg->release();
      delete l_msg;
      break;
    }
    case CLOUD_LOG_TXN_ACK :{
      LogFlushedMessage * f_msg = (LogFlushedMessage*)msg;
      f_msg->release();
      delete f_msg;
      break;
    }
    default: {
      assert(false);
    }
  }
}
/************************/

uint64_t QueryMessage::get_size() {
  uint64_t size = Message::mget_size();
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == WOOKONG || CC_ALG == DTA || CC_ALG == SNAPPER
  size += sizeof(ts);
#endif
#if CC_ALG == OCC || CC_ALG == FOCC || CC_ALG == BOCC || CC_ALG == SSI || CC_ALG == WSI || \
    CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || \
    CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC
  size += sizeof(start_ts);
#endif
#if CC_ALG == ARIA
  size += sizeof(aria_phase);
#endif
  size += sizeof(bool);
  return size;
}

void QueryMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == WOOKONG || CC_ALG == DTA || CC_ALG == SNAPPER
  ts = txn->get_timestamp();
  assert(ts != 0);
#endif
#if CC_ALG == OCC || CC_ALG == FOCC || CC_ALG == BOCC || CC_ALG == SSI || CC_ALG == WSI || \
    CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || \
    CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC
  start_ts = txn->get_start_timestamp();
#endif
#if CC_ALG == HDCC || CC_ALG == SNAPPER
  algo = txn->algo;
  isDeterministicAbort = txn->query->isDeterministicAbort;
#endif
}

void QueryMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == WOOKONG || CC_ALG == DTA || CC_ALG == SNAPPER
  assert(ts != 0);
  txn->set_timestamp(ts);
#endif
#if CC_ALG == OCC || CC_ALG == FOCC || CC_ALG == BOCC || CC_ALG == SSI || CC_ALG == WSI || \
    CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || \
    CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC
  txn->set_start_timestamp(start_ts);
#endif
#if CC_ALG == HDCC || CC_ALG == SNAPPER
  txn->algo = algo;
  txn->query->isDeterministicAbort = isDeterministicAbort;
#endif
}

void QueryMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr __attribute__ ((unused));
  ptr = Message::mget_size();
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == WOOKONG || CC_ALG == DTA || CC_ALG == SNAPPER
 COPY_VAL(ts,buf,ptr);
  assert(ts != 0);
#endif
#if CC_ALG == OCC || CC_ALG == FOCC || CC_ALG == BOCC || CC_ALG == SSI || CC_ALG == WSI || \
    CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || \
    CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC
 COPY_VAL(start_ts,buf,ptr);
#endif
#if CC_ALG == ARIA
  COPY_VAL(aria_phase,buf,ptr);
#endif
  COPY_VAL(isDeterministicAbort, buf, ptr);
}

void QueryMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr __attribute__ ((unused));
  ptr = Message::mget_size();
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == WOOKONG || CC_ALG == DTA || CC_ALG == SNAPPER
 COPY_BUF(buf,ts,ptr);
  assert(ts != 0);
#endif
#if CC_ALG == OCC || CC_ALG == FOCC || CC_ALG == BOCC || CC_ALG == SSI || CC_ALG == WSI || \
    CC_ALG == DLI_BASE || CC_ALG == DLI_OCC || CC_ALG == DLI_MVCC_OCC || \
    CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC
 COPY_BUF(buf,start_ts,ptr);
#endif
#if CC_ALG == ARIA
  COPY_BUF(buf,aria_phase,ptr);
#endif
  COPY_BUF(buf, isDeterministicAbort, ptr);
}

void LogCloudTxnMessage::copy_from_msg(Message * msg)
{
#if WORKLOAD == YCSB
  YCSBClientQueryMessage* ycsb_msg = ((YCSBClientQueryMessage*)msg);
  requests.init(g_req_per_query);
  uint64_t req_count = ycsb_msg->requests.get_count();
  array_size1 = 0;
  array_size2 = req_count;
  for(uint64_t i = 0 ; i < req_count ; i++)
  {
    requests.add(ycsb_msg->requests.get(i));
  }
#elif WORKLOAD == TPCC
  TPCCClientQueryMessage* tpcc_msg = ((TPCCClientQueryMessage*)msg);
  txn_type = tpcc_msg->txn_type;
  w_id = tpcc_msg->w_id;
  d_id = tpcc_msg->d_id;
  array_size1 = 0;
  array_size2 = 0;
  switch (txn_type)
  {
  case TPCC_PAYMENT:
    c_id = tpcc_msg->c_id;
    d_w_id = tpcc_msg->d_w_id;
    c_w_id = tpcc_msg->c_w_id;
    c_d_id = tpcc_msg->c_d_id;
    memcpy(c_last, tpcc_msg->c_last , LASTNAME_LEN);
    h_amount = tpcc_msg->h_amount;
    by_last_name = tpcc_msg->by_last_name;
    break;
  case TPCC_NEW_ORDER:
    c_id = tpcc_msg->c_id;
    items.copy(tpcc_msg->items);
    array_size1 = tpcc_msg->items.get_count();
    rbk = tpcc_msg->rbk;
    remote = tpcc_msg->remote;
    ol_cnt = tpcc_msg->ol_cnt;
    o_entry_d = tpcc_msg->o_entry_d;
    break;
  case TPCC_ORDER_STATUS:
    o_id = tpcc_msg->o_id;
    break;
  case TPCC_DELIVERY:
    o_id = tpcc_msg->o_id;
    o_carrier_id = tpcc_msg->o_carrier_id;
    ol_delivery_d = tpcc_msg->ol_delivery_d;
    break;
  case TPCC_STOCK_LEVEL:
    o_id = tpcc_msg->o_id;
    threshold = tpcc_msg->threshold;
    break;
  default:
    assert(false);
  }
#endif
}

/************************/

void YCSBClientQueryMessage::init() {}

void YCSBClientQueryMessage::release() {
  ClientQueryMessage::release();
  // Freeing requests is the responsibility of txn at commit time
/*
  for(uint64_t i = 0; i < requests.size(); i++) {
    DEBUG_M("YCSBClientQueryMessage::release ycsb_request free\n");
    mem_allocator.free(requests[i],sizeof(ycsb_request));
  }
*/
  requests.release();
}

uint64_t YCSBClientQueryMessage::get_size() {
  uint64_t size = ClientQueryMessage::get_size();
  size += sizeof(size_t);
  size += sizeof(ycsb_request) * requests.size();
  return size;
}

void YCSBClientQueryMessage::copy_from_query(BaseQuery * query) {
  ClientQueryMessage::copy_from_query(query);
/*
  requests.init(g_req_per_query);
  for(uint64_t i = 0; i < ((YCSBQuery*)(query))->requests.size(); i++) {
      YCSBQuery::copy_request_to_msg(((YCSBQuery*)(query)),this,i);
  }
*/
  requests.copy(((YCSBQuery*)(query))->requests);
}


void YCSBClientQueryMessage::copy_from_txn(TxnManager * txn) {
  ClientQueryMessage::mcopy_from_txn(txn);
/*
  requests.init(g_req_per_query);
  for(uint64_t i = 0; i < ((YCSBQuery*)(txn->query))->requests.size(); i++) {
      YCSBQuery::copy_request_to_msg(((YCSBQuery*)(txn->query)),this,i);
  }
*/
  requests.copy(((YCSBQuery*)(txn->query))->requests);
  isDeterministicAbort = txn->query->isDeterministicAbort;
}

void YCSBClientQueryMessage::copy_to_txn(TxnManager * txn) {
  // this only copies over the pointers, so if requests are freed, we'll lose the request data
  ClientQueryMessage::copy_to_txn(txn);
  // Copies pointers to txn
  ((YCSBQuery*)(txn->query))->requests.append(requests);
/*
  for(uint64_t i = 0; i < requests.size(); i++) {
      YCSBQuery::copy_request_to_qry(((YCSBQuery*)(txn->query)),this,i);
  }
*/
}

void YCSBClientQueryMessage::copy_from_buf(char * buf) {
  ClientQueryMessage::copy_from_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();
  size_t size;
  //DEBUG("1YCSBClientQuery %ld\n",ptr);
  COPY_VAL(size,buf,ptr);
  requests.init(size);
  //DEBUG("2YCSBClientQuery %ld\n",ptr);
  for(uint64_t i = 0 ; i < size;i++) {
    DEBUG_M("YCSBClientQueryMessage::copy ycsb_request alloc\n");
    ycsb_request * req = (ycsb_request*)mem_allocator.alloc(sizeof(ycsb_request));
    COPY_VAL(*req,buf,ptr);
    //DEBUG("3YCSBClientQuery %ld\n",ptr);
    assert(req->key < g_synth_table_size);
    requests.add(req);
  }
 assert(ptr == get_size());
}

void YCSBClientQueryMessage::copy_to_buf(char * buf) {
  ClientQueryMessage::copy_to_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();
  //DEBUG("1YCSBClientQuery %ld\n",ptr);
  size_t size = requests.size();
  COPY_BUF(buf,size,ptr);
  //DEBUG("2YCSBClientQuery %ld\n",ptr);
  for(uint64_t i = 0; i < requests.size(); i++) {
    ycsb_request * req = requests[i];
    assert(req->key < g_synth_table_size);
    COPY_BUF(buf,*req,ptr);
    //DEBUG("3YCSBClientQuery %ld\n",ptr);
  }
 assert(ptr == get_size());
}
/************************/

void TPCCClientQueryMessage::init() {}

void TPCCClientQueryMessage::release() {
  ClientQueryMessage::release();
  // Freeing requests is the responsibility of txn
  /*
  for(uint64_t i = 0; i < items.size(); i++) {
    DEBUG_M("TPCCClientQueryMessage::release item free\n");
    mem_allocator.free(items[i],sizeof(Item_no));
  }
  */
  items.release();
}

uint64_t TPCCClientQueryMessage::get_size() {
  uint64_t size = ClientQueryMessage::get_size();
  size += sizeof(uint64_t) * 10;
  size += sizeof(char) * LASTNAME_LEN;
  size += sizeof(bool) * 3;
  size += sizeof(size_t);
  size += sizeof(Item_no) * items.size();
  size += sizeof(uint64_t) * 4;
  return size;
}

void TPCCClientQueryMessage::copy_from_query(BaseQuery * query) {
  ClientQueryMessage::copy_from_query(query);
  TPCCQuery* tpcc_query = (TPCCQuery*)(query);

  txn_type = tpcc_query->txn_type;
	// common txn input for both payment & new-order
  w_id = tpcc_query->w_id;
  d_id = tpcc_query->d_id;
  c_id = tpcc_query->c_id;

  // payment
  d_w_id = tpcc_query->d_w_id;
  c_w_id = tpcc_query->c_w_id;
  c_d_id = tpcc_query->c_d_id;
  strcpy(c_last,tpcc_query->c_last);
  h_amount = tpcc_query->h_amount;
  by_last_name = tpcc_query->by_last_name;

  // new order
  items.copy(tpcc_query->items);
  rbk = tpcc_query->rbk;
  remote = tpcc_query->remote;
  ol_cnt = tpcc_query->ol_cnt;
  o_entry_d = tpcc_query->o_entry_d;

  // delivery
  o_carrier_id = tpcc_query->o_carrier_id;
  ol_delivery_d = tpcc_query->ol_delivery_d;

  // stock level
  o_id = tpcc_query->o_id;
  threshold = tpcc_query->threshold;
}


void TPCCClientQueryMessage::copy_from_txn(TxnManager * txn) {
  ClientQueryMessage::mcopy_from_txn(txn);
  copy_from_query(txn->query);
}

void TPCCClientQueryMessage::copy_to_txn(TxnManager * txn) {
  ClientQueryMessage::copy_to_txn(txn);
  TPCCQuery* tpcc_query = (TPCCQuery*)(txn->query);

  txn->client_id = return_node_id;


  tpcc_query->txn_type = (TPCCTxnType)txn_type;
  if(tpcc_query->txn_type == TPCC_PAYMENT)
    ((TPCCTxnManager*)txn)->state = TPCC_PAYMENT0;
  else if (tpcc_query->txn_type == TPCC_NEW_ORDER)
    ((TPCCTxnManager*)txn)->state = TPCC_NEWORDER0;
  else if (tpcc_query->txn_type == TPCC_ORDER_STATUS)
    ((TPCCTxnManager*)txn)->state = TPCC_ORDER_STATUS0;
  else if (tpcc_query->txn_type == TPCC_DELIVERY)
    ((TPCCTxnManager*)txn)->state = TPCC_DELIVERY0;
  else if (tpcc_query->txn_type == TPCC_STOCK_LEVEL)
    ((TPCCTxnManager*)txn)->state = TPCC_STOCK_LEVEL0;
  else
    assert(false);
	// common txn input for both payment & new-order
  tpcc_query->w_id = w_id;
  tpcc_query->d_id = d_id;
  tpcc_query->c_id = c_id;

  // payment
  tpcc_query->d_w_id = d_w_id;
  tpcc_query->c_w_id = c_w_id;
  tpcc_query->c_d_id = c_d_id;
  strcpy(tpcc_query->c_last,c_last);
  tpcc_query->h_amount = h_amount;
  tpcc_query->by_last_name = by_last_name;

  // new order
  tpcc_query->items.append(items);
  tpcc_query->rbk = rbk;
  tpcc_query->remote = remote;
  tpcc_query->ol_cnt = ol_cnt;
  tpcc_query->o_entry_d = o_entry_d;

  // others
  tpcc_query->o_id = o_id;
  tpcc_query->o_carrier_id = o_carrier_id;
  tpcc_query->ol_delivery_d = ol_delivery_d;
  tpcc_query->threshold = threshold;
}

void TPCCClientQueryMessage::copy_from_buf(char * buf) {
  ClientQueryMessage::copy_from_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();

  COPY_VAL(txn_type,buf,ptr);
	// common txn input for both payment & new-order
  COPY_VAL(w_id,buf,ptr);
  COPY_VAL(d_id,buf,ptr);
  COPY_VAL(c_id,buf,ptr);

  // payment
  COPY_VAL(d_w_id,buf,ptr);
  COPY_VAL(c_w_id,buf,ptr);
  COPY_VAL(c_d_id,buf,ptr);
	COPY_VAL(c_last,buf,ptr);
  COPY_VAL(h_amount,buf,ptr);
  COPY_VAL(by_last_name,buf,ptr);

  // new order
  size_t size;
  COPY_VAL(size,buf,ptr);
  items.init(size);
  for(uint64_t i = 0 ; i < size;i++) {
    DEBUG_M("TPCCClientQueryMessage::copy_from_buf item alloc\n");
    Item_no * item = (Item_no*)mem_allocator.alloc(sizeof(Item_no));
    COPY_VAL(*item,buf,ptr);
    items.add(item);
  }

  COPY_VAL(rbk,buf,ptr);
  COPY_VAL(remote,buf,ptr);
  COPY_VAL(ol_cnt,buf,ptr);
  COPY_VAL(o_entry_d,buf,ptr);

  COPY_VAL(o_id,buf,ptr);
  COPY_VAL(o_carrier_id,buf,ptr);
  COPY_VAL(ol_delivery_d,buf,ptr);
  COPY_VAL(threshold,buf,ptr);

 assert(ptr == get_size());
}

void TPCCClientQueryMessage::copy_to_buf(char * buf) {
  ClientQueryMessage::copy_to_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();

  COPY_BUF(buf,txn_type,ptr);
	// common txn input for both payment & new-order
  COPY_BUF(buf,w_id,ptr);
  COPY_BUF(buf,d_id,ptr);
  COPY_BUF(buf,c_id,ptr);

  // payment
  COPY_BUF(buf,d_w_id,ptr);
  COPY_BUF(buf,c_w_id,ptr);
  COPY_BUF(buf,c_d_id,ptr);
	COPY_BUF(buf,c_last,ptr);
  COPY_BUF(buf,h_amount,ptr);
  COPY_BUF(buf,by_last_name,ptr);

  size_t size = items.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < items.size(); i++) {
    Item_no * item = items[i];
    COPY_BUF(buf,*item,ptr);
  }

  COPY_BUF(buf,rbk,ptr);
  COPY_BUF(buf,remote,ptr);
  COPY_BUF(buf,ol_cnt,ptr);
  COPY_BUF(buf,o_entry_d,ptr);

  COPY_BUF(buf,o_id,ptr);
  COPY_BUF(buf,o_carrier_id,ptr);
  COPY_BUF(buf,ol_delivery_d,ptr);
  COPY_BUF(buf,threshold,ptr);
  
 assert(ptr == get_size());
}

/************************/

/************************/

void PPSClientQueryMessage::init() {}

void PPSClientQueryMessage::release() { ClientQueryMessage::release(); }

uint64_t PPSClientQueryMessage::get_size() {
  uint64_t size = ClientQueryMessage::get_size();
  size += sizeof(uint64_t);
  size += sizeof(uint64_t)*3;
  size += sizeof(size_t);
  size += sizeof(uint64_t) * part_keys.size();
#if CC_ALG == CALVIN
  size += sizeof(bool);
#endif
  return size;
}

void PPSClientQueryMessage::copy_from_query(BaseQuery * query) {
  ClientQueryMessage::copy_from_query(query);
  PPSQuery* pps_query = (PPSQuery*)(query);

  txn_type = pps_query->txn_type;

  part_key = pps_query->part_key;
  product_key = pps_query->product_key;
  supplier_key = pps_query->supplier_key;

  part_keys.copy(pps_query->part_keys);

}


void PPSClientQueryMessage::copy_from_txn(TxnManager * txn) {
  ClientQueryMessage::mcopy_from_txn(txn);
  copy_from_query(txn->query);
#if CC_ALG == CALVIN
  recon = txn->isRecon();
#endif
}

void PPSClientQueryMessage::copy_to_txn(TxnManager * txn) {
  ClientQueryMessage::copy_to_txn(txn);
  PPSQuery* pps_query = (PPSQuery*)(txn->query);

  txn->client_id = return_node_id;
  pps_query->txn_type = (PPSTxnType)txn_type;
  if(pps_query->txn_type == PPS_GETPART)
    ((PPSTxnManager*)txn)->state = PPS_GETPART0;
  else if(pps_query->txn_type == PPS_GETPRODUCT)
    ((PPSTxnManager*)txn)->state = PPS_GETPRODUCT0;
  else if(pps_query->txn_type == PPS_GETSUPPLIER)
    ((PPSTxnManager*)txn)->state = PPS_GETSUPPLIER0;
  else if(pps_query->txn_type == PPS_GETPARTBYPRODUCT)
    ((PPSTxnManager*)txn)->state = PPS_GETPARTBYPRODUCT0;
  else if(pps_query->txn_type == PPS_GETPARTBYSUPPLIER)
    ((PPSTxnManager*)txn)->state = PPS_GETPARTBYSUPPLIER0;
  else if(pps_query->txn_type == PPS_ORDERPRODUCT)
    ((PPSTxnManager*)txn)->state = PPS_ORDERPRODUCT0;
  else if(pps_query->txn_type == PPS_UPDATEPRODUCTPART)
    ((PPSTxnManager*)txn)->state = PPS_UPDATEPRODUCTPART0;
  else if(pps_query->txn_type == PPS_UPDATEPART)
    ((PPSTxnManager*)txn)->state = PPS_UPDATEPART0;
  pps_query->part_key = part_key;
  pps_query->product_key = product_key;
  pps_query->supplier_key = supplier_key;
  pps_query->part_keys.append(part_keys);

#if CC_ALG == CALVIN
  txn->recon = recon;
#endif
#if DEBUG_DISTR
  std::cout << "PPSClient::copy_to_txn "
            << "type " << (PPSTxnType)txn_type << " part_key " << part_key << " product_key "
            << product_key << " supplier_key " << supplier_key << std::endl;
#endif
}

void PPSClientQueryMessage::copy_from_buf(char * buf) {
  ClientQueryMessage::copy_from_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();

  COPY_VAL(txn_type,buf,ptr);
	// common txn input for both payment & new-order
  COPY_VAL(part_key,buf,ptr);
  COPY_VAL(product_key,buf,ptr);
  COPY_VAL(supplier_key,buf,ptr);

  size_t size;
  COPY_VAL(size,buf,ptr);
  part_keys.init(size);
  for(uint64_t i = 0 ; i < size;i++) {
    uint64_t item;
    COPY_VAL(item,buf,ptr);
    part_keys.add(item);
  }

#if CC_ALG == CALVIN
  COPY_VAL(recon,buf,ptr);
#endif

 assert(ptr == get_size());
#if DEBUG_DISTR
  std::cout << "PPSClient::copy_from_buf "
            << "type " << (PPSTxnType)txn_type << " part_key " << part_key << " product_key "
            << product_key << " supplier_key " << supplier_key << std::endl;
#endif
}

void PPSClientQueryMessage::copy_to_buf(char * buf) {
  ClientQueryMessage::copy_to_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();

  COPY_BUF(buf,txn_type,ptr);
	// common txn input for both payment & new-order
  COPY_BUF(buf,part_key,ptr);
  COPY_BUF(buf,product_key,ptr);
  COPY_BUF(buf,supplier_key,ptr);

  size_t size = part_keys.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < part_keys.size(); i++) {
    uint64_t item = part_keys[i];
    COPY_BUF(buf,item,ptr);
  }

#if CC_ALG == CALVIN
  COPY_BUF(buf,recon,ptr);
#endif

 assert(ptr == get_size());
#if DEBUG_DISTR
  std::cout << "PPSClient::copy_to_buf "
            << "type " << (PPSTxnType)txn_type << " part_key " << part_key << " product_key "
            << product_key << " supplier_key " << supplier_key << std::endl;
#endif
}


/***************DA zone*********/
void DAClientQueryMessage::init() {}
void DAClientQueryMessage::copy_from_query(BaseQuery* query) {
  ClientQueryMessage::copy_from_query(query);
  DAQuery* da_query = (DAQuery*)(query);

  txn_type= da_query->txn_type;
	trans_id= da_query->trans_id;
	item_id= da_query->item_id;
	seq_id= da_query->seq_id;
	write_version=da_query->write_version;
  state= da_query->state;
	next_state= da_query->next_state;
	last_state= da_query->last_state;
}
void DAClientQueryMessage::copy_to_buf(char* buf) {
  ClientQueryMessage::copy_to_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();

  COPY_BUF(buf, txn_type, ptr);
  COPY_BUF(buf, trans_id, ptr);
  COPY_BUF(buf, item_id, ptr);
  COPY_BUF(buf, seq_id, ptr);
  COPY_BUF(buf, write_version, ptr);
  COPY_BUF(buf, state, ptr);
  COPY_BUF(buf, next_state, ptr);
  COPY_BUF(buf, last_state, ptr);
  assert(ptr == get_size());
}
void DAClientQueryMessage::copy_from_txn(TxnManager* txn) {
  ClientQueryMessage::mcopy_from_txn(txn);
  copy_from_query(txn->query);
}

void DAClientQueryMessage::copy_from_buf(char* buf) {
  ClientQueryMessage::copy_from_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();

  COPY_VAL(txn_type, buf, ptr);
  // common txn input for both payment & new-order
  COPY_VAL(trans_id, buf, ptr);
  COPY_VAL(item_id, buf, ptr);
  COPY_VAL(seq_id, buf, ptr);
  COPY_VAL(write_version, buf, ptr);
  // payment
  COPY_VAL(state, buf, ptr);
  COPY_VAL(next_state, buf, ptr);
  COPY_VAL(last_state, buf, ptr);
  assert(ptr == get_size());
}

void DAClientQueryMessage::copy_to_txn(TxnManager* txn) {
  ClientQueryMessage::copy_to_txn(txn);
  DAQuery* da_query = (DAQuery*)(txn->query);


  txn->client_id = return_node_id;
  da_query->txn_type = (DATxnType)txn_type;
  da_query->trans_id = trans_id;
  da_query->item_id = item_id;
  da_query->seq_id = seq_id;
  da_query->write_version = write_version;
  da_query->state = state;
  da_query->next_state = next_state;
  da_query->last_state = last_state;

}

uint64_t DAClientQueryMessage::get_size() {
  uint64_t size = ClientQueryMessage::get_size();
  size += sizeof(DATxnType);
  size += sizeof(uint64_t) * 7;
  return size;

}
void DAClientQueryMessage::release() { ClientQueryMessage::release(); }

/************************/

void ClientQueryMessage::init() { first_startts = 0; }

void ClientQueryMessage::release() {
  partitions.release();
  first_startts = 0;
}

uint64_t ClientQueryMessage::get_size() {
  uint64_t size = Message::mget_size();
  size += sizeof(client_startts);
  /*
  uint64_t size = sizeof(ClientQueryMessage);
  */
  size += sizeof(size_t);
  size += sizeof(uint64_t) * partitions.size();
  size += sizeof(bool);
  return size;
}

void ClientQueryMessage::copy_from_query(BaseQuery * query) {
  partitions.clear();
  partitions.copy(query->partitions);
  isDeterministicAbort = query->isDeterministicAbort;
}

void ClientQueryMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  //ts = txn->txn->timestamp;
  partitions.clear();
  partitions.copy(txn->query->partitions);
  client_startts = txn->client_startts;
  isDeterministicAbort = txn->query->isDeterministicAbort;
}

void ClientQueryMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  //txn->txn->timestamp = ts;
  txn->query->partitions.clear();
  txn->query->partitions.append(partitions);
  txn->client_startts = client_startts;
  txn->client_id = return_node_id;
  txn->query->isDeterministicAbort = isDeterministicAbort;
}

void ClientQueryMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  //COPY_VAL(ts,buf,ptr);
  COPY_VAL(client_startts,buf,ptr);
  size_t size;
  COPY_VAL(size,buf,ptr);
  partitions.init(size);
  for(uint64_t i = 0; i < size; i++) {
    //COPY_VAL(partitions[i],buf,ptr);
    uint64_t part;
    COPY_VAL(part,buf,ptr);
    partitions.add(part);
  }
  COPY_VAL(isDeterministicAbort, buf, ptr);
}

void ClientQueryMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  //COPY_BUF(buf,ts,ptr);
  COPY_BUF(buf,client_startts,ptr);
  size_t size = partitions.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < size; i++) {
    uint64_t part = partitions[i];
    COPY_BUF(buf,part,ptr);
  }
  COPY_BUF(buf, isDeterministicAbort, ptr);
}

/************************/


uint64_t ClientResponseMessage::get_size() {
  uint64_t size = Message::mget_size();
  size += sizeof(uint64_t);
  return size;
}

void ClientResponseMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  client_startts = txn->client_startts;
}

void ClientResponseMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  txn->client_startts = client_startts;
}

void ClientResponseMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(client_startts,buf,ptr);
 assert(ptr == get_size());
}

void ClientResponseMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,client_startts,ptr);
 assert(ptr == get_size());
}

/************************/

uint64_t DoneMessage::get_size() {
  uint64_t size = Message::mget_size();
  return size;
}

void DoneMessage::copy_from_txn(TxnManager* txn) { Message::mcopy_from_txn(txn); }

void DoneMessage::copy_to_txn(TxnManager* txn) { Message::mcopy_to_txn(txn); }

void DoneMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
 assert(ptr == get_size());
}

void DoneMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
 assert(ptr == get_size());
}

/************************/


uint64_t ForwardMessage::get_size() {
  uint64_t size = Message::mget_size();
  size += sizeof(RC);
#if WORKLOAD == TPCC
	size += sizeof(uint64_t);
#endif
  return size;
}

void ForwardMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  rc = txn->get_rc();
#if WORKLOAD == TPCC
  o_id = ((TPCCQuery*)txn->query)->o_id;
#endif
}

void ForwardMessage::copy_to_txn(TxnManager * txn) {
  // Don't copy return ID
  //Message::mcopy_to_txn(txn);
#if WORKLOAD == TPCC
  ((TPCCQuery*)txn->query)->o_id = o_id;
#endif
}

void ForwardMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(rc,buf,ptr);
#if WORKLOAD == TPCC
  COPY_VAL(o_id,buf,ptr);
#endif
 assert(ptr == get_size());
}

void ForwardMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,rc,ptr);
#if WORKLOAD == TPCC
  COPY_BUF(buf,o_id,ptr);
#endif
 assert(ptr == get_size());
}

/************************/

uint64_t PrepareMessage::get_size() {
  uint64_t size = Message::mget_size();
  //size += sizeof(uint64_t);
#if CC_ALG == TICTOC
  size += sizeof(uint64_t);
#endif
  return size;
}

void PrepareMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
#if CC_ALG == TICTOC
  _min_commit_ts = txn->_min_commit_ts;
#endif
}
void PrepareMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
}
void PrepareMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
#if CC_ALG == TICTOC
  COPY_VAL(_min_commit_ts,buf,ptr);
#endif
  assert(ptr == get_size());
}

void PrepareMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
#if CC_ALG == TICTOC
  COPY_BUF(buf,_min_commit_ts,ptr);
#endif
  assert(ptr == get_size());
}

/************************/

uint64_t ValidationMessage::get_size() {
  uint64_t size = Message::mget_size();
  size += sizeof(RC);
#if CC_ALG == HDCC
  size += sizeof(uint64_t);
  size += sizeof(uint64_t);
#endif
  return size;
}

void ValidationMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  rc = txn->get_rc();
#if CC_ALG == HDCC
  max_calvin_tid = txn->max_calvin_tid;
  max_calvin_bid = txn->max_calvin_bid;
#endif
}
void ValidationMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
}
void ValidationMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(rc,buf,ptr);
#if CC_ALG == HDCC
  COPY_VAL(max_calvin_tid,buf,ptr);
  COPY_VAL(max_calvin_bid,buf,ptr);
#endif
  assert(ptr == get_size());
}

void ValidationMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,rc,ptr);
#if CC_ALG == HDCC
  COPY_BUF(buf,max_calvin_tid,ptr);
  COPY_BUF(buf,max_calvin_bid,ptr);
#endif
  assert(ptr == get_size());
}

/************************/

uint64_t AckMessage::get_size() {
  uint64_t size = Message::mget_size();
  size += sizeof(RC);
#if CC_ALG == MAAT || CC_ALG == WOOKONG || CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
  size += sizeof(uint64_t) * 2;
#endif
#if CC_ALG == SILO
  size += sizeof(uint64_t);
#endif
#if CC_ALG == HDCC
  size += sizeof(bool);
#endif
#if CC_ALG == ARIA
  size += sizeof(bool);
  size += sizeof(bool);
#endif
#if WORKLOAD == PPS && CC_ALG == CALVIN
  size += sizeof(size_t);
  size += sizeof(uint64_t) * part_keys.size();
#endif
#if CC_ALG == SNAPPER
  size += sizeof(size_t);
  size += sizeof(uint64_t) * dependOn.size();
  size += sizeof(size_t);
  size += sizeof(uint64_t) * dependBy.size();
#endif
  return size;
}

void AckMessage::release(){
#if CC_ALG == SNAPPER
  dependOn.clear();
  dependBy.clear();
#endif
}

void AckMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  //rc = query->rc;
  rc = txn->get_rc();
#if CC_ALG == MAAT
  lower = time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
  upper = time_table.get_upper(txn->get_thd_id(),txn->get_txn_id());
#endif
#if CC_ALG == WOOKONG
  lower = wkdb_time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
  upper = wkdb_time_table.get_upper(txn->get_thd_id(),txn->get_txn_id());
#endif
#if CC_ALG == SILO
  max_tid = txn->max_tid;
#endif
#if CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
  lower = dta_time_table.get_lower(txn->get_thd_id(), txn->get_txn_id());
  upper = dta_time_table.get_upper(txn->get_thd_id(), txn->get_txn_id());
#endif
#if CC_ALG == HDCC
  isCommit = !txn->aborted;
#endif
#if CC_ALG == ARIA
  raw = txn->raw;
  war = txn->war;
#endif
#if CC_ALG == SNAPPER
  // what to do with txn's dependon and dependby? potential memory leak
  dependOn = txn->dependOn;
  dependBy = txn->dependBy;
#endif

#if WORKLOAD == PPS && CC_ALG == CALVIN
  PPSQuery* pps_query = (PPSQuery*)(txn->query);
  part_keys.copy(pps_query->part_keys);
#endif
}

void AckMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  //query->rc = rc;
#if CC_ALG == SNAPPER
// what to do with message's dependon and dependby? potential memory leak
  txn->dependOn = dependOn;
  txn->dependBy = dependBy;
#endif
#if WORKLOAD == PPS && CC_ALG == CALVIN

  PPSQuery* pps_query = (PPSQuery*)(txn->query);
  pps_query->part_keys.append(part_keys);
#endif
}

void AckMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(rc,buf,ptr);
#if CC_ALG == MAAT || CC_ALG == WOOKONG || CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
  COPY_VAL(lower,buf,ptr);
  COPY_VAL(upper,buf,ptr);
#endif
#if CC_ALG == SILO
  COPY_VAL(max_tid,buf,ptr);
#endif
#if CC_ALG == HDCC
  COPY_VAL(isCommit,buf,ptr);
#endif
#if CC_ALG == ARIA
  COPY_VAL(raw,buf,ptr);
  COPY_VAL(war,buf,ptr);
#endif
#if CC_ALG == SNAPPER
  size_t size;
  COPY_VAL(size,buf,ptr);
  for(uint64_t i = 0; i < size; ++i){
    uint64_t item;
    COPY_VAL(item,buf,ptr);
    dependOn.insert(item);
  }
  COPY_VAL(size,buf,ptr);
  for(uint64_t i = 0; i < size; ++i){
    uint64_t item;
    COPY_VAL(item,buf,ptr);
    dependBy.insert(item);
  }
#endif
#if WORKLOAD == PPS && CC_ALG == CALVIN

  size_t size;
  COPY_VAL(size,buf,ptr);
  part_keys.init(size);
  for(uint64_t i = 0 ; i < size;i++) {
    uint64_t item;
    COPY_VAL(item,buf,ptr);
    part_keys.add(item);
  }
#endif
 assert(ptr == get_size());
}

void AckMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,rc,ptr);
#if CC_ALG == MAAT || CC_ALG == WOOKONG || CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
  COPY_BUF(buf,lower,ptr);
  COPY_BUF(buf,upper,ptr);
#endif
#if CC_ALG == SILO
  COPY_BUF(buf,max_tid,ptr);
#endif
#if CC_ALG == HDCC
  COPY_BUF(buf,isCommit,ptr);
#endif
#if CC_ALG == ARIA
  COPY_BUF(buf,raw,ptr);
  COPY_BUF(buf,war,ptr);
#endif
#if CC_ALG == SNAPPER
  size_t size = dependOn.size();
  COPY_BUF(buf,size,ptr);
  for(auto it = dependOn.begin(); it != dependOn.end(); ++it) {
    uint64_t item = *it;
    COPY_BUF(buf,item,ptr);
  }
  size = dependBy.size();
  COPY_BUF(buf,size,ptr);
  for(auto it = dependBy.begin(); it != dependBy.end(); ++it) {
    uint64_t item = *it;
    COPY_BUF(buf,item,ptr);
  }
#endif
#if WORKLOAD == PPS && CC_ALG == CALVIN

  size_t size = part_keys.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < part_keys.size(); i++) {
    uint64_t item = part_keys[i];
    COPY_BUF(buf,item,ptr);
  }
#endif
 assert(ptr == get_size());
}
/************************/

uint64_t QueryResponseMessage::get_size() {
  uint64_t size = Message::mget_size();
  size += sizeof(RC);
#if CC_ALG == TICTOC
  size += sizeof(uint64_t);
#endif
  //size += sizeof(uint64_t);
  return size;
}

void QueryResponseMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  rc = txn->get_rc();
#if CC_ALG == TICTOC
  _min_commit_ts = txn->_min_commit_ts;
#endif
}

void QueryResponseMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  //query->rc = rc;
}

void QueryResponseMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(rc,buf,ptr);
#if CC_ALG == TICTOC
  COPY_VAL(_min_commit_ts,buf,ptr);
#endif

 assert(ptr == get_size());
}

void QueryResponseMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,rc,ptr);
#if CC_ALG == TICTOC
  COPY_BUF(buf,_min_commit_ts,ptr);
#endif
 assert(ptr == get_size());
}

/************************/

uint64_t FinishMessage::get_size() {
  uint64_t size = Message::mget_size();
  size += sizeof(uint64_t);
  size += sizeof(RC);
  size += sizeof(bool);
#if CC_ALG == MAAT || CC_ALG == WOOKONG || CC_ALG == SSI || CC_ALG == WSI || \
    CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC_OCC || \
    CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC || CC_ALG == SILO
  size += sizeof(uint64_t);
#endif
  return size;
}

void FinishMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  rc = txn->get_rc();
  readonly = txn->query->readonly();

#if CC_ALG == MAAT || CC_ALG == WOOKONG || CC_ALG == SSI || CC_ALG == WSI || \
    CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC_OCC || \
    CC_ALG == DLI_MVCC || CC_ALG == SILO
  commit_timestamp = txn->get_commit_timestamp();
#endif
}

void FinishMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);

#if CC_ALG == MAAT || CC_ALG == WOOKONG || CC_ALG == SSI || CC_ALG == WSI || \
    CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC_OCC || \
    CC_ALG == DLI_MVCC || CC_ALG == SILO
  txn->commit_timestamp = commit_timestamp;
#endif
}

void FinishMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(pid,buf,ptr);
  COPY_VAL(rc,buf,ptr);
  COPY_VAL(readonly,buf,ptr);
#if CC_ALG == MAAT || CC_ALG == WOOKONG || CC_ALG == SSI || CC_ALG == WSI || \
    CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC_OCC || \
    CC_ALG == DLI_MVCC || CC_ALG == SILO
  COPY_VAL(commit_timestamp,buf,ptr);
#endif
 assert(ptr == get_size());
}

void FinishMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,pid,ptr);
  COPY_BUF(buf,rc,ptr);
  COPY_BUF(buf,readonly,ptr);
#if CC_ALG == MAAT || CC_ALG == WOOKONG || CC_ALG == SSI || CC_ALG == WSI || \
    CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || CC_ALG == DLI_MVCC_OCC || \
    CC_ALG == DLI_MVCC || CC_ALG == SILO
  COPY_BUF(buf,commit_timestamp,ptr);
#endif

 assert(ptr == get_size());
}

/************************/

void LogMessage::release() {
  mem_allocator.free(image_size, sizeof(LogRecord *) * record_cnt);
  mem_allocator.free(records, sizeof(LogRecord *) * record_cnt);
}

uint64_t LogMessage::get_size() {
  uint64_t size = Message::mget_size();
  size += sizeof(uint64_t);
  for (uint64_t i = 0; i < record_cnt; i++) {
    size += sizeof(uint64_t);
    size += sizeof(LogRecord) + image_size[i] * 2 - 1;
  }
  return size;
}

void LogMessage::copy_from_txn(TxnManager* txn) { Message::mcopy_from_txn(txn); }

void LogMessage::copy_to_txn(TxnManager* txn) { Message::mcopy_to_txn(txn); }

void LogMessage::copy_from_records(std::vector<LogRecord *> records) {
  record_cnt = records.size();
  image_size = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t) * record_cnt);
  this->records = (LogRecord **)mem_allocator.alloc(sizeof(LogRecord *) * record_cnt);
  for(uint64_t i = 0; i < record_cnt; i++) {
    image_size[i] = records[i]->rcd.image_size;
    this->records[i] = records[i];
  }
}

void LogMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(record_cnt,buf,ptr);
  image_size = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t) * record_cnt);
  records = (LogRecord **)mem_allocator.alloc(sizeof(LogRecord *) * record_cnt);
  for (uint64_t i = 0; i < record_cnt; i++) {
    memcpy(image_size + i, buf + ptr, sizeof(uint64_t));
    ptr += sizeof(uint64_t);
    records[i] = (LogRecord *)mem_allocator.alloc(sizeof(LogRecord) + 2 * image_size[i] - 1);
    memcpy(records[i], buf + ptr, sizeof(LogRecord) + 2 * image_size[i] - 1);
    ptr += sizeof(LogRecord) + 2 * image_size[i] - 1;
  }
  assert(ptr == get_size());
}

void LogMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,record_cnt,ptr);
  for (uint64_t i = 0; i < record_cnt; i++) {
    memcpy(buf + ptr, image_size + i, sizeof(uint64_t));
    ptr += sizeof(uint64_t);
    memcpy(buf + ptr, records[i], sizeof(LogRecord) + 2 * image_size[i] - 1);
    ptr += sizeof(LogRecord) + 2 * image_size[i] - 1;
  }
  assert(ptr == get_size());
}

/************************/

uint64_t LogRspMessage::get_size() {
  uint64_t size = Message::mget_size();
  return size;
}

void LogRspMessage::copy_from_txn(TxnManager* txn) { Message::mcopy_from_txn(txn); }

void LogRspMessage::copy_to_txn(TxnManager* txn) { Message::mcopy_to_txn(txn); }

void LogRspMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  //uint64_t ptr = Message::mget_size();
}

void LogRspMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  //uint64_t ptr = Message::mget_size();
}

void RStorageMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(size,buf,ptr);
  table_ids.init(size);
  keys.init(size);
  for (uint64_t i = 0; i < size; i++) {
    uint64_t table_id;
    COPY_VAL(table_id,buf,ptr);
    table_ids.add(table_id);
    uint64_t key;
    COPY_VAL(key,buf,ptr);
    keys.add(key);
  }
  assert(ptr == get_size());
}

void RStorageMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  size_t size = table_ids.size();
  COPY_BUF(buf,size,ptr);
  for (uint64_t i = 0; i < size; i++) {
    uint64_t table_id = table_ids[i];
    COPY_BUF(buf,table_id,ptr);
    uint64_t key = keys[i];
    COPY_BUF(buf,key,ptr);
  }
  assert(ptr == get_size());
}

void RStorageMessage::copy_from_txn(TxnManager* txn) {Message::mcopy_from_txn(txn);}

void RStorageMessage::copy_to_txn(TxnManager* txn) {Message::mcopy_to_txn(txn);}

uint64_t RStorageMessage::get_size() {
  uint64_t size = Message::mget_size();
  size += sizeof(size_t);
  size += sizeof(uint64_t) * table_ids.size();
  size += sizeof(uint64_t) * keys.size();
  return size;
}

void RStorageMessage::release() {
  table_ids.release();
  keys.release();
}

void RStorageMessage::init() {}


void RStorageResponseMessage::init() {}

void RStorageResponseMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  // COPY_VAL(size,buf,ptr);
  // results.init(size);
  // for (uint64_t i = 0; i < size; i++) {
  //   uint64_t result;
  //   COPY_VAL(result,buf,ptr);
  //   results.add(result);
  // }
  assert(ptr == get_size());
}

void RStorageResponseMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  // size_t size = results.size();
  // COPY_BUF(buf,size,ptr);
  // for (uint64_t i = 0; i < size; i++) {
  //   uint64_t result = results[i];
  //   COPY_BUF(buf,result,ptr);
  // }
  assert(ptr == get_size());
}

void RStorageResponseMessage::copy_from_txn(TxnManager* txn) {Message::mcopy_from_txn(txn);}

void RStorageResponseMessage::copy_to_txn(TxnManager* txn) {Message::mcopy_to_txn(txn);}

uint64_t RStorageResponseMessage::get_size() {
  uint64_t size = Message::mget_size();
  // size += sizeof(size_t);
  // size += sizeof(uint64_t) * results.size();
  return size;
}

void RStorageResponseMessage::release() {
  // results.release();
}


/************************/

uint64_t InitDoneMessage::get_size() {
  uint64_t size = Message::mget_size();
  return size;
}

void InitDoneMessage::copy_from_txn(TxnManager* txn) {}

void InitDoneMessage::copy_to_txn(TxnManager* txn) { Message::mcopy_to_txn(txn); }

void InitDoneMessage::copy_from_buf(char* buf) { Message::mcopy_from_buf(buf); }

void InitDoneMessage::copy_to_buf(char* buf) { Message::mcopy_to_buf(buf); }

/************************/

uint64_t ConflictStaticsMessage::get_size() {
  uint64_t size = Message::mget_size();
  size+=sizeof(size_t);
  size += sizeof(uint16_t)*conflict_statics.size();
  return size;
}

void ConflictStaticsMessage::init(){
  conflict_statics.init(g_total_shard_num);
}
void ConflictStaticsMessage::release(){
  conflict_statics.release();
}

void ConflictStaticsMessage::copy_from_txn(TxnManager* txn) { Message::mcopy_from_txn(txn); }

void ConflictStaticsMessage::copy_to_txn(TxnManager* txn) { Message::mcopy_to_txn(txn); }

void ConflictStaticsMessage::copy_from_buf(char* buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  size_t size;
  COPY_VAL(size,buf,ptr);
  conflict_statics.init(size);
  for(uint64_t i = 0 ; i < size;i++) {
    DEBUG_M("ConflictStaticsMessage::copy_from_buf\n");
    uint16_t stat;
    COPY_VAL(stat,buf,ptr);
    conflict_statics.add(stat);
  }
  assert(ptr == get_size());
}

void ConflictStaticsMessage::copy_to_buf(char* buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  size_t size=conflict_statics.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i=0;i<size;i++){
    uint16_t stat=conflict_statics[i];
    COPY_BUF(buf,stat,ptr);
  }
  assert(ptr == get_size());
}

/************************/

void YCSBQueryMessage::init() {}

void YCSBQueryMessage::release() {
  QueryMessage::release();
  // Freeing requests is the responsibility of txn
/*
  for(uint64_t i = 0; i < requests.size(); i++) {
    DEBUG_M("YCSBQueryMessage::release ycsb_request free\n");
    mem_allocator.free(requests[i],sizeof(ycsb_request));
  }
*/
  requests.release();
}

uint64_t YCSBQueryMessage::get_size() {
  uint64_t size = QueryMessage::get_size();
  size += sizeof(size_t);
  size += sizeof(ycsb_request) * requests.size();
  return size;
}

void YCSBQueryMessage::copy_from_txn(TxnManager * txn) {
  QueryMessage::copy_from_txn(txn);
  requests.init(g_req_per_query);
  ((YCSBTxnManager*)txn)->copy_remote_requests(this);
  //requests.copy(((YCSBQuery*)(txn->query))->requests);
}

void YCSBQueryMessage::copy_to_txn(TxnManager * txn) {
  QueryMessage::copy_to_txn(txn);
#if CC_ALG==TICTOC
  ((YCSBQuery*)(txn->query))->requests.clear();
#endif
  //((YCSBQuery*)(txn->query))->requests.copy(requests);
#if ONE_NODE_RECIEVE == 1 && defined(NO_REMOTE) && LESS_DIS_NUM == 10
#else
  ((YCSBQuery*)(txn->query))->requests.append(requests);
  ((YCSBQuery*)(txn->query))->orig_request = &requests;
#endif
}

void YCSBQueryMessage::copy_from_buf(char * buf) {
  QueryMessage::copy_from_buf(buf);
  uint64_t ptr = QueryMessage::get_size();
  size_t size;
  COPY_VAL(size,buf,ptr);
  assert(size<=g_req_per_query);
  requests.init(size);
  for(uint64_t i = 0 ; i < size;i++) {
    DEBUG_M("YCSBQueryMessage::copy ycsb_request alloc\n");
    ycsb_request * req = (ycsb_request*)mem_allocator.alloc(sizeof(ycsb_request));
    COPY_VAL(*req,buf,ptr);
    ASSERT(req->key < g_synth_table_size);
    requests.add(req);
  }
 assert(ptr == get_size());
}

void YCSBQueryMessage::copy_to_buf(char * buf) {
  QueryMessage::copy_to_buf(buf);
  uint64_t ptr = QueryMessage::get_size();
  size_t size = requests.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < requests.size(); i++) {
    ycsb_request * req = requests[i];
    COPY_BUF(buf,*req,ptr);
  }
 assert(ptr == get_size());
}
/************************/

void TPCCQueryMessage::init() {}

void TPCCQueryMessage::release() {
  QueryMessage::release();
  // Freeing items is the responsibility of txn
  /*
  for(uint64_t i = 0; i < items.size(); i++) {
    DEBUG_M("TPCCQueryMessage::release item free\n");
    mem_allocator.free(items[i],sizeof(Item_no));
  }
  */
  items.release();
}

uint64_t TPCCQueryMessage::get_size() {
  uint64_t size = QueryMessage::get_size();

  size += sizeof(uint64_t); //txn_type
  size += sizeof(uint64_t); //state
  size += sizeof(uint64_t) * 3; // w_id, d_id, c_id

  // Payment
  if(txn_type == TPCC_PAYMENT) {

    size += sizeof(uint64_t) * 4; // d_w_id, c_w_id, c_d_id;, h_amount
    size += sizeof(char) * LASTNAME_LEN; // c_last[LASTNAME_LEN]
    size += sizeof(bool); // by_last_name

  }

  // New Order
  if(txn_type == TPCC_NEW_ORDER) {
    size += sizeof(uint64_t) * 2; // ol_cnt, o_entry_d,
    size += sizeof(bool) * 2; // rbk, remote
    size += sizeof(Item_no) * items.size();
    size += sizeof(uint64_t); // items size
    size += sizeof(uint64_t); //o_id
  }

  return size;
}

void TPCCQueryMessage::copy_from_txn(TxnManager * txn) {
  QueryMessage::copy_from_txn(txn);
  TPCCQuery* tpcc_query = (TPCCQuery*)(txn->query);

  txn_type = tpcc_query->txn_type;
  state = (uint64_t)((TPCCTxnManager*)txn)->state;
	// common txn input for both payment & new-order
  w_id = tpcc_query->w_id;
  d_id = tpcc_query->d_id;
  c_id = tpcc_query->c_id;

  // payment
  if(txn_type == TPCC_PAYMENT) {
    d_w_id = tpcc_query->d_w_id;
    c_w_id = tpcc_query->c_w_id;
    c_d_id = tpcc_query->c_d_id;
    strcpy(c_last,tpcc_query->c_last);
    h_amount = tpcc_query->h_amount;
    by_last_name = tpcc_query->by_last_name;
  }

  // new order
  //items.copy(tpcc_query->items);
  if(txn_type == TPCC_NEW_ORDER) {
#if CC_ALG == ARIA
    items.copy(tpcc_query->items);
#else
    ((TPCCTxnManager*)txn)->copy_remote_items(this);
#endif
    rbk = tpcc_query->rbk;
    remote = tpcc_query->remote;
    ol_cnt = tpcc_query->ol_cnt;
    o_entry_d = tpcc_query->o_entry_d;
    o_id = tpcc_query->o_id;
  }

}

void TPCCQueryMessage::copy_to_txn(TxnManager * txn) {
  QueryMessage::copy_to_txn(txn);

  TPCCQuery* tpcc_query = (TPCCQuery*)(txn->query);

  tpcc_query->txn_type = (TPCCTxnType)txn_type;
  ((TPCCTxnManager*)txn)->state = (TPCCRemTxnType)state;
	// common txn input for both payment & new-order
  tpcc_query->w_id = w_id;
  tpcc_query->d_id = d_id;
  tpcc_query->c_id = c_id;

  // payment
  if(txn_type == TPCC_PAYMENT) {
    tpcc_query->d_w_id = d_w_id;
    tpcc_query->c_w_id = c_w_id;
    tpcc_query->c_d_id = c_d_id;
    strcpy(tpcc_query->c_last,c_last);
    tpcc_query->h_amount = h_amount;
    tpcc_query->by_last_name = by_last_name;
  }

  // new order
  if(txn_type == TPCC_NEW_ORDER) {
#if CC_ALG==TICTOC || CC_ALG == ARIA
    tpcc_query->items.clear();
#endif
    tpcc_query->items.append(items);
    tpcc_query->rbk = rbk;
    tpcc_query->remote = remote;
    tpcc_query->ol_cnt = ol_cnt;
    tpcc_query->o_entry_d = o_entry_d;
    tpcc_query->o_id = o_id;
  }


}


void TPCCQueryMessage::copy_from_buf(char * buf) {
  QueryMessage::copy_from_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_VAL(txn_type,buf,ptr);
  assert(txn_type == TPCC_PAYMENT || txn_type == TPCC_NEW_ORDER);
  COPY_VAL(state,buf,ptr);
	// common txn input for both payment & new-order
  COPY_VAL(w_id,buf,ptr);
  COPY_VAL(d_id,buf,ptr);
  COPY_VAL(c_id,buf,ptr);

  // payment
  if(txn_type == TPCC_PAYMENT) {
    COPY_VAL(d_w_id,buf,ptr);
    COPY_VAL(c_w_id,buf,ptr);
    COPY_VAL(c_d_id,buf,ptr);
    COPY_VAL(c_last,buf,ptr);
    COPY_VAL(h_amount,buf,ptr);
    COPY_VAL(by_last_name,buf,ptr);
  }

  // new order
  if(txn_type == TPCC_NEW_ORDER) {
    size_t size;
    COPY_VAL(size,buf,ptr);
    items.init(size);
    for(uint64_t i = 0 ; i < size;i++) {
      DEBUG_M("TPCCQueryMessage::copy item alloc\n");
      Item_no * item = (Item_no*)mem_allocator.alloc(sizeof(Item_no));
      COPY_VAL(*item,buf,ptr);
      items.add(item);
    }

    COPY_VAL(rbk,buf,ptr);
    COPY_VAL(remote,buf,ptr);
    COPY_VAL(ol_cnt,buf,ptr);
    COPY_VAL(o_entry_d,buf,ptr);
    COPY_VAL(o_id,buf,ptr);
  }

 assert(ptr == get_size());

}

void TPCCQueryMessage::copy_to_buf(char * buf) {
  QueryMessage::copy_to_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_BUF(buf,txn_type,ptr);
  COPY_BUF(buf,state,ptr);
	// common txn input for both payment & new-order
  COPY_BUF(buf,w_id,ptr);
  COPY_BUF(buf,d_id,ptr);
  COPY_BUF(buf,c_id,ptr);

  // payment
  if(txn_type == TPCC_PAYMENT) {
    COPY_BUF(buf,d_w_id,ptr);
    COPY_BUF(buf,c_w_id,ptr);
    COPY_BUF(buf,c_d_id,ptr);
    COPY_BUF(buf,c_last,ptr);
    COPY_BUF(buf,h_amount,ptr);
    COPY_BUF(buf,by_last_name,ptr);
  }

  if(txn_type == TPCC_NEW_ORDER) {
    size_t size = items.size();
    COPY_BUF(buf,size,ptr);
    for(uint64_t i = 0; i < items.size(); i++) {
      Item_no * item = items[i];
      COPY_BUF(buf,*item,ptr);
    }

    COPY_BUF(buf,rbk,ptr);
    COPY_BUF(buf,remote,ptr);
    COPY_BUF(buf,ol_cnt,ptr);
    COPY_BUF(buf,o_entry_d,ptr);
    COPY_BUF(buf,o_id,ptr);
  }
 assert(ptr == get_size());
}
/************************/

void PPSQueryMessage::init() {}

void PPSQueryMessage::release() { QueryMessage::release(); }

uint64_t PPSQueryMessage::get_size() {
  uint64_t size = QueryMessage::get_size();

  size += sizeof(uint64_t); // txn_type
  size += sizeof(uint64_t); // state
  size += sizeof(uint64_t); // part/product/supply key
  size += sizeof(size_t);
  size += sizeof(uint64_t) * part_keys.size();
  return size;
}

void PPSQueryMessage::copy_from_txn(TxnManager * txn) {
  QueryMessage::copy_from_txn(txn);
  PPSQuery* pps_query = (PPSQuery*)(txn->query);

  txn_type = pps_query->txn_type;
  state = (uint64_t)((PPSTxnManager*)txn)->state;

  if (txn_type == PPS_GETPART) {
    part_key = pps_query->part_key;
  }
  if (txn_type == PPS_GETPRODUCT) {
    product_key = pps_query->product_key;
  }
  if (txn_type == PPS_GETSUPPLIER) {
    supplier_key = pps_query->supplier_key;
  }
  if (txn_type == PPS_GETPARTBYPRODUCT) {
    //product_key = pps_query->product_key;
    part_key = pps_query->part_key;
  }
  if (txn_type == PPS_GETPARTBYSUPPLIER) {
    //supplier_key = pps_query->supplier_key;
    part_key = pps_query->part_key;
  }
  if (txn_type == PPS_ORDERPRODUCT) {
      part_key = pps_query->part_key;
  }
  if (txn_type == PPS_UPDATEPRODUCTPART) {
      product_key = pps_query->product_key;
  }
  if (txn_type == PPS_UPDATEPART) {
      part_key = pps_query->part_key;
  }

  part_keys.copy(pps_query->part_keys);

}

void PPSQueryMessage::copy_to_txn(TxnManager * txn) {
  QueryMessage::copy_to_txn(txn);

  PPSQuery* pps_query = (PPSQuery*)(txn->query);

  pps_query->txn_type = (PPSTxnType)txn_type;
  ((PPSTxnManager*)txn)->state = (PPSRemTxnType)state;

  if (txn_type == PPS_GETPART) {
    pps_query->part_key = part_key;
  }
  if (txn_type == PPS_GETPRODUCT) {
    pps_query->product_key = product_key;
  }
  if (txn_type == PPS_GETSUPPLIER) {
    pps_query->supplier_key = supplier_key;
  }
  if (txn_type == PPS_GETPARTBYPRODUCT) {
    //pps_query->product_key = product_key;
    pps_query->part_key = part_key;
  }
  if (txn_type == PPS_GETPARTBYSUPPLIER) {
    //pps_query->supplier_key = supplier_key;
    pps_query->part_key = part_key;
  }
  if (txn_type == PPS_ORDERPRODUCT) {
    //pps_query->product_key = product_key;
    pps_query->part_key = part_key;
  }
  if (txn_type == PPS_UPDATEPRODUCTPART) {
      pps_query->product_key = product_key;
  }
  if (txn_type == PPS_UPDATEPART) {
      pps_query->part_key = part_key;
  }
  pps_query->part_keys.append(part_keys);

}


void PPSQueryMessage::copy_from_buf(char * buf) {
  QueryMessage::copy_from_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_VAL(txn_type,buf,ptr);
  COPY_VAL(state,buf,ptr);
  if (txn_type == PPS_GETPART) {
    COPY_VAL(part_key,buf,ptr);
  }
  if (txn_type == PPS_GETPRODUCT) {
    COPY_VAL(product_key,buf,ptr);
  }
  if (txn_type == PPS_GETSUPPLIER) {
    COPY_VAL(supplier_key,buf,ptr);
  }
  if (txn_type == PPS_GETPARTBYPRODUCT) {
    //COPY_VAL(product_key,buf,ptr);
    COPY_VAL(part_key,buf,ptr);
  }
  if (txn_type == PPS_GETPARTBYSUPPLIER) {
    //COPY_VAL(supplier_key,buf,ptr);
    COPY_VAL(part_key,buf,ptr);
  }
  if (txn_type == PPS_ORDERPRODUCT) {
    //COPY_VAL(product_key,buf,ptr);
    COPY_VAL(part_key,buf,ptr);
  }
  if (txn_type == PPS_UPDATEPRODUCTPART) {
      COPY_VAL(product_key,buf,ptr);
  }
  if (txn_type == PPS_UPDATEPART) {
      COPY_VAL(part_key,buf,ptr);
  }

  size_t size;
  COPY_VAL(size,buf,ptr);
  part_keys.init(size);
  for(uint64_t i = 0 ; i < size;i++) {
    uint64_t item;
    COPY_VAL(item,buf,ptr);
    part_keys.add(item);
  }

 assert(ptr == get_size());

}

void PPSQueryMessage::copy_to_buf(char * buf) {
  QueryMessage::copy_to_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_BUF(buf,txn_type,ptr);
  COPY_BUF(buf,state,ptr);

  if (txn_type == PPS_GETPART) {
    COPY_BUF(buf,part_key,ptr);
  }
  if (txn_type == PPS_GETPRODUCT) {
    COPY_BUF(buf,product_key,ptr);
  }
  if (txn_type == PPS_GETSUPPLIER) {
    COPY_BUF(buf,supplier_key,ptr);
  }
  if (txn_type == PPS_GETPARTBYPRODUCT) {
    //COPY_BUF(buf,product_key,ptr);
    COPY_BUF(buf,part_key,ptr);
  }
  if (txn_type == PPS_GETPARTBYSUPPLIER) {
    //COPY_BUF(buf,supplier_key,ptr);
    COPY_BUF(buf,part_key,ptr);
  }
  if (txn_type == PPS_ORDERPRODUCT) {
    //COPY_BUF(buf,product_key,ptr);
    COPY_BUF(buf,part_key,ptr);
  }
  if (txn_type == PPS_UPDATEPRODUCTPART) {
    //COPY_BUF(buf,product_key,ptr);
    COPY_BUF(buf,product_key,ptr);
  }
  if (txn_type == PPS_UPDATEPART) {
    //COPY_BUF(buf,product_key,ptr);
    COPY_BUF(buf,part_key,ptr);
  }

  size_t size = part_keys.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < part_keys.size(); i++) {
    uint64_t item = part_keys[i];
    COPY_BUF(buf,item,ptr);
  }

 assert(ptr == get_size());
}
//---DAquerymessage zone------------

void DAQueryMessage::init() {}
/*
void DAQueryMessage::copy_from_query(BaseQuery* query) {
  QueryMessage::copy_from_query(query);
  DAQuery* da_query = (DAQuery*)(query);

  txn_type= da_query->txn_type;
	trans_id= da_query->trans_id;
	item_id= da_query->item_id;
	seq_id= da_query->seq_id;
	write_version=da_query->write_version;
  state= da_query->state;
	next_state= da_query->next_state;
	last_state= da_query->last_state;
}*/
void DAQueryMessage::copy_to_buf(char* buf) {
  QueryMessage::copy_to_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_BUF(buf, txn_type, ptr);
  COPY_BUF(buf, trans_id, ptr);
  COPY_BUF(buf, item_id, ptr);
  COPY_BUF(buf, seq_id, ptr);
  COPY_BUF(buf, write_version, ptr);
  COPY_BUF(buf, state, ptr);
  COPY_BUF(buf, next_state, ptr);
  COPY_BUF(buf, last_state, ptr);

}
void DAQueryMessage::copy_from_txn(TxnManager* txn) {
  QueryMessage::mcopy_from_txn(txn);
  DAQuery* da_query = (DAQuery*)(txn->query);

  txn_type = da_query->txn_type;
  trans_id = da_query->trans_id;
  item_id = da_query->item_id;
  seq_id = da_query->seq_id;
  write_version = da_query->write_version;
  state = da_query->state;
  next_state = da_query->next_state;
  last_state = da_query->last_state;
}

void DAQueryMessage::copy_from_buf(char* buf) {
  QueryMessage::copy_from_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_VAL(txn_type, buf, ptr);
  // common txn input for both payment & new-order
  COPY_VAL(trans_id, buf, ptr);
  COPY_VAL(item_id, buf, ptr);
  COPY_VAL(seq_id, buf, ptr);
  COPY_VAL(write_version, buf, ptr);
  // payment
  COPY_VAL(state, buf, ptr);
  COPY_VAL(next_state, buf, ptr);
  COPY_VAL(last_state, buf, ptr);
  assert(ptr == get_size());
}

void DAQueryMessage::copy_to_txn(TxnManager* txn) {
  QueryMessage::copy_to_txn(txn);
  DAQuery* da_query = (DAQuery*)(txn->query);


  txn->client_id = return_node_id;
  da_query->txn_type = (DATxnType)txn_type;
  da_query->trans_id = trans_id;
  da_query->item_id = item_id;
  da_query->seq_id = seq_id;
  da_query->write_version = write_version;
  da_query->state = state;
  da_query->next_state = next_state;
  da_query->last_state = last_state;

}

uint64_t DAQueryMessage::get_size() {
  uint64_t size = QueryMessage::get_size();
  size += sizeof(DATxnType);
  size += sizeof(uint64_t) * 7;
  return size;
}
void DAQueryMessage::release() { QueryMessage::release(); }


void LogCloudTxnMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(array_size1,buf,ptr);
  COPY_VAL(array_size2,buf,ptr);
  items.init(g_max_items_per_txn);
  requests.init(g_req_per_query);
  for(uint64_t i = 0 ; i < g_max_items_per_txn ; i++)
  {
    if(i < array_size1)
    {
      Item_no * item = new Item_no();
      COPY_VAL(*item,buf,ptr);
      items.add(item);
    }
    else
    {
      Item_no * item = new Item_no();
      COPY_VAL(*item,buf,ptr);
      delete item;
    }
  }
  for(uint64_t i = 0 ; i < g_req_per_query ; i++)
  {
    if(i < array_size2)
    {
      ycsb_request * req = new ycsb_request();
      COPY_VAL(*req,buf,ptr);
      requests.add(req);
    }
    else
    {
      ycsb_request * req = new ycsb_request();
      COPY_VAL(*req,buf,ptr);
      delete req;
    }
  }
  for(uint64_t i = 0 ; i < LASTNAME_LEN ; i++)
  {
    COPY_VAL(*(c_last+i),buf,ptr);
  }
  COPY_VAL(txn_type,buf,ptr);
  COPY_VAL(w_id,buf,ptr);
  COPY_VAL(d_id,buf,ptr);
  COPY_VAL(c_id,buf,ptr);
  COPY_VAL(d_w_id,buf,ptr);
  COPY_VAL(c_w_id,buf,ptr);
  COPY_VAL(c_d_id,buf,ptr);
  COPY_VAL(h_amount,buf,ptr);
  COPY_VAL(by_last_name,buf,ptr);
  COPY_VAL(rbk,buf,ptr);
  COPY_VAL(remote,buf,ptr);
  COPY_VAL(ol_cnt,buf,ptr);
  COPY_VAL(o_entry_d,buf,ptr);
  COPY_VAL(o_id,buf,ptr);
  COPY_VAL(o_carrier_id,buf,ptr);
  COPY_VAL(ol_delivery_d,buf,ptr);
  COPY_VAL(threshold,buf,ptr);
  assert(ptr == get_size());
}
void LogCloudTxnMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  // COPY_BUF(buf,client_startts,ptr);
  COPY_BUF(buf,array_size1,ptr);
  COPY_BUF(buf,array_size2,ptr);
  for(uint64_t i = 0 ; i < g_max_items_per_txn ; i++)
  {
    if(i < array_size1)
    {
      Item_no * item = items.get(i);
      COPY_BUF(buf,*item,ptr);
    }
    else
    {
      Item_no * item = new Item_no;
      COPY_BUF(buf,*item,ptr);
      delete item;
    }
  }
  for(uint64_t i = 0 ; i < g_req_per_query ; i++)
  {
    if(i < array_size2)
    {
      ycsb_request * req = requests.get(i);
      COPY_BUF(buf,*req,ptr);
    }
    else
    {
      ycsb_request * req = new ycsb_request();
      COPY_BUF(buf,*req,ptr);
      delete req;
    }
  }
  for(uint64_t i = 0 ; i < LASTNAME_LEN ; i++)
  {
    COPY_BUF(buf,*(c_last+i),ptr);
  }
  COPY_BUF(buf,txn_type,ptr);
  COPY_BUF(buf,w_id,ptr);
  COPY_BUF(buf,d_id,ptr);
  COPY_BUF(buf,c_id,ptr);
  COPY_BUF(buf,d_w_id,ptr);
  COPY_BUF(buf,c_w_id,ptr);
  COPY_BUF(buf,c_d_id,ptr);
  COPY_BUF(buf,h_amount,ptr);
  COPY_BUF(buf,by_last_name,ptr);
  COPY_BUF(buf,rbk,ptr);
  COPY_BUF(buf,remote,ptr);
  COPY_BUF(buf,ol_cnt,ptr);
  COPY_BUF(buf,o_entry_d,ptr);
  COPY_BUF(buf,o_id,ptr);
  COPY_BUF(buf,o_carrier_id,ptr);
  COPY_BUF(buf,ol_delivery_d,ptr);
  COPY_BUF(buf,threshold,ptr);
  assert(ptr == get_size());
}
uint64_t LogCloudTxnMessage::get_size() {
  uint64_t size_else = Message::mget_size();
  size_else += (sizeof(uint64_t) * 2);
  size_else += (g_max_items_per_txn * sizeof(Item_no));
  size_else += (g_req_per_query * sizeof(ycsb_request));
  size_else += (LASTNAME_LEN * sizeof(char));
  size_else += (14 * sizeof(uint64_t));
  size_else += (3 * sizeof(bool));
  return size_else;
}