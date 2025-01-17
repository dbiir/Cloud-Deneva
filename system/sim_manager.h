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

#ifndef _SIMMAN_H_
#define _SIMMAN_H_

#include "global.h"

// Aria
enum ARIA_PHASE {
  ARIA_INIT = -1,
  ARIA_COLLECT = 0,
  ARIA_READ,
  ARIA_RESERVATION,
  ARIA_CHECK,
  ARIA_COMMIT
};

class SimManager {
public:
	volatile bool sim_init_done;
	volatile bool warmup;
  volatile uint64_t warmup_end_time;
	bool start_set;
	volatile bool sim_done;
  uint64_t run_starttime;
  uint64_t rsp_cnt;
  uint64_t seq_epoch;
#if CC_ALG == CALVIN && CALVIN_W
  uint64_t * workers_epoch;
#else
  uint64_t worker_epoch;
#endif
  uint64_t last_worker_epoch_time;
  uint64_t last_seq_epoch_time;
  int64_t epoch_txn_cnt;
  uint64_t txn_cnt;
  uint64_t inflight_cnt;
  uint64_t flushed_batch;
  uint64_t now_batch;
  uint64_t now_batch_txn_cnt;
  uint64_t last_da_query_time;
  ARIA_PHASE aria_phase;
  uint64_t batch_process_count;
  uint64_t barrier_count;
  bool * barriers;

  void init();
  bool is_setup_done();
  bool is_done();
  bool is_warmup_done();
  void set_setup_done();
  void set_done();
  bool timeout();
  void set_starttime(uint64_t starttime);
  void process_setup_msg();
  void inc_txn_cnt();
  void inc_inflight_cnt();
  void dec_inflight_cnt();
#if CC_ALG == CALVIN && CALVIN_W
  uint64_t get_worker_epoch(uint32_t locker_id);
  void next_worker_epoch(uint32_t locker_id);
#else
  uint64_t get_worker_epoch();
  void next_worker_epoch();
#endif
  uint64_t get_seq_epoch();
  void advance_seq_epoch();
  void inc_epoch_txn_cnt();
  void decr_epoch_txn_cnt();
  double seconds_from_start(uint64_t time);
  void next_aria_phase();
};

#endif
