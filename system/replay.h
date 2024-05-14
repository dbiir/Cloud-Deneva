#ifndef _REPLAY_H_
#define _REPLAY_H_

#include "global.h"
#include "logger.h"

class Workload;
class LogRecord;

class Replay {
public:
    void init(Workload * wl);
    void replay_enqueue(uint64_t thd_id, LogRecord * record);
    void replay_log(uint64_t thd_id);
    void notify_compute_node(uint64_t thd_id);
private:
    uint64_t txn_cnt;
    uint64_t batch_id;

    boost::lockfree::queue<LogRecord *> * log_queue;

    Workload * _wl;
};
#endif
