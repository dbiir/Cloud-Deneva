#include "data_thread.h"

void DataThread::setup() {
    if(get_thd_id() == 0) {
        send_init_done_to_all_nodes();
    }
}

RC DataThread::run() {
    tsetup();
    while (!simulation->is_done()) {
        replay.replay_log(get_thd_id());
    }
    return FINISH;
}
