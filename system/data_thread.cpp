#include "data_thread.h"
#include "message.h"

void DataThread::setup() {
    if(get_thd_id() == 0) {
        send_init_done_to_all_nodes();
    }
}

RC DataThread::run() {
    tsetup();
    while (!simulation->is_done()) {
        Message * msg = replay.request_dequeue(get_thd_id());
        if (msg) {
            replay.process_request(get_thd_id(), msg);
            msg->release();
            delete msg;
        } else {
            replay.replay_log(get_thd_id());
        }
    }
    return FINISH;
}
