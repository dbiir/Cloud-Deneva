#include "data_thread.h"

void DataThread::setup() {
    if(get_thd_id() == 0) {
        send_init_done_to_all_nodes();
    }
}

RC DataThread::run() {
    // uint64_t id = get_thd_id() - g_thread_cnt - g_rem_thread_cnt - g_send_thread_cnt;
    tsetup();
    while (!simulation->is_done()) {}
    return FINISH;
}
