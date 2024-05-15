#include "replay.h"
#include "mem_alloc.h"
#include "wl.h"
#include "ycsb.h"
#include "tpcc.h"
#include "index_btree.h"
#include "index_hash.h"
#include "catalog.h"
#include "table.h"
#include "msg_queue.h"

void Replay::init(Workload * wl) {
    txn_cnt = 0;
    batch_id = 0;
    log_queue = new boost::lockfree::queue<LogRecord *>(0);
    _wl = wl;
}

void Replay::replay_enqueue(uint64_t thd_id, LogRecord * record) {
    log_queue->push(record);
}

void Replay::replay_log(uint64_t thd_id) {
    LogRecord * record;
    bool valid = log_queue->pop(record);
    if (valid) {
        if (record->rcd.iud == L_COMMIT) {
            while (true) {
                uint64_t cnt = txn_cnt;
                uint64_t new_cnt = cnt + 1;
                bool next_batch = false;
                if (new_cnt == g_replay_batch_size) {
                    new_cnt = 0;
                    next_batch = true;
                }
                if (ATOM_CAS(txn_cnt, cnt, new_cnt)) {
                    if (next_batch) {
                        batch_id ++;
                        notify_compute_node(thd_id);
                    }
                    break;
                }
            }
            
        } else {
            INDEX * index;
            table_t * table;
#if WORKLOAD == YCSB
            index = ((YCSBWorkload *)_wl)->the_index;
            table = ((YCSBWorkload *)_wl)->the_table;
#elif WORKLOAD == TPCC
            switch(record->rcd.table_id) {
                case 0:
                    index = ((TPCCWorkload *)_wl)->i_warehouse;
                    table = ((TPCCWorkload *)_wl)->t_warehouse;
                    break;
                case 1:
                    index = ((TPCCWorkload *)_wl)->i_district;
                    table = ((TPCCWorkload *)_wl)->t_district;
                    break;
                case 2:
                    index = ((TPCCWorkload *)_wl)->i_customer_id;
                    table = ((TPCCWorkload *)_wl)->t_customer;
                    break;
                case 3:
                    index = ((TPCCWorkload *)_wl)->i_history;
                    table = ((TPCCWorkload *)_wl)->t_history;
                    break;
                case 4:
                    index = ((TPCCWorkload *)_wl)->i_neworder;
                    table = ((TPCCWorkload *)_wl)->t_neworder;
                    break;
                case 5:
                    index = ((TPCCWorkload *)_wl)->i_order;
                    table = ((TPCCWorkload *)_wl)->t_order;
                    break;
                case 6:
                    index = ((TPCCWorkload *)_wl)->i_orderline;
                    table = ((TPCCWorkload *)_wl)->t_orderline;
                    break;
                case 7:
                    index = ((TPCCWorkload *)_wl)->i_item;
                    table = ((TPCCWorkload *)_wl)->t_item;
                    break;
                case 8:
                    index = ((TPCCWorkload *)_wl)->i_stock;
                    table = ((TPCCWorkload *)_wl)->t_stock;
                    break;
                default:
                    assert(false);
            }
#else
            assert(false);
#endif
            itemid_t * item;
            if (record->rcd.iud == L_UPDATE) {
                uint64_t image_size = record->rcd.image_size;
                index->index_read(record->rcd.key, item, -1, thd_id);
                row_t * row = (row_t *)item->location;
                if (row->versions[row->cur_ver].batch_id == batch_id) {
                    char * data = row->get_data();
                    int pos = row->get_schema()->get_field_index(record->rcd.start_feild_id);
                    char * after_image = (char *)mem_allocator.alloc(image_size);
                    memcpy(after_image, record->rcd.before_and_after_image + image_size, image_size);
                    memcpy(data + pos, after_image, image_size);
                    mem_allocator.free(after_image, image_size);
                } else {
                    uint64_t old_ver = row->cur_ver;
                    row->cur_ver = (row->cur_ver + 1) % g_version_cnt;
                    memcpy(row->versions[row->cur_ver].data, row->versions[old_ver].data, row->get_tuple_size());
                    row->versions[row->cur_ver].batch_id = batch_id;
                    row->data = row->versions[row->cur_ver].data;
                    char * data = row->get_data();
                    int pos = row->get_schema()->get_field_index(record->rcd.start_feild_id);
                    char * after_image = (char *)mem_allocator.alloc(image_size);
                    memcpy(after_image, record->rcd.before_and_after_image + image_size, image_size);
                    memcpy(data + pos, after_image, image_size);
                    mem_allocator.free(after_image, image_size);
                }
            } else if(record->rcd.iud == L_INSERT) {
                row_t * row;
                table->get_new_row(row, 0, record->rcd.key);
                char * data = row->get_data();
                int pos = row->get_schema()->get_field_index(record->rcd.start_feild_id);
                memcpy(data + pos, record->rcd.before_and_after_image, record->rcd.image_size);
                index->index_insert(record->rcd.key, item, -1);
            } else if (record->rcd.iud == L_DELETE) {
                index->index_read(record->rcd.key, item, -1, thd_id);
                index->index_remove(record->rcd.key, -1);
            } else {
                assert(false);
            }
        }
        mem_allocator.free(record, sizeof(LogRecord));
    }
}

void Replay::notify_compute_node(uint64_t thd_id) {
    for (uint64_t i = g_servers_per_storage * (g_node_id - g_node_cnt - g_client_node_cnt); i < g_node_cnt || i < g_servers_per_storage * (g_node_id - g_node_cnt - g_client_node_cnt + 1); i++) {
        Message * msg = Message::create_message(RPDONE);
        ((DoneMessage *)msg)->batch_id = batch_id;
        msg_queue.enqueue(thd_id, msg, i);
    }
}
