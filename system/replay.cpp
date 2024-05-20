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
    request_queue = new boost::lockfree::queue<Message *>(0);
    _wl = wl;
}

//[ ]: 加入统计信息
void Replay::replay_enqueue(uint64_t thd_id, LogRecord * record) {
    while(!log_queue->push(record) && !simulation->is_done()) {}
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
                    row->versions[row->cur_ver].valid_until = batch_id;
                    uint64_t old_ver = row->cur_ver;
                    row->cur_ver = (row->cur_ver + 1) % g_version_cnt;
                    memcpy(row->versions[row->cur_ver].data, row->versions[old_ver].data, row->get_tuple_size());
                    row->data = row->versions[row->cur_ver].data;
                    char * data = row->get_data();
                    int pos = row->get_schema()->get_field_index(record->rcd.start_feild_id);
                    char * after_image = (char *)mem_allocator.alloc(image_size);
                    memcpy(after_image, record->rcd.before_and_after_image + image_size, image_size);
                    memcpy(data + pos, after_image, image_size);
                    mem_allocator.free(after_image, image_size);
                    row->versions[row->cur_ver].batch_id = batch_id;
                    row->versions[row->cur_ver].valid_until = UINT64_MAX;
                }
            } else if(record->rcd.iud == L_INSERT) {
                row_t * row;
                table->get_new_row(row, 0, record->rcd.key);
                row->set_primary_key(record->rcd.key);
                char * data = row->get_data();
                int pos = row->get_schema()->get_field_index(record->rcd.start_feild_id);
                memcpy(data + pos, record->rcd.before_and_after_image, record->rcd.image_size);
                item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t));
                item->init();
                item->type = DT_row;
	            item->location = row;
	            item->valid = true;
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

/*--------------------------------
------------read request----------
----------------------------------*/
//[ ]: 加入统计信息
void Replay::request_enqueue(uint64_t thd_id, Message * msg) {
    while(!request_queue->push(msg) && !simulation->is_done()) {}
}
//[ ]: 加入统计信息
Message * Replay::request_dequeue(uint64_t thd_id) {
    Message * msg;
    bool valid = request_queue->pop(msg);
    if (valid) {
        return msg;
    }
    return NULL;
}

void Replay::process_request(uint64_t thd_id, Message * msg) {
    RStorageMessage * rmsg = (RStorageMessage *)msg;
    for (uint64_t i = 0; i < rmsg->table_ids.size(); i++) {
#if WORKLOAD == YCSB
        INDEX * index = ((YCSBWorkload *)_wl)->the_index;
#elif WORKLOAD == TPCC
        INDEX * index;
        switch (rmsg->table_ids[i]) {
            case 0:
                index = ((TPCCWorkload *)_wl)->i_warehouse;
                break;
            case 1:
                index = ((TPCCWorkload *)_wl)->i_district;
                break;
            case 2:
                index = ((TPCCWorkload *)_wl)->i_customer_id;
                break;
            case 3:
                index = ((TPCCWorkload *)_wl)->i_history;
                break;
            case 4:
                index = ((TPCCWorkload *)_wl)->i_neworder;
                break;
            case 5:
                index = ((TPCCWorkload *)_wl)->i_order;
                break;
            case 6:
                index = ((TPCCWorkload *)_wl)->i_orderline;
                break;
            case 7:
                index = ((TPCCWorkload *)_wl)->i_item;
                break;
            case 8:
                index = ((TPCCWorkload *)_wl)->i_stock;
                break;
            default:
                assert(false);
        }
#endif
        uint64_t key = rmsg->keys[i];
        itemid_t * item;
        index->index_read(key, item, -1, thd_id);
        // row_t * row = (row_t *)item->location;
    }
    Message * rsp_msg = Message::create_message(RSTO_RSP);
    rsp_msg->batch_id = msg->batch_id;
    rsp_msg->txn_id = msg->txn_id;
    msg_queue.enqueue(thd_id, rsp_msg, msg->return_node_id);
}
    