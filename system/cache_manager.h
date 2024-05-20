#ifndef _CACHE_MANAGER_
#define _CACHE_MANAGER_

#include "global.h"

struct WaitTxnNode {
	TxnManager * txn_man;
	WaitTxnNode * next;
	WaitTxnNode * prev;
};

struct CacheNode {
    CacheNode *prev, *next;
    row_t * row;
    uint64_t dirty_batch;
    pthread_mutex_t * locker;
    int64_t use_cache_num;
	bool is_cache_required;
	WaitTxnNode * wait_list_head;
    WaitTxnNode * wait_list_tail;
};

class CacheManager {
public:
    void init();
    void insert_to_tail(TxnManager * txn, row_t *row, uint64_t hash_id);
    void remove(TxnManager * txn, row_t *row, uint64_t hash_id);
    bool remove_a_victim(TxnManager * txn, uint64_t hash_id);
    void move_to_tail(TxnManager * txn, CacheNode * node, uint64_t hash_id);

    pthread_mutex_t ** locker;
private:
    CacheNode **head, **tail;
    uint64_t *size;
    uint64_t capacity;
};

#endif
