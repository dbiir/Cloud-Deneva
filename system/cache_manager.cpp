#include "cache_manager.h"
#include "mem_alloc.h"
#include "row.h"

void CacheManager::init() {
    head = (CacheNode **)mem_allocator.alloc(sizeof(CacheNode *) * g_cache_list_num);
    tail = (CacheNode **)mem_allocator.alloc(sizeof(CacheNode *) * g_cache_list_num);
    size = (uint64_t *)mem_allocator.alloc(sizeof(uint64_t) * g_cache_list_num);
    this->capacity = g_cache_max_row / g_cache_list_num;
    locker = (pthread_mutex_t **) mem_allocator.alloc(sizeof(pthread_mutex_t *) * g_cache_list_num);
    for (uint64_t i = 0; i < g_cache_list_num; i ++) {
        head[i] = NULL;
        tail[i] = NULL;
        size[i] = 0;
        locker[i] = (pthread_mutex_t *) mem_allocator.alloc(sizeof(pthread_mutex_t));
        pthread_mutex_init(locker[i], NULL);
    }
}

// Txn should get the node lock before calling this function
void CacheManager::insert_to_tail(TxnManager * txn, row_t *row, uint64_t hash_id) {
    pthread_mutex_lock(locker[hash_id]);
    CacheNode * node = row->cache_node;
    if (size[hash_id] >= capacity) {
        while (remove_a_victim(txn, hash_id)) {}
    }
    bool valid = ATOM_CAS(node->use_cache_num, -1, 0);
    assert(valid);
    node->dirty_batch = 0;
    size[hash_id] ++;
    LIST_PUT_TAIL(head[hash_id], tail[hash_id], node);
    pthread_mutex_unlock(locker[hash_id]);
}

// Txn should get the node lock before calling this function
void CacheManager::remove(TxnManager * txn, row_t *row, uint64_t hash_id) {
    pthread_mutex_lock(locker[hash_id]);
    CacheNode * node = row->cache_node;
    bool valid = ATOM_CAS(node->use_cache_num, 1, -1);
    assert(valid);
    LIST_REMOVE_HT(node, head[hash_id], tail[hash_id]);
    size[hash_id] --;
    pthread_mutex_unlock(locker[hash_id]);
}

// Txn should get the node lock before calling this function
void CacheManager::move_to_tail(TxnManager * txn, CacheNode * node, uint64_t hash_id) {
    if (tail[hash_id] == node) {
        return;
    }
    pthread_mutex_lock(locker[hash_id]);
    LIST_REMOVE_HT(node, head[hash_id], tail[hash_id]);
    LIST_PUT_TAIL(head[hash_id], tail[hash_id], node);
    pthread_mutex_unlock(locker[hash_id]);
}

// already lock in the caller
bool CacheManager::remove_a_victim(TxnManager * txn, uint64_t hash_id) {
    CacheNode * node = head[hash_id];
    while (node != NULL) {
        if (node->dirty_batch <= simulation->flushed_batch && node->use_cache_num == 0) {
            pthread_mutex_lock(node->locker);
            if (node->use_cache_num == 0) {
                if (ATOM_CAS(node->use_cache_num, 0, -1)) {
                    LIST_REMOVE_HT(node, head[hash_id], tail[hash_id]);
                    size[hash_id] --;
                    pthread_mutex_unlock(node->locker);
                    return true;
                }
            }
            pthread_mutex_unlock(node->locker);
        }
        node = node->next;
    }
    return false;
    // printf("Error: no cache node to remove\n");
    // assert(false);
}
