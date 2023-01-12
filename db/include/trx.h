#ifndef DB_TRX_H_
#define DB_TRX_H_

//#include "buffer.h"
#include "db.h"
#include "file.h"
#include "log.h"
#include <unordered_map>
#include <map>
#include <vector>
#include <stdint.h>
#include <set>
#include <pthread.h>
#include <map>

typedef struct lock_t lock_t;

const int SHARED = 0;
const int EXCLUSIVE = 1;

namespace std {
    template<>
    struct hash<pair<int64_t, pagenum_t>> {
        std::size_t operator()(const pair<int64_t, pagenum_t> &k) const {
            using std::hash;

            return (hash<int64_t>()(k.first) ^ (hash<pagenum_t>()(k.second) << 1));
        }
    };
    template<>
    struct hash<pair<pair<int64_t, pagenum_t>, int64_t>> {
        std::size_t operator()(const pair<pair<int64_t, pagenum_t>, int> &k) const {
            using std::hash;

            return ((hash<int64_t>()(k.first.first) ^ (hash<pagenum_t>()(k.first.second) << 1)) ^ (hash<int>()(k.second) << 2));
        }
    };
}

struct hash_entry {
    int64_t table_id;
    pagenum_t page_id;
    lock_t *tail;
    lock_t *head;
};

struct lock_t {
    /* GOOD LOCK :) */
    lock_t *prev;
    lock_t *next;
    hash_entry *sentinel;
    pthread_cond_t conditional_variable;
    int record_id;
    int lock_mode;
    lock_t *trx_next_lock = nullptr;
    int owner_trx_id;
    uint64_t record_bitmap;
};

struct trx_t {
    trx_t();

    lock_t *last_lock;
    std::unordered_map<std::pair<int64_t, pagenum_t>, lock_t *[2]> lock_bucket;
    lock_t *temp_lock;
    std::vector<int> wait_graph;
//    std::map<std::pair<std::pair<int64_t, pagenum_t>, int64_t>, rollback_log_t *> rollback_logs;
    LSN_t last_LSN;
    std::set<log_t *> logs;
};

/* APIs for lock table */
int init_lock_table();

void get_trx_latch();

void unlock_trx_latch();

// Just call lock_acquire()
int implicit_to_explicit(int64_t table_id,pagenum_t page_id,int64_t key,int trx_id);

lock_t *lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode);

int lock_release(lock_t *lock_obj);

int update_wait_graph(lock_t *lock);

int find_cycle(int trx_id, int start_point);

bool find_cycle_help(int vertex);

int abort_trx(int trx_id);

// if trx has specific lock_t return it otherwise return nullptr
lock_t *get_trx_lock(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id);

// lock_t *upgrade_lock(lock_t *lock_obj); // I used it in milestone 1, but change design in milestone 2.

// find next lock_t has same record id in lock entry if there is no such thing return nullptr
lock_t *get_next_same_lock(lock_t *lock_obj);

lock_t *find_first_lock(int64_t table_id, pagenum_t page_id, int64_t key);

bool check_trx_alive(int trx_id);

bool can_work(lock_t *lock_obj);

// 락을 지운 이후에 남은 락이 없으면 1 아니면 0을 리턴
int remove_lock(lock_t *lock_obj);

int append_lock_to_tail(lock_t *lock_obj);

//int add_trx_log(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, char *value, uint16_t length);

int trx_begin();

int trx_commit(int trx_id);

int trx_abort(int trx_id);

void release_help(lock_t *lock_obj);

//void abort_help(lock_t *lock_obj);

bool check_bitmap(uint64_t bitmap, int index);

lock_t *get_next_same_lock(lock_t *lock_obj, int index);

bool can_work(lock_t *lock_obj, int index);

int update_wait_graph(lock_t *lock, int index);

// if lock is redundant merge lock, delete temp_bucket and free lock.
int merge_lock(lock_t *lock);

#endif /* DB_TRX_H_ */
