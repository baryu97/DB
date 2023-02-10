#include "trx.h"
#include "log.h"

#include <unordered_map>
#include <map>
#include <vector>
#include <iostream>
#include <list>

#define DEBUG_MODE 0
#define MAX_RECORD 64

using namespace std;

int trx_count = 0;
list<pair<int, struct trx_t *>> trx_table;
pthread_mutex_t trx_manager_latch;
unordered_map<int, int> visited;

unordered_map<pair<int64_t, pagenum_t>, hash_entry *> hash_table;

trx_t * find_trx(int trx_id){
    for (auto & i : trx_table) {
        if (i.first == trx_id)
            return i.second;
    }
    // exit(1);
    // cout << "trere is no trx : " << trx_id << endl;
    return nullptr;
}

void delete_trx(int trx_id){
    for (auto i = trx_table.begin(); i != trx_table.end(); ++i){
        if (i->first == trx_id){
            i = trx_table.erase(i);
            return;
        }
    }
}

void print_lock_table(int64_t table_id, pagenum_t page_id){
    hash_entry *bucket; 
    if (hash_table.count({table_id,page_id})){
        bucket = hash_table[{table_id,page_id}];
    } else
        return;
    cout << "lock table : ";
    for (auto cur = bucket->head; cur != nullptr; cur = cur->next){
        cout << "trx_id : " << cur->owner_trx_id << ", lock_mode : " << cur->lock_mode << " ";
    }
    cout << "\n";
}

int trx_begin() {
    pthread_mutex_lock(&trx_manager_latch);
    trx_table.push_front({++trx_count, new trx_t()});
    int trx_id = trx_count;
//    trx_table[trx_count]->wait_graph.push_back(0);
    append_log(trx_id,BEGIN);
    pthread_mutex_unlock(&trx_manager_latch);
    return trx_id;
}

int trx_commit(int trx_id) {
    pthread_mutex_lock(&trx_manager_latch);
    #if DEBUG_MODE
    cout << "Commit trx_id : " << trx_id << "\n";
    #endif
    trx_t * trx = find_trx(trx_id);
    if (trx->temp_lock != nullptr){
        cout << "error : (trx commit)\n";
        lock_release(trx->temp_lock);
    }
    lock_t *cur = trx->last_lock;
    release_help(cur);
    append_log(trx_id,COMMIT);
    flush_log();
    for (log_t * log : trx->logs) {
        delete log;
    }
    trx->lock_bucket.clear();
    trx->wait_graph.clear();
    delete trx;
    delete_trx(trx_id);
    pthread_mutex_unlock(&trx_manager_latch);
    return trx_id;
}

int trx_abort(int trx_id){
    pthread_mutex_lock(&trx_manager_latch);
    int ret_val = abort_trx(trx_id);
    pthread_mutex_unlock(&trx_manager_latch);
    return trx_id;
}

int init_lock_table() {
    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&trx_manager_latch, &mattr);
    trx_table.clear();
    return 0;
}

lock_t *lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode) {
    pthread_mutex_lock(&trx_manager_latch);
//    if (!trx_table.count(trx_id)){
//        cout << "error : get lock trx_id is not valid" << trx_id << "\n";
//        return nullptr;
//    }
    pair<int64_t, pagenum_t> key_pair = {table_id, page_id};
    trx_t * trx = find_trx(trx_id);
    if (hash_table.find(key_pair) == hash_table.end()) {
        hash_table[key_pair] = new hash_entry;
        hash_table[key_pair]->table_id = table_id;
        hash_table[key_pair]->page_id = page_id;
        hash_table[key_pair]->head = nullptr;
        hash_table[key_pair]->tail = nullptr;
    } 
    lock_t *lock = get_trx_lock(table_id, page_id, key, trx_id);
    if (lock == nullptr || (lock->lock_mode == SHARED && lock_mode == EXCLUSIVE)) {
        if (!trx->lock_bucket.count(key_pair)){
            trx->lock_bucket[key_pair][SHARED] = nullptr;
            trx->lock_bucket[key_pair][EXCLUSIVE] = nullptr;
        }
        lock_t *cur = trx->lock_bucket[key_pair][lock_mode];
        if (cur == nullptr){
            lock = new lock_t;

            pthread_cond_init(&lock->conditional_variable, nullptr);
            lock->sentinel = hash_table[key_pair];
            lock->record_id = key;
            lock->lock_mode = lock_mode;
            lock->owner_trx_id = trx_id;
            lock->record_bitmap = ((uint64_t)1 << key);

            append_lock_to_tail(lock);

            lock->trx_next_lock = trx->last_lock;
            trx->last_lock = lock;
            trx->lock_bucket[key_pair][lock_mode] = lock;
        }
        while (cur != nullptr){
            if (check_bitmap(cur->record_bitmap,key) && !(lock_mode == SHARED && cur->lock_mode == SHARED) && trx_id != cur->owner_trx_id){
                break;
            }
            cur = cur->next;
        }
        if (cur == nullptr){
            lock = trx->lock_bucket[key_pair][lock_mode];
            lock->record_bitmap |= ((uint64_t)1 << key);
        } else {
            lock = new lock_t;

            pthread_cond_init(&lock->conditional_variable, nullptr);
            lock->sentinel = hash_table[key_pair];
            lock->record_id = key;
            lock->lock_mode = lock_mode;
            lock->owner_trx_id = trx_id;
            lock->record_bitmap = ((uint64_t)1 << key);

            append_lock_to_tail(lock);
            if (trx->temp_lock != nullptr){
                cout << "error : (lock_acquire)\n";
            }
            trx->temp_lock = lock;
        }
    }
    #if DEBUG_MODE
    print_lock_table(table_id,page_id);
    #endif
    if (update_wait_graph(lock,key) == -1){
        #if DEBUG_MODE
        cout << "Abort trx_id : " << trx_id << "\n";
        #endif
        abort_trx(trx_id);
        pthread_mutex_unlock(&trx_manager_latch);
        return nullptr;
    }

    if (!can_work(lock,key)) {
        #if DEBUG_MODE
        cout << "Sleep trx_id : " << trx_id << "\n";
        // print_lock_table(table_id,page_id);
        #endif
        pthread_cond_wait(&lock->conditional_variable, &trx_manager_latch);
        merge_lock(lock);
    }

    pthread_mutex_unlock(&trx_manager_latch);
    return lock;
}

int lock_release(lock_t *lock_obj) {
    if (lock_obj == nullptr){
        cout << "error : lock_obj is null(release)\n";
        exit(1);
    }
    lock_t *next_wake[MAX_RECORD];
    for (int i = 0; i < MAX_RECORD; i++){
        if (check_bitmap(lock_obj->record_bitmap, i))
            next_wake[i] = get_next_same_lock(lock_obj, i);
        else
            next_wake[i] = nullptr;
    }
    if (remove_lock(lock_obj) == 1) {
        hash_table.erase({lock_obj->sentinel->table_id,lock_obj->sentinel->page_id});
        delete lock_obj->sentinel;
    }
    for (int i = 0; i < MAX_RECORD; i++){
        if (next_wake[i] != nullptr && next_wake[i]->owner_trx_id != lock_obj->owner_trx_id) {
            #if DEBUG_MODE
            cout << "Wake up record id : " << i << "\n";
            #endif
            // case * -> E
            if (next_wake[i]->lock_mode == EXCLUSIVE) {
                if (can_work(next_wake[i],i)) {
                    #if DEBUG_MODE
                    cout << "Wake up x trx id : " << next_wake[i]->owner_trx_id << "\n";
                    #endif
                    pthread_cond_signal(&next_wake[i]->conditional_variable);
                }
            } else {
                // case E -> S S S S
                if (lock_obj->lock_mode == EXCLUSIVE) {
                    while (next_wake[i] != nullptr && next_wake[i]->lock_mode == SHARED) {
                        #if DEBUG_MODE
                        cout << "Wake up s trx id : " << next_wake[i]->owner_trx_id << "\n";
                        #endif
                        if (can_work(next_wake[i],i)){
                            pthread_cond_signal(&next_wake[i]->conditional_variable);
                        }
                        next_wake[i] = get_next_same_lock(next_wake[i],i);
                    }
                } else {
                    // case S -> S (don't need to do anything)
                    lock_t *tmp = get_next_same_lock(next_wake[i],i);
                    if (tmp != nullptr && tmp->owner_trx_id == next_wake[i]->owner_trx_id && can_work(tmp,i)){
                        #if DEBUG_MODE
                        cout << "Wake up sx trx id : " << next_wake[i]->owner_trx_id << "\n";
                        #endif
                        pthread_cond_signal(&tmp->conditional_variable);
                    }
                }
            }
        }
    }
    delete lock_obj;
    return 0;
}

lock_t *get_trx_lock(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id) {
    trx_t * trx = find_trx(trx_id);
    if (!trx->lock_bucket.count({table_id, page_id})){
        // cout << "error : get trx lock trx_id is not valid" << trx_id << "\n";
        return nullptr;
    }
    if (trx->lock_bucket[{table_id, page_id}][EXCLUSIVE] != nullptr &&
        check_bitmap(trx->lock_bucket[{table_id, page_id}][EXCLUSIVE]->record_bitmap,key))
        return trx->lock_bucket[{table_id, page_id}][EXCLUSIVE];
    if (trx->lock_bucket[{table_id, page_id}][SHARED] != nullptr &&
        check_bitmap(trx->lock_bucket[{table_id, page_id}][SHARED]->record_bitmap,key))
        return trx->lock_bucket[{table_id, page_id}][SHARED];
    return nullptr;
}

lock_t *upgrade_lock(lock_t *lock_obj) {
    #if DEBUG_MODE
    // cout << "Upgrade Lock\n";
    #endif
    lock_obj->lock_mode = EXCLUSIVE;
    lock_t *next = get_next_same_lock(lock_obj);
    if (remove_lock(lock_obj) == 1) {
        lock_obj->sentinel->head = lock_obj;
        lock_obj->sentinel->tail = lock_obj;
        return lock_obj;
    }
    while (next != nullptr && next->lock_mode != EXCLUSIVE) {
        next = get_next_same_lock(next);
    }
    if (next == nullptr) {
        append_lock_to_tail(lock_obj);
    } else {
        lock_obj->prev = next->prev;
        lock_obj->next = next;
        if (next->prev != nullptr) {
            next->prev->next = lock_obj;
        } else {
            lock_obj->sentinel->head = lock_obj;
        }
        next->prev = lock_obj;
    }
    return lock_obj;
}

lock_t *get_next_same_lock(lock_t *lock_obj) {
    if (lock_obj == nullptr){
        cout << "error : lock_obj is null(get_next)\n";
        exit(1);
    }
    lock_t *cur = lock_obj->next;
    while (cur != nullptr) {
        if (cur->record_id == lock_obj->record_id) {
            break;
        }
        cur = cur->next;
    }
    return cur;
}

lock_t *get_next_same_lock(lock_t *lock_obj,int key) {
    if (lock_obj == nullptr){
        cout << "error : lock_obj is null(get_next)\n";
        exit(1);
    }
    lock_t *cur = lock_obj->next;
    while (cur != nullptr) {
        if (check_bitmap(cur->record_bitmap,key)
        //  && cur->owner_trx_id != lock_obj->owner_trx_id)
        )
            break;
        cur = cur->next;
    }
    return cur;
}

bool can_work(lock_t *lock_obj) {
    if (lock_obj == nullptr){
        cout << "error : lock_obj is null(can_work)\n";
        exit(1);
    }
    lock_t *cur = lock_obj->sentinel->head;
    bool first_lock = true;
    while (cur != lock_obj && first_lock) {
        if (cur->record_id == lock_obj->record_id) {
            if (lock_obj->lock_mode != SHARED || cur->lock_mode != SHARED)
                first_lock = false;
        }
        cur = cur->next;
    }
    return first_lock;
}

bool can_work(lock_t *lock_obj, int key) {
    if (lock_obj == nullptr){
        cout << "error : lock_obj is null(can_work)\n";
        exit(1);
    }
    lock_t *cur = lock_obj->sentinel->head;
    bool first_lock = true;
    while (cur != nullptr && cur != lock_obj && first_lock) {
        if (check_bitmap(cur->record_bitmap,key) && cur->owner_trx_id != lock_obj->owner_trx_id) {
            if (lock_obj->lock_mode != SHARED || cur->lock_mode != SHARED)
                first_lock = false;
        }
        cur = cur->next;
    }
    if (cur == nullptr)
        exit(1);
    return first_lock;
}

int update_wait_graph(lock_t *lock) {
    lock_t *cur = lock->prev;
    trx_t * trx = find_trx(lock->owner_trx_id);
    int start_edge = trx->wait_graph.size();
    bool se_flag = false;
    while (cur != nullptr) {
        if (cur->record_id == lock->record_id) {
            if (cur->lock_mode == SHARED) {
                // case S -> S
                // keep find exclusive lock
                // case S -> E
                if (lock->lock_mode == EXCLUSIVE) {
                    se_flag = true;
                    trx->wait_graph.push_back(cur->owner_trx_id);
                }
            } else {
                // case E -> *
                if (se_flag) {
                    break;
                }
                trx->wait_graph.push_back(cur->owner_trx_id);
                break;
            }
        }
        cur = cur->prev;
    }
    return find_cycle(lock->owner_trx_id, start_edge);
}

int update_wait_graph(lock_t *lock, int key) {
    lock_t *cur = lock->prev;
    trx_t * trx = find_trx(lock->owner_trx_id);
    int start_edge = trx->wait_graph.size();
    bool se_flag = false;
    while (cur != nullptr) {
        if (check_bitmap(cur->record_bitmap, key) && cur->owner_trx_id != lock->owner_trx_id) {
            if (cur->lock_mode == SHARED) {
                // case S -> S
                // keep find exclusive lock
                // case S -> E
                if (lock->lock_mode == EXCLUSIVE) {
                    se_flag = true;
                    trx->wait_graph.push_back(cur->owner_trx_id);
                    #if DEBUG_MODE
                    cout << "add wait graph : " << lock->owner_trx_id << " to " << cur->owner_trx_id << "\n";
                    #endif
                }
            } else {
                // case E -> *
                if (se_flag) {
                    break;
                }
                trx->wait_graph.push_back(cur->owner_trx_id);
                #if DEBUG_MODE
                cout << "add wait graph : " << lock->owner_trx_id << " to " << cur->owner_trx_id << "\n";
                #endif
                break;
            }
        }
        cur = cur->prev;
    }
    return find_cycle(lock->owner_trx_id, start_edge);
}


int find_cycle(int trx_id, int start_point) {
    visited.clear();
    trx_t * trx = find_trx(trx_id);
    for (int i = start_point; i < trx->wait_graph.size(); ++i) {
    // for (int i = 0; i < trx->wait_graph.size(); ++i) {
        visited[trx->wait_graph[i]] = -1;
        for (int next_vertex: find_trx(trx->wait_graph[i])->wait_graph) {
            if (find_trx(next_vertex) == nullptr)
                continue;
            if (find_cycle_help(next_vertex)) {
                return -1;
            }
        }
        visited[trx->wait_graph[i]] = 1;
    }
    // for (int i : trx->wait_graph){
    //     visited[i] = -1;
    //     for (int next_vertex: trx_table[i]->wait_graph) {
    //         if (trx_table.count(next_vertex) == 0)
    //             continue;
    //         if (find_cycle_help(next_vertex)) {
    //             return -1;
    //         }
    //     }
    //     visited[i] = 1;
    // }
    return 0;
}

bool find_cycle_help(int vertex) {
    if (visited.find(vertex) != visited.end()) {
        if (visited[vertex] == -1) {
            return true;
        }
        return false;
    }

    visited[vertex] = -1;
    trx_t * trx = find_trx(vertex);
    for (int next_vertex: trx->wait_graph) {
        if (find_trx(next_vertex) == nullptr)
            continue;
        if (find_cycle_help(next_vertex)) {
            return true;
        }
    }
    visited[vertex] = 1;

    return false;
}

trx_t::trx_t() {
    last_lock = nullptr;
    temp_lock = nullptr;
    last_LSN = -1;
}

int remove_lock(lock_t *lock_obj){
    if (lock_obj == nullptr){
        cout << "error : lock_obj is null(remove)\n";
        exit(1);
    }
    if (lock_obj->sentinel->head == lock_obj->sentinel->tail) {
        lock_obj->sentinel->head = nullptr;
        lock_obj->sentinel->tail = nullptr;
        if (lock_obj->next != nullptr || lock_obj->prev != nullptr){
            cout << "error : remove one lock\n";
            exit(1);
        }
        return 1;
    }
    if (lock_obj->prev != nullptr) {
        lock_obj->prev->next = lock_obj->next;
    } else {
        if (lock_obj->sentinel->head != lock_obj){
            cout << "error : remove head\n";
            exit(1);
        }
        lock_obj->sentinel->head = lock_obj->next;
        lock_obj->sentinel->head->prev = nullptr;
    }
    if (lock_obj->next != nullptr) {
        lock_obj->next->prev = lock_obj->prev;
    } else {
        if (lock_obj->sentinel->tail != lock_obj){
            cout << "error : remove tail\n";
            exit(1);
        }
        lock_obj->sentinel->tail = lock_obj->prev;
        lock_obj->sentinel->tail->next = nullptr;
    }
    return 0;
}

int append_lock_to_tail(lock_t *lock_obj){
    if (lock_obj == nullptr){
        cout << "error : lock_obj is null(append)\n";
        exit(1);
    }
    lock_obj->next = nullptr;
    lock_obj->prev = lock_obj->sentinel->tail;
    if (lock_obj->sentinel->tail == nullptr){
        if (lock_obj->sentinel->head != nullptr){
            cout << "error : sentinel head wrong!(append)\n";
            exit(1);
        }
        lock_obj->sentinel->head = lock_obj;
    } else {
        lock_obj->sentinel->tail->next = lock_obj;
    }
    lock_obj->sentinel->tail = lock_obj;
    return 0;
}

/*int add_trx_log(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, char *value, uint16_t length){
    #if DEBUG_MODE
    cout << "table_id : " << table_id << " page_id : " << page_id << " key : " << key << " trx_id : " << trx_id <<"\n";
    #endif
    if (trx_table[trx_id]->rollback_logs.count({{table_id,page_id},key})){
        return 0;
    }
    auto log_obj = new rollback_log_t();
    log_obj->before = new char[length];
    log_obj->length = length;
    memcpy(log_obj->before, value, length);
    trx_table[trx_id]->rollback_logs[{{table_id,page_id},key}] = log_obj;
    return 0;
}*/

void release_help(lock_t *lock_obj){
    if (lock_obj == nullptr){
        return;
    }
    release_help(lock_obj->trx_next_lock);
    lock_release(lock_obj);
}

/*void abort_help(lock_t *lock_obj){
    if (lock_obj == nullptr){
        return;
    }
    abort_help(lock_obj->trx_next_lock);
    for (int i = 0; i < MAX_RECORD; ++i){
        if (lock_obj->lock_mode == EXCLUSIVE && trx_table[lock_obj->owner_trx_id]->rollback_logs.count({{lock_obj->sentinel->table_id,lock_obj->sentinel->page_id},i})){
        // if (lock_obj->lock_mode == EXCLUSIVE){
            #if DEBUG_MODE
            cout << "rollback record = " << i << ", trx_id = " << lock_obj->owner_trx_id << "\n";
            #endif
            rollback_update(lock_obj->sentinel->table_id,lock_obj->sentinel->page_id,i,trx_table[lock_obj->owner_trx_id]->rollback_logs[{{lock_obj->sentinel->table_id,lock_obj->sentinel->page_id},i}],lock_obj->owner_trx_id);
        }
    }
    lock_release(lock_obj);
}*/

int abort_trx(int trx_id){
    trx_t * trx = find_trx(trx_id);
    if (find_trx(trx_id) == nullptr){
        cout << "error : trx_id is not valid " << trx_id << "\n";
        return 0;
    }
    lock_t *cur = trx->last_lock;
    if (trx->temp_lock != nullptr){
        lock_release(trx->temp_lock);
    }
    for (auto i = trx->logs.rbegin(); i != trx->logs.rend(); ++i) {
        rollback_log(*i);
    }
//    abort_help(cur);
    release_help(cur);
    append_log(trx_id,ROLLBACK);
    flush_log();
    trx->lock_bucket.clear();
    trx->wait_graph.clear();
    for (log_t * log : trx->logs) {
        delete log;
    }
    delete trx;
    delete_trx(trx_id);
    return trx_id;
}

lock_t *find_first_lock(int64_t table_id, pagenum_t page_id, int64_t key){
    pthread_mutex_lock(&trx_manager_latch);
    if (!hash_table.count({table_id,page_id})){
        pthread_mutex_unlock(&trx_manager_latch);
        return nullptr;
    }    
    auto cur = hash_table[{table_id,page_id}]->head;
    while (cur != nullptr){
        if (check_bitmap(cur->record_bitmap, key)){
            break;
        }
        cur = cur->next;
    }
    pthread_mutex_unlock(&trx_manager_latch);
    return cur;
}

void get_trx_latch(){
    pthread_mutex_lock(&trx_manager_latch);
}

void unlock_trx_latch(){
    pthread_mutex_unlock(&trx_manager_latch);
}

int implicit_to_explicit(int64_t table_id,pagenum_t page_id,int64_t key,int trx_id){
    #if DEBUG_MODE
    cout << "implicit to explicit trx_id : " << trx_id << "\n";
    #endif
    if (!check_trx_alive(trx_id))
        cout << "error (implicit_to_explicit)\n";
    if (lock_acquire(table_id,page_id,key,trx_id,EXCLUSIVE) == nullptr){
        cout << "error (implicit_to_explicit)\n";
        return -1;
    }
    return 0;
}

bool check_trx_alive(int trx_id){
    if (find_trx(trx_id) != nullptr){
        return true;
    }
    return false;
}

bool check_bitmap(uint64_t bitmap, int index){
    return bitmap & ((uint64_t)1 << index);
}

int merge_lock(lock_t *lock_obj){
    int trx_id = lock_obj->owner_trx_id;
    trx_t * trx = find_trx(trx_id);
    pair<int64_t,pagenum_t> key_pair = {lock_obj->sentinel->table_id,lock_obj->sentinel->page_id};
    if (trx->temp_lock == lock_obj){
        trx->lock_bucket[key_pair][lock_obj->lock_mode]->record_bitmap |= lock_obj->record_bitmap;
        trx->temp_lock = nullptr;
        remove_lock(lock_obj);
        delete lock_obj;
    }
    return 0;
}

