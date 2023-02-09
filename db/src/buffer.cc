#include "buffer.h"
#include "log.h"
#include <cstring>    

#include <iostream>            
#include <unordered_map>
        
#define DEBUG_MODE 0                                             

using namespace std;

namespace std {
    template<>
    struct hash<pair<int64_t, pagenum_t>> {
        std::size_t operator()(const pair<int64_t, pagenum_t> &k) const {
            using std::hash;

            return (hash<int64_t>()(k.first) ^ (hash<pagenum_t>()(k.second) << 1));
        }
    };
}

pthread_mutex_t buffer_manager_latch;
Buffer *head = nullptr;
int max_size;
int cur_size = 0;
unordered_map<pair<int64_t, pagenum_t>, Buffer *> buffer_hash;
// extern unordered_map<int64_t,int> fd_table;              

int buffer_init(int num_buf) {
    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&buffer_manager_latch, 0);
    if (head == nullptr) {
        cur_size = 0;
        max_size = num_buf;
        return 0;
    }
    return -1;
}


pagenum_t buffer_alloc_page(int64_t table_id) {
    Buffer *head_page = buffer_find(table_id, 0);
    pagenum_t alloc_page_num;
    if (head_page == nullptr) {
        return file_alloc_page(fd_table[table_id]);
    }
    if (head_page->is_dirty) {
        file_write_page(fd_table[table_id], 0, &head_page->frame);
        head_page->is_dirty = false;
    }
    alloc_page_num = file_alloc_page(fd_table[table_id]);
    file_read_page(fd_table[table_id], 0, &head_page->frame);
    return alloc_page_num;
}

void buffer_free_page(int64_t table_id, pagenum_t pagenum) { //free 한 페이지가 버퍼에 있는 경우
    Buffer *head_page = buffer_find(table_id, 0);
    Buffer *free = buffer_find(table_id, pagenum);
    if (free != nullptr) {
        free->is_dirty = false;
//        free->is_pinned = false;
    }
    if (head_page == nullptr) {
        return file_free_page(fd_table[table_id], pagenum);
    }
    if (head_page->is_dirty) {
        file_write_page(fd_table[table_id], 0, &head_page->frame);
        head_page->is_dirty = false;
    }
    file_free_page(fd_table[table_id], pagenum);
    file_read_page(fd_table[table_id], 0, &head_page->frame);
}

int page_latch_unlock(int64_t table_id,pagenum_t pagenum){
    pthread_mutex_lock(&buffer_manager_latch);
    Buffer *buffer = buffer_find(table_id,pagenum);
    pthread_mutex_unlock(&buffer->page_latch);
    pthread_mutex_unlock(&buffer_manager_latch);
    return 0;
}

void buffer_read_page(int64_t table_id, pagenum_t pagenum,
                      struct page_t *dest) {
    pthread_mutex_lock(&buffer_manager_latch);
    Buffer *src = buffer_find(table_id, pagenum);
    if (src == nullptr) {
        src = buffer_register(table_id, pagenum);
    }
    if (src != head) {
        src->next->prev = src->prev;
        src->prev->next = src->next;
        src->next = head;
        src->prev = head->prev;
        head->prev->next = src;
        head->prev = src;
        head = src;
    }
    memcpy(dest, &src->frame, PAGE_SIZE);
    pthread_mutex_unlock(&buffer_manager_latch); 
}

void buffer_read_page(int64_t table_id, pagenum_t pagenum,
                      struct page_t *dest, bool is_leaf) {
    pthread_mutex_lock(&buffer_manager_latch);
    #if DEBUG_MODE
    cout << "hold buffer latch : " << pagenum << "\n";
    #endif
    Buffer *src = buffer_find(table_id, pagenum);
    if (src == nullptr) {
        src = buffer_register(table_id, pagenum);
    }
    if (pthread_mutex_trylock(&src->page_latch) != 0){
        pthread_mutex_unlock(&buffer_manager_latch);  
        while (pthread_mutex_trylock(&src->page_latch) != 0);
        if (src->table_id != table_id || src->page_num != pagenum){
            pthread_mutex_unlock(&src->page_latch);
            pthread_mutex_lock(&buffer_manager_latch);  
            src = buffer_register(table_id, pagenum);
            pthread_mutex_lock(&src->page_latch);
            pthread_mutex_unlock(&buffer_manager_latch);  
        }
    } else {
        pthread_mutex_unlock(&buffer_manager_latch);
    }
    #if DEBUG_MODE
    cout << "hold page latch : " << pagenum << "\n";
    #endif
    pthread_mutex_lock(&buffer_manager_latch);  
    if (src != head) {
        src->next->prev = src->prev;
        src->prev->next = src->next;
        src->next = head;
        src->prev = head->prev;
        head->prev->next = src;
        head->prev = src;
        head = src;
    }
    pthread_mutex_unlock(&buffer_manager_latch);  
    memcpy(dest, &src->frame, PAGE_SIZE);
    if (!is_leaf){
        pthread_mutex_unlock(&src->page_latch);
    }
}

void buffer_write_page(int64_t table_id, pagenum_t pagenum,
                       const struct page_t *src) {
    pthread_mutex_lock(&buffer_manager_latch);
    Buffer *dest = buffer_find(table_id, pagenum);
    if (dest == nullptr) {
        dest = buffer_register(table_id, pagenum);
    }
    dest->is_dirty = true;
    memcpy(&dest->frame, src, PAGE_SIZE);
    pthread_mutex_unlock(&buffer_manager_latch);
}

void buffer_write_page(int64_t table_id, pagenum_t pagenum,
                       const struct page_t *src, bool is_leaf) {
    pthread_mutex_lock(&buffer_manager_latch);
    Buffer *dest = buffer_find(table_id, pagenum);
    if (dest == nullptr) {
        dest = buffer_register(table_id, pagenum);
    }
    dest->is_dirty = true;
    memcpy(&dest->frame, src, PAGE_SIZE);
    pthread_mutex_unlock(&dest->page_latch);
    #if DEBUG_MODE
    cout << "free page latch : " << pagenum << "\n";
    #endif
    pthread_mutex_unlock(&buffer_manager_latch);
}

Buffer *buffer_find(int64_t table_id, pagenum_t pagenum) {
//    if (buffer_hash.count({table_id,pagenum}) == 0)
//        return nullptr;
//    return buffer_hash[{table_id,pagenum}];
    Buffer *start_point = head;
    Buffer *cur = head;
    // while(cur != nullptr &&
    //         (cur->page_num != pagenum || cur->table_id != table_id)){
    //     if(cur->next == start_point){
    //         return nullptr;
    //     }
    //     cur = cur->next;
    // }
    // return cur;
    for (int i = 0; i < cur_size; ++i) {
        if (cur->table_id == table_id && cur->page_num == pagenum) {
            return cur;
        }
        cur = cur->next;
    }
    return nullptr;
}

Buffer *buffer_register(int64_t table_id, pagenum_t pagenum) {
    #if DEBUG_MODE
    cout << "register page : " << pagenum << "\n";
    #endif
    Buffer *buff;
    if (cur_size == 0) {
        buff = new Buffer();
        head = buff;
        head->next = head;
        head->prev = head;
        cur_size++;
    } else if (cur_size < max_size) {
        buff = new Buffer();
        buff->prev = head->prev;
        head->prev = buff;
        buff->next = head;
        buff->prev->next = buff;
        cur_size++;
    } else {
        buff = head->prev;
        while(pthread_mutex_trylock(&buff->page_latch) != 0){
            if(buff->prev == head->prev){
                cout << "error (buffer register)" << endl;
                exit(1);
            }
            buff = buff->prev;
        }
        if (buff->is_dirty) {
            flush_log();
            // cout << "table_id : " << buff->page_num << ", page_LSN : " << ((leaf_page *)(&buff->frame))->header.page_LSN << "\n";
            file_write_page(fd_table[buff->table_id], buff->page_num, &buff->frame);
        }
//        buffer_hash.erase({buff->table_id,buff->page_num});
    }
    buff->page_num = pagenum;
    buff->table_id = table_id;
    buff->is_dirty = false;
    buffer_hash[{table_id,pagenum}] = buff;
    file_read_page(fd_table[table_id], pagenum, &buff->frame);
    pthread_mutex_unlock(&buff->page_latch);
    return buff;
}

int shutdown_buffer() {
    Buffer *cur = head;
    Buffer *tmp;
    buffer_hash.clear();
    for (int i = 0; i < cur_size; ++i) {
        if (cur->is_dirty) {
            file_write_page(fd_table[cur->table_id], cur->page_num, &cur->frame);
        }
        tmp = cur;
        cur = cur->next;
        delete tmp;
    }
    head = nullptr;
    cur_size = 0;
    max_size = 0;
    return 0;
}

