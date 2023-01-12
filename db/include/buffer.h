#ifndef DB_BUFFER_H_
#define DB_BUFFER_H_

#include "file.h"
#include "db.h"
#include <cstdlib>
#include <unordered_map>
#include <pthread.h>

struct Buffer {
    page_t frame;
    int64_t table_id;
    pagenum_t page_num;
    bool is_dirty = false;
    pthread_mutex_t page_latch;
    Buffer *next;
    Buffer *prev;

    Buffer(){
        pthread_mutexattr_t mattr;
        pthread_mutexattr_init(&mattr);
        pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_RECURSIVE);
        pthread_mutex_init(&page_latch, 0);
    }
};


void print_buffer();

int buffer_init(int num_buf);

pagenum_t buffer_alloc_page(int64_t table_id);

void buffer_free_page(int64_t table_id, pagenum_t pagenum);

int page_latch_unlock(int64_t table_id,pagenum_t pagenum);

void buffer_read_page(int64_t table_id, pagenum_t pagenum,
                      struct page_t *dest);

void buffer_read_page(int64_t table_id, pagenum_t pagenum,
                      struct page_t *dest, bool is_leaf);

void buffer_write_page(int64_t table_id, pagenum_t pagenum,
                       const struct page_t *src);

void buffer_write_page(int64_t table_id, pagenum_t pagenum,
                       const struct page_t *src, bool is_leaf);

Buffer *buffer_find(int64_t table_id, pagenum_t pagenum);

Buffer *buffer_register(int64_t table_id, pagenum_t pagenum);

int shutdown_buffer();

#endif
