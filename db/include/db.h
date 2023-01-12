#ifndef DB_DB_H_
#define DB_DB_H_

// db.h
#include <cstdint>
#include <vector>
#include <cstring>
#include <cstdlib>
#include "file.h"
#include "log.h"
#include "buffer.h"

#define order 249

#include <iostream>
#include <unordered_map>

using namespace std;

struct page_header {
    pagenum_t parent_num;
    int is_leaf;
    int num_keys = 0;
    char reserve1[8];
    LSN_t page_LSN = 0;
    char reserved[128 - 16 - 16 - 16];
    uint64_t free_space = 3968;
    pagenum_t page_pointer;
};

struct leaf_page {
    struct page_header header;
    char page_body[4096 - sizeof(page_header)];
};

struct entry {
    int64_t key;
    pagenum_t page_num;
};

struct internal_page {
    struct page_header header;
    struct entry entrys[248];
};

#pragma pack (push, 2)
struct slot {
    int64_t key;
    uint16_t size;
    uint16_t offset;
    int32_t trx_id;
};
#pragma pack(pop)

struct leaf_and_data {
    struct leaf_page *page;
    struct slot *keys;
    char **values;

    leaf_and_data() {
        page = (leaf_page *) malloc(PAGE_SIZE);
    }

    ~leaf_and_data() {
        free(page);
    }
};

//struct rollback_log_t{
//    uint16_t length;
//    char *before;
//    char *after;
//};

 extern unordered_map<int64_t,int> fd_table;

// Output and utility.

void print_tree(int64_t table_id);

void print_internal(int64_t table_id, pagenum_t offset);

void print_leaf(int64_t table_id, pagenum_t offset);

void print_keys(const leaf_and_data &);

bool find_key(int64_t table_id, int64_t key);

pagenum_t make_node_page(int64_t table_id);

pagenum_t make_leaf_page(int64_t table_id);

pagenum_t find_leaf(int64_t table_id, int64_t key);

int read_page_body(struct leaf_and_data &leaf_node);

int read_page_body(struct leaf_and_data &leaf_node, int n);

int write_page_body(struct leaf_and_data &leaf_node);

// Initialize the database system.
int init_db (int buf_num, int flag, int log_num, char* log_path, char* logmsg_path);

// Open an existing database file or create one if not exist.
int64_t open_table(const char *pathname);

// Insert a record to the given table.
int db_insert(int64_t table_id, int64_t key, const char *value,
              uint16_t val_size);

pagenum_t insert_into_parent(int64_t table_id, pagenum_t left, int64_t key, pagenum_t right);

pagenum_t insert_into_leaf_after_splitting(int64_t table_id, pagenum_t leaf_offset,
                                           int64_t key, const char *value, uint16_t val_size);

pagenum_t insert_into_internal_after_splitting(int64_t table_id, pagenum_t old_page_offset,
                                               int left_index, int64_t key, pagenum_t right_offset);

// Find a record with the matching key from the given table.
int db_find(int64_t table_id, int64_t key, char *ret_val,
            uint16_t *val_size, int trx_id);

int db_update(int64_t table_id, int64_t key, char *value, uint16_t new_val_size, uint16_t *old_val_size, int trx_id);

//int rollback_update(int64_t table_id, pagenum_t page_id, int64_t key, rollback_log_t *log_obj,int trx_id);

int rollback_log(log_t *log);

// Delete a record with the matching key from the given table.
int db_delete(int64_t table_id, int64_t key);

pagenum_t delete_entry(int64_t table_id, pagenum_t n_offset, int deletion_index);

pagenum_t coalesce_nodes(int64_t table_id, pagenum_t n_offset, pagenum_t neighbor_offset, int neighbor_index);

pagenum_t redistribute_nodes(int64_t table_id, pagenum_t n_offset, pagenum_t neighbor_offset, int neighbor_index);

// Find records with a key between the range: begin_key ≤ key ≤ end_key
int db_scan(int64_t table_id, int64_t begin_key,
            int64_t end_key, std::vector<int64_t> *keys,
            std::vector<char *> *values,
            std::vector<uint16_t> *val_sizes);

// Shutdown the database system.
int shutdown_db();

#endif
