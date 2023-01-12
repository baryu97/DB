#ifndef DB_FILE_H_
#define DB_FILE_H_

#include <stdint.h>

// These definitions are not requirements.
// You may build your own way to handle the constants.
#define INITIAL_DB_FILE_SIZE (10 * 1024 * 1024)  // 10 MiB
#define PAGE_SIZE (4 * 1024)                     // 4 KiB

typedef uint64_t pagenum_t;

struct page_t {
    // in-memory page structure
    char reserved_space[PAGE_SIZE];
};


struct header_page {
    uint64_t magic_num = 2022;
    pagenum_t next = INITIAL_DB_FILE_SIZE - PAGE_SIZE;
    uint64_t num_of_pages = INITIAL_DB_FILE_SIZE / PAGE_SIZE;
    pagenum_t root_page_num = 0;
    char reserved_space[PAGE_SIZE - sizeof(uint64_t) * 4];
};

struct free_page {
    pagenum_t next;
    char reserved_space[PAGE_SIZE - sizeof(pagenum_t)];
};


// Open existing database file or create one if it doesn't exist
int64_t file_open_table_file(const char *pathname);

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id);

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t pagenum,
                    struct page_t *dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t pagenum,
                     const struct page_t *src);

// Close the database file
void file_close_table_files();

pagenum_t get_next_freepage(int64_t fd, pagenum_t pagenum);

void get_head_info(int64_t fd, uint64_t *dest);

#endif  // DB_FILE_H_
