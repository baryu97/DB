#include "file.h"
#include <cstdlib>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <vector>
#include <unistd.h>

// lib for debug
#include <errno.h>
#include <iostream>

// extern int errno;

using namespace std;

//pagenum_t root[30];

static vector<int64_t> file_descriptors;

// Open existing database file or create one if it doesn't exist
/**
 *  Open the database file
 *  It opens an existing database file using 'pathname'(parameter) or create a new file if absent
 *  If a new file needs to be created, the default file size should be 10MiB. => file size 변경해주는 함수 찾아야할듯
 *  Then it returns the file descriptor of the opened database file.
 *  All other 5 commands below should be handled after open data file.
 *  If it opens an existing database file, it must check the magic number of database file.
 *  If the checked number is different from the expected value, then it returns a negative value for error handling.
 */
int64_t file_open_table_file(const char *pathname) {
    int64_t fd;
    fd = open(pathname, O_RDWR);
    if (fd == -1 && errno == 2) { //when file does not exist
        fd = open(pathname, O_RDWR | O_CREAT, 0666); // O_SYNC 추가??
        struct header_page head;
        pwrite(fd, &head, PAGE_SIZE, 0);
        pagenum_t next_freepage;
        for (int i = 1; i < INITIAL_DB_FILE_SIZE / PAGE_SIZE; ++i) {
            next_freepage = (i - 1) * PAGE_SIZE;
            pwrite(fd, &next_freepage, PAGE_SIZE, PAGE_SIZE * i);
        }
    }
    if (fd >= 0) {
        file_descriptors.push_back(fd);
    } else {
        return -1;
    }
    struct header_page first_page;
    pread(fd, &first_page, PAGE_SIZE, 0);
    if (first_page.magic_num != 2022)
        return -1;
//    root[fd] = first_page.root_page_num;
    return fd;
}

// Allocate an on-disk page from the free page list
/**
 *  Allocate a page
 *  It returns a new page # from the free page list
 *  If the free page list is empty, then it should grow the database file and return a free page #.
 */
pagenum_t file_alloc_page(int64_t fd) {
    pagenum_t head_info[3];
    pagenum_t second;
    pagenum_t result;
    pread(fd, head_info, sizeof(pagenum_t) * 3, 0);
    if (!head_info[1]) { // free page list is empty
        pagenum_t next_freepage = 0;
        pwrite(fd, &next_freepage, PAGE_SIZE, head_info[2] * PAGE_SIZE);
        for (int i = head_info[2] + 1; i < (head_info[2] << 1); ++i) {
            next_freepage = (i - 1) * PAGE_SIZE;
            pwrite(fd, &next_freepage, PAGE_SIZE, PAGE_SIZE * i);
        }
        head_info[2] <<= 1;
        head_info[1] = (head_info[2] - 1) * PAGE_SIZE;
    }
    pread(fd, &second, sizeof(pagenum_t), head_info[1]);
    result = head_info[1];
    head_info[1] = second;
    pwrite(fd, head_info, sizeof(pagenum_t) * 3, 0);
    return result;
}

// Free an on-disk page to the free page list
/**
 *  Free a page
 *  It informs the disk space manager of returning the page with 'page_number' for freeing it to the free page list.
 */
void file_free_page(int64_t fd, pagenum_t pagenum) {
    pagenum_t header_info[3];
    pread(fd, header_info, sizeof(pagenum_t) * 3, 0);
    pwrite(fd, &header_info[1], sizeof(pagenum_t), pagenum);
    header_info[1] = pagenum;
    pwrite(fd, header_info, sizeof(pagenum_t) * 3, 0);
}

// Read an on-disk page into the in-memory page structure(dest)
/**
 *  Read a page
 *  It fetches the disk page corresponding to 'page_number' to the in-memory buffer (i.e., 'dest').
 */
void file_read_page(int64_t fd, pagenum_t pagenum, struct page_t *dest) {
    pread(fd, dest, sizeof(page_t), pagenum);
}

// Write an in-memory page(src) to the on-disk page
/**
 *  Write a page
 *  It writes the in-memory page content in the buffer (i.e., 'src') to the disk page pointed by 'page_number'.
 */
void file_write_page(int64_t fd, pagenum_t pagenum, const struct page_t *src) {
    pwrite(fd, src, sizeof(page_t), pagenum);
    fsync(fd);
}

// Close the database file
/**
 *  Close the database file
 *  This API doesn't receive a file descriptor as a parameter.
 *  So a means for referencing the descriptor of the opened file(i.e., global variable) is required
 */
void file_close_table_files() {
    for (int i = 0; i < file_descriptors.size(); i++) {
        close(file_descriptors[i]);
    }
    file_descriptors.clear();
}

pagenum_t get_next_freepage(int64_t fd, pagenum_t pagenum) { //return next free page num. for testing
    pagenum_t nextpage;
    pread(fd, &nextpage, sizeof(pagenum_t), pagenum);
    return nextpage;
}

void get_head_info(int64_t fd, uint64_t *dest) {
    struct header_page head;
    pread(fd, &head, PAGE_SIZE, 0);
    dest[0] = head.magic_num;
    dest[1] = head.next;
    dest[2] = head.num_of_pages;
}
