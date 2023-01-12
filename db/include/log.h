#ifndef DB_LOG_H_
#define DB_LOG_H_
#include <stdint.h>
#include <cstdio>
#include "file.h"

#define BEGIN 0
#define UPDATE 1
#define COMMIT 2
#define ROLLBACK 3
#define COMPENSATE 4

//char *log_file_name;
// FILE *log_file;
// LSN_t LSN;

typedef int64_t LSN_t;

#pragma pack (push, 2)
struct log_header{
    uint32_t log_size;
    LSN_t LSN;
    LSN_t prev_LSN;
    int trx_id;
    int type;
};
#pragma pack(pop)

struct log_t{
    log_header header;
    char *log_data;
    log_t(const log_header &logHeader);
    log_t(LSN_t prev_LSN, int trx_id,int type);
    log_t(LSN_t prev_LSN, int trx_id,int type, int64_t table_id, pagenum_t pagenum, uint16_t offset,
          uint16_t data_length, const char *before_img, const char *after_img);
    log_t(LSN_t prev_LSN, int trx_id,int type, int64_t table_id, pagenum_t pagenum, uint16_t offset,
          uint16_t data_length, const char *before_img, const char *after_img, LSN_t nextUndo);
    ~log_t();
    char *get_byte();
    int64_t get_table_id();
    pagenum_t get_page_num();
    uint16_t get_offset();
    uint16_t get_data_length();
    char *get_before_img();
    char *get_after_img();
    LSN_t get_next_undo();
     bool operator <(const log_t &a) const
    {
        return header.LSN < a.header.LSN;
    }

    bool operator <=(const log_t &a) const
    {
        return header.LSN <= a.header.LSN;
    }

    bool operator >(const log_t &a) const
    {
        return header.LSN > a.header.LSN;
    }

    bool operator >=(const log_t &a) const
    {
        return header.LSN >= a.header.LSN;
    }

    bool operator ==(const log_t &a) const
    {
        return header.LSN == a.header.LSN;
    }
};

// append log for begin/commit/rollback
LSN_t append_log(int trx_id, int type);

// append log for update
LSN_t append_log(int trx_id, int type, int64_t table_id, pagenum_t pagenum, uint16_t offset,
               uint16_t data_length, const char *before_img, const char *after_img);

// append log for compensate
LSN_t append_log(int trx_id, int type, int64_t table_id, pagenum_t pagenum, uint16_t offset,
               uint16_t data_length, const char *before_img, const char *after_img, LSN_t next_undo);

int recovery(int flag, int log_num, FILE *logmsg_file);

int redo(log_t &log, FILE *logmsg);

// return next undo LSN
LSN_t undo(log_t &log, FILE *logmsg);

log_t *get_log(LSN_t LSN);

int delete_log(LSN_t LSN);

int get_stable_log();

int flush_log();

#endif /* DB_LOG_H_ */