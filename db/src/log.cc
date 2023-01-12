#include "log.h"
#include "trx.cc"

#include <iostream>
#include <vector>
#include <set>
#include <queue>
#include <string>
#include <pthread.h>
using namespace std;

vector<log_t *> log_buf;
vector<log_t *> stable_log;
LSN_t LSN;
FILE *log_file;
pthread_mutex_t log_manager_latch = PTHREAD_MUTEX_INITIALIZER;

void print_log_info (log_t &log){
    cout << "LSN : " << log.header.LSN << ", type : " << log.header.type << ", trx_id : " << log.header.trx_id << ", prev_LSN : " << log.header.prev_LSN;
    if (log.header.type == BEGIN || log.header.type == ROLLBACK || log.header.type == COMMIT){
        cout << "\n";
        return;
    }
    int64_t table_id = log.get_table_id();
    pagenum_t page_id = log.get_page_num();
    uint16_t offset = log.get_offset();
    uint16_t data_length = log.get_data_length();
    cout << ", table_id : " << table_id << ", page_id : " << page_id << ", offset : " << offset << ", data_length : " << data_length;
    cout << "\n";
}

LSN_t append_log(int trx_id, int type){
    pthread_mutex_lock(&log_manager_latch);
    if (type != BEGIN && type != COMMIT && type != ROLLBACK){
        cout << "error : append log wrong type\n";
        pthread_mutex_unlock(&log_manager_latch);
        return -1;
    }
    log_t *log_ogj = new log_t(trx_table[trx_id]->last_LSN,trx_id,type);
    trx_table[trx_id]->last_LSN = log_ogj->header.LSN;
    trx_table[trx_id]->logs.insert(log_ogj);
    log_buf.push_back(log_ogj);
    LSN_t ret_val = log_ogj->header.LSN;
    pthread_mutex_unlock(&log_manager_latch);
    return ret_val;
}

LSN_t append_log(int trx_id, int type, int64_t table_id, pagenum_t pagenum, uint16_t offset, uint16_t data_length, const char *before_img, const char *after_img){
    pthread_mutex_lock(&log_manager_latch);
    if (type != UPDATE){
        cout << "error : append log wrong type\n";
        pthread_mutex_unlock(&log_manager_latch);
        return -1;
    }
    log_t *log_ogj = new log_t(trx_table[trx_id]->last_LSN,trx_id,type,table_id,pagenum,offset,data_length,before_img,after_img);
    trx_table[trx_id]->last_LSN = log_ogj->header.LSN;
    trx_table[trx_id]->logs.insert(log_ogj);
    log_buf.push_back(log_ogj);
    LSN_t ret_val = log_ogj->header.LSN;
    pthread_mutex_unlock(&log_manager_latch);
    return ret_val;
}

LSN_t append_log(int trx_id, int type, int64_t table_id, pagenum_t pagenum, uint16_t offset, uint16_t data_length, const char *before_img, const char *after_img,LSN_t next_undo){
    pthread_mutex_lock(&log_manager_latch);
    if (type != COMPENSATE){
        cout << "error : append log wrong type\n";
        pthread_mutex_unlock(&log_manager_latch);
        return -1;
    }
    log_t *log_ogj = new log_t(trx_table[trx_id]->last_LSN,trx_id,type,table_id,pagenum,offset,data_length,before_img,after_img,next_undo);
    trx_table[trx_id]->last_LSN = log_ogj->header.LSN;
    trx_table[trx_id]->logs.insert(log_ogj);
    log_buf.push_back(log_ogj);
    LSN_t ret_val = log_ogj->header.LSN;
    pthread_mutex_unlock(&log_manager_latch);
    return ret_val;
}

int recovery(int flag, int log_num, FILE *logmsg_file){
    stable_log.clear();
    while(get_stable_log() == 0);
    if (stable_log.empty()){
        LSN = 0;
        return 0;
    }
    LSN = (*stable_log.rbegin())->header.LSN + (*stable_log.rbegin())->header.log_size;
    vector<int> winners;
    int biggest_trx_id = 0;
    int n;
    fprintf(logmsg_file, "[ANALYSIS] Analysis pass start\n");
    set<int64_t> table_ids;
    // analysis pass
    for (log_t *log : stable_log){
        if (log->header.type == BEGIN){
            trx_table[log->header.trx_id] = new trx_t();
            trx_table[log->header.trx_id]->last_LSN = log->header.LSN;
            biggest_trx_id = max(biggest_trx_id, log->header.trx_id);
        } else if (log->header.type == COMMIT || log->header.type == ROLLBACK){
            delete trx_table[log->header.trx_id];
            trx_table.erase(log->header.trx_id);
            winners.push_back(log->header.trx_id);
        } else {
            int64_t table_id = log->get_table_id();
            trx_table[log->header.trx_id]->last_LSN = log->header.LSN;
            if (!table_ids.count(table_id)){
                open_table(("DATA" + to_string(table_id)).c_str());
                table_ids.insert(table_id);
            }
        }
    }

    fprintf(logmsg_file, "[ANALYSIS] Analysis success. Winner:");
    for (int winner : winners){
        fprintf(logmsg_file," %d",winner);
    }
    fprintf(logmsg_file,", Loser:");
    for (auto [loser,tmp] : trx_table){
        fprintf(logmsg_file," %d",loser);
    }
    fprintf(logmsg_file,".\n");
    fprintf(logmsg_file, "[REDO] Redo pass start\n");
//    for (log_t *log : stable_log){
//        // redo all log (ARIES)
//        redo((log_t &)*log,logmsg_file);
//    }
    for (int i = 0; i < stable_log.size(); ++i){
        log_t *log = stable_log[i];
        if (flag == 1 && i == log_num)
            return 0;
        // redo all log (ARIES)
        redo((log_t &)*log,logmsg_file);
    }
    fprintf(logmsg_file, "[REDO] Redo pass end\n");
    fflush(logmsg_file);
    fprintf(logmsg_file, "[UNDO] Undo pass start\n");
    priority_queue<LSN_t> LSN_queue;
    for (auto [temp, trx_obj] : trx_table) {
        if (trx_obj->last_LSN != -1)
            LSN_queue.push(trx_obj->last_LSN);
    }
    for (int i = 0; !LSN_queue.empty(); ++i){
        if (flag == 2 && i == log_num)
            return 0;
        LSN_t undoLSN = LSN_queue.top();
        LSN_queue.pop();
        if (undoLSN == -1)
            continue;
        // cout << "i want to get " << undoLSN << "\n";
        log_t *log = get_log(undoLSN);
        // log_t *log = stable_log[undoLSN];
        if (undoLSN != log->header.LSN)
            cout << "error : get wrong log\n";
        // undo loser log (ARIES)
        LSN_t next_undo;
        if ((next_undo = undo((log_t &)*log,logmsg_file)) != -1){
            LSN_queue.push(next_undo);
        }
    }
    fprintf(logmsg_file, "[UNDO] Undo pass end\n");
    flush_log();
    for (log_t *log : stable_log){
        delete log;
    }
    trx_table.clear();
    return 0;
}

int get_stable_log(){
    log_header logHeader;
    if (fread(&logHeader,sizeof(log_header),1,log_file) != 1)
        return -1;
    log_t * log = new log_t(logHeader);
    fread(log->log_data,log->header.log_size - sizeof(log_header),1,log_file);
    stable_log.push_back(log);
    return 0;
}

int flush_log(){
    pthread_mutex_lock(&log_manager_latch);
//    FILE *log_file = fopen(log_file_name, "a+b");
    for (log_t *log : log_buf) {
        char *byte = log->get_byte();
        if (fwrite(byte,log->header.log_size,1,log_file) != 1)
            cout << "error : fwrite\n";
        delete[] byte;
//        delete log;
    }
    log_buf.clear();
    fflush(log_file);
    pthread_mutex_unlock(&log_manager_latch);
    return 0;
}

int redo(log_t &log, FILE *logmsg){
    switch (log.header.type) {
        case BEGIN:
            fprintf(logmsg, "LSN %lu [BEGIN] Transaction id %d\n", log.header.LSN, log.header.trx_id);
            return 0;
        case ROLLBACK:
            fprintf(logmsg, "LSN %lu [ROLLBACK] Transaction id %d\n", log.header.LSN, log.header.trx_id);
            return 0;
        case COMMIT:
            fprintf(logmsg, "LSN %lu [COMMIT] Transaction id %d\n", log.header.LSN, log.header.trx_id);
            return 0;
        case UPDATE:
            break;
        case COMPENSATE:
            break;
        default:
            cout << "error : redo wrong type\n";
            exit(1);
    }
    struct leaf_and_data dest;

    int64_t table_id = log.get_table_id();
    pagenum_t page_id = log.get_page_num();
    uint16_t offset = log.get_offset();
    uint16_t data_length = log.get_data_length();
    char *before = log.get_before_img();
    char *after = log.get_after_img();

    buffer_read_page(table_id, page_id, (page_t *) dest.page);
    // fprintf(logmsg, "page_id : %ld, page_LSN : %lu\n", page_id,dest.page->header.page_LSN);
    if (dest.page->header.page_LSN < log.header.LSN){
        dest.page->header.page_LSN = log.header.LSN;

        memcpy((char *)dest.page + offset, after, data_length);

        buffer_write_page(table_id, page_id, (page_t *) dest.page);
        if (log.header.type == UPDATE)
            fprintf(logmsg, "LSN %lu [UPDATE] Transaction id %d redo apply\n", log.header.LSN, log.header.trx_id);
        else
            fprintf(logmsg, "LSN %lu [CLR] next undo lsn %lu\n", log.header.LSN, log.get_next_undo());
    } else {
        fprintf(logmsg, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", log.header.LSN, log.header.trx_id);
    }
    delete[] before;
    delete[] after;
    return 0;
}

LSN_t undo(log_t &log, FILE *logmsg){
    if (log.header.type == BEGIN){
        append_log(log.header.trx_id,ROLLBACK);
        return -1;
    }
    if (log.header.type == COMPENSATE){
        fprintf(logmsg, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", log.header.LSN, log.header.trx_id);
        return log.get_next_undo();
    }
    if (log.header.type != UPDATE){
        cout << "error : undo wrong type\n";
        exit(1);
    }
    struct leaf_and_data dest;

    int64_t table_id = log.get_table_id();
    pagenum_t page_id = log.get_page_num();
    uint16_t offset = log.get_offset();
    uint16_t data_length = log.get_data_length();
    char *before = log.get_before_img();
    char *after = log.get_after_img();

    buffer_read_page(table_id, page_id, (page_t *) dest.page);

    if (dest.page->header.page_LSN >= log.header.LSN){
        dest.page->header.page_LSN = append_log(log.header.trx_id,COMPENSATE,table_id,page_id,offset,data_length,after,before,log.header.prev_LSN);

        memcpy((char *)dest.page + offset, before, data_length);

        buffer_write_page(table_id, page_id, (page_t *) dest.page);
        fprintf(logmsg, "LSN %lu [UPDATE] Transaction id %d undo apply\n", log.header.LSN, log.header.trx_id);
    } else {
        // fprintf(logmsg, "LSN %lu [CONSIDER-REDO] Transaction id %d\n", log.header.LSN, log.header.trx_id);
    }
    delete[] before;
    delete[] after;
    return log.header.prev_LSN;
}

log_t::log_t(LSN_t prev_LSN, int trx_id, int type){
    header.log_size = sizeof(log_header);
    header.LSN = LSN;
    LSN += header.log_size;
    header.prev_LSN = prev_LSN;
    header.trx_id = trx_id;
    header.type = type;
    log_data = nullptr;
}

log_t::log_t(LSN_t prev_LSN, int trx_id,int type, int64_t table_id, pagenum_t pagenum, uint16_t offset,uint16_t data_length, const char *before_img, const char *after_img){
    header.log_size = sizeof(log_header) + 20 + 2 * data_length;
    header.LSN = LSN;
    LSN += header.log_size;
    header.prev_LSN = prev_LSN;
    header.trx_id = trx_id;
    header.type = type;
    log_data = new char[header.log_size - sizeof(log_header)];
    memcpy(log_data,&table_id,8);
    memcpy(log_data + 8,&pagenum,8);
    memcpy(log_data + 16,&offset,2);
    memcpy(log_data + 18,&data_length,2);
    memcpy(log_data + 20,before_img,data_length);
    memcpy(log_data + 20 + data_length,after_img,data_length);
}

log_t::log_t(LSN_t prev_LSN, int trx_id,int type, int64_t table_id, pagenum_t pagenum, uint16_t offset,uint16_t data_length, const char *before_img, const char *after_img, LSN_t nextUndo){
    header.log_size = sizeof(log_header) + 28 + 2 * data_length;
    header.LSN = LSN;
    LSN += header.log_size;
    header.prev_LSN = prev_LSN;
    header.trx_id = trx_id;
    header.type = type;
    log_data = new char[header.log_size - sizeof(log_header)];
    memcpy(log_data,&table_id,8);
    memcpy(log_data + 8,&pagenum,8);
    memcpy(log_data + 16,&offset,2);
    memcpy(log_data + 18,&data_length,2);
    memcpy(log_data + 20,before_img,data_length);
    memcpy(log_data + 20 + data_length,after_img,data_length);
    memcpy(log_data + 20 + 2 * data_length,&nextUndo,8);
}

char* log_t::get_byte(){
    // print_log_info((log_t &)*this);
    char *byte = new char[header.log_size];
    memcpy(byte,&header.log_size,4);
    memcpy(byte + 4,&header.LSN,8);
    memcpy(byte + 12,&header.prev_LSN,8);
    memcpy(byte + 20,&header.trx_id,4);
    memcpy(byte + 24,&header.type,4);
    if (log_data != nullptr)
        memcpy(byte + sizeof(log_header),log_data,header.log_size - sizeof(log_header));
    return byte;
}

int64_t log_t::get_table_id(){
    int64_t ret_val;
    memcpy(&ret_val,log_data,8);
    return ret_val;
}

pagenum_t log_t::get_page_num(){
    pagenum_t ret_val;
    memcpy(&ret_val,log_data + 8,8);
    return ret_val;
}

uint16_t log_t::get_offset(){
    uint16_t ret_val;
    memcpy(&ret_val,log_data + 16,2);
    return ret_val;
}

uint16_t log_t::get_data_length(){
    uint16_t ret_val;
    memcpy(&ret_val,log_data + 18,2);
    return ret_val;
}

char* log_t::get_before_img(){
    uint16_t data_length = get_data_length();
    char *ret_val = new char[data_length];
    memcpy(ret_val,log_data + 20, data_length);
    return ret_val;
}
char* log_t::get_after_img(){
    uint16_t data_length = get_data_length();
    char *ret_val = new char[data_length];
    memcpy(ret_val,log_data + 20 + data_length, data_length);
    return ret_val;
}
LSN_t log_t::get_next_undo(){
    uint16_t data_length = get_data_length();
    LSN_t ret_val;
    memcpy(&ret_val,log_data + 20 + 2 * data_length,8);
    return ret_val;
}

log_t::~log_t(){
    unsigned int size = header.log_size - sizeof(log_header);
    if (size){
        delete[] log_data;
    }
}

log_t::log_t(const log_header &logHeader){
    header = logHeader;
    log_data = new char[header.log_size - sizeof(log_header)];
}

log_t *get_log(LSN_t LSN){
    int left = 0, right = stable_log.size() - 1;
    while (left <= right){
        int mid = (left + right) / 2;
        if(stable_log[mid]->header.LSN == LSN){
            return stable_log[mid];
        } else if (stable_log[mid]->header.LSN > LSN){
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }
    return stable_log[left];
}