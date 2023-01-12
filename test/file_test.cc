#include "file.h"
#include "buffer.h"
#include "db.h"
#include "trx.h"
#include <iostream>
#include <ctime>
#include <gtest/gtest.h>
#include <string>

#define BUF 50
#define N 3000
#define LOG_BUF 100
#define CRASH_NUM 2000
#define TRX_NUM 10
#define DEBUG_MODE 0

void *thread_func(void *arg) {
    int trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    int table_id = ((int*)arg)[0];
    int n = N;
    int err = 0;
    uint16_t val_size;
    int st = ((int*)arg)[1];
    for (int i = st * (n / 10) + 1; i <= (st+1) * (n / 10); i++) {
        // char ret_val[112] = "\0";
        std::string data = "12345678901234567890123456789012345678901234567890" + std::to_string(i);
        uint16_t old_val_size = 0;
        #if DEBUG_MODE
        std::cout << "[DEBUG] Updating key = " << i << ", trx_id = " << trx_id << std::endl;
        #endif
        int res = db_update(table_id, i, const_cast<char*>(data.c_str()), data.length(), &old_val_size, trx_id);
        EXPECT_EQ(res, 0);
        // res = db_find(table_id, i, ret_val, &old_val_size);
        // EXPECT_EQ(res, 0);

        // for (int j = 0; j < val_size; j++) {
        //     EXPECT_EQ(ret_val[j], data[j]);
        // }
    }
    if (st == 8){
        exit(0);
    }
    trx_commit(trx_id);
    
    return NULL;
}

// /*
TEST(RecoveryTest, InsertAndCrash) {
    if((access("logfile.data", 0) != -1))  //파일 존재 여부 체크
    { 
        return;
    }
    if(std::remove("logfile.data") == 0) {
        std::cout << "deleting logfile.data\n";
    }

    // if(std::remove("DATA1") == 0) {
    //     std::cout << "deleting DATA1\n";
    // }

    if(std::remove("logmsg.txt") == 0) {
        std::cout << "deleting logmsg.txt\n";
    }


    EXPECT_EQ(init_db(BUF, 0, 0, "logfile.data", "logmsg.txt"), 0);
    int64_t table_id = open_table("DATA1");

    int n = N;

    // for (int i = 1; i <= n; i++) {
    //     std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
    //     int res = db_insert(table_id, i, const_cast<char*>(data.c_str()), data.length());
    //     EXPECT_EQ(res, 0);
    // }

    // std::cout << "[INFO] Population done, now testing XLockOnly Test" << std::endl;

    int m = 10;
    uint16_t val_size;
    pthread_t threads[m];

    std::vector<int*> args;
    // pthread_attr_t attr;
    // pthread_attr_setstacksize(&attr, 128 * 1024 * 1024);
    for (int i = 0; i < m; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Create thread " << i << std::endl;
        #endif
        int *arg = (int*)malloc(sizeof(int) * 2);
        arg[0] = table_id;
        arg[1] = i;
        args.push_back(arg);
        pthread_create(&threads[i], NULL, thread_func, arg);
    }

    for (int i = 0; i < m; i++) {
        pthread_join(threads[i], NULL);
    }

    for(auto i:args){
        free(i);
    }

    EXPECT_EQ(shutdown_db(), 0);
}

TEST(RecoveryTest, Recovery1) {
    // if((access("logmsg.txt", 0) != -1))  //파일 존재 여부 체크
    // { 
    //     return;
    // }
    EXPECT_EQ(init_db(BUF, 0, 0, "logfile.data", "logmsg.txt"), 0);   
    remove("logfile.data");
    shutdown_db();
    // remove("logfile.data");
    // remove("DATA1");
    // exit(0);
}

// TEST(RecoveryTest, Recovery2) {
//     EXPECT_EQ(init_db(BUF, 2, 2500, "logfile.data", "logmsg.txt"), 0);
//     remove("logmsg.txt");
//     exit(0);
// }

// */