#include "db.h"

#include <gtest/gtest.h>

#include <string>
#include <algorithm>

#include <ctime> // to make random value
#include <vector>
#include <queue>

#define BUF_SIZE 500

#include <iostream>
using namespace std;


class DBTest : public ::testing::Test {
 protected:
  /*
   * NOTE: You can also use constructor/destructor instead of SetUp() and
   * TearDown(). The official document says that the former is actually
   * perferred due to some reasons. Checkout the document for the difference
   */
  DBTest() {
    init_db(BUF_SIZE, 0, 0, "logfile.data", "logmsg.txt");
    tid = open_table(pathname.c_str());
  }

  ~DBTest() {
    shutdown_db();
  }
  int n = 100000;
  int64_t tid;                // file descriptor
  std::string pathname = "DATA9";  // path for the file
};

TEST_F(DBTest, InsertAsc){ //오름차순
  // int n = 10;
  srand((unsigned int)time(NULL));
  int max_size = 112;
  // char * find_val_dest = (char *) malloc(sizeof(char) * max_size);
  uint16_t find_size_dest;

  for (int i = 1; i <= n; i++) {
        // cout << "insert i" << i << endl;
        // int value_size = rand() % 63 + 50;
        int value_size = 112;
        EXPECT_EQ(db_insert(tid, i, "HaHaHaHaHoHoHoHo", value_size), 0);
        // print_buffer();
        // cout << i << endl;
  }
  remove(pathname.c_str());
}

TEST_F(DBTest, InsertRan){ //랜덤
  // int n = 1000;
  srand((unsigned int)time(NULL));
  int max_size = 112;
  vector<int> v(n);
  int64_t key;
  int size;

  for (int i = 1; i <= n; ++i){
    v[i-1] = i;
  }

  random_shuffle(v.begin(),v.end());

  for (int i = 0; i < n; i++) {
      key = v[i];
      size = rand() % 63 + 50;
      // size = 112;
      db_insert(tid, key, "HAAABAAA", size);
  }
}

// TEST_F(DBTest, TestDeleteAsc){
//   // int n = 50000;
//   int p = 100;
//   int x = n/p;

//   int64_t key = 1;

//   for(int i = 0; i < x; ++i){
//     for(int j = 0; j < p; ++j){
//       if (find_key(tid,key)){
//         db_delete(tid,key++);
//       }
//     }
//     // print_tree(tid);
//   }
//   remove(pathname.c_str());
// }

// TEST_F(DBTest, TestDeleteDesc){
//   // int n = 50000;
//   int p = 100;
//   int x = n/p;

//   int64_t key = n;

//   for(int i = 0; i < x; ++i){
//     for(int j = 0; j < p; ++j){
//       if (find_key(tid,key)){
//         db_delete(tid,key--);
//       }
//     }
//   }
//   remove(pathname.c_str());
// }

// TEST_F(DBTest, TestDeleteRan){ //랜덤
//   // int n = 1000;
//   srand((unsigned int)time(NULL));
//   int max_size = 112;
//   vector<int> v(n);
//   int64_t key;
//   int size;

//   for (int i = 1; i <= n; ++i){
//     v[i-1] = i;
//   }

//   random_shuffle(v.begin(),v.end());

//   for (int i = 0; i < n; i++) {
//       key = v[i];
//       db_delete(tid, key);
//   }
// }

// TEST_F(DBTest, Findkey){
//   for(int i = 1; i <= n; ++i){
//     EXPECT_TRUE(find_key(tid,i))
//     << i;
//   }
// }


// TEST_F(DBTest, InsertionAfterDelete){
//   srand((unsigned int)time(NULL));

//   int t = 100;
//   int y = n / 10;
//   int size;
//   int64_t key;

//   for(int i = 0; i < 5; ++i){
//     key = y * i * 2;
//     for(int j = 0; j < y; ++j){
//       if (find_key(tid,++key)){
//         db_delete(tid,key);
//       }
//     }
//   }
//   key = 0;
//   for(int i = 0; i < n; ++i){
//     if (!find_key(tid,++key)){
//       size = rand() % 63 + 50;
//       db_insert(tid,key,"HAAH",size);
//     }
//   }
//   key = 0;
//   for(int i = 0; i < n; ++i){
//     EXPECT_TRUE(find_key(tid,++key));
//   }
//   // remove(pathname.c_str());
// }

TEST_F(DBTest,print){
  print_tree(tid);
  // for (int i = 1; i <= n; i++) {
  //     EXPECT_EQ(find_key(tid,i),true);
  // }
  remove(pathname.c_str());
}
