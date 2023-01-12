#include "db.h"
#include "trx.h"

#define SPLIT_OFFSET (1984)
#define THRESHOLD (2500)
#define ORDER (124)

#include <queue>

unordered_map<int64_t, pagenum_t> root;
unordered_map<int64_t,int> fd_table;
extern FILE *log_file;

// Initialize the database system.
int init_db (int buf_num, int flag, int log_num, char* log_path, char* logmsg_path){
    buffer_init(buf_num);
    init_lock_table();
    FILE *fp = fopen( logmsg_path, "wt");
    log_file = fopen(log_path, "a+b");
    recovery(flag,log_num,fp);
    return 0;
}

// Find a record with the matching key from the given table.
int db_find(int64_t table_id, int64_t key, char *ret_val, uint16_t *val_size, int trx_id) {
    struct leaf_and_data src;
    int i;

    pagenum_t leaf_offset = find_leaf(table_id, key);

    buffer_read_page(table_id, leaf_offset, (page_t *) src.page);

    if (leaf_offset == 0) { // no tree
        cout << "find error : there is no tree." << endl;
        return 1;
    }
    read_page_body(src);
    for (i = 0; i < src.page->header.num_keys; ++i) {
        if (src.keys[i].key == key) {
            break;
        }
    }
    if (i == src.page->header.num_keys) { // no key
        cout << "find error : there is no key. (" << key << ")\n";
        write_page_body(src);
        return 1;
    }

    write_page_body(src);
    key = i;
    get_trx_latch();
    if (find_first_lock(table_id, leaf_offset, key) == nullptr) {
        buffer_read_page(table_id, leaf_offset, (page_t *) src.page, true);
        read_page_body(src);
        if (src.keys[i].trx_id != trx_id && check_trx_alive(src.keys[i].trx_id)) {
            implicit_to_explicit(table_id, leaf_offset, key, src.keys[i].trx_id);
            write_page_body(src);
            page_latch_unlock(table_id,leaf_offset);
        } else {
            unlock_trx_latch();

            memcpy(ret_val, src.values[i], src.keys[i].size);
            *val_size = src.keys[i].size;

            write_page_body(src);
            page_latch_unlock(table_id,leaf_offset);
            return 0;
        }
    }
    unlock_trx_latch();
    if (lock_acquire(table_id, leaf_offset, key, trx_id, SHARED) == nullptr) {
        return -1;
    }

    buffer_read_page(table_id, leaf_offset, (page_t *) src.page, true);
    read_page_body(src);

    memcpy(ret_val, src.values[i], src.keys[i].size);
    *val_size = src.keys[i].size;

    write_page_body(src);
    page_latch_unlock(table_id,leaf_offset);
    return 0;
}


int db_update(int64_t table_id, int64_t key, char *value, uint16_t new_val_size, uint16_t *old_val_size, int trx_id) {
    struct leaf_and_data dest;
    int i;

    pagenum_t leaf_offset = find_leaf(table_id, key);


    if (leaf_offset == 0) { // no tree
        cout << "find error : there is no tree." << endl;
        return 1;
    }

    buffer_read_page(table_id, leaf_offset, (page_t *) dest.page);
    read_page_body(dest);
    for (i = 0; i < dest.page->header.num_keys; ++i) {
        if (dest.keys[i].key == key) {
            break;
        }
    }
    if (i == dest.page->header.num_keys) { // no key
        cout << "find error : there is no key. (" << key << ")\n";
        write_page_body(dest);
        return 1;
    }
    write_page_body(dest);
    key = i;
    get_trx_latch();
    if (find_first_lock(table_id, leaf_offset, key) == nullptr) {
        buffer_read_page(table_id, leaf_offset, (page_t *) dest.page, true);
        read_page_body(dest);
        if (dest.keys[i].trx_id != trx_id && check_trx_alive(dest.keys[i].trx_id)) {
            implicit_to_explicit(table_id, leaf_offset, key, dest.keys[i].trx_id);
            write_page_body(dest);
            page_latch_unlock(table_id,leaf_offset);
        } else {
            unlock_trx_latch();
//            add_trx_log(table_id, leaf_offset, key, trx_id, dest.values[i], dest.keys[i].size);

            *old_val_size = dest.keys[i].size;
            dest.keys[i].trx_id = trx_id;

            dest.page->header.page_LSN = append_log(trx_id,UPDATE,table_id,leaf_offset,dest.keys[i].offset,max(new_val_size,dest.keys[i].size),dest.values[i],value);

            memcpy(dest.values[i], value, new_val_size);
            dest.keys[i].size = new_val_size;

            write_page_body(dest);

            buffer_write_page(table_id, leaf_offset, (page_t *) dest.page, true);
            return 0;
        }
    }
    unlock_trx_latch();
    if (lock_acquire(table_id, leaf_offset, key, trx_id, EXCLUSIVE) == nullptr) {
        return -1;
    }

    buffer_read_page(table_id, leaf_offset, (page_t *) dest.page, true);
    read_page_body(dest);

//    add_trx_log(table_id, leaf_offset, key, trx_id, dest.values[i], dest.keys[i].size);

    *old_val_size = dest.keys[i].size;
    dest.keys[i].trx_id = trx_id;

    append_log(trx_id,UPDATE,table_id,leaf_offset,dest.keys[i].offset,max(new_val_size,dest.keys[i].size),dest.values[i],value);

    memcpy(dest.values[i], value, new_val_size);
    dest.keys[i].size = new_val_size;

    write_page_body(dest);

    buffer_write_page(table_id, leaf_offset, (page_t *) dest.page, true);
    return 0;
}

/*int rollback_update(int64_t table_id, pagenum_t page_id, int64_t key, rollback_log_t *log_obj,int trx_id) {
    struct leaf_and_data dest;
    int i = key;

    buffer_read_page(table_id, page_id, (page_t *) dest.page, true);

    read_page_body(dest);

    // for (i = 0; i < dest.page->header.num_keys; ++i) {
    //     if (dest.keys[i].key == key) {
    //         break;
    //     }
    // }
    // if (i == dest.page->header.num_keys) { // no key
    //     cout << "error : there is no key. (rollback)" << key << "\n";
    //     write_page_body(dest);
    //     delete[] log_obj->before;
    //     delete log_obj;
    //     return -1;
    // }

    append_log(trx_id,COMPENSATE,table_id,page_id,dest.keys[key].offset,max(log_obj->length,dest.keys[i].size),log_obj->before,dest.values[i]);

    // dest.keys[i].size = log_obj->length;
    memcpy(dest.values[i], log_obj->before, dest.keys[i].size);

    write_page_body(dest);

    buffer_write_page(table_id, page_id, (page_t *) dest.page, true);

    delete[] log_obj->before;
    delete log_obj;
    return 0;
}*/

int rollback_log(log_t *log) {
    struct leaf_and_data dest;
    if (log->header.type != UPDATE){
        return 0;
    }
    int64_t table_id = log->get_table_id();
    pagenum_t page_id = log->get_page_num();
    uint16_t offset = log->get_offset();
    uint16_t data_length = log->get_data_length();
    char *before = log->get_before_img();
    char *after = log->get_after_img();

    buffer_read_page(table_id, page_id, (page_t *) dest.page, true);

    dest.page->header.page_LSN = append_log(log->header.trx_id,COMPENSATE,table_id,page_id,offset,data_length,after,before,log->header.prev_LSN);

    memcpy((char *)dest.page + offset, before, data_length);
    delete[] before;
    delete[] after;

    buffer_write_page(table_id, page_id, (page_t *) dest.page, true);

    return 0;
}

void print_tree(int64_t table_id) {
    queue<pagenum_t> bfs;
    internal_page cur_page;
    pagenum_t cur_num = root[table_id];
    if (cur_num == 0) {
        return;
    }
    bfs.push(cur_num);
    buffer_read_page(table_id, cur_num, (page_t *) &cur_page);

    // print internal
    cout << endl;
    while (!cur_page.header.is_leaf) {
        bfs.pop();
        cout << "internal num of keys : " << cur_page.header.num_keys << "\n";
        print_internal(table_id, cur_num);
        cout << " / ";
        bfs.push(cur_page.header.page_pointer);
        for (int i = 0; i < cur_page.header.num_keys; ++i) {
            bfs.push(cur_page.entrys[i].page_num);
        }
        cur_num = bfs.front();
        buffer_read_page(table_id, cur_num, (page_t *) &cur_page);
    }
    // // print leaf
    // while (!bfs.empty()) {
    //     print_leaf(table_id, bfs.front());
    //     cout << " / ";
    //     bfs.pop();
    // }
}

void print_internal(int64_t table_id, pagenum_t offset) {
    internal_page node;
    buffer_read_page(table_id, offset, (page_t *) &node);

    // cout << "{"; //print just key
    cout << "{ offset : " << node.header.page_pointer; //print offset
    for (int i = 0; i < node.header.num_keys; ++i) {
        // cout << " key : " << node.entrys[i].key;
        cout << ", key : " << node.entrys[i].key << ", offset : " << node.entrys[i].page_num;
    }
    cout << " }\n";
}

void print_leaf(int64_t table_id, pagenum_t offset) {
    leaf_and_data leaf;
    buffer_read_page(table_id, offset, (page_t *) leaf.page); //// 원래 프린트는 핀 안박는게 맞지만 뮤텍스랑 핀이랑 같이 써서 조심해야함
    read_page_body(leaf);
    print_keys(leaf);
    write_page_body(leaf);
    free(leaf.keys);
    free(leaf.values);
}

void print_keys(const leaf_and_data &leaf) {
    // print key range
    cout << "num of keys is : " << leaf.page->header.num_keys << " "
         << "key range : " << leaf.keys[0].key << "~" << leaf.keys[leaf.page->header.num_keys - 1].key << endl;

//      // print values
//      for (int i = 0; i < leaf.page->header.num_keys; ++i){
//          cout << "key : "<< leaf.keys[i].key <<
// //         " size : " <<  leaf.keys[i].size << //"\n";
// //         " offset : " << leaf.keys[i].offset <<
// //         " value : " << leaf.values[i][0] << leaf.values[i][1] <<
//          "\n";
//      }
}

bool find_key(int64_t table_id, int64_t key) {
    pagenum_t leaf_offset = find_leaf(table_id, key);
    leaf_and_data leaf;
    int i;
    if (leaf_offset == 0)
        return false;

    buffer_read_page(table_id, leaf_offset, (page_t *) leaf.page); //// 이것도 핀 조심
    read_page_body(leaf);

    for (i = 0; i < leaf.page->header.num_keys; i++)
        if (leaf.keys[i].key == key) break;
    if (i == leaf.page->header.num_keys) {
        write_page_body(leaf);
        return false;
    } else {
        write_page_body(leaf);
        return true;
    }
}

pagenum_t make_node_page(int64_t table_id) {
    struct internal_page *node;
    pagenum_t dest_num = buffer_alloc_page(table_id);

    node = (internal_page *) malloc(PAGE_SIZE);
    node->header.num_keys = 0;
    node->header.free_space = PAGE_SIZE - 128;
    node->header.is_leaf = 0;
    node->header.page_LSN = 0;
    node->header.page_pointer = 0;
    node->header.parent_num = 0;

    buffer_write_page(table_id, dest_num, (page_t *) node);
    free(node);
    return dest_num;
}

pagenum_t make_leaf_page(int64_t table_id) {
    struct leaf_page node;
    pagenum_t dest_num = make_node_page(table_id);
    buffer_read_page(table_id, dest_num, (page_t *) &node);
    node.header.is_leaf = 1;
    node.header.page_LSN = 0;
    buffer_write_page(table_id, dest_num, (page_t *) &node);
    return dest_num;
}

pagenum_t find_leaf(int64_t table_id, int64_t key) {
    if (root[table_id] == 0)
        return root[table_id];

    pagenum_t offset = root[table_id];
    page_t *cur_page;
    cur_page = (page_t *) malloc(PAGE_SIZE);
    buffer_read_page(table_id, root[table_id], cur_page);

    while (((internal_page *) cur_page)->header.is_leaf == 0) {
        int i = 0;

        while (i < ((internal_page *) cur_page)->header.num_keys) { // num_keys 인터널이면 0이 아님
            if (key >= ((internal_page *) cur_page)->entrys[i].key) i++;
            else break;
        }

        if (i == 0) // 키가 제일 작을때
            offset = ((internal_page *) cur_page)->header.page_pointer;
        else
            offset = ((internal_page *) cur_page)->entrys[i - 1].page_num;
        buffer_read_page(table_id, offset, cur_page);
    }
    free(cur_page);
    return offset;
}

int read_page_body(struct leaf_and_data &leaf_node) {
    // 나중에 인서트할 공간 미리 할당
    leaf_node.keys = (slot *) malloc(sizeof(slot) * (leaf_node.page->header.num_keys + 2));
    leaf_node.values = (char **) malloc(sizeof(char *) * (leaf_node.page->header.num_keys + 2));

    memcpy(leaf_node.keys, leaf_node.page->page_body, leaf_node.page->header.num_keys * sizeof(slot));

    for (int i = 0; i < leaf_node.page->header.num_keys; ++i) {
        leaf_node.values[i] = (char *) malloc(sizeof(char) * leaf_node.keys[i].size);
        memcpy(leaf_node.values[i], leaf_node.page->page_body + leaf_node.keys[i].offset - sizeof(page_header),
               leaf_node.keys[i].size);
    }
    return 0;
}

int read_page_body(struct leaf_and_data &leaf_node, int n) {
    // 나중에 인서트할 공간 미리 할당
    leaf_node.keys = (slot *) malloc(sizeof(slot) * (leaf_node.page->header.num_keys + n));
    leaf_node.values = (char **) malloc(sizeof(char *) * (leaf_node.page->header.num_keys + n));

    memcpy(leaf_node.keys, leaf_node.page->page_body, leaf_node.page->header.num_keys * sizeof(slot));

    for (int i = 0; i < leaf_node.page->header.num_keys; ++i) {
        leaf_node.values[i] = (char *) malloc(sizeof(char) * leaf_node.keys[i].size);
        memcpy(leaf_node.values[i], leaf_node.page->page_body + leaf_node.keys[i].offset - sizeof(page_header),
               leaf_node.keys[i].size);
    }
    return 0;
}

int write_page_body(struct leaf_and_data &leaf_node) {
    for (int i = 0; i < leaf_node.page->header.num_keys; ++i) {
        leaf_node.keys[i].offset = i ? leaf_node.keys[i - 1].offset - leaf_node.keys[i].size : PAGE_SIZE -
                                                                                               leaf_node.keys[i].size;

        memcpy(leaf_node.page->page_body + leaf_node.keys[i].offset - sizeof(page_header), leaf_node.values[i],
               leaf_node.keys[i].size);
        free(leaf_node.values[i]);
    }

    memcpy(leaf_node.page->page_body, leaf_node.keys, leaf_node.page->header.num_keys * sizeof(slot));
    free(leaf_node.keys);
    free(leaf_node.values);
    return 0;
}

// Open an existing database file or create one if not exist.
int64_t open_table(const char *pathname) {
    int64_t file_descriptor = file_open_table_file(pathname);
    int64_t table_id = stoi(pathname + 4);
    fd_table[table_id] = file_descriptor;
    struct header_page first_page;
    buffer_read_page(table_id, 0, (page_t *) &first_page);
    root[table_id] = first_page.root_page_num;
    // table_ids.push_back(file_descriptor);
    return table_id;
}

// Insert a record to the given table.
int db_insert(int64_t table_id, int64_t key, const char *value, uint16_t val_size) {
    pagenum_t leaf_offset;
    leaf_and_data leaf;

    if (find_key(table_id, key)) { // duplicates
        cout << "insertion error : key is already here (" << key << ")\n";
        return -1;
    }

    if (root[table_id] == 0) { // no tree

        header_page first_page;
        root[table_id] = make_leaf_page(table_id);


        buffer_read_page(table_id, 0, (page_t *) &first_page);
        first_page.root_page_num = root[table_id];
        buffer_write_page(table_id, 0, (page_t *) &first_page);

        buffer_read_page(table_id, root[table_id], (page_t *) leaf.page);
        read_page_body(leaf);
        leaf.keys[0].key = key;
        leaf.keys[0].size = val_size;
        leaf.keys[0].trx_id = 0;
        leaf.values[0] = (char *) malloc(sizeof(char) * val_size);
        memcpy(leaf.values[0], value, val_size);
        leaf.page->header.num_keys++;
        leaf.page->header.free_space -= (val_size + sizeof(slot));

        write_page_body(leaf);

        buffer_write_page(table_id, root[table_id], (page_t *) leaf.page);
        return 0;
    }

    leaf_offset = find_leaf(table_id, key);
    buffer_read_page(table_id, leaf_offset, (page_t *) leaf.page);

    if (leaf.page->header.free_space >= val_size + sizeof(slot)) { // leaf has room
        read_page_body(leaf);

        int i, insertion_point;

        insertion_point = 0;

        while (insertion_point < leaf.page->header.num_keys && leaf.keys[insertion_point].key < key)
            insertion_point++;

        for (i = leaf.page->header.num_keys; i > insertion_point; i--) {
            leaf.keys[i] = leaf.keys[i - 1];
            leaf.values[i] = leaf.values[i - 1];
        }

        leaf.keys[insertion_point].key = key;
        leaf.keys[insertion_point].size = val_size;
        leaf.keys[insertion_point].trx_id = 0;

        leaf.values[insertion_point] = (char *) malloc(sizeof(char) * val_size);
        memcpy(leaf.values[insertion_point], value, val_size);

        leaf.page->header.num_keys++;
        leaf.page->header.free_space -= (val_size + sizeof(slot));

        write_page_body(leaf);


        buffer_write_page(table_id, leaf_offset, (page_t *) leaf.page);

        return 0;
    }


    insert_into_leaf_after_splitting(table_id, leaf_offset, key, value, val_size);

    return 0;
}

pagenum_t insert_into_parent(int64_t table_id, pagenum_t left, int64_t key, pagenum_t right) {
    int left_index;
    pagenum_t parent;
    leaf_page left_page;
    leaf_page right_page;

    buffer_read_page(table_id, left, (page_t *) &left_page);
    buffer_read_page(table_id, right, (page_t *) &right_page);

    parent = left_page.header.parent_num;

    // new root
    if (parent == 0) {
        header_page first_page;
        internal_page new_root;
        pagenum_t new_root_offset;


        new_root_offset = make_node_page(table_id);
        root[table_id] = new_root_offset;

        buffer_read_page(table_id, 0, (page_t *) &first_page);
        first_page.root_page_num = new_root_offset;
        buffer_write_page(table_id, 0, (page_t *) &first_page);

        buffer_read_page(table_id, new_root_offset, (page_t *) &new_root);
        new_root.header.page_pointer = left;
        new_root.entrys[0].page_num = right;
        new_root.entrys[0].key = key;

        new_root.header.num_keys++;

        new_root.header.parent_num = 0;
        left_page.header.parent_num = new_root_offset;
        right_page.header.parent_num = new_root_offset;

        buffer_write_page(table_id, root[table_id], (page_t *) &new_root);
        buffer_write_page(table_id, left, (page_t *) &left_page);
        buffer_write_page(table_id, right, (page_t *) &right_page);

        // print_tree(table_id); // code for debug

        return new_root_offset;
    }

    internal_page parent_page;
    buffer_read_page(table_id, parent, (page_t *) &parent_page);

    left_index = 0;
    if (parent_page.header.page_pointer == left) {
        left_index = 0;
    } else {
        while (left_index < parent_page.header.num_keys &&
               parent_page.entrys[left_index].page_num != left)
            left_index++;
        left_index++;
    }

    // inter node has room
    if (parent_page.header.num_keys < ORDER * 2) {
        for (int i = parent_page.header.num_keys; i > left_index; --i) {
            parent_page.entrys[i] = parent_page.entrys[i - 1];
        }

        parent_page.entrys[left_index].key = key;
        parent_page.entrys[left_index].page_num = right;
        parent_page.header.num_keys++;

        buffer_write_page(table_id, parent, (page_t *) &parent_page);

        return root[table_id];
    }

    // node have to split

    return insert_into_internal_after_splitting(table_id, parent, left_index, key, right);
}

pagenum_t insert_into_leaf_after_splitting(int64_t table_id, pagenum_t leaf_offset,
                                           int64_t key, const char *value, uint16_t val_size) {
    pagenum_t new_leaf_offset;
    int insertion_point, split_index, i, j;
    int64_t new_key;
    leaf_and_data new_leaf, old_leaf;
    leaf_and_data *leaf_to_insert;

    new_leaf_offset = make_leaf_page(table_id);

    buffer_read_page(table_id, new_leaf_offset, (page_t *) new_leaf.page);
    buffer_read_page(table_id, leaf_offset, (page_t *) old_leaf.page);
    read_page_body(old_leaf);

    for (split_index = 0;
         old_leaf.keys[split_index].offset + old_leaf.keys[split_index].size >= SPLIT_OFFSET; ++split_index);

    // 이론상 1984 다음 것부터 스플릿 인덱스
    new_leaf.page->header.num_keys = old_leaf.page->header.num_keys - split_index;
    new_leaf.keys = (slot *) malloc(sizeof(slot) * (new_leaf.page->header.num_keys + 1));
    new_leaf.values = (char **) malloc(sizeof(char *) * (new_leaf.page->header.num_keys + 1));

    // 반으로 나눔
    for (i = split_index, j = 0; j < new_leaf.page->header.num_keys; ++i, ++j) {
        new_leaf.keys[j] = old_leaf.keys[i];

        new_leaf.values[j] = (char *) malloc(sizeof(char) * new_leaf.keys[j].size);

        memcpy(new_leaf.values[j], old_leaf.values[i], new_leaf.keys[j].size);

        free(old_leaf.values[i]);


        old_leaf.page->header.free_space += (old_leaf.keys[i].size + sizeof(slot));
        new_leaf.page->header.free_space -= (old_leaf.keys[i].size + sizeof(slot));

        old_leaf.page->header.num_keys--;
    }
    new_key = new_leaf.keys[0].key;

    // 값을 집어넣을 리프를 찾음
    if (new_leaf.keys[0].key <= key) {
        leaf_to_insert = &new_leaf;
        write_page_body(old_leaf);
    } else {
        leaf_to_insert = &old_leaf;
        write_page_body(new_leaf);
    }

    // insertion part

    insertion_point = 0;

    while (insertion_point < leaf_to_insert->page->header.num_keys && leaf_to_insert->keys[insertion_point].key < key)
        insertion_point++;

    for (i = leaf_to_insert->page->header.num_keys; i > insertion_point; i--) {
        leaf_to_insert->keys[i] = leaf_to_insert->keys[i - 1];
        leaf_to_insert->values[i] = leaf_to_insert->values[i - 1];
    }

    leaf_to_insert->keys[insertion_point].key = key;
    leaf_to_insert->keys[insertion_point].size = val_size;

    leaf_to_insert->values[insertion_point] = (char *) malloc(sizeof(char) * val_size);
    memcpy(leaf_to_insert->values[insertion_point], value, val_size);

    leaf_to_insert->page->header.num_keys++;
    leaf_to_insert->page->header.free_space -= (val_size + sizeof(slot));
    write_page_body(*leaf_to_insert);

    // 부모 통일
    new_leaf.page->header.parent_num = old_leaf.page->header.parent_num;
    // 다름 리프 연결
    new_leaf.page->header.page_pointer = old_leaf.page->header.page_pointer;
    old_leaf.page->header.page_pointer = new_leaf_offset;

    buffer_write_page(table_id, leaf_offset, (page_t *) old_leaf.page);
    buffer_write_page(table_id, new_leaf_offset, (page_t *) new_leaf.page);

    return insert_into_parent(table_id, leaf_offset, new_key, new_leaf_offset);
}

pagenum_t insert_into_internal_after_splitting(int64_t table_id, pagenum_t old_page_offset,
                                               int left_index, int64_t key, pagenum_t right_offset) {
    pagenum_t new_page_offset = make_node_page(table_id);
    internal_page new_page;
    internal_page old_page;
    int64_t new_key;
    leaf_page child;
    int j;

    buffer_read_page(table_id, new_page_offset, (page_t *) &new_page);
    buffer_read_page(table_id, old_page_offset, (page_t *) &old_page);


    if (left_index <= ORDER) {
        j = 0;
        for (int i = ORDER; i < ORDER * 2; ++i, ++j) {
            new_page.entrys[j] = old_page.entrys[i];
            old_page.entrys[i] = {0, 0};
            old_page.header.num_keys--;
            new_page.header.num_keys++;
        }

        if (left_index < ORDER) {
            new_page.header.page_pointer = old_page.entrys[ORDER - 1].page_num;
            new_key = old_page.entrys[ORDER - 1].key; // 인터널에서 남는 값을 올리기
//            for (int i = old_page.header.num_keys - 1; i > left_index; --i) {
            for (int i = ORDER - 1; i > left_index; --i) {
                old_page.entrys[i] = old_page.entrys[i - 1];
            }
            old_page.entrys[left_index].key = key;
            old_page.entrys[left_index].page_num = right_offset;
        } else {
            new_key = key; // 인터널에서 남는 값을 올리기
            new_page.header.page_pointer = right_offset;
        }
    } else if (left_index > ORDER) {
        new_page.header.page_pointer = old_page.entrys[ORDER].page_num;
        new_key = old_page.entrys[ORDER].key; // 인터널에서 남는 값을 올리기

        j = 0;
        for (int i = ORDER + 1; i < ORDER * 2; ++i, ++j) {
            if (i == left_index) {
                new_page.entrys[j].key = key;
                new_page.entrys[j++].page_num = right_offset;
                old_page.header.num_keys--;
                new_page.header.num_keys++;
            }
            new_page.entrys[j] = old_page.entrys[i];
            old_page.entrys[i] = {0, 0};
            old_page.header.num_keys--;
            new_page.header.num_keys++;
        }
        if (left_index == ORDER * 2) {
            new_page.entrys[j].key = key;
            new_page.entrys[j].page_num = right_offset;
            old_page.header.num_keys--;
            new_page.header.num_keys++;
        }
    }

//    new_key = new_page.entrys[0].key; //값 복사해서 올리기
    new_page.header.parent_num = old_page.header.parent_num;

//  new_page 로 부모 바꿔주기
    buffer_read_page(table_id, new_page.header.page_pointer, (page_t *) &child);
    child.header.parent_num = new_page_offset;
    buffer_write_page(table_id, new_page.header.page_pointer, (page_t *) &child);
    for (int i = 0; i < new_page.header.num_keys; ++i) {
        buffer_read_page(table_id, new_page.entrys[i].page_num, (page_t *) &child);
        child.header.parent_num = new_page_offset;
        buffer_write_page(table_id, new_page.entrys[i].page_num, (page_t *) &child);
    }

    buffer_write_page(table_id, new_page_offset, (page_t *) &new_page);
    buffer_write_page(table_id, old_page_offset, (page_t *) &old_page);

    return insert_into_parent(table_id, old_page_offset, new_key, new_page_offset);
}


// Delete a record with the matching key from the given table.
int db_delete(int64_t table_id, int64_t key) {
    if (!find_key(table_id, key)) {
        cout << "delete error : there is no key (" << key << ")\n";
        return -1;
    }
    pagenum_t leaf_offset = find_leaf(table_id, key);
    pagenum_t neighbor_offset;
    leaf_and_data leaf;
    leaf_and_data neighbor;
    internal_page parent;
    int neighbor_index;
    int new_key_index;
    int64_t k_prime;
    int i, j;

    buffer_read_page(table_id, leaf_offset, (page_t *) leaf.page);
    read_page_body(leaf);

    i = 0;
    while (leaf.keys[i].key != key)
        i++;
    free(leaf.values[i]);
    leaf.page->header.free_space += (leaf.keys[i].size + sizeof(slot));
    for (i++; i < leaf.page->header.num_keys; ++i) {
        leaf.keys[i - 1] = leaf.keys[i];
        leaf.values[i - 1] = leaf.values[i];
    }

    leaf.page->header.num_keys--;
    write_page_body(leaf);

    // case : deletion from the root.
    if (leaf.page->header.parent_num == 0) {
        if (leaf.page->header.num_keys == 0) {
            buffer_free_page(table_id, leaf_offset);
            header_page hp;
            buffer_read_page(table_id, 0, (page_t *) &hp);
            hp.root_page_num = 0;
            buffer_write_page(table_id, 0, (page_t *) &hp);
            root[table_id] = 0;
            // cout << "flag : root disappear" << endl;
            return 0;
        }
        buffer_write_page(table_id, leaf_offset, (page_t *) leaf.page);
        return 0;
    }
    // case : leaf has enough size
    if (leaf.page->header.free_space < THRESHOLD) {
        buffer_write_page(table_id, leaf_offset, (page_t *) leaf.page);
        return 0;
    }

    buffer_read_page(table_id, leaf.page->header.parent_num, (page_t *) &parent);

    if (parent.header.page_pointer == leaf_offset) {
        neighbor_index = -1;
        neighbor_offset = parent.entrys[0].page_num;
    } else {
        for (i = 0; i < parent.header.num_keys; ++i) {
            if (parent.entrys[i].page_num == leaf_offset) {
                neighbor_index = i;
                if (i == 0) {
                    neighbor_offset = parent.header.page_pointer;
                } else {
                    neighbor_offset = parent.entrys[i - 1].page_num;
                }
            }
        }
    }

    new_key_index = neighbor_index == -1 ? 0 : neighbor_index;

    buffer_read_page(table_id, neighbor_offset, (page_t *) neighbor.page); //here
    read_page_body(neighbor);
    read_page_body(leaf);

    // merge
    if (leaf.page->header.free_space + neighbor.page->header.free_space + sizeof(page_header) >= PAGE_SIZE) {
        // cout << "flag : merge leaf \n";
        leaf_and_data *leaf_pointer = &leaf;
        leaf_and_data *neighbor_pointer = &neighbor;

        if (neighbor_index == -1) {
            pagenum_t tmp1 = leaf_offset;
            leaf_offset = neighbor_offset;
            neighbor_offset = tmp1;
            leaf_and_data *tmp2 = leaf_pointer;
            leaf_pointer = neighbor_pointer;
            neighbor_pointer = tmp2;
        }

        int insertion_index = neighbor_pointer->page->header.num_keys;

        write_page_body(*neighbor_pointer);
        read_page_body(*neighbor_pointer, leaf_pointer->page->header.num_keys); //neighbor 동적할당 해줘야함

        for (i = insertion_index, j = 0; j < leaf_pointer->page->header.num_keys; ++i, ++j) {
            neighbor_pointer->keys[i] = leaf_pointer->keys[j];
            neighbor_pointer->values[i] = leaf_pointer->values[j];

            neighbor_pointer->page->header.free_space -= (neighbor_pointer->keys[i].size + sizeof(slot));
            neighbor_pointer->page->header.num_keys++;
        }

        leaf_pointer->page->header.num_keys = 0;

        neighbor_pointer->page->header.page_pointer = leaf_pointer->page->header.page_pointer;

        write_page_body(*neighbor_pointer);
        write_page_body(*leaf_pointer);

        buffer_write_page(table_id, neighbor_offset, (page_t *) neighbor_pointer->page);
        buffer_free_page(table_id, leaf_offset);

        delete_entry(table_id, neighbor_pointer->page->header.parent_num, new_key_index);

        return 0;
    }
        // redistribute
    else {
        // cout << "flag : redistribute leaf \n";
        if (neighbor_index != -1) {
            int n = 2;
            if (leaf.page->header.free_space - //하나만 재분배 해주면 될 경우
                (neighbor.keys[neighbor.page->header.num_keys - 1].size + sizeof(slot))
                < THRESHOLD) {
                n = 1;
            }
            // 옆으로 옮겨주는 과정
            for (int k = leaf.page->header.num_keys + n - 1; k >= n; --k) {
                leaf.keys[k] = leaf.keys[k - n];
                leaf.values[k] = leaf.values[k - n];
            }
            // 재분배
            for (int k = n - 1; k >= 0; --k) { //식 검토
                leaf.keys[k] = neighbor.keys[neighbor.page->header.num_keys - 1];
                leaf.values[k] = neighbor.values[neighbor.page->header.num_keys - 1];

                leaf.page->header.free_space -= (leaf.keys[k].size + sizeof(slot));
                neighbor.page->header.free_space += (leaf.keys[k].size + sizeof(slot));

                leaf.page->header.num_keys++;
                neighbor.page->header.num_keys--;
            }
            k_prime = leaf.keys[0].key;
        } else {
            int n = 2;
            if (leaf.page->header.free_space - //하나만 재분배 해주면 될 경우
                (neighbor.keys[0].size + sizeof(slot))
                < THRESHOLD) {
                n = 1;
            }
            // 재분배
            for (int k = 0; k < n; ++k) {
                leaf.keys[leaf.page->header.num_keys] = neighbor.keys[k];
                leaf.values[leaf.page->header.num_keys] = neighbor.values[k];

                leaf.page->header.free_space -= (neighbor.keys[k].size + sizeof(slot));
                neighbor.page->header.free_space += (neighbor.keys[k].size + sizeof(slot));

                leaf.page->header.num_keys++;
                neighbor.page->header.num_keys--;
            }
            // 옆으로 옮겨주는 과정
            for (int k = 0; k < neighbor.page->header.num_keys; ++k) {
                neighbor.keys[k] = neighbor.keys[k + n];
                neighbor.values[k] = neighbor.values[k + n];
            }
            k_prime = neighbor.keys[0].key;
        }

        // 부모 키 바꿔줘야함
        parent.entrys[new_key_index].key = k_prime;
        buffer_write_page(table_id, leaf.page->header.parent_num, (page_t *) &parent);

        write_page_body(leaf);
        write_page_body(neighbor);

        buffer_write_page(table_id, leaf_offset, (page_t *) leaf.page);
        buffer_write_page(table_id, neighbor_offset, (page_t *) neighbor.page);

        return 0;
    }

    return 0;
}

pagenum_t delete_entry(int64_t table_id, pagenum_t n_offset, int deletion_index) {
    internal_page n;
    buffer_read_page(table_id, n_offset, (page_t *) &n);

    n.header.num_keys--;
    for (int i = deletion_index; i < n.header.num_keys; ++i) {
        n.entrys[i] = n.entrys[i + 1];
    }
    // case : root
    // if (n_offset == root[table_id]){
    if (n.header.parent_num == 0) {
        if (n.header.num_keys > 0) {
            buffer_write_page(table_id, n_offset, (page_t *) &n);
            return root[table_id];
        }
        // cout << "flag : delete root" << endl;
        header_page hp;
        internal_page new_root_page;
        pagenum_t new_root_offset = n.header.page_pointer;

        buffer_read_page(table_id, 0, (page_t *) &hp);
        hp.root_page_num = new_root_offset;
        buffer_write_page(table_id, 0, (page_t *) &hp);


        buffer_read_page(table_id, new_root_offset, (page_t *) &new_root_page);
        new_root_page.header.parent_num = 0;
        buffer_write_page(table_id, new_root_offset, (page_t *) &new_root_page);

        root[table_id] = new_root_offset;

        buffer_free_page(table_id, n_offset);

        return root[table_id];
    }
    // case : internal node has enough keys
    if (n.header.num_keys >= ORDER) {
        buffer_write_page(table_id, n_offset, (page_t *) &n);
        return root[table_id];
    }

    /* Case:  node falls below minimum.
     * Either coalescence or redistribution
     * is needed.
     */

    buffer_write_page(table_id, n_offset, (page_t *) &n);

    int i, neighbor_index, new_key_index;
    pagenum_t neighbor_offset;
    internal_page neighbor;
    internal_page parent;

    buffer_read_page(table_id, n.header.parent_num, (page_t *) &parent);

    if (parent.header.page_pointer == n_offset) {
        neighbor_index = -1;
        neighbor_offset = parent.entrys[0].page_num;
    } else {
        for (i = 0; i < parent.header.num_keys; ++i) {
            if (parent.entrys[i].page_num == n_offset) {
                neighbor_index = i;
                if (i == 0) {
                    neighbor_offset = parent.header.page_pointer;
                } else {
                    neighbor_offset = parent.entrys[i - 1].page_num;
                }
            }
        }
    }

    buffer_read_page(table_id, neighbor_offset, (page_t *) &neighbor);

    if (neighbor.header.num_keys + n.header.num_keys < ORDER * 2)
        return coalesce_nodes(table_id, n_offset, neighbor_offset, neighbor_index);

    else
        return redistribute_nodes(table_id, n_offset, neighbor_offset, neighbor_index);
}

pagenum_t coalesce_nodes(int64_t table_id, pagenum_t n_offset, pagenum_t neighbor_offset, int neighbor_index) {
    internal_page n, neighbor, parent;
    int i, j, neighbor_insertion_index, n_end, k_prime_index;
    int64_t k_prime;
    pagenum_t tmp;
    internal_page tmp_page;

    //  cout << "flag : coalesce internal n is " << n_offset << " neighbor_offset : " << neighbor_offset << " neighbor_index : " << neighbor_index << endl;

    if (neighbor_index == -1) {
        tmp = n_offset;
        n_offset = neighbor_offset;
        neighbor_offset = tmp;
    }

    buffer_read_page(table_id, n_offset, (page_t *) &n);
    buffer_read_page(table_id, neighbor_offset, (page_t *) &neighbor);
    buffer_read_page(table_id, neighbor.header.parent_num, (page_t *) &parent);

    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    k_prime = parent.entrys[k_prime_index].key;

    neighbor_insertion_index = neighbor.header.num_keys;

    neighbor.entrys[neighbor_insertion_index].key = k_prime;
    neighbor.entrys[neighbor_insertion_index].page_num = n.header.page_pointer;
    neighbor.header.num_keys++;

    n_end = n.header.num_keys;

    for (i = neighbor_insertion_index + 1, j = 0; j < n_end; ++i, ++j) {
        neighbor.entrys[i] = n.entrys[j];
        neighbor.header.num_keys++;
        n.header.num_keys--;
    }

    // 부모를 바꿔주는 과정
    for (i = 0; i < neighbor.header.num_keys; ++i) {
        buffer_read_page(table_id, neighbor.entrys[i].page_num, (page_t *) &tmp_page);
        tmp_page.header.parent_num = neighbor_offset;
        buffer_write_page(table_id, neighbor.entrys[i].page_num, (page_t *) &tmp_page);
    }

    buffer_write_page(table_id, neighbor_offset, (page_t *) &neighbor);
    buffer_free_page(table_id, n_offset);

    tmp = delete_entry(table_id, neighbor.header.parent_num, k_prime_index);

    return tmp;
}

pagenum_t redistribute_nodes(int64_t table_id, pagenum_t n_offset, pagenum_t neighbor_offset, int neighbor_index) {
    pagenum_t tmp;
    internal_page tmp_page, parent, n, neighbor;
    int k_prime_index, i;

    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;

    buffer_read_page(table_id, n_offset, (page_t *) &n);
    buffer_read_page(table_id, neighbor_offset, (page_t *) &neighbor);
    buffer_read_page(table_id, n.header.parent_num, (page_t *) &parent);

    //부모변경 해줘야함!!
    if (neighbor_index != -1) {
        //  cout << "flag : redistribute internal (not -1)" << endl;
        for (i = n.header.num_keys; i > 0; --i) {
            n.entrys[i] = n.entrys[i - 1];
        }
        n.entrys[0].page_num = n.header.page_pointer;
        n.entrys[0].key = parent.entrys[k_prime_index].key;

        parent.entrys[k_prime_index].key = neighbor.entrys[neighbor.header.num_keys - 1].key;

        n.header.page_pointer = neighbor.entrys[neighbor.header.num_keys - 1].page_num; // 여기 부모 바뀜
        neighbor.header.num_keys--;
        n.header.num_keys++;

        tmp = n.header.page_pointer;
    } else {
        // cout << "flag : redistribute internal (-1)" << endl;
        n.entrys[n.header.num_keys].key = parent.entrys[k_prime_index].key;
        n.entrys[n.header.num_keys].page_num = neighbor.header.page_pointer; // 여기 부모 바뀜
        tmp = n.entrys[n.header.num_keys].page_num;

        parent.entrys[k_prime_index].key = neighbor.entrys[0].key;

        neighbor.header.page_pointer = neighbor.entrys[0].page_num;
        neighbor.header.num_keys--;
        n.header.num_keys++;


        for (i = 0; i < neighbor.header.num_keys; ++i) {
            neighbor.entrys[i] = neighbor.entrys[i + 1];
        }
    }

    buffer_read_page(table_id, tmp, (page_t *) &tmp_page);
    tmp_page.header.parent_num = n_offset;
    buffer_write_page(table_id, tmp, (page_t *) &tmp_page);

    buffer_write_page(table_id, n_offset, (page_t *) &n);
    buffer_write_page(table_id, neighbor_offset, (page_t *) &neighbor);
    buffer_write_page(table_id, n.header.parent_num, (page_t *) &parent);

    return root[table_id];
}

// Find records with a key between the range: begin_key ≤ key ≤ end_key
int db_scan(int64_t table_id, int64_t begin_key,
            int64_t end_key, std::vector<int64_t> *keys,
            std::vector<char *> *values,
            std::vector<uint16_t> *val_sizes) {
    int i;
    long long num_found = 0;
    pagenum_t n;
    leaf_and_data leaf;

    n = find_leaf(table_id, begin_key);
    buffer_read_page(table_id, n, (page_t *) leaf.page);


    if (n == 0) {
        cout << "scan error : there is no tree.\n";
        return -1;
    }

    read_page_body(leaf);

    for (i = 0; i < leaf.page->header.num_keys &&
                leaf.keys[i].key < begin_key; ++i);
    if (i == leaf.page->header.num_keys) {
        write_page_body(leaf);
        return 0; // 범위에 해당하는 키가 없는경우
    }
    while (n != 0) {
        for (; i < leaf.page->header.num_keys && leaf.keys[i].key <= end_key; ++i) {
            keys->push_back(leaf.keys[i].key);
            val_sizes->push_back(leaf.keys[i].size);
            values->push_back(nullptr);
            (*values)[num_found] = (char *) malloc(sizeof(char) * leaf.keys[i].size);
            memcpy((*values)[num_found], leaf.values[i], leaf.keys[i].size);
            num_found++;
        }
        write_page_body(leaf);
        if (i < leaf.page->header.num_keys)
            break;
        n = leaf.page->header.page_pointer;
        if (n == 0)
            break;
        buffer_read_page(table_id, n, (page_t *) leaf.page);
        read_page_body(leaf);
        i = 0;
    }

    return 0;
}

// Shutdown the database system.
int shutdown_db() {
    // if (!db_opened){
    //     return -1;
    // }
    // db_opened = false;
    // free(root);
    // close tables
    shutdown_buffer();
    file_close_table_files();
    return 0;
}
