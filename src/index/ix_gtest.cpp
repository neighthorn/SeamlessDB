#undef NDEBUG

#define private public
#include "ix.h"
#undef private  // for use private variables in "ix.h"

#include <cassert>
#include <ctime>
#include <iostream>
#include <map>

#include "gtest/gtest.h"
#include "transaction/transaction.h"

// 创建IxManager类的对象ix_manager
auto disk_manager = std::make_unique<DiskManager>();
auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
auto ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
auto txn = std::make_unique<Transaction>(0);

// dfs遍历整个树
void check_tree(const IxIndexHandle *ih, int now_page_no) {
    IxNodeHandle *node = ih->fetch_node(now_page_no);
    if (node->is_leaf_page()) {
        buffer_pool_manager->unpin_page(node->get_page_id(), false);
        return;
    }
    for (int i = 0; i < node->get_size(); i++) {                 // 遍历node的所有孩子
        IxNodeHandle *child = ih->fetch_node(node->value_at(i));  // 第i个孩子
        // check parent
        assert(child->get_parent_page_no() == now_page_no);
        // check first key
        int node_key = node->key_at(i);  // node的第i个key
        int child_first_key = child->key_at(0);
        int child_last_key = child->key_at(child->get_size() - 1);
        if (i != 0) {
            // 除了第0个key之外，node的第i个key与其第i个孩子的第0个key的值相同
            assert(node_key == child_first_key);
            // assert(memcmp(node->get_key(i), child->get_key(0), ih->file_hdr_.col_len) == 0);
        }
        if (i + 1 < node->get_size()) {
            assert(child_last_key < node->key_at(i + 1));
        }

        buffer_pool_manager->unpin_page(child->get_page_id(), false);

        check_tree(ih, node->value_at(i));  // 递归子树
    }
    buffer_pool_manager->unpin_page(node->get_page_id(), false);
}

void check_leaf(const IxIndexHandle *ih) {
    // check leaf list
    int leaf_no = ih->file_hdr_.first_leaf;
    while (leaf_no != IX_LEAF_HEADER_PAGE) {
        IxNodeHandle *curr = ih->fetch_node(leaf_no);
        IxNodeHandle *prev = ih->fetch_node(curr->get_prev_leaf());
        IxNodeHandle *next = ih->fetch_node(curr->get_next_leaf());
        // Ensure prev->next == curr && next->prev == curr
        assert(prev->get_next_leaf() == leaf_no);
        assert(next->get_prev_leaf() == leaf_no);
        leaf_no = curr->get_next_leaf();
        buffer_pool_manager->unpin_page(curr->get_page_id(), false);
        buffer_pool_manager->unpin_page(prev->get_page_id(), false);
        buffer_pool_manager->unpin_page(next->get_page_id(), false);
    }
}

void check_equal(IxIndexHandle *ih, const std::multimap<int, Rid> &mock) {
    // check_tree(ih, ih->file_hdr_.root_page);
    // check_leaf(ih);
    // for (auto &entry : mock) {
    //     int mock_key = entry.first;
    //     // test lower bound
    //     {
    //         auto mock_lower = mock.lower_bound(mock_key);        // multimap的lower_bound方法
    //         Iid iid = ih->lower_bound((const char *)&mock_key);  // IxIndexHandle的lower_bound方法
    //         Rid rid = ih->get_rid(iid);
    //         assert(rid == mock_lower->second);
    //     }
    //     // test upper bound
    //     {
    //         auto mock_upper = mock.upper_bound(mock_key);
    //         Iid iid = ih->upper_bound((const char *)&mock_key);
    //         if (iid != ih->leaf_end()) {
    //             // 在调用ih->get_rid(iid)时，其中的参数iid!=ih->leaf_end()
    //             Rid rid = ih->get_rid(iid);
    //             assert(rid == mock_upper->second);
    //         }
    //     }
    // }

    // // test scan
    // IxScan scan(ih, ih->leaf_begin(), ih->leaf_end(), buffer_pool_manager.get());
    // auto it = mock.begin();

    // int leaf_no = ih->file_hdr_.first_leaf;
    // // printf("leaf_no=%d scan.iid.page_no=%d\n", leaf_no, scan.iid().page_no);
    // assert(leaf_no == scan.iid().page_no);

    // // 注意在scan里面是iid的slot_no自增
    // while (!scan.is_end() && it != mock.end()) {
    //     Rid mock_rid = it->second;
    //     Rid rid = scan.rid();
    //     assert(rid == mock_rid);
    //     // go to next slot_no
    //     it++;
    //     scan.next();
    // }
    // assert(scan.is_end() && it == mock.end());
    check_tree(ih, ih->file_hdr_.root_page);
    // printf("ih->file_hdr_.num_pages=%d\n", ih->file_hdr_.num_pages);  // DEBUG

    // if (ih->file_hdr_.num_pages > 2) {
    if (!ih->is_empty()) {
        check_leaf(ih);
    }

    for (auto &entry : mock) {
        int mock_key = entry.first;
        // test lower bound
        {
            auto mock_lower = mock.lower_bound(mock_key);        // multimap的lower_bound方法
            Iid iid = ih->lower_bound((const char *)&mock_key);  // IxIndexHandle的lower_bound方法
            Rid rid = ih->get_rid(iid);
            ASSERT_EQ(rid, mock_lower->second);
        }
        // test upper bound
        {
            auto mock_upper = mock.upper_bound(mock_key);
            Iid iid = ih->upper_bound((const char *)&mock_key);
            if (iid != ih->leaf_end()) {
                Rid rid = ih->get_rid(iid);
                ASSERT_EQ(rid, mock_upper->second);
            }
        }
    }

    // test scan
    IxScan scan(ih, ih->leaf_begin(), ih->leaf_end(), buffer_pool_manager.get());
    auto it = mock.begin();
    int leaf_no = ih->file_hdr_.first_leaf;
    assert(leaf_no == scan.iid().page_no);
    // 注意在scan里面是iid的slot_no进行自增
    while (!scan.is_end() && it != mock.end()) {
        Rid mock_rid = it->second;
        Rid rid = scan.rid();
        // printf("rid=(%d,%d) mock_rid=(%d,%d)\n", rid.page_no, rid.slot_no, mock_rid.page_no, mock_rid.slot_no);
        // assert(rid == mock_rid);
        ASSERT_EQ(rid, mock_rid);
        // go to next slot_no
        it++;
        scan.next();
    }
    // assert(scan.is_end() && it == mock.end());
    ASSERT_EQ(scan.is_end(), true);
    ASSERT_EQ(it, mock.end());
}

// void print_btree(IxIndexHandle &ih, int root_page, int offset) {
//     IxNodeHandle node = ih.fetch_node_handle(root_page);
//     for (int i = node.page_hdr->num_key - 1; i > -1; i--) {
//         // print key
//         std::cout << std::string(offset, ' ') << *(int *)node.get_key(i) << std::endl;
//         // print child
//         if (!node.page_hdr->is_leaf) {
//             print_btree(ih, node.get_rid(i)->page_no, offset + 4);
//         }
//     }
// }

/**
 * @brief
 *
 * @param order 每个结点可存储的键值对数量的最大值(maxsize=order+1)
 * @param round 插入的个数，也是删除的个数
 */
void test_ix_insert_delete(int order, int round) {
    std::string filename = "abc";
    int index_no = 0;
    if (ix_manager->exists(filename, index_no)) {
        ix_manager->destroy_index(filename, index_no);
    }
    ix_manager->create_index(filename, index_no, TYPE_INT, sizeof(int));
    auto ih = ix_manager->open_index(filename, index_no);
    if (order > 2 && order <= ih->file_hdr_.btree_order) {
        ih->file_hdr_.btree_order = order;
    }
    std::multimap<int, Rid> mock;  // mock记录所有插入的(key,rid)键值对
    mock.clear();
    // 唯一索引，不支持相同的key，也不支持key和rid同时相同
    for (int i = 1; i < round; i++) {
        int rand_key = i;
        Rid rand_val = {.page_no = i, .slot_no = i};

        ih->insert_entry((const char *)&rand_key, rand_val, txn.get());
        mock.insert(std::make_pair(rand_key, rand_val));
        if (round % 5 == 0) {
            ix_manager->close_index(ih.get());
            ih = ix_manager->open_index(filename, index_no);
        }
    }
    std::cout << "Insert " << round << std::endl;
    //    print_btree(ih, ih.hdr.root_page, 0);
    check_equal(ih.get(), mock);
    for (int i = 1; i < round; i++) {
        auto it = mock.begin();
        int key = it->first;
        Rid rid = it->second;
        ih->delete_entry((const char *)&key, rid, txn.get());
        mock.erase(it);
        if (round % 5 == 0) {
            ix_manager->close_index(ih.get());
            ih = ix_manager->open_index(filename, index_no);
        }
    }
    std::cout << "Delete " << round << std::endl;
    check_equal(ih.get(), mock);
    ix_manager->close_index(ih.get());
    ix_manager->destroy_index(filename, index_no);
}

void test_ix(int order, int round) {
    std::string filename = "abc";
    int index_no = 0;
    if (ix_manager->exists(filename, index_no)) {
        ix_manager->destroy_index(filename, index_no);
    }
    ix_manager->create_index(filename, index_no, TYPE_INT, sizeof(int));
    auto ih = ix_manager->open_index(filename, index_no);

    // if (order >= 2 && order <= ih->file_hdr_.btree_order) {
    //     ih->file_hdr_.btree_order = order;
    // }
    // int add_cnt = 0;
    // int del_cnt = 0;
    // std::multimap<int, Rid> mock;
    // mock.clear();
    // for (int i = 0; i < round; i++) {
    //     double dice = rand() * 1. / RAND_MAX;
    //     double insert_prob = 1. - mock.size() / (0.5 * round);
    //     if (mock.empty() || dice < insert_prob) {
    //         // Insert
    //         int rand_key = rand() % round;
    //         if (mock.find(rand_key) != mock.end()) {
    //             // printf("重复key=%d!\n", rand_key);
    //             continue;
    //         }
    //         Rid rand_val = {.page_no = rand(), .slot_no = rand()};
    //         ih->insert_entry((const char *)&rand_key, rand_val, txn.get());
    //         mock.insert(std::make_pair(rand_key, rand_val));
    //         add_cnt++;
    //         // printf("insert rand_key=%d\n", rand_key);
    //     } else {
    //         // Delete
    //         int rand_idx = rand() % mock.size();
    //         auto it = mock.begin();
    //         for (int k = 0; k < rand_idx; k++) {
    //             it++;
    //         }
    //         int key = it->first;
    //         Rid rid = it->second;
    //         // ih->delete_entry((const char *)&key, rid, txn.get());
    //         mock.erase(it);
    //         del_cnt++;
    //         // printf("delete rand key=%d\n", key);
    //     }
    //     // Randomly re-open file
    //     if (round % 10 == 0) {
    //         ix_manager->close_index(ih.get());
    //         ih = ix_manager->open_index(filename, index_no);
    //     }
    // }
    // //    print_btree(ih, ih.hdr.root_page, 0);
    // std::cout << "Insert " << add_cnt << '\n' << "Delete " << del_cnt << '\n';
    // check_equal(ih.get(), mock);

    const int reopen_mod = 1;
    if (order >= 2 && order <= ih->file_hdr_.btree_order) {
        ih->file_hdr_.btree_order = order;
    }
    int add_cnt = 0;
    int del_cnt = 0;
    std::multimap<int, Rid> mock;
    mock.clear();
    int num = 0;
    while(num < round) {
        double dice = rand() * 1. / RAND_MAX;
        double insert_prob = 1. - mock.size() / (0.5 * round);
        if (mock.empty() || dice < insert_prob) {
            // Insert
            int rand_key = rand() % round;
            // printf("insert rand_key=%d\n", rand_key);
            if (mock.find(rand_key) != mock.end()) {
                // printf("重复key=%d!\n", rand_key);
                continue;
            }
            Rid rand_val = {.page_no = rand(), .slot_no = rand()};
            ih->insert_entry((const char *)&rand_key, rand_val, txn.get());
            mock.insert(std::make_pair(rand_key, rand_val));
            add_cnt++;
            // Draw(buffer_pool_manager_.get(),
            //      "MixTest2_" + std::to_string(num) + "_insert" + std::to_string(rand_key) + ".dot");
        } else {
            // Delete
            int rand_idx = rand() % mock.size();
            auto it = mock.begin();
            for (int k = 0; k < rand_idx; k++) {
                it++;
            }
            int key = it->first;
            // printf("delete rand key=%d\n", key);
            Rid rid = it->second;
            ih->delete_entry((const char *)&key, rid, txn.get());
            mock.erase(it);
            del_cnt++;
            // Draw(buffer_pool_manager_.get(),
            //      "MixTest2_" + std::to_string(num) + "_delete" + std::to_string(key) + ".dot");
        }
        num++;
        // printf("num=%d\n",num);
        // Randomly re-open file
        if (round % reopen_mod == 0) {
            ix_manager->close_index(ih.get());
            ih = ix_manager->open_index(filename, index_no);
        }
        // check_equal(ih.get(), mock);
    }
    std::cout << "Insert " << add_cnt << '\n' << "Delete " << del_cnt << '\n';
    check_equal(ih.get(), mock);

    while (!mock.empty()) {
        int rand_idx = rand() % mock.size();
        auto it = mock.begin();
        for (int k = 0; k < rand_idx; k++) {
            it++;
        }
        int key = it->first;
        Rid rid = it->second;
        ih->delete_entry((const char *)&key, rid, txn.get());
        mock.erase(it);
        // Randomly re-open file
        if (round % 10 == 0) {
            ix_manager->close_index(ih.get());
            ih = ix_manager->open_index(filename, index_no);
        }
    }
    check_equal(ih.get(), mock);


    ix_manager->close_index(ih.get());
    ix_manager->destroy_index(filename, index_no);
}

TEST(IndexManagerTest, SimpleTest1) {
    srand((unsigned)time(nullptr));
    test_ix_insert_delete(4, 10);
}

TEST(IndexManagerTest, SimpleTest2) {
    srand((unsigned)time(nullptr));
    test_ix(10, 500);
    // test_ix(3, 10);
}

TEST(IndexManagerTest, SimpleTest3) {
    srand((unsigned)time(nullptr));
    test_ix(-1, 500);
}

// big data
// test_ix_insert_delete(3, 1000);
// test_ix(4, 1000);
// test_ix(-1, 100000);