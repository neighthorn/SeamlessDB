#undef NDEBUG

#define private public
#include "ix.h"
#undef private  // for use private variables in "ix.h"

#include <cassert>
#include <ctime>
#include <iostream>
#include <map>

#include "gtest/gtest.h"

// 创建IxManager类的对象ix_manager
auto disk_manager = std::make_unique<DiskManager>();
auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
auto ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());

void check_tree(const IxIndexHandle *ih, int root_page) {
    IxNodeHandle node = ih->fetch_node_handle(root_page);
    if (node.page_hdr->is_leaf) {
        return;
    }
    for (int i = 0; i < node.page_hdr->num_child; i++) {
        IxNodeHandle child = ih->fetch_node_handle(node.get_rid(i)->page_no);
        // check parent
        assert(child.page_hdr->parent == root_page);
        // check last key
        assert(memcmp(node.get_key(i), child.get_key(child.page_hdr->num_key - 1), ih->file_hdr_.col_len) == 0);
        check_tree(ih, node.get_rid(i)->page_no);
    }
}

void check_leaf(const IxIndexHandle *ih) {
    // check leaf list
    int leaf_no = ih->file_hdr_.first_leaf;
    while (leaf_no != IX_LEAF_HEADER_PAGE) {
        IxNodeHandle curr = ih->fetch_node_handle(leaf_no);
        IxNodeHandle prev = ih->fetch_node_handle(curr.page_hdr->prev_leaf);
        IxNodeHandle next = ih->fetch_node_handle(curr.page_hdr->next_leaf);
        // Ensure prev->next == curr && next->prev == curr
        assert(prev.page_hdr->next_leaf == leaf_no);
        assert(next.page_hdr->prev_leaf == leaf_no);
        leaf_no = curr.page_hdr->next_leaf;
    }
}

void check_equal(const IxIndexHandle *ih, const std::multimap<int, Rid> &mock) {
    check_tree(ih, ih->file_hdr_.root_page);
    check_leaf(ih);
    for (auto &entry : mock) {
        int mock_key = entry.first;
        // test lower bound
        {
            auto mock_lower = mock.lower_bound(mock_key);
            Iid iid = ih->lower_bound((const char *)&mock_key);
            Rid rid = ih->get_rid(iid);
            assert(rid == mock_lower->second);
        }
        // test upper bound
        {
            auto mock_upper = mock.upper_bound(mock_key);
            Iid iid = ih->upper_bound((const char *)&mock_key);
            if (mock_upper == mock.end()) {
                assert(iid == ih->leaf_end());
            } else {
                Rid rid = ih->get_rid(iid);
                assert(rid == mock_upper->second);
            }
        }
    }
    // test scan
    IxScan scan(ih, ih->leaf_begin(), ih->leaf_end());
    auto it = mock.begin();
    while (!scan.is_end() && it != mock.end()) {
        Rid mock_rid = it->second;
        Rid rid = scan.rid();
        assert(rid == mock_rid);
        it++;
        scan.next();
    }
    assert(scan.is_end() && it == mock.end());
}

void print_btree(IxIndexHandle &ih, int root_page, int offset) {
    IxNodeHandle node = ih.fetch_node_handle(root_page);
    for (int i = node.page_hdr->num_child - 1; i > -1; i--) {
        // print key
        std::cout << std::string(offset, ' ') << *(int *)node.get_key(i) << std::endl;
        // print child
        if (!node.page_hdr->is_leaf) {
            print_btree(ih, node.get_rid(i)->page_no, offset + 4);
        }
    }
}

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
    std::multimap<int, Rid> mock;
    for (int i = 0; i < round; i++) {
        int rand_key = rand() % round;
        Rid rand_val = {.page_no = rand(), .slot_no = rand()};
        ih->insert_entry((const char *)&rand_key, rand_val);
        mock.insert(std::make_pair(rand_key, rand_val));
        if (round % 500 == 0) {
            ix_manager->close_index(ih.get());
            ih = ix_manager->open_index(filename, index_no);
        }
    }
    std::cout << "Insert " << round << std::endl;
    //    print_btree(ih, ih.hdr.root_page, 0);
    check_equal(ih.get(), mock);
    for (int i = 0; i < round; i++) {
        auto it = mock.begin();
        int key = it->first;
        Rid rid = it->second;
        ih->delete_entry((const char *)&key, rid);
        mock.erase(it);
        if (round % 500 == 0) {
            ix_manager->close_index(ih.get());
            ih = ix_manager->open_index(filename, index_no);
        }
    }
    std::cout << "delete " << round << std::endl;
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
    if (order >= 2 && order <= ih->file_hdr_.btree_order) {
        ih->file_hdr_.btree_order = order;
    }
    int add_cnt = 0;
    int del_cnt = 0;
    std::multimap<int, Rid> mock;
    for (int i = 0; i < round; i++) {
        double dice = rand() * 1. / RAND_MAX;
        double insert_prob = 1. - mock.size() / (0.5 * round);
        if (mock.empty() || dice < insert_prob) {
            // Insert
            int rand_key = rand() % round;
            Rid rand_val = {.page_no = rand(), .slot_no = rand()};
            ih->insert_entry((const char *)&rand_key, rand_val);
            mock.insert(std::make_pair(rand_key, rand_val));
            add_cnt++;
        } else {
            // Delete
            int rand_idx = rand() % mock.size();
            auto it = mock.begin();
            for (int k = 0; k < rand_idx; k++) {
                it++;
            }
            int key = it->first;
            Rid rid = it->second;
            ih->delete_entry((const char *)&key, rid);
            mock.erase(it);
            del_cnt++;
        }
        // Randomly re-open file
        if (round % 500 == 0) {
            ix_manager->close_index(ih.get());
            ih = ix_manager->open_index(filename, index_no);
        }
    }
    //    print_btree(ih, ih.hdr.root_page, 0);
    std::cout << "Insert " << add_cnt << '\n' << "Delete " << del_cnt << '\n';
    while (!mock.empty()) {
        int rand_idx = rand() % mock.size();
        auto it = mock.begin();
        for (int k = 0; k < rand_idx; k++) {
            it++;
        }
        int key = it->first;
        Rid rid = it->second;
        ih->delete_entry((const char *)&key, rid);
        mock.erase(it);
        // Randomly re-open file
        if (round % 500 == 0) {
            ix_manager->close_index(ih.get());
            ih = ix_manager->open_index(filename, index_no);
        }
    }
    check_equal(ih.get(), mock);
    ix_manager->close_index(ih.get());
    ix_manager->destroy_index(filename, index_no);
}

int main() {
    srand((unsigned) time(nullptr));
    // small data for test
    test_ix_insert_delete(3, 100);
    test_ix(4, 100);
    test_ix(-1, 100);
    // init
    // test_ix_insert_delete(3, 1000);
    // test_ix(4, 1000);
    // test_ix(-1, 100000);
    return 0;
}
