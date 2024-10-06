/**
 * b_plus_tree_insert_test.cpp
 */

#include <algorithm>
#include <cstdio>
#include <random>  // for std::default_random_engine

#include "gtest/gtest.h"

#define private public
#include "ix.h"
#undef private  // for use private variables in "ix.h"

#include "storage/buffer_pool_manager.h"

const std::string TEST_DB_NAME = "BPlusTreeInsertTest_db";  // 以数据库名作为根目录
const std::string TEST_FILE_NAME = "table1";                // 测试文件名的前缀
const int index_no = 0;                                     // 索引编号
// 创建的索引文件名为"table1.0.idx"（TEST_FILE_NAME + index_no + .idx）

/** 注意：每个测试点只测试了单个文件！
 * 对于每个测试点，先创建和进入目录TEST_DB_NAME
 * 然后在此目录下创建和打开索引文件"table1.0.idx"，记录IxIndexHandle */

// Add by jiawen
class BPlusTreeTests : public ::testing::Test {
   public:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<IxIndexHandle> ih_;
    std::unique_ptr<Transaction> txn_;

   public:
    // This function is called before every test.
    void SetUp() override {
        ::testing::Test::SetUp();
        // For each test, we create a new IxManager...
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(100, disk_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        txn_ = std::make_unique<Transaction>(0);

        // 如果测试目录不存在，则先创建测试目录
        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
        // 进入测试目录
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
        // 如果测试文件存在，则先删除原文件（最后留下来的文件存的是最后一个测试点的数据）
        if (ix_manager_->exists(TEST_FILE_NAME, index_no)) {
            ix_manager_->destroy_index(TEST_FILE_NAME, index_no);
        }
        // 创建测试文件
        ix_manager_->create_index(TEST_FILE_NAME, index_no, TYPE_INT, sizeof(int));
        assert(ix_manager_->exists(TEST_FILE_NAME, index_no));
        // 打开测试文件
        ih_ = ix_manager_->open_index(TEST_FILE_NAME, index_no);
        assert(ih_ != nullptr);
    }

    // This function is called after every test.
    void TearDown() override {
        ix_manager_->close_index(ih_.get());
        // ix_manager_->destroy_index(TEST_FILE_NAME, index_no);  // 若不删除数据库文件，则将保留最后一个测试点的数据

        // 返回上一层目录
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
    };

    void ToGraph(const IxIndexHandle *ih, IxNodeHandle *node, BufferPoolManager *bpm, std::ofstream &out) const {
        std::string leaf_prefix("LEAF_");
        std::string internal_prefix("INT_");
        // printf("node_page=%d root_page=%d\n", node.get_page_no(), node.GetRootPage());
        if (node->is_leaf_page()) {
            IxNodeHandle *leaf = node;
            // Print node name
            out << leaf_prefix << leaf->get_page_no();
            // Print node properties
            out << "[shape=plain color=green ";
            // Print data of the node
            out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
            // Print data
            out << "<TR><TD COLSPAN=\"" << leaf->get_size() << "\">page_no=" << leaf->get_page_no() << "</TD></TR>\n";
            out << "<TR><TD COLSPAN=\"" << leaf->get_size() << "\">"
                << "max_size=" << leaf->get_max_size() << ",min_size=" << leaf->get_min_size() << "</TD></TR>\n";
            out << "<TR>";
            for (int i = 0; i < leaf->get_size(); i++) {
                out << "<TD>" << leaf->key_at(i) << "</TD>\n";
            }
            out << "</TR>";
            // Print table end
            out << "</TABLE>>];\n";
            // Print Leaf node link if there is a next page
            if (leaf->get_next_leaf() != INVALID_PAGE_ID && leaf->get_next_leaf() > 1) {
                // 注意加上一个大于1的判断条件，否则若GetNextPageNo()是1，会把1那个结点也画出来
                out << leaf_prefix << leaf->get_page_no() << " -> " << leaf_prefix << leaf->get_next_leaf() << ";\n";
                out << "{rank=same " << leaf_prefix << leaf->get_page_no() << " " << leaf_prefix << leaf->get_next_leaf()
                    << "};\n";
            }

            // Print parent links if there is a parent
            if (leaf->get_parent_page_no() != INVALID_PAGE_ID) {
                out << internal_prefix << leaf->get_parent_page_no() << ":p" << leaf->get_page_no() << " -> " << leaf_prefix
                    << leaf->get_page_no() << ";\n";
            }
        } else {
            IxNodeHandle *inner = node;
            // Print node name
            out << internal_prefix << inner->get_page_no();
            // Print node properties
            out << "[shape=plain color=pink ";  // why not?
            // Print data of the node
            out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
            // Print data
            out << "<TR><TD COLSPAN=\"" << inner->get_size() << "\">page_no=" << inner->get_page_no() << "</TD></TR>\n";
            out << "<TR><TD COLSPAN=\"" << inner->get_size() << "\">"
                << "max_size=" << inner->get_max_size() << ",min_size=" << inner->get_min_size() << "</TD></TR>\n";
            out << "<TR>";
            for (int i = 0; i < inner->get_size(); i++) {
                out << "<TD PORT=\"p" << inner->value_at(i) << "\">";
                out << inner->key_at(i);
                // if (inner->key_at(i) != 0) {  // 原判断条件是if (i > 0)
                //     out << inner->key_at(i);
                // } else {
                //     out << " ";
                // }
                out << "</TD>\n";
            }
            out << "</TR>";
            // Print table end
            out << "</TABLE>>];\n";
            // Print Parent link
            if (inner->get_parent_page_no() != INVALID_PAGE_ID) {
                out << internal_prefix << inner->get_parent_page_no() << ":p" << inner->get_page_no() << " -> "
                    << internal_prefix << inner->get_page_no() << ";\n";
            }
            // Print leaves
            for (int i = 0; i < inner->get_size(); i++) {
                // auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->value_at(i))->get_data());
                IxNodeHandle *child_node = ih->fetch_node(inner->value_at(i));
                ToGraph(ih, child_node, bpm, out);  // 继续递归
                if (i > 0) {
                    // auto sibling_page =
                    //     reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->value_at(i - 1))->get_data());
                    IxNodeHandle *sibling_node = ih->fetch_node(inner->value_at(i - 1));

                    if (!sibling_node->is_leaf_page() && !child_node->is_leaf_page()) {
                        out << "{rank=same " << internal_prefix << sibling_node->get_page_no() << " " << internal_prefix
                            << child_node->get_page_no() << "};\n";
                    }
                    bpm->unpin_page(sibling_node->get_page_id(), false);
                }
            }
        }
        bpm->unpin_page(node->get_page_id(), false);
    }

    /**
     * @brief 生成B+树可视化图
     *
     * @param bpm 缓冲池
     * @param outf dot文件名
     */
    void Draw(BufferPoolManager *bpm, const std::string &outf) {
        std::ofstream out(outf);
        out << "digraph G {" << std::endl;
        IxNodeHandle *node = ih_->fetch_node(ih_->file_hdr_.root_page);
        ToGraph(ih_.get(), node, bpm, out);
        out << "}" << std::endl;
        out.close();

        // 由dot文件生成png文件
        std::string prefix = outf;
        prefix.replace(outf.rfind(".dot"), 4, "");
        std::string png_name = prefix + ".png";
        std::string cmd = "dot -Tpng " + outf + " -o " + png_name;
        system(cmd.c_str());

        // printf("Generate picture: build/%s/%s\n", TEST_DB_NAME.c_str(), png_name.c_str());
        printf("Generate picture: %s\n", png_name.c_str());
    }
};

/**
 * @brief 插入10个key，范围为1~10，插入的value取key的低32位，使用GetValue()函数测试插入的value(即Rid)是否正确
 * @note 每次插入后都会调用Draw()函数生成一个B+树的图
 */
TEST_F(BPlusTreeTests, InsertTest) {
    // SplitRootTest:
    // const int scale = 4;
    // const int order = 3;

    const int64_t scale = 10;
    const int order = 3;
    const int reopen_mod = 1;

    assert(order > 2 && order <= ih_->file_hdr_.btree_order);
    ih_->file_hdr_.btree_order = order;

    std::vector<int64_t> keys;
    for (int64_t key = 1; key <= scale; key++) {
        keys.push_back(key);
    }

    const char *index_key;
    for (auto key : keys) {
        int32_t value = key & 0xFFFFFFFF;  // key的低32位
        Rid rid = {.page_no = static_cast<int32_t>(key >> 32),
                   .slot_no = value};  // page_id = (key>>32), slot_num = (key & 0xFFFFFFFF)
        index_key = (const char *)&key;
        ih_->insert_entry(index_key, rid, txn_.get());  // 调用Insert

        Draw(buffer_pool_manager_.get(), "insert" + std::to_string(key) + ".dot");

        // reopen test
        if (key % reopen_mod == 0) {
            ix_manager_->close_index(ih_.get());
            ih_ = ix_manager_->open_index(TEST_FILE_NAME, index_no);
        }
    }

    std::vector<Rid> rids;
    for (auto key : keys) {
        rids.clear();
        index_key = (const char *)&key;
        ih_->get_value(index_key, &rids, txn_.get());  // 调用GetValue
        EXPECT_EQ(rids.size(), 1);

        int32_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].slot_no, value);

        // reopen test
        if (key % reopen_mod == 0) {
            ix_manager_->close_index(ih_.get());
            ih_ = ix_manager_->open_index(TEST_FILE_NAME, index_no);
        }
    }

    // 找不到未插入的数据
    for (int key = scale + 1; key <= scale + 100; key++) {
        rids.clear();
        index_key = (const char *)&key;
        ih_->get_value(index_key, &rids, txn_.get());  // 调用GetValue
        EXPECT_EQ(rids.size(), 0);
    }
}

/**
 * @brief 插入后，使用Ixscan来遍历
 */
TEST_F(BPlusTreeTests, InsertTestWithIxscan) {
    const int64_t scale = 1000;  // 插入规模：1000个数
    const int order = 100;
    const int reopen_mod = 1;

    assert(order > 2 && order <= ih_->file_hdr_.btree_order);
    ih_->file_hdr_.btree_order = order;

    std::vector<int64_t> keys;
    for (int64_t key = 1; key <= scale; key++) {
        keys.push_back(key);
    }

    const char *index_key;
    for (auto key : keys) {
        int32_t slot_no = key & 0xFFFFFFFF;  // key的低32位
        Rid rid = {.page_no = static_cast<int32_t>(key >> 32),
                   .slot_no = slot_no};  // page_id = (key>>32), slot_num = (key & 0xFFFFFFFF)
        index_key = (const char *)&key;

        ih_->insert_entry(index_key, rid, txn_.get());  // 调用Insert

        // reopen test
        if (key % reopen_mod == 0) {
            ix_manager_->close_index(ih_.get());
            ih_ = ix_manager_->open_index(TEST_FILE_NAME, index_no);
        }
    }

    int64_t start_key = 1;
    int64_t current_key = start_key;
    IxScan scan(ih_.get(), ih_->leaf_begin(), ih_->leaf_end(), buffer_pool_manager_.get());
    while (!scan.is_end()) {
        int32_t insert_page_no = static_cast<int32_t>(current_key >> 32);
        int32_t insert_slot_no = current_key & 0xFFFFFFFF;
        Rid rid = scan.rid();
        EXPECT_EQ(rid.page_no, insert_page_no);
        EXPECT_EQ(rid.slot_no, insert_slot_no);
        current_key++;
        scan.next();
    }
    EXPECT_EQ(current_key, keys.size() + 1);
}

/**
 * @brief debug
 */
// TEST_F(BPlusTreeTests, InsertTest2) {
//     const int order = 3;
//     const int reopen_mod = 1;

//     assert(order > 2 && order <= ih_->file_hdr_.btree_order);
//     ih_->file_hdr_.btree_order = order;

//     std::vector<int64_t> keys = {16, 25, 1, 19, 6, 8, 20};

//     const char *index_key;

//     // insert keys
//     int num = 0;
//     for (auto key : keys) {
//         int32_t value = key & 0xFFFFFFFF;  // key的低32位
//         Rid rid = {.page_no = static_cast<int32_t>(key >> 32),
//                    .slot_no = value};  // page_id = (key>>32), slot_num = (key & 0xFFFFFFFF)
//         index_key = (const char *)&key;
//         ih_->insert_entry(index_key, rid, txn_.get());
//         Draw(buffer_pool_manager_.get(), "InsertTest2_" + std::to_string(num) + "_" + std::to_string(key) + ".dot");

//         // reopen test
//         if (key % reopen_mod == 0) {
//             ix_manager_->close_index(ih_.get());
//             ih_ = ix_manager_->open_index(TEST_FILE_NAME, index_no);
//         }

//         num++;
//     }

//     std::vector<Rid> rids;
//     for (auto key : keys) {
//         rids.clear();
//         index_key = (const char *)&key;
//         ih_->get_value(index_key, &rids, txn_.get());  // 调用GetValue
//         EXPECT_EQ(rids.size(), 1);

//         int32_t value = key & 0xFFFFFFFF;
//         EXPECT_EQ(rids[0].slot_no, value);

//         // reopen test
//         if (key % reopen_mod == 0) {
//             ix_manager_->close_index(ih_.get());
//             ih_ = ix_manager_->open_index(TEST_FILE_NAME, index_no);
//         }
//     }
// }

/**
 * @brief Insert a set of keys range from 1 to 10000 in a random order.
 * Check whether the key-value pair is valid using get_value.
 * @note 在设置参数的时候注意：order不能太小。若order太小，而插入数据过多，将会超出缓冲池
 * 缓冲池的大小也不能太大，因为每次调用close_index()都会FlushAllPages将缓冲池里面的所有页面刷到磁盘
 */
TEST_F(BPlusTreeTests, LargeScaleTest) {
    const int64_t scale = 10000;
    const int order = 256;
    const int reopen_mod = 1;

    assert(order > 2 && order <= ih_->file_hdr_.btree_order);
    ih_->file_hdr_.btree_order = order;

    std::vector<int64_t> keys;
    for (int64_t key = 1; key <= scale; key++) {
        keys.push_back(key);
    }

    // randomized the insertion order
    auto rng = std::default_random_engine{};
    std::shuffle(keys.begin(), keys.end(), rng);

    // for (auto key : keys) {
    //     printf("key=%ld\n", key);
    // }

    const char *index_key;
    for (auto key : keys) {
        int32_t value = key & 0xFFFFFFFF;  // key的低32位
        Rid rid = {.page_no = static_cast<int32_t>(key >> 32),
                   .slot_no = value};  // page_id = (key>>32), slot_num = (key & 0xFFFFFFFF)
        index_key = (const char *)&key;
        ih_->insert_entry(index_key, rid, txn_.get());  // 调用Insert
        // printf("insert key=%ld page_no=%d\n", key, page_no);

        // Draw(buffer_pool_manager_.get(), "scale_insert" + std::to_string(key) + ".dot");
        // printf("\n");

        // reopen test
        if (key % reopen_mod == 0) {
            ix_manager_->close_index(ih_.get());
            ih_ = ix_manager_->open_index(TEST_FILE_NAME, index_no);
        }
    }

    // test get_value
    std::vector<Rid> rids;
    for (auto key : keys) {
        rids.clear();
        index_key = (const char *)&key;
        ih_->get_value(index_key, &rids, txn_.get());  // 调用GetValue
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].slot_no, value);

        // reopen test
        if (key % reopen_mod == 0) {
            ix_manager_->close_index(ih_.get());
            ih_ = ix_manager_->open_index(TEST_FILE_NAME, index_no);
        }
    }

    // test Ixscan
    int64_t start_key = 1;
    int64_t current_key = start_key;
    IxScan scan(ih_.get(), ih_->leaf_begin(), ih_->leaf_end(), buffer_pool_manager_.get());
    while (!scan.is_end()) {
        int32_t insert_page_no = static_cast<int32_t>(current_key >> 32);
        int32_t insert_slot_no = current_key & 0xFFFFFFFF;
        Rid rid = scan.rid();
        EXPECT_EQ(rid.page_no, insert_page_no);
        EXPECT_EQ(rid.slot_no, insert_slot_no);
        current_key++;
        scan.next();
    }
    EXPECT_EQ(current_key, keys.size() + 1);
}