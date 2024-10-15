#include "ix_index_handle.h"

#include "ix_scan.h"

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd, const TabMeta& table_meta)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd), table_meta_(table_meta) {
    // init file_hdr_
    // disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    // char buf[PAGE_SIZE];
    // memset(buf, 0, PAGE_SIZE);
    // disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
    PageId pageid = PageId{table_meta.table_id_, IX_FILE_HDR_PAGE};
    Page* page = buffer_pool_manager_->fetch_page(pageid);
    // file_hdr_= IxFileHdr();
    file_hdr_ = new IxFileHdr();
    file_hdr_->deserialize(page->get_data());
    buffer_pool_manager_->unpin_page(pageid, false);
    // disk_manager管理的fd对应的文件中，设置从file_hdr_->num_pages开始分配page_no
    // disk_manager_->set_fd2pageno(fd, file_hdr_->num_pages);
    // int now_page_no = disk_manager_->get_fd2pageno(fd);
    // if(now_page_no )
    // disk_manager_->set_fd2pageno(fd, now_page_no + 1);
}

std::unique_ptr<Record> IxIndexHandle::get_record(const Rid& rid, Context* context) {
    IxNodeHandle* node = fetch_node(rid.page_no);
    char* record_slot = node->leaf_get_record_at(rid.slot_no);
    auto record = std::make_unique<Record>(record_slot, file_hdr_->record_len_);
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);
    delete node;
    return record;
}

Rid IxIndexHandle::insert_record(const char *key, char* record, Context* context) {
    return insert_entry(key, record, context->txn_);
}

// rid, key, record, leaf_node
void IxIndexHandle::replay_insert_record(Rid rid, const char *key, char *record, Context *context) {
    // // 构造一个空事务
    auto txn_empty = std::make_unique<Transaction>(0);
    Rid insert_rid = insert_entry(key, record, txn_empty.get());
    if(rid != insert_rid) {
        std::cout << "Error: replay_rid is not equal to rid!\n";
    }
}

void IxIndexHandle::delete_record(const Rid& rid, Context* context) {
    IxNodeHandle* node = fetch_node(rid.page_no);
    char* record_slot = node->leaf_get_record_at(rid.slot_no);
    *(bool*)(record_slot + RECHDR_OFF_DELETE_MARK) = true;

    delete node;
    // make delete redo log
    // redo_log_manager->make_delete_log(rid, )
}

void IxIndexHandle::rollback_delete_record(const Rid& rid, Context* context) {
    IxNodeHandle* node = fetch_node(rid.page_no);
    char* record_slot = node->leaf_get_record_at(rid.slot_no);
    *(bool*)(record_slot + RECHDR_OFF_DELETE_MARK) = false;
    
    delete node;
}

void IxIndexHandle::update_record(const Rid& rid, char* raw_data, Context* context) {
    IxNodeHandle* node = fetch_node(rid.page_no);
    char* record_slot = node->leaf_get_record_at(rid.slot_no);
    // update record hdr and raw_data
    // memcpy(record_slot, raw_data, file_hdr_->record_len_);
    memcpy(record_slot + sizeof(RecordHdr), raw_data, file_hdr_->record_len_ - sizeof(RecordHdr));
    // make redo log
    // redo_log_manager_->make_update_log(rid, std::string(raw_data, file_hdr_->record_len_))
    // memcpy(record_slot + sizeof(RecordHdr), raw_data, file_hdr_->record_len_ - sizeof(RecordHdr));

    delete node;
}

/**
 * @brief Travel through inner nodes until find the leaf page which store the target key
 *
 * @return [leaf node] and [root_is_latched]
 * @note need to Unlatch and unpin the leaf node outside!
 * 注意：用了FindLeafPage之后一定要unlatch叶结点，否则下次latch该结点会堵塞！
 */
std::pair<IxNodeHandle *, bool> IxIndexHandle::find_leaf_page(const char *key, Operation operation,
                                                            Transaction *transaction, bool find_first) {
    // printf("Enter find_leaf_page\n");

    /**
     * TODO: 并发改成蟹形协议
    */
    // root latch是mutex类型的锁(互斥锁)，它只能由一个线程来获取(lock)
    // 如果其他线程已经获取占用，本线程就获取不到，本线程的该函数就会阻塞在这个位置，直到其他线程释放(unlcok)锁
    root_latch_.lock();  // 获取mutex锁
    bool root_is_latched = true;

    IxNodeHandle *node = fetch_node(file_hdr_->root_page_);  // 从buffer pool中读取该节点
    Page *page = node->page_;
    // std::cout << "find_leaf_page: " << "root_page.page_no: " << page->get_page_id().page_no << "\n";

    if (operation == Operation::FIND) {  // 读操作不需要对根结点上锁
        page->RLatch();
        root_is_latched = false;
        root_latch_.unlock();
    } else {
        page->WLatch();
        if (is_safe(node, operation)) {
            root_is_latched = false;
            root_latch_.unlock();
        }
    }

    // Travel through inner nodes
    while (!node->is_leaf_page()) {
        page = node->page_;
        page_id_t child_page_no;
        if (find_first == true) {
            child_page_no = node->internal_child_page_at(0);
        } else {
            child_page_no = node->internal_lookup(key);
            // std::cout << "child_page_no: " << child_page_no << "\n";
        }
        IxNodeHandle *child_node = fetch_node(child_page_no);  // pin the child node
        Page *child_page = child_node->page_;

        if (operation == Operation::FIND) {
            child_page->RLatch();
            page->RUnlatch();
            buffer_pool_manager_->unpin_page(page->get_page_id(), false);  // unpin old parent page
        } else {
            child_page->WLatch();
            transaction->append_index_latch_page_set(page);
            // child node is safe, release all locks on ancestors(NOT including child_page)
            if (is_safe(child_node, operation)) {
                if (root_is_latched) {
                    root_is_latched = false;
                    root_latch_.unlock();
                }
                unlock_unpin_pages(transaction);
            }
        }
        
        delete node;
        node = child_node;  // go to child node
    }

    // printf("Out find_leaf_page\n");
    return std::make_pair(node, root_is_latched);
}

/**
 * @brief  将传入的一个node拆分(split)成两个结点，会在node的右边生成一个新结点new node
 * 首先初始化新结点的parent id和max size，接下来注意分情况讨论node是叶结点还是内部结点
 * 如果node为internal page，则产生的新结点的孩子结点的父指针要更新为新结点
 * 如果node为leaf page，则产生的新结点要连接原结点，即更新这两个结点的next page id
 *
 * split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 *
 * @param node 需要拆分的结点
 * @return 拆分得到的new_node
 * @note need to unpin the new node outside
 * 注意：本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 */
IxNodeHandle *IxIndexHandle::split(IxNodeHandle *node) {
    // Allocate brother node
    IxNodeHandle *bro = create_node();  // node分裂产生的右兄弟结点
    // printf("split bro=%d\n", bro->get_page_no());

    // init brother node
    bro->page_hdr_->parent_ = node->get_parent_page_no();  // They have the same parent
    bro->page_hdr_->num_key_ = 0;
    bro->page_hdr_->num_records_ = 0;
    bro->page_hdr_->tot_num_records_ = 0;
    bro->page_hdr_->free_space_offset_ = 0;             // the free_space_offset starts from the UserRecord space's beginning
    bro->page_hdr_->first_deleted_offset_ = INVALID_OFFSET;
    bro->page_hdr_->is_leaf_ = node->is_leaf_page();  // Brother node is leaf only if current node is leaf
    bro->page_hdr_->prev_page_ = node->get_page_no();
    bro->page_hdr_->next_page_ = node->get_next_page();

    if(node->get_next_page() != IX_NO_PAGE) {
        IxNodeHandle *next = fetch_node(node->get_next_page());
        next->page_hdr_->prev_page_ = bro->get_page_no();
        buffer_pool_manager_->unpin_page(next->get_page_id(), true);
        delete next;
    }

    node->page_hdr_->next_page_ = bro->get_page_no();

    /**
     * TODO: the split of leaf page is different from the internal page
    */
    if(node->is_leaf_page()) {
        // split at middle position
        int split_idx = node->leaf_get_min_size();
        int num_transfer = node->leaf_get_tot_record_num() - split_idx;
        // insert [split_idx, tot_num_record) into bro_leaf (both record data and page directory)
        bro->leaf_insert_continous_records(0, node->records_, node->leaf_get_directory_entry_at(split_idx), num_transfer);
        assert(bro->leaf_get_tot_record_num() == num_transfer);

        // update current node's record num
        node->page_hdr_->tot_num_records_ = split_idx;
        // update current node's first_deleted_offset
        node->page_hdr_->first_deleted_offset_ = *(int32_t*)(node->leaf_get_directory_entry_at(split_idx) + file_hdr_->col_tot_len_);
    }
    else {
        // split at middle position
        // split_idx是左区间的num_key(长度)，左区间范围[0, split_idx)
        int split_idx = node->internal_get_min_size();
        // num_transfer是右区间的num_key(长度)，右区间范围[split_idx, num_key)
        int num_transfer = node->internal_get_key_num() - split_idx;

        // Keys in [0, split_idx) stay in current node, [split_idx, num_key) go to brother node
        // for example:
        // {0,1,2}即[0,3) num_key=3
        // split_idx=1 num_transfer=2 分为区间 [0,1) 和 [1,3)
        // split_idx + num_transfer = num_key 即为右区间的开端点
        bro->internal_insert_pairs(0, node->internal_key_at(split_idx), node->internal_value_at(split_idx), num_transfer);
        assert(bro->internal_get_key_num() == num_transfer);

        // update current node's key_num
        node->page_hdr_->num_key_ = split_idx;

        // Update children's parent 将bro的所有孩子结点的父节点置为bro
        for (int child_idx = 0; child_idx < bro->internal_get_key_num(); child_idx++) {
            maintain_child(bro, child_idx);
        }
    }

    return bro;
}

/**
 * @brief 拆分(split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>=maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 * 注意：本函数执行完毕后，new node和old node都需要在函数外面进行unpin
 * Insert key & value pair into internal page after split
 *
 * @param old_node input page from split() method
 * @param key 要插入parent的key
 * @param new_node returned page from split() method
 * @note User needs to first find the parent page of old_node, parent node must be adjusted to take info of new_node
 * into account. Remember to deal with split recursively if necessary.
 */
void IxIndexHandle::insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node,
                                     Transaction *transaction) {
    if (old_node->is_root_page()) {
        // If current page is root node, allocate new root
        IxNodeHandle *root = create_node();

        // init root node
        root->page_hdr_->parent_ = IX_NO_PAGE;
        root->page_hdr_->num_key_ = 0;
        root->page_hdr_->is_leaf_ = false;
        root->page_hdr_->num_records_ = 0;
        root->page_hdr_->tot_num_records_ = 0;
        root->page_hdr_->free_space_offset_ = INVALID_OFFSET;       // internal page does not use this variable
        root->page_hdr_->first_deleted_offset_ = INVALID_OFFSET;
        root->page_hdr_->prev_page_ = IX_NO_PAGE;
        root->page_hdr_->next_page_ = IX_NO_PAGE;

        // Insert current node's key & rid
        // Rid curr_rid = {.page_no = old_node->get_page_no(), .slot_no = -1};
        // Rid new_node_rid = {.page_no = new_node->get_page_no(), .slot_no = -1};
        page_id_t old_page_no = old_node->get_page_no();
        page_id_t new_page_no = new_node->get_page_no();

        // Populate new root
        // root->set_key(0, old_node->get_key(0));  // DEBUG （这样第一个键就不是无效了，配合main_parent()
        root->internal_set_key_at(0, old_node->get_key_at(0));
        // root->set_rid(0, curr_rid);
        root->internal_set_child_at(0, old_page_no);
        // root->set_key(1, key);
        root->internal_set_key_at(1, new_node->get_key_at(0));
        // root->set_rid(1, new_node_rid);
        root->internal_set_child_at(1, new_page_no);
        // root->set_size(2);
        root->page_hdr_->num_key_ = 2;

        // update parent
        old_node->set_parent_page_no(root->get_page_no());
        new_node->set_parent_page_no(root->get_page_no());

        // update global root page
        update_root_page_no(root->get_page_no());

        buffer_pool_manager_->unpin_page(root->get_page_id(), true);

        // 新的root必定不在transaction的page_set_队列中
        root_latch_.unlock();
        // unlock_pages(transaction);
        // TODO??
        unlock_unpin_pages(transaction);
        return;
    }

    IxNodeHandle *parent = fetch_node(old_node->get_parent_page_no());

    int child_idx = parent->internal_find_child(old_node); // child_idx是old_node在parent中的下标
    // Rid new_node_rid = {.page_no = new_node->get_page_no(), .slot_no = -1};
    page_id_t new_page_no = new_node->get_page_no();
    // Insert popup key into parent
    // 将(key,new_node->page_id)插入到父结点中 value==old_node->page_id 的下标之后
    parent->internal_insert_pairs(child_idx + 1, key, (char*)&new_page_no, 1);  // size+1

    // 父节点未满，结束递归
    if (parent->internal_get_key_num() < parent->internal_get_max_size()) {
        // unlock_pages(transaction);  // unlock除了叶子结点以外的所有上锁的祖先节点
        unlock_unpin_pages(transaction);
        buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
        return;
    }

    // 父节点已满，继续分裂，递归插入
    IxNodeHandle *new_parent = split(parent);
    insert_into_parent(parent, new_parent->internal_key_at(0), new_parent, transaction);

    buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
    buffer_pool_manager_->unpin_page(new_parent->get_page_id(), true);
    
    delete parent;
    delete new_parent;
}

/**
 * @brief
 *
 * @param key
 * @param value
 * @param transaction
 * @return page_id_t 插入到的叶结点的page_no
 */
Rid IxIndexHandle::insert_entry(const char* key, const char* record_value, Transaction *transaction) {
    // std::scoped_lock lock{root_latch_};
    if (is_empty()) {
        // LOG_WARN("Tree is empty when insert entry\n");
        // start_new_tree(key, value);
        // return -1;
    }

    // find the leaf page that the key-value has to be inserted
    auto [leaf_node, root_is_latched] = find_leaf_page(key, Operation::INSERT, transaction);
    Page *leaf_page = leaf_node->page_;

    // printf("insert_entry: find leaf node=%d\n", leaf_node->get_page_no());

    int origin_size = leaf_node->leaf_get_tot_record_num();

    int insert_index = leaf_node->leaf_insert_key_record(key, record_value);

    int new_size = leaf_node->leaf_get_tot_record_num();

    maintain_parent(leaf_node);  // NOTE THIS!

    // 不允许重复的key
    if (new_size == origin_size) {
        // printf("重复key=%d\n", *(int *)key);
        if (root_is_latched) {
            root_latch_.unlock();
        }
        unlock_unpin_pages(transaction);  // 此函数中会释放叶子的所有现在被锁住的祖先（不包括叶子）
        leaf_page->WUnlatch();
        buffer_pool_manager_->unpin_page(leaf_page->get_page_id(), false);  // unpin leaf page
        // return false;
        return Rid{.page_no = INVALID_PAGE_ID, .slot_no = -1, .record_no = -1};
    }

    if (new_size < leaf_node->leaf_get_max_size()) {
        int record_no = leaf_node->leaf_get_record_no_at(insert_index);
        if(root_is_latched)  {
            root_latch_.unlock();
        }
        unlock_unpin_pages(transaction);  // 此函数中会释放叶子的所有现在被锁住的祖先（不包括叶子）
        leaf_page->WUnlatch();
        buffer_pool_manager_->unpin_page(leaf_page->get_page_id(), true);  // unpin leaf page
        // return true;
        return Rid{.page_no = leaf_node->get_page_no(), .slot_no = insert_index, .record_no = record_no};
    }

    IxNodeHandle *new_leaf_node = split(leaf_node);  // pin new leaf node

    // printf("3 new_leaf_node=%d size=%d\n", new_leaf_node->get_page_no(), new_leaf_node->get_size());

    // Update global last_leaf if needed
    if (file_hdr_->last_leaf_ == leaf_node->get_page_no()) {
        file_hdr_->last_leaf_ = new_leaf_node->get_page_no();
    }

    insert_into_parent(leaf_node, new_leaf_node->leaf_get_directory_entry_at(0), new_leaf_node,
                     transaction);  // 此函数内将会W Unlatch除叶结点外的结点

    Rid rid;
    int lower_id = new_leaf_node->leaf_directory_lower_bound(key);
    int upper_id = new_leaf_node->leaf_directory_upper_bound(key);
    if(lower_id != upper_id) {
        rid.page_no = new_leaf_node->get_page_no();
        rid.slot_no = lower_id;
        rid.record_no = new_leaf_node->leaf_get_record_no_at(lower_id);
    }
    else {
        rid.page_no = leaf_node->get_page_no();
        rid.slot_no = insert_index;
        rid.record_no = leaf_node->leaf_get_record_no_at(insert_index);
    }

    // 必须unpin，InsertIntoParent函数里面并不会unpin old node和new node
    leaf_page->WUnlatch();
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), true);
    buffer_pool_manager_->unpin_page(new_leaf_node->get_page_id(), true);
    
    unlock_unpin_pages(transaction);
    
    delete new_leaf_node;

    // return true;
    return rid;
}

// 上层传入的key本来是int类型，通过(const char *)&key进行了转换
// 可用*(int *)key转换回去
/**
 * @brief find_leaf_page + lower_bound
 *
 * @param key
 * @return 返回第一个大于/等于目标元素的Rid
 */
Rid IxIndexHandle::lower_bound(const char *key) {
    // int int_key = *(int *)key;
    // printf("my_lower_bound key=%d\n", int_key);
    // std::cout << "lower_bound: key=" << *(int*)key << ", index's total page number: " << file_hdr_->num_pages_ << ", root_page: " << file_hdr_->root_page_ << "\n";

    IxNodeHandle *node = find_leaf_page(key, Operation::FIND, nullptr).first;
    int key_idx = node->leaf_directory_lower_bound(key);
    // int32_t offset = *(int32_t*)(node->leaf_get_directory_entry_at(key_idx) + file_hdr_->col_tot_len_);

    Rid iid;
    if(key_idx == node->leaf_get_tot_record_num()) {
        page_id_t next_page_id = node->get_next_page();
        if(next_page_id == -1) iid = leaf_end();
        else {
            IxNodeHandle* next_node = fetch_node(next_page_id);
            iid = {.page_no = next_page_id, .slot_no = 0, .record_no = next_node->leaf_get_record_no_at(0)};
            buffer_pool_manager_->unpin_page(next_node->get_page_id(), false);
            delete next_node;
        }
    }
    else {
        iid = {.page_no = node->get_page_no(), .slot_no = key_idx, .record_no = node->leaf_get_record_no_at(key_idx)};
    }

    // unlatch and unpin leaf node
    node->page_->RUnlatch();
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);
    delete node;

    return iid;
}

// 这里有点类似CMU中的FindLeafPageByOperation，其调用了internal page的Lookup函数，得到upper_bound的下标-1
// 但这里的upper_bound其实只是测试的时候用到了，也许可以改成私有函数
// 在insert_entry函数中用upper_bound来找key所在的叶子结点
/**
 * @brief find_leaf_page + upper_bound
 *
 * @param key
 * @return 返回第一个大于目标元素的Rid
 */
Rid IxIndexHandle::upper_bound(const char *key) {
    // int int_key = *(int *)key;
    // printf("my_upper_bound key=%d\n", int_key);

    IxNodeHandle *node = find_leaf_page(key, Operation::FIND, nullptr).first;
    int key_idx = node->leaf_directory_upper_bound(key);
    // int32_t offset = *(int32_t*)(node->leaf_get_directory_entry_at(key_idx) + file_hdr_->col_tot_len_);

    Rid iid;
    if (key_idx == node->leaf_get_tot_record_num()) {
        // 这种情况无法根据iid找到rid，即后续无法调用ih->get_rid(iid)
        // iid = leaf_end();
        page_id_t next_page_id = node->get_next_page();
        if(next_page_id == -1) iid = leaf_end();
        else {
            IxNodeHandle* next_node = fetch_node(next_page_id);
            iid = {.page_no = next_page_id, .slot_no = 0, .record_no = next_node->leaf_get_record_no_at(0)};
            buffer_pool_manager_->unpin_page(next_node->get_page_id(), false);
            delete next_node;
        }
    } else {
        iid = {.page_no = node->get_page_no(), .slot_no = key_idx, .record_no = node->leaf_get_record_no_at(key_idx)};
    }

    // unlatch and unpin leaf node
    node->page_->RUnlatch();
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);
    delete node;

    return iid;
}

Rid IxIndexHandle::leaf_end() const {
    IxNodeHandle *node = fetch_node(file_hdr_->last_leaf_);
    Rid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->leaf_get_tot_record_num(), .record_no = -1};
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);  // unpin it!
    // Rid iid = 
    delete node;

    return iid;
}

Rid IxIndexHandle::leaf_begin() const {
    Rid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0, .record_no = 0};
    return iid;
}

/** -- 以下为辅助函数 -- */
// pin the page, remember to unpin it outside!
IxNodeHandle *IxIndexHandle::fetch_node(int page_no) const {
    // assert(page_no < file_hdr_->num_pages); // 不再生效，由于删除操作，page_no可以大于个数
    // Page *page = buffer_pool_manager_->fetch_page(fd_, page_no);
    /**
     * TODO: 需要把InternalPage固定在buffer中
    */
    Page *page = buffer_pool_manager_->fetch_page(PageId{table_meta_.table_id_, page_no});
    // IxNodeHandle node(&file_hdr_, page);
    IxNodeHandle *node = new IxNodeHandle(file_hdr_, page);
    // auto node = std::make_unique<IxNodeHandle>(&file_hdr_, page);
    return node;
}

IxNodeHandle *IxIndexHandle::create_node() {
    IxNodeHandle *node;
    file_hdr_->num_pages_++;

    PageId new_page_id = {.table_id = table_meta_.table_id_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    // printf("IxIndexHandle::create_node_handle file_hdr_.num_pages=%d\n", file_hdr_.num_pages);
    // assert(new_page_id.page_no == file_hdr_.num_pages - 1);
    // file_hdr_.num_pages = new_page_id.page_no + 1;
    // 注意，和Record的free_page定义不同，此处【不能】加上：file_hdr_.first_free_page_no = page->get_page_id().page_no
    node = new IxNodeHandle(file_hdr_, page);
    // std::cout << "NewNode's page_no: " << page->get_page_id().page_no << "\n";
    return node;
}

// 这里从node开始更新其父节点的第一个key，一直向上更新直到根节点
void IxIndexHandle::maintain_parent(IxNodeHandle *node) {
    IxNodeHandle *curr = node;
    while (curr->get_parent_page_no() != IX_NO_PAGE) {
        // Load its parent
        IxNodeHandle *parent = fetch_node(curr->get_parent_page_no());
        int rank = parent->internal_find_child(curr);
        char *parent_key = parent->internal_key_at(rank);
        // char *child_max_key = curr.get_key(curr.page_hdr->num_key - 1);
        char *child_first_key = curr->get_key_at(0);
        if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0) {
            buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
            delete parent;
            break;
        }
        memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_);  // 修改了parent node
        curr = parent;

        buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
        delete parent;
    }
}

void IxIndexHandle::release_node_handle(IxNodeHandle &node) {
    file_hdr_->num_pages_--;
    // node.page_hdr->next_free_page_no = file_hdr_->first_free_page_no;
    // file_hdr_->first_free_page_no = node.page->GetPageId().page_no;
}

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 */
void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx) {
    if (!node->is_leaf_page()) {
        //  Current node is inner node, load its child and set its parent to current node
        int child_page_no = node->internal_child_page_at(child_idx);
        IxNodeHandle *child = fetch_node(child_page_no);
        child->set_parent_page_no(node->get_page_no());
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
        delete child;
    }
}

/**
 * @brief unlock all pages in transaction PageSet
 */
void IxIndexHandle::unlock_pages(Transaction *transaction) {
    if (transaction == nullptr) {
        // printf("unlock_pages transaction == nullptr\n");
        return;
    }
    // unlock 和 unpin 事务经过的所有parent page
    for (Page *page : *transaction->get_index_latch_page_set()) {  // 前面加*是因为page set是shared_ptr类型
        page->WUnlatch();
    }
    transaction->get_index_latch_page_set()->clear();  // 清空page set
}

/**
 * @brief unlock and unpin all pages in transaction PageSet
 */
void IxIndexHandle::unlock_unpin_pages(Transaction *transaction) {
    if (transaction == nullptr) {
        return;
    }    
    
    // unlock 和 unpin 事务经过的所有parent page
    for(Page *page : *transaction->get_index_latch_page_set()) {  // 前面加*是因为page set是shared_ptr类型
        page->WUnlatch();
        buffer_pool_manager_->unpin_page(page->get_page_id(), false);  // 此处dirty为false
        // 此函数只在向下find leaf page时使用，向上进行修改时是手动一步步unpin true，这里是一次性unpin
    }
    transaction->get_index_latch_page_set()->clear();  // 清空page set
}

/**
 * @brief check safe node
 *
 * @param node
 * @param op
 * @return true
 * @return false
 */
bool IxIndexHandle::is_safe(IxNodeHandle *node, Operation op) {
    if (node->is_root_page()) {
        return (op == Operation::INSERT && node->get_size() + 1 < node->get_max_size()) ||
               (op == Operation::DELETE && node->get_size() - 1 >= 2);
        // 根结点的min_size=2，其余结点的min_size=max_size/2
        // size - 1 >= 2 即 size >= 3 即 size > 2
    }

    if (op == Operation::INSERT) {
        // 注意此处的逻辑：
        // 任何结点在稳定状态size<max size，如果要插入一个键值对后还能保持<max size，那就是size + 1 < max size
        return node->get_size() + 1 < node->get_max_size();
    }

    if (op == Operation::DELETE) {
        // 注意此处逻辑需要和coalesce函数的删除一个键值对之后结点的size对应（删除后结点最小能等于min size）
        // 任何结点在稳定状态size>=min size，如果要删除一个键值对后还能保持>=min size，那就是size - 1 >= min size
        return node->get_size() - 1 >= node->get_min_size();
    }

    return true;
}

// bool IxIndexHandle::get_value(const char *key, std::vector<Rid> *result, Transaction *transaction) {
//     // std::scoped_lock lock{root_latch_};  // DEBUG

//     // 1 先找到leaf page，这里面会调用fetch page
//     IxNodeHandle *leaf_node = find_leaf_page(key, Operation::FIND, transaction).first;
//     Page *leaf_page = leaf_node->page;

//     // 2 在leaf page里找这个key
//     Rid *value;
//     bool exist = leaf_node->leaf_lookup(key, &value);

//     // 3 page用完后记得unpin page
//     leaf_page->RUnlatch();
//     buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);  // unpin leaf page

//     if (!exist) {
//         return false;
//     }

//     result->push_back(*value);
//     return true;
// }

/**
 * @brief only used in get_value()
 *
 * @note need to Unlatch and unpin the leaf node outside!
 */
// IxNodeHandle *IxIndexHandle::find_leaf_page(const char *key, Transaction *transaction) {
//     return FindLeafPageByOperation(key, Operation::FIND, transaction).first;
// }

// // TODO!!! 删掉value这个在CMU的删除逻辑中无意义的参数（只要删除一个key及其对应value）
// /**
//  * @brief
//  *
//  * @param key
//  * @param value
//  * @param transaction
//  */
// void IxIndexHandle::delete_entry(const char *key, const Rid &value, Transaction *transaction) {
//     // 这里初始化的时候root page是等于2的，只有把root page也删掉，才认为它为空？
//     if (is_empty()) {
//         return;
//     }

//     // printf("delete_entry: delete key=%d\n", *(int *)key);

//     auto [leaf_node, root_is_latched] = find_leaf_page(key, Operation::DELETE, transaction);

//     Page *leaf_page = leaf_node->page;
//     int old_size = leaf_node->get_size();
//     int new_size = leaf_node->remove(key);  // 在leaf中删除key和value（如果不存在该key，则size不变）

//     maintain_parent(leaf_node);  // NOTE THIS!

//     // 1 删除失败
//     if (new_size == old_size) {
//         if (root_is_latched) {
//             root_latch_.unlock();
//         }
//         unlock_unpin_pages(transaction);

//         leaf_page->WUnlatch();
//         buffer_pool_manager_->unpin_page(leaf_page->get_page_id(), false);  // unpin leaf page

//         return;
//     }

//     // 2 删除成功，然后调用CoalesceOrRedistribute
//     bool *pointer_root_is_latched = new bool(root_is_latched);

//     bool leaf_should_delete = coalesce_or_redistribute(leaf_node, transaction, pointer_root_is_latched);
//     // NOTE: unlock and unpin are finished in coalesce_or_redistribute()
//     // NOTE: root node must be unlocked in coalesce_or_redistribute()
//     assert((*pointer_root_is_latched) == false);

//     delete pointer_root_is_latched;

//     leaf_page->WUnlatch();
//     buffer_pool_manager_->unpin_page(leaf_page->get_page_id(), true);  // unpin leaf page

//     // assert(transaction->get_deleted_page_set() != nullptr);

//     if (leaf_should_delete) {
//         transaction->append_index_deleted_page(leaf_page);
//     }

//     // NOTE: ensure deleted pages have been unpined
//     // 删除并清空deleted page set
//     if (transaction != nullptr) {
//         for (Page *page : *transaction->get_index_deleted_page_set()) {
//             buffer_pool_manager_->delete_page(page->get_page_id());
//         }
//         transaction->get_index_deleted_page_set()->clear();
//     }
//     // printf("\n");
// }

// /**
//  * @brief User needs to first find the sibling of input page. If sibling's size + input
//  * page's size >= 2 * page's minsize, then redistribute. Otherwise, merge(coalesce).
//  *
//  * @return true means target leaf page(or its sibling) should be deleted, false means no deletion happens
//  */
// bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction, bool *root_is_latched) {
//     if (node->is_root_page()) {
//         bool root_should_delete = adjust_root(node);

//         // 疑问：此处必须要有？
//         if (*root_is_latched) {
//             *root_is_latched = false;
//             root_latch_.unlock();
//         }

//         unlock_pages(transaction);

//         return root_should_delete;  // NOTE: size of root page can be less than min size
//     }

//     // 不需要合并或者重分配，直接返回false
//     if (node->get_size() >= node->get_min_size()) {
//         // 注意：此处不用root_latch_.unlock()
//         unlock_pages(transaction);
//         return false;
//     }

//     // 需要合并或者重分配
//     // 先获取node的parent page
//     IxNodeHandle *parent = fetch_node(node->get_parent_page_no());
//     Page *parent_page = parent->page;
//     // printf("parent=%d\n", parent->get_page_no());

//     // 获得node在parent的孩子指针(value)的index
//     int index = parent->find_child(node);
//     // 寻找兄弟结点，尽量找到前一个结点(前驱结点)
//     page_id_t sibling_page_no = parent->value_at(index == 0 ? 1 : index - 1);
//     IxNodeHandle *sibling_node = fetch_node(sibling_page_no);
//     Page *sibling_page = sibling_node->page;

//     sibling_page->WLatch();  // 记得要锁住兄弟结点

//     // 1 redistribute 当kv总和能支撑两个Node，那么重新分配即可，不必删除node
//     if (node->get_size() + sibling_node->get_size() >= node->get_min_size() * 2) {
//         if (*root_is_latched) {
//             *root_is_latched = false;
//             root_latch_.unlock();
//         }

//         // printf("redistribute: node=%d sibling_node=%d\n", node->get_page_no(), sibling_node->get_page_no());
//         redistribute(sibling_node, node, parent, index);  // 无返回值
//         assert(sibling_node->get_size() >= sibling_node->get_min_size() && node->get_size() >= node->get_min_size());

//         // printf("after node=%d redistribute:\n", node->get_page_no());
//         // for (int i = 0; i < node->get_size(); i++) {
//         //     printf("i=%d node->key(i)=%d\n", i, node->key_at(i));
//         // }
//         // for (int i = 0; i < sibling_node->get_size(); i++) {
//         //     printf("i=%d sibling_node->key(i)=%d\n", i, sibling_node->key_at(i));
//         // }
//         // for (int i = 0; i < parent->get_size(); i++) {
//         //     printf("i=%d parent->key(i)=%d\n", i, parent->key_at(i));
//         // }
//         // printf("\n");

//         unlock_pages(transaction);
//         buffer_pool_manager_->unpin_page(parent_page->get_page_id(), true);

//         sibling_page->WUnlatch();
//         buffer_pool_manager_->unpin_page(sibling_page->get_page_id(), true);

//         return false;  // node不必被删除
//     }

//     // 2 coalesce 当sibling和node只能凑成一个Node，那么合并两个结点，将右边的结点合并到左边的结点
//     // Coalesce函数继续递归调用CoalesceOrRedistribute
//     // printf("coalesce: node=%d sibling_node=%d\n", node->get_page_no(), sibling_node->get_page_no());
//     bool parent_should_delete =
//         coalesce(&sibling_node, &node, &parent, index, transaction, root_is_latched);  // 返回parent是否需要被删除
//     // 若index=0，则node在左边，所以要把右边的sibling_node合并到左边的node，将会删除sibling_node
//     // 若index>0，则node在右边，所以要把右边的node合并到左边的sibling_node，将会删除node
//     assert((index == 0 && sibling_node->get_size() == 0) || (index > 0 && node->get_size() == 0));
//     // printf("after node=%d coalesce:\n", node->get_page_no());
//     // for (int i = 0; i < node->get_size(); i++) {
//     //     printf("i=%d node->key(i)=%d\n", i, node->key_at(i));
//     // }
//     // for (int i = 0; i < sibling_node->get_size(); i++) {
//     //     printf("i=%d sibling_node->key(i)=%d\n", i, sibling_node->key_at(i));
//     // }
//     // for (int i = 0; i < parent->get_size(); i++) {
//     //     printf("i=%d parent->key(i)=%d\n", i, parent->key_at(i));
//     // }
//     // printf("\n");

//     assert((*root_is_latched) == false);

//     if (parent_should_delete) {
//         transaction->append_index_deleted_page(parent_page);
//     }

//     // NOTE: parent unlock is finished in coalesce
//     buffer_pool_manager_->unpin_page(parent_page->get_page_id(), true);

//     sibling_page->WUnlatch();
//     buffer_pool_manager_->unpin_page(sibling_page->get_page_id(), true);

//     // printf("coalesce_or_redistribute out!!\n");

//     return true;  // node需要被删除
// }

// /**
//  * @brief Update root page if necessary
//  *
//  * @note size of root page can be less than min size and this method is only called within coalesceOrRedistribute()
//  * method
//  * case 1: when you delete the last element in root page, but root page still as one last child
//  * case 2: when you delete the last element in whole b+ tree
//  * @return true means root page should be deleted, false means no deletion happend
//  */
// bool IxIndexHandle::adjust_root(IxNodeHandle *old_root_node) {
//     // Case 1: old_root_node是内部结点，且大小为1。表示内部结点其实已经没有key了，所以要把它的孩子更新成新的根结点
//     // old_root_node (internal node) has only one size
//     if (!old_root_node->is_leaf_page() && old_root_node->get_size() == 1) {
//         // get child page as new root page
//         IxNodeHandle *internal_node = old_root_node;
//         page_id_t child_page_no = internal_node->remove_and_return_only_child();

//         // NOTE: don't need to unpin old_root_node, this operation will be done in coalesce_or_redistribute function

//         // update root page
//         update_root_page_no(child_page_no);

//         // update parent page of new root node
//         IxNodeHandle *new_root_node = fetch_node(file_hdr_->root_page_);
//         Page *new_root_page = new_root_node->page;
//         new_root_node->set_parent_page_no(IX_NO_PAGE);

//         buffer_pool_manager_->unpin_page(new_root_page->get_page_id(), true);

//         // NOTE THIS!
//         release_node_handle(*old_root_node);  // DEBUG

//         return true;
//     }
//     // 注意：这种case 2的情况，redbase没有考虑（只考虑了case 1），因为redbase不让产生空树
//     // Case 2: old_root_node是叶结点，且大小为0。直接更新root page
//     // all elements deleted from the B+ tree
//     if (old_root_node->is_leaf_page() && old_root_node->get_size() == 0) {
//         // NOTE: don't need to unpin old_root_node, this operation will be done in Remove function
//         // assert(false);  // 测试时，暂时禁止空树的情况

//         // update root page
//         // update_root_page_no(INVALID_PAGE_ID);

//         // NOTE THIS!
//         // release_node_handle(*old_root_node);  // DEBUG
//         // file_hdr_->num_pages++;

//         // return true;
//         return false;
//     }
//     // 否则不需要有page被删除，直接返回false
//     return false;
// }

// /**
//  * @brief redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
//  * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
//  *
//  * @param neighbor_node sibling page of input "node"
//  * @param node input from method coalesceOrRedistribute()
//  * @param parent the parent of "node" and "neighbor_node"
//  * @param index node在parent中的rid_idx
//  * @note node是之前刚被删除过一个key的结点
//  * index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
//  * index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
//  * 注意更新parent结点的相关kv对
//  */
// void IxIndexHandle::redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
//     if (index > 0) {
//         // case 1: neighbor_node(left) and node(right)

//         // 1.1 set node's first key to parent's index key
//         // 注意这里只处理内部结点，因为内部结点的第一个key是无效的（叶子结点第一个key有效）
//         // 注意：这里必须要写
//         if (!node->is_leaf_page()) {
//             node->set_key(0, parent->get_key(index));
//         }

//         // 1.2 move neighbor's last to node's front
//         node->insert_pair(0, neighbor_node->get_key(neighbor_node->get_size() - 1),
//                           *neighbor_node->get_rid(neighbor_node->get_size() - 1));
//         neighbor_node->erase_pair(neighbor_node->get_size() - 1);

//         // 1.3 set parent's index key to node's "new" first key
//         parent->set_key(index, node->get_key(0));

//         // 1.4 maintain child when node is internal page
//         maintain_child(node, 0);
//     } else if (index == 0) {
//         // case 2: node(left) and neighbor_node(right)

//         // 2.1 set neighbor's first key to parent's second key
//         // 疑问：这里是不是可以不写？？
//         if (!node->is_leaf_page()) {
//             // assert(neighbor_node->get_key(0) == parent->get_key(1));  // DEBUG：这样就能测试要不要写了
//             neighbor_node->set_key(0, parent->get_key(1));
//         }

//         // 2.2 move neighbor's first to node's end
//         node->insert_pair(node->get_size(), neighbor_node->get_key(0), *neighbor_node->get_rid(0));
//         neighbor_node->erase_pair(0);

//         // 2.3 set parent's second key to neighbor's "new" first key
//         parent->set_key(1, neighbor_node->get_key(0));

//         // 2.4 maintain child when node is internal page
//         maintain_child(node, node->get_size() - 1);
//     }
//     buffer_pool_manager_->unpin_page(parent->page->get_page_id(), true);
// }

// /**
//  * @brief 合并(coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
//  * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
//  * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
//  * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
//  * recursively if necessary.
//  *
//  * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
//  * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
//  * @param parent parent page of input "node"
//  * @param index node在parent中的rid_idx
//  * @return true means parent node should be deleted, false means no deletion happend
//  * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
//  */
// bool IxIndexHandle::coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
//                              Transaction *transaction, bool *root_is_latched) {
//     // key_index表示 交换后的 node在parent中的rid_idx
//     // 若index=0，说明node为neighbor前驱，要保证neighbor为node的前驱，则交换变量neighbor和node，且key_index=1
//     // printf("coalesce index=%d\n", index);
//     int key_index = index;
//     if (index == 0) {
//         // 保证neighbor_node为node的前驱
//         std::swap(neighbor_node, node);  // 注意这里不要写成**node，否则调用此函数的传入参数**node也变了
//         key_index = 1;
//     }

//     // Maintain leaf list
//     if ((*node)->is_leaf_page()) {
//         erase_leaf(*node);
//     }
//     // Update global last leaf
//     if (file_hdr_->last_leaf_ == (*node)->get_page_no()) {
//         file_hdr_->last_leaf_ = (*neighbor_node)->get_page_no();
//     }

//     // 将当前node的第0个key 赋值为 父结点中下标为key_index的key
//     // 这里应该是必须要写，因为相当于删除了第0个key
//     // printf("node=%d (*parent)->get_key(key_index)=%d\n", (*node)->get_page_no(), *(int
//     // *)(*parent)->get_key(key_index));
//     if (!(*node)->is_leaf_page()) {
//         (*node)->set_key(0, (*parent)->get_key(key_index));
//     }

//     // Move all pairs from node to neighbor_node
//     int node_size = (*node)->get_size();
//     for (int i = 0; i < node_size; i++) {
//         (*neighbor_node)->insert_pair((*neighbor_node)->get_size(), (*node)->get_key(0), *(*node)->get_rid(0));
//         maintain_child(*neighbor_node, (*neighbor_node)->get_size() - 1);
//         (*node)->erase_pair(0);
//     }

//     // Free node
//     release_node_handle(**node);

//     // 删除node在parent中的kv信息
//     (*parent)->erase_pair(key_index);  // 注意，是key_index，不是index

//     // 因为parent中删除了kv对，所以递归调用CoalesceOrRedistribute函数判断parent结点是否需要被删除
//     return coalesce_or_redistribute(*parent, transaction, root_is_latched);
// }

// 注意：不能把iid的作用删掉！iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
// 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
// node其实就是把slot_no作为键值对数组的下标。换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
// Rid IxIndexHandle::get_rid(const Iid &iid) const {
//     IxNodeHandle *node = fetch_node(iid.page_no);
//     if (iid.slot_no >= node->get_size()) {
//         throw IndexEntryNotFoundError();
//     }
//     buffer_pool_manager_->unpin_page(node->get_page_id(), false);  // unpin it!
//     return *node->get_rid(iid.slot_no);
// }



