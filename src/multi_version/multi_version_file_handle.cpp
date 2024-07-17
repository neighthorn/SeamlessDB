#include "multi_version_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<MultiVersionRecord>} rid对应的记录对象指针
 */
std::unique_ptr<MultiVersionRecord> MultiVersionFileHandle::get_record(const Rid& rid, Context* context) const {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向MultiVersionRecord的指针（赋值其内部的data和size）

    // Don't need lock
    // if(context != nullptr)
    //     context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);

    auto record = std::make_unique<MultiVersionRecord>(file_hdr_.record_size);
    MultiVersionPageHandle page_handle = fetch_page_handle(rid.page_no);
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    char *slot = page_handle.get_slot(rid.slot_no);  // record对应的地址
    // copy record into slot 把位于slot的record拷贝一份到当前的record
    memcpy(record->data, slot, file_hdr_.record_size);
    record->size = file_hdr_.record_size;

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid MultiVersionFileHandle::insert_record(char* buf, Context* context) {
    // Todo:
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no

    MultiVersionPageHandle page_handle = create_page_handle();  // 调用辅助函数获取当前可用(未满)的page handle
    // get slot number 找page_handle.bitmap中第一个为0的位
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);
    assert(slot_no < file_hdr_.num_records_per_page);
    Rid rid{.page_no = page_handle.page->get_page_id().page_no, .slot_no = slot_no};

    // don't need lock
    // if(context != nullptr)
    //     context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);

    // update bitmap 将此位置1
    Bitmap::set(page_handle.bitmap, slot_no);
    // update page header
    page_handle.page_hdr->num_records++;  // NOTE THIS
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        // page is full
        // 当前page handle中的page插入后已满，那么更新文件中第一个可用的page_no
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    }
    // copy record data into slot
    char *slot = page_handle.get_slot(slot_no);
    memcpy(slot, buf, file_hdr_.record_size);

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);

    return rid;
}


/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void MultiVersionFileHandle::delete_record(const Rid& rid, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()

    // don't need lock
    // if(context != nullptr)
    //     context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);

    MultiVersionPageHandle page_handle = fetch_page_handle(rid.page_no);  // 调用辅助函数获取指定page handle
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        // originally full, now available for new record
        // 当前page handle中的page已满，但要进行删除，那么page handle状态从已满更新为未满
        release_page_handle(page_handle);  // 调用辅助函数释放page handle
    }
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records--;  // NOTE THIS

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}


// /**
//  * @description: 更新记录文件中记录号为rid的记录
//  * @param {Rid&} rid 要更新的记录的记录号（位置）
//  * @param {char*} buf 新记录的数据
//  * @param {Context*} context
//  */
// void MultiVersionFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
//     // Todo:
//     // 1. 获取指定记录所在的page handle
//     // 2. 更新记录

//     if(context != nullptr)
//         context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);

//     auto rec = get_record(rid, context);
//     MultiVersionRecord update_record{rec->size};
//     memcpy(update_record.data, rec->data, rec->size);

//     MultiVersionPageHandle page_handle = fetch_page_handle(rid.page_no);
//     if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
//         throw RecordNotFoundError(rid.page_no, rid.slot_no);
//     }
//     char *slot = page_handle.get_slot(rid.slot_no);
//     memcpy(slot, buf, file_hdr_.record_size);

//     buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
//     // BufferPoolManager::mark_dirty(page_handle.page);

//     // record a update operation into the transaction
//     // WriteRecord * write_record = new WriteRecord(WType::UPDATE_TUPLE, this, rid, update_record);
//     // context->txn_->append_write_record(write_record);
// }

/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {MultiVersionPageHandle} 指定页面的句柄
 */
MultiVersionPageHandle MultiVersionFileHandle::fetch_page_handle(int page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception

    // assert(page_no >= 0 && page_no < file_hdr_.num_pages);
    // Page *page = buffer_pool_manager_->fetch_page(fd_, page_no);  // bpm->fetch_page
    // printf("MultiVersionFileHandle::fetch_page fd=%d page_no=%d\n", fd_, page_no);
    assert(page_no >= 0);
    // TODO: throw exception
    if (page_no >= file_hdr_.num_pages) throw PageNotExistError(disk_manager_->get_file_name(fd_), page_no);
    Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});  // bpm->fetch_page
    MultiVersionPageHandle page_handle(&file_hdr_, page);
    return page_handle;
}

/**
 * @description: 创建一个新的页面并返回该页面的句柄
 * @return {MultiVersionPageHandle} 新页面的句柄
 */
MultiVersionPageHandle MultiVersionFileHandle::create_new_page_handle() {
    // Todo:
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_

    // Page *page = buffer_pool_manager_->create_page(fd_, file_hdr_.num_pages);  // bpm->create_page
    PageId new_page_id = {.table_id = fd_, .page_no = INVALID_PAGE_ID};
    Page *page = buffer_pool_manager_->new_page(&new_page_id);  // 此处NewPage会调用disk_manager的AllocatePage()
    // printf("MultiVersionFileHandle::create_page_handle fd=%d page_no=%d\n", fd_, new_page_id.page_no);
    assert(new_page_id.page_no != INVALID_PAGE_ID && new_page_id.page_no != MULTI_FILE_HDR_PAGE);

    // Init page handle
    MultiVersionPageHandle page_handle = MultiVersionPageHandle(&file_hdr_, page);
    page_handle.page_hdr->num_records = 0;
    // 这个page handle中的page满了之后，下一个可用的page_no=-1（即没有下一个可用的了）
    page_handle.page_hdr->next_free_page_no = MULTI_NO_PAGE;
    Bitmap::init(page_handle.bitmap, file_hdr_.bitmap_size);

    // Update file header
    // file_hdr_.num_pages++;
    file_hdr_.num_pages = new_page_id.page_no + 1;
    file_hdr_.first_free_page_no = page->get_page_id().page_no;  // 更新文件中当前第一个可用的page_no
    return page_handle;
}

/**
 * @description: 找到一个有空闲空间的页面，返回该页面的句柄
 * @return {MultiVersionPageHandle} 有空闲空间页面的句柄
 */
MultiVersionPageHandle MultiVersionFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层

    if (file_hdr_.first_free_page_no == MULTI_NO_PAGE) {
        // No free pages. Need to allocate a new page.
        // 最开始num_pages=1，这里实际上是从磁盘文件中的第1页开始创建（第0页用来存file header）
        return create_new_page_handle();
    } else {
        // Fetch the first free page.
        MultiVersionPageHandle page_handle = fetch_page_handle(file_hdr_.first_free_page_no);
        return page_handle;
    }
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void MultiVersionFileHandle::release_page_handle(MultiVersionPageHandle&page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no

    // page handle下一个可用的page_no 更新为 文件中当前第一个可用的page_no
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    // 文件中当前第一个可用的page_no 更新为 page handle的page_no
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
}

// /**
//  * @description: 在当前表中的指定位置插入一条记录
//  * @param {Rid&} rid 要插入记录的位置
//  * @param {char*} buf 要插入记录的数据
//  */
// void MultiVersionFileHandle::insert_record(const Rid& rid, char* buf) {
//     // 这个时候锁还没有释放因此不需要重新加锁
//     MultiVersionPageHandle page_handle = fetch_page_handle(rid.page_no);
//     assert(Bitmap::is_set(page_handle.bitmap, rid.slot_no) == false);
//     Bitmap::set(page_handle.bitmap, rid.slot_no);
//     page_handle.page_hdr->num_records ++;
//     if(page_handle.page_hdr ->num_records == file_hdr_.num_records_per_page) {
//         file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
//     }
//     char* slot = page_handle.get_slot(rid.slot_no);
//     memcpy(slot, buf, file_hdr_.record_size);
//     buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
// }