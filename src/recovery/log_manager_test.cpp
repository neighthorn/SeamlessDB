#include "gtest/gtest.h"
#include "log_manager.h"

const std::string TEST_DB_NAME = "LogMangerTestDB";

class LogManagerTest: public ::testing::Test {
public:
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<DiskManager> disk_manager_;

    void SetUp() override {
        ::testing::Test::SetUp();
        disk_manager_ = std::make_unique<DiskManager>();
        log_manager_ = std::make_unique<LogManager>(disk_manager_.get());
        disk_manager_->create_file(LOG_FILE_NAME);
        disk_manager_->SetLogFd(disk_manager_->open_file(LOG_FILE_NAME));
    }

    void TearDown() override {
        disk_manager_->close_file(disk_manager_->GetLogFd());
        disk_manager_->destroy_file(LOG_FILE_NAME);
    }
};

TEST_F(LogManagerTest, TxnLogSerializeTest) {
    BeginLogRecord* begin_record = new BeginLogRecord();
    begin_record->format_print();
    EXPECT_EQ(begin_record->log_type_, LogType::begin);

    CommitLogRecord* commit_record = new CommitLogRecord();
    AbortLogRecord* abort_record = new AbortLogRecord();

    // char* data = "test data";
    // RmRecord record(10, data);
    // InsertLogRecord* insert_record = new InsertLogRecord(0, record);
    // insert_record->format_print();
    // EXPECT_EQ(insert_record->log_type_, LogType::INSERT);

    log_manager_->add_log_to_buffer(begin_record);
    // log_manager_->add_log_to_buffer(insert_record);
    log_manager_->add_log_to_buffer(commit_record);
    log_manager_->add_log_to_buffer(abort_record);

    LogBuffer* buffer = log_manager_->get_log_buffer();
    EXPECT_EQ(buffer->offset_, begin_record->log_tot_len_ + commit_record->log_tot_len_ + abort_record->log_tot_len_);

    int offset = OFFSET_LOG_TYPE;
    EXPECT_EQ((LogType)*(buffer->buffer_ + offset), LogType::begin);
    offset += begin_record->log_tot_len_;
    EXPECT_EQ((LogType)*(buffer->buffer_ + offset), LogType::commit);
    offset += commit_record->log_tot_len_;
    EXPECT_EQ((LogType)*(buffer->buffer_ + offset), LogType::ABORT);
}

TEST_F(LogManagerTest, InsertLogSerializeTest) {
    char* data = "insert data";
    RmRecord record(12, data);
    Rid rid{1,1};
    std::string table_name = "insert_table";
    txn_id_t txn_id = 1;
    InsertLogRecord* insert_log = new InsertLogRecord(txn_id, record, rid, table_name);
    insert_log->format_print();
    EXPECT_EQ(insert_log->log_type_, LogType::INSERT);

    log_manager_->add_log_to_buffer(insert_log);

    LogBuffer* buffer = log_manager_->get_log_buffer();
    EXPECT_EQ(buffer->offset_, insert_log->log_tot_len_);

    int offset = 0;
    EXPECT_EQ((LogType)*(buffer->buffer_ + offset), LogType::INSERT);
    offset = OFFSET_LOG_DATA;
    EXPECT_EQ((int)*(buffer->buffer_ + offset), record.size);

    InsertLogRecord* new_log = new InsertLogRecord();
    new_log->deserialize(buffer->buffer_);
    new_log->format_print();
    EXPECT_EQ(insert_log->log_tid_, new_log->log_tid_);
    EXPECT_EQ(insert_log->table_name_size_, new_log->table_name_size_);
}

TEST_F(LogManagerTest, DeleteLogSerializeTest) {
    char* data = "delete data";
    RmRecord record(12, data);
    Rid rid{1,1};
    std::string table_name = "delete_table";
    txn_id_t txn_id = 1;
    DeleteLogRecord* delete_log = new DeleteLogRecord(txn_id, record, rid, table_name);
    delete_log->format_print();

    log_manager_->add_log_to_buffer(delete_log);

    LogBuffer* buffer = log_manager_->get_log_buffer();
    EXPECT_EQ(buffer->offset_, delete_log->log_tot_len_);

    DeleteLogRecord* new_log = new DeleteLogRecord();
    new_log->deserialize(buffer->buffer_);
    new_log->format_print();
}

TEST_F(LogManagerTest, UpdateLogSerializeTest) {
    char* old_data = "old data";
    RmRecord old_record(9, old_data);
    char* new_data = "new data";
    RmRecord new_record(9, new_data);
    Rid rid{1,1};
    std::string table_name = "update_table";
    txn_id_t txn_id = 1;
    UpdateLogRecord* update_log = new UpdateLogRecord(txn_id, old_record, new_record, rid, table_name);
    update_log->format_print();

    log_manager_->add_log_to_buffer(update_log);

    printf("finish add to buffer\n");

    LogBuffer* buffer = log_manager_->get_log_buffer();
    printf("buffer offset: %d\n", buffer->offset_);
    EXPECT_EQ(buffer->offset_, update_log->log_tot_len_);

    UpdateLogRecord* new_log = new UpdateLogRecord();
    new_log->deserialize(buffer->buffer_);
    new_log->format_print();
}

TEST_F(LogManagerTest, DeserializeTest) {
    // BeginLogRecord* begin_record = new BeginLogRecord();
    // begin_record->format_print();
    // EXPECT_EQ(begin_record->log_type_, LogType::begin);

    // char* data = "test data";
    // RmRecord record(10, data);
    // InsertLogRecord* insert_record = new InsertLogRecord(0, record);
    // insert_record->format_print();
    // EXPECT_EQ(insert_record->log_type_, LogType::INSERT);

    // log_manager_->add_log_to_buffer(begin_record);
    // log_manager_->add_log_to_buffer(insert_record);

    // log_manager_->print_buffer();

    // LogBuffer* log_buffer = log_manager_->get_log_buffer();
    // BeginLogRecord* new_begin_log = new BeginLogRecord();
    // InsertLogRecord* new_insert_log = new InsertLogRecord();
    
    // printf("deserialize records: \n");
    // new_insert_log->format_print();
    // int offset = 0;
    // new_begin_log->deserialize(log_buffer->buffer_ + offset);
    // offset += new_begin_log->log_tot_len_;
    // new_insert_log->deserialize(log_buffer->buffer_ + offset);
    // new_begin_log->format_print();
    // new_insert_log->format_print();
}

TEST_F(LogManagerTest, FlushToDiskTest) {
    BeginLogRecord* begin_record = new BeginLogRecord();
    begin_record->format_print();
    EXPECT_EQ(begin_record->log_type_, LogType::begin);
    CommitLogRecord* commit_record = new CommitLogRecord();
    AbortLogRecord* abort_record = new AbortLogRecord();

    char* data = "insert data";
    RmRecord record(12, data);
    Rid rid{1,1};
    std::string table_name = "insert_table";
    txn_id_t txn_id = 1;
    InsertLogRecord* insert_record = new InsertLogRecord(txn_id, record, rid, table_name);
    insert_record->format_print();
    EXPECT_EQ(insert_record->log_type_, LogType::INSERT);

    char* delete_data = "delete data";
    RmRecord delete_record(12, delete_data);
    Rid delete_rid{1,1};
    std::string delete_table_name = "delete_table";
    txn_id_t delete_txn_id = 1;
    DeleteLogRecord* delete_log = new DeleteLogRecord(delete_txn_id, delete_record, delete_rid, delete_table_name);

    char* old_data = "old data";
    RmRecord old_record(9, old_data);
    char* new_data = "new data";
    RmRecord new_record(9, new_data);
    Rid update_rid{1,1};
    std::string update_table_name = "update_table";
    txn_id_t update_txn_id = 1;
    UpdateLogRecord* update_log = new UpdateLogRecord(update_txn_id, old_record, new_record, update_rid, update_table_name);

    log_manager_->add_log_to_buffer(begin_record);
    log_manager_->add_log_to_buffer(commit_record);
    log_manager_->add_log_to_buffer(abort_record);
    log_manager_->add_log_to_buffer(insert_record);
    log_manager_->add_log_to_buffer(delete_log);
    log_manager_->add_log_to_buffer(update_log);

    printf("\nflush log into disk\n\n");
    log_manager_->flush_log_to_disk();

    printf("print deserialized logs\n");
    int offset = 0;
    LogBuffer* buffer = new LogBuffer();
    disk_manager_->read_log(buffer->buffer_, LOG_BUFFER_SIZE, 0);
    BeginLogRecord* new_begin_record = new BeginLogRecord();
    new_begin_record->deserialize(buffer->buffer_ + offset);
    new_begin_record->format_print();
    // for (size_t i = new_begin_record->log_tot_len_; i < 60; i++) {
    //     printf("data[%lu]: %c, 0x%x.\n", i, buffer->buffer_[i], buffer->buffer_[i]);
    // }

    offset += new_begin_record->log_tot_len_;
    CommitLogRecord* new_commit_log = new CommitLogRecord();
    new_commit_log->deserialize(buffer->buffer_ + offset);
    new_commit_log->format_print();

    offset += new_commit_log->log_tot_len_;
    AbortLogRecord* new_abort_log = new AbortLogRecord();
    new_abort_log->deserialize(buffer->buffer_ + offset);
    new_abort_log->format_print();

    offset += new_abort_log->log_tot_len_;
    InsertLogRecord* new_insert_record = new InsertLogRecord();
    new_insert_record->deserialize(buffer->buffer_ + offset);
    new_insert_record->format_print();

    offset += new_insert_record->log_tot_len_;
    DeleteLogRecord* new_delete_log = new DeleteLogRecord();
    new_delete_log->deserialize(buffer->buffer_ + offset);
    new_delete_log->format_print();

    offset += new_delete_log->log_tot_len_;
    UpdateLogRecord* new_update_log = new UpdateLogRecord();
    new_update_log->deserialize(buffer->buffer_ + offset);
    new_update_log->format_print();
}