#include "gtest/gtest.h"
#include "log_manager.h"
#include "system/sm_manager.h"

const std::string TEST_DB_NAME = "RecoveryTestDB";

class RecoveryTest: public ::testing::Test {
public:
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<SmManager> sm_manager_;

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

TEST_F(LogManagerTest, SerializeTest) {

}
