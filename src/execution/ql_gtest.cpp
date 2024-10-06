#undef NDEBUG

#include "execution.h"
#include "gtest/gtest.h"
#include "optimizer/optimizer.h"

#define BUFFER_LENGTH 8192

// Add by jiawen
class ExecutorTest : public ::testing::Test {
   public:
    std::string db_name_ = "ExecutorTest_db";
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SmManager> sm_manager_;
    std::unique_ptr<QlManager> ql_manager_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> txn_manager_;
    std::unique_ptr<Interp> interp_;
    txn_id_t txn_id = INVALID_TXN_ID;
    char *result = new char[BUFFER_LENGTH];
    int offset;

   public:
    // This function is called before every test.
    void SetUp() override {
        ::testing::Test::SetUp();
        // For each test, we create a new BufferPoolManager...
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        sm_manager_ = std::make_unique<SmManager>(disk_manager_.get(), buffer_pool_manager_.get(), rm_manager_.get(),
                                                  ix_manager_.get());
        ql_manager_ = std::make_unique<QlManager>(sm_manager_.get());
        log_manager_ = std::make_unique<LogManager>(disk_manager_.get());
        lock_manager_ = std::make_unique<LockManager>();
        txn_manager_ = std::make_unique<TransactionManager>(lock_manager_.get());
        interp_ = std::make_unique<Interp>(sm_manager_.get(), ql_manager_.get(), txn_manager_.get());
        // create db and open db
        if (sm_manager_->is_dir(db_name_)) {
            sm_manager_->drop_db(db_name_);
        }
        sm_manager_->create_db(db_name_);
        sm_manager_->open_db(db_name_);
    }

    // This function is called after every test.
    void TearDown() override {
        sm_manager_->close_db();  // exit
        // sm_manager_->drop_db(db_name_);  // 若不删除数据库文件，则将保留最后一个测试点的数据库
    };

    // The below helper functions are useful for testing.
    void exec_sql(const std::string &sql) {
        YY_BUFFER_STATE yy_buffer = yy_scan_string(sql.c_str());
        assert(yyparse() == 0 && ast::parse_tree != nullptr);
        yy_delete_buffer(yy_buffer);
        memset(result, 0, BUFFER_LENGTH);
        offset = 0;
        Context *context = new Context(lock_manager_.get(), log_manager_.get(),
                                       nullptr, result, &offset);
        interp_->interp_sql(ast::parse_tree, &txn_id, context);  // 主要执行逻辑
        std::cout << result << std::endl;
    };
};

TEST_F(ExecutorTest, CreateTest) {
    exec_sql("create table tb2(x int, y float, z char(16), s int);");
    exec_sql("select * from tb2;");

    exec_sql("create table tb1(s int, a int, b float, c char(16));");
    exec_sql("select * from tb1;");
}

TEST_F(ExecutorTest, InsertTest) {
    exec_sql("create table student (id int, name char(32), major char(32));");

    exec_sql("select * from student;");
    exec_sql("insert into student values(3, 'Tom', 'Computer Science');");
    exec_sql("insert into student values(2, 'Jerry', 'Computer Science');");
    exec_sql("insert into student values(1, 'Jack', 'Electrical Engineering');");
    exec_sql("select * from student;");
}

TEST_F(ExecutorTest, InsertWithIndexTest) {
    exec_sql("create table student (id int, name char(32), major char(32));");
    exec_sql("create index student (id);");  // NOTE THIS

    exec_sql("select * from student;");
    exec_sql("insert into student values(3, 'Tom', 'Computer Science');");
    exec_sql("insert into student values(2, 'Jerry', 'Computer Science');");
    exec_sql("insert into student values(1, 'Jack', 'Electrical Engineering');");
    exec_sql("select * from student;");
}

TEST_F(ExecutorTest, ReopenTest) {
    exec_sql("create table student (id int, name char(32), major char(32));");
    exec_sql("select * from student;");
    sm_manager_->close_db();

    sm_manager_->open_db(db_name_);
    exec_sql("create index student (id);");  // NOTE THIS
    exec_sql("select * from student;");
    exec_sql("insert into student values (1, 'Tom', 'Computer Science');");
    exec_sql("select * from student;");
    sm_manager_->close_db();

    sm_manager_->open_db(db_name_);
    exec_sql("select * from student;");
    exec_sql("insert into student values (2, 'Tom', 'Computer Science');");
    exec_sql("select * from student;");
}

TEST_F(ExecutorTest, IntegratedTest) {
    exec_sql("create table tb1(s int, a int, b float, c char(16));");
    exec_sql("desc tb1;");
    exec_sql("select * from tb1;");

    exec_sql("create table tb2(x int, y float, z char(16), s int);");
    exec_sql("desc tb2;");
    exec_sql("select * from tb2;");

    exec_sql("show tables;");

    exec_sql("create index tb1(s);");
    exec_sql("desc tb1;");

    exec_sql("insert into tb1 values (0, 1, 1.1, 'abc');");
    exec_sql("insert into tb1 values (2, 2, 2.2, 'def');");
    exec_sql("insert into tb1 values (5, 3, 2.2, 'xyz');");
    exec_sql("insert into tb1 values (4, 4, 2.2, '0123456789abcdef');");
    exec_sql("insert into tb1 values (2, 5, -1.11, '');");
    exec_sql("select * from tb1;");

    exec_sql("insert into tb2 values (1, 2., '123', 0);");
    exec_sql("insert into tb2 values (2, 3., '456', 1);");
    exec_sql("insert into tb2 values (3, 1., '789', 2);");
    exec_sql("select * from tb2;");

    exec_sql("select * from tb1, tb2;");
    exec_sql("select * from tb1, tb2 where tb1.s = tb2.s;");

    exec_sql("create index tb2(s);");
    exec_sql("select * from tb1, tb2;");
    exec_sql("select * from tb1, tb2 where tb1.s = tb2.s;");
    exec_sql("drop index tb2(s);");

    try {
        exec_sql("insert into tb1 values (0, 1, 2., 100);");
        assert(0);
    } catch (IncompatibleTypeError &) {
    }
    try {
        exec_sql("insert into tb1 values (0, 1, 2., 'abc', 1);");
        assert(0);
    } catch (InvalidValueCountError &) {
    }
    try {
        exec_sql("insert into oops values (1, 2);");
        assert(0);
    } catch (TableNotFoundError &) {
    }
    try {
        exec_sql("create table tb1 (a int, b float);");
        assert(0);
    } catch (TableExistsError &) {
    }
    try {
        exec_sql("create index tb1(oops);");
        assert(0);
    } catch (ColumnNotFoundError &e) {
    }
    try {
        exec_sql("create index tb1(s);");
        assert(0);
    } catch (IndexExistsError &e) {
    }
    try {
        exec_sql("drop index tb1(a);");
        assert(0);
    } catch (IndexNotFoundError &) {
    }
    try {
        exec_sql("insert into tb1 values (0, 1, 2., '0123456789abcdefg');");
        assert(0);
    } catch (StringOverflowError &) {
    }
    try {
        exec_sql("select x from tb1;");
        assert(0);
    } catch (ColumnNotFoundError &) {
    }
    try {
        exec_sql("select s from tb1, tb2;");
        assert(0);
    } catch (AmbiguousColumnError &) {
    }
    try {
        exec_sql("select * from tb1, tb2 where s = 1;");
        assert(0);
    } catch (AmbiguousColumnError &) {
    }
    try {
        exec_sql("select * from tb1 where s = 'oops';");
        assert(0);
    } catch (IncompatibleTypeError &) {
    }
    exec_sql("select * from tb1;");
    exec_sql("select * from tb2;");
    exec_sql("select * from tb1, tb2;");
}
