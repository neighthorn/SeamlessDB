#undef NDEBUG

#include "execution.h"
#include "optimizer/optimizer.h"

#define BUFFER_LENGTH 8192

auto disk_manager = std::make_unique<DiskManager>();
auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
auto rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
auto ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
auto sm_manager =
    std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(), ix_manager.get());
auto ql_manager = std::make_unique<QlManager>(sm_manager.get());
auto lock_manager = std::make_unique<LockManager>();
auto txn_manager = std::make_unique<TransactionManager>(lock_manager.get());
auto log_manager = std::make_unique<LogManager>(disk_manager.get());
auto interp = std::make_unique<Interp>(sm_manager.get(), ql_manager.get(), txn_manager.get());
char *result = new char[BUFFER_LENGTH];
int offset;
txn_id_t txn_id = INVALID_TXN_ID;

void exec_sql(const std::string &sql) {
    std::cout << sql << std::endl;
    YY_BUFFER_STATE buffer = yy_scan_string(sql.c_str());
    assert(yyparse() == 0 && ast::parse_tree != nullptr);
    yy_delete_buffer(buffer);
    memset(result, 0, BUFFER_LENGTH);
    offset = 0;
    Context *context = new Context(lock_manager.get(), log_manager.get(),
                                   nullptr, result, &offset);
    interp->interp_sql(ast::parse_tree, &txn_id, context);
}

int main() {
    std::string db_name = "db";
    if (sm_manager->is_dir(db_name)) {
        sm_manager->drop_db(db_name);
    }
    sm_manager->create_db(db_name);
    sm_manager->open_db(db_name);

    exec_sql("create table tb1(s int, a int, b float, c char(16));");
    exec_sql("create table tb2(x int, y float, z char(16), s int);");
    exec_sql("create index tb1(s);");

    exec_sql("show tables;");
    exec_sql("desc tb1;");

    exec_sql("select * from tb1;");
    exec_sql("insert into tb1 values (0, 1, 1.1, 'abc');");
    exec_sql("insert into tb1 values (2, 2, 2.2, 'def');");
    exec_sql("insert into tb1 values (5, 3, 2.2, 'xyz');");
    exec_sql("insert into tb1 values (4, 4, 2.2, '0123456789abcdef');");
    exec_sql("insert into tb1 values (2, 5, -1.11, '');");

    exec_sql("insert into tb2 values (1, 2., '123', 0);");
    exec_sql("insert into tb2 values (2, 3., '456', 1);");
    exec_sql("insert into tb2 values (3, 1., '789', 2);");

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
    sm_manager->close_db();
    return 0;
}
