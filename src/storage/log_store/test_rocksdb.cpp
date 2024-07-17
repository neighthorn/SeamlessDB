#include <iostream>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

int main() {
  // 打开数据库
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, "testdb", &db);
  if (!status.ok()) {
    std::cerr << "Failed to open database. Error: " << status.ToString() << std::endl;
    return -1;
  }

  // 写入数据
  rocksdb::WriteOptions write_options;
  status = db->Put(write_options, "key1", "value1");
  if (!status.ok()) {
    std::cerr << "Failed to write data. Error: " << status.ToString() << std::endl;
    return -1;
  }

  // 读取数据
  rocksdb::ReadOptions read_options;
  std::string value;
  status = db->Get(read_options, "key1", &value);
  if (status.ok()) {
    std::cout << "Retrieved value: " << value << std::endl;
  } else {
    std::cerr << "Failed to retrieve data. Error: " << status.ToString() << std::endl;
    return -1;
  }

  // 删除数据
  status = db->Delete(write_options, "key1");
  if (!status.ok()) {
    std::cerr << "Failed to delete data. Error: " << status.ToString() << std::endl;
    return -1;
  }

  // 关闭数据库
  delete db;

  return 0;
}
