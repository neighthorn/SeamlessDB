#pragma once

#include "index/ix.h"
#include "sm_meta.h"
#include "common/context.h"

struct ColDef {
    std::string name;  // Column name
    ColType type;      // Type of column
    int len;           // Length of column

    ColDef() {}
    ColDef(std::string _name, ColType _type, int _len) {
        name = std::move(_name);
        type = _type;
        len = _len;
    }

    void serialize(char* dest, int& offset) {
        int name_size = name.length();
        memcpy(dest + offset, &name_size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, name.c_str(), name_size);
        offset += name_size;
        memcpy(dest + offset, &type, sizeof(ColType));
        offset += sizeof(ColType);
        memcpy(dest + offset, &len, sizeof(int));
        offset += sizeof(int);
    }

    void deserialize(char* src, int& offset) {
        int name_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        
        name = std::string(src + offset, name_size);
        offset += name_size;

        type = *reinterpret_cast<const ColType*>(src + offset);
        offset += sizeof(ColType);

        len = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
    }
};

/* 系统管理器，负责元数据管理和DDL语句的执行 */
class SmManager {
   public:
    DbMeta db_;             // 当前打开的数据库的元数据
    // std::unordered_map<std::string, std::unique_ptr<RmFileHandle>> fhs_;    // file name -> record file handle, 当前数据库中每张表的数据文件
    std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>> ihs_;   // file name -> index file handle, 当前数据库中每个索引的文件
    std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>> primary_index_;     // table_name -> primary_key index handle
    std::unordered_map<std::string, std::unique_ptr<MultiVersionFileHandle>> old_versions_; // table_name -> old_version每个表的旧版本数据
    
   private:
    DiskManager* disk_manager_;
    BufferPoolManager* buffer_pool_manager_;
    IxManager* ix_manager_;
    MultiVersionManager *multi_version_manager_;

   public:
    SmManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager,
              IxManager* ix_manager, MultiVersionManager* multi_version_manager)
        : disk_manager_(disk_manager),
          buffer_pool_manager_(buffer_pool_manager),
          ix_manager_(ix_manager),
          multi_version_manager_(multi_version_manager) {}

    ~SmManager() {}

    BufferPoolManager* get_bpm() { return buffer_pool_manager_; }

    IxManager* get_ix_manager() { return ix_manager_; }  

    bool is_dir(const std::string& db_name);

    void create_db(const std::string& db_name);

    void drop_db(const std::string& db_name);

    void open_db(const std::string& db_name);

    void close_db();

    void flush_meta();

    void show_tables(Context* context);

    void desc_table(const std::string& tab_name, Context* context);

    void create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, const std::vector<std::string>& pkeys, Context* context);

    void drop_table(const std::string& tab_name, Context* context);

    int get_table_id(const std::string& tab_name);

    void create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);

    void drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);
    
    void drop_index(const std::string& tab_name, const std::vector<ColMeta>& col_names, Context* context);
};