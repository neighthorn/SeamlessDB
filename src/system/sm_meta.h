#pragma once

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <assert.h>

#include "errors.h"

enum ColType: uint16_t {
    TYPE_INT, TYPE_FLOAT, TYPE_STRING
};

inline std::string coltype2str(ColType type) {
    std::map<ColType, std::string> m = {
            {TYPE_INT,    "INT"},
            {TYPE_FLOAT,  "FLOAT"},
            {TYPE_STRING, "STRING"}
    };
    return m.at(type);
}

template<typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::ostream &operator<<(std::ostream &os, const T &enum_val) {
    os << static_cast<int>(enum_val);
    return os;
}

template<typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::istream &operator>>(std::istream &is, T &enum_val) {
    int int_val;
    is >> int_val;
    enum_val = static_cast<T>(int_val);
    return is;
}

/* 字段元数据 */
struct ColMeta {
    std::string tab_name;   // 字段所属表名称
    std::string name;       // 字段名称
    ColType type;           // 字段类型
    int len;                // 字段长度
    int offset;             // 字段位于记录中的偏移量

    friend std::ostream &operator<<(std::ostream &os, const ColMeta &col) {
        // ColMeta中有各个基本类型的变量，然后调用重载的这些变量的操作符<<（具体实现逻辑在defs.h）
        return os << col.tab_name << ' ' << col.name << ' ' << col.type << ' ' << col.len << ' ' << col.offset;
    }

    friend std::istream &operator>>(std::istream &is, ColMeta &col) {
        return is >> col.tab_name >> col.name >> col.type >> col.len >> col.offset;
    }

    void serialize(char* dest, int& _offset) {
        int tab_name_len = tab_name.length();
        int name_len = name.length();
        memcpy(dest + _offset, &tab_name_len, sizeof(int));
        _offset += sizeof(int);
        memcpy(dest + _offset, tab_name.c_str(), tab_name_len);
        _offset += tab_name_len;
        memcpy(dest + _offset, &name_len, sizeof(int));
        _offset += sizeof(int);
        memcpy(dest + _offset, name.c_str(), name_len);
        _offset += name_len;
        memcpy(dest + _offset, &type, sizeof(ColType));
        _offset += sizeof(ColType);
        memcpy(dest + _offset, &len, sizeof(int));
        _offset += sizeof(int);
        memcpy(dest + _offset, &offset, sizeof(int));
        _offset += sizeof(int);
    }

    void deserialize(char* src, int& _offset) {
        int tab_name_len = *reinterpret_cast<const int*>(src + _offset);
        _offset += sizeof(int);
        tab_name = std::string(src + _offset, tab_name_len);
        _offset += tab_name_len;
        int name_len = *reinterpret_cast<const int*>(src + _offset);
        _offset += sizeof(int);
        name = std::string(src + _offset, name_len);
        _offset += name_len;
        type = *reinterpret_cast<const ColType*>(src + _offset);
        _offset += sizeof(ColType);
        len = *reinterpret_cast<const int*>(src + _offset);
        _offset += sizeof(int);
        offset = *reinterpret_cast<const int*>(src + _offset);
        _offset += sizeof(int);
    }
};

/* 索引元数据 */
struct IndexMeta {
    std::string tab_name;           // 索引所属表名称
    int col_tot_len;                // 索引字段长度总和
    int col_num;                    // 索引字段数量
    std::vector<ColMeta> cols;      // 索引包含的字段

    IndexMeta() {}

    IndexMeta(const IndexMeta& other) {
        tab_name = other.tab_name;
        col_tot_len = other.col_tot_len;
        col_num = other.col_num;
        for(auto col: other.cols) cols.emplace_back(col);
    }

    friend std::ostream &operator<<(std::ostream &os, const IndexMeta &index) {
        os << index.tab_name << " " << index.col_tot_len << " " << index.col_num;
        for(auto& col: index.cols) {
            os << "\n" << col;
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, IndexMeta &index) {
        is >> index.tab_name >> index.col_tot_len >> index.col_num;
        for(int i = 0; i < index.col_num; ++i) {
            ColMeta col;
            is >> col;
            index.cols.push_back(col);
        }
        return is;
    }
};

/* The meta data of table, stored separately from the table data */
struct TabMeta {
    std::string name_;                  // table name
    std::vector<ColMeta> cols_;         // column meta
    std::vector<IndexMeta> indexes_;    // index meta, the first is the primary index
    int record_length_;                 // the length of the record (fixed), does not contain the RecordHdr
    int table_id_;

    TabMeta(){}

    TabMeta(const TabMeta &other) {
        name_ = other.name_;
        record_length_ = other.record_length_;
        table_id_ = other.table_id_;
        for(auto col : other.cols_) cols_.push_back(col);
        for(auto index: other.indexes_) indexes_.emplace_back(index);
    }

    /* 判断当前表中是否存在名为col_name的字段 */
    bool is_col(const std::string &col_name) const {
        auto pos = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta &col) { return col.name == col_name; });
        return pos != cols_.end();
    }

    /* 判断当前表上是否建有指定索引，索引包含的字段为col_names */
    bool is_index(const std::vector<std::string>& col_names) const {
        for(auto& index: indexes_) {
            if(index.col_num == (int)col_names.size()) {
                int i = 0;
                for(; i < index.col_num; ++i) {
                    if(index.cols[i].name.compare(col_names[i]) != 0)
                        break;
                }
                if(i == index.col_num) return true;
            }
        }

        return false;
    }

    std::vector<IndexMeta>::iterator get_primary_index_meta() {
        assert(indexes_.begin() != indexes_.end());
        return indexes_.begin();
    }

    /* 根据字段名称集合获取索引元数据 */
    std::vector<IndexMeta>::iterator get_index_meta(const std::vector<std::string>& col_names) {
        for(auto index = indexes_.begin(); index != indexes_.end(); ++index) {
            if((*index).col_num != (int)col_names.size()) continue;
            auto& index_cols = (*index).cols;
            size_t i = 0;
            for(; i < col_names.size(); ++i) {
                if(index_cols[i].name.compare(col_names[i]) != 0) 
                    break;
            }
            if(i == col_names.size()) return index;
        }
        throw IndexNotFoundError(name_, col_names);
    }

    /* 根据字段名称获取字段元数据 */
    std::vector<ColMeta>::iterator get_col(const std::string &col_name) {
        auto pos = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta &col) { return col.name == col_name; });
        if (pos == cols_.end()) {
            throw ColumnNotFoundError(col_name);
        }
        return pos;
    }

    friend std::ostream &operator<<(std::ostream &os, const TabMeta &tab) {
        os << tab.name_ << '\n' << tab.table_id_ << '\n' << tab.cols_.size() << '\n' << tab.record_length_ << '\n';
        for (auto &col : tab.cols_) {
            os << col << '\n';  // col是ColMeta类型，然后调用重载的ColMeta的操作符<<
        }
        os << tab.indexes_.size() << "\n";
        for (auto &index : tab.indexes_) {
            os << index << "\n";
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, TabMeta &tab) {
        int n;
        is >> tab.name_  >> tab.table_id_ >> n >> tab.record_length_;
        for (int i = 0; i < n; i++) {
            ColMeta col;
            is >> col;
            tab.cols_.push_back(col);
        }
        is >> n;
        for(int i = 0; i < n; ++i) {
            IndexMeta index;
            is >> index;
            tab.indexes_.push_back(index);
        }
        return is;
    }
};

// 注意重载了操作符 << 和 >>，这需要更底层同样重载TabMeta、ColMeta的操作符 << 和 >>
/* 数据库元数据 */
class DbMeta {
   public:
    std::string name_;                      // 数据库名称
    std::map<std::string, TabMeta> tabs_;   // 数据库中包含的表
    int next_table_id_ = 0;                     // 用于table_id的分配

   public:
    // DbMeta(std::string name) : name_(name) {}

    /* 判断数据库中是否存在指定名称的表 */
    bool is_table(const std::string &tab_name) const { return tabs_.find(tab_name) != tabs_.end(); }

    void SetTabMeta(const std::string &tab_name, const TabMeta &meta) {
        tabs_[tab_name] = meta;
    }

    /* 获取指定名称表的元数据 */
    TabMeta &get_table(const std::string &tab_name) {
        auto pos = tabs_.find(tab_name);
        if (pos == tabs_.end()) {
            throw TableNotFoundError(tab_name);
        }

        return pos->second;
    }

    // 重载操作符 <<
    friend std::ostream &operator<<(std::ostream &os, const DbMeta &db_meta) {
        os << db_meta.name_ << '\n' << db_meta.next_table_id_ << '\n';
        for (auto &entry : db_meta.tabs_) {
            os << entry.second << '\n';
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, DbMeta &db_meta) {
        // size_t n;
        is >> db_meta.name_ >> db_meta.next_table_id_;
        for (int i = 0; i < db_meta.next_table_id_; i++) {
            TabMeta tab;
            is >> tab;
            db_meta.tabs_[tab.name_] = tab;
        }
        return is;
    }
};
