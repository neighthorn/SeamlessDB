#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "record/rm_defs.h"
#include "system/sm_meta.h"

struct TabCol {
    std::string tab_name;
    std::string col_name;

    friend bool operator<(const TabCol &x, const TabCol &y) {
        return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
    }

    void serialize(char* dest, int& offset) {
        int tab_name_size = tab_name.size();
        int col_name_size = col_name.size();
        memcpy(dest + offset, &tab_name_size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, tab_name.c_str(), tab_name_size);
        offset += tab_name_size;
        memcpy(dest + offset, &col_name_size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, col_name.c_str(), col_name_size);
        offset += col_name_size;
    }

    void deserialize(char* src, int& offset) {
        int tab_name_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        tab_name = std::string(src + offset, tab_name_size);
        offset += tab_name_size;
        int col_name_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        col_name = std::string(src + offset, col_name_size);
        offset += col_name_size;
    }
};

// 定义了check type
inline bool check_type(ColType lhs, ColType rhs) {
    // int 和 float可以比较
    if((lhs == TYPE_INT && rhs == TYPE_FLOAT) || (lhs == TYPE_FLOAT && rhs == TYPE_INT)){
        return true;
    } else {
        return lhs == rhs;
    }
}

struct Value {
    ColType type;  // type of value
    union {
        int int_val;      // int value
        float float_val;  // float value
    };
    std::string str_val;  // string value

    std::shared_ptr<RmRecord> raw;  // raw record buffer

    void set_int(int int_val_) {
        type = TYPE_INT;
        int_val = int_val_;
    }

    void set_float(float float_val_) {
        type = TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_) {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }

    void init_raw(int len) {
        assert(raw == nullptr);
        raw = std::make_shared<RmRecord>(len);
        if (type == TYPE_INT) {
            assert(len == sizeof(int));
            *(int *)(raw->data) = int_val;
        } else if (type == TYPE_FLOAT) {
            assert(len == sizeof(float));
            *(float *)(raw->data) = float_val;
        } else if (type == TYPE_STRING) {
            if (len < (int)str_val.size()) {
                throw StringOverflowError();
            }
            memset(raw->data, 0, len);
            memcpy(raw->data, str_val.c_str(), str_val.size());
        }
    }

    void serialize(char* dest, int& offset) {
        memcpy(dest + offset, &type, sizeof(ColType));
        offset += sizeof(ColType);
        memcpy(dest + offset, &raw->size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, raw->data, raw->size);
        offset += raw->size;
    }

    void deserialize(char* src, int& offset) {
        type = *reinterpret_cast<const ColType*>(src + offset);
        offset += sizeof(ColType);
        int len = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        raw = std::make_shared<RmRecord>(len);
        memcpy(raw->data, src + offset, len);
        offset += len;
    }

    // 重载value的比较运算符 > < == != >= <=
    bool operator>(const Value& rhs) const {
        if (!check_type(type, rhs.type)) {
            throw IncompatibleTypeError(coltype2str(type), coltype2str(rhs.type));
        }
        switch (type) {
            case TYPE_INT:
                if(rhs.type == TYPE_INT) {
                    return int_val > rhs.int_val;
                }else if(rhs.type == TYPE_FLOAT) {
                    return int_val > rhs.float_val;
                }         
            case TYPE_FLOAT:
                if (rhs.type == TYPE_INT) {
                    return float_val > rhs.int_val;
                } else if (rhs.type == TYPE_FLOAT) {
                    return float_val > rhs.float_val;
                }      
            case TYPE_STRING:
                return strcmp(str_val.c_str(), rhs.str_val.c_str()) > 0 ? true : false;
                // return str_val > rhs.str_val;
        }
        throw IncompatibleTypeError(coltype2str(type), coltype2str(rhs.type));
    }

    bool operator<(const Value& rhs) const {
        if (!check_type(type, rhs.type)) {
            throw IncompatibleTypeError(coltype2str(type), coltype2str(rhs.type));
        }
        
        switch (type) {
            case TYPE_INT:
                if(rhs.type == TYPE_INT){
                    return int_val < rhs.int_val;
                }else if(rhs.type == TYPE_FLOAT){
                    return int_val < rhs.float_val;
                }        
            case TYPE_FLOAT:
                if (rhs.type == TYPE_INT) {
                    return float_val < rhs.int_val;
                } else if (rhs.type == TYPE_FLOAT) {
                    return float_val < rhs.float_val;
                }  
            case TYPE_STRING:
                return strcmp(str_val.c_str(), rhs.str_val.c_str()) < 0 ? true : false;
        }

        throw IncompatibleTypeError(coltype2str(type), coltype2str(rhs.type));
    }

    bool operator==(const Value& rhs) const {
        if (!check_type(type, rhs.type)) {
            return false;
        }
        
        switch (type) {
            case TYPE_INT:
                if(rhs.type == TYPE_INT){
                    return int_val == rhs.int_val;
                }else if(rhs.type == TYPE_FLOAT){
                    return int_val == rhs.float_val;
                }      
            case TYPE_FLOAT:
                if (rhs.type == TYPE_INT) {
                    return float_val == rhs.int_val;
                } else if (rhs.type == TYPE_FLOAT) {
                    return float_val == rhs.float_val;
                }  
            case TYPE_STRING:
                return strcmp(str_val.c_str(), rhs.str_val.c_str()) == 0 ? true : false;
        }
        
        throw IncompatibleTypeError(coltype2str(type), coltype2str(rhs.type));
    }

    bool operator!=(const Value& rhs) const {
        return !(*this == rhs);
    }

    bool operator>=(const Value& rhs) const {
        if (!check_type(type, rhs.type)) {
            throw IncompatibleTypeError(coltype2str(type), coltype2str(rhs.type));
        }
        
        switch (type) {
            case TYPE_INT:
                if(rhs.type == TYPE_INT){
                    return int_val >= rhs.int_val;
                }else if(rhs.type == TYPE_FLOAT){
                    return int_val >= rhs.float_val;
                }      
            case TYPE_FLOAT:
                if (rhs.type == TYPE_INT) {
                    return float_val >= rhs.int_val;
                } else if (rhs.type == TYPE_FLOAT) {
                    return float_val >= rhs.float_val;
                }  
            case TYPE_STRING:
                return strcmp(str_val.c_str(), rhs.str_val.c_str()) >= 0 ? true : false;
        }
        
        throw IncompatibleTypeError(coltype2str(type), coltype2str(rhs.type));
    }

    bool operator<=(const Value& rhs) const {
        if (!check_type(type, rhs.type)) {
            throw IncompatibleTypeError(coltype2str(type), coltype2str(rhs.type));
        }
        switch (type) {
            case TYPE_INT:
                if(rhs.type == TYPE_INT){
                    return int_val <= rhs.int_val;
                }else if(rhs.type == TYPE_FLOAT){
                    return int_val <= rhs.float_val;
                }        
            case TYPE_FLOAT:
                if (rhs.type == TYPE_INT) {
                    return float_val <= rhs.int_val;
                } else if (rhs.type == TYPE_FLOAT) {
                    return float_val <= rhs.float_val;
                }  
            case TYPE_STRING:
                return strcmp(str_val.c_str(), rhs.str_val.c_str()) <= 0 ? true : false;
        }
        
        throw IncompatibleTypeError(coltype2str(type), coltype2str(rhs.type));
    }
};

enum CompOp: int { OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE };
static std::string CompOpString[] = {"=", "!=", "<", ">", "<=", ">="};

struct Condition {
    TabCol lhs_col;   // left-hand side column
    CompOp op;        // comparison operator
    bool is_rhs_val;  // true if right-hand side is a value (not a column)
    TabCol rhs_col;   // right-hand side column
    Value rhs_val;    // right-hand side value

    void serialize(char* dest, int& offset) {
        lhs_col.serialize(dest, offset);
        memcpy(dest + offset, &op, sizeof(CompOp));
        offset += sizeof(CompOp);
        memcpy(dest + offset, &is_rhs_val, sizeof(bool));
        offset += sizeof(bool);
        if(is_rhs_val) {
            rhs_val.serialize(dest, offset);
        }
        else {
            rhs_col.serialize(dest, offset);
        }
    }

    void deserialize(char* src, int& offset) {
        lhs_col.deserialize(src, offset);
        op = *reinterpret_cast<const CompOp*>(src + offset);
        offset += sizeof(CompOp);
        is_rhs_val = *reinterpret_cast<const bool*>(src + offset);
        offset += sizeof(bool);
        if(is_rhs_val) {
            rhs_val.deserialize(src, offset);
        }
        else {
            rhs_col.deserialize(src, offset);
        }
    }
};

struct SetClause {
    TabCol lhs;
    Value rhs;

    void serialize(char* dest, int& offset) {
        lhs.serialize(dest, offset);
        rhs.serialize(dest, offset);
    }

    void deserialize(char* src, int& offset) {
        lhs.deserialize(src, offset);
        rhs.deserialize(src, offset);
    }
};

typedef enum PlanTag{
    T_Invalid = 1,
    T_Help,
    T_ShowTable,
    T_DescTable,
    T_CreateTable,
    T_DropTable,
    T_CreateIndex,
    T_DropIndex,
    T_Insert,
    T_Update,
    T_Delete,
    T_select,
    T_Transaction_begin,
    T_Transaction_commit,
    T_Transaction_abort,
    T_Transaction_rollback,
    T_SeqScan,
    T_IndexScan,
    T_NestLoop,
    T_HashJoin,
    T_Sort,
    T_Projection
} PlanTag;

enum NodeType: int {
    COMPUTE_NODE,
    STORAGE_NODE
};