#pragma once

#include "execution_defs.h"
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor {
   public:
    Rid _abstract_rid;

    Context *context_;

    ExecutionType   exec_type_;

    int     sql_id_;

    int     operator_id_;

    inline AbstractExecutor(int sql_id = -1, int op_id = -1) : sql_id_(sql_id), operator_id_(op_id) , exec_type_(ExecutionType::NOT_DEFINED){}

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    virtual const std::vector<ColMeta> &cols() const {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    virtual std::string getType() { return "AbstractExecutor"; };

    virtual void beginTuple(){};

    virtual void nextTuple(){};

    virtual bool is_end() const { return true; };

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<Record> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta();};

    // virtual void load_op_checkpoint() {}

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

    virtual int checkpoint(char* dest) = 0;

        // 从Record中取出某一列的Value
    Value fetch_value(const std::unique_ptr<Record> &record, const ColMeta& col) const {
        return fetch_value(record.get(), col);
    }

            // 从Record中取出某一列的Value
    Value fetch_value(const Record *record, const ColMeta& col) const {
        char *data = record->raw_data_ + col.offset;
        size_t len = col.len;
        Value ret;
        ret.type = col.type;
        // ret.init_raw(len);
        if(col.type == TYPE_INT) {
            int tmp;
            memcpy((char*)&tmp, data, len);
            ret.set_int(tmp);
        } else if(col.type == TYPE_FLOAT) {
            float tmp;
            memcpy((char*)&tmp, data, len);
            ret.set_float(tmp);
        } else if(col.type == TYPE_STRING) {
            std::string tmp(data, len);
            ret.set_str(tmp);
        } else {
            throw InvalidTypeError();
        }
        ret.init_raw(len);
        return ret;
    }

    // compare_value的功能是比较left_vlaue和right_value的值是否符合CompOp中定义的op
    bool compare_value(const Value& left_value, const Value& right_value, CompOp op) const {
        // 检查type是否一致
        if(!check_type(left_value.type, right_value.type)) {
            throw IncompatibleTypeError(coltype2str(left_value.type), coltype2str(right_value.type));
        }
        switch (op)
        {
            case OP_EQ:
                return left_value == right_value; 
            case OP_NE: 
                return left_value != right_value;
            case OP_LT: 
                return left_value < right_value;
            case OP_GT: 
                return left_value > right_value;
            case OP_LE: 
                return left_value <= right_value;
            case OP_GE:
                return left_value >= right_value;
        }
        throw IncompatibleTypeError(coltype2str(left_value.type), coltype2str(right_value.type));
    }
};