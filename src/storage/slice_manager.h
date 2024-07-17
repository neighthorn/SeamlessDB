#pragma once

#include <unordered_map>

#include "common/config.h"

class SliceId {
public:
    SliceId(int table_id, int slice_id) : table_id_(table_id), slice_id_(slice_id) {}

    inline int64_t Get() const {
        return ((static_cast<int64_t>(table_id_)) << 32) | (static_cast<int64_t>(slice_id_));
    }

    bool operator == (const SliceId &other) const {
        if(table_id_ != other.table_id_) return false;
        return slice_id_ == other.slice_id_;
    }

    int table_id_;
    int slice_id_;
};

template <>
struct std::hash<SliceId> {
    size_t operator() (const SliceId &obj) const { return std::hash<int64_t>()(obj.Get()); }
};

/**
 * Each compute node has a SliceMetaManager which is used to maintain the latest_lsn that has sent to the slice.
*/
class SliceMetaManager {
public:
    void set_latest_lsn(SliceId slice_id, lsn_t latest_lsn) {
        if(slice_lsn_.count(slice_id) == 0) {
            slice_lsn_.emplace(slice_id, latest_lsn);
            return;
        }
        slice_lsn_[slice_id] = latest_lsn;
    }
    lsn_t get_latest_lsn(SliceId slice_id) {
        if(slice_lsn_.count(slice_id) == 0) {
            return -1;
        }
        return slice_lsn_.find(slice_id)->second;
    }
private:
    std::unordered_map<SliceId, lsn_t> slice_lsn_;
};