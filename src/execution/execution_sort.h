#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor;
class SortOperatorState;

struct SortCheckpointInfo {
    std::chrono::time_point<std::chrono::system_clock> ck_timestamp_;
};

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    //目前只支持一个键排序
    ColMeta cols_;
    size_t tuple_num;
    bool is_desc_;
    std::vector<size_t> used_tuple;
    std::unique_ptr<Record> current_tuple;

    std::vector<SortCheckpointInfo> ck_infos_;
    int be_call_times_;
    int left_child_call_times_;

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc) {
        prev_ = std::move(prev);
        cols_ = prev_->get_col_offset(sel_cols);
        is_desc_ = is_desc;
        tuple_num = 0;
        used_tuple.clear();
    }

    std::string getType() override { return "Sort"; }

    size_t tupleLen() const override { return prev_->tupleLen(); }

    const std::vector<ColMeta> &cols() const override { return prev_->cols(); }

    void beginTuple() override { 
        // 使用暴力排序，sort节点并不会存储所有的元组（只保存已经扫描rid和当前的元组），
        // begin时需要找到最大，或者最小的元组，然后记录元组的数量
        // 首先清理保存元素的vector（防止上层节点重复调用）
        used_tuple.clear();
        bool start = true;
        char *current;
        size_t tuple_index = 0, curr_index = 0;
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto Tuple = prev_->Next();
            char *rec_buf = Tuple->raw_data_ + cols_.offset;
            if(start) {
                current = rec_buf;
                curr_index = tuple_index;
                start = false;
                current_tuple = std::move(Tuple);
            } else {
                int cpm = ix_compare(current, rec_buf, cols_.type, cols_.len);
                if((is_desc_ && cpm < 0) || (!is_desc_ && cpm > 0 )) {
                    current = rec_buf;
                    curr_index = tuple_index;
                    current_tuple = std::move(Tuple);
                }
            }
            tuple_num++;
            tuple_index++;
        }
        used_tuple.emplace_back(curr_index);
    }

    void nextTuple() override {
        bool start = true;
        char *current;
        size_t tuple_index = 0, curr_index = 0;
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            if (std::find(used_tuple.begin(), used_tuple.end(), tuple_index) != used_tuple.end()) {
                tuple_index++;
                continue;
            }
            auto Tuple = prev_->Next();
            char *rec_buf = Tuple->raw_data_ + cols_.offset;
            if(start) {
                current = rec_buf;
                curr_index = tuple_index;
                start = false;
                current_tuple = std::move(Tuple);
            } else {
                int cpm = ix_compare(current, rec_buf, cols_.type, cols_.len);
                if((is_desc_ && cpm < 0) || (!is_desc_ && cpm > 0 )) {
                    current = rec_buf;
                    curr_index = tuple_index;
                    current_tuple = std::move(Tuple);
                }
            }
            tuple_index++;
        }
        tuple_num--;
        used_tuple.emplace_back(curr_index);
    }

    bool is_end() const override { return tuple_num == 0; }

    std::unique_ptr<Record> Next() override {
        return std::move(current_tuple);
    }
    
    ColMeta get_col_offset(const TabCol &target) override {
        return prev_->get_col_offset(target);
    }

    Rid &rid() override { return _abstract_rid; }

    int checkpoint(char* dest) {
        return 0;
    }

    std::pair<bool, double> judge_state_reward(SortCheckpointInfo* curr_ck_info);
    int64_t getRCop(std::chrono::time_point<std::chrono::system_clock> curr_time);
    void write_state_if_allow(int type = 0);
    void load_state_info(SortOperatorState *sort_op);
};