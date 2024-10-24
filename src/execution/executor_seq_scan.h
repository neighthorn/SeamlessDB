#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    // RmFileHandle *fh_;                  // 表的数据文件句柄
    IxIndexHandle* pindex_handle_;                 // 索引文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    // std::unique_ptr<RecScan> scan_;     // table_iterator
    std::unique_ptr<IxScan> scan_;

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        // fh_ = sm_manager_->fhs_.at(tab_name_).get();
        pindex_handle_ = sm_manager->primary_index_.at(tab_name_).get();

        cols_ = tab.cols_;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;

        if(context_ != nullptr) {
            Lock* lock = context_->lock_mgr_->request_table_lock(tab.table_id_, context_->txn_, LockMode::LOCK_S, context_->coro_sched_->t_id_);
            assert(lock != nullptr);
            context_->txn_->append_lock(lock);
        }
    }

    std::string getType() override { return "SeqScan"; }

    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const Record* rec) {
        if(rec->is_deleted()) {
            return false;
        }

        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        char *lhs = rec->raw_data_ + lhs_col->offset;
        char *rhs;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
            rhs = cond.rhs_val.raw->data;
        } else {
            // rhs is a column
            auto rhs_col = get_col(rec_cols, cond.rhs_col);
            rhs_type = rhs_col->type;
            rhs = rec->raw_data_ + rhs_col->offset;
        }
        assert(rhs_type == lhs_col->type);  // TODO convert to common type
        int cmp = ix_compare(lhs, rhs, rhs_type, lhs_col->len);
        if (cond.op == OP_EQ) {
            return cmp == 0;
        } else if (cond.op == OP_NE) {
            return cmp != 0;
        } else if (cond.op == OP_LT) {
            return cmp < 0;
        } else if (cond.op == OP_GT) {
            return cmp > 0;
        } else if (cond.op == OP_LE) {
            return cmp <= 0;
        } else if (cond.op == OP_GE) {
            return cmp >= 0;
        } else {
            throw InternalError("Unexpected op type");
        }
    }

    bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds, const Record* rec) {
        return std::all_of(conds.begin(), conds.end(),
                           [&](const Condition &cond) { return eval_cond(rec_cols, cond, rec); });
    }

    void beginTuple() override {
        check_runtime_conds();

        // scan_ = std::make_unique<RmScan>(fh_);
        scan_ = std::make_unique<IxScan>(pindex_handle_);

        // Get the first record
        while(!scan_->is_end()) {
            rid_ = scan_->rid();
            // std::cout << "SeqScan, rid: {page_no=" << rid_.page_no << ", slot_no=" << rid_.slot_no << "}\n";
            auto record = pindex_handle_->get_record(scan_->rid(), context_);
            if(eval_conds(cols_, fed_conds_, record.get()) && record->is_deleted() == false) {
                break;
            }
            scan_->next();
        }
    }

    void nextTuple() override {
        check_runtime_conds();
        assert(!scan_->is_end());
        // for (scan_->next(); !scan_->is_end(); scan_->next()) {  // 用TableIterator遍历TableHeap中的所有Tuple
        //     rid_ = scan_->rid();
        //     auto rec = fh_->get_record(rid_, context_);
        //     if (eval_conds(cols_, fed_conds_, rec.get())) {  // Tuple是否满足Schema模式
        //         break;
        //     }
        // }

        for(scan_->next(); !scan_->is_end(); scan_->next()) {
            rid_ = scan_->rid();
            auto record = pindex_handle_->get_record(rid_, context_);
            if(eval_conds(cols_, fed_conds_, record.get()) && record->is_deleted() == false) {
                break;
            }
        }
    }

    bool is_end() const override { return scan_->is_end(); }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    std::unique_ptr<Record> Next() override {
        assert(!is_end());
        return pindex_handle_->get_record(rid_, context_);
    }

    Rid &rid() override { return rid_; }

    ColMeta get_col_offset(const TabCol &target) override {
        auto pos = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == cols_.end()) {
            ColMeta col = {.tab_name = "",
               .name = "",
               .type = TYPE_INT,
               .len = -1,
               .offset = -1};
            return col;
        }
        return *pos;
    }

    void check_runtime_conds() {
        for (auto &cond : fed_conds_) {
            assert(cond.lhs_col.tab_name == tab_name_);
            if (!cond.is_rhs_val) {
                assert(cond.rhs_col.tab_name == tab_name_);
            }
        }
    }

    int checkpoint(char* dest) override {
        int offset = 0;
        memcpy(dest + offset, &scan_->rid(), sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &scan_->end(), sizeof(Rid));
        offset += sizeof(Rid);
        
        return offset;
    }
    std::chrono::time_point<std::chrono::system_clock> get_latest_ckpt_time() override {
        assert(0);
        return std::chrono::high_resolution_clock::now();
    }
    
    double get_curr_suspend_cost() override {
        assert(0);
        return 0;
    }
};