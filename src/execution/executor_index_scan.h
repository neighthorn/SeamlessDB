#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"
#include "state/state_item/op_state.h"

// 索引查询的条件：(a,b,c) 遇到第一个非等值查询就停止

static LockMode get_lock_mode_for_plan(PlanTag plan_tag) {
    switch(plan_tag) {
        case T_select: return LOCK_S;
        case T_Update:
        case T_Delete:  return LOCK_X;
        default: 
            std::cout << "Invalid plan tag to request lock\n";
            return NON_LOCK;
    }
}

// primary索引上的用IndexScanExecutor
class IndexScanExecutor : public AbstractExecutor {

friend class IndexScanOperatorState;

   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> index_conds_;              // 扫描条件
    // RmFileHandle *fh_;                          // 表的数据文件句柄
    IxIndexHandle* pindex_handle_;              // 只考虑primary索引
    MultiVersionFileHandle* old_version_handle_;    // old_version handle
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> filter_conds_;          // 扫描条件，和conds_字段相同

    // std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<IxScan> scan_;
    std::unique_ptr<Record> current_record_;    // currrent record

    SmManager *sm_manager_;

    bool min_lock_ = false;
    bool max_lock_ = false;

    Rid lower_rid_;
    Rid upper_rid_;

    bool load_from_state_ = false;
    IndexScanOperatorState *index_scan_op_ = nullptr;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> filter_conds, std::vector<Condition> index_conds, Context *context, int sql_id, int operator_id)
        : AbstractExecutor(sql_id, operator_id) {
        exec_type_ = ExecutionType::INDEX_SCAN;
        
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        filter_conds_ = std::move(filter_conds);
        index_conds_ = std::move(index_conds);
        // index_no_ = index_no;
        // index_col_names_ = index_col_names; 
        // index_meta_ = *(tab_.get_index_meta(index_col_names_));
        index_meta_ = *(tab_.get_primary_index_meta());
        // fh_ = sm_manager_->fhs_.at(tab_name_).get();
        pindex_handle_ = sm_manager->primary_index_.at(tab_name_).get();
        old_version_handle_ = sm_manager->old_versions_.at(tab_name_).get();
        cols_ = tab_.cols_;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : filter_conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        // fed_conds_ = conds_;

        // first request LOCK_IX on table
        Lock* lock = nullptr;
        if(context_ != nullptr) {
            switch(context_->plan_tag_) {
                case T_Update:
                case T_Delete: {
                    lock = context_->lock_mgr_->request_table_lock(tab_.table_id_, context_->txn_, LockMode::LOCK_IX, context_->coro_sched_->t_id_);
                } break;
                case T_select: {
                    lock = context_->lock_mgr_->request_table_lock(tab_.table_id_, context_->txn_, LockMode::LOCK_IS, context_->coro_sched_->t_id_);
                } break;
                default:
                    std::cout << "Invalid plan tag\n";
                    break;
            }
            assert(lock != nullptr);
            context_->txn_->append_lock(lock);
        }

        be_call_times_ = 0;
        left_child_call_times_ = 0;
    }

    std::string getType() override { return "indexScan"; }

    void setMaxKey(char* key, int offset, int len, ColType type) {
        int int_val = INT32_MAX;
        float float_val = std::numeric_limits<float>::max();
        switch(type) {
            case ColType::TYPE_INT: {
                memcpy(key + offset, reinterpret_cast<char*>(&int_val), len);
            } break;
            case ColType::TYPE_FLOAT: {
                memcpy(key + offset, reinterpret_cast<char*>(&float_val), len);
            } break;
            case ColType::TYPE_STRING: {
                std::fill(key + offset, key + offset + len, 0xFF);
            } break;
            default:
                std::cout << "no match type in index\n";
                break;
        }
    }

    void setMinKey(char* key, int offset, int len, ColType type) {
        int int_val = INT32_MIN;
        float float_val = std::numeric_limits<float>::min();
        switch(type) {
            case ColType::TYPE_INT: {
                memcpy(key + offset, reinterpret_cast<char*>(&int_val), len);
            } break;
            case ColType::TYPE_FLOAT: {
                memcpy(key + offset, reinterpret_cast<char*>(&float_val), len);
            } break;
            case ColType::TYPE_STRING: {
                std::fill(key + offset, key + offset + len, 0x00);
            } break;
            default:
                std::cout << "no match type in index\n";
                break;
        }
    }

    void beginTuple() override {
        check_runtime_conds();

            // CompOp right_op = OP_EQ;
        CompOp op = OP_EQ;
        char* min_key = new char[index_meta_.col_tot_len];
        char* max_key = new char[index_meta_.col_tot_len];
        int offset = 0;
        // lower 是第一条记录
        auto lower = pindex_handle_->leaf_begin();
        // upper 是最后一条记录的后面一条
        auto upper = pindex_handle_->leaf_end();
        // 整张表的记录查询范围是[leaf_begin, leaf_end)
        // TODO: 现有逻辑只支持一个字段一个condition，不支持一个字段多个condition
        int i;
        for(i = 0; i < (int)index_conds_.size() && i < index_meta_.col_num; ++i) {
            auto& col = index_meta_.cols[i];
            auto& cond = index_conds_[i];

            if(cond.op != OP_EQ) {
                op = cond.op;
                switch(op) {
                    case OP_GT: {
                        // min_key: the remained cols must be max_val
                        // max_key: the remained cols and the current col must be max_val
                        memcpy(min_key + offset, cond.rhs_val.raw->data, col.len);
                        setMaxKey(max_key, offset, col.len, col.type);
                        offset += col.len;
                        for(int j = i + 1; j < index_meta_.col_num; ++j) {
                            col = index_meta_.cols[j];
                            setMaxKey(min_key, offset, col.len, col.type);
                            setMaxKey(max_key, offset, col.len, col.type);
                            offset += col.len;
                        }
                        lower = pindex_handle_->upper_bound(min_key);
                        upper = pindex_handle_->upper_bound(max_key);
                        // std::cout << "lower_rid: {page_no=" << lower.page_no << ", slot_no=" << lower.slot_no << "}\n";
                        // std::cout << "upper_rid: {page_no=" << upper.page_no << ", slot_no=" << upper.slot_no << "}\n";
                    } break;
                    case OP_GE: {
                        // min_key: the remained cols must be min_val
                        // max_key: the remained cols and the current col must be max_val
                        memcpy(min_key + offset, cond.rhs_val.raw->data, col.len);
                        setMaxKey(max_key, offset, col.len, col.type);
                        offset += col.len;
                        for(int j = i + 1; j < index_meta_.col_num; ++j) {
                            col = index_meta_.cols[j];
                            setMinKey(min_key, offset, col.len, col.type);
                            setMaxKey(max_key, offset, col.len, col.type);
                            offset += col.len;
                        }
                        lower = pindex_handle_->lower_bound(min_key);
                        upper = pindex_handle_->upper_bound(max_key);
                    } break;
                    case OP_LT: {
                        // min_key: the remained cols and the current col must be min_val
                        // max_key: the remained cols must be min_val
                        setMinKey(min_key, offset, col.len, col.type);
                        memcpy(max_key + offset, cond.rhs_val.raw->data, col.len);
                        offset += col.len;
                        for(int j = i + 1; j < index_meta_.col_num; ++j) {
                            col = index_meta_.cols[j];
                            setMinKey(min_key, offset, col.len, col.type);
                            setMinKey(max_key, offset, col.len, col.type);
                            offset += col.len;
                        }
                        lower = pindex_handle_->lower_bound(min_key);
                        upper = pindex_handle_->lower_bound(max_key);
                    } break;
                    case OP_LE: {
                        // min_key: the remained cols and the current col must be min_val
                        // max_key: the remained cols must be max_val
                        setMinKey(min_key, offset, col.len, col.type);
                        memcpy(max_key + offset, cond.rhs_val.raw->data, col.len);
                        offset += col.len;
                        for(int j = i + 1; j < index_meta_.col_num; ++j) {
                            col = index_meta_.cols[j];
                            setMinKey(min_key, offset, col.len, col.type);
                            setMaxKey(max_key, offset, col.len, col.type);
                            offset += col.len;
                        }
                        lower = pindex_handle_->lower_bound(min_key);
                        upper = pindex_handle_->upper_bound(max_key);
                    } break;
                    default:
                        std::cout << "Invalid operator type.\n";
                    break;
                }
                break;
            }
            else {
                memcpy(min_key + offset, cond.rhs_val.raw->data, col.len);
                memcpy(max_key + offset, cond.rhs_val.raw->data, col.len);
                offset += col.len;
            }
        }

        if(op == OP_EQ) {
            for(; i < index_meta_.col_num; ++i) {
                auto& col = index_meta_.cols[i];
                setMinKey(min_key, offset, col.len, col.type);
                setMaxKey(max_key, offset, col.len, col.type);
                offset += col.len;
            }
            lower = pindex_handle_->lower_bound(min_key);
            upper = pindex_handle_->upper_bound(max_key);
        }
        /*
            赋值
        */
        lower_rid_ = lower;
        upper_rid_ = upper;

        scan_ = std::make_unique<IxScan>(pindex_handle_, lower, upper);

        // Get the first record
        auto lower_record = pindex_handle_->get_record(lower, context_);
        auto upper_record = pindex_handle_->get_record(upper, context_);

        // if(context_ == nullptr) return;
        assert(context_ != nullptr);

        // check the first record's lock type
        LockManager* lock_mgr = context_->lock_mgr_;
        Transaction* txn = context_->txn_;
        Lock* lock = nullptr;
        switch(op) {
            case OP_EQ: {
                if(index_meta_.col_num == (int)index_conds_.size()) {
                    // where id = x
                    if(lower == upper) {
                        // no record satisfies id = x, gap lock
                        lock = lock_mgr->request_record_lock(tab_.table_id_, lower, txn, RECORD_LOCK_GAP, get_lock_mode_for_plan(context_->plan_tag_), context_->coro_sched_->t_id_);
                    }
                    else {
                        // record id = x exists, record_not_gap lock
                        lock = lock_mgr->request_record_lock(tab_.table_id_, lower, txn, RECORD_LOCK_REC_NOT_GAP, get_lock_mode_for_plan(context_->plan_tag_), context_->coro_sched_->t_id_);
                    }
                    min_lock_ = max_lock_ = true;
                } else {
                    lock = lock_mgr->request_record_lock(tab_.table_id_, upper, txn, RECORD_LOCK_GAP, get_lock_mode_for_plan(context_->plan_tag_), context_->coro_sched_->t_id_);
                    max_lock_ = true;
                }
            } break;
            case OP_GT: {
                // where id > x
                // first record id > x, next-key lock
                lock = lock_mgr->request_record_lock(tab_.table_id_, lower, txn, RECORD_LOCK_ORDINARY, get_lock_mode_for_plan(context_->plan_tag_), context_->coro_sched_->t_id_);
                min_lock_ = true;
                assert(lock != nullptr);
                txn->append_lock(lock);
                // lock upper
                lock = lock_mgr->request_record_lock(tab_.table_id_, upper, txn, RECORD_LOCK_GAP, get_lock_mode_for_plan(context_->plan_tag_), context_->coro_sched_->t_id_);
                max_lock_ = true;
            } break;
            case OP_GE: {
                // where id >= x
                if(check_match(cols_, index_conds_, lower_record.get())) {
                    // if record id = x exists, record_not_gap lock
                    lock = lock_mgr->request_record_lock(tab_.table_id_, lower, txn, RECORD_LOCK_REC_NOT_GAP, get_lock_mode_for_plan(context_->plan_tag_), context_->coro_sched_->t_id_);
                }
                else {
                    // if record id = x not exists, next_key lock
                    lock = lock_mgr->request_record_lock(tab_.table_id_, lower, txn, RECORD_LOCK_ORDINARY, get_lock_mode_for_plan(context_->plan_tag_), context_->coro_sched_->t_id_);
                }
                min_lock_ = true;
                assert(lock != nullptr);
                txn->append_lock(lock);
                // lock upper
                lock = lock_mgr->request_record_lock(tab_.table_id_, upper, txn, RECORD_LOCK_GAP, get_lock_mode_for_plan(context_->plan_tag_), context_->coro_sched_->t_id_);
                max_lock_ = true;
            } break;
            case OP_LT: {
                // where id < x
                // first record id >= x, gap lock
                lock = lock_mgr->request_record_lock(tab_.table_id_, upper, txn, RECORD_LOCK_GAP, get_lock_mode_for_plan(context_->plan_tag_), context_->coro_sched_->t_id_);
                min_lock_ = true;
            } break;
            case OP_LE: {
                // where id <= x
                if(check_match(cols_, index_conds_, upper_record.get())) {
                    // if record id = x exists, next_key lock
                    lock = lock_mgr->request_record_lock(tab_.table_id_, upper, txn, RECORD_LOCK_ORDINARY, get_lock_mode_for_plan(context_->plan_tag_), context_->coro_sched_->t_id_);
                }
                else {
                    // if record id = x not exists, gap lock
                    lock = lock_mgr->request_record_lock(tab_.table_id_, upper, txn, RECORD_LOCK_GAP, get_lock_mode_for_plan(context_->plan_tag_), context_->coro_sched_->t_id_);
                }
                max_lock_ = true;
            } break;
            default:
                std::cout << "Invalid oprator in IndexScan\n";
                break;
        }
        assert(lock != nullptr);
        txn->append_lock(lock);

        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            // auto [is_visible, rec] = mvcc_get_record(rid_);
            // 如果不可见
            // if(!is_visible) {
            //     continue;
            // }

            // if (eval_conds(cols_, index_conds_, rec.get()) && rec->is_deleted() == false) {
            //     current_record_ = std::move(rec);
            auto rec = pindex_handle_->get_record(rid_, context_);
            
            if (rec->is_deleted() == false && eval_conds(cols_, filter_conds_, rec.get())) {
                if(min_lock_ == false) {
                    lock = lock_mgr->request_record_lock(tab_.table_id_, rid_, txn, RECORD_LOCK_ORDINARY, get_lock_mode_for_plan(context_->plan_tag_), context_->coro_sched_->t_id_);
                    assert(lock != nullptr);
                    txn->append_lock(lock);
                }
                else {
                    min_lock_ = false;
                }
                current_record_ = std::move(rec);
                break;
            }
            else {
                if(min_lock_ == false) {
                    lock = lock_mgr->request_record_lock(tab_.table_id_, rid_, txn, RECORD_LOCK_ORDINARY, LOCK_S, context_->coro_sched_->t_id_);
                    assert(lock != nullptr);
                    txn->append_lock(lock);
                }
                else {
                    min_lock_ = false;
                }
            }
            scan_->next();
        }
    }

    bool check_match_for_key(const std::vector<ColMeta> &rec_cols, const Condition &cond, const Record *rec) {
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
        return cmp == 0;
    }

    bool check_match(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds,
                           const Record *rec) {
        for(size_t i = 0; i < conds.size(); ++i) {
            if(!check_match_for_key(rec_cols, conds[i], rec)) return false;
            if(conds[i].op != OP_EQ) return true;
        }
        return true;
    }

    void nextTuple() override {
        check_runtime_conds();
        assert(!is_end());
        Lock* lock = nullptr;
        for (scan_->next(); !scan_->is_end(); scan_->next()) {
            rid_ = scan_->rid();
            // auto [is_visible, rec] = mvcc_get_record(rid_);
            // 如果不可见
            // if(!is_visible) {
            //     continue;
            // }
            // if (eval_conds(cols_, index_conds_, rec.get())&& rec->is_deleted() == false) {
            //     current_record_ = std::move(rec);
            auto rec = pindex_handle_->get_record(rid_, context_);

            if (rec->is_deleted() == false && eval_conds(cols_, filter_conds_, rec.get())) {
                assert(context_ != nullptr);
                lock = context_->lock_mgr_->request_record_lock(tab_.table_id_, rid_, context_->txn_, RECORD_LOCK_ORDINARY, get_lock_mode_for_plan(context_->plan_tag_), context_->coro_sched_->t_id_);
                assert(lock != nullptr);
                context_->txn_->append_lock(lock);
                current_record_ = std::move(rec);
                break;
            }
            else {
                assert(context_ != nullptr);
                lock = context_->lock_mgr_->request_record_lock(tab_.table_id_, rid_, context_->txn_, RECORD_LOCK_ORDINARY, LOCK_S, context_->coro_sched_->t_id_);
                assert(lock != nullptr);
                context_->txn_->append_lock(lock);
            }
        }
    }

    bool is_end() const override { return scan_->is_end(); }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

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

    std::unique_ptr<Record> Next() override {
        assert(!is_end());
        // return pindex_handle_->get_record(rid_, context_);
        // 如果使用std::move，调用一次Next，current_record就为空了
        // return std::move(current_record_);
        return std::make_unique<Record>(*current_record_);   // 复制构造，代价稍微高一些
    }

    Rid &rid() override { return rid_; }

    void check_runtime_conds() {
        // for (auto &cond : index_conds_) {
        //     assert(cond.lhs_col.tab_name == tab_name_);
        //     if (!cond.is_rhs_val) {
        //         assert(cond.rhs_col.tab_name == tab_name_);
        //     }
        // }
    }

    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const Record *rec) {
        if(rec->is_deleted()) { return false; }
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

    bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds,
                           const Record *rec) {
        return std::all_of(conds.begin(), conds.end(),
                           [&](const Condition &cond) { return eval_cond(rec_cols, cond, rec); });
    }

    int checkpoint(char* dest) override {
        // store the current IxScan
        int offset = 0;
        memcpy(dest + offset, &scan_->rid(), sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &scan_->end(), sizeof(Rid));
        offset += sizeof(Rid);
        
        return offset;
    }

    std::pair<bool, std::unique_ptr<Record>> mvcc_get_record(Rid rid) {
        auto rec = pindex_handle_->get_record(rid_, context_);
        RecordHdr *record_hdr = (RecordHdr*)rec->record_;
        if(ReadView::read_view_sees_trx_id(context_->txn_->get_read_view(), record_hdr->trx_id_)) {
            return {true, std::move(rec)};
        }
        while(true) {
            Rid old_rid{.page_no = record_hdr->rollback_page_no_, .slot_no = record_hdr->rollback_slot_no_};
            if(old_rid.page_no == INVALID_PAGE_ID) {
                return {false, nullptr};
            }
            auto old_record = old_version_handle_->get_record(old_rid, context_);
            record_hdr = (RecordHdr*)old_record->data;
            if(ReadView::read_view_sees_trx_id(context_->txn_->get_read_view(), record_hdr->trx_id_)){
                return {true, std::make_unique<Record>(old_record->data, old_record->size)};
            }
        }
        return {false, nullptr};
    }


    /*
        从state中恢复，类似beginTuple
    */
    void load_state_info(IndexScanOperatorState  *index_scan_op_state) {
        /*
            如果是从IndexScanOperatorState中恢复的，那么设置load from state = true
        */
        if(index_scan_op_state != nullptr) {
            load_from_state_ = true;
            index_scan_op_ = std::move(index_scan_op_state);
                // Rid rid_;
                // std::unique_ptr<IxScan> scan_;
                // std::unique_ptr<Record> current_record_;    // currrent record

                // SmManager *sm_manager_;

                // bool min_lock_ = false;
                // bool max_lock_ = false;

                // Rid lower_rid_;
                // Rid upper_rid_;
            rid_ = index_scan_op_->current_rid_;
            lower_rid_ = index_scan_op_->lower_rid_;
            upper_rid_ = index_scan_op_->upper_rid_;

            scan_ = std::make_unique<IxScan>(pindex_handle_, rid_, upper_rid_);
            current_record_ = pindex_handle_->get_record(rid_, context_);
        } else {
            load_from_state_ = false;
            index_scan_op_ = nullptr;
        }
        return ;
    }
};
