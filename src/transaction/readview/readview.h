#pragma once

#include <iostream>
#include <vector>
#include "transaction/txn_defs.h"

struct ReadView {
    txn_id_t creator_txn_id_; // 记录创建readview的txn_id
    int visible_lsn_;           // used for read-only transactions to get consistent pages
    std::vector<txn_id_t> active_txn_ids_;  // 记录创建creadview时的活跃事务列表
    txn_id_t up_limit_id_;  // 记录活跃事务列表中的最小值
    txn_id_t low_limit_id_;  // 记录事务管理器要分配的下一个事务的值，大于活跃事务列表中的所有值

    ReadView() {
        up_limit_id_ = low_limit_id_ = INVALID_TXN_ID;
        active_txn_ids_.clear();
        visible_lsn_ = INVALID_LSN;
        creator_txn_id_ = INVALID_TXN_ID;
    }
    ReadView(txn_id_t creator_txn_id, std::vector<txn_id_t> txn_ids, txn_id_t up_limit_id, txn_id_t low_limit_id) {
        this->creator_txn_id_ = creator_txn_id;
        this->active_txn_ids_ = txn_ids;
        this->up_limit_id_ = up_limit_id;
        this->low_limit_id_ = low_limit_id;
    }

    void clear() {
        active_txn_ids_.clear();
        up_limit_id_ = low_limit_id_ = INVALID_TXN_ID;
        visible_lsn_ = INVALID_LSN;
        creator_txn_id_ = INVALID_TXN_ID;
    }

    void udpate_readview(txn_id_t creator_txn_id, std::vector<txn_id_t> txn_ids, txn_id_t up_limit_id, txn_id_t low_limit_id) {
        this->creator_txn_id_ = creator_txn_id;
        this->active_txn_ids_ = txn_ids;
        this->up_limit_id_ = up_limit_id;
        this->low_limit_id_ = low_limit_id;
    }

    static bool read_view_sees_trx_id(std::shared_ptr<ReadView> readview, txn_id_t txn_id) {
        if(txn_id == readview->creator_txn_id_) {
            return true;
        }else if(txn_id < readview->up_limit_id_) {
            return true;
        }else if(txn_id >= readview->low_limit_id_) {
            return false;
        }else {
            // 查找readview->active_txn_ids_中txn_id是否存在
            // 优化：使用二分查找
            for (txn_id_t id : readview->active_txn_ids_) {
                if (id == txn_id) {
                    return false;
                }
            }
        }
        return true;
    }
};


