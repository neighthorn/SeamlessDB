#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/context.h"
#include "execution_defs.h"
#include "system/sm.h"
#include "common/common.h"
#include "optimizer/plan.h"
#include "executor_abstract.h"
#include "transaction/transaction_manager.h"


class QlManager {
   private:
    SmManager *sm_manager_;
    TransactionManager *txn_mgr_;

   public:
    QlManager(SmManager *sm_manager, TransactionManager *txn_mgr) 
        : sm_manager_(sm_manager),  txn_mgr_(txn_mgr) {}

    void run_mutli_query(std::shared_ptr<Plan> plan, Context *context);
    void run_cmd_utility(std::shared_ptr<Plan> plan, Context *context);
    void select_from(std::shared_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                        Context *context);
    void re_run_select_from(std::shared_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols, 
                        Context *context);
    void run_dml(std::shared_ptr<AbstractExecutor> exec);

};
