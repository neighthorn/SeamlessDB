#pragma once

#include <assert.h>
#include <unordered_map>
#include <deque>
#include <chrono>

#include "execution/execution_defs.h"
#include "execution/executor_abstract.h"
#include "common/config.h"
#include "state/op_state_manager.h"

class Operator{
public:
    Operator(std::shared_ptr<AbstractExecutor> op) {
        current_op_ = op;
        op_id_ = op->operator_id_;
    }

    int op_id_;
    std::shared_ptr<AbstractExecutor> current_op_;
    std::vector<std::shared_ptr<AbstractExecutor>> ancestors_;
    double suspend_cost_;
    double resume_cost_;
};

class CompCkptManager {
public:
    std::unordered_map<int, std::unique_ptr<Operator>> operators_;

    CompCkptManager();
    ~CompCkptManager() {}
    
    static bool create_instance();
    static void destroy_instance();
    static CompCkptManager* get_instance() {
        assert(ckpt_mgr_ != nullptr);
        return ckpt_mgr_;
    }

    void add_new_query_tree(std::shared_ptr<AbstractExecutor> root_op);
    void update_operator_ancestors(std::shared_ptr<AbstractExecutor> op, std::deque<std::shared_ptr<AbstractExecutor>>& ancestors);
    void update_operator_cost();
    void solve_mip(OperatorStateManager* op_state_mgr);
    void create_ckpts(const std::vector<int>& best_solutions);
    void create_ckpts(OperatorStateManager* op_state_mgr_);

    static CompCkptManager* ckpt_mgr_;
    std::chrono::time_point<std::chrono::system_clock> last_ckpt_time_;
};