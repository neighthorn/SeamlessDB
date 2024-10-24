#include "comp_ckpt_mgr.h"

#include "state/state_item/op_state.h"
#include "state/op_state_manager.h"
#include "executor_block_join.h"
#include "execution_sort.h"
#include "executor_hash_join.h"
#include "executor_projection.h"
#include "executor_index_scan.h"
#include <cmath>
#include <limits>

CompCkptManager* CompCkptManager::ckpt_mgr_ = nullptr;

bool CompCkptManager::create_instance() {
    ckpt_mgr_ = new CompCkptManager();
    if(ckpt_mgr_ == nullptr) return true;
    return false;
}

void CompCkptManager::destroy_instance() {
    delete ckpt_mgr_;
    ckpt_mgr_ = nullptr;
}

void CompCkptManager::update_operator_ancestors(std::shared_ptr<AbstractExecutor> op, std::deque<std::shared_ptr<AbstractExecutor>>& ancestors) {
    operators_[op->operator_id_] = std::make_unique<Operator>(op);
    auto curr_op = operators_.find(op->operator_id_);

    // 从0到size()-1，分别是parent到root
    for(int i = ancestors.size() - 1; i >= 0; --i) {
        curr_op->second->ancestors_.push_back(ancestors.at(i));
    }
    ancestors.push_back(op);

    if(auto x = dynamic_cast<ProjectionExecutor *>(op.get())) {
        update_operator_ancestors(x->prev_, ancestors);
    }
    else if(auto x = dynamic_cast<SortExecutor *>(op.get())) {
        update_operator_ancestors(x->prev_, ancestors);
    }
    else if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(op.get())) {
        update_operator_ancestors(x->left_, ancestors);
        update_operator_ancestors(x->right_, ancestors);
    }
    else if(auto x = dynamic_cast<HashJoinExecutor *>(op.get())) {
        update_operator_ancestors(x->left_, ancestors);
        update_operator_ancestors(x->right_, ancestors);
    }

    ancestors.pop_back();
}

void CompCkptManager::add_new_query_tree(std::shared_ptr<AbstractExecutor> root_op) {
    operators_.clear();
    std::deque<std::shared_ptr<AbstractExecutor>> current_ops;
    current_ops.push_back(root_op);
    operators_[root_op->operator_id_] = std::make_unique<Operator>(root_op);
    update_operator_ancestors(dynamic_cast<ProjectionExecutor *>(root_op.get())->prev_, current_ops);
    current_ops.pop_back();
}

CompCkptManager::CompCkptManager() {
    last_ckpt_time_ = std::chrono::high_resolution_clock::now();
}

void CompCkptManager::update_operator_cost() {

}

void CompCkptManager::solve_mip(OperatorStateManager* op_state_mgr) {
    if(cost_model_ == 2) {
        auto curr_time = std::chrono::high_resolution_clock::now();
        if(std::chrono::duration_cast<std::chrono::seconds>(curr_time - last_ckpt_time_).count() >= interval_) {
            create_ckpts(op_state_mgr);
            std::cout << "IntervalCkpt: create ckpt, interval: " << interval_ << "\n";
            last_ckpt_time_ = curr_time;
        }
        return;
    }

    
    if(interval_ > 0) {
        // interval = 0 代表每次状态改变都需要implement cost model
        // interval > 0 代表每隔interval(s)implement cost model
        auto curr_time = std::chrono::high_resolution_clock::now();
        if(std::chrono::duration_cast<std::chrono::seconds>(curr_time - last_ckpt_time_).count() < interval_) {
            return;
        }
        else {
            last_ckpt_time_ = curr_time;
        }
    }
    int n = operators_.size();
    double min_cost = std::numeric_limits<double>::max();
    std::vector<int> best_solutions;
    std::chrono::time_point<std::chrono::system_clock> curr_time = std::chrono::high_resolution_clock::now();
    int valid_sol_cnt = 0;

    for(int i = 0; i < std::pow(2, n); ++i) {
        std::vector<int> current_solutions(n, 0);
        double current_cost = 0.0;
        bool valid_solution = true;

        for(int j = n - 1; j >= 0; j--) {
            if(i & (1 << j)) {
                // 节点j选择了go back
                current_solutions[j] = 1;
                bool find_go_back_anc = false;

                auto curr_op = operators_.find(j);
                for(auto anc: curr_op->second->ancestors_) {
                    // 找到最近的op的 ancestor.parent choose Dumpstate or root
                    if(auto x = dynamic_cast<ProjectionExecutor *>(anc.get())) {
                        if(x->is_root_) {
                            auto latest_anc_ckpt_time = anc->get_latest_ckpt_time();
                            current_cost += std::chrono::duration_cast<std::chrono::microseconds>(curr_time - latest_anc_ckpt_time).count();
                            find_go_back_anc = true;
                            break;    
                        }
                        else {
                            auto anc_operator = operators_.find(anc->operator_id_);
                            // ancestor的parent
                            auto anc_par = anc_operator->second->ancestors_[0];
                            // 如果ancestor的parent选择了DumpState
                            if(current_solutions[anc_par->operator_id_] == 0) {
                                // resume cost应该是重做到anc 最近一个检查点的开销
                                auto latest_anc_ckpt_time = anc->get_latest_ckpt_time();
                                current_cost += std::chrono::duration_cast<std::chrono::microseconds>(curr_time - latest_anc_ckpt_time).count();
                                find_go_back_anc = true;
                                break;
                            }
                        }
                    }
                    else {
                        auto anc_operator = operators_.find(anc->operator_id_);
                        auto anc_par = anc_operator->second->ancestors_[0];
                        if(current_solutions[anc_par->operator_id_] == 0) {
                            // resume cost应该是重做到anc 最近一个检查点的开销
                            auto latest_anc_ckpt_time = anc->get_latest_ckpt_time();
                            current_cost += std::chrono::duration_cast<std::chrono::microseconds>(curr_time - latest_anc_ckpt_time).count();
                            find_go_back_anc = true;
                            break;
                        }
                    }
                    
                }

                if(find_go_back_anc == false && !dynamic_cast<IndexScanExecutor *>(curr_op->second->current_op_.get())) {
                    current_cost += std::chrono::duration_cast<std::chrono::microseconds>(curr_time - curr_op->second->current_op_->get_latest_ckpt_time()).count();
                }

                // current_cost += operators_.find(j)->second->resume_cost_;
            }
            else {
                // 节点j选择了dumpstate
                current_solutions[j] = 0;

                auto curr_op = operators_.find(j);
                // 剪枝条件：如果算子是stateless算子，则如果其父亲选择了go back，那么该算子也必须选择go back
                if(auto x = dynamic_cast<ProjectionExecutor *>(curr_op->second->current_op_.get())) {
                    if(!x->is_root_) {
                        auto par = curr_op->second->ancestors_[0];
                        if(current_solutions[par->operator_id_] == 1) {
                            valid_solution = false;
                            break;
                        }
                    }
                }
                else if(auto x = dynamic_cast<IndexScanExecutor *>(curr_op->second->current_op_.get())) {
                    auto par = curr_op->second->ancestors_[0];
                    if(current_solutions[par->operator_id_] == 1) {
                        valid_solution = false;
                        break;
                    }
                }

                if(valid_solution == true)
                    current_cost += curr_op->second->current_op_->get_curr_suspend_cost();
                else break;
            }
        }

        valid_sol_cnt += valid_solution == true;
        if(valid_solution && current_cost < min_cost) {
            min_cost = current_cost;
            best_solutions = current_solutions;
        }
    }
    // std::cout << "operator cnt: " << n << ", solution cnt: " << pow(2, n) << ", valid_solution cnt: " << valid_sol_cnt << "\n";

    create_ckpts(best_solutions);
}

void CompCkptManager::create_ckpts(const std::vector<int>& best_solutions) {
    for(const auto& op: operators_) {
        if(best_solutions[op.second->current_op_->operator_id_] == 0) {
            if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(op.second->current_op_.get())) {
                if(!x->is_end())
                    x->write_state();
            }
            else if(auto x = dynamic_cast<HashJoinExecutor *>(op.second->current_op_.get())) {
                if(!x->is_end())
                    x->write_state();
            }
            else if(auto x = dynamic_cast<SortExecutor *>(op.second->current_op_.get())) {
                if(!x->is_end())
                    x->write_state();
            }
            else if(auto x = dynamic_cast<ProjectionExecutor *>(op.second->current_op_.get())) {
                if(!x->is_end())
                    x->write_state();
            }
        }
    }
}

void CompCkptManager::create_ckpts(OperatorStateManager* op_state_mgr) {
    op_state_mgr->clear_op_meta();
    for(const auto& op: operators_) {
        if(auto x = dynamic_cast<BlockNestedLoopJoinExecutor *>(op.second->current_op_.get())) {
            if(!x->is_end())
                x->write_state();
        }
        else if(auto x = dynamic_cast<HashJoinExecutor *>(op.second->current_op_.get())) {
            if(!x->is_end())
                x->write_state();
        }
        else if(auto x = dynamic_cast<SortExecutor *>(op.second->current_op_.get())) {
            if(!x->is_end())
                x->write_state();
        }
        else if(auto x = dynamic_cast<ProjectionExecutor *>(op.second->current_op_.get())) {
            if(!x->is_end())
                x->write_state();
        }
    }
}