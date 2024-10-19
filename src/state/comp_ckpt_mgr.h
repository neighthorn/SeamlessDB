#include "state_item/op_state.h"
#include "op_state_manager.h"
#include "execution/execution_defs.h"
#include "execution/executor_abstract.h"
#include "portal.h"

struct Operator{
    int op_id;
    std::vector<int> ancestors_;
    std::vector<double> gs_;
    std::vector<double> gr_;
};

class CompCkptManager {
    std::shared_ptr<PortalStmt> portal_stmt_;
    
};