#pragma once

#include "compute_pool/rw_server.h"

std::shared_ptr<PortalStmt> rebuild_exec_plan_from_state(RWNode *node, Context *context, SQLState *sql_state, CheckPointMeta *op_ck_meta, std::vector<std::unique_ptr<OperatorState>> &op_checkpoints);
std::shared_ptr<PortalStmt> rebuild_exec_plan_without_state(RWNode *node, Context *context, SQLState *sql_state);
