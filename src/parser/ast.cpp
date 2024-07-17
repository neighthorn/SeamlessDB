#include "ast.h"

namespace ast {

// std::shared_ptr<TreeNode> parse_tree;
thread_local std::shared_ptr<ast::TreeNode> parse_tree;
}
