#include "planning.h"

#include <math.h>

#include <cmath>
#include <memory>
#include <string>

#include "generic/database.h"
#include "sql/expr.h"
#include "sql/statements/select.h"

namespace {
enum class MermaidNodeType {
  kCircle,
  kRectangle,
  kRhombus,
  kRectangleRounded,
};

void createMermaidNode(std::string& result, const std::string& name, const std::string& expr,
                       MermaidNodeType type = MermaidNodeType::kRectangle) {
  result += "  " + name;
  switch (type) {
    case MermaidNodeType::kCircle:
      result += "((\"" + expr + "\"))";
      break;
    case MermaidNodeType::kRectangle:
      result += "[\"" + expr + "\"]";
      break;
    case MermaidNodeType::kRhombus:
      result += "{\"" + expr + "\"}";
      break;
    case MermaidNodeType::kRectangleRounded:
      result += "(\"" + expr + "\")";
      break;
    default:
      result += "[\"" + expr + "\"]";
  }
  result += "\n";
}

void connectMermaidNodes(std::string& result, const std::string& from, const std::string& to) {
  result += "  " + from + " --> " + to + "\n";
}

size_t log2(size_t x) {
  if (x == 0) return 0;
  return static_cast<size_t>(std::ceil(std::log2(x)));
}

}  // namespace

namespace csql {
namespace storage {

std::shared_ptr<QueryPlan> QueryPlan::create(std::shared_ptr<Expr> query,
                                             std::shared_ptr<Database> db) {
  std::shared_ptr<QueryPlan> plan;
  switch (query->type) {
    case kExprSelect: {
      std::shared_ptr<SelectStatement> select = query->select;
      plan = std::make_shared<QueryPlan>(QueryType::kStepEval, query, db);
      plan->left_ = std::make_shared<QueryPlan>(QueryType::kStepFilter, select->whereClause, db);
      plan->left_->left_ =
          std::make_shared<QueryPlan>(QueryType::kStepFullScan, select->fromSource, db);
      plan->left_->left_->left_ = create(select->fromSource, db);
    } break;
    case kExprJoin: {
      plan = std::make_shared<QueryPlan>(QueryType::kStepJoin, query, db);
      plan->left_ = create(query->expr, db);
      plan->right_ = create(query->expr2, db);
    } break;
    case kExprTableRef: {
      plan = std::make_shared<QueryPlan>(QueryType::kStepProject, query, db);
    } break;
    case kExprOperator: {
      if (query->opType == OperatorType::kOpParenthesis) {
        plan = create(query->expr, db);
      } else {
        throw std::runtime_error("Unsupported expression type: " + std::to_string(query->type));
      }
    } break;
    default:
      throw std::runtime_error("Unsupported expression type: " + std::to_string(query->type));
  }
  plan->calculateCost();
  return plan;
}

void makeMermaid(std::string& result, const csql::storage::QueryPlan& plan,
                 const std::string& name) {
  std::string left_name = name + "L";
  std::string right_name = name + "R";
  MermaidNodeType type = MermaidNodeType::kRectangle;

  if (plan.type_ == QueryType::kStepJoin) {
    createMermaidNode(result, name, "Join", MermaidNodeType::kRectangleRounded);
  } else if (plan.type_ == QueryType::kStepFilter) {
    createMermaidNode(result, name, "Filter", MermaidNodeType::kRectangleRounded);
  } else if (plan.type_ == QueryType::kStepHashMerge) {
    createMermaidNode(result, name, "HashMerge", MermaidNodeType::kRectangleRounded);
  } else if (plan.type_ == QueryType::kStepSort) {
    createMermaidNode(result, name, "Sort", MermaidNodeType::kRectangleRounded);
  } else if (plan.type_ == QueryType::kStepEval) {
    createMermaidNode(result, name, "Eval", MermaidNodeType::kRectangleRounded);
  } else if (plan.type_ == QueryType::kStepProject) {
    createMermaidNode(result, name, "Project: " + plan.query_->name, MermaidNodeType::kRectangle);
  } else if (plan.type_ == QueryType::kStepFullScan) {
    createMermaidNode(result, name, "FullScan", MermaidNodeType::kCircle);
  } else if (plan.type_ == QueryType::kStepRangeScan) {
    createMermaidNode(result, name, "RangeScan", MermaidNodeType::kCircle);
  } else {
    createMermaidNode(result, name, "\"Unknown\"", type);
  }
  if (plan.left_) {
    makeMermaid(result, *plan.left_, left_name);
    connectMermaidNodes(result, left_name, name);
  }
  if (plan.right_) {
    makeMermaid(result, *plan.right_, right_name);
    connectMermaidNodes(result, right_name, name);
  }
}

std::string QueryPlan::toString() const {
  switch (type_) {
    case QueryType::kStepFullScan:
      return "FullScan";
    case QueryType::kStepRangeScan:
      return "RangeScan";
    case QueryType::kStepJoin:
      return "Join";
    case QueryType::kStepHashMerge:
      return "HashMerge";
    case QueryType::kStepSort:
      return "Sort";
    case QueryType::kStepFilter:
      return "Filter";
    case QueryType::kStepEval:
      return "Eval";
    case QueryType::kStepProject:
      return "Project: " + query_->name;
    default:
      return "Unknown";
  }
}

std::string QueryPlan::toMermaid(const std::string& name) const {
  std::string result = "graph TD\n";
  makeMermaid(result, *this, name);
  return result;
}

Cost QueryPlan::calculateCost() {
  if (type_ == QueryType::kStepJoin) {
    auto join_expr = query_;
    if (join_expr->type != kExprJoin) {
      throw std::runtime_error("Expected join expression");
    }
    auto left = left_->getCost();
    auto right = right_->getCost();
    if (join_expr->opType == OperatorType::kOpInnerJoin ||
        join_expr->opType == OperatorType::kOpCrossJoin) {
      cost_ = Cost{
          .total_steps = left.total_steps + left.amount * right.total_steps,
          .self_steps = 0,
          .amount = left.amount + right.amount,
      };
    } else if (join_expr->opType == OperatorType::kOpLeftJoin) {
      cost_ = Cost{
          .total_steps = left.total_steps + left.amount * right.total_steps,
          .self_steps = 0,
          .amount = left.amount,
      };
    } else if (join_expr->opType == OperatorType::kOpRightJoin) {
      cost_ = Cost{
          .total_steps = left.total_steps + left.amount * right.total_steps,
          .self_steps = 0,
          .amount = right.amount,
      };
    } else if (join_expr->opType == OperatorType::kOpOuterJoin) {
      cost_ = Cost{
          .total_steps = left.total_steps + left.amount * right.total_steps,
          .self_steps = 0,
          .amount = left.amount * right.amount,
      };
    } else {
      throw std::runtime_error("Unsupported join type");
    }
  } else if (type_ == QueryType::kStepHashMerge) {
    auto left = left_->getCost();
    auto right = right_->getCost();
    cost_ = Cost{
        .total_steps = left.total_steps + right.total_steps,
        .self_steps = 0,
        .amount = left.amount + right.amount,
    };
  } else if (type_ == QueryType::kStepSort) {
    auto left = left_->getCost();
    cost_ = Cost{
        .total_steps = left.total_steps + left.amount * log2(left.amount),
        .self_steps = left.amount * log2(left.amount),
        .amount = left.amount,
    };
  } else if (type_ == QueryType::kStepFilter) {
    auto left = left_->getCost();
    // TODO: Implement better prediction by evaluating the where clause expression
    cost_ = Cost{
        .total_steps = left.total_steps,
        .self_steps = 0,
        .amount = left.amount,
    };
  } else if (type_ == QueryType::kStepEval) {
    auto left = left_->getCost();
    cost_ = Cost{
        .total_steps = left.total_steps,
        .self_steps = 0,
        .amount = left.amount,
    };
  } else if (type_ == QueryType::kStepFullScan) {
    auto left = left_->getCost();
    cost_ = Cost{
        .total_steps = left.total_steps + left.amount,
        .self_steps = 0,
        .amount = left.amount,
    };
  } else if (type_ == QueryType::kStepRangeScan) {
    auto left = left_->getCost();
    cost_ = Cost{
        .total_steps = left.total_steps + left.amount,
        .self_steps = 0,
        .amount = left.amount,
    };
  } else if (type_ == QueryType::kStepProject) {
    auto table = std::dynamic_pointer_cast<StorageTable>(db_.lock()->getTable(query_));
    if (!table) {
      throw std::runtime_error("Expected storage table");
    }
    cost_ = Cost{
        .total_steps = 0,
        .self_steps = 0,
        .amount = table->getRowsCount(),
    };
  } else {
    throw std::runtime_error("Unsupported query type");
  }
  return cost_;
}

const Cost& QueryPlan::getCost() const {
  return cost_;
}

}  // namespace storage
}  // namespace csql