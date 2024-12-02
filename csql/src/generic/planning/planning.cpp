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
  result += "  ";
  switch (type) {
    case MermaidNodeType::kCircle:
      result += name + "((" + expr + "))";
      break;
    case MermaidNodeType::kRectangle:
      result += name + "[" + expr + "]";
      break;
    case MermaidNodeType::kRhombus:
      result += name + "{" + expr + "}";
      break;
    case MermaidNodeType::kRectangleRounded:
      result += name + "(" + expr + ")";
      break;
    default:
      result += name + "[" + expr + "]";
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
      plan = std::make_shared<QueryPlan>(QueryType::kStepFilter, query, db);
      std::shared_ptr<SelectStatement> select = query->select;
      plan->left_ = create(select->fromSource, db);
      plan->scanType_ = ScanType::kScanFullTable;  // only full table scan is supported
    } break;
    case kExprJoin: {
      plan = std::make_shared<QueryPlan>(QueryType::kStepJoin, query, db);
      plan->left_ = create(query->expr, db);
      plan->right_ = create(query->expr2, db);
      plan->scanType_ = ScanType::kScanFullTable;  // only full of both tables is supported
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
    createMermaidNode(result, name,
                      "\"Join<br>Cost: " + std::to_string(plan.getCost().self_steps) +
                          "<br>Amount: " + std::to_string(plan.getCost().amount) + "\"",
                      MermaidNodeType::kRectangleRounded);
  } else if (plan.type_ == QueryType::kStepFilter) {
    createMermaidNode(result, name,
                      "\"Filter<br>Cost: " + std::to_string(plan.getCost().self_steps) + "\"",
                      MermaidNodeType::kRectangleRounded);
  } else if (plan.type_ == QueryType::kStepProject) {
    createMermaidNode(result, name,
                      "\"Project:" + plan.query_->name +
                          "<br> Amount: " + std::to_string(plan.getCost().amount) + "\"",
                      MermaidNodeType::kRectangle);
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

std::string QueryPlan::toMermaid(const std::string& name) const {
  std::string result = "graph TD\n";
  makeMermaid(result, *this, name);
  createMermaidNode(result, "TOTAL", "\"Total cost: " + std::to_string(cost_.total_steps) + "\"",
                    MermaidNodeType::kCircle);
  connectMermaidNodes(result, name, "TOTAL");
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
          .total_steps = left.amount * right.amount + left.total_steps + right.total_steps,
          .self_steps = left.amount * right.amount,
          .amount = left.amount + right.amount,
      };
    } else if (join_expr->opType == OperatorType::kOpLeftJoin) {
      cost_ = Cost{
          .total_steps = left.amount * right.amount + left.total_steps + right.total_steps,
          .self_steps = left.amount * right.amount,
          .amount = left.amount,
      };
    } else if (join_expr->opType == OperatorType::kOpRightJoin) {
      cost_ = Cost{
          .total_steps = left.amount * right.amount + left.total_steps + right.total_steps,
          .self_steps = left.amount * right.amount,
          .amount = right.amount,
      };
    } else if (join_expr->opType == OperatorType::kOpOuterJoin) {
      cost_ = Cost{
          .total_steps = left.amount * right.amount + left.total_steps + right.total_steps,
          .self_steps = left.amount * right.amount,
          .amount = left.amount * right.amount,
      };
    } else {
      throw std::runtime_error("Unsupported join type");
    }
  } else if (type_ == QueryType::kStepHashMerge) {
    auto left = left_->getCost();
    auto right = right_->getCost();
    cost_ = Cost{
        .total_steps = left.amount + right.amount + left.total_steps + right.total_steps,
        .self_steps = left.amount + right.amount,
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
        .total_steps = left.total_steps + left.amount,
        .self_steps = left.amount,
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