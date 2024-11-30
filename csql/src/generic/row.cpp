#include "row.h"

#include <memory>

#include "column.h"
#include "row.h"
#include "sql/expr.h"
#include "table.h"

namespace {

std::shared_ptr<csql::Expr> applyUnaryOpToLiteral(csql::OperatorType opType,
                                                  std::shared_ptr<csql::Expr> operand) {
  if (!operand->isLiteral()) {
    throw std::runtime_error("Expected literal");
  }

  if (opType == csql::OperatorType::kOpParenthesis) return operand;
  if (operand->isType(csql::ExprType::kExprLiteralNull)) {
    return csql::Expr::makeNullLiteral();
  }
  if (operand->isType(csql::ExprType::kExprLiteralInt)) {
    if (opType == csql::OperatorType::kOpUnaryMinus) {
      return csql::Expr::makeLiteral(-operand->ival);
    }
    if (opType == csql::OperatorType::kOpNot) return csql::Expr::makeLiteral(operand->ival == 0);
    if (opType == csql::OperatorType::kOpBitNot) return csql::Expr::makeLiteral(~operand->ival);
    if (opType == csql::OperatorType::kOpLength)
      throw std::runtime_error("Invalid operation: LENGTH on INT");
    if (opType == csql::OperatorType::kOpIsNull) return csql::Expr::makeLiteral(false);
  }
  if (operand->isType(csql::ExprType::kExprLiteralString)) {
    if (opType == csql::OperatorType::kOpUnaryMinus)
      throw std::runtime_error("Invalid operation: - on STRING");
    if (opType == csql::OperatorType::kOpNot) return csql::Expr::makeLiteral(operand->name.empty());
    if (opType == csql::OperatorType::kOpBitNot)
      throw std::runtime_error("Invalid operation: ~ on STRING");
    if (opType == csql::OperatorType::kOpLength)
      return csql::Expr::makeLiteral((int)operand->name.size());
    if (opType == csql::OperatorType::kOpIsNull) return csql::Expr::makeLiteral(false);
  }
  if (operand->isType(csql::ExprType::kExprLiteralBool)) {
    if (opType == csql::OperatorType::kOpUnaryMinus)
      throw std::runtime_error("Invalid operation: - on BOOL");
    if (opType == csql::OperatorType::kOpNot) return csql::Expr::makeLiteral(!operand->ival);
    if (opType == csql::OperatorType::kOpBitNot) return csql::Expr::makeLiteral(!operand->ival);
    if (opType == csql::OperatorType::kOpLength)
      throw std::runtime_error("Invalid operation: LENGTH on BOOL");
    if (opType == csql::OperatorType::kOpIsNull) return csql::Expr::makeLiteral(false);
  }
  if (operand->isType(csql::ExprType::kExprLiteralBytes)) {
    if (opType == csql::OperatorType::kOpUnaryMinus)
      throw std::runtime_error("Invalid operation: - on BYTES");
    if (opType == csql::OperatorType::kOpNot) return csql::Expr::makeLiteral(operand->name.empty());
    if (opType == csql::OperatorType::kOpBitNot) {
      std::vector<uint8_t> bytes(operand->name.size());
      for (size_t i = 0; i < operand->name.size(); i++) {
        bytes[i] = ~operand->name[i];
      }
      return csql::Expr::makeLiteral(bytes);
    }
    if (opType == csql::OperatorType::kOpLength)
      return csql::Expr::makeLiteral((int)operand->name.size());
    if (opType == csql::OperatorType::kOpIsNull) return csql::Expr::makeLiteral(false);
  }
  throw std::runtime_error("Invalid operation");
}

std::shared_ptr<csql::Expr> applyBinaryOpToLiterals(std::shared_ptr<csql::Expr> left,
                                                    csql::OperatorType opType,
                                                    std::shared_ptr<csql::Expr> right) {
  if (!left->isLiteral() || !right->isLiteral()) {
    throw std::runtime_error("Expected literals");
  }

  if (left->type != right->type) {
    if (opType == csql::OperatorType::kOpEquals) return csql::Expr::makeLiteral(false);
    if (opType == csql::OperatorType::kOpNotEquals) return csql::Expr::makeLiteral(true);
    return csql::Expr::makeLiteral(false);
  }

  if (left->isType(csql::ExprType::kExprLiteralNull)) {
    if (opType == csql::OperatorType::kOpEquals) return csql::Expr::makeLiteral(false);
    if (opType == csql::OperatorType::kOpNotEquals) return csql::Expr::makeLiteral(true);
    throw std::runtime_error("Invalid operation, NULLs can only be compared with = or !=");
  }
  if (left->isType(csql::ExprType::kExprLiteralString)) {
    if (opType == csql::OperatorType::kOpEquals)
      return csql::Expr::makeLiteral(left->name == right->name);
    if (opType == csql::OperatorType::kOpNotEquals)
      return csql::Expr::makeLiteral(left->name != right->name);
    if (opType == csql::OperatorType::kOpLess)
      return csql::Expr::makeLiteral(left->name < right->name);
    if (opType == csql::OperatorType::kOpLessEq)
      return csql::Expr::makeLiteral(left->name <= right->name);
    if (opType == csql::OperatorType::kOpGreater)
      return csql::Expr::makeLiteral(left->name > right->name);
    if (opType == csql::OperatorType::kOpGreaterEq)
      return csql::Expr::makeLiteral(left->name >= right->name);
    if (opType == csql::OperatorType::kOpPlus)
      return csql::Expr::makeStringLiteral(left->name + right->name);
    throw std::runtime_error("Invalid operation on strings: " + std::to_string(opType));
  }
  if (left->isType(csql::ExprType::kExprLiteralInt)) {
    if (opType == csql::OperatorType::kOpEquals)
      return csql::Expr::makeLiteral(left->ival == right->ival);
    if (opType == csql::OperatorType::kOpNotEquals)
      return csql::Expr::makeLiteral(left->ival != right->ival);
    if (opType == csql::OperatorType::kOpLess)
      return csql::Expr::makeLiteral(left->ival < right->ival);
    if (opType == csql::OperatorType::kOpLessEq)
      return csql::Expr::makeLiteral(left->ival <= right->ival);
    if (opType == csql::OperatorType::kOpGreater)
      return csql::Expr::makeLiteral(left->ival > right->ival);
    if (opType == csql::OperatorType::kOpGreaterEq)
      return csql::Expr::makeLiteral(left->ival >= right->ival);
    if (opType == csql::OperatorType::kOpPlus)
      return csql::Expr::makeLiteral(left->ival + right->ival);
    if (opType == csql::OperatorType::kOpMinus)
      return csql::Expr::makeLiteral(left->ival - right->ival);
    if (opType == csql::OperatorType::kOpAsterisk)
      return csql::Expr::makeLiteral(left->ival * right->ival);
    if (opType == csql::OperatorType::kOpSlash)
      return csql::Expr::makeLiteral(left->ival / right->ival);
    if (opType == csql::OperatorType::kOpPercentage)
      return csql::Expr::makeLiteral(left->ival % right->ival);
    throw std::runtime_error("Invalid operation on ints: " + std::to_string(opType));
  }
  if (left->isType(csql::ExprType::kExprLiteralBool)) {
    if (opType == csql::OperatorType::kOpEquals)
      return csql::Expr::makeLiteral(left->ival == right->ival);
    if (opType == csql::OperatorType::kOpNotEquals)
      return csql::Expr::makeLiteral(left->ival != right->ival);
    if (opType == csql::OperatorType::kOpAnd)
      return csql::Expr::makeLiteral(left->ival && right->ival);
    if (opType == csql::OperatorType::kOpOr)
      return csql::Expr::makeLiteral(left->ival || right->ival);
    if (opType == csql::OperatorType::kOpLess)
      return csql::Expr::makeLiteral(left->ival < right->ival);
    if (opType == csql::OperatorType::kOpLessEq)
      return csql::Expr::makeLiteral(left->ival <= right->ival);
    if (opType == csql::OperatorType::kOpGreater)
      return csql::Expr::makeLiteral(left->ival > right->ival);
    if (opType == csql::OperatorType::kOpGreaterEq)
      return csql::Expr::makeLiteral(left->ival >= right->ival);
    throw std::runtime_error("Invalid operation on bools: " + std::to_string(opType));
  }
  if (left->isType(csql::ExprType::kExprLiteralBytes)) {
    if (left->name.size() != right->name.size()) {
      throw std::runtime_error("Byte arrays must be of the same size");
    }
    if (opType == csql::OperatorType::kOpEquals)
      return csql::Expr::makeLiteral(left->name == right->name);
    if (opType == csql::OperatorType::kOpNotEquals)
      return csql::Expr::makeLiteral(left->name != right->name);
    throw std::runtime_error("Invalid operation on bytes: " + std::to_string(opType));
  }
  throw std::runtime_error("Invalid operation");
}

}  // namespace

namespace csql {
namespace storage {

Row::Row(std::shared_ptr<Table> table, Cell* cell) : table_(table), cell_(cell) {}

template <>
int32_t Row::get<int32_t>(size_t index) const {
  return cell_->values_[index].getInt();
}

template <>
bool Row::get<bool>(size_t index) const {
  return cell_->values_[index].getBool();
}

template <>
std::string Row::get<std::string>(size_t index) const {
  return cell_->values_[index].getString();
}

template <>
std::vector<uint8_t> Row::get<std::vector<uint8_t>>(size_t index) const {
  return cell_->values_[index].getBytes();
}

template <>
int32_t Row::get<int32_t>(std::string columnName) const {
  auto table = table_.lock();
  for (size_t i = 0; i < table->columns_.size(); i++) {
    if (table->columns_[i]->getName() == columnName) {
      return get<int32_t>(i);
    }
  }

  throw std::runtime_error("Column not found: " + columnName);
}

template <>
bool Row::get<bool>(std::string columnName) const {
  auto table = table_.lock();
  for (size_t i = 0; i < table->columns_.size(); i++) {
    if (table->columns_[i]->getName() == columnName) {
      return get<bool>(i);
    }
  }

  throw std::runtime_error("Column not found: " + columnName);
}

template <>
std::string Row::get<std::string>(std::string columnName) const {
  auto table = table_.lock();
  for (size_t i = 0; i < table->columns_.size(); i++) {
    if (table->columns_[i]->getName() == columnName) {
      return get<std::string>(i);
    }
  }

  throw std::runtime_error("Column not found: " + columnName);
}

template <>
std::vector<uint8_t> Row::get<std::vector<uint8_t>>(std::string columnName) const {
  auto table = table_.lock();
  for (size_t i = 0; i < table->columns_.size(); i++) {
    if (table->columns_[i]->getName() == columnName) {
      return get<std::vector<uint8_t>>(i);
    }
  }

  throw std::runtime_error("Column not found: " + columnName);
}

bool Row::isNull(size_t index) const {
  return cell_->values_[index].value == nullptr;
}

bool Row::isNull(std::string columnName) const {
  auto table = table_.lock();
  for (size_t i = 0; i < table->columns_.size(); i++) {
    if (table->columns_[i]->getName() == columnName) {
      return isNull(i);
    }
  }

  throw std::runtime_error("Column not found: " + columnName);
}

std::ostream& operator<<(std::ostream& stream, const Row& row) {
  auto table = row.table_.lock();
  for (size_t i = 0; i < table->columns_.size(); i++) {
    auto column = table->columns_[i];
    std::string data;
    if (column->type().data_type == DataType::STRING) {
      data = row.get<std::string>(i);
    } else if (column->type().data_type == DataType::INT32) {
      data = std::to_string(row.get<int32_t>(i));
    } else if (column->type().data_type == DataType::BOOL) {
      data = row.get<bool>(i) ? "true" : "false";
    } else {
      data = "null";
    }

    stream << column->getName() << ": " << data;
    if (i < table->columns_.size() - 1) {
      stream << ", ";
    }

    stream << std::endl;
  }

  return stream;
}

std::shared_ptr<Expr> Row::getColumnValue(std::string columnName) {
  auto table = table_.lock();
  if (!table) {
    throw std::runtime_error("Table not found");
  }
  auto columns = table->getColumns();
  for (size_t i = 0; i < columns.size(); i++) {
    if (columns[i]->getName() == columnName) {
      if (isNull(i)) {
        return Expr::makeNullLiteral();
      }

      if (columns[i]->type().data_type == DataType::INT32) {
        return Expr::makeLiteral(get<int32_t>(i));
      } else if (columns[i]->type().data_type == DataType::STRING) {
        return Expr::makeStringLiteral(get<std::string>(i));
      } else if (columns[i]->type().data_type == DataType::BOOL) {
        return Expr::makeLiteral(get<bool>(i));
      } else if (columns[i]->type().data_type == DataType::BYTES) {
        return Expr::makeLiteral(get<std::vector<uint8_t>>(i));
      }
    }
  }
  throw std::runtime_error("Column not found: " + columnName);
}

std::shared_ptr<Expr> Row::evaluate(std::shared_ptr<Expr> expr) {
  if (expr == nullptr) {
    return nullptr;
  } else if (expr->isType(kExprColumnRef)) {
    auto table = table_.lock();
    if (!table) {
      throw std::runtime_error("Table not found");
    }
    if (expr->hasTable()) {
      if (expr->table != table->getName()) {
        throw std::runtime_error("Table name mismatch: " + expr->table + " vs. " +
                                 table->getName());
      }
    }
    return getColumnValue(expr->getName());
  } else if (expr->isLiteral()) {
    return expr;
  } else if (expr->isType(kExprOperator)) {
    if (isUnaryOperator(expr->opType)) {
      auto operand = evaluate(expr->expr);
      return applyUnaryOpToLiteral(expr->opType, operand);
    } else if (isBinaryOperator(expr->opType)) {
      auto operand1 = evaluate(expr->expr);
      auto operand2 = evaluate(expr->expr2);
      return applyBinaryOpToLiterals(operand1, expr->opType, operand2);
    } else {
      throw std::runtime_error("Unknown operator type: " + std::to_string(expr->opType));
    }
  }
  throw std::runtime_error("Invalid expression type: " + std::to_string(expr->type));
}

}  // namespace storage
}  // namespace csql