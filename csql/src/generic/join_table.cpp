#include <memory>
#include <string>

#include "column.h"
#include "row.h"
#include "sql/expr.h"
#include "table.h"

namespace csql {
namespace storage {

JoinTable::JoinTable(std::shared_ptr<ITable> left, std::shared_ptr<ITable> right,
                     std::shared_ptr<Expr> onClause, OperatorType joinType)
    : left_(left), right_(right), onClause_(onClause), joinType_(joinType) {
  name_ = left->getName() + "_" + right->getName();
}

std::shared_ptr<JoinTable> JoinTable::create(std::shared_ptr<ITable> left,
                                             std::shared_ptr<ITable> right,
                                             std::shared_ptr<Expr> onClause,
                                             OperatorType joinType) {
  auto table = std::make_shared<JoinTable>(left, right, onClause, joinType);
  for (const auto& column : left->getColumns()) {
    table->columns_.push_back(column->clone(table));
  }
  for (const auto& column : right->getColumns()) {
    table->columns_.push_back(column->clone(table));
  }
  return table;
}

std::shared_ptr<TableIterator> JoinTable::getIterator() {
  if (joinType_ == kOpInnerJoin) {
    return std::make_shared<InnerJoinIterator>(shared_from_this());
  } else {
    throw std::runtime_error("Unsupported join type");
  }
}

const std::string& JoinTable::getName() const {
  return name_;
}

const std::vector<std::shared_ptr<Column>>& JoinTable::getColumns() {
  return columns_;
}

std::shared_ptr<Column> JoinTable::getColumn(std::shared_ptr<Expr> columnExpr) {
  if (columnExpr->isType(kExprColumnRef)) {
    if (!columnExpr->hasTable()) {
      throw std::runtime_error("Join column must have table name: " + columnExpr->name);
    }
    for (const auto& column : columns_) {
      if (column->refferedColumn()->getName() == columnExpr->name &&
          column->refferedColumn()->table()->getName() == columnExpr->table) {
        return column;
      }
    }
    throw std::runtime_error("Column not found: " + columnExpr->toString());
  } else {
    throw std::runtime_error("Unsupported expression type: " + columnExpr->toString());
  }
}

void JoinTable::insert(std::shared_ptr<InsertStatement> insertStatement) {
  throw std::runtime_error("Insert not supported for join tables");
}

void JoinTable::delete_(std::shared_ptr<DeleteStatement> deleteStatement) {
  throw std::runtime_error("Delete not supported for join tables");
}

void JoinTable::update(std::shared_ptr<UpdateStatement> updateStatement) {
  throw std::runtime_error("Update not supported for join tables");
}

std::shared_ptr<VirtualTable> JoinTable::select(std::shared_ptr<SelectStatement> selectStatement) {
  return SelectedTable::create(shared_from_this(), selectStatement);
}

JoinTableIterator::JoinTableIterator(std::shared_ptr<JoinTable> table)
    : table_(table),
      leftTableIterator_(table_->left_->getIterator()),
      rightTableIterator_(table_->right_->getIterator()) {}

bool JoinTableIterator::hasValue() const {
  return leftTableIterator_->hasValue() && rightTableIterator_->hasValue();
}

std::shared_ptr<Row> JoinTableIterator::mergeRows() {
  std::shared_ptr<Cell> cell = std::make_shared<Cell>();
  auto leftColumnsCount = table_->left_->getColumns().size();
  auto columns = table_->getColumns();
  for (size_t i = 0; i < columns.size(); i++) {
    if (i < leftColumnsCount) {
      cell->values.push_back(columns[i]->createValue((*(*leftTableIterator_))->getColumnValue(i)));
    } else {
      cell->values.push_back(
          columns[i]->createValue((*(*rightTableIterator_))->getColumnValue(i - leftColumnsCount)));
    }
  }

  return std::make_shared<Row>(table_, cell);
}

std::shared_ptr<Row> JoinTableIterator::operator*() {
  return row_;
}

std::shared_ptr<Iterator> JoinTableIterator::getMemoryIterator() {
  return nullptr;
}

void JoinTableIterator::resetRight() {
  rightTableIterator_ = table_->right_->getIterator();
}

bool JoinTableIterator::match() {
  return row_->evaluate(table_->onClause_)->ival;
}

InnerJoinIterator::InnerJoinIterator(std::shared_ptr<JoinTable> table) : JoinTableIterator(table) {
  if (!hasValue()) {
    row_ = nullptr;
  }
  row_ = mergeRows();
  if (!match()) {
    operator++();
  }
}

InnerJoinIterator& InnerJoinIterator::operator++() {
  if (!hasValue()) {
    throw std::runtime_error("No more values");
  }
  ++(*rightTableIterator_);
  if (!rightTableIterator_->hasValue()) {
    ++(*leftTableIterator_);
    resetRight();
  }
  if (!hasValue()) {
    row_ = nullptr;
    return *this;
  }
  row_ = mergeRows();

  while (!match()) {
    ++(*rightTableIterator_);
    if (!rightTableIterator_->hasValue()) {
      ++(*leftTableIterator_);
      resetRight();
    }
    if (!hasValue()) {
      row_ = nullptr;
      return *this;
    }
    row_ = mergeRows();
  }

  return *this;
}

}  // namespace storage
}  // namespace csql