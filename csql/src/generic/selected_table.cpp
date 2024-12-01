#include <memory>
#include <string>
#include <vector>

#include "column.h"
#include "memory/cell.h"
#include "memory/dummy.h"
#include "row.h"
#include "sql/expr.h"
#include "sql/statements/select.h"
#include "table.h"

namespace csql {
namespace storage {

WhereClauseIterator::WhereClauseIterator(std::shared_ptr<TableIterator> tableIterator,
                                         std::shared_ptr<Expr> whereClause)
    : tableIterator_(tableIterator), whereClause_(whereClause) {
  while (tableIterator_->hasValue()) {  // skip rows that don't match the where clause
    auto row = *(*tableIterator_);
    auto expr = row->evaluate(whereClause_);
    if (expr->type != kExprLiteralBool) {
      throw std::runtime_error("Expected boolean expression");
    }
    if (expr->ival) {
      break;
    }
    ++(*tableIterator_);
  }
}

bool WhereClauseIterator::hasValue() const {
  return tableIterator_->hasValue();
}

std::shared_ptr<Row> WhereClauseIterator::operator*() {
  return tableIterator_->operator*();
}

WhereClauseIterator& WhereClauseIterator::operator++() {
  ++(*tableIterator_);
  while (tableIterator_->hasValue()) {
    auto row = *(*tableIterator_);
    auto expr = row->evaluate(whereClause_);
    if (expr->type != kExprLiteralBool) {
      throw std::runtime_error("Expected boolean expression");
    }
    if (expr->ival) {
      break;
    }
    ++(*tableIterator_);
  }
  return *this;
}

std::shared_ptr<Iterator> WhereClauseIterator::getMemoryIterator() {
  return tableIterator_->getMemoryIterator();
}

SelectedTable::SelectedTable(std::shared_ptr<ITable> table,
                             std::shared_ptr<SelectStatement> selectStatement)
    : table_(table), selectStatement_(selectStatement) {}

std::shared_ptr<SelectedTable> SelectedTable::create(
    std::shared_ptr<ITable> table, std::shared_ptr<SelectStatement> selectStatement) {
  auto table_ = std::make_shared<SelectedTable>(table, selectStatement);
  for (auto columnExpr : *selectStatement->selectList) {
    if (columnExpr->isType(kExprColumnRef)) {
      std::shared_ptr<Column> originalColumn = table->getColumn(columnExpr);
      if (columnExpr->hasAlias()) {
        table_->columns_.push_back(originalColumn->clone(table_, columnExpr->alias));
      } else {
        table_->columns_.push_back(originalColumn->clone(table_));
      }
    } else if (columnExpr->isType(kExprStar)) {
      for (auto column : table->getColumns()) {
        table_->columns_.push_back(column->clone(table_));
      }
    } else {
      throw std::runtime_error("Unsupported expression type");
    }
  }

  return table_;
}

void SelectedTable::insert(std::shared_ptr<InsertStatement> insertStatement) {
  throw std::runtime_error("Insert not supported on selected table");
}

void SelectedTable::delete_(std::shared_ptr<DeleteStatement> deleteStatement) {
  throw std::runtime_error("Delete not supported on selected table");
}

void SelectedTable::update(std::shared_ptr<UpdateStatement> updateStatement) {
  throw std::runtime_error("Update not supported on selected table");
}

std::shared_ptr<VirtualTable> SelectedTable::select(
    std::shared_ptr<SelectStatement> selectStatement) {
  return SelectedTable::create(shared_from_this(), selectStatement);
}

std::shared_ptr<TableIterator> SelectedTable::getIterator() {
  return std::make_shared<SelectedTableIterator>(shared_from_this());
}

const std::string& SelectedTable::getName() const {
  return table_->getName();
}

const std::vector<std::shared_ptr<Column>>& SelectedTable::getColumns() {
  return columns_;
}

std::shared_ptr<Column> SelectedTable::getColumn(std::shared_ptr<Expr> columnExpr) {
  if (columnExpr->isType(kExprColumnRef)) {
    if (columnExpr->hasTable() && columnExpr->table != getName()) {
      throw std::runtime_error("Table not found: " + columnExpr->table);
    }

    for (const auto& column : columns_) {
      if (column->getName() == columnExpr->name) {
        return column;
      }
    }
    throw std::runtime_error("Column not found: " + columnExpr->name);
  } else {
    throw std::runtime_error("Unsupported expression type: " + columnExpr->toString());
  }
}

SelectedTableIterator::SelectedTableIterator(std::shared_ptr<SelectedTable> table)
    : table_(table),
      whereClauseIterator_(std::make_shared<WhereClauseIterator>(
          table->table_->getIterator(), table->selectStatement_->whereClause)) {}

bool SelectedTableIterator::hasValue() const {
  return whereClauseIterator_->hasValue();
}

SelectedTableIterator& SelectedTableIterator::operator++() {
  ++(*whereClauseIterator_);
  return *this;
}

std::shared_ptr<Row> SelectedTableIterator::operator*() {
  auto row = *(*whereClauseIterator_);
  std::shared_ptr<Cell> cell = std::make_shared<Cell>();
  for (auto column : table_->getColumns()) {
    cell->values.push_back(column->createValue(row->getColumnValue(column->refferedColumn())));
  }
  return std::make_shared<Row>(table_, cell);
}

std::shared_ptr<Iterator> SelectedTableIterator::getMemoryIterator() {
  return whereClauseIterator_->getMemoryIterator();
}

}  // namespace storage
}  // namespace csql