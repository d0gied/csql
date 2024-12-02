#include <memory>
#include <string>
#include <vector>

#include "column.h"
#include "memory/cell.h"
#include "row.h"
#include "sql/expr.h"
#include "sql/statements/create.h"
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

FilteredTable::FilteredTable(std::shared_ptr<ITable> table, std::shared_ptr<Expr> whereClause)
    : table_(table), whereClause_(whereClause) {}

std::shared_ptr<FilteredTable> FilteredTable::create(std::shared_ptr<ITable> table,
                                                     std::shared_ptr<Expr> whereClause) {
  auto table_ = std::make_shared<FilteredTable>(table, whereClause);
  for (auto column : table->getColumns()) {
    table_->columns_.push_back(column);
  }

  return table_;
}

std::shared_ptr<TableIterator> FilteredTable::getIterator() {
  return std::make_shared<WhereClauseIterator>(table_->getIterator(), whereClause_);
}

}  // namespace storage
}  // namespace csql