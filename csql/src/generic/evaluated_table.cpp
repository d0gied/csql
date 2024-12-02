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

EvaluatedTable::EvaluatedTable(std::shared_ptr<ITable> table,
                               std::shared_ptr<std::vector<std::shared_ptr<Expr>>> expressions)
    : table_(table), expressions_(expressions) {}

std::shared_ptr<EvaluatedTable> EvaluatedTable::create(
    std::shared_ptr<ITable> table,
    std::shared_ptr<std::vector<std::shared_ptr<Expr>>> expressions) {
  auto table_ = std::make_shared<EvaluatedTable>(table, expressions);
  for (auto columnExpr : *expressions) {
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
      auto predictedType = table->predictType(columnExpr);
      if (predictedType.data_type != DataType::UNKNOWN) {
        if (columnExpr->hasAlias()) {
          table_->columns_.push_back(
              Column::create(columnExpr->alias, predictedType, table_, columnExpr));
        } else {
          throw std::runtime_error("Expression must have an alias");
        }
      } else {
        throw std::runtime_error("Bad expression");
      }
    }
  }

  return table_;
}

std::shared_ptr<TableIterator> EvaluatedTable::getIterator() {
  return std::make_shared<EvaluateIterator>(
      std::dynamic_pointer_cast<EvaluatedTable>(shared_from_this()));
}

EvaluateIterator::EvaluateIterator(std::shared_ptr<EvaluatedTable> table)
    : table_(table), it_(table->table_->getIterator()) {}

bool EvaluateIterator::hasValue() const {
  return it_->hasValue();
}

EvaluateIterator& EvaluateIterator::operator++() {
  ++(*it_);
  return *this;
}

std::shared_ptr<Row> EvaluateIterator::operator*() {
  auto row = *(*it_);
  std::shared_ptr<Cell> cell = std::make_shared<Cell>();
  for (auto column : table_->getColumns()) {
    if (column->refferedExpr()) {  // expression
      cell->values.push_back(column->createValue(row->evaluate(column->refferedExpr())));
    } else {
      cell->values.push_back(column->createValue(row->getColumnValue(column->refferedColumn())));
    }
  }
  return std::make_shared<Row>(table_, cell);
}

std::shared_ptr<Iterator> EvaluateIterator::getMemoryIterator() {
  return it_->getMemoryIterator();
}

}  // namespace storage
}  // namespace csql