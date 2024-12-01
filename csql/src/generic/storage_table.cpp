#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "column.h"
#include "memory/dummy.h"
#include "row.h"
#include "table.h"

namespace csql {
namespace storage {
StorageTable::StorageTable() : storage_(std::make_shared<DummyStorage>()) {}

StorageTable::~StorageTable() {}

std::shared_ptr<StorageTable> StorageTable::create(
    std::shared_ptr<CreateStatement> createStatement) {
  std::shared_ptr<StorageTable> table = std::make_shared<StorageTable>();
  for (const auto& column : *createStatement->columns) {
    table->addColumn(Column::create(column));
  }
  table->name_ = createStatement->tableName;
  return table;
}

std::shared_ptr<StorageTable> StorageTable::create(std::shared_ptr<CreateStatement> createStatement,
                                                   std::shared_ptr<ITable> refTable) {
  std::shared_ptr<StorageTable> table = std::make_shared<StorageTable>();
  for (auto column : table->getColumns()) {
    table->addColumn(column->clone(refTable));
  }
  table->name_ = createStatement->tableName;
  auto it = refTable->getIterator();
  while (it->hasValue()) {
    table->storage_->insert((*(*it))->cell_);
    ++(*it);
  }
  return table;
}

void StorageTable::addColumn(std::shared_ptr<Column> column) {
  columns_.push_back(column);
  column->table_ = shared_from_this();
}

const std::string& StorageTable::getName() const {
  return name_;
}

const std::vector<std::shared_ptr<Column>>& StorageTable::getColumns() {
  return columns_;
}

std::shared_ptr<Column> StorageTable::getColumn(std::shared_ptr<Expr> columnExpr) {
  if (columnExpr->type == kExprColumnRef) {
    for (const auto& column : columns_) {
      if (column->getName() == columnExpr->name) {
        return column;
      }
    }
  } else {
    throw std::runtime_error("Unsupported expression type: " + columnExpr->toString());
  }
  throw std::runtime_error("Column not found: " + columnExpr->toString());
}

void StorageTable::insert(std::shared_ptr<InsertStatement> insertStatement) {
  std::shared_ptr<Cell> cell = std::make_shared<Cell>();
  if (insertStatement->insertType == InsertType::kInsertKeysValues) {
    for (auto column : columns_) {
      bool found = false;
      for (const auto& columnValue : *insertStatement->columnValues) {
        if (column->getName() == columnValue->name) {
          cell->values.push_back(column->createValue(columnValue->value));
          found = true;
          break;
        }
      }
      if (!found) {
        cell->values.push_back(column->createValue());
      }
    }
  } else {
    for (size_t i = 0; i < columns_.size(); i++) {
      if (i < insertStatement->columnValues->size()) {
        cell->values.push_back(
            columns_[i]->createValue(insertStatement->columnValues->at(i)->value));
      } else {
        cell->values.push_back(columns_[i]->createValue());
      }
    }
  }

  std::vector<size_t> unique_columns;
  for (size_t i = 0; i < columns_.size(); i++) {
    if (columns_[i]->isUnique() || columns_[i]->is_key_) {
      unique_columns.push_back(i);
    }
  }

  std::shared_ptr<Row> newRow = std::make_shared<Row>(shared_from_this(), cell);

  if (!unique_columns.empty()) {
    auto it = getIterator();
    bool unique = true;
    while (it->hasValue()) {
      auto row = *(*it);
      ++(*it);
      for (auto i : unique_columns) {
        auto column = columns_[i];
        switch (column->type().data_type) {
          case DataType::INT32:
            unique &= row->get<int32_t>(i) != newRow->get<int32_t>(i);
            break;
          case DataType::STRING:
            unique &= row->get<std::string>(i) != newRow->get<std::string>(i);
            break;
          case DataType::BOOL:
            unique &= row->get<bool>(i) != newRow->get<bool>(i);
            break;
          case DataType::BYTES:
            unique &= row->get<std::vector<uint8_t>>(i) != newRow->get<std::vector<uint8_t>>(i);
            break;
          default:
            throw std::runtime_error("Invalid data type");
            break;
        }
      }
    }
    if (!unique) {
      throw std::runtime_error("Duplicate key");
    }
  }

  storage_->insert(cell);
}

void StorageTable::delete_(std::shared_ptr<DeleteStatement> deleteStatement) {
  auto it = getIterator();
  while (it->hasValue()) {
    auto row = *(*it);
    auto expr = row->evaluate(deleteStatement->whereClause);
    if (expr->type != kExprLiteralBool) {
      throw std::runtime_error("Expected boolean expression");
    }
    if (expr->ival) {
      storage_->remove(it->getMemoryIterator());
    } else {
      ++(*it);
    }
  }
}

void StorageTable::update(std::shared_ptr<UpdateStatement> updateStatement) {
  throw std::runtime_error("Update not supported on storage table");
}

std::shared_ptr<VirtualTable> StorageTable::select(
    std::shared_ptr<SelectStatement> selectStatement) {
  return SelectedTable::create(shared_from_this(), selectStatement);
}

std::shared_ptr<TableIterator> StorageTable::getIterator() {
  return std::make_shared<StorageTableIterator>(shared_from_this(), storage_->getIterator());
}

}  // namespace storage
}  // namespace csql