#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "column.h"
#include "memory/set_storage.h"
#include "memory/storage.h"
#include "row.h"
#include "sql/column_type.h"
#include "sql/expr.h"
#include "table.h"

namespace {
using namespace csql;
using namespace csql::storage;
csql::storage::KeyComparator hash_comparator = [](std::shared_ptr<csql::storage::Cell> left,
                                                  std::shared_ptr<csql::storage::Cell> right) {
  return left < right;
};

csql::storage::KeyComparator get_comparator(const std::vector<size_t>& keyColumns,
                                            const std::vector<csql::ColumnType>& keyColumnTypes) {
  if (keyColumns.empty()) {
    return hash_comparator;
  }
  if (keyColumns.size() != keyColumnTypes.size()) {
    throw std::runtime_error("Key columns and types size mismatch");
  }
  return [keyColumns, keyColumnTypes](std::shared_ptr<Cell> left, std::shared_ptr<Cell> right) {
    for (size_t i = 0; i < keyColumns.size(); i++) {
      switch (keyColumnTypes[i].data_type) {
        case DataType::INT32: {
          auto leftValue = left->get<int32_t>(keyColumns[i]);
          auto rightValue = right->get<int32_t>(keyColumns[i]);
          if (leftValue != rightValue) {
            return leftValue < rightValue;
          }
        } break;
        case DataType::STRING: {
          auto leftValue = left->get<std::string>(keyColumns[i]);
          auto rightValue = right->get<std::string>(keyColumns[i]);
          if (leftValue != rightValue) {
            return leftValue < rightValue;
          }
        } break;
        case DataType::BOOL: {
          auto leftValue = left->get<bool>(keyColumns[i]);
          auto rightValue = right->get<bool>(keyColumns[i]);
          if (leftValue != rightValue) {
            return leftValue < rightValue;
          }
        } break;
        case DataType::BYTES: {
          int64_t size = keyColumnTypes[i].length;
          auto leftBytes = left->getBytes(keyColumns[i], size);
          auto rightBytes = right->getBytes(keyColumns[i], size);
          for (size_t j = 0; j < size; j++) {
            if (leftBytes[size - j - 1] != rightBytes[size - j - 1]) {
              return leftBytes[j] < rightBytes[j];
            }
          }
        } break;
        default:
          throw std::runtime_error("Unknown data type");
          break;
      }
    }
    return false;  // Equal
  };
}

}  // namespace

namespace csql {
namespace storage {
StorageTable::StorageTable() {}

StorageTable::~StorageTable() {}

std::shared_ptr<StorageTable> StorageTable::create(
    std::shared_ptr<CreateStatement> createStatement) {
  std::shared_ptr<StorageTable> table = std::make_shared<StorageTable>();

  std::vector<size_t> keyColumns;
  std::vector<ColumnType> keyColumnTypes;

  size_t i = 0;
  for (const auto& columnDef : *createStatement->columns) {
    auto column = Column::create(columnDef);
    table->addColumn(column);
    if (column->isKey()) {
      keyColumns.push_back(i);
      keyColumnTypes.push_back(column->type());
    }
  }

  table->storage_ = std::make_shared<SetStorage>(get_comparator(keyColumns, keyColumnTypes));

  table->name_ = createStatement->tableName;
  return table;
}

std::shared_ptr<StorageTable> StorageTable::create(std::shared_ptr<CreateStatement> createStatement,
                                                   std::shared_ptr<ITable> refTable) {
  std::shared_ptr<StorageTable> table = std::make_shared<StorageTable>();
  std::vector<size_t> keyColumns;
  std::vector<ColumnType> keyColumnTypes;
  for (auto refColumn : refTable->getColumns()) {
    auto column = refColumn->clone(table);
    table->addColumn(column);
    if (column->isKey()) {
      keyColumns.push_back(table->columns_.size() - 1);
      keyColumnTypes.push_back(column->type());
    }
  }
  table->name_ = createStatement->tableName;
  table->storage_ = std::make_shared<SetStorage>(get_comparator(keyColumns, keyColumnTypes));
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

size_t StorageTable::getRowsCount() const {
  return storage_->size();
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

std::shared_ptr<VirtualTable> StorageTable::filter(std::shared_ptr<Expr> whereClause) {
  return FilteredTable::create(shared_from_this(), whereClause);
}

std::shared_ptr<TableIterator> StorageTable::getIterator() {
  return std::make_shared<StorageTableIterator>(shared_from_this(), storage_->getIterator());
}

}  // namespace storage
}  // namespace csql