#include "table.h"

#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "column.h"
#include "memory/dummy.h"
#include "row.h"

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

void StorageTable::addColumn(std::shared_ptr<Column> column) {
  columns_.push_back(column);
  column->table_ = shared_from_this();
}

const std::string& StorageTable::getName() const {
  return name_;
}

const std::vector<std::shared_ptr<Column>>& StorageTable::getColumns() const {
  return columns_;
}

void StorageTable::insert(std::shared_ptr<InsertStatement> insertStatement) {
  std::vector<std::shared_ptr<Value>> values;
  if (insertStatement->insertType == InsertType::kInsertKeysValues) {
    for (auto column : columns_) {
      bool found = false;
      for (const auto& columnValue : *insertStatement->columnValues) {
        if (column->getName() == columnValue->name) {
          values.push_back(column->createValue(columnValue->value));
          found = true;
          break;
        }
      }
      if (!found) {
        values.push_back(column->createValue());
      }
    }
  } else {
    for (size_t i = 0; i < columns_.size(); i++) {
      if (i < insertStatement->columnValues->size()) {
        values.push_back(columns_[i]->createValue(insertStatement->columnValues->at(i)->value));
      } else {
        values.push_back(columns_[i]->createValue());
      }
    }
  }

  bool check_unique = false;
  for (const auto& column : columns_) {
    if (column->isUnique() || column->is_key_) {
      check_unique = true;
      break;
    }
  }

  if (check_unique) {
    auto it = getIterator();
    while (it->hasValue()) {
      auto row = *(*it);
      ++(*it);
      bool is_unique = true;
      for (size_t i = 0; i < columns_.size(); i++) {
        if (values[i]->isNull()) {
          continue;
        }
        if (columns_[i]->isUnique() || columns_[i]->is_key_ && !row->isNull(i)) {
          if (columns_[i]->type().data_type == DataType::STRING) {
            if (row->get<std::string>(i) == values[i]->getString()) {
              is_unique = false;
              break;
            }
          } else if (columns_[i]->type().data_type == DataType::INT32) {
            if (row->get<int32_t>(i) == values[i]->getInt()) {
              is_unique = false;
              break;
            }
          } else if (columns_[i]->type().data_type == DataType::BOOL) {
            if (row->get<bool>(i) == values[i]->getBool()) {
              is_unique = false;
              break;
            }
          } else if (columns_[i]->type().data_type == DataType::BYTES) {
            if (row->get<std::vector<uint8_t>>(i) == values[i]->getBytes()) {
              is_unique = false;
              break;
            }
          }
        }
        if (!is_unique) {
          throw std::runtime_error("Unique constraint violation");
        }
      }
    }
  }
  storage_->insert(*Cell::create(values));
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
  // auto it = getIterator();
  // while (it->hasValue()) {
  //   auto row = *(*it);
  //   auto expr = row->evaluate(updateStatement->whereClause);
  //   if (expr->type != kExprLiteralBool) {
  //     throw std::runtime_error("Expected boolean expression");
  //   }
  //   if (expr->ival) {
  //     for (const auto& columnValue : *updateStatement->columnValues) {
  //       for (size_t i = 0; i < columns_.size(); i++) {
  //         if (columns_[i]->getName() == columnValue->name) {
  //           row->set(i, columnValue->value);
  //           break;
  //         }
  //       }
  //     }
  //     storage_->update(it->getMemoryIterator(), *Cell::create(row->values()));
  //   }
  //   ++(*it);
  // }
}

std::shared_ptr<TableIterator> StorageTable::getIterator() const {
  return std::make_shared<AllTableIterator>(shared_from_this(), storage_->getIterator());
}

AllTableIterator::AllTableIterator(std::shared_ptr<const ITable> table,
                                   std::shared_ptr<Iterator> iterator)
    : iterator_(iterator), table_(table) {}

bool AllTableIterator::hasValue() const {
  return iterator_->hasValue();
}

std::shared_ptr<Row> AllTableIterator::operator*() const {
  return std::make_shared<Row>(table_, iterator_->get());
}

AllTableIterator& AllTableIterator::operator++() {
  iterator_->next();
  return *this;
}

std::shared_ptr<Iterator> AllTableIterator::getMemoryIterator() const {
  return iterator_;
}

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

std::shared_ptr<Row> WhereClauseIterator::operator*() const {
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

std::shared_ptr<Iterator> WhereClauseIterator::getMemoryIterator() const {
  return tableIterator_->getMemoryIterator();
}

void StorageTable::exportToCSV(const std::string& filename) {
  // export table to csv
  std::ofstream file(filename);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open file: " + filename);
  }

  for (const auto& column : columns_) {
    file << column->getName();
    if (&column != &columns_.back()) {
      file << ",";
    }
  }
  file << std::endl;

  auto it = getIterator();

  while (it->hasValue()) {
    auto row = *(*it);
    ++(*it);  // increment iterator
    for (size_t i = 0; i < columns_.size(); i++) {
      auto column = columns_[i];
      if (row->isNull(i)) {
        file << "null";
      } else if (column->type().data_type == DataType::STRING) {
        file << row->get<std::string>(i);
      } else if (column->type().data_type == DataType::INT32) {
        file << row->get<int32_t>(i);
      } else if (column->type().data_type == DataType::BOOL) {
        file << (row->get<bool>(i) ? "true" : "false");
      } else if (column->type().data_type == DataType::BYTES) {
        auto bytes = row->get<std::vector<uint8_t>>(i);
        file << "0x";
        for (size_t j = 0; j < bytes.size(); j++) {
          int byte = bytes[bytes.size() - j - 1];
          file << std::setw(2) << std::setfill('0') << std::hex << byte;
        }
      } else {
        file << "null";
      }

      if (i < columns_.size() - 1) {
        file << ",";
      }
    }
    file << std::endl;
  }

  file.close();
}

}  // namespace storage
}  // namespace csql