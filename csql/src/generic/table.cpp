#include "table.h"

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "column.h"
#include "memory/dummy.h"
#include "row.h"

namespace csql {
namespace storage {
Table::Table() : storage_(std::make_shared<DummyStorage>()) {}

Table::~Table() {}

std::shared_ptr<Table> Table::create(std::shared_ptr<CreateStatement> createStatement) {
  std::shared_ptr<Table> table = std::make_shared<Table>();
  for (const auto& column : *createStatement->columns) {
    table->addColumn(Column::create(column));
  }
  return table;
}

void Table::addColumn(std::shared_ptr<Column> column) {
  columns_.push_back(column);
  column->table_ = shared_from_this();
}

size_t Table::size() const {
  return storage_->size();
}

void Table::insert(std::shared_ptr<InsertStatement> insertStatement) {
  std::vector<std::shared_ptr<Value>> values;
  if (insertStatement->insertType == InsertType::kInsertKeysValues) {
    for (const auto& column : columns_) {
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

  storage_->insert(*Cell::create(values));
}

std::shared_ptr<TableIterator> Table::getIterator() {
  return std::make_shared<TableIterator>(shared_from_this(), storage_->getIterator());
}

TableIterator::TableIterator(std::shared_ptr<Table> table, std::shared_ptr<Iterator> iterator)
    : table_(table), iterator_(iterator) {}

bool TableIterator::hasNext() {
  return iterator_->hasNext();
}

std::shared_ptr<Row> TableIterator::next() {
  if (!hasNext()) return nullptr;
  auto result = std::make_shared<Row>(table_, iterator_->get());
  iterator_->next();
  return result;
}

void Table::exportToCSV(const std::string& filename) {
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

  while (it->hasNext()) {
    auto row = it->next();
    for (size_t i = 0; i < columns_.size(); i++) {
      auto column = columns_[i];
      if (column->type().data_type == DataType::STRING) {
        file << row->get<std::string>(i);
      } else if (column->type().data_type == DataType::INT32) {
        file << row->get<int32_t>(i);
      } else if (column->type().data_type == DataType::BOOL) {
        file << (row->get<bool>(i) ? "true" : "false");
      } else if (column->type().data_type == DataType::BYTES) {
        auto bytes = row->get<std::vector<uint8_t>>(i);
        file << "0x";
        bool has_non_zero = false;
        for (size_t j = 0; j < bytes.size(); j++) {
          if (bytes[bytes.size() - j - 1] != 0) {
            has_non_zero = true;
          }
          if (has_non_zero) {
            file << std::hex << (int)bytes[bytes.size() - j - 1];
          }
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

std::shared_ptr<Row> TableIterator::operator++() {
  return next();
}

}  // namespace storage
}  // namespace csql