#include "table.h"

#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "column.h"
#include "memory/iterator.h"
#include "row.h"

namespace csql {
namespace storage {

StorageTableIterator::StorageTableIterator(std::shared_ptr<StorageTable> table,
                                           std::shared_ptr<Iterator> iterator)
    : iterator_(iterator), table_(table) {}

bool StorageTableIterator::hasValue() const {
  return iterator_->hasValue();
}

std::shared_ptr<Row> StorageTableIterator::operator*() {
  return std::make_shared<Row>(table_, iterator_->get());
}

StorageTableIterator& StorageTableIterator::operator++() {
  iterator_->next();
  return *this;
}

std::shared_ptr<Iterator> StorageTableIterator::getMemoryIterator() {
  return iterator_;
}

void ITable::exportToCSV(const std::string& filename) {
  // export table to csv
  std::ofstream file(filename);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open file: " + filename);
  }
  auto columns = getColumns();

  for (const auto& column : columns) {
    file << column->getName();
    if (&column != &columns.back()) {
      file << ",";
    }
  }
  file << std::endl;

  auto it = getIterator();

  while (it->hasValue()) {
    auto row = *(*it);
    ++(*it);  // increment iterator
    for (size_t i = 0; i < columns.size(); i++) {
      auto column = columns[i];
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

      if (i < columns.size() - 1) {
        file << ",";
      }
    }
    file << std::endl;
  }

  file.close();
}

}  // namespace storage
}  // namespace csql