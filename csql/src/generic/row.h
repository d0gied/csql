#pragma once

#include <memory>
#include <ostream>
#include <vector>

#include "../memory/cell.h"
#include "table.h"

namespace csql {
namespace storage {

class Row;
class Column;
class Cell;

class Row {
 public:
  Row(std::shared_ptr<Table> table, Cell* cell);
  virtual ~Row() = default;

  template <typename T>
  T get(size_t index) const;

  template <typename T>
  T get(std::string columnName) {
    auto table = table_.lock();
    for (size_t i = 0; i < table->columns_.size(); i++) {
      if (table->columns_[i]->getName() == columnName) {
        return get<T>(i);
      }
    }
    throw std::runtime_error("Column not found: " + columnName);
    return T();
  }

  bool isNull(size_t index) const;
  bool isNull(std::string columnName) const;

  friend std::ostream& operator<<(std::ostream& stream, const Row& row);

 private:
  std::weak_ptr<Table> table_;
  Cell* cell_;
};
}  // namespace storage
}  // namespace csql