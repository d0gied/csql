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
  T get(std::string columnName) const;

  bool isNull(size_t index) const;
  bool isNull(std::string columnName) const;

  std::shared_ptr<Expr> evaluate(std::shared_ptr<Expr> expr);
  std::shared_ptr<Expr> getColumnValue(std::string columnName);

  friend std::ostream& operator<<(std::ostream& stream, const Row& row);
  friend class Table;

 private:
  std::weak_ptr<Table> table_;
  Cell* cell_;
};
}  // namespace storage
}  // namespace csql