#pragma once

#include <memory>
#include <ostream>

#include "../memory/cell.h"
#include "memory/iterator.h"
#include "table.h"

namespace csql {
namespace storage {

class Row;
class Column;
class Cell;
class ITable;
class StorageTable;
class Iterator;
class WhereClauseIterator;
class SelectedTableIterator;

class Row {
 public:
  Row(std::shared_ptr<ITable> table, std::shared_ptr<Cell> cell);
  virtual ~Row() = default;

  template <typename T>
  T get(size_t index) const;

  template <typename T>
  T get(std::string columnName) const;

  bool isNull(size_t index) const;
  bool isNull(std::string columnName) const;

  friend std::ostream& operator<<(std::ostream& stream, const Row& row);

  friend class ITable;
  friend class StorageTable;
  friend class Iterator;
  friend class WhereClauseIterator;
  friend class SelectedTableIterator;

 private:
  std::shared_ptr<Expr> evaluate(std::shared_ptr<Expr> expr);
  std::shared_ptr<Expr> getColumnValue(size_t index);
  std::shared_ptr<Expr> getColumnValue(std::string columnName);
  std::shared_ptr<Expr> getColumnValue(std::shared_ptr<Column> column);

  size_t getIndexOfColumn(std::string columnName) const;
  size_t getIndexOfColumn(std::shared_ptr<Column> column) const;

 private:
  std::weak_ptr<ITable> table_;
  std::shared_ptr<Cell> cell_;
};
}  // namespace storage
}  // namespace csql