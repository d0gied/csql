#pragma once

#include <memory>
#include <ostream>

#include "../memory/cell.h"
#include "../sql/statements/create.h"

namespace csql {
namespace storage {
class Table;

class Column {
 public:
  static std::shared_ptr<Column> create(std::shared_ptr<ColumnDefinition> columnDefinition);
  Column() = default;
  virtual ~Column() = default;

  const std::string& getName() const;

  bool isNullable() const;
  bool isAutoincrement() const;
  bool isKey() const;
  bool isUnique() const;

  int32_t maxValue() const;

  std::shared_ptr<Value> createValue() const;
  std::shared_ptr<Value> createValue(std::shared_ptr<Expr> value) const;

  const ColumnType& type() const;
  friend std::ostream& operator<<(std::ostream& stream, const Column& column);
  friend class Table;

 private:
  ColumnType column_type_;
  std::string name_;
  bool nullable_ = true;
  bool is_autoincrement_ = false;
  bool is_key_ = false;
  bool is_unique_ = false;
  void* default_value_;

  std::weak_ptr<Table> table_;
};

}  // namespace storage
}  // namespace csql