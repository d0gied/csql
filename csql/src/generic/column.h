#pragma once

#include <memory>
#include <ostream>

#include "../memory/cell.h"
#include "../sql/statements/create.h"
#include "generic/table.h"
#include "sql/expr.h"

namespace csql {
namespace storage {

class ITable;

class Column : public std::enable_shared_from_this<Column> {
 public:
  static std::shared_ptr<Column> create(std::shared_ptr<ColumnDefinition> columnDefinition);
  static std::shared_ptr<Column> create(const std::string& name, const ColumnType& type,
                                        std::shared_ptr<ITable> table,
                                        std::shared_ptr<Expr> refExpr);
  Column() = default;
  virtual ~Column() = default;

  const std::string& getName() const;

  bool isNullable() const;
  bool isAutoincrement() const;
  bool isKey() const;
  bool isUnique() const;

  int32_t maxValue() const;

  void* createValue() const;
  void* createValue(std::shared_ptr<Expr> value) const;

  std::shared_ptr<Column> clone(std::shared_ptr<ITable> table, const std::string& name = "");
  std::shared_ptr<Column> refferedColumn() const;
  std::shared_ptr<Expr> refferedExpr() const;
  std::shared_ptr<ITable> table() const;

  const ColumnType& type() const;
  friend std::ostream& operator<<(std::ostream& stream, const Column& column);
  friend class ITable;
  friend class StorageTable;

 private:
  ColumnType column_type_;
  std::string name_;
  bool nullable_ = true;
  bool is_autoincrement_ = false;
  bool is_key_ = false;
  bool is_unique_ = false;
  std::shared_ptr<Expr> default_value_;

  std::weak_ptr<ITable> table_;
  std::shared_ptr<Column> reffered_column_;
  std::shared_ptr<Expr> reffered_expr_;
};

}  // namespace storage
}  // namespace csql