#pragma once

#include <memory>
#include <ostream>
#include <unordered_set>
#include <vector>

#include "../column_type.h"
#include "../expr.h"
#include "statement.h"

namespace csql {

// Represents definition of a table column
struct ColumnValueDefinition {
  ColumnValueDefinition(const std::string &name, std::shared_ptr<Expr> value);
  ColumnValueDefinition(std::shared_ptr<Expr> value);

  std::string name;
  std::shared_ptr<Expr> value;
  bool isNamed;
};
enum InsertType {
  kUnknown,
  kInsertValues,
  kInsertKeysValues,
};

struct InsertStatement : SQLStatement {
  InsertStatement(InsertType type, std::shared_ptr<Expr> tableRef);
  ~InsertStatement() = default;

  void setColumnValues(
      std::shared_ptr<std::vector<std::shared_ptr<ColumnValueDefinition>>> columnValues);

  InsertType insertType;
  std::shared_ptr<Expr> tableRef;
  std::shared_ptr<std::vector<std::shared_ptr<ColumnValueDefinition>>> columnValues;
};

std::ostream &operator<<(std::ostream &, const ColumnValueDefinition &);
std::ostream &operator<<(std::ostream &, const InsertStatement &);

}  // namespace csql
