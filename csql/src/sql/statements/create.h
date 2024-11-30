#pragma once

#include <memory>
#include <ostream>
#include <unordered_set>
#include <vector>

#include "../column_type.h"
#include "../expr.h"
#include "statement.h"

namespace csql {
struct DeleteStatement;

enum struct ConstraintType {
  None,
  Key,
  Unique,
  AutoIncrement,
};

// Superclass for both TableConstraint and Column Definition
struct TableElement {
  virtual ~TableElement() = default;
};

// Represents definition of a table constraint
struct TableConstraint : TableElement {
  TableConstraint(ConstraintType keyType, std::shared_ptr<std::vector<std::string>> columnNames);

  ConstraintType type;
  std::shared_ptr<std::vector<std::string>> columnNames;
};

// Represents definition of a table column
struct ColumnDefinition : TableElement {
  ColumnDefinition(std::string name, ColumnType type,
                   std::shared_ptr<std::unordered_set<ConstraintType>> column_constraints,
                   std::shared_ptr<Expr> default_value);

  std::shared_ptr<std::unordered_set<ConstraintType>> column_constraints;
  std::string name;
  ColumnType type;
  bool nullable;
  std::shared_ptr<Expr> default_value;
};

enum CreateType { kCreateTable, kCreateIndex };

struct CreateStatement : SQLStatement {
  CreateStatement(CreateType type);
  ~CreateStatement() = default;

  void setColumnDefs(std::shared_ptr<std::vector<std::shared_ptr<ColumnDefinition>>> _columns);

  CreateType type;
  bool ifNotExists;
  std::string tableName;
  std::string indexName;
  std::shared_ptr<std::vector<std::string>> indexColumns;
  std::shared_ptr<std::vector<std::shared_ptr<ColumnDefinition>>> columns;
  std::shared_ptr<std::vector<std::shared_ptr<TableConstraint>>> tableConstraints;
};

std::ostream &operator<<(std::ostream &, const ColumnDefinition &);
std::ostream &operator<<(std::ostream &, const CreateStatement &);

}  // namespace csql
