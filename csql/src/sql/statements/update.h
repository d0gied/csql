#pragma once

#include <memory>
#include <vector>

#include "insert.h"
#include "sql/expr.h"
#include "statement.h"

namespace csql {

struct UpdateStatement : SQLStatement {
  UpdateStatement() : SQLStatement(kStmtUpdate) {}
  ~UpdateStatement() = default;

  std::shared_ptr<Expr> table;
  std::shared_ptr<std::vector<std::shared_ptr<ColumnValueDefinition>>> columnValues;
  std::shared_ptr<Expr> whereClause;
};

}  // namespace csql
