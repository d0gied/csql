#pragma once

#include <memory>

#include "../expr.h"
#include "statement.h"

namespace csql {

struct DeleteStatement : SQLStatement {
  DeleteStatement() : SQLStatement(kStmtDelete) {}
  ~DeleteStatement() override = default;

  std::shared_ptr<Expr> tableRef;
  std::shared_ptr<Expr> whereClause;
};

std::ostream &operator<<(std::ostream &stream, const DeleteStatement &delete_statement);

}  // namespace csql
