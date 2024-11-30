#pragma once

#include "../expr.h"
#include "statement.h"

namespace csql {

struct DeleteStatement : SQLStatement {
  DeleteStatement() : SQLStatement(kStmtDelete) {}
  ~DeleteStatement() override = default;

  std::string fromTable;
  std::shared_ptr<Expr> whereClause;
};

std::ostream &operator<<(std::ostream &stream, const DeleteStatement &delete_statement);

}  // namespace csql
