#pragma once

#include <memory>

#include "../expr.h"
#include "statement.h"

namespace csql {

// Description of the limit clause within a select statement.
struct LimitDescription {
  LimitDescription(Expr *limit, Expr *offset);
  virtual ~LimitDescription();

  std::shared_ptr<Expr> limit;
  std::shared_ptr<Expr> offset;
};

// Representation of a full SQL select statement.
struct SelectStatement : SQLStatement {
  SelectStatement() : SQLStatement(kStmtSelect), selectDistinct(false) {}
  ~SelectStatement() override = default;

  std::shared_ptr<Expr> fromSource;
  bool selectDistinct;
  std::shared_ptr<std::vector<std::shared_ptr<Expr>>> selectList;  // List of expressions to select.
  std::shared_ptr<Expr> whereClause;
  // std::shared_ptr<LimitDescription> limit;
};

std::ostream &operator<<(std::ostream &stream, const SelectStatement &select_statement);

}  // namespace csql
