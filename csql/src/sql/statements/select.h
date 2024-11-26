#pragma once

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
  SelectStatement() = default;
  ~SelectStatement() override = default;

  std::string fromTable;
  bool selectDistinct;
  std::shared_ptr<std::vector<std::shared_ptr<Expr>>> selectList;
  std::shared_ptr<Expr> whereClause;
  // std::shared_ptr<LimitDescription> limit;
};

}  // namespace csql
