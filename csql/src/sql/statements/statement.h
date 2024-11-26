#pragma once

#include <iostream>
#include <vector>

namespace csql {
enum StatementType {
  kStmtSelect,
  kStmtInsert,
  kStmtUpdate,
  kStmtDelete,
  kStmtCreate,
  kStmtDrop,
};

// Base struct for every SQL statement
struct SQLStatement {
  SQLStatement(StatementType type);

  virtual ~SQLStatement() = default;

  StatementType type() const;

  bool isType(StatementType type) const;

  bool is(StatementType type) const;

 private:
  StatementType type_;
};

std::ostream& operator<<(std::ostream&, const SQLStatement&);

}  // namespace csql
