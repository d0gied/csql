
#include "statement.h"

#include <ostream>

namespace csql {

// SQLStatement
SQLStatement::SQLStatement(StatementType type) : type_(type) {}

StatementType SQLStatement::type() const {
  return type_;
}

bool SQLStatement::isType(StatementType type) const {
  return (type_ == type);
}

bool SQLStatement::is(StatementType type) const {
  return isType(type);
}

std::ostream& operator<<(std::ostream& stream, const SQLStatement& statement) {
  switch (statement.type()) {
    case kStmtSelect:
      stream << "SELECT";
      break;
    case kStmtInsert:
      stream << "INSERT";
      break;
    case kStmtUpdate:
      stream << "UPDATE";
      break;
    case kStmtDelete:
      stream << "DELETE";
      break;
    case kStmtCreate:
      stream << "CREATE";
      break;
    case kStmtDrop:
      stream << "DROP";
      break;
  }
  return stream;
}

}  // namespace csql