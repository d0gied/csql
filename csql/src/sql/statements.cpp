#include "statements.h"

#include "table.h"

namespace csql {

// KeyConstraints
TableConstraint::TableConstraint(ConstraintType type,
                                 std::shared_ptr<std::vector<std::string>> columnNames)
    : type(type), columnNames(columnNames) {}

// ColumnDefinition
ColumnDefinition::ColumnDefinition(
    std::string name, ColumnType type,
    std::shared_ptr<std::unordered_set<ConstraintType>> column_constraints,
    std::shared_ptr<Expr> default_value)
    : column_constraints(column_constraints),
      name(name),
      type(type),
      default_value(default_value),
      nullable(true) {}

ColumnType::ColumnType(DataType data_type, int64_t length) : data_type(data_type), length(length) {}

bool operator==(const ColumnType& lhs, const ColumnType& rhs) {
  if (lhs.data_type != rhs.data_type) return false;
  return lhs.length == rhs.length;
}

bool operator!=(const ColumnType& lhs, const ColumnType& rhs) {
  return !(lhs == rhs);
}

std::ostream& operator<<(std::ostream& stream, const ColumnType& column_type) {
  switch (column_type.data_type) {
    case DataType::UNKNOWN:
      stream << "UNKNOWN";
      break;
    case DataType::INT32:
      stream << "int32";
      break;
    case DataType::STRING:
      stream << "string[" << column_type.length << "]";
      break;
    case DataType::BOOL:
      stream << "bool";
      break;
    case DataType::BYTES:
      stream << "bytes[" << column_type.length << "]";
      break;
  }
  return stream;
}

// InsertStatement
ColumnValueDefinition::ColumnValueDefinition(const std::string& name, std::shared_ptr<Expr> value)
    : name(name), value(value), isNamed(true) {}

ColumnValueDefinition::ColumnValueDefinition(std::shared_ptr<Expr> value)
    : value(value), isNamed(false) {}

InsertStatement::InsertStatement(InsertType type, std::string tableName)
    : SQLStatement(kStmtInsert), insertType(type), tableName(tableName), columnValues(nullptr) {}

void InsertStatement::setColumnValues(
    std::shared_ptr<std::vector<std::shared_ptr<ColumnValueDefinition>>> columnValues) {
  this->columnValues = columnValues;
}

std::ostream& operator<<(std::ostream& stream, const ColumnValueDefinition& column_value) {
  if (column_value.isNamed) {
    stream << column_value.name << " = ";
  }
  stream << *column_value.value;
  return stream;
}

std::ostream& operator<<(std::ostream& stream, const InsertStatement& insert_statement) {
  stream << "INSERT INTO " << insert_statement.tableName << " ";
  if (insert_statement.insertType == InsertType::kInsertKeysValues) {
    stream << "(";
    for (size_t i = 0; i < insert_statement.columnValues->size(); i++) {
      stream << *insert_statement.columnValues->at(i);
      if (i + 1 < insert_statement.columnValues->size()) {
        stream << ", ";
      }
    }
    stream << ")";
  } else {
    stream << "VALUES (";
    for (size_t i = 0; i < insert_statement.columnValues->size(); i++) {
      stream << *insert_statement.columnValues->at(i);
      if (i + 1 < insert_statement.columnValues->size()) {
        stream << ", ";
      }
    }
    stream << ")";
  }
  return stream;
}

// LimitDescription
// LimitDescription::LimitDescription(Expr* limit, Expr* offset) : limit(limit), offset(offset) {}

// SelectStatement
// SelectStatement::SelectStatement()
//     : SQLStatement(kStmtSelect),
//       fromTable(""),
//       selectDistinct(false),
//       selectList(nullptr),
//       whereClause(nullptr) {}
// limit(nullptr) {}

// UpdateStatement
/*
UpdateStatement::UpdateStatement()
    : SQLStatement(kStmtUpdate),
      table(nullptr),
      updates(nullptr),
      where(nullptr) {}

UpdateStatement::~UpdateStatement() {
  delete table;
  delete where;

  if (updates) {
    for (UpdateClause* update : *updates) {
      free(update->column);
      delete update->value;
      delete update;
    }
    delete updates;
  }
}
*/
}  // namespace csql