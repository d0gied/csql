#include <string>

#include "sql/statements/create.h"
#include "sql/statements/delete.h"
#include "sql/statements/insert.h"
#include "sql/statements/select.h"
#include "sql/statements/update.h"
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

std::string to_string(const ColumnType& column_type) {
  switch (column_type.data_type) {
    case DataType::UNKNOWN:
      return "UNKNOWN";
    case DataType::INT32:
      return "int32";
    case DataType::STRING:
      return "string[" + std::to_string(column_type.length) + "]";
    case DataType::BOOL:
      return "bool";
    case DataType::BYTES:
      return "bytes[" + std::to_string(column_type.length) + "]";
  }
  return "UNKNOWN";
}

std::ostream& operator<<(std::ostream& stream, const ColumnType& column_type) {
  stream << to_string(column_type);
  return stream;
}

// InsertStatement
ColumnValueDefinition::ColumnValueDefinition(const std::string& name, std::shared_ptr<Expr> value)
    : name(name), value(value), isNamed(true) {}

ColumnValueDefinition::ColumnValueDefinition(std::shared_ptr<Expr> value)
    : value(value), isNamed(false) {}

InsertStatement::InsertStatement(InsertType type, std::shared_ptr<Expr> table)
    : SQLStatement(kStmtInsert), insertType(type), tableRef(table) {}

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
  stream << "INSERT INTO " << *insert_statement.tableRef << " ";
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
std::ostream& operator<<(std::ostream& stream, const SelectStatement& select_statement) {
  stream << "SELECT ";
  if (select_statement.selectDistinct) {
    stream << "DISTINCT ";
  }
  for (size_t i = 0; i < select_statement.selectList->size(); i++) {
    stream << *select_statement.selectList->at(i);
    if (i + 1 < select_statement.selectList->size()) {
      stream << ", ";
    }
  }
  stream << " FROM " << *select_statement.fromSource;
  if (select_statement.whereClause) {
    stream << " WHERE " << *select_statement.whereClause;
  }
  return stream;
}

// DeleteStatement
std::ostream& operator<<(std::ostream& stream, const DeleteStatement& delete_statement) {
  stream << "DELETE FROM " << *delete_statement.tableRef;
  if (delete_statement.whereClause) {
    stream << " WHERE " << *delete_statement.whereClause;
  }

  return stream;
}

}  // namespace csql