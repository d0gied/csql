#include "create.h"

#include "select.h"

namespace csql {

// CreateStatemnet
CreateStatement::CreateStatement(CreateType type)
    : SQLStatement(kStmtCreate),
      type(type),
      ifNotExists(false),
      tableName(""),
      indexName(""),
      indexColumns(nullptr),
      columns(nullptr),
      tableConstraints(nullptr) {}

void CreateStatement::setColumnDefs(
    std::shared_ptr<std::vector<std::shared_ptr<ColumnDefinition>>> _columns) {
  columns = _columns;
}

std::ostream& operator<<(std::ostream& stream, const ColumnDefinition& column) {
  for (const auto& constraint : *column.column_constraints) {
    switch (constraint) {
      case ConstraintType::None:
        break;
      case ConstraintType::Key:
        stream << "KEY ";
        break;
      case ConstraintType::Unique:
        stream << "UNIQUE ";
        break;
      case ConstraintType::AutoIncrement:
        stream << "AUTOINCREMENT ";
        break;
    }
  }
  stream << column.name << " " << column.type;
  if (column.default_value) {
    stream << " DEFAULT " << *column.default_value;
  }
  return stream;
}

std::ostream& operator<<(std::ostream& stream, const CreateStatement& create) {
  stream << "CREATE ";
  switch (create.type) {
    case CreateType::kCreateTable:
      stream << "TABLE ";
      if (create.ifNotExists) {
        stream << "IF NOT EXISTS ";
      }
      stream << create.tableName << " (\n";
      for (auto column : *create.columns) {
        stream << *column << ",\n";
      }
      stream << ")";
      break;
    case CreateType::kCreateIndex:
      stream << "INDEX ";
      break;
    case CreateType::kCreateTableAsSelect:
      stream << "TABLE ";
      stream << create.tableName << " AS " << *create.sourceRef;
      break;
  }
  return stream;
}

}  // namespace csql