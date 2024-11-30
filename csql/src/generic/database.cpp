#include "database.h"

#include <iostream>
#include <memory>

#include "row.h"
#include "sql/parser.h"
#include "sql/statements.h"
#include "sql/statements/statement.h"
#include "table.h"

namespace csql {
namespace storage {

std::shared_ptr<TableIterator> Database::execute(const std::string& sql) {
  std::shared_ptr<SQLParserResult> result = std::make_shared<SQLParserResult>();
  SQLParser::parse(sql, result);
  std::cout << *result;

  if (result->isValid()) {
    for (auto stmt : result->getStatements()) {
      if (stmt->is(kStmtCreate)) {
        return create(std::dynamic_pointer_cast<CreateStatement>(stmt));
      } else if (stmt->is(kStmtInsert)) {
        // return nullptr;
        return insert(std::dynamic_pointer_cast<InsertStatement>(stmt));
      } else if (stmt->is(kStmtSelect)) {
        return select(std::dynamic_pointer_cast<SelectStatement>(stmt));
      } else {
        std::cout << "Unknown statement" << std::endl;
      }
    }
  } else {
    std::cout << "Parsing failed: " << result->errorMsg() << std::endl;
  }

  return nullptr;
}

std::shared_ptr<TableIterator> Database::create(std::shared_ptr<CreateStatement> createStatement) {
  tables_[createStatement->tableName] = Table::create(createStatement);

  return nullptr;
}

std::shared_ptr<TableIterator> Database::select(std::shared_ptr<SelectStatement> selectStatement) {
  if (tables_.count(selectStatement->fromTable) == 0) {
    throw std::runtime_error("Table not found: " + selectStatement->fromTable);
  }
  return std::make_shared<WhereClauseIterator>(tables_[selectStatement->fromTable]->getIterator(),
                                               selectStatement->whereClause);
}

std::shared_ptr<TableIterator> Database::insert(std::shared_ptr<InsertStatement> insertStatement) {
  if (tables_.count(insertStatement->tableName) == 0) {
    throw std::runtime_error("Table not found: " + insertStatement->tableName);
  }
  tables_[insertStatement->tableName]->insert(insertStatement);
  return nullptr;
}

void Database::exportTableToCSV(const std::string& tableName, const std::string& filename) {
  // export table to csv
  if (tables_.count(tableName) == 0) {
    throw std::runtime_error("Table not found: " + tableName);
  }
  tables_[tableName]->exportToCSV(filename);
}

}  // namespace storage
}  // namespace csql