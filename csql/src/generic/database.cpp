#include "database.h"

#include <iostream>
#include <memory>

#include "row.h"
#include "sql/expr.h"
#include "sql/parser.h"
#include "sql/statements/create.h"
#include "sql/statements/delete.h"
#include "sql/statements/insert.h"
#include "sql/statements/select.h"
#include "sql/statements/statement.h"
#include "sql/statements/update.h"
#include "table.h"

namespace csql {
namespace storage {

std::shared_ptr<TableIterator> Database::execute(const std::string& sql) {
  std::shared_ptr<SQLParserResult> result = std::make_shared<SQLParserResult>();
  SQLParser::parse(sql, result);
  std::cout << *result << std::endl;

  if (result->isValid()) {
    for (auto stmt : result->getStatements()) {
      if (stmt->is(kStmtCreate)) {
        return create(std::dynamic_pointer_cast<CreateStatement>(stmt));
      } else if (stmt->is(kStmtInsert)) {
        return insert(std::dynamic_pointer_cast<InsertStatement>(stmt));
      } else if (stmt->is(kStmtSelect)) {
        return select(std::dynamic_pointer_cast<SelectStatement>(stmt));
      } else if (stmt->is(kStmtDelete)) {
        return delete_(std::dynamic_pointer_cast<DeleteStatement>(stmt));
        // } else if (stmt->is(kStmtUpdate)) {
        //   return update(std::dynamic_pointer_cast<UpdateStatement>(stmt));
      } else {
        std::cout << "Unknown statement" << std::endl;
      }
    }
  } else {
    std::cout << "Parsing failed: " << result->errorMsg() << std::endl;
  }

  return nullptr;
}

std::shared_ptr<ITable> Database::getTable(std::shared_ptr<Expr> tableRef) const {
  if (tableRef->type != kExprTableRef) {
    throw std::runtime_error("Only table sources are supported");
  }
  if (tables_.count(tableRef->name) == 0) {
    throw std::runtime_error("Table not found: " + tableRef->name);
  }
  return tables_.at(tableRef->name);
}

std::shared_ptr<TableIterator> Database::create(std::shared_ptr<CreateStatement> createStatement) {
  tables_[createStatement->tableName] = StorageTable::create(createStatement);
  return nullptr;
}

std::shared_ptr<TableIterator> Database::select(std::shared_ptr<SelectStatement> selectStatement) {
  return std::make_shared<WhereClauseIterator>(getTable(selectStatement->fromSource)->getIterator(),
                                               selectStatement->whereClause);
}

std::shared_ptr<TableIterator> Database::insert(std::shared_ptr<InsertStatement> insertStatement) {
  getTable(insertStatement->tableRef)->insert(insertStatement);
  return nullptr;
}

std::shared_ptr<TableIterator> Database::delete_(std::shared_ptr<DeleteStatement> deleteStatement) {
  getTable(deleteStatement->tableRef)->delete_(deleteStatement);
  return nullptr;
}

// std::shared_ptr<TableIterator> Database::update(std::shared_ptr<UpdateStatement> updateStatement)
// {

//   if (tables_.count(updateStatement->tableRef->name) == 0) {
//     throw std::runtime_error("Table not found: " + updateStatement->tableName);
//   }
//   tables_[updateStatement->tableName]->update(updateStatement);
//   return nullptr;
// }

void Database::exportTableToCSV(const std::string& tableName, const std::string& filename) {
  // export table to csv
  if (tables_.count(tableName) == 0) {
    throw std::runtime_error("Table not found: " + tableName);
  }
  tables_[tableName]->exportToCSV(filename);
}

}  // namespace storage
}  // namespace csql