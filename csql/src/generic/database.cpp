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
  if (!result->isValid()) {
    std::cout << "Parsing failed: " << result->errorMsg() << std::endl;
    return nullptr;
  }
  std::cout << *result;

  if (result->isValid()) {
    for (auto stmt : result->getStatements()) {
      if (stmt->is(kStmtCreate)) {
        create(std::dynamic_pointer_cast<CreateStatement>(stmt));
        return nullptr;
      } else if (stmt->is(kStmtInsert)) {
        insert(std::dynamic_pointer_cast<InsertStatement>(stmt));
        return nullptr;
      } else if (stmt->is(kStmtSelect)) {
        return select(std::dynamic_pointer_cast<SelectStatement>(stmt))->getIterator();
      } else if (stmt->is(kStmtDelete)) {
        delete_(std::dynamic_pointer_cast<DeleteStatement>(stmt));
        return nullptr;
        // } else if (stmt->is(kStmtUpdate)) {
        //   return update(std::dynamic_pointer_cast<UpdateStatement>(stmt));
      } else {
        std::cout << "Unknown statement" << std::endl;
      }
    }
  }

  return nullptr;
}

std::shared_ptr<ITable> Database::getTable(std::shared_ptr<Expr> tableRef) const {
  switch (tableRef->type) {
    case kExprTableRef: {
      if (tables_.count(tableRef->name) == 0) {
        throw std::runtime_error("Table not found: " + tableRef->name);
      }
      return tables_.at(tableRef->name);
    } break;
    case kExprSelect: {
      return select(tableRef->select);
    } break;
    case kExprJoin: {
      throw std::runtime_error("Join not supported");
      return nullptr;
    } break;
    case kExprOperator: {
      if (tableRef->opType == kOpParenthesis) {
        return getTable(tableRef->expr);
      } else {
        throw std::runtime_error("Unsupported expression type on getTable");
      }
    } break;
    default:
      throw std::runtime_error("Failed to get table");
  }
}

std::shared_ptr<ITable> Database::create(std::shared_ptr<CreateStatement> createStatement) {
  if (tables_.count(createStatement->tableName) > 0) {
    throw std::runtime_error("Table already exists: " + createStatement->tableName);
  }
  if (createStatement->type == CreateType::kCreateTable) {
    if (createStatement->columns->empty()) {
      throw std::runtime_error("No columns specified");
    }
    tables_[createStatement->tableName] = StorageTable::create(createStatement);
    return tables_[createStatement->tableName];
  } else if (createStatement->type == CreateType::kCreateTableAsSelect) {
    return tables_[createStatement->tableName] =
               StorageTable::create(createStatement, getTable(createStatement->sourceRef));
  } else {
    throw std::runtime_error("Unsupported create type");
  }
}

std::shared_ptr<ITable> Database::select(std::shared_ptr<SelectStatement> selectStatement) const {
  std::shared_ptr<ITable> table = getTable(selectStatement->fromSource);
  return table->select(selectStatement);
}

std::shared_ptr<ITable> Database::insert(std::shared_ptr<InsertStatement> insertStatement) {
  getTable(insertStatement->tableRef)->insert(insertStatement);
  return nullptr;
}

std::shared_ptr<ITable> Database::delete_(std::shared_ptr<DeleteStatement> deleteStatement) {
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