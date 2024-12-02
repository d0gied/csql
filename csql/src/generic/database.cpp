#include "database.h"

#include <iostream>
#include <memory>

#include "planning/planning.h"
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
    throw std::runtime_error(result->errorMsg());
  }

  std::cout << *result;
  for (auto stmt : result->getStatements()) {
    if (stmt->is(kStmtCreate)) {
      create(std::dynamic_pointer_cast<CreateStatement>(stmt));
    } else if (stmt->is(kStmtInsert)) {
      insert(std::dynamic_pointer_cast<InsertStatement>(stmt));
    } else if (stmt->is(kStmtSelect)) {
      return execute(plan(stmt))->getIterator();
    } else if (stmt->is(kStmtDelete)) {
      delete_(std::dynamic_pointer_cast<DeleteStatement>(stmt));
      // } else if (stmt->is(kStmtUpdate)) {
      //   return update(std::dynamic_pointer_cast<UpdateStatement>(stmt));
    } else {
      std::cout << "Unknown statement" << std::endl;
    }
  }

  return nullptr;
}

std::shared_ptr<QueryPlan> Database::plan(std::shared_ptr<SQLStatement> statement) {
  if (statement->is(kStmtSelect)) {
    auto select = std::dynamic_pointer_cast<SelectStatement>(statement);
    return QueryPlan::create(Expr::makeSelect(select), shared_from_this());
  } else if (statement->is(kStmtCreate)) {
    auto create = std::dynamic_pointer_cast<CreateStatement>(statement);
    auto db = shared_from_this();
    return QueryPlan::create(create->sourceRef, db);
  } else if (statement->is(kStmtInsert)) {
    throw std::runtime_error("INSERT not supported for planning");
  } else if (statement->is(kStmtDelete)) {
    throw std::runtime_error("DELETE not supported for planning");
  } else {
    throw std::runtime_error("Unsupported statement");
  }
}

std::shared_ptr<QueryPlan> Database::plan(const std::string& sql) {
  std::shared_ptr<SQLParserResult> result = std::make_shared<SQLParserResult>();
  SQLParser::parse(sql, result);

  if (!result->isValid()) {
    throw std::runtime_error(result->errorMsg());
  }

  if (result->getStatements().size() != 1) {
    throw std::runtime_error("Only one statement is supported for planning");
  }
  return plan(result->getStatement(0));
}

std::shared_ptr<ITable> Database::execute(std::shared_ptr<QueryPlan> plan) {
  if (plan->type_ == QueryType::kStepFilter) {
    auto table = execute(plan->left_);
    std::cout << "Executing plan: " << plan->toString() << std::endl;
    if (!plan->query_) {
      throw std::runtime_error("Where clause not found");
    }
    if (!table) {
      throw std::runtime_error("Table not found");
    }
    return table->filter(plan->query_);
  } else if (plan->type_ == QueryType::kStepJoin) {
    auto left = execute(plan->left_);
    auto right = execute(plan->right_);
    std::cout << "Executing plan: " << plan->toString() << std::endl;
    if (!left || !right) {
      throw std::runtime_error("Table not found");
    }
    return JoinTable::create(left, right, plan->query_->on, plan->query_->opType);
  } else if (plan->type_ == QueryType::kStepProject) {
    std::cout << "Executing plan: " << plan->toString() << std::endl;
    if (plan->query_->type == kExprTableRef) {
      return getTable(plan->query_);
    }
    throw std::runtime_error("Unsupported query type");
  } else if (plan->type_ == QueryType::kStepEval) {
    auto left = execute(plan->left_);
    std::cout << "Executing plan: " << plan->toString() << std::endl;
    if (!left) {
      throw std::runtime_error("Table not found");
    }
    return EvaluatedTable::create(left, plan->query_->select->selectList);
  } else if (plan->type_ == QueryType::kStepFullScan) {
    auto left = execute(plan->left_);  // Return table itself
    std::cout << "Executing plan: " << plan->toString() << std::endl;
    if (!left) {
      throw std::runtime_error("Table not found");
    }
    return left;
  } else if (plan->type_ == QueryType::kStepRangeScan) {
    std::cout << "Executing plan: " << plan->toString() << std::endl;
    throw std::runtime_error("Range scan not supported");
  } else {
    throw std::runtime_error("Unsupported query type");
  }
}

std::shared_ptr<ITable> Database::getTable(std::shared_ptr<Expr> tableRef) const {
  if (tableRef->type == kExprOperator && tableRef->opType == kOpParenthesis) {
    return getTable(tableRef->expr);
  }
  if (tableRef->type != kExprTableRef) {
    throw std::runtime_error("Unsupported expression type on getTable: " +
                             std::to_string(tableRef->type));
  }
  if (tables_.count(tableRef->name) == 0) {
    throw std::runtime_error("Table not found: " + tableRef->name);
  }
  return tables_.at(tableRef->name);
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
    auto refTable = execute(plan(createStatement));
    return tables_[createStatement->tableName] = StorageTable::create(createStatement, refTable);
  } else {
    throw std::runtime_error("Unsupported create type");
  }
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