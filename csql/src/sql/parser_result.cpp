
#include "parser_result.h"

#include <algorithm>

#include "sql/parser.h"
#include "sql/statements/create.h"
#include "sql/statements/delete.h"
#include "sql/statements/insert.h"
#include "sql/statements/select.h"
#include "sql/statements/update.h"

namespace csql {

SQLParserResult::SQLParserResult() : isValid_(false), errorMsg_("") {}

SQLParserResult::SQLParserResult(std::shared_ptr<SQLStatement> stmt)
    : isValid_(false), errorMsg_("") {
  addStatement(stmt);
}

void SQLParserResult::addStatement(std::shared_ptr<SQLStatement> stmt) {
  statements_.push_back(stmt);
}

std::shared_ptr<SQLStatement> SQLParserResult::popStatement() {
  if (statements_.empty()) {
    return nullptr;
  }
  auto stmt = statements_.back();
  statements_.pop_back();
  return stmt;
}

std::shared_ptr<SQLStatement> SQLParserResult::getStatement(size_t index) const {
  return statements_[index];
}

size_t SQLParserResult::size() const {
  return statements_.size();
}

bool SQLParserResult::isValid() const {
  return isValid_;
}

const std::string SQLParserResult::errorMsg() const {
  return errorMsg_ + "\n" + "Got: " + token_.value +
         "\nToken type: " + tokenTypeToString(token_.type);
}

int SQLParserResult::errorLine() const {
  return errorLine_;
}

int SQLParserResult::errorColumn() const {
  return errorColumn_;
}

void SQLParserResult::setIsValid(bool isValid) {
  isValid_ = isValid;
}

void SQLParserResult::setErrorDetails(std::string errorMsg, int errorLine, int errorColumn,
                                      Token token) {
  isValid_ = false;
  errorMsg_ = errorMsg;
  errorLine_ = errorLine;
  errorColumn_ = errorColumn;
  token_ = token;
}

const std::vector<std::shared_ptr<SQLStatement>>& SQLParserResult::getStatements() const {
  return statements_;
}

void SQLParserResult::reset() {
  statements_.clear();

  isValid_ = false;
  errorMsg_ = nullptr;
  errorLine_ = -1;
  errorColumn_ = -1;
}

std::ostream& operator<<(std::ostream& stream, const SQLParserResult& result) {
  if (result.isValid()) {
    for (const auto& stmt : result.getStatements()) {
      if (stmt->is(kStmtCreate)) {
        stream << *std::dynamic_pointer_cast<CreateStatement>(stmt);
      } else if (stmt->is(kStmtInsert)) {
        stream << *std::dynamic_pointer_cast<InsertStatement>(stmt);
      } else if (stmt->is(kStmtSelect)) {
        stream << *std::dynamic_pointer_cast<SelectStatement>(stmt);
      } else if (stmt->is(kStmtDelete)) {
        stream << *std::dynamic_pointer_cast<DeleteStatement>(stmt);
      } else if (stmt->is(kStmtUpdate)) {
        stream << *std::dynamic_pointer_cast<UpdateStatement>(stmt);
      } else {
        stream << "Unknown statement";
      }
      stream << ";" << std::endl;
    }
  } else {
    stream << "Invalid: " << result.errorMsg();
  }
  return stream;
}

}  // namespace csql