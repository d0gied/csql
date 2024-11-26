
#include "parser_result.h"

#include <algorithm>

namespace csql {

SQLParserResult::SQLParserResult() : isValid_(false), errorMsg_("") {}

SQLParserResult::SQLParserResult(std::shared_ptr<SQLStatement> stmt)
    : isValid_(false), errorMsg_("") {
  addStatement(stmt);
}

void SQLParserResult::addStatement(std::shared_ptr<SQLStatement> stmt) {
  statements_.push_back(stmt);
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

const std::string& SQLParserResult::errorMsg() const {
  return errorMsg_;
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

void SQLParserResult::setErrorDetails(std::string errorMsg, int errorLine, int errorColumn) {
  errorMsg_ = errorMsg;
  errorLine_ = errorLine;
  errorColumn_ = errorColumn;
}

void SQLParserResult::setTokens(std::shared_ptr<std::vector<std::string>> tokens) {
  tokens_ = tokens;
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

}  // namespace csql