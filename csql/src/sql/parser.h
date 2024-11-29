#pragma once
#include <boost/regex.hpp>
#include <string>

#include "grammar.h"
#include "parser_result.h"
#include "sql/statements.h"

namespace csql {

class SQLParser {
 public:
  static bool parse(const std::string &sql, std::shared_ptr<SQLParserResult> result);

 private:
  SQLParser();
};

}  // namespace csql
