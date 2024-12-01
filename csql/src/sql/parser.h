#pragma once
#include <boost/regex.hpp>
#include <string>

#include "parser_result.h"

namespace csql {

class SQLParser {
 public:
  static bool parse(const std::string &sql, std::shared_ptr<SQLParserResult> result);

 private:
  SQLParser();
};

}  // namespace csql
