#pragma once
#include <string>

#include "parser_result.h"
#include "sql/statements.h"

namespace csql {

// Static methods used to parse SQL strings.
class SQLParser {
 public:
  static bool parse(const std::string &sql, std::shared_ptr<SQLParserResult> result);

 private:
  SQLParser();

  static bool parseCreate(const std::vector<std::string> &tokens,
                          std::shared_ptr<SQLParserResult> result);
  static bool parseCreateTable(const std::vector<std::string> &tokens,
                               std::shared_ptr<SQLParserResult> result);
  //   static bool parseSelect(const std::vector<std::string>& tokens,
  //                           SQLParserResult* result);
  static bool parseInsert(const std::vector<std::string> &tokens,
                          std::shared_ptr<SQLParserResult> result);
  //   static bool parseDelete(const std::vector<std::string>& tokens,
  //                           SQLParserResult* result);
  //   static bool parseDrop(const std::vector<std::string>& tokens,
  //                         SQLParserResult* result);
  static std::shared_ptr<std::vector<std::string>> tokenize(const std::string &sql);
};

}  // namespace csql
