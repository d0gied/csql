#pragma once

#include <boost/regex/v5/regex_fwd.hpp>
#include <regex>
#include <string>
#include <string_view>

namespace {
const std::string join(const std::vector<std::string> &v, const std::string &delim) {
  std::string s;
  for (size_t i = 0; i < v.size(); ++i) {
    s += v[i];
    if (i != v.size() - 1) {
      s += delim;
    }
  }
  return s;
}

}  // namespace

namespace csql {

enum class TokenType {
  NONE,
  KEYWORD,
  TYPE,
  NAME,
  COLUMN_NAME,
  ALL_COLS,
  OPERATOR,
  INTEGER,
  HEX,
  PUNCTUATION,
  WHITESPACE,
  TERMINAL,
  STRING,
};

namespace token {
const std::string KEYWORDS =
    "SELECT|INSERT|CREATE|DELETE|UPDATE|DROP|TO|FROM|WHERE|AND|OR|TABLE|ORDERED INDEX|UNORDERED "
    "INDEX|AUTOINCREMENT|UNIQUE|KEY|TRUE|FALSE|IS NULL|IS NOT NULL|NULL|NOT|SET|JOIN|ON";
const std::string TYPE = "BOOL|INT32|STRING\\[\\d+\\]|BYTES\\[\\d+\\]";
const std::string NAME = "[a-zA-Z_][a-zA-Z_0-9]*";
const std::string COLUMN_NAME = NAME + "\\." + NAME;  // table.column
const std::string ALL_COLS = "\\*";
const std::string OPERATOR = ">=|>|<=|<|=|!=|\\(|\\)|\\||\\+|\\-|\\*|\\/|\\%|\\&|\\~";
const std::string INTEGER = "\\d+";
const std::string HEX = "0x[0-9a-fA-F]+";
const std::string PUNCTUATION = "[,\\:\\{\\}]";
const std::string WHITESPACE = "[ \\n]";
const std::string TERMINAL = ";";
const std::string STRING = R"((\"[^\"]*\")|(\'[^\']*\'))";

const std::string ALL = "(" +
                        join(
                            {
                                KEYWORDS,
                                TYPE,
                                COLUMN_NAME,
                                NAME,
                                ALL_COLS,
                                OPERATOR,
                                HEX,
                                INTEGER,
                                PUNCTUATION,
                                WHITESPACE,
                                TERMINAL,
                                STRING,
                            },
                            ")|(") +
                        ")";
}  // namespace token

}  // namespace csql