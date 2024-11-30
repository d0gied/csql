#pragma once
#include <boost/regex.hpp>
#include <string>

#include "grammar.h"

namespace csql {

std::string tokenTypeToString(TokenType type);

struct Token {
  TokenType type;
  std::string value;
};

class SQLTokenizer {
 public:
  SQLTokenizer(const std::string &sql);

  const Token get();
  const Token nextToken();
  bool hasNext() const;

 private:
  boost::sregex_token_iterator next;
  std::string sql_;
};
}  // namespace csql
