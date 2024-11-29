#include "tokenizer.h"

#include <boost/regex.hpp>
#include <cstring>
#include <string>

#include "grammar.h"

namespace {
std::string uppercase(const std::string &str) {
  std::string upper = str;
  std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
  return upper;
}
}  // namespace

namespace csql {
std::string tokenTypeToString(TokenType type) {
  switch (type) {
    case TokenType::NONE:
      return "NONE";
    case TokenType::KEYWORD:
      return "KEYWORD";
    case TokenType::TYPE:
      return "TYPE";
    case TokenType::NAME:
      return "NAME";
    case TokenType::NAME_LEN:
      return "NAME_LEN";
    case TokenType::ALL_COLS:
      return "ALL_COLS";
    case TokenType::OPERATOR:
      return "OPERATOR";
    case TokenType::INTEGER:
      return "INTEGER";
    case TokenType::HEX:
      return "HEX";
    case TokenType::PUNCTUATION:
      return "PUNCTUATION";
    case TokenType::WHITESPACE:
      return "WHITESPACE";
    case TokenType::TERMINAL:
      return "TERMINAL";
    case TokenType::STRING:
      return "STRING";
    default:
      break;
  }
  return "";
}

SQLTokenizer::SQLTokenizer(const std::string &sql) : sql_(sql) {
  boost::regex re(token::ALL, boost::regex::icase);
  next = boost::sregex_token_iterator(sql_.begin(), sql_.end(), re);
}

const Token SQLTokenizer::nextToken() {
  std::string value = *next;
  TokenType type = TokenType::NONE;

  if (boost::regex_match(value, boost::regex(token::KEYWORDS, boost::regex::icase))) {
    type = TokenType::KEYWORD;
    value = uppercase(value);
  } else if (boost::regex_match(value, boost::regex(token::TYPE, boost::regex::icase))) {
    type = TokenType::TYPE;
    value = uppercase(value);
  } else if (boost::regex_match(value, boost::regex(token::NAME))) {
    type = TokenType::NAME;
  } else if (boost::regex_match(value, boost::regex(token::NAME_LEN))) {
    type = TokenType::NAME_LEN;
  } else if (boost::regex_match(value, boost::regex(token::ALL_COLS))) {
    type = TokenType::ALL_COLS;
  } else if (boost::regex_match(value, boost::regex(token::OPERATOR))) {
    type = TokenType::OPERATOR;
  } else if (boost::regex_match(value, boost::regex(token::HEX))) {
    type = TokenType::HEX;
  } else if (boost::regex_match(value, boost::regex(token::INTEGER))) {
    type = TokenType::INTEGER;
  } else if (boost::regex_match(value, boost::regex(token::PUNCTUATION))) {
    type = TokenType::PUNCTUATION;
  } else if (boost::regex_match(value, boost::regex(token::WHITESPACE))) {
    type = TokenType::WHITESPACE;
  } else if (boost::regex_match(value, boost::regex(token::TERMINAL))) {
    type = TokenType::TERMINAL;
  } else if (boost::regex_match(value, boost::regex(token::STRING))) {
    type = TokenType::STRING;
  }
  ++next;
  return type != TokenType::WHITESPACE ? Token{type, value} : nextToken();  // Skip whitespaces
}

bool SQLTokenizer::hasNext() const {
  return next != boost::sregex_token_iterator();
}

}  // namespace csql