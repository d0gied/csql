#include "sql/parser.h"

#include <boost/regex.hpp>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "sql/column_type.h"
#include "sql/expr.h"
#include "sql/grammar.h"
#include "sql/parser_result.h"
#include "sql/statements/create.h"
#include "sql/statements/insert.h"
#include "sql/tokenizer.h"

namespace {

csql::ColumnType columnTypeFromString(const std::string &type) {
  if (type == "BOOL") {
    return csql::ColumnType(csql::DataType::BOOL);
  } else if (type == "INT32") {
    return csql::ColumnType(csql::DataType::INT32);
  } else if (type.length() > 6 && type.substr(0, 6) == "STRING") {  // string[32]
    int64_t length = std::stoll(type.substr(7, type.length() - 8));
    return csql::ColumnType(csql::DataType::STRING, length);
  } else if (type.length() > 5 && type.substr(0, 5) == "BYTES") {  // bytes[32]
    int64_t length = std::stoll(type.substr(6, type.length() - 7));
    return csql::ColumnType(csql::DataType::BYTES, length);
  }
  throw std::runtime_error("Unknown type: " + type);
}

std::shared_ptr<csql::ColumnDefinition> parseColumnDefinition(
    csql::SQLTokenizer &tokenizer, std::shared_ptr<csql::SQLParserResult> result, bool &hasComma,
    bool &hasParen) {
  csql::Token token = tokenizer.nextToken();

  std::string name;
  std::unordered_set<csql::ConstraintType> constraints;
  csql::ColumnType type;
  std::shared_ptr<csql::Expr> default_value;

  if (token.value == "{") {
    while (token.value != "}") {
      token = tokenizer.nextToken();
      if (token.value == "AUTOINCREMENT") {
        constraints.insert(csql::ConstraintType::AutoIncrement);
      } else if (token.value == "UNIQUE") {
        constraints.insert(csql::ConstraintType::Unique);
      } else if (token.value == "KEY") {
        constraints.insert(csql::ConstraintType::Key);
      } else {
        result->setErrorDetails("Expected AUTOINCREMENT, UNIQUE or KEY", 0, 0, token);
        return nullptr;
      }
      token = tokenizer.nextToken();
      if (token.value != "," && token.value != "}") {
        result->setErrorDetails("Expected , or }", 0, 0, token);
        return nullptr;
      }
    }
    token = tokenizer.nextToken();  // Consume }
  }

  if (token.type != csql::TokenType::NAME) {
    result->setErrorDetails("Expected column name", 0, 0, token);
    return nullptr;
  }
  name = token.value;

  token = tokenizer.nextToken();
  if (token.value != ":") {
    result->setErrorDetails("Expected :", 0, 0, token);
    return nullptr;
  }

  token = tokenizer.nextToken();
  if (token.type != csql::TokenType::TYPE) {
    result->setErrorDetails("Expected column type", 0, 0, token);
    return nullptr;
  }
  type = columnTypeFromString(token.value);

  token = tokenizer.nextToken();
  if (token.value == "=") {
    token = tokenizer.nextToken();
    if (token.type != csql::TokenType::STRING && token.type != csql::TokenType::INTEGER &&
        token.type != csql::TokenType::HEX && token.value != "TRUE" && token.value != "FALSE") {
      result->setErrorDetails("Expected string, integer, hex or boolean", 0, 0, token);
      return nullptr;
    }
    default_value = csql::Expr::makeLiteral(token.value);
    token = tokenizer.nextToken();
  }

  if (token.value == ",") {
    hasComma = true;
  } else if (token.value == ")") {
    hasParen = true;
  } else {
    result->setErrorDetails("Expected , or )", 0, 0, token);
    return nullptr;
  }

  return std::make_shared<csql::ColumnDefinition>(
      name, type, std::make_shared<std::unordered_set<csql::ConstraintType>>(constraints),
      default_value);
}

bool parseCreateTable(csql::SQLTokenizer &tokenizer,
                      std::shared_ptr<csql::SQLParserResult> result) {
  csql::Token token = tokenizer.nextToken();
  if (token.type != csql::TokenType::NAME) {
    result->setErrorDetails("Expected table name", 0, 0, token);
    return false;
  }
  std::string tableName = token.value;

  token = tokenizer.nextToken();
  if (token.value != "(") {
    result->setErrorDetails("Expected (", 0, 0, token);
    return false;
  }

  std::shared_ptr<csql::CreateStatement> createStatement =
      std::make_shared<csql::CreateStatement>(csql::CreateType::kCreateTable);
  createStatement->tableName = tableName;
  createStatement->columns =
      std::make_shared<std::vector<std::shared_ptr<csql::ColumnDefinition>>>();

  bool hasComma = false;
  bool hasParen = false;

  while (!hasParen) {
    std::shared_ptr<csql::ColumnDefinition> column =
        parseColumnDefinition(tokenizer, result, hasComma, hasParen);
    if (!column) {
      return false;
    }
    createStatement->columns->push_back(column);
  }

  result->addStatement(createStatement);
  result->setIsValid(true);
  return true;
}

bool parseCreate(csql::SQLTokenizer &tokenizer, std::shared_ptr<csql::SQLParserResult> result) {
  csql::Token token = tokenizer.nextToken();
  if (token.value == "TABLE") {
    return parseCreateTable(tokenizer, result);
  } else if (token.value == "ORDERED INDEX") {
    result->setErrorDetails("Ordered indexes are not supported", 0, 0, token);
    return false;
  } else if (token.value == "UNORDERED INDEX") {
    result->setErrorDetails("Unordered indexes are not supported", 0, 0, token);
    return false;
  }
  result->setErrorDetails("Expected TABLE, ORDERED INDEX or UNORDERED INDEX", 0, 0, token);
  return false;
}

std::shared_ptr<csql::ColumnValueDefinition> parseColumnValueDefinition(
    csql::SQLTokenizer &tokenizer, std::shared_ptr<csql::SQLParserResult> result, bool &hasComma,
    bool &hasParen) {
  csql::Token token = tokenizer.nextToken();
  if (token.value == ")") {
    hasParen = true;
    return nullptr;
  }

  if (token.type == csql::TokenType::NAME) {
    std::string name = token.value;
    token = tokenizer.nextToken();
    if (token.value != "=") {
      result->setErrorDetails("Expected =", 0, 0, token);
      return nullptr;
    }
    token = tokenizer.nextToken();
    if (token.type != csql::TokenType::STRING && token.type != csql::TokenType::INTEGER &&
        token.type != csql::TokenType::HEX && token.value != "TRUE" && token.value != "FALSE") {
      result->setErrorDetails("Expected string, integer, hex or boolean", 0, 0, token);
      return nullptr;
    }
    std::string value = token.value;

    token = tokenizer.nextToken();
    if (token.value == ",") {
      hasComma = true;
    } else if (token.value == ")") {
      hasParen = true;
    } else {
      result->setErrorDetails("Expected , or )", 0, 0, token);
      return nullptr;
    }
    return std::make_shared<csql::ColumnValueDefinition>(name, csql::Expr::makeLiteral(value));
  } else if (token.type != csql::TokenType::STRING && token.type != csql::TokenType::INTEGER &&
             token.type != csql::TokenType::HEX && token.value != "TRUE" &&
             token.value != "FALSE") {
    result->setErrorDetails("Expected string, integer, hex or boolean", 0, 0, token);
    return nullptr;
  } else {
    std::string value = token.value;
    token = tokenizer.nextToken();
    if (token.value == ",") {
      hasComma = true;
    } else if (token.value == ")") {
      hasParen = true;
    } else {
      result->setErrorDetails("Expected , or )", 0, 0, token);
      return nullptr;
    }
    return std::make_shared<csql::ColumnValueDefinition>(csql::Expr::makeLiteral(value));
  }
}

bool parseInsert(csql::SQLTokenizer &tokenizer, std::shared_ptr<csql::SQLParserResult> result) {
  csql::Token token = tokenizer.nextToken();
  if (token.value != "(") {
    result->setErrorDetails("Expected (", 0, 0, token);
    return false;
  }

  bool hasComma = false;
  bool hasParen = false;

  std::string tableName;

  std::shared_ptr<std::vector<std::shared_ptr<csql::ColumnValueDefinition>>> columnValues =
      std::make_shared<std::vector<std::shared_ptr<csql::ColumnValueDefinition>>>();

  while (!hasParen) {
    std::shared_ptr<csql::ColumnValueDefinition> columnValue =
        parseColumnValueDefinition(tokenizer, result, hasComma, hasParen);
    if (columnValue) {
      columnValues->push_back(columnValue);
    } else if (!hasParen) {
      return false;
    }
  }

  token = tokenizer.nextToken();
  if (token.value != "TO") {
    result->setErrorDetails("Expected TO", 0, 0, token);
    return false;
  }

  token = tokenizer.nextToken();
  if (token.type != csql::TokenType::NAME) {
    result->setErrorDetails("Expected table name", 0, 0, token);
    return false;
  }
  tableName = token.value;

  csql::InsertType insertType = csql::InsertType::kUnknown;
  for (const auto &columnValue : *columnValues) {
    if (insertType == csql::InsertType::kUnknown) {
      insertType = columnValue->isNamed ? csql::InsertType::kInsertKeysValues
                                        : csql::InsertType::kInsertValues;
    } else if ((insertType == csql::InsertType::kInsertKeysValues && !columnValue->isNamed) ||
               (insertType == csql::InsertType::kInsertValues && columnValue->isNamed)) {
      result->setErrorDetails("Cannot mix named and unnamed values", 0, 0, token);
      return false;
    }
  }

  std::shared_ptr<csql::InsertStatement> insertStatement =
      std::make_shared<csql::InsertStatement>(insertType, tableName);
  insertStatement->columnValues = columnValues;

  result->addStatement(insertStatement);
  result->setIsValid(true);
  return true;
}

std::shared_ptr<csql::Expr> applyOpOnSameLevel(
    std::shared_ptr<csql::Expr> left, csql::OperatorType op,
    std::shared_ptr<csql::Expr> right = nullptr,
    bool isInlineNot = false  // inline NOT operator (IS NOT NULL)
) {
  if (csql::isUnaryOperator(op)) {
    if (left->isLiteral() || !(left->opType < op)) {
      auto res = csql::Expr::makeOpUnary(op, left);
      if (isInlineNot) {
        res = csql::Expr::makeOpUnary(csql::OperatorType::kOpNot, res);
      }
      return res;
    } else if (isUnaryOperator(left->opType)) {
      left->expr = applyOpOnSameLevel(left->expr, op, nullptr, isInlineNot);
      return left;
    } else {
      left->expr2 = applyOpOnSameLevel(left->expr2, op, nullptr, isInlineNot);
      return left;
    }
  } else {
    if (left->isLiteral()) {
      return csql::Expr::makeOpBinary(left, op, right);  // left is a literal or unary operator
    } else if (left->opType < op) {
      if (isUnaryOperator(left->opType)) {
        left->expr = applyOpOnSameLevel(left->expr, op, right);
        return left;
      } else {
        left->expr2 = applyOpOnSameLevel(left->expr2, op, right);  // go down the tree
        return left;
      }
    } else {
      return csql::Expr::makeOpBinary(left, op, right);  // left is higher or equal
    }
  }
}

std::shared_ptr<csql::Expr> parseExpr(csql::SQLTokenizer &tokenizer,
                                      std::shared_ptr<csql::SQLParserResult> result,
                                      const std::string_view until = "") {
  csql::Token token = tokenizer.nextToken();
  std::shared_ptr<csql::Expr> left;

  if (token.value == "(") {
    left = csql::Expr::makeOpUnary(csql::OperatorType::kOpParenthesis,
                                   parseExpr(tokenizer, result, ")"));
  } else if (token.value == "|") {
    left =
        csql::Expr::makeOpUnary(csql::OperatorType::kOpLength, parseExpr(tokenizer, result, "|"));
  } else if (token.value == "NOT") {
    left = csql::Expr::makeOpUnary(csql::OperatorType::kOpNot, parseExpr(tokenizer, result));
  } else if (token.value == "-") {
    left = csql::Expr::makeOpUnary(csql::OperatorType::kOpUnaryMinus, parseExpr(tokenizer, result));
  } else if (token.value == "~") {
    left = csql::Expr::makeOpUnary(csql::OperatorType::kOpBitNot, parseExpr(tokenizer, result));
  } else if (token.type == csql::TokenType::INTEGER) {
    left = csql::Expr::makeLiteral(std::stoi(token.value));
  } else if (token.type == csql::TokenType::STRING) {
    left = csql::Expr::makeLiteral(token.value);
  } else if (token.type == csql::TokenType::HEX) {
    left = csql::Expr::makeLiteral(token.value);
  } else if (token.value == "TRUE") {
    left = csql::Expr::makeLiteral(true);
  } else if (token.value == "FALSE") {
    left = csql::Expr::makeLiteral(false);
  } else if (token.type == csql::TokenType::NAME || token.type == csql::TokenType::COLUMN_NAME) {
    left = csql::Expr::makeColumnRef(token.value);
  } else {
    result->setErrorDetails("Expected expression", 0, 0, token);
    return nullptr;
  }
  if (until.empty()) {
    return left;
  }

  token = tokenizer.nextToken();
  while (token.value != until && token.type != csql::TokenType::TERMINAL) {
    if (token.type != csql::TokenType::OPERATOR && token.value != "OR" && token.value != "AND" &&
        token.value != "IS NULL" && token.value != "IS NOT NULL") {
      result->setErrorDetails("Expected operator", 0, 0, token);
      return nullptr;
    }

    csql::OperatorType op;
    if (token.value == "OR") {
      op = csql::OperatorType::kOpOr;
    } else if (token.value == "AND") {
      op = csql::OperatorType::kOpAnd;
    } else if (token.value == "+") {
      op = csql::OperatorType::kOpPlus;
    } else if (token.value == "-") {
      op = csql::OperatorType::kOpMinus;
    } else if (token.value == "*") {
      op = csql::OperatorType::kOpAsterisk;
    } else if (token.value == "/") {
      op = csql::OperatorType::kOpSlash;
    } else if (token.value == "%") {
      op = csql::OperatorType::kOpPercentage;
    } else if (token.value == "=") {
      op = csql::OperatorType::kOpEquals;
    } else if (token.value == "!=") {
      op = csql::OperatorType::kOpNotEquals;
    } else if (token.value == "<") {
      op = csql::OperatorType::kOpLess;
    } else if (token.value == "<=") {
      op = csql::OperatorType::kOpLessEq;
    } else if (token.value == ">") {
      op = csql::OperatorType::kOpGreater;
    } else if (token.value == ">=") {
      op = csql::OperatorType::kOpGreaterEq;
    } else if (token.value == "IN") {
      op = csql::OperatorType::kOpIn;
    } else if (token.value == "IS NULL" || token.value == "IS NOT NULL") {
      op = csql::OperatorType::kOpIsNull;
    } else {
      result->setErrorDetails("Unknown operator", 0, 0, token);
      return nullptr;
    }

    if (op == csql::OperatorType::kOpIn) {
      result->setErrorDetails("IN operator is not supported", 0, 0, token);
      return nullptr;
    } else if (op == csql::OperatorType::kOpIsNull) {
      left = applyOpOnSameLevel(left, op, nullptr, token.value == "IS NOT NULL");
    } else {
      std::shared_ptr<csql::Expr> right = parseExpr(tokenizer, result);
      if (!right) {
        return nullptr;
      }
      left = applyOpOnSameLevel(left, op, right);  // if op is higher precedence, it will go down
                                                   // the tree until it finds the right place
    }

    token = tokenizer.nextToken();
  }
  return left;
}

bool parseSelect(csql::SQLTokenizer &tokenizer, std::shared_ptr<csql::SQLParserResult> result) {
  csql::Token token = tokenizer.nextToken();
  std::shared_ptr<std::vector<std::shared_ptr<csql::Expr>>> selectList =
      std::make_shared<std::vector<std::shared_ptr<csql::Expr>>>();
  if (token.value != "*") {
    result->setErrorDetails("Only * is supported", 0, 0, token);
    return false;
  }
  selectList->push_back(csql::Expr::makeStar());

  token = tokenizer.nextToken();
  if (token.value != "FROM") {
    result->setErrorDetails("Expected FROM", 0, 0, token);
    return false;
  }

  token = tokenizer.nextToken();
  if (token.type != csql::TokenType::NAME) {
    result->setErrorDetails("Expected table name", 0, 0, token);
    return false;
  }

  std::shared_ptr<csql::SelectStatement> selectStatement =
      std::make_shared<csql::SelectStatement>();
  selectStatement->fromTable = token.value;
  selectStatement->selectList = selectList;

  token = tokenizer.nextToken();
  if (token.value != "WHERE") {
    result->setErrorDetails("Expected WHERE", 0, 0, token);
    return false;
  }

  selectStatement->whereClause = parseExpr(tokenizer, result, ";");

  if (!selectStatement->whereClause) {
    return false;
  }
  result->addStatement(selectStatement);
  result->setIsValid(true);

  return true;
}
}  // namespace

namespace csql {

bool SQLParser::parse(const std::string &sql, std::shared_ptr<SQLParserResult> result) {
  SQLTokenizer tokenizer(sql);

  Token token = tokenizer.nextToken();

  if (token.type != TokenType::KEYWORD) {
    result->setErrorDetails("Expected keyword at the beginning of the query", 0, 0, token);
    return false;
  }

  if (token.value == "CREATE") {
    return parseCreate(tokenizer, result);
  } else if (token.value == "INSERT") {
    return parseInsert(tokenizer, result);
  } else if (token.value == "SELECT") {
    return parseSelect(tokenizer, result);
  }

  result->setErrorDetails("Unknown keyword", 0, 0, token);
  return false;
}

}  // namespace csql