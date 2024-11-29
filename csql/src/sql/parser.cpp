#include "sql/parser.h"

#include <boost/regex.hpp>
#include <cstring>
#include <string>
#include <unordered_set>

#include "sql/column_type.h"
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
  }

  result->setErrorDetails("Unknown keyword", 0, 0, token);
  return false;
}

}  // namespace csql