#include "parser.h"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <sstream>

namespace {

const std::string SPECIAL_CHARS = "()=<>+-*/%|&^~,;:{}";

std::string lower(const std::string &str) {
  std::string lower_str = str;
  std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(), ::tolower);
  return lower_str;
}

std::string get_lower(const std::vector<std::string> &tokens, size_t index) {
  if (index >= tokens.size()) {
    return "";
  }
  return lower(tokens[index]);
}

bool is_number(const std::string &s) {
  return !s.empty() &&
         std::find_if(s.begin(), s.end(), [](char c) { return !std::isdigit(c); }) == s.end();
}

bool is_valid_column_name(const std::string &name) {
  if (name.empty()) {
    return false;
  }

  if (!isalpha(name[0]) && name[0] != '_') {
    return false;
  }

  for (char c : name) {
    if (!isalnum(c) && c != '_') {
      return false;
    }
  }

  return true;
}

bool is_valid_table_name(const std::string &name) {
  return is_valid_column_name(name);
}

bool is_valid_type(const std::string &type) {
  if (type == "int32" || type == "bool") {
    return true;
  }
  // string[10]
  if (type.size() > 6 && type.substr(0, 6) == "string") {
    std::string length = type.substr(6);
    if (length.size() < 3 || length[0] != '[' || length.back() != ']') {
      return false;
    }
    length = length.substr(1, length.size() - 2);
    if (!is_number(length) || std::stoll(length) <= 0) {
      return false;
    }
    return true;
  }

  // bytes[10]
  if (type.size() > 5 && type.substr(0, 5) == "bytes") {
    std::string length = type.substr(5);
    if (length.size() < 3 || length[0] != '[' || length.back() != ']') {
      return false;
    }
    length = length.substr(1, length.size() - 2);
    if (!is_number(length) || std::stoll(length) <= 0) {
      return false;
    }
    return true;
  }

  return false;
}

bool is_valid_constraint(const std::string &constraint) {
  if (constraint == "key" || constraint == "unique" || constraint == "autoincrement") {
    return true;
  }
  return false;
}

}  // namespace

namespace csql {

SQLParser::SQLParser() {}

std::shared_ptr<std::vector<std::string>> SQLParser::tokenize(const std::string &sql) {
  // SELECT * FROM table
  // ["SELECT", "*", "FROM", "table"]
  // SELECT () FROM table WHERE -|id| % 2 = -1
  // ["SELECT", "*", "FROM", "table", "WHERE", "-", "|", "id" "|", "%",  "2",
  // "=", "-1"]

  std::string word;
  std::shared_ptr<std::vector<std::string>> tokens = std::make_shared<std::vector<std::string>>();

  for (int i = 0; i < sql.size(); i++) {
    char c = sql[i];
    if (c == ' ' || c == '\n' || c == '\t') {
      if (!word.empty()) {
        tokens->push_back(word);
        word.clear();
      }
    } else if (SPECIAL_CHARS.find(c) != std::string::npos) {
      if (!word.empty()) {
        tokens->push_back(word);
        word.clear();
      }
      tokens->push_back(std::string(1, c));
    } else {
      word += c;
    }
  }

  if (!word.empty()) {
    tokens->push_back(word);
  }

  return tokens;
}

bool SQLParser::parseCreate(const std::vector<std::string> &tokens,
                            std::shared_ptr<SQLParserResult> result) {
  if (tokens.size() < 3) {
    result->setIsValid(false);
    result->setErrorDetails("Invalid CREATE statement, expected more tokens.", 0, 0);
    return false;
  }

  if (get_lower(tokens, 1) == "table") {
    return parseCreateTable(tokens, result);
  } else if ((get_lower(tokens, 1) == "ordered" || get_lower(tokens, 1) == "unordered") &&
             get_lower(tokens, 2) == "index") {
    return false;
    // return parseCreateIndex(tokens, result);
  } else {
    result->setIsValid(false);
    result->setErrorDetails("Unknown CREATE statement: " + get_lower(tokens, 1), 0, 0);
    return false;
  }

  return false;
}

bool SQLParser::parseCreateTable(const std::vector<std::string> &tokens,
                                 std::shared_ptr<SQLParserResult> result) {
  // CREATE TABLE table_name ([{constraints}] column1: type [= default_value],
  // ...)
  std::shared_ptr<CreateStatement> create_statement =
      std::make_shared<CreateStatement>(CreateType::kCreateTable);

  if (tokens.size() < 4) {
    result->setIsValid(false);
    result->setErrorDetails("Invalid CREATE TABLE statement, expected more tokens.", 0, 0);
    return false;
  }

  std::string table_name = tokens[2];
  if (!is_valid_table_name(table_name)) {
    result->setIsValid(false);
    result->setErrorDetails("Invalid table name: " + table_name, 0, 0);
    return false;
  }

  create_statement->tableName = table_name;

  size_t i = 3;
  if (tokens[i] != "(") {
    result->setIsValid(false);
    result->setErrorDetails("Expected '('.", 0, 0);
    return false;
  }

  i++;

  std::shared_ptr<std::vector<std::shared_ptr<ColumnDefinition>>> columns =
      std::make_shared<std::vector<std::shared_ptr<ColumnDefinition>>>();

  while (i < tokens.size()) {
    std::shared_ptr<std::unordered_set<ConstraintType>> column_constraints =
        std::make_shared<std::unordered_set<ConstraintType>>();
    std::string column_name;
    ColumnType column_type;
    std::shared_ptr<Expr> default_value = nullptr;

    if (tokens[i] == "{") {
      i++;
      while (tokens[i] != "}") {
        std::string constraint = lower(tokens[i]);

        if (!is_valid_constraint(constraint)) {
          result->setIsValid(false);
          result->setErrorDetails("Invalid constraint: " + constraint, 0, i);
          return false;
        }

        if (constraint == "key") {
          column_constraints->insert(ConstraintType::Key);
        } else if (constraint == "unique") {
          column_constraints->insert(ConstraintType::Unique);
        } else if (constraint == "autoincrement") {
          column_constraints->insert(ConstraintType::AutoIncrement);
        }

        i++;
        if (get_lower(tokens, i) != "," && get_lower(tokens, i) != "}") {
          result->setIsValid(false);
          result->setErrorDetails("Expected ',' or '}', got " + get_lower(tokens, i), 0, i);
          return false;
        }
        if (get_lower(tokens, i) == ",") {
          i++;
        } else {
          break;
        }
      }
      i++;
    }

    column_name = tokens[i];
    if (!is_valid_column_name(column_name)) {
      result->setIsValid(false);
      result->setErrorDetails("Invalid column name: " + column_name, 0, i);
      return false;
    }

    i++;
    if (get_lower(tokens, i) != ":") {
      result->setIsValid(false);
      result->setErrorDetails("Expected ':', got " + get_lower(tokens, i), 0, i);
      return false;
    }

    i++;
    std::string type = get_lower(tokens, i);

    if (!is_valid_type(type)) {
      result->setIsValid(false);
      result->setErrorDetails("Invalid type: " + type, 0, 0);
      return false;
    }

    if (type == "int32") {
      column_type = ColumnType(DataType::INT32);
    } else if (type == "bool") {
      column_type = ColumnType(DataType::BOOL);
    } else if (type.size() > 6 && type.substr(0, 6) == "string") {
      std::string length = type.substr(6);
      length = length.substr(1, length.size() - 2);
      column_type = ColumnType(DataType::STRING, std::stoll(length));
    } else if (type.size() > 5 && type.substr(0, 5) == "bytes") {
      std::string length = type.substr(5);
      length = length.substr(1, length.size() - 2);
      column_type = ColumnType(DataType::BYTES, std::stoll(length));
    }

    if (column_constraints->count(ConstraintType::AutoIncrement) > 0 &&
        column_type.data_type != DataType::INT32) {
      result->setIsValid(false);
      result->setErrorDetails("Autoincrement column must be of type int32.", 0, i);
      return false;
    }

    i++;
    if (get_lower(tokens, i) == "=") {
      if (column_constraints->count(ConstraintType::AutoIncrement) > 0) {
        result->setIsValid(false);
        result->setErrorDetails("Autoincrement column cannot have default value.", 0, i);
        return false;
      }
      i++;
      default_value = Expr::makeLiteral(tokens[i]);
      if (!default_value) {
        result->setIsValid(false);
        result->setErrorDetails("Invalid default value: " + tokens[i], 0, 0);
        return false;
      }
      if (column_type.data_type == DataType::INT32 && default_value->type != kExprLiteralInt &&
          default_value->type != kExprLiteralNull) {
        result->setIsValid(false);
        result->setErrorDetails("Invalid default value for int32 column: " + tokens[i], 0, 0);
        return false;
      }
      if (column_type.data_type == DataType::BOOL && default_value->type != kExprLiteralBool &&
          default_value->type != kExprLiteralNull) {
        result->setIsValid(false);
        result->setErrorDetails("Invalid default value for bool column: " + tokens[i], 0, 0);
        return false;
      }
      if (column_type.data_type == DataType::STRING && default_value->type != kExprLiteralString &&
          default_value->type != kExprLiteralNull) {
        result->setIsValid(false);
        result->setErrorDetails("Invalid default value for string column: " + tokens[i], 0, 0);
        return false;
      }
      if (column_type.data_type == DataType::BYTES && default_value->type != kExprLiteralBytes &&
          default_value->type != kExprLiteralString && default_value->type != kExprLiteralNull) {
        result->setIsValid(false);
        result->setErrorDetails("Invalid default value for bytes column: " + tokens[i], 0, 0);
        return false;
      }
      i++;
    }

    std::shared_ptr<ColumnDefinition> column_definition = std::make_shared<ColumnDefinition>(
        column_name, column_type, column_constraints, default_value);

    columns->push_back(column_definition);

    if (get_lower(tokens, i) != "," && get_lower(tokens, i) != ")") {
      result->setIsValid(false);
      result->setErrorDetails("Expected ',' or ')', got " + get_lower(tokens, i), 0, i);
      return false;
    }
    if (get_lower(tokens, i) == ",") {
      i++;
      continue;
    }
    if (get_lower(tokens, i) == ")") {
      break;
    }
  }

  create_statement->setColumnDefs(columns);

  result->addStatement(create_statement);
  result->setIsValid(true);
  return true;
}

bool SQLParser::parseInsert(const std::vector<std::string> &tokens,
                            std::shared_ptr<SQLParserResult> result) {
  // INSERT (value1, value2, ...) to table_name
  // INSERT (key1=value1, key2=value2, ...) to table_name

  size_t i = 1;  // Skip "INSERT"
  if (tokens[i] != "(") {
    result->setIsValid(false);
    result->setErrorDetails("Expected '('.", 0, 0);
    return false;
  }

  std::shared_ptr<std::vector<std::shared_ptr<ColumnValueDefinition>>> column_values =
      std::make_shared<std::vector<std::shared_ptr<ColumnValueDefinition>>>();
  i++;
  while (get_lower(tokens, i) != ")") {
    if (get_lower(tokens, i) == ",") {
      i++;
      continue;
    }

    std::shared_ptr<ColumnValueDefinition> column_value;

    if (get_lower(tokens, i + 1) == "=") {
      std::string column_name = tokens[i];
      if (!is_valid_column_name(column_name)) {
        result->setIsValid(false);
        result->setErrorDetails("Invalid column name: " + column_name, 0, 0);
        return false;
      }
      if (tokens.size() < i + 3 || tokens[i + 2] == "," || tokens[i + 2] == ")") {
        result->setIsValid(false);
        result->setErrorDetails("Expected value after '='.", 0, 0);
        return false;
      }

      std::shared_ptr<Expr> value = Expr::makeLiteral(tokens[i + 2]);
      if (!value) {
        result->setIsValid(false);
        result->setErrorDetails("Invalid value: " + tokens[i + 2], 0, 0);
        return false;
      }

      column_value = std::make_shared<ColumnValueDefinition>(column_name, value);
      i += 3;
    } else {
      std::shared_ptr<Expr> value = Expr::makeLiteral(tokens[i]);
      if (!value) {
        result->setIsValid(false);
        result->setErrorDetails("Invalid value: " + tokens[i], 0, 0);
        return false;
      }

      column_value = std::make_shared<ColumnValueDefinition>(value);
      i += 1;
    }

    column_values->push_back(column_value);

    if (get_lower(tokens, i) != "," && get_lower(tokens, i) != ")") {
      result->setIsValid(false);
      result->setErrorDetails("Expected ',' or ')', got " + get_lower(tokens, i), 0, 0);
      return false;
    }
    if (get_lower(tokens, i) == ",") {
      i++;
      continue;
    }
    if (get_lower(tokens, i) == ")") {
      break;
    }
  }

  i++;
  if (get_lower(tokens, i) != "to") {
    result->setIsValid(false);
    result->setErrorDetails("Expected 'to', got " + get_lower(tokens, i), 0, 0);
    return false;
  }

  i++;
  if (i >= tokens.size()) {
    result->setIsValid(false);
    result->setErrorDetails("Expected table name.", 0, 0);
    return false;
  }

  std::string table_name = tokens[i];
  if (!is_valid_table_name(table_name)) {
    result->setIsValid(false);
    result->setErrorDetails("Invalid table name: " + table_name, 0, 0);
    return false;
  }

  InsertType type = InsertType::kUnknown;
  for (const auto &column_value : *column_values) {
    if (type == InsertType::kUnknown) {
      type = column_value->isNamed ? InsertType::kInsertKeysValues : InsertType::kInsertValues;
    } else {
      if ((type == InsertType::kInsertKeysValues && !column_value->isNamed) ||
          (type == InsertType::kInsertValues && column_value->isNamed)) {
        result->setIsValid(false);
        result->setErrorDetails("Cannot mix named and unnamed values.", 0, 0);
        return false;
      }
    }
  }

  std::shared_ptr<InsertStatement> insert_statement =
      std::make_shared<InsertStatement>(type, table_name);
  insert_statement->setColumnValues(column_values);

  result->addStatement(insert_statement);
  result->setIsValid(true);
  return true;
}

bool SQLParser::parse(const std::string &sql, std::shared_ptr<SQLParserResult> result) {
  std::cout << "Parsing SQL: " << sql << std::endl;

  std::shared_ptr<std::vector<std::string>> tokens = tokenize(sql);
  result->setTokens(tokens);
  if (tokens->empty()) {
    result->setIsValid(false);
    result->setErrorDetails("No tokens found.", 0, 0);
    return false;
  }

  std::string first_token = get_lower(*tokens, 0);

  if (first_token == "create") {
    return parseCreate(*tokens, result);
  } else if (first_token == "select") {
    // return parseSelect(tokens, result);
  } else if (first_token == "insert") {
    return parseInsert(*tokens, result);
  } else if (first_token == "delete") {
    // return parseDelete(tokens, result);
  } else if (first_token == "drop") {
    // return parseDrop(tokens, result);
  } else {
    result->setIsValid(false);
    result->setErrorDetails("Unknown statement type: " + first_token, 0, 0);
    return false;
  }

  return false;
}

}  // namespace csql