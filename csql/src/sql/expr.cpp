#include "expr.h"

#include <stdio.h>
#include <string.h>

#include <string>

#include "statements/select.h"

namespace csql {
Expr::Expr(ExprType type)
    : type(type),
      expr(nullptr),
      expr2(nullptr),
      select(nullptr),
      name(""),
      table(""),
      ival(0),
      ival2(0),
      columnType(DataType::UNKNOWN, 0),
      opType(kOpNone),
      distinct(false) {}

bool isUnaryOperator(OperatorType op) {
  return op == kOpUnaryMinus || op == kOpNot || op == kOpIsNull || op == kOpExists ||
         op == kOpParenthesis || op == kOpBitNot || op == kOpLength;
}

bool isBinaryOperator(OperatorType op) {
  return !isUnaryOperator(op);
}

bool isLogicalOperator(OperatorType op) {
  return op == kOpAnd || op == kOpOr || op == kOpNot;
}

bool isComparisonOperator(OperatorType op) {
  return op == kOpEquals || op == kOpNotEquals || op == kOpLess || op == kOpLessEq ||
         op == kOpGreater || op == kOpGreaterEq;
}

bool isArithmeticOperator(OperatorType op) {
  return op == kOpPlus || op == kOpMinus || op == kOpAsterisk || op == kOpSlash ||
         op == kOpPercentage;
}

bool operator<(OperatorType lhs, OperatorType rhs) {
  if (isUnaryOperator(lhs) && isUnaryOperator(rhs)) {
    if (lhs == kOpParenthesis || lhs == kOpLength || lhs == kOpBitNot || lhs == kOpUnaryMinus ||
        lhs == kOpIsNull || lhs == kOpExists) {
      return false;
    } else if (rhs == kOpParenthesis || rhs == kOpLength || rhs == kOpBitNot ||
               rhs == kOpUnaryMinus || rhs == kOpIsNull || rhs == kOpExists) {
      return true;
    }

    if (lhs == kOpNot) {
      return false;
    } else if (rhs == kOpNot) {
      return true;
    }
    throw std::runtime_error("Unknown unary operator: " + std::to_string(lhs) + " vs. " +
                             std::to_string(rhs));

  } else if (isUnaryOperator(lhs) && isBinaryOperator(rhs)) {
    return isLogicalOperator(lhs);  // Unary operators have higher precedence.
  } else if (isBinaryOperator(lhs) && isUnaryOperator(rhs)) {
    return !isLogicalOperator(rhs);  // Unary operators have higher precedence.
  }
  // Both are binary operators.

  if (isLogicalOperator(lhs) && isLogicalOperator(rhs)) {
    return (lhs == kOpAnd) < (rhs == kOpAnd);  // AND has higher precedence than OR.
  } else if (isLogicalOperator(lhs)) {
    return true;  // Logical operators have lower precedence than arithmetic/comparison.
  } else if (isLogicalOperator(rhs)) {
    return false;  // Logical operators have lower precedence than arithmetic/comparison.
  }

  // Comparison operators
  if (isComparisonOperator(lhs) && isComparisonOperator(rhs)) {
    return false;  // All comparison operators have the same precedence.
  }

  // Arithmetic vs. comparison
  if (isArithmeticOperator(lhs) && isComparisonOperator(rhs)) {
    return false;  // Arithmetic operators have higher precedence.
  } else if (isComparisonOperator(lhs) && isArithmeticOperator(rhs)) {
    return true;  // Logical/comparison operators have lower precedence.
  }

  // Arithmetic operators
  if (isArithmeticOperator(lhs) && isArithmeticOperator(rhs)) {
    // *, /, % have higher precedence than +, -
    return (lhs == kOpAsterisk || lhs == kOpSlash || lhs == kOpPercentage) <
           (rhs == kOpAsterisk || rhs == kOpSlash || rhs == kOpPercentage);
  }

  throw std::runtime_error("Unknown operator type: " + std::to_string(lhs) + " vs. " +
                           std::to_string(rhs));
}

std::shared_ptr<Expr> Expr::make(ExprType type) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(type);
  return e;
}

std::shared_ptr<Expr> Expr::makeOpUnary(OperatorType op, std::shared_ptr<Expr> expr) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprOperator);
  e->opType = op;
  e->expr = expr;
  e->expr2 = nullptr;
  return e;
}

std::shared_ptr<Expr> Expr::makeOpBinary(std::shared_ptr<Expr> expr1, OperatorType op,
                                         std::shared_ptr<Expr> expr2) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprOperator);
  e->opType = op;
  e->expr = expr1;
  e->expr2 = expr2;
  return e;
}

std::shared_ptr<Expr> Expr::makeLiteral(int32_t val) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprLiteralInt);
  e->ival = val;
  return e;
}

std::shared_ptr<Expr> Expr::makeLiteral(const std::string& val) {
  if (val == "null" || val == "NULL") {
    return makeNullLiteral();
  }
  if (val == "true" || val == "false" || val == "TRUE" || val == "FALSE") {
    return makeLiteral(val == "true" || val == "TRUE");
  }
  if ((val[0] == '\'' && val[val.size() - 1] == '\'') ||
      (val[0] == '"' && val[val.size() - 1] == '"')) {
    std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprLiteralString);
    e->name = val.substr(1, val.size() - 2);
    return e;
  }
  if (val[0] == '0' && val.size() > 2 && val[1] == 'x') {
    std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprLiteralBytes);

    // Remove the 0x prefix
    std::string bytes = val.substr(2);

    if (bytes.size() % 2 != 0) {
      bytes = "0" + bytes;
    }
    e->name = "";
    for (int32_t i = bytes.size() - 2; i >= 0; i -= 2) {
      char byte[3];
      byte[0] = bytes[i];
      byte[1] = bytes[i + 1];
      byte[2] = '\0';
      e->name += (char)strtol(byte, nullptr, 16);
    }

    return e;
  }
  bool is_number = true;
  std::string val2 = val;
  if (val2[0] == '-') {
    val2 = val2.substr(1);
  }
  for (char c : val2) {
    if (!isdigit(c)) {
      is_number = false;
      break;
    }
  }
  if (is_number) {
    return makeLiteral(std::stoi(val2));
  }
  return nullptr;
}

std::shared_ptr<Expr> Expr::makeLiteral(bool val) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprLiteralBool);
  e->ival = (int)val;
  return e;
}

std::shared_ptr<Expr> Expr::makeLiteral(std::vector<uint8_t> val) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprLiteralBytes);
  e->name = "";
  for (int8_t byte : val) {
    e->name += byte;
  }
  return e;
}

std::shared_ptr<Expr> Expr::makeStringLiteral(const std::string& val) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprLiteralString);
  e->name = val;
  return e;
}

std::shared_ptr<Expr> Expr::makeNullLiteral() {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprLiteralNull);
  return e;
}

std::shared_ptr<Expr> Expr::makeColumnRef(const std::string& name) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprColumnRef);
  size_t dot_pos = name.find('.');
  if (dot_pos != std::string::npos) {
    e->table = name.substr(0, dot_pos);
    e->name = name.substr(dot_pos + 1);
  } else {
    e->table = "";
    e->name = name;
  }
  return e;
}

std::shared_ptr<Expr> Expr::makeColumnRef(const std::string& table, const std::string& name) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprColumnRef);
  e->name = name;
  e->table = table;
  return e;
}

std::shared_ptr<Expr> Expr::makeStar(void) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprStar);
  return e;
}

std::shared_ptr<Expr> Expr::makeStar(const std::string& table) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprStar);
  e->table = table;
  return e;
}

std::shared_ptr<Expr> Expr::makeSelect(std::shared_ptr<DeleteStatement> select) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprSelect);
  e->select = select;
  return e;
}

bool Expr::isType(ExprType exprType) const {
  return exprType == type;
}

bool Expr::isLiteral() const {
  return isType(kExprLiteralInt) || isType(kExprLiteralString) || isType(kExprLiteralNull) ||
         isType(kExprLiteralBool) || isType(kExprLiteralBytes) || isType(kExprColumnRef);
}

bool Expr::hasTable() const {
  return table != "";
}

const std::string& Expr::getName() const {
  return name;
}

std::ostream& operator<<(std::ostream& stream, const Expr& expr) {
  switch (expr.type) {
    case kExprLiteralInt:
      stream << expr.ival;
      break;
    case kExprLiteralString:
      stream << "\"" << expr.name << "\"";
      break;
    case kExprLiteralBool:
      stream << (expr.ival == 1 ? "true" : "false");
      break;
    case kExprLiteralBytes: {
      stream << "0x";
      bool has_non_zero = false;
      for (size_t j = 0; j < expr.name.size(); j++) {
        uint8_t byte = expr.name[expr.name.size() - j - 1];
        if (byte != 0) {
          has_non_zero = true;
        }
        if (has_non_zero) {
          stream << std::hex << (int)byte;
        }
      }
    } break;
    case kExprLiteralNull:
      stream << "NULL";
      break;
    case kExprColumnRef:
      if (expr.table != "") {
        stream << expr.table << ".";
      }
      stream << expr.name;
      break;
    case kExprStar:
      if (expr.table != "") {
        stream << expr.table << ".";
      }
      stream << "*";
      break;
    case kExprOperator: {
      if (expr.opType == kOpUnaryMinus) {
        stream << "-" << *expr.expr;
      } else if (expr.opType == kOpPlus) {
        stream << *expr.expr << " + " << *expr.expr2;
      } else if (expr.opType == kOpMinus) {
        stream << *expr.expr << " - " << *expr.expr2;
      } else if (expr.opType == kOpAsterisk) {
        stream << *expr.expr << " * " << *expr.expr2;
      } else if (expr.opType == kOpSlash) {
        stream << *expr.expr << " / " << *expr.expr2;
      } else if (expr.opType == kOpPercentage) {
        stream << *expr.expr << " % " << *expr.expr2;
      } else if (expr.opType == kOpEquals) {
        stream << *expr.expr << " = " << *expr.expr2;
      } else if (expr.opType == kOpNotEquals) {
        stream << *expr.expr << " != " << *expr.expr2;
      } else if (expr.opType == kOpLess) {
        stream << *expr.expr << " < " << *expr.expr2;
      } else if (expr.opType == kOpLessEq) {
        stream << *expr.expr << " <= " << *expr.expr2;
      } else if (expr.opType == kOpGreater) {
        stream << *expr.expr << " > " << *expr.expr2;
      } else if (expr.opType == kOpGreaterEq) {
        stream << *expr.expr << " >= " << *expr.expr2;
      } else if (expr.opType == kOpAnd) {
        stream << *expr.expr << " AND " << *expr.expr2;
      } else if (expr.opType == kOpOr) {
        stream << *expr.expr << " OR " << *expr.expr2;
      } else if (expr.opType == kOpIn) {
        stream << *expr.expr << " IN " << *expr.expr2;
      } else if (expr.opType == kOpNot) {
        stream << "NOT " << *expr.expr;
      } else if (expr.opType == kOpIsNull) {
        stream << *expr.expr << " IS NULL";
      } else if (expr.opType == kOpExists) {
        stream << "EXISTS " << *expr.expr;
      } else if (expr.opType == kOpParenthesis) {
        stream << "(" << *expr.expr << ")";
      } else if (expr.opType == kOpBitNot) {
        stream << "~" << *expr.expr;
      } else if (expr.opType == kOpLength) {
        stream << "|" << *expr.expr << "|";
      } else {
        stream << "UNKNOWN_OPERATOR";
      }

    } break;
  }
  return stream;
}

std::string Expr::toMermaid(std::string node_name, bool subexpr) const {
  std::string result = node_name + "{";
  switch (type) {
    case kExprLiteralInt:
      result = node_name + "[" + std::to_string(ival) + "]";
      break;
    case kExprLiteralString:
      result = node_name + "[\'" + name + "\']";
      break;
    case kExprLiteralBool:
      result = node_name + "[" + (ival == 1 ? "true" : "false") + "]";
      break;
    case kExprLiteralBytes:
      result = node_name + "[0x";
      for (size_t j = 0; j < name.size(); j++) {
        uint8_t byte = name[name.size() - j - 1];
        result += std::to_string(byte);
      }
      result += "]";
      break;
    case kExprLiteralNull:
      result = node_name + "[NULL]";
      break;
    case kExprColumnRef:
      if (table != "") {
        result = node_name + "(" + table + "." + name + ")";
      } else {
        result = node_name + "(" + name + ")";
      }
      break;
    case kExprStar:
      result = node_name + "(*)";
      break;
    case kExprOperator: {
      if (opType == kOpUnaryMinus || opType == kOpMinus) {
        result += "-}";
      } else if (opType == kOpPlus) {
        result += "'+'}";
      } else if (opType == kOpAsterisk) {
        result += "'*'}";
      } else if (opType == kOpSlash) {
        result += "'/'}";
      } else if (opType == kOpPercentage) {
        result += "'%'}";
      } else if (opType == kOpEquals) {
        result += "=}";
      } else if (opType == kOpNotEquals) {
        result += "'!='}";
      } else if (opType == kOpLess) {
        result += "'<'}";
      } else if (opType == kOpLessEq) {
        result += "'<='}";
      } else if (opType == kOpGreater) {
        result += "'>'}";
      } else if (opType == kOpGreaterEq) {
        result += "'>='}";
      } else if (opType == kOpAnd) {
        result += "AND}";
      } else if (opType == kOpOr) {
        result += "OR}";
      } else if (opType == kOpIn) {
        result += "IN}";
      } else if (opType == kOpNot) {
        result += "NOT}";
      } else if (opType == kOpIsNull) {
        result += "IS NULL}";
      } else if (opType == kOpExists) {
        result += "EXISTS}";
      } else if (opType == kOpParenthesis) {
        result = expr->toMermaid(node_name, true);
        if (!subexpr) {
          result = "graph TD\n  " + result;
        }
        return result;
      } else if (opType == kOpBitNot) {
        result += "NOT}";
      } else if (opType == kOpLength) {
        result += "LENGTH}";
      }
    } break;
    case kExprSelect:
      result += "SELECT}";
      break;
  }
  result = result + "\n";
  if (expr) {
    result += "  " + expr->toMermaid(node_name + "L", true);
    result += "  " + node_name + " --> " + node_name + "L\n";
  }
  if (expr2) {
    result += "  " + expr2->toMermaid(node_name + "R", true);
    result += "  " + node_name + " --> " + node_name + "R\n";
  }

  if (!subexpr) {
    result = "graph TD\n  " + result;
  }
  return result;
}

}  // namespace csql