#include "expr.h"

#include <stdio.h>
#include <string.h>

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

std::shared_ptr<Expr> Expr::makeNullLiteral() {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprLiteralNull);
  return e;
}

std::shared_ptr<Expr> Expr::makeColumnRef(const std::string& name) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprColumnRef);
  e->name = name;
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

std::shared_ptr<Expr> Expr::makeParameter(int id) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprParameter);
  e->ival = id;
  return e;
}

std::shared_ptr<Expr> Expr::makeSelect(std::shared_ptr<SelectStatement> select) {
  std::shared_ptr<Expr> e = std::make_shared<Expr>(kExprSelect);
  e->select = select;
  return e;
}

bool Expr::isType(ExprType exprType) const {
  return exprType == type;
}

bool Expr::isLiteral() const {
  return isType(kExprLiteralInt) || isType(kExprLiteralString) || isType(kExprParameter) ||
         isType(kExprLiteralNull);
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
    case kExprOperator:
      stream << "(";
      if (expr.expr) {
        stream << *expr.expr;
      }
      switch (expr.opType) {
        case kOpPlus:
          stream << " + ";
          break;
        case kOpMinus:
          stream << " - ";
          break;
        case kOpAsterisk:
          stream << " * ";
          break;
        case kOpSlash:
          stream << " / ";
          break;
        case kOpPercentage:
          stream << " % ";
          break;
        case kOpEquals:
          stream << " = ";
          break;
        case kOpNotEquals:
          stream << " != ";
          break;
        case kOpLess:
          stream << " < ";
          break;
        case kOpLessEq:
          stream << " <= ";
          break;
        case kOpGreater:
          stream << " > ";
          break;
        case kOpGreaterEq:
          stream << " >= ";
          break;
        case kOpAnd:
          stream << " AND ";
          break;
        case kOpOr:
          stream << " OR ";
          break;
        case kOpIn:
          stream << " IN ";
          break;
        case kOpNot:
          stream << " NOT ";
          break;
        case kOpUnaryMinus:
          stream << " -";
          break;
        case kOpIsNull:
          stream << " IS NULL";
          break;
        case kOpExists:
          stream << " EXISTS";
          break;
        default:
          stream << " UNKNOWN";
          break;
      }
      if (expr.expr2) {
        stream << *expr.expr2;
      }
      stream << ")";
      break;
    case kExprSelect:
      stream << *expr.select;
      break;
  }
  return stream;
}

}  // namespace csql