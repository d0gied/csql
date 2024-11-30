#pragma once

#include <stdlib.h>

#include <memory>
#include <vector>

#include "column_type.h"
// #include "sql/statements/select.h"

namespace csql {
struct DeleteStatement;
struct OrderDescription;

enum ExprType {
  kExprLiteralString,
  kExprLiteralInt,
  kExprLiteralBool,
  kExprLiteralBytes,
  kExprLiteralNull,

  kExprStar,
  kExprColumnRef,
  kExprOperator,
  kExprSelect,
};

// Operator types. These are important for expressions of type kExprOperator.
enum OperatorType {
  kOpNone,  // 0

  // Binary operators.
  kOpPlus,        // 1
  kOpMinus,       // 2
  kOpAsterisk,    // 3
  kOpSlash,       // 4
  kOpPercentage,  // 5

  kOpEquals,     // 6
  kOpNotEquals,  // 7
  kOpLess,       // 8
  kOpLessEq,     // 9
  kOpGreater,    // 10
  kOpGreaterEq,  // 11
  kOpLike,       // 12
  kOpNotLike,    // 13
  kOpILike,      // 14
  kOpAnd,        // 15
  kOpOr,         // 16
  kOpIn,         // 17

  // Unary operators.
  kOpNot,          // NOT expr
  kOpBitNot,       // ~expr
  kOpUnaryMinus,   // -expr
  kOpLength,       // |expr|
  kOpIsNull,       // IS NULL
  kOpExists,       // EXISTS
  kOpParenthesis,  // (expr)
};

bool isUnaryOperator(OperatorType op);
bool isBinaryOperator(OperatorType op);
bool isLogicalOperator(OperatorType op);
bool isComparisonOperator(OperatorType op);
bool isArithmeticOperator(OperatorType op);

bool operator<(OperatorType lhs, OperatorType rhs);

typedef struct Expr Expr;

// Represents SQL expressions (i.e. literals, operators, column_refs).
struct Expr {
  Expr(ExprType type);
  virtual ~Expr() = default;

  ExprType type;

  // TODO: Replace expressions by list.
  std::shared_ptr<Expr> expr;
  std::shared_ptr<Expr> expr2;
  std::shared_ptr<DeleteStatement> select;
  std::string name;
  std::string table;
  int32_t ival;
  int32_t ival2;
  ColumnType columnType;

  OperatorType opType;
  bool distinct;

  // Convenience accessor methods.

  bool isType(ExprType exprType) const;
  bool isLiteral() const;
  bool hasTable() const;
  const std::string& getName() const;

  // Static constructors.

  static std::shared_ptr<Expr> make(ExprType type);
  static std::shared_ptr<Expr> makeOpUnary(OperatorType op, std::shared_ptr<Expr> expr);
  static std::shared_ptr<Expr> makeOpBinary(std::shared_ptr<Expr> expr1, OperatorType op,
                                            std::shared_ptr<Expr> expr2);
  static std::shared_ptr<Expr> makeLiteral(int32_t val);
  static std::shared_ptr<Expr> makeLiteral(const std::string& val);
  static std::shared_ptr<Expr> makeStringLiteral(const std::string& val);
  static std::shared_ptr<Expr> makeLiteral(bool val);
  static std::shared_ptr<Expr> makeLiteral(std::vector<uint8_t> val);
  static std::shared_ptr<Expr> makeNullLiteral();
  static std::shared_ptr<Expr> makeColumnRef(const std::string& name);
  static std::shared_ptr<Expr> makeColumnRef(const std::string& table, const std::string& name);
  static std::shared_ptr<Expr> makeStar(void);
  static std::shared_ptr<Expr> makeStar(const std::string& table);
  static std::shared_ptr<Expr> makeParameter(int id);
  static std::shared_ptr<Expr> makeSelect(std::shared_ptr<DeleteStatement> select);
  static std::shared_ptr<Expr> makeCast(std::shared_ptr<Expr> expr, ColumnType columnType);

  // Debugging.
  std::string toMermaid(const std::string node_name = "A", bool subexpr = false) const;
};

std::ostream& operator<<(std::ostream&, const Expr&);

}  // namespace csql
