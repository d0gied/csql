#pragma once

#include <stdlib.h>

#include <memory>

#include "column_type.h"
// #include "sql/statements/select.h"

namespace csql {
struct SelectStatement;
struct OrderDescription;

enum ExprType {
  kExprLiteralString,
  kExprLiteralInt,
  kExprLiteralBool,
  kExprLiteralBytes,
  kExprLiteralNull,

  kExprStar,
  kExprParameter,
  kExprColumnRef,
  kExprOperator,
  kExprSelect,
};

// Operator types. These are important for expressions of type kExprOperator.
enum OperatorType {
  kOpNone,

  // Binary operators.
  kOpPlus,
  kOpMinus,
  kOpAsterisk,
  kOpSlash,
  kOpPercentage,

  kOpEquals,
  kOpNotEquals,
  kOpLess,
  kOpLessEq,
  kOpGreater,
  kOpGreaterEq,
  kOpLike,
  kOpNotLike,
  kOpILike,
  kOpAnd,
  kOpOr,
  kOpIn,

  // Unary operators.
  kOpNot,
  kOpUnaryMinus,
  kOpIsNull,
  kOpExists
};

typedef struct Expr Expr;

// Represents SQL expressions (i.e. literals, operators, column_refs).
struct Expr {
  Expr(ExprType type);
  virtual ~Expr() = default;

  ExprType type;

  // TODO: Replace expressions by list.
  std::shared_ptr<Expr> expr;
  std::shared_ptr<Expr> expr2;
  std::shared_ptr<SelectStatement> select;
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
  static std::shared_ptr<Expr> makeLiteral(bool val);
  static std::shared_ptr<Expr> makeNullLiteral();
  static std::shared_ptr<Expr> makeColumnRef(const std::string& name);
  static std::shared_ptr<Expr> makeColumnRef(const std::string& table, const std::string& name);
  static std::shared_ptr<Expr> makeStar(void);
  static std::shared_ptr<Expr> makeStar(const std::string& table);
  static std::shared_ptr<Expr> makeParameter(int id);
  static std::shared_ptr<Expr> makeSelect(std::shared_ptr<SelectStatement> select);
  static std::shared_ptr<Expr> makeCast(std::shared_ptr<Expr> expr, ColumnType columnType);
};

std::ostream& operator<<(std::ostream&, const Expr&);

}  // namespace csql
