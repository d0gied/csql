#include "column.h"

#include "row.h"
#include "table.h"

namespace csql {
namespace storage {

std::shared_ptr<Column> Column::create(std::shared_ptr<ColumnDefinition> columnDefinition) {
  std::shared_ptr<Column> column = std::make_shared<Column>();
  column->name_ = columnDefinition->name;
  column->column_type_ = columnDefinition->type;
  column->nullable_ = columnDefinition->nullable;
  column->is_autoincrement_ =
      columnDefinition->column_constraints->count(ConstraintType::AutoIncrement) > 0;
  column->is_key_ = columnDefinition->column_constraints->count(ConstraintType::Key) > 0;
  column->is_unique_ = columnDefinition->column_constraints->count(ConstraintType::Unique) > 0;
  column->default_value_ = columnDefinition->default_value;
  return column;
}

std::shared_ptr<Column> Column::create(const std::string& name, const ColumnType& type,
                                       std::shared_ptr<ITable> table,
                                       std::shared_ptr<Expr> refExpr) {
  std::shared_ptr<Column> column = std::make_shared<Column>();
  column->name_ = name;
  column->column_type_ = type;
  column->reffered_expr_ = refExpr;
  column->table_ = table;

  column->nullable_ = true;
  column->is_autoincrement_ = false;
  column->is_key_ = false;
  column->is_unique_ = false;
  column->default_value_ = nullptr;

  return column;
}

const std::string& Column::getName() const {
  return name_;
}

const ColumnType& Column::type() const {
  return column_type_;
}

bool Column::isNullable() const {
  return nullable_;
}

bool Column::isAutoincrement() const {
  return is_autoincrement_;
}

bool Column::isKey() const {
  return is_key_;
}

bool Column::isUnique() const {
  return is_unique_;
}

int32_t Column::maxValue() const {
  if (column_type_.data_type != DataType::INT32) {
    throw std::runtime_error("Invalid type, expected INT32");
  }
  auto table = table_.lock();
  if (!table) {
    throw std::runtime_error("Table not found");
  }
  auto iter = table->getIterator();
  int32_t max = 0;
  while (iter->hasValue()) {
    auto row = *(*iter);
    ++(*iter);
    if (row->isNull(name_)) continue;
    int32_t value = row->get<int32_t>(name_);
    if (value > max) max = value;
  }
  return max;
}

void* Column::createValue() const {
  if (default_value_) return createValue(default_value_);
  if (column_type_.data_type == DataType::INT32 && is_autoincrement_) {
    return new int32_t(maxValue() + 1);
  }
  return nullptr;
}

void* Column::createValue(std::shared_ptr<Expr> value) const {
  if (value->type == kExprLiteralNull) return nullptr;
  if (column_type_.data_type == DataType::INT32 && value->type == kExprLiteralInt) {
    return new int32_t(value->ival);
  } else if (column_type_.data_type == DataType::BOOL && value->type == kExprLiteralBool) {
    return new bool(value->ival);
  } else if (column_type_.data_type == DataType::STRING && value->type == kExprLiteralString) {
    return new std::string(value->name);
  } else if (column_type_.data_type == DataType::BYTES &&
             (value->type == kExprLiteralBytes || value->type == kExprLiteralString)) {
    uint8_t* bytes = new uint8_t[column_type_.length];
    for (size_t i = 0; i < column_type_.length; i++) {
      bytes[i] = value->name[i];
    }
    return bytes;
  }
  return nullptr;
}

std::shared_ptr<Column> Column::refferedColumn() const {
  return reffered_column_;
}

std::shared_ptr<ITable> Column::table() const {
  return table_.lock();
}

std::shared_ptr<Expr> Column::refferedExpr() const {
  return reffered_expr_;
}

std::shared_ptr<Column> Column::clone(std::shared_ptr<ITable> table, const std::string& name) {
  std::shared_ptr<Column> column = std::make_shared<Column>();

  if (name != "") {
    column->name_ = name;
  } else {
    column->name_ = name_;
  }
  column->column_type_ = column_type_;
  column->nullable_ = nullable_;
  column->is_autoincrement_ = is_autoincrement_;
  column->is_key_ = is_key_;
  column->is_unique_ = is_unique_;
  column->default_value_ = default_value_;

  column->table_ = table;
  column->reffered_column_ = shared_from_this();

  return column;
}

}  // namespace storage
}  // namespace csql