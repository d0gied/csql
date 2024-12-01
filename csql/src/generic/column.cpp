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

  if (columnDefinition->default_value) {
    if (columnDefinition->type.data_type == DataType::INT32) {
      column->default_value_ = new int32_t(columnDefinition->default_value->ival);
    } else if (columnDefinition->type.data_type == DataType::BOOL) {
      column->default_value_ = new bool(columnDefinition->default_value->ival);
    } else if (columnDefinition->type.data_type == DataType::STRING) {
      column->default_value_ = new std::string(columnDefinition->default_value->name);
    } else if (columnDefinition->type.data_type == DataType::BYTES) {
      int8_t* bytes = new int8_t[columnDefinition->type.length];
      for (size_t i = 0; i < columnDefinition->type.length; i++) {
        bytes[i] = columnDefinition->default_value->name[i];
      }
      column->default_value_ = bytes;
    }
  } else {
    column->default_value_ = nullptr;
  }

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

std::shared_ptr<Value> Column::createValue() const {
  if (default_value_ != nullptr) return std::make_shared<Value>(column_type_, default_value_);
  if (column_type_.data_type == DataType::INT32 && is_autoincrement_) {
    return std::make_shared<Value>(column_type_, new int32_t(maxValue() + 1));
  }
  return std::make_shared<Value>(column_type_, nullptr);
}

std::shared_ptr<Value> Column::createValue(std::shared_ptr<Expr> value) const {
  if (value->type == kExprLiteralNull) return std::make_shared<Value>(column_type_, nullptr);
  if (column_type_.data_type == DataType::INT32 && value->type == kExprLiteralInt) {
    return std::make_shared<Value>(column_type_, new int32_t(value->ival));
  } else if (column_type_.data_type == DataType::BOOL && value->type == kExprLiteralBool) {
    return std::make_shared<Value>(column_type_, new bool(value->ival));
  } else if (column_type_.data_type == DataType::STRING && value->type == kExprLiteralString) {
    return std::make_shared<Value>(column_type_, new std::string(value->name));
  } else if (column_type_.data_type == DataType::BYTES &&
             (value->type == kExprLiteralBytes || value->type == kExprLiteralString)) {
    int8_t* bytes = new int8_t[column_type_.length];
    for (size_t i = 0; i < column_type_.length; i++) {
      bytes[i] = value->name[i];
    }
    return std::make_shared<Value>(column_type_, bytes);
  }
  return nullptr;
}

}  // namespace storage
}  // namespace csql