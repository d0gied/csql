#include "cell.h"

#include <iostream>

namespace csql {
namespace storage {

Value::Value(ColumnType type, void *value) : type(type), value(value) {}

bool Value::isNull() const {
  return value == nullptr;
}

int32_t Value::getInt() const {
  return *static_cast<int32_t *>(value);
}

bool Value::getBool() const {
  if (type.data_type != DataType::BOOL) {
    throw std::runtime_error("Invalid type, expected BOOL");
  }
  return *static_cast<bool *>(value);
}

std::string Value::getString() const {
  if (type.data_type != DataType::STRING) {
    throw std::runtime_error("Invalid type, expected STRING, got " + to_string(type.data_type));
  }
  return *static_cast<std::string *>(value);
}

std::vector<uint8_t> Value::getBytes() const {
  if (type.data_type != DataType::BYTES) {
    throw std::runtime_error("Invalid type, expected BYTES");
  }

  return std::vector<uint8_t>(static_cast<uint8_t *>(value),
                              static_cast<uint8_t *>(value) + type.length);
}

std::shared_ptr<Cell> Cell::create(const std::vector<std::shared_ptr<Value>> &values) {
  std::shared_ptr<Cell> cell = std::make_shared<Cell>();
  cell->values_ = new Value[values.size()];
  for (size_t i = 0; i < values.size(); i++) {
    cell->values_[i] = *values[i];
  }
  return cell;
}

}  // namespace storage
}  // namespace csql