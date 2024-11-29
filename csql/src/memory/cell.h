#pragma once

#include <memory>
#include <ostream>
#include <vector>

#include "../sql/column_type.h"

namespace csql {
namespace storage {

struct Value {
  Value() = default;
  Value(ColumnType type, void* value);

  bool isNull() const;
  int32_t getInt() const;
  bool getBool() const;
  std::string getString() const;
  std::vector<uint8_t> getBytes() const;

  friend std::ostream& dump(std::ostream& stream, const Value& value);
  friend std::istream& load(std::istream& stream, Value& value);

  friend std::ostream& operator<<(std::ostream& stream, const Value& value);
  ColumnType type;
  void* value;
};

struct Cell {
 public:
  Cell() = default;
  virtual ~Cell() = default;

  static std::shared_ptr<Cell> create(const std::vector<std::shared_ptr<Value>>& values);
  Value* values_;  // Array of values
};

}  // namespace storage
}  // namespace csql