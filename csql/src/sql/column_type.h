#pragma once

#include <ostream>

namespace csql {
enum class DataType {
  UNKNOWN,
  INT32,
  BOOL,
  STRING,
  BYTES,
};

// Represents the type of a column, e.g., FLOAT or STRING(10)
struct ColumnType {
  ColumnType() = default;
  ColumnType(DataType data_type, int64_t length = 0);
  int64_t length;  // Used for, e.g., string[10]
  DataType data_type;
};

bool operator==(const ColumnType& lhs, const ColumnType& rhs);
bool operator!=(const ColumnType& lhs, const ColumnType& rhs);
std::ostream& operator<<(std::ostream&, const ColumnType&);

}  // namespace csql
