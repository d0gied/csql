#pragma once

#include <memory>
#include <ostream>
#include <vector>

#include "../sql/column_type.h"
#include "sql/expr.h"

namespace csql {
namespace storage {

struct Cell {
 public:
  Cell() = default;
  virtual ~Cell() = default;

  template <typename T>
  T get(size_t index) const;

  std::vector<uint8_t> getBytes(size_t index, size_t length) const;

  bool isNull(size_t index) const;

  std::vector<void*> values;
};

}  // namespace storage
}  // namespace csql