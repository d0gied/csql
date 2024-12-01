#pragma once

#include <memory>
#include <vector>

#include "cell.h"

namespace csql {
namespace storage {

class Iterator {
 public:
  virtual ~Iterator() = default;

  virtual bool hasValue() = 0;
  virtual void next() = 0;
  virtual std::shared_ptr<Cell> get() = 0;
};

}  // namespace storage
}  // namespace csql