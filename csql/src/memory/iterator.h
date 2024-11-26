#pragma once

#include <memory>
#include <vector>

#include "cell.h"

namespace csql {
namespace storage {

class Iterator {
 public:
  virtual ~Iterator() = default;

  virtual bool hasNext() = 0;
  virtual void next() = 0;
  virtual Cell* get() = 0;
};

}  // namespace storage
}  // namespace csql