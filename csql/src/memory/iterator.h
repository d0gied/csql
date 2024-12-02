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

/* start <= cell < end */
class RangeIterator : public Iterator {
 protected:
  RangeIterator(std::shared_ptr<Cell> start, std::shared_ptr<Cell> end)
      : start_(start), end_(end) {}

  std::shared_ptr<Cell> start_;
  std::shared_ptr<Cell> end_;
};

}  // namespace storage
}  // namespace csql