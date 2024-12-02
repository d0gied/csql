#pragma once

#include <functional>
#include <vector>

#include "cell.h"
#include "iterator.h"

namespace csql {
namespace storage {

// Comparator type
typedef std::function<bool(std::shared_ptr<Cell>, std::shared_ptr<Cell>)>
    KeyComparator;  // left < right

class IStorage {
 public:
  virtual ~IStorage() = default;

  virtual void insert(std::shared_ptr<Cell> cell) = 0;
  virtual void remove(std::shared_ptr<Iterator> it) = 0;
  virtual bool containsKey(std::shared_ptr<Cell> cell) = 0;

  virtual std::shared_ptr<Iterator> getIterator() = 0;
  virtual std::shared_ptr<RangeIterator> getRangeIterator(std::shared_ptr<Cell> start,
                                                          std::shared_ptr<Cell> end) = 0;

  virtual size_t size() = 0;

  virtual void clear() = 0;
};

}  // namespace storage

}  // namespace csql
