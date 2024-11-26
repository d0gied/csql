#pragma once

#include <vector>

#include "cell.h"
#include "iterator.h"

namespace csql {
namespace storage {

class IStorage {
 public:
  virtual ~IStorage() = default;

  virtual void insert(const Cell& cell) = 0;

  virtual std::shared_ptr<Iterator> getIterator() = 0;

  virtual size_t size() = 0;

  virtual void clear() = 0;
};

}  // namespace storage

}  // namespace csql
