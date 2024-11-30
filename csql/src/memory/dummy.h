#pragma once

#include "iterator.h"
#include "storage.h"

namespace csql {
namespace storage {

class DummyStorage;
class DummyIterator;

class DummyStorage : public IStorage, public std::enable_shared_from_this<DummyStorage> {
 public:
  DummyStorage() = default;

  void insert(const Cell& cell) override;
  void remove(std::shared_ptr<Iterator> it) override;
  std::shared_ptr<Iterator> getIterator() override;
  size_t size() override;
  void clear() override;

 private:
  std::vector<Cell> cells_;

  friend class DummyIterator;
};

class DummyIterator : public Iterator {
 public:
  DummyIterator(std::shared_ptr<DummyStorage> storage);

  bool hasValue() override;
  void next() override;
  Cell* get() override;

 private:
  std::vector<Cell>::iterator it_;
  std::shared_ptr<DummyStorage> storage_;

  friend class DummyStorage;
};

}  // namespace storage
}  // namespace csql