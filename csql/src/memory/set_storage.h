#pragma once

#include <memory>
#include <set>

#include "iterator.h"
#include "storage.h"

namespace csql {
namespace storage {

class SetStorage;
class DummyIterator;

typedef std::set<std::shared_ptr<Cell>, KeyComparator> Set;

class SetStorage : public IStorage, public std::enable_shared_from_this<SetStorage> {
 public:
  SetStorage(KeyComparator comparator) : cells_(comparator), comparator_(comparator) {}

  void insert(std::shared_ptr<Cell> cell) override;
  void remove(std::shared_ptr<Iterator> it) override;
  bool containsKey(std::shared_ptr<Cell> cell) override;
  std::shared_ptr<Iterator> getIterator() override;
  std::shared_ptr<RangeIterator> getRangeIterator(std::shared_ptr<Cell> start,
                                                  std::shared_ptr<Cell> end) override;

  size_t size() override;
  void clear() override;

 private:
  Set cells_;
  KeyComparator comparator_;

  friend class SetIterator;
  friend class SetRangeIterator;
};

class SetIterator : public Iterator {
 public:
  SetIterator(std::shared_ptr<SetStorage> storage);
  bool hasValue() override;
  void next() override;
  std::shared_ptr<Cell> get() override;

 private:
  Set::iterator it_;
  std::shared_ptr<SetStorage> storage_;

  friend class SetStorage;
};

class SetRangeIterator : public RangeIterator {
 public:
  SetRangeIterator(std::shared_ptr<Cell> start, std::shared_ptr<Cell> end,
                   std::shared_ptr<SetStorage> storage);

  bool hasValue() override;
  void next() override;
  std::shared_ptr<Cell> get() override;

 private:
  Set::iterator it_;
  Set::iterator end_;
  std::shared_ptr<SetStorage> storage_;

  friend class SetStorage;
};

}  // namespace storage
}  // namespace csql