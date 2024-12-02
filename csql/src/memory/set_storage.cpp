#include "set_storage.h"

#include <memory>

#include "memory/cell.h"
#include "memory/storage.h"

namespace csql {
namespace storage {

bool SetStorage::containsKey(std::shared_ptr<Cell> cell) {
  return cells_.find(cell) != cells_.end();
}

void SetStorage::insert(std::shared_ptr<Cell> cell) {
  if (containsKey(cell)) {
    throw std::runtime_error("Key already exists");
  }
  cells_.insert(cell);
}

void SetStorage::remove(std::shared_ptr<Iterator> it) {
  auto it_ = std::dynamic_pointer_cast<SetIterator>(it);
  if (!it_) {
    throw std::runtime_error("Invalid iterator");
  }
  it_->it_ = cells_.erase(it_->it_);
}

std::shared_ptr<Iterator> SetStorage::getIterator() {
  return std::make_shared<SetIterator>(shared_from_this());
}

std::shared_ptr<RangeIterator> SetStorage::getRangeIterator(std::shared_ptr<Cell> start,
                                                            std::shared_ptr<Cell> end) {
  return std::make_shared<SetRangeIterator>(start, end, shared_from_this());
}

size_t SetStorage::size() {
  return cells_.size();
}

void SetStorage::clear() {
  cells_.clear();
}

SetIterator::SetIterator(std::shared_ptr<SetStorage> storage)
    : storage_(storage), it_(storage->cells_.begin()) {}

bool SetIterator::hasValue() {
  return it_ != storage_->cells_.end();
}

void SetIterator::next() {
  if (!hasValue()) return;
  ++it_;
}

std::shared_ptr<Cell> SetIterator::get() {
  return *it_;
}

SetRangeIterator::SetRangeIterator(std::shared_ptr<Cell> start, std::shared_ptr<Cell> end,
                                   std::shared_ptr<SetStorage> storage)
    : RangeIterator(start, end), storage_(storage) {
  if (storage->comparator_(end, start)) {  // end < start
    throw std::runtime_error("Invalid range");
  }
  if (!start) {
    it_ = storage_->cells_.begin();
  } else {
    it_ = storage_->cells_.lower_bound(start);  // start <= cell
  }
  if (!end) {
    end_ = storage_->cells_.end();
  } else {
    end_ = storage_->cells_.lower_bound(end);  // cell < end
  }
}

bool SetRangeIterator::hasValue() {
  return it_ != end_;
}

void SetRangeIterator::next() {
  if (!hasValue()) return;
  ++it_;
}

std::shared_ptr<Cell> SetRangeIterator::get() {
  return *it_;
}

}  // namespace storage
}  // namespace csql