#include "dummy.h"

#include <iostream>

#include "memory/iterator.h"

namespace csql {
namespace storage {

void DummyStorage::insert(const Cell& cell) {
  cells_.push_back(cell);
}

void DummyStorage::remove(std::shared_ptr<Iterator> it) {
  auto it_ = std::dynamic_pointer_cast<DummyIterator>(it);
  if (!it_) {
    throw std::runtime_error("Invalid iterator");
  }
  it_->it_ = cells_.erase(it_->it_);
}

std::shared_ptr<Iterator> DummyStorage::getIterator() {
  return std::make_shared<DummyIterator>(shared_from_this());
}

void DummyStorage::clear() {
  cells_.clear();
}

DummyIterator::DummyIterator(std::shared_ptr<DummyStorage> storage)
    : storage_(storage), it_(storage->cells_.begin()) {}

bool DummyIterator::hasValue() {
  return it_ != storage_->cells_.end();
}

size_t DummyStorage::size() {
  return cells_.size();
}

void DummyIterator::next() {
  if (!hasValue()) return;
  ++it_;
}

Cell* DummyIterator::get() {
  return &(*it_);
}

}  // namespace storage
}  // namespace csql