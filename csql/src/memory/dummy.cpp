#include "dummy.h"

namespace csql {
namespace storage {

void DummyStorage::insert(const Cell& cell) {
  cells_.push_back(cell);
}

std::shared_ptr<Iterator> DummyStorage::getIterator() {
  if (cells_.empty()) return nullptr;
  return std::make_shared<DummyIterator>(shared_from_this());
}

void DummyStorage::clear() {
  cells_.clear();
}

DummyIterator::DummyIterator(std::shared_ptr<DummyStorage> storage)
    : storage_(storage), index_(0) {}

bool DummyIterator::hasValue() {
  return index_ < storage_->cells_.size();
}

size_t DummyStorage::size() {
  return cells_.size();
}

void DummyIterator::next() {
  if (!hasValue()) return;
  index_++;
}

Cell* DummyIterator::get() {
  return &storage_->cells_[index_];
}

}  // namespace storage
}  // namespace csql