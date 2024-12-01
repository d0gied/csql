#include "cell.h"

#include <iostream>
#include <string>

namespace csql {
namespace storage {

template <>
int32_t Cell::get<int32_t>(size_t index) const {
  return *static_cast<int32_t*>(values[index]);
}

template <>
bool Cell::get<bool>(size_t index) const {
  return *static_cast<bool*>(values[index]);
}

template <>
std::string Cell::get<std::string>(size_t index) const {
  return *static_cast<std::string*>(values[index]);
}

std::vector<uint8_t> Cell::getBytes(size_t index, size_t length) const {
  uint8_t* bytes = static_cast<uint8_t*>(values[index]);
  return std::vector<uint8_t>(bytes, bytes + length);
}

bool Cell::isNull(size_t index) const {
  return values[index] == nullptr;
}

}  // namespace storage
}  // namespace csql