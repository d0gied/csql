#include "row.h"

#include "column.h"
#include "table.h"

namespace csql {
namespace storage {

Row::Row(std::shared_ptr<Table> table, Cell* cell) : table_(table), cell_(cell) {}

template <>
int32_t Row::get<int32_t>(size_t index) const {
  return cell_->values_[index].getInt();
}

template <>
bool Row::get<bool>(size_t index) const {
  return cell_->values_[index].getBool();
}

template <>
std::string Row::get<std::string>(size_t index) const {
  return cell_->values_[index].getString();
}

template <>
std::vector<uint8_t> Row::get<std::vector<uint8_t>>(size_t index) const {
  return cell_->values_[index].getBytes();
}

bool Row::isNull(size_t index) const {
  return cell_->values_[index].value == nullptr;
}

bool Row::isNull(std::string columnName) const {
  auto table = table_.lock();
  for (size_t i = 0; i < table->columns_.size(); i++) {
    if (table->columns_[i]->getName() == columnName) {
      return isNull(i);
    }
  }

  throw std::runtime_error("Column not found: " + columnName);
}

std::ostream& operator<<(std::ostream& stream, const Row& row) {
  auto table = row.table_.lock();
  for (size_t i = 0; i < table->columns_.size(); i++) {
    auto column = table->columns_[i];
    std::string data;
    if (column->type().data_type == DataType::STRING) {
      data = row.get<std::string>(i);
    } else if (column->type().data_type == DataType::INT32) {
      data = std::to_string(row.get<int32_t>(i));
    } else if (column->type().data_type == DataType::BOOL) {
      data = row.get<bool>(i) ? "true" : "false";
    } else {
      data = "null";
    }

    stream << column->getName() << ": " << data;
    if (i < table->columns_.size() - 1) {
      stream << ", ";
    }

    stream << std::endl;
  }

  return stream;
}

}  // namespace storage
}  // namespace csql