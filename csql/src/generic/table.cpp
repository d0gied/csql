#include "table.h"

#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "column.h"
#include "memory/iterator.h"
#include "row.h"
#include "sql/column_type.h"
#include "sql/expr.h"

namespace csql {
namespace storage {

StorageTableIterator::StorageTableIterator(std::shared_ptr<StorageTable> table,
                                           std::shared_ptr<Iterator> iterator)
    : iterator_(iterator), table_(table) {}

bool StorageTableIterator::hasValue() const {
  return iterator_->hasValue();
}

std::shared_ptr<Row> StorageTableIterator::operator*() {
  return std::make_shared<Row>(table_, iterator_->get());
}

StorageTableIterator& StorageTableIterator::operator++() {
  iterator_->next();
  return *this;
}

std::shared_ptr<Iterator> StorageTableIterator::getMemoryIterator() {
  return iterator_;
}

ColumnType ITable::predictType(std::shared_ptr<Expr> expr) {
  if (expr->type == kExprColumnRef) {
    auto column = getColumn(expr);
    return column->type();
  } else if (expr->type == kExprLiteralInt) {
    return ColumnType(DataType::INT32);
  } else if (expr->type == kExprLiteralString) {
    return ColumnType(DataType::STRING, expr->name.size());
  } else if (expr->type == kExprLiteralBool) {
    return ColumnType(DataType::BOOL);
  } else if (expr->type == kExprLiteralBytes) {
    return ColumnType(DataType::BYTES, expr->name.size());
  } else if (expr->type == kExprOperator) {
    if (expr->opType == OperatorType::kOpParenthesis) {
      return predictType(expr->expr);
    } else if (expr->opType == OperatorType::kOpUnaryMinus) {
      return predictType(expr->expr);
    } else if (expr->opType == OperatorType::kOpNot) {
      return ColumnType(DataType::BOOL);
    } else if (expr->opType == OperatorType::kOpBitNot) {
      return predictType(expr->expr);
    } else if (expr->opType == OperatorType::kOpLength) {
      return ColumnType(DataType::INT32);
    } else if (expr->opType == OperatorType::kOpIsNull) {
      return ColumnType(DataType::BOOL);
    } else if (expr->opType == OperatorType::kOpPlus) {
      auto left = predictType(expr->expr);
      auto right = predictType(expr->expr2);
      if (left.data_type == DataType::STRING && right.data_type == DataType::STRING) {
        return ColumnType(DataType::STRING, left.length + right.length);
      } else if (left.data_type == DataType::INT32 && right.data_type == DataType::INT32) {
        return ColumnType(DataType::INT32);
      } else if (left.data_type == DataType::BYTES && right.data_type == DataType::BYTES) {
        return ColumnType(DataType::BYTES, left.length + right.length);
      } else {
        throw std::runtime_error("Invalid operation");
      }
    } else if (expr->opType == OperatorType::kOpMinus ||
               expr->opType == OperatorType::kOpAsterisk ||
               expr->opType == OperatorType::kOpSlash ||
               expr->opType == OperatorType::kOpPercentage) {
      auto predict = predictType(expr->expr);
      if (predict.data_type != DataType::INT32) {
        throw std::runtime_error("Invalid operation");
      }
      return predict;
    } else if (isLogicalOperator(expr->opType)) {
      return ColumnType(DataType::BOOL);
    } else if (isComparisonOperator(expr->opType)) {
      return ColumnType(DataType::BOOL);
    } else if (isArithmeticOperator(expr->opType)) {
      return ColumnType(DataType::INT32);
    } else {
      throw std::runtime_error("Unsupported operator: " + std::to_string(expr->opType));
    }
  } else {
    throw std::runtime_error("Unsupported expression type: " + std::to_string(expr->type));
  }
}

void ITable::exportToCSV(const std::string& filename) {
  // export table to csv
  std::ofstream file(filename);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open file: " + filename);
  }
  auto columns = getColumns();

  for (const auto& column : columns) {
    file << column->getName();
    if (&column != &columns.back()) {
      file << ",";
    }
  }
  file << std::endl;

  auto it = getIterator();

  while (it->hasValue()) {
    auto row = *(*it);
    ++(*it);  // increment iterator
    for (size_t i = 0; i < columns.size(); i++) {
      auto column = columns[i];
      if (row->isNull(i)) {
        file << "null";
      } else if (column->type().data_type == DataType::STRING) {
        file << "\"" << row->get<std::string>(i) << "\"";
      } else if (column->type().data_type == DataType::INT32) {
        file << std::to_string(row->get<int32_t>(i));
      } else if (column->type().data_type == DataType::BOOL) {
        file << (row->get<bool>(i) ? "true" : "false");
      } else if (column->type().data_type == DataType::BYTES) {
        auto bytes = row->get<std::vector<uint8_t>>(i);
        file << "0x";
        for (size_t j = 0; j < bytes.size(); j++) {
          int byte = bytes[bytes.size() - j - 1];
          file << std::setw(2) << std::setfill('0') << std::hex << byte;
        }
      } else {
        file << "null";
      }

      if (i < columns.size() - 1) {
        file << ",";
      }
    }
    file << std::endl;
  }

  file.close();
}

}  // namespace storage
}  // namespace csql