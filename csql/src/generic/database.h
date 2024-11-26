#pragma once

#include <memory>
#include <ostream>
#include <unordered_map>

#include "../sql/statements.h"
#include "table.h"

namespace csql {
namespace storage {
class Database {
 public:
  Database() = default;
  virtual ~Database() = default;

  std::shared_ptr<TableIterator> execute(const std::string& sql);

  void exportTableToCSV(const std::string& tableName, const std::string& filename);

 private:
  std::shared_ptr<TableIterator> create(std::shared_ptr<CreateStatement> createStatement);
  // std::shared_ptr<TableIterator> select(std::shared_ptr<SelectStatement> selectStatement);
  std::shared_ptr<TableIterator> insert(std::shared_ptr<InsertStatement> insertStatement);

  std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
};

}  // namespace storage
}  // namespace csql