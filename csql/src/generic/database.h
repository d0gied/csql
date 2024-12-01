#pragma once

#include <memory>
#include <ostream>
#include <unordered_map>

#include "row.h"
#include "sql/statements/create.h"
#include "sql/statements/delete.h"
#include "sql/statements/insert.h"
#include "sql/statements/select.h"
#include "sql/statements/update.h"
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
  std::shared_ptr<ITable> getTable(std::shared_ptr<Expr> tableRef) const;
  std::shared_ptr<ITable> create(std::shared_ptr<CreateStatement> createStatement);
  std::shared_ptr<ITable> select(std::shared_ptr<SelectStatement> selectStatement) const;
  std::shared_ptr<ITable> insert(std::shared_ptr<InsertStatement> insertStatement);
  std::shared_ptr<ITable> delete_(std::shared_ptr<DeleteStatement> deleteStatement);
  std::shared_ptr<ITable> update(std::shared_ptr<UpdateStatement> updateStatement);

  std::unordered_map<std::string, std::shared_ptr<StorageTable>> tables_;
};

}  // namespace storage
}  // namespace csql