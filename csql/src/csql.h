#include <memory>

#include "generic/database.h"
#include "sql/parser.h"

namespace csql {

using TableIterator = storage::TableIterator;
using QueryPlan = storage::QueryPlan;

class Database {
 public:
  Database() : db_(std::make_shared<storage::Database>()) {}
  std::shared_ptr<TableIterator> execute(const std::string& sql) {
    return db_->execute(sql);
  }
  std::shared_ptr<QueryPlan> plan(const std::string& sql) {
    return db_->plan(sql);
  }
  void exportTableToCSV(const std::string& tableName, const std::string& filename) {
    db_->exportTableToCSV(tableName, filename);
  }

 private:
  std::shared_ptr<storage::Database> db_;
};

}  // namespace csql