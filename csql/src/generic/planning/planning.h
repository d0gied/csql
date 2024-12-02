#pragma once

#include <memory>

#include "sql/expr.h"
#include "sql/statements/statement.h"

namespace csql {
namespace storage {

class QueryPlan;
class Database;

enum class QueryType {
  kStepJoin,       // Join two tables
  kStepHashMerge,  // Hash merge
  kStepSort,       // Sort (order by)
  kStepFilter,     // Filter (where clause)
  kStepProject,    // Project (select columns)
};

enum class ScanType {
  kScanFullTable,  // Full table scan
  kScanIndex,      // Scan index
};

struct Cost {
  size_t total_steps;  // number of steps including all sub-steps
  size_t self_steps;   // number of steps for this step
  size_t amount;       // predicted amount of rows
};

struct QueryPlan {
 public:
  QueryPlan(QueryType type, std::shared_ptr<Expr> query, std::shared_ptr<Database> db)
      : type_(type), query_(query), scanType_(ScanType::kScanFullTable), db_(db) {}
  static std::shared_ptr<QueryPlan> create(std::shared_ptr<Expr> query,
                                           std::shared_ptr<Database> db);

  virtual ~QueryPlan() = default;

  const Cost& getCost() const;
  std::shared_ptr<QueryPlan> optimize();

 protected:
  Cost calculateCost();

  QueryType type_;
  ScanType scanType_;

  std::shared_ptr<QueryPlan> left_;
  std::shared_ptr<QueryPlan> right_;

  std::shared_ptr<Expr> query_;
  std::weak_ptr<Database> db_;
  Cost cost_;

 public:  // DEBUG
  std::string toMermaid(const std::string& name = "A") const;
  friend void makeMermaid(std::string& result, const csql::storage::QueryPlan& plan,
                          const std::string& name);
};

// std::ostream& operator<<(std::ostream& os, const QueryPlan& stepPlan);
// std::string toString(const QueryPlan& stepPlan);

}  // namespace storage
}  // namespace csql