#pragma once

#include <memory>
#include <string>
#include <vector>

#include "../memory/iterator.h"
#include "../memory/storage.h"
#include "../sql/statements/create.h"
#include "../sql/statements/insert.h"
#include "column.h"
#include "row.h"

namespace csql {
namespace storage {

class Table;
class TableIterator;
class AllTableIterator;
class WhereClauseIterator;
class Row;
class Column;

class TableIterator {
 public:
  virtual bool hasValue() const = 0;
  virtual TableIterator& operator++() = 0;
  virtual std::shared_ptr<Row> operator*() const = 0;

  friend class Table;
};

class AllTableIterator : public TableIterator {
 public:
  AllTableIterator(std::shared_ptr<Table> table, std::shared_ptr<Iterator> iterator);
  virtual ~AllTableIterator() = default;

  virtual bool hasValue() const override;
  virtual AllTableIterator& operator++() override;
  virtual std::shared_ptr<Row> operator*() const override;

 protected:
  std::shared_ptr<Table> table_;
  std::shared_ptr<Iterator> iterator_;
  friend class Table;
};

class WhereClauseIterator : public TableIterator {
 public:
  WhereClauseIterator(std::shared_ptr<TableIterator> tableIterator,
                      std::shared_ptr<Expr> whereClause);
  virtual ~WhereClauseIterator() = default;

  virtual bool hasValue() const override;
  virtual WhereClauseIterator& operator++() override;
  virtual std::shared_ptr<Row> operator*() const override;

 protected:
  std::shared_ptr<TableIterator> tableIterator_;
  std::shared_ptr<Expr> whereClause_;
  friend class Table;
};

class Table : public std::enable_shared_from_this<Table> {
 public:
  Table();
  virtual ~Table();

  static std::shared_ptr<Table> create(std::shared_ptr<CreateStatement> createStatement);

  void insert(std::shared_ptr<InsertStatement> insertStatement);

  std::shared_ptr<AllTableIterator> getIterator();

  size_t size() const;
  const std::string& getName() const;
  const std::vector<std::shared_ptr<Column>>& getColumns() const;

  void exportToCSV(const std::string& filename);

 private:
  void addColumn(std::shared_ptr<Column> column);

  std::string name_;
  std::vector<std::shared_ptr<Column>> columns_;
  std::shared_ptr<IStorage> storage_;
  friend class TableIterator;
  friend class Column;
  friend class Row;

  friend std::ostream& operator<<(std::ostream& stream, const Row& row);
};

}  // namespace storage
}  // namespace csql