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
#include "sql/statements/delete.h"
#include "sql/statements/update.h"

namespace csql {
namespace storage {

class ITable;
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

  virtual std::shared_ptr<Iterator> getMemoryIterator() const = 0;

  friend class ITable;
};

class AllTableIterator : public TableIterator {
 public:
  AllTableIterator(std::shared_ptr<const ITable> table, std::shared_ptr<Iterator> iterator);
  virtual ~AllTableIterator() = default;

  virtual bool hasValue() const override;
  virtual AllTableIterator& operator++() override;
  virtual std::shared_ptr<Row> operator*() const override;
  virtual std::shared_ptr<Iterator> getMemoryIterator() const override;

 protected:
  std::shared_ptr<const ITable> table_;
  std::shared_ptr<Iterator> iterator_;
  friend class StorageTable;
};

class WhereClauseIterator : public TableIterator {
 public:
  WhereClauseIterator(std::shared_ptr<TableIterator> tableIterator,
                      std::shared_ptr<Expr> whereClause);
  virtual ~WhereClauseIterator() = default;

  virtual bool hasValue() const override;
  virtual WhereClauseIterator& operator++() override;
  virtual std::shared_ptr<Row> operator*() const override;
  virtual std::shared_ptr<Iterator> getMemoryIterator() const override;

 protected:
  std::shared_ptr<TableIterator> tableIterator_;
  std::shared_ptr<Expr> whereClause_;
  friend class StorageTable;
};

class VirtualTable;

class ITable {
 public:
  virtual ~ITable() = default;

  virtual void insert(std::shared_ptr<InsertStatement> insertStatement) = 0;
  virtual void delete_(std::shared_ptr<DeleteStatement> deleteStatement) = 0;
  virtual void update(std::shared_ptr<UpdateStatement> updateStatement) = 0;

  virtual std::shared_ptr<TableIterator> getIterator() const = 0;
  virtual const std::string& getName() const = 0;
  virtual const std::vector<std::shared_ptr<Column>>& getColumns() const = 0;

  // virtual std::shared_ptr<VirtualTable> select(std::shared_ptr<Expr> whereClause) = 0;
  // virtual std::shared_ptr<VirtualTable> join(std::shared_ptr<ITable> table,
  //                                            std::shared_ptr<Expr> onClause) = 0;

  virtual void exportToCSV(const std::string& filename) = 0;
};

class StorageTable : public ITable, public std::enable_shared_from_this<StorageTable> {
 public:
  StorageTable();
  virtual ~StorageTable();

  static std::shared_ptr<StorageTable> create(std::shared_ptr<CreateStatement> createStatement);

  void insert(std::shared_ptr<InsertStatement> insertStatement) override;
  void delete_(std::shared_ptr<DeleteStatement> deleteStatement) override;
  void update(std::shared_ptr<UpdateStatement> updateStatement) override;

  std::shared_ptr<TableIterator> getIterator() const override;

  const std::string& getName() const override;
  const std::vector<std::shared_ptr<Column>>& getColumns() const override;

  void exportToCSV(const std::string& filename) override;

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

class VirtualTable : public ITable {
 public:
  VirtualTable();
  virtual ~VirtualTable();
};

}  // namespace storage
}  // namespace csql