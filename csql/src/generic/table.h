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
#include "sql/column_type.h"
#include "sql/statements/delete.h"
#include "sql/statements/select.h"
#include "sql/statements/update.h"

namespace csql {
namespace storage {

class ITable;
class TableIterator;
class StorageTableIterator;
class WhereClauseIterator;
class SelectedTable;
class Row;
class Column;

class TableIterator {
 public:
  virtual bool hasValue() const = 0;
  virtual TableIterator& operator++() = 0;
  virtual std::shared_ptr<Row> operator*() = 0;

  virtual std::shared_ptr<Iterator> getMemoryIterator() = 0;

  friend class ITable;
};

class WhereClauseIterator : public TableIterator {
 public:
  WhereClauseIterator(std::shared_ptr<TableIterator> tableIterator,
                      std::shared_ptr<Expr> whereClause);
  virtual ~WhereClauseIterator() = default;

  bool hasValue() const override;
  WhereClauseIterator& operator++() override;
  std::shared_ptr<Row> operator*() override;
  std::shared_ptr<Iterator> getMemoryIterator() override;

 protected:
  std::shared_ptr<TableIterator> tableIterator_;
  std::shared_ptr<Expr> whereClause_;
  friend class StorageTable;
};

class SelectedTableIterator : public TableIterator {
 public:
  SelectedTableIterator(std::shared_ptr<SelectedTable> table);
  virtual ~SelectedTableIterator() = default;

  virtual bool hasValue() const override;
  virtual SelectedTableIterator& operator++() override;
  virtual std::shared_ptr<Row> operator*() override;
  virtual std::shared_ptr<Iterator> getMemoryIterator() override;

 protected:
  std::shared_ptr<WhereClauseIterator> whereClauseIterator_;
  std::shared_ptr<SelectedTable> table_;
  friend class SelectedTable;
};

class VirtualTable;

class ITable {
 public:
  virtual ~ITable() = default;

  virtual void insert(std::shared_ptr<InsertStatement> insertStatement) = 0;
  virtual void delete_(std::shared_ptr<DeleteStatement> deleteStatement) = 0;
  virtual void update(std::shared_ptr<UpdateStatement> updateStatement) = 0;
  virtual std::shared_ptr<VirtualTable> select(
      std::shared_ptr<SelectStatement> selectStatement) = 0;

  virtual std::shared_ptr<TableIterator> getIterator() = 0;
  virtual const std::string& getName() const = 0;
  virtual const std::vector<std::shared_ptr<Column>>& getColumns() = 0;
  virtual std::shared_ptr<Column> getColumn(std::shared_ptr<Expr> columnExpr) {
    return nullptr;
  };

  virtual ColumnType predictType(std::shared_ptr<Expr> expr);

  // virtual std::shared_ptr<VirtualTable> select(std::shared_ptr<Expr> whereClause) = 0;
  // virtual std::shared_ptr<VirtualTable> join(std::shared_ptr<ITable> table,
  //                                            std::shared_ptr<Expr> onClause) = 0;

  virtual void exportToCSV(const std::string& filename);
};  // namespace storage

class StorageTable : public ITable, public std::enable_shared_from_this<StorageTable> {
 public:
  StorageTable();
  virtual ~StorageTable();

  static std::shared_ptr<StorageTable> create(std::shared_ptr<CreateStatement> createStatement);
  static std::shared_ptr<StorageTable> create(std::shared_ptr<CreateStatement> createStatement,
                                              std::shared_ptr<ITable> refTable);

  void insert(std::shared_ptr<InsertStatement> insertStatement) override;
  void delete_(std::shared_ptr<DeleteStatement> deleteStatement) override;
  void update(std::shared_ptr<UpdateStatement> updateStatement) override;
  std::shared_ptr<VirtualTable> select(std::shared_ptr<SelectStatement> selectStatement) override;

  std::shared_ptr<TableIterator> getIterator() override;

  const std::string& getName() const override;
  const std::vector<std::shared_ptr<Column>>& getColumns() override;
  std::shared_ptr<Column> getColumn(std::shared_ptr<Expr> columnExpr) override;

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

class StorageTableIterator : public TableIterator {
 public:
  StorageTableIterator(std::shared_ptr<StorageTable> table, std::shared_ptr<Iterator> iterator);
  virtual ~StorageTableIterator() = default;

  virtual bool hasValue() const override;
  virtual StorageTableIterator& operator++() override;
  virtual std::shared_ptr<Row> operator*() override;
  virtual std::shared_ptr<Iterator> getMemoryIterator() override;

 protected:
  std::shared_ptr<StorageTable> table_;
  std::shared_ptr<Iterator> iterator_;
  friend class StorageTable;
};

class VirtualTable : public ITable {
 public:
  VirtualTable() = default;
  virtual ~VirtualTable() = default;
};

class JoinTable : public VirtualTable, public std::enable_shared_from_this<JoinTable> {
 public:
  JoinTable(std::shared_ptr<ITable> left, std::shared_ptr<ITable> right,
            std::shared_ptr<Expr> onClause, OperatorType joinType);
  static std::shared_ptr<JoinTable> create(std::shared_ptr<ITable> left,
                                           std::shared_ptr<ITable> right,
                                           std::shared_ptr<Expr> onClause, OperatorType joinType);
  virtual ~JoinTable() = default;

  void insert(std::shared_ptr<InsertStatement> insertStatement) override;
  void delete_(std::shared_ptr<DeleteStatement> deleteStatement) override;
  void update(std::shared_ptr<UpdateStatement> updateStatement) override;
  std::shared_ptr<VirtualTable> select(std::shared_ptr<SelectStatement> selectStatement) override;

  std::shared_ptr<TableIterator> getIterator() override;
  const std::string& getName() const override;
  const std::vector<std::shared_ptr<Column>>& getColumns() override;
  std::shared_ptr<Column> getColumn(std::shared_ptr<Expr> columnExpr) override;

  friend class JoinTableIterator;
  friend class InnerJoinIterator;

 private:
  std::shared_ptr<ITable> left_;
  std::shared_ptr<ITable> right_;
  std::shared_ptr<Expr> onClause_;
  OperatorType joinType_;
  std::string name_;
  std::vector<std::shared_ptr<Column>> columns_;
};

class JoinTableIterator : public TableIterator {
 public:
  JoinTableIterator(std::shared_ptr<JoinTable> table);
  virtual ~JoinTableIterator() = default;

  virtual bool hasValue() const override;
  // virtual JoinTableIterator& operator++() override;
  virtual std::shared_ptr<Row> operator*() override;
  virtual std::shared_ptr<Iterator> getMemoryIterator() override;

 protected:
  std::shared_ptr<JoinTable> table_;
  std::shared_ptr<TableIterator> leftTableIterator_;
  std::shared_ptr<TableIterator> rightTableIterator_;
  std::shared_ptr<Row> row_;

  bool match();
  void resetRight();
  std::shared_ptr<Row> mergeRows();
};

class InnerJoinIterator : public JoinTableIterator {
 public:
  InnerJoinIterator(std::shared_ptr<JoinTable> table);
  virtual ~InnerJoinIterator() = default;
  InnerJoinIterator& operator++() override;
};

class SelectedTable : public VirtualTable, public std::enable_shared_from_this<SelectedTable> {
 public:
  SelectedTable(std::shared_ptr<ITable> table, std::shared_ptr<SelectStatement> selectStatement);
  static std::shared_ptr<SelectedTable> create(std::shared_ptr<ITable> table,
                                               std::shared_ptr<SelectStatement> selectStatement);
  virtual ~SelectedTable() = default;

  void insert(std::shared_ptr<InsertStatement> insertStatement) override;
  void delete_(std::shared_ptr<DeleteStatement> deleteStatement) override;
  void update(std::shared_ptr<UpdateStatement> updateStatement) override;
  std::shared_ptr<VirtualTable> select(std::shared_ptr<SelectStatement> selectStatement) override;

  std::shared_ptr<TableIterator> getIterator() override;
  const std::string& getName() const override;
  const std::vector<std::shared_ptr<Column>>& getColumns() override;
  std::shared_ptr<Column> getColumn(std::shared_ptr<Expr> columnExpr) override;

  std::shared_ptr<ITable> getOriginalTable() const;
  std::shared_ptr<Expr> getWhereClause() const;

  friend class SelectedTableIterator;

 private:
  std::string name_;
  std::shared_ptr<ITable> table_;
  std::shared_ptr<SelectStatement> selectStatement_;
  std::vector<std::shared_ptr<Column>> columns_;
};

}  // namespace storage
}  // namespace csql