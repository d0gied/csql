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
class FilteredTable;
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

class VirtualTable;

class ITable {
 public:
  virtual ~ITable() = default;

  virtual void insert(std::shared_ptr<InsertStatement> insertStatement) = 0;
  virtual void delete_(std::shared_ptr<DeleteStatement> deleteStatement) = 0;
  virtual void update(std::shared_ptr<UpdateStatement> updateStatement) = 0;
  virtual std::shared_ptr<VirtualTable> filter(std::shared_ptr<Expr> whereClause) = 0;

  virtual std::shared_ptr<TableIterator> getIterator() = 0;
  virtual const std::string& getName() const;
  virtual const std::vector<std::shared_ptr<Column>>& getColumns();
  virtual std::shared_ptr<Column> getColumn(std::shared_ptr<Expr> columnExpr);

  virtual ColumnType predictType(std::shared_ptr<Expr> expr);

  static std::shared_ptr<ITable> hashMerge(std::shared_ptr<ITable> left,
                                           std::shared_ptr<ITable> right);

  virtual void exportToCSV(const std::string& filename);

 protected:
  std::vector<std::shared_ptr<Column>> columns_;
  std::string name_;
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

  std::shared_ptr<VirtualTable> filter(std::shared_ptr<Expr> whereClause) override;
  std::shared_ptr<TableIterator> getIterator() override;

  size_t getRowsCount() const;

 private:
  void addColumn(std::shared_ptr<Column> column);

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

class VirtualTable : public ITable, public std::enable_shared_from_this<VirtualTable> {
 public:
  VirtualTable() = default;
  virtual ~VirtualTable() = default;

  virtual void insert(std::shared_ptr<InsertStatement> insertStatement) override;
  virtual void delete_(std::shared_ptr<DeleteStatement> deleteStatement) override;
  virtual void update(std::shared_ptr<UpdateStatement> updateStatement) override;
  std::shared_ptr<VirtualTable> filter(std::shared_ptr<Expr> whereClause) override;
};

class JoinTable : public VirtualTable {
 public:
  JoinTable(std::shared_ptr<ITable> left, std::shared_ptr<ITable> right,
            std::shared_ptr<Expr> onClause, OperatorType joinType);
  static std::shared_ptr<JoinTable> create(std::shared_ptr<ITable> left,
                                           std::shared_ptr<ITable> right,
                                           std::shared_ptr<Expr> onClause, OperatorType joinType);
  virtual ~JoinTable() = default;

  std::shared_ptr<TableIterator> getIterator() override;

  friend class JoinTableIterator;
  friend class InnerJoinIterator;

 private:
  std::shared_ptr<ITable> left_;
  std::shared_ptr<ITable> right_;
  std::shared_ptr<Expr> onClause_;
  OperatorType joinType_;
};

class JoinTableIterator : public TableIterator {
 public:
  JoinTableIterator(std::shared_ptr<JoinTable> table);
  virtual ~JoinTableIterator() = default;

  virtual bool hasValue() const override;
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

class OuterJoinIterator : public JoinTableIterator {
 public:
  OuterJoinIterator(std::shared_ptr<JoinTable> table);
  virtual ~OuterJoinIterator() = default;
  OuterJoinIterator& operator++() override;
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

class FilteredTable : public VirtualTable {
 public:
  FilteredTable(std::shared_ptr<ITable> table, std::shared_ptr<Expr> whereClause);
  static std::shared_ptr<FilteredTable> create(std::shared_ptr<ITable> table,
                                               std::shared_ptr<Expr> whereClause);
  virtual ~FilteredTable() = default;

  std::shared_ptr<TableIterator> getIterator() override;

  std::shared_ptr<ITable> getOriginalTable() const;
  std::shared_ptr<Expr> getWhereClause() const;

  friend class FilteredTableIterator;

 private:
  std::shared_ptr<ITable> table_;
  std::shared_ptr<Expr> whereClause_;
};

class EvaluatedTable;
class EvaluateIterator : public TableIterator {
 public:
  EvaluateIterator(std::shared_ptr<EvaluatedTable> table);
  virtual ~EvaluateIterator() = default;

  virtual bool hasValue() const override;
  virtual EvaluateIterator& operator++() override;
  virtual std::shared_ptr<Row> operator*() override;
  virtual std::shared_ptr<Iterator> getMemoryIterator() override;

 protected:
  std::shared_ptr<EvaluatedTable> table_;
  std::shared_ptr<TableIterator> it_;
  friend class EvaluatedTable;
};

class EvaluatedTable : public VirtualTable {
 public:
  EvaluatedTable(std::shared_ptr<ITable> table,
                 std::shared_ptr<std::vector<std::shared_ptr<Expr>>> expressions);

  static std::shared_ptr<EvaluatedTable> create(
      std::shared_ptr<ITable> table,
      std::shared_ptr<std::vector<std::shared_ptr<Expr>>> expressions);
  virtual ~EvaluatedTable() = default;

  std::shared_ptr<TableIterator> getIterator() override;

  std::shared_ptr<ITable> getOriginalTable() const;
  std::shared_ptr<Expr> getWhereClause() const;

  friend class EvaluateIterator;

 private:
  std::shared_ptr<ITable> table_;
  std::shared_ptr<std::vector<std::shared_ptr<Expr>>> expressions_;
};

}  // namespace storage
}  // namespace csql