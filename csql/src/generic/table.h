#pragma once

#include <memory>
#include <vector>

#include "../memory/iterator.h"
#include "../memory/storage.h"
#include "../sql/statements/create.h"
#include "../sql/statements/insert.h"
#include "column.h"

namespace csql {
namespace storage {

class Table;
class TableIterator;
class Row;
class Column;

class TableIterator {
 public:
  TableIterator(std::shared_ptr<Table> table, std::shared_ptr<Iterator> iterator);

  bool hasNext();
  std::shared_ptr<Row> next();
  std::shared_ptr<Row> operator++();

 private:
  std::shared_ptr<Table> table_;
  std::shared_ptr<Iterator> iterator_;
  friend class Table;
};

class Table : public std::enable_shared_from_this<Table> {
 public:
  Table();
  virtual ~Table();

  static std::shared_ptr<Table> create(std::shared_ptr<CreateStatement> createStatement);

  void insert(std::shared_ptr<InsertStatement> insertStatement);

  std::shared_ptr<TableIterator> getIterator();

  size_t size() const;

  void exportToCSV(const std::string& filename);

 private:
  void addColumn(std::shared_ptr<Column> column);

  std::vector<std::shared_ptr<Column>> columns_;
  std::shared_ptr<IStorage> storage_;
  friend class TableIterator;
  friend class Column;
  friend class Row;

  friend std::ostream& operator<<(std::ostream& stream, const Row& row);
};

}  // namespace storage
}  // namespace csql