/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <map>
#include <memory>
#include <vector>
#include "deephaven/client/column/column_source.h"
#include "deephaven/client/container/row_sequence.h"

namespace deephaven::client::table {
class Table;

namespace internal {
class TableStreamAdaptor {
  typedef deephaven::client::container::RowSequence RowSequence;
public:
  TableStreamAdaptor(const Table &table,
      std::vector<std::shared_ptr<RowSequence>> rowSequences, bool wantHeaders, bool wantRowNumbers,
      bool highlightCells) : table_(table), rowSequences_(std::move(rowSequences)),
      wantHeaders_(wantHeaders), wantRowNumbers_(wantRowNumbers), highlightCells_(highlightCells) {}
  TableStreamAdaptor(const TableStreamAdaptor &) = delete;
  TableStreamAdaptor &operator=(const TableStreamAdaptor &) = delete;
  ~TableStreamAdaptor() = default;

private:
  const Table &table_;
  std::vector<std::shared_ptr<RowSequence>> rowSequences_;
  bool wantHeaders_ = false;
  bool wantRowNumbers_ = false;
  bool highlightCells_ = false;

  friend std::ostream &operator<<(std::ostream &s, const TableStreamAdaptor &o);
};
}  // namespace internal

/**
 * The table schema that goes along with a Table class. This Schema object tells you about
 * the names and data types of the table columns.
 */
class Schema {
public:
  /**
   * Constructor.
   */
  explicit Schema(std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> columns);
  /**
   * Move constructor.
   */
  Schema(Schema &&other) noexcept;
  /**
   * Move assignment operator.
   */
  Schema &operator=(Schema &&other) noexcept;
  /**
   * Destructor.
   */
  ~Schema();

  /**
   * The schema (represented as a pair of name, column type) for each column in your Table.
   */
  const std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> &columns() const {
    return columns_;
  }

private:
  std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> columns_;
};

/**
 * An abstract base class representing a Deephaven table. This is used for example in
 * TickingUpdate to provide table snapshots to a caller who has subscribed to ticking tables.
 */
class Table {
public:
  /**
   * Alias.
   */
  typedef deephaven::client::column::ColumnSource ColumnSource;
  /**
   * Alias.
   */
  typedef deephaven::client::container::RowSequence RowSequence;

  /**
   * Constructor.
   */
  Table() = default;
  /**
   * Destructor.
   */
  virtual ~Table() = default;

  /**
   * Get the RowSequence (in position space) that underlies this Table.
   */
  virtual std::shared_ptr<RowSequence> getRowSequence() const = 0;
  /**
   * Gets a ColumnSource from the table by index.
   * @param columnIndex Must be in the half-open interval [0, numColumns).
   */
  virtual std::shared_ptr<ColumnSource> getColumn(size_t columnIndex) const = 0;

  /**
   * Gets a ColumnSource from the table by name. 'strict' controls whether the method
   * must succeed.
   * @param name The name of the column.
   * @param strict Whether the method must succeed.
   * @return If 'name' was found, returns the ColumnSource. If 'name' was not found and 'strict'
   * is true, throws an exception. If 'name' was not found and 'strict' is false, returns nullptr.
   */
  std::shared_ptr<ColumnSource> getColumn(std::string_view name, bool strict) const;
  /**
   * Gets the index of a ColumnSource from the table by name. 'strict' controls whether the method
   * must succeed.
   * @param name The name of the column.
   * @param strict Whether the method must succeed.
   * @return If 'name' was found, returns the index of the ColumnSource. If 'name' was not found and
   * 'strict' is true, throws an exception. If 'name' was not found and 'strict' is false, returns
   * (size_t)-1.
   */
  size_t getColumnIndex(std::string_view name, bool strict) const;

  /**
   * Number of rows in the table.
   */
  virtual size_t numRows() const = 0;
  /**
   * Number of columns in the table.
   */
  virtual size_t numColumns() const = 0;
  /**
   * The table schema.
   */
  virtual const Schema &schema() const = 0;

  /**
   * Creates an 'ostream adaptor' to use when printing the table. Example usage:
   * std::cout << myTable.stream(true, false).
   */
  internal::TableStreamAdaptor stream(bool wantHeaders, bool wantRowNumbers) const;

  /**
   * Creates an 'ostream adaptor' to use when printing the table. Example usage:
   * std::cout << myTable.stream(true, false, rowSeq).
   */
  internal::TableStreamAdaptor stream(bool wantHeaders, bool wantRowNumbers,
      std::shared_ptr<RowSequence> rowSequence) const;

  /**
   * Creates an 'ostream adaptor' to use when printing the table. Example usage:
   * std::cout << myTable.stream(true, false, rowSequences).
   */
  internal::TableStreamAdaptor stream(bool wantHeaders, bool wantRowNumbers,
      std::vector<std::shared_ptr<RowSequence>> rowSequences) const;
};
}  // namespace deephaven::client::table
