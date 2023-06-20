/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <map>
#include <memory>
#include <optional>
#include <vector>
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"

namespace deephaven::dhcore::clienttable {
/**
 * Declaration provided in deephaven/dhcore/schema/schema.h
 */
class Schema;
/**
 * Forward declaration (provided below).
 */
class ClientTable;

namespace internal {
class TableStreamAdaptor {
  typedef deephaven::dhcore::container::RowSequence RowSequence;
public:
  TableStreamAdaptor(const ClientTable &table,
      std::vector<std::shared_ptr<RowSequence>> rowSequences, bool wantHeaders, bool wantRowNumbers,
      bool highlightCells) : table_(table), rowSequences_(std::move(rowSequences)),
      wantHeaders_(wantHeaders), wantRowNumbers_(wantRowNumbers), highlightCells_(highlightCells) {}
  TableStreamAdaptor(const TableStreamAdaptor &) = delete;
  TableStreamAdaptor &operator=(const TableStreamAdaptor &) = delete;
  ~TableStreamAdaptor() = default;

private:
  const ClientTable &table_;
  std::vector<std::shared_ptr<RowSequence>> rowSequences_;
  bool wantHeaders_ = false;
  bool wantRowNumbers_ = false;
  bool highlightCells_ = false;

  friend std::ostream &operator<<(std::ostream &s, const TableStreamAdaptor &o);
};
}  // namespace internal

/**
 * An abstract base class representing a Deephaven table. This is used for example in
 * TickingUpdate to provide table snapshots to a caller who has subscribed to ticking tables.
 */
class ClientTable {
public:
  /**
   * Alias.
   */
  typedef deephaven::dhcore::column::ColumnSource ColumnSource;
  /**
   * Alias.
   */
  typedef deephaven::dhcore::container::RowSequence RowSequence;

  /**
   * Constructor.
   */
  ClientTable() = default;
  /**
   * Destructor.
   */
  virtual ~ClientTable() = default;

  /**
   * Get the RowSequence (in position space) that underlies this Table.
   */
  virtual std::shared_ptr<RowSequence> getRowSequence() const = 0;
  /**
   * Gets a ColumnSource from the clienttable by index.
   * @param columnIndex Must be in the half-open interval [0, numColumns).
   */
  virtual std::shared_ptr<ColumnSource> getColumn(size_t columnIndex) const = 0;

  /**
   * Gets a ColumnSource from the clienttable by name. 'strict' controls whether the method
   * must succeed.
   * @param name The name of the column.
   * @param strict Whether the method must succeed.
   * @return If 'name' was found, returns the ColumnSource. If 'name' was not found and 'strict'
   * is true, throws an exception. If 'name' was not found and 'strict' is false, returns nullptr.
   */
  std::shared_ptr<ColumnSource> getColumn(std::string_view name, bool strict) const;
  /**
   * Gets the index of a ColumnSource from the clienttable by name. 'strict' controls whether the method
   * must succeed.
   * @param name The name of the column.
   * @param strict Whether the method must succeed.
   * @return If 'name' was found, returns the index of the ColumnSource. If 'name' was not found and
   * 'strict' is true, throws an exception. If 'name' was not found and 'strict' is false, returns
   * an empty optional.
   */
  std::optional<size_t> getColumnIndex(std::string_view name, bool strict) const;

  /**
   * Number of rows in the clienttable.
   */
  virtual size_t numRows() const = 0;
  /**
   * Number of columns in the clienttable.
   */
  virtual size_t numColumns() const = 0;
  /**
   * The clienttable schema.
   */
  virtual std::shared_ptr<Schema> schema() const = 0;

  /**
   * Creates an 'ostream adaptor' to use when printing the clienttable. Example usage:
   * std::cout << myTable.stream(true, false).
   */
  internal::TableStreamAdaptor stream(bool wantHeaders, bool wantRowNumbers) const;

  /**
   * Creates an 'ostream adaptor' to use when printing the clienttable. Example usage:
   * std::cout << myTable.stream(true, false, rowSeq).
   */
  internal::TableStreamAdaptor stream(bool wantHeaders, bool wantRowNumbers,
      std::shared_ptr<RowSequence> rowSequence) const;

  /**
   * Creates an 'ostream adaptor' to use when printing the clienttable. Example usage:
   * std::cout << myTable.stream(true, false, rowSequences).
   */
  internal::TableStreamAdaptor stream(bool wantHeaders, bool wantRowNumbers,
      std::vector<std::shared_ptr<RowSequence>> rowSequences) const;

  /**
   * For debugging and demos.
   */
   std::string toString(bool wantHeaders, bool wantRowNumbers) const;

  /**
   * For debugging and demos.
   */
  std::string toString(bool wantHeaders, bool wantRowNumbers,
      std::shared_ptr<RowSequence> rowSequence) const;

  /**
   * For debugging and demos.
   */
  std::string toString(bool wantHeaders, bool wantRowNumbers,
      std::vector<std::shared_ptr<RowSequence>> rowSequences) const;
};
}  // namespace deephaven::dhcore::clienttable
