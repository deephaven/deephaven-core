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

class Schema {
public:
  explicit Schema(std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> columns);
  Schema(Schema &&other) noexcept;
  Schema &operator=(Schema &&other) noexcept;
  ~Schema();

  const std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> &columns() const {
    return columns_;
  }

private:
  std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> columns_;
};

class Table {
protected:
  typedef deephaven::client::column::ColumnSource ColumnSource;
  typedef deephaven::client::container::RowSequence RowSequence;

public:
  Table() = default;
  virtual ~Table() = default;

  virtual std::shared_ptr<RowSequence> getRowSequence() const = 0;
  virtual std::shared_ptr<ColumnSource> getColumn(size_t columnIndex) const = 0;

  std::shared_ptr<ColumnSource> getColumn(std::string_view name, bool strict) const;

  virtual size_t numRows() const = 0;
  virtual size_t numColumns() const = 0;

  virtual const Schema &schema() const = 0;

  internal::TableStreamAdaptor stream(bool wantHeaders, bool wantRowNumbers) const;

  internal::TableStreamAdaptor stream(bool wantHeaders, bool wantRowNumbers,
      std::shared_ptr<RowSequence> rowSequence) const;

  internal::TableStreamAdaptor stream(bool wantHeaders, bool wantRowNumbers,
      std::vector<std::shared_ptr<RowSequence>> rowSequences) const;
};
}  // namespace deephaven::client::table
