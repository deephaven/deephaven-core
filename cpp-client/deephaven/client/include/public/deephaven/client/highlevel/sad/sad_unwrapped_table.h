#pragma once

#include <map>
#include <memory>
#include <vector>
#include "deephaven/client/highlevel/sad/sad_column_source.h"
#include "deephaven/client/highlevel/sad/sad_row_sequence.h"

namespace deephaven::client::highlevel::sad {
class SadUnwrappedTable {
  struct Private {};
public:
  static std::shared_ptr<SadUnwrappedTable> create(std::shared_ptr<SadLongChunk> rowKeys,
      size_t numRows, std::vector<std::shared_ptr<SadColumnSource>> columns);

  SadUnwrappedTable(Private, std::shared_ptr<SadLongChunk> &&rowKeys,
      size_t numRows, std::vector<std::shared_ptr<SadColumnSource>> &&columns);
  ~SadUnwrappedTable();

  std::shared_ptr<SadLongChunk> getUnorderedRowKeys() const;
  std::shared_ptr<SadColumnSource> getColumn(size_t columnIndex) const;

  size_t numRows() const { return numRows_; }
  size_t numColumns() const { return columns_.size(); }

private:
  std::shared_ptr<SadLongChunk> rowKeys_;
  size_t numRows_ = 0;
  std::vector<std::shared_ptr<SadColumnSource>> columns_;
};
}  // namespace deephaven::client::highlevel::sad
