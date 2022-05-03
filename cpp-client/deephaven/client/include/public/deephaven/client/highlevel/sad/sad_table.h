#pragma once

#include <map>
#include <memory>
#include <vector>
#include "deephaven/client/highlevel/sad/sad_column_source.h"
#include "deephaven/client/highlevel/sad/sad_row_sequence.h"
#include "deephaven/client/highlevel/sad/sad_unwrapped_table.h"

namespace deephaven::client::highlevel::sad {
class SadTable {
public:
  SadTable();
  virtual ~SadTable();

  virtual std::shared_ptr<SadRowSequence> getRowSequence() const = 0;
  virtual std::shared_ptr<SadColumnSource> getColumn(size_t columnIndex) const = 0;

  virtual std::shared_ptr<SadUnwrappedTable> unwrap(const std::shared_ptr<SadRowSequence> &rows,
      const std::vector<size_t> &cols) const = 0;

  virtual size_t numRows() const = 0;
  virtual size_t numColumns() const = 0;
};

}  // namespace deephaven::client::highlevel::sad
