#pragma once
#include <memory>
#include <vector>
#include "deephaven/client/highlevel/sad/sad_column_source.h"
#include "deephaven/client/highlevel/sad/sad_row_sequence.h"
#include "deephaven/client/highlevel/sad/sad_table.h"
#include "deephaven/client/highlevel/sad/sad_unwrapped_table.h"

namespace deephaven::client::highlevel::sad {
class SadTickingTable final : public SadTable {
  struct Private {};

public:
  static std::shared_ptr<SadTickingTable> create(std::vector<std::shared_ptr<SadColumnSource>> columns);

  explicit SadTickingTable(Private, std::vector<std::shared_ptr<SadColumnSource>> columns);
  ~SadTickingTable() final;

  /**
   * Adds the rows (which are assumed not to exist) to the table's index in the table's (source)
   * coordinate space and returns the target aka redirected row indices in the redirected
   * coordinate space. It is the caller's responsibility
   * to actually move the new column data to the right place in the column sources.
   */
  std::shared_ptr<SadUnwrappedTable> add(const SadRowSequence &addedRows);
  /**
   * Erases the rows (which are provided in the source coordinate space).
   */
  void erase(const SadRowSequence &removedRows);
  void shift(const SadRowSequence &startIndex, const SadRowSequence &endIndex,
      const SadRowSequence &destIndex);

  std::shared_ptr<SadRowSequence> getRowSequence() const final;

  std::shared_ptr<SadUnwrappedTable> unwrap(const std::shared_ptr<SadRowSequence> &rows,
      const std::vector<size_t> &cols) const final;

  std::shared_ptr<SadColumnSource> getColumn(size_t columnIndex) const final;

  size_t numRows() const final {
    return redirection_->size();
  }

  size_t numColumns() const final {
    return columns_.size();
  }

private:
  std::vector<std::shared_ptr<SadColumnSource>> columns_;
  std::shared_ptr<std::map<int64_t, int64_t>> redirection_;
  /**
   * These are slots (in the target, aka the redirected space) that we once allocated but
   * then subsequently removed, so they're available for reuse.
   */
  std::vector<int64_t> slotsToReuse_;
};
}  // namespace deephaven::client::highlevel::sad

