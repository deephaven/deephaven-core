#include "deephaven/client/highlevel/sad/sad_unwrapped_table.h"

#include <utility>
#include "deephaven/client/utility/utility.h"

namespace deephaven::client::highlevel::sad {
std::shared_ptr<SadUnwrappedTable> SadUnwrappedTable::create(std::shared_ptr<SadLongChunk> rowKeys,
    size_t numRows, std::vector<std::shared_ptr<SadColumnSource>> columns) {
  return std::make_shared<SadUnwrappedTable>(Private(), std::move(rowKeys), numRows, std::move(columns));
}

SadUnwrappedTable::SadUnwrappedTable(Private, std::shared_ptr<SadLongChunk> &&rowKeys, size_t numRows,
    std::vector<std::shared_ptr<SadColumnSource>> &&columns) : rowKeys_(std::move(rowKeys)),
    numRows_(numRows), columns_(std::move(columns)) {}

SadUnwrappedTable::~SadUnwrappedTable() = default;

std::shared_ptr<SadLongChunk> SadUnwrappedTable::getUnorderedRowKeys() const {
  return rowKeys_;
}

std::shared_ptr<SadColumnSource> SadUnwrappedTable::getColumn(size_t columnIndex) const {
  return columns_[columnIndex];
}
}  // namespace deephaven::client::highlevel::sad
