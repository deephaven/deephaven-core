/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/ticking.h"
#include "deephaven/client/container/row_sequence.h"

namespace deephaven::client {
using deephaven::client::container::RowSequence;
using deephaven::client::container::RowSequenceBuilder;

TickingCallback::~TickingCallback() = default;

TickingUpdate::TickingUpdate() = default;
TickingUpdate::TickingUpdate(std::shared_ptr<Table> prev, std::shared_ptr<RowSequence> removedRows,
    std::shared_ptr<Table> afterRemoves, std::shared_ptr<RowSequence> addedRows,
    std::shared_ptr<Table> afterAdds, std::vector<std::shared_ptr<RowSequence>> modifiedRows,
    std::shared_ptr<Table> afterModifies) : prev_(std::move(prev)),
    removedRows_(std::move(removedRows)), afterRemoves_(std::move(afterRemoves)),
    addedRows_(std::move(addedRows)), afterAdds_(std::move(afterAdds)),
    modifiedRows_(std::move(modifiedRows)), afterModifies_(std::move(afterModifies)),
    onDemandState_(std::make_shared<internal::OnDemandState>()) {}
TickingUpdate::TickingUpdate(const TickingUpdate &other) = default;
TickingUpdate &TickingUpdate::operator=(const TickingUpdate &other) = default;
TickingUpdate::TickingUpdate(TickingUpdate &&other) noexcept = default;
TickingUpdate &TickingUpdate::operator=(TickingUpdate &&other) noexcept = default;
TickingUpdate::~TickingUpdate() = default;

namespace internal {
OnDemandState::OnDemandState() = default;
OnDemandState::~OnDemandState() = default;

const std::shared_ptr<RowSequence> &OnDemandState::allModifiedRows(
    const std::vector<std::shared_ptr<RowSequence>> &modifiedRows) {
  std::unique_lock guard(mutex_);
  if (allModifiedRows_ != nullptr) {
    return allModifiedRows_;
  }

  RowSequenceBuilder builder;
  auto cb = [&builder](uint64_t begin, uint64_t end) {
    builder.addInterval(begin, end);
  };
  for (const auto &rs : modifiedRows) {
    rs->forEachInterval(cb);
  }
  allModifiedRows_ = builder.build();
  return allModifiedRows_;
}
}  // namespace internal
}  // namespace deephaven::client
