/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/ticking/ticking.h"

#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace deephaven::dhcore::ticking {
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::container::RowSequenceBuilder;

TickingCallback::~TickingCallback() = default;

TickingUpdate::TickingUpdate() = default;
TickingUpdate::TickingUpdate(std::shared_ptr<ClientTable> prev,
    std::shared_ptr<RowSequence> removed_rows,
    std::shared_ptr<ClientTable> after_removes, std::shared_ptr<RowSequence> added_rows,
    std::shared_ptr<ClientTable> after_adds, std::vector<std::shared_ptr<RowSequence>> modified_rows,
    std::shared_ptr<ClientTable> after_modifies) : prev_(std::move(prev)),
    removedRows_(std::move(removed_rows)), afterRemoves_(std::move(after_removes)),
    addedRows_(std::move(added_rows)), afterAdds_(std::move(after_adds)),
    modifiedRows_(std::move(modified_rows)), afterModifies_(std::move(after_modifies)),
    onDemandState_(std::make_shared<internal::OnDemandState>()) {}
TickingUpdate::TickingUpdate(const TickingUpdate &other) = default;
TickingUpdate &TickingUpdate::operator=(const TickingUpdate &other) = default;
TickingUpdate::TickingUpdate(TickingUpdate &&other) noexcept = default;
TickingUpdate &TickingUpdate::operator=(TickingUpdate &&other) noexcept = default;
TickingUpdate::~TickingUpdate() = default;

namespace internal {
OnDemandState::OnDemandState() = default;
OnDemandState::~OnDemandState() = default;

const std::shared_ptr<RowSequence> &OnDemandState::AllModifiedRows(
    const std::vector<std::shared_ptr<RowSequence>> &modified_rows) {
  std::unique_lock guard(mutex_);
  if (allModifiedRows_ != nullptr) {
    return allModifiedRows_;
  }

  RowSequenceBuilder builder;
  auto cb = [&builder](uint64_t begin, uint64_t end) {
    builder.AddInterval(begin, end);
  };
  for (const auto &rs : modified_rows) {
    rs->ForEachInterval(cb);
  }
  allModifiedRows_ = builder.Build();
  return allModifiedRows_;
}
}  // namespace internal
}  // namespace deephaven::dhcore::ticking

