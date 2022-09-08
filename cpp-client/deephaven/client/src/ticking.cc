/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/ticking.h"

namespace deephaven::client {
TickingUpdate::TickingUpdate(std::shared_ptr<Table> prev, std::shared_ptr<RowSequence> removedRows,
    std::shared_ptr<Table> afterRemoves, std::shared_ptr<RowSequence> addedRows,
    std::shared_ptr<Table> afterAdds, std::vector<std::shared_ptr<RowSequence>> modifiedRows,
    std::shared_ptr<Table> afterModifies) : prev_(std::move(prev)),
    removedRows_(std::move(removedRows)), afterRemoves_(std::move(afterRemoves)),
    addedRows_(std::move(addedRows)), afterAdds_(std::move(afterAdds)),
    modifiedRows_(std::move(modifiedRows)), afterModifies_(std::move(afterModifies)) {}
TickingUpdate::TickingUpdate(TickingUpdate &&other) noexcept = default;
TickingUpdate &TickingUpdate::operator=(TickingUpdate &&other) noexcept = default;
TickingUpdate::~TickingUpdate() = default;
}  // namespace deephaven::client
