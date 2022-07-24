/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/ticking.h"

namespace deephaven::client {
ClassicTickingUpdate::ClassicTickingUpdate(std::shared_ptr<RowSequence> removedRowsKeySpace,
    UInt64Chunk removedRowsIndexSpace,
    std::shared_ptr<RowSequence> addedRowsKeySpace,
    UInt64Chunk addedRowsIndexSpace,
    std::vector<std::shared_ptr<RowSequence>> modifiedRowsKeySpace,
    std::vector<UInt64Chunk> modifiedRowsIndexSpace,
    std::shared_ptr<Table> currentTableKeySpace,
    std::shared_ptr<Table> currentTableIndexSpace) :
    removedRowsKeySpace_(std::move(removedRowsKeySpace)),
    removedRowsIndexSpace_(std::move(removedRowsIndexSpace)),
    addedRowsKeySpace_(std::move(addedRowsKeySpace)),
    addedRowsIndexSpace_(std::move(addedRowsIndexSpace)),
    modifiedRowsKeySpace_(std::move(modifiedRowsKeySpace)),
    modifiedRowsIndexSpace_(std::move(modifiedRowsIndexSpace)),
    currentTableKeySpace_(std::move(currentTableKeySpace)),
    currentTableIndexSpace_(std::move(currentTableIndexSpace)) {}
ClassicTickingUpdate::ClassicTickingUpdate(ClassicTickingUpdate &&other) noexcept = default;
ClassicTickingUpdate &ClassicTickingUpdate::operator=(ClassicTickingUpdate &&other) noexcept = default;
ClassicTickingUpdate::~ClassicTickingUpdate() = default;

ImmerTickingUpdate::ImmerTickingUpdate(std::shared_ptr<Table> beforeRemoves,
    std::shared_ptr<Table> beforeModifies,
    std::shared_ptr<Table> current,
    std::shared_ptr<RowSequence> removed,
    std::vector<std::shared_ptr<RowSequence>> modified,
    std::shared_ptr<RowSequence> added) : beforeRemoves_(std::move(beforeRemoves)),
    beforeModifies_(std::move(beforeModifies)),
    current_(std::move(current)),
    removed_(std::move(removed)),
    modified_(std::move(modified)),
    added_(std::move(added)) {}

ImmerTickingUpdate::ImmerTickingUpdate(ImmerTickingUpdate &&other) noexcept = default;
ImmerTickingUpdate &ImmerTickingUpdate::operator=(ImmerTickingUpdate &&other) noexcept = default;
ImmerTickingUpdate::~ImmerTickingUpdate() = default;

}  // namespace deephaven::client
