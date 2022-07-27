/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/misc.h"

namespace deephaven::client::utility {
ColumnDefinitions::ColumnDefinitions(vec_t vec, map_t map) : vec_(std::move(vec)),
    map_(std::move(map)) {}
ColumnDefinitions::~ColumnDefinitions() = default;
}  // namespace deephaven::client::utility
