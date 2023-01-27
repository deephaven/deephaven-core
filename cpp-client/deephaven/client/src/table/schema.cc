/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/table/schema.h"

namespace deephaven::client::table {
Schema::Schema(std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>> columns) :
    columns_(std::move(columns)) {}
Schema::Schema(Schema &&other) noexcept = default;
Schema &Schema::operator=(Schema &&other) noexcept = default;
Schema::~Schema() = default;
}  // namespace deephaven::client::table
