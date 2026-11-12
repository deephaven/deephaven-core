/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <vector>
#include "deephaven/third_party/fmt/core.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/clienttable/client_table.h"
#include "deephaven/dhcore/column/array_column_source.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/types.h"

// Helpers shared by the tests that drive the ticking machinery without a server.
namespace deephaven::client::tests {
inline std::shared_ptr<deephaven::dhcore::container::RowSequence> Range(uint64_t begin, uint64_t end) {
  return deephaven::dhcore::container::RowSequence::CreateSequential(begin, end);
}

inline std::shared_ptr<deephaven::dhcore::container::RowSequence> Keys(std::initializer_list<uint64_t> keys) {
  deephaven::dhcore::container::RowSequenceBuilder builder;
  for (auto key : keys) {
    builder.Add(key);
  }
  return builder.Build();
}

inline std::shared_ptr<deephaven::dhcore::container::RowSequence> None() {
  return deephaven::dhcore::container::RowSequence::CreateEmpty();
}

// Makes a long column source holding 'values', the shape in which record batch data reaches the
// ticking machinery.
inline std::shared_ptr<deephaven::dhcore::column::ColumnSource> LongColumn(const std::vector<int64_t> &values) {
  using deephaven::dhcore::ElementType;
  using deephaven::dhcore::ElementTypeId;
  auto n = values.size();
  auto elements = std::make_unique<int64_t[]>(n);
  auto nulls = std::make_unique<bool[]>(n);
  for (size_t i = 0; i != n; ++i) {
    elements[i] = values[i];
    nulls[i] = false;
  }
  return deephaven::dhcore::column::Int64ArrayColumnSource::CreateFromArrays(
      ElementType::Of(ElementTypeId::kInt64), std::move(elements), std::move(nulls), n);
}

inline std::vector<int64_t> Elements(const deephaven::dhcore::container::RowSequence &rows) {
  std::vector<int64_t> result;
  rows.ForEachInterval([&result](uint64_t begin, uint64_t end) {
    for (auto key = begin; key != end; ++key) {
      result.push_back(static_cast<int64_t>(key));
    }
  });
  return result;
}

inline std::vector<int64_t> Column(const deephaven::dhcore::clienttable::ClientTable &table,
    size_t column_index) {
  auto rows = table.GetRowSequence();
  auto chunk = deephaven::dhcore::chunk::Int64Chunk::Create(rows->Size());
  table.GetColumn(column_index)->FillChunk(*rows, &chunk, nullptr);
  return {chunk.data(), chunk.data() + chunk.Size()};
}

// Renders a RowSequence as its half-open intervals, e.g. "[[0,400), [500,501)". Empty renders as "[]".
inline std::string Render(const deephaven::dhcore::container::RowSequence &rows) {
  return fmt::format("{}", rows);
}

// Describes the first place two columns differ, or returns "" when they are identical.
inline std::string FirstDifference(const char *column, const std::vector<int64_t> &expected,
    const std::vector<int64_t> &actual) {
  auto n = std::min(expected.size(), actual.size());
  for (size_t i = 0; i != n; ++i) {
    if (expected[i] != actual[i]) {
      return fmt::format("{}: row {} is {}, expected {}", column, i, actual[i], expected[i]);
    }
  }
  if (expected.size() != actual.size()) {
    return fmt::format("{}: {} rows, expected {}", column, actual.size(), expected.size());
  }
  return "";
}
}  // namespace deephaven::client::tests
