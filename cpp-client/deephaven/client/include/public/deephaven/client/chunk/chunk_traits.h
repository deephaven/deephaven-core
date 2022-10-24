/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdint>
#include "deephaven/client/chunk/chunk.h"

namespace deephaven::client::chunk {
template<typename T>
struct TypeToChunk {};

template<>
struct TypeToChunk<int8_t> {
  typedef deephaven::client::chunk::Int8Chunk type_t;
};

template<>
struct TypeToChunk<int16_t> {
  typedef deephaven::client::chunk::Int16Chunk type_t;
};

template<>
struct TypeToChunk<int32_t> {
  typedef deephaven::client::chunk::Int32Chunk type_t;
};

template<>
struct TypeToChunk<int64_t> {
  typedef deephaven::client::chunk::Int64Chunk type_t;
};

template<>
struct TypeToChunk<uint64_t> {
  typedef deephaven::client::chunk::UInt64Chunk type_t;
};

template<>
struct TypeToChunk<float> {
  typedef deephaven::client::chunk::FloatChunk type_t;
};

template<>
struct TypeToChunk<double> {
  typedef deephaven::client::chunk::DoubleChunk type_t;
};

template<>
struct TypeToChunk<bool> {
  typedef deephaven::client::chunk::BooleanChunk type_t;
};

template<>
struct TypeToChunk<std::string> {
  typedef deephaven::client::chunk::StringChunk type_t;
};

template<>
struct TypeToChunk<deephaven::client::DateTime> {
  typedef deephaven::client::chunk::DateTimeChunk type_t;
};
}  // namespace deephaven::client::chunk
