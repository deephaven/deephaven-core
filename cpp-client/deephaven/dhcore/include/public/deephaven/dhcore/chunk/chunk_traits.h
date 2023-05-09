/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdint>
#include "deephaven/dhcore/chunk/chunk.h"

namespace deephaven::dhcore::chunk {
template<typename T>
struct TypeToChunk {};

template<>
struct TypeToChunk<char16_t> {
  typedef deephaven::dhcore::chunk::CharChunk type_t;
};

template<>
struct TypeToChunk<int8_t> {
  typedef deephaven::dhcore::chunk::Int8Chunk type_t;
};

template<>
struct TypeToChunk<int16_t> {
  typedef deephaven::dhcore::chunk::Int16Chunk type_t;
};

template<>
struct TypeToChunk<int32_t> {
  typedef deephaven::dhcore::chunk::Int32Chunk type_t;
};

template<>
struct TypeToChunk<int64_t> {
  typedef deephaven::dhcore::chunk::Int64Chunk type_t;
};

template<>
struct TypeToChunk<uint64_t> {
  typedef deephaven::dhcore::chunk::UInt64Chunk type_t;
};

template<>
struct TypeToChunk<float> {
  typedef deephaven::dhcore::chunk::FloatChunk type_t;
};

template<>
struct TypeToChunk<double> {
  typedef deephaven::dhcore::chunk::DoubleChunk type_t;
};

template<>
struct TypeToChunk<bool> {
  typedef deephaven::dhcore::chunk::BooleanChunk type_t;
};

template<>
struct TypeToChunk<std::string> {
  typedef deephaven::dhcore::chunk::StringChunk type_t;
};

template<>
struct TypeToChunk<deephaven::dhcore::DateTime> {
  typedef deephaven::dhcore::chunk::DateTimeChunk type_t;
};
}  // namespace deephaven::client::chunk
