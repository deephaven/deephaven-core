/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdint>
#include "deephaven/dhcore/chunk/chunk.h"

namespace deephaven::dhcore::container {
/**
 * Forward declaration
 */
class ContainerBase;
}  // namespace deephaven::dhcore::container

namespace deephaven::dhcore::chunk {
template<typename T>
struct TypeToChunk {};

template<>
struct TypeToChunk<char16_t> {
  using type_t = deephaven::dhcore::chunk::CharChunk;
};

template<>
struct TypeToChunk<int8_t> {
  using type_t = deephaven::dhcore::chunk::Int8Chunk;
};

template<>
struct TypeToChunk<int16_t> {
  using type_t = deephaven::dhcore::chunk::Int16Chunk;
};

template<>
struct TypeToChunk<int32_t> {
  using type_t = deephaven::dhcore::chunk::Int32Chunk;
};

template<>
struct TypeToChunk<int64_t> {
  using type_t = deephaven::dhcore::chunk::Int64Chunk;
};

template<>
struct TypeToChunk<uint64_t> {
  using type_t = deephaven::dhcore::chunk::UInt64Chunk;
};

template<>
struct TypeToChunk<float> {
  using type_t = deephaven::dhcore::chunk::FloatChunk;
};

template<>
struct TypeToChunk<double> {
  using type_t = deephaven::dhcore::chunk::DoubleChunk;
};

template<>
struct TypeToChunk<bool> {
  using type_t = deephaven::dhcore::chunk::BooleanChunk;
};

template<>
struct TypeToChunk<std::string> {
  using type_t = deephaven::dhcore::chunk::StringChunk;
};

template<>
struct TypeToChunk<deephaven::dhcore::DateTime> {
  using type_t = deephaven::dhcore::chunk::DateTimeChunk;
};

template<>
struct TypeToChunk<deephaven::dhcore::LocalDate> {
  using type_t = deephaven::dhcore::chunk::LocalDateChunk;
};

template<>
struct TypeToChunk<deephaven::dhcore::LocalTime> {
  using type_t = deephaven::dhcore::chunk::LocalTimeChunk;
};

template<>
struct TypeToChunk<std::shared_ptr<deephaven::dhcore::container::ContainerBase>> {
  using type_t = deephaven::dhcore::chunk::ContainerBaseChunk;
};
}  // namespace deephaven::client::chunk
