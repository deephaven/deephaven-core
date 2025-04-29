/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/chunk/chunk.h"

#include <cstddef>
#include <string_view>
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/core.h"

using deephaven::dhcore::chunk::Chunk;

namespace deephaven::dhcore::chunk {
void Chunk::CheckSize(size_t proposed_size, std::string_view what) const {
  if (proposed_size > size_) {
    auto message = fmt::format("{}: new size > size ({} > {})", what, proposed_size, size_);
    throw std::runtime_error(message);
  }
}

namespace {
struct MyVisitor {
  template<typename T>
  Chunk &operator()(T &arg) {
    return arg;
  }
};

struct MyConstVisitor {
  template<typename T>
  const Chunk &operator()(const T &arg) const {
    return arg;
  }
};
}  // namespace

Chunk &AnyChunk::Unwrap() {
  MyVisitor v;
  return std::visit(v, variant_);
}

const Chunk &AnyChunk::Unwrap() const {
  MyConstVisitor v;
  return std::visit(v, variant_);
}
}  // namespace deephaven::client::chunk
