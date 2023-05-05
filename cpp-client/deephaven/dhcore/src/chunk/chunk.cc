/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::chunk::Chunk;
using deephaven::dhcore::utility::separatedList;
using deephaven::dhcore::utility::stringf;

namespace deephaven::dhcore::chunk {
void Chunk::checkSize(size_t proposedSize, std::string_view what) const {
  if (proposedSize > size_) {
    auto message = stringf("%o: new size > size (%o > %o)", what, proposedSize, size_);
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

Chunk &AnyChunk::unwrap() {
  MyVisitor v;
  return std::visit(v, variant_);
}

const Chunk &AnyChunk::unwrap() const {
  MyConstVisitor v;
  return std::visit(v, variant_);
}
}  // namespace deephaven::client::chunk
