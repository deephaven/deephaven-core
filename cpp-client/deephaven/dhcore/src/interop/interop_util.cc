/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/interop/interop_util.h"
#include "deephaven/dhcore/utility/utility.h"

#include <vector>

using deephaven::dhcore::interop::NativePtr;
using deephaven::dhcore::interop::StringPool;
using deephaven::dhcore::utility::MakeReservedVector;

namespace deephaven::dhcore::interop {
int32_t StringPool::ExportAndDestroy(StringPool *self,
    uint8_t *bytes, int32_t bytes_length,
    int32_t *ends, int32_t ends_length) {
  // StringPoolBuilder::Build is allowed to return null if there are no strings.
  if (self == nullptr) {
    if (bytes_length != 0 || ends_length != 0) {
      // This arbitrary return code indicates that something has gone wrong.
      return 1;
    }
    return 0;
  }
  if (bytes_length != static_cast<int32_t>(self->bytes_.size()) ||
      ends_length != static_cast<int32_t>(self->ends_.size())) {
    // This arbitrary return code indicates that something has gone wrong.
    return 2;
  }
  std::copy(self->bytes_.begin(), self->bytes_.end(), bytes);
  std::copy(self->ends_.begin(), self->ends_.end(), ends);
  delete self;
  return 0;
}

StringPool::StringPool(std::vector<uint8_t> bytes, std::vector<int32_t> ends) :
   bytes_(std::move(bytes)), ends_(std::move(ends)) {}
StringPool::~StringPool() = default;

StringPoolBuilder::StringPoolBuilder() = default;
StringPoolBuilder::~StringPoolBuilder() = default;

StringHandle StringPoolBuilder::Add(std::string_view sv) {
  StringHandle result(static_cast<int32_t>(ends_.size()));
  bytes_.insert(bytes_.end(), sv.begin(), sv.end());
  ends_.push_back(static_cast<int32_t>(bytes_.size()));
  return result;
}

StringPoolHandle StringPoolBuilder::Build() {
  auto num_bytes = bytes_.size();
  auto num_strings = ends_.size();
  if (num_strings == 0) {
    return StringPoolHandle(nullptr, 0, 0);
  }
  auto *sp = new StringPool(std::move(bytes_), std::move(ends_));
  return StringPoolHandle(sp, static_cast<int32_t>(num_bytes),
      static_cast<int32_t>(num_strings));
}
}  // namespace deephaven::dhcore::interop

extern "C" {
int32_t deephaven_dhcore_interop_StringPool_ExportAndDestroy(
    NativePtr<StringPool> string_pool,
    uint8_t *bytes, int32_t bytes_length,
    int32_t *ends, int32_t ends_length) {
  return StringPool::ExportAndDestroy(string_pool.Get(), bytes, bytes_length, ends, ends_length);
}
}  // extern "C"
