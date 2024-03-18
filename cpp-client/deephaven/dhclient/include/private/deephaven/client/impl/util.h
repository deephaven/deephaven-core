/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <string_view>
#include <vector>
#include <google/protobuf/repeated_ptr_field.h>

namespace deephaven::client::impl {
template<typename T>
void MoveVectorData(std::vector<T> src, google::protobuf::RepeatedPtrField<T> *dest) {
  for (auto &s: src) {
    dest->Add(std::move(s));
  }
}
}  // namespace deephaven::client::impl
