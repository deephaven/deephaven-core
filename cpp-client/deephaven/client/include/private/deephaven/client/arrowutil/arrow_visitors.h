/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <arrow/type.h>
#include "deephaven/client/types.h"

namespace deephaven::client::arrowutil {

template<typename Inner>
class ArrowTypeVisitor final : public arrow::TypeVisitor {
public:
  ArrowTypeVisitor() = default;
  explicit ArrowTypeVisitor(Inner inner) : inner_(std::move(inner)) {}

  arrow::Status Visit(const arrow::Int8Type &) final {
    inner_.template operator()<int8_t>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int16Type &) final {
    inner_.template operator()<int16_t>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int32Type &) final {
    inner_.template operator()<int32_t>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int64Type &) final {
    inner_.template operator()<int64_t>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::FloatType &) final {
    inner_.template operator()<float>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType &) final {
    inner_.template operator()<double>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanType &) final {
    inner_.template operator()<bool>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringType &) final {
    inner_.template operator()<std::string>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::TimestampType &) final {
    inner_.template operator()<deephaven::client::DateTime>();
    return arrow::Status::OK();
  }

  Inner &inner() { return inner_; }
  const Inner &inner() const { return inner_; }

private:
  Inner inner_;
};

template<typename Inner>
class ArrowArrayTypeVisitor : public arrow::ArrayVisitor {
public:
  ArrowArrayTypeVisitor() = default;
  explicit ArrowArrayTypeVisitor(Inner inner) : inner_(std::move(inner)) {}

  arrow::Status Visit(const arrow::Int8Array &) final {
    inner_.template operator()<int8_t>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int16Array &) final {
    inner_.template operator()<int16_t>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int32Array &) final {
    inner_.template operator()<int32_t>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int64Array &) final {
    inner_.template operator()<int64_t>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::FloatArray &) final {
    inner_.template operator()<float>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleArray &) final {
    inner_.template operator()<double>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringArray &) final {
    inner_.template operator()<std::string>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanArray &) final {
    inner_.template operator()<bool>();
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::TimestampArray &) final {
    inner_.template operator()<deephaven::client::DateTime>();
    return arrow::Status::OK();
  }

  Inner &inner() { return inner_; }
  const Inner &inner() const { return inner_; }

private:
  Inner inner_;
};
}  // namespace deephaven::client::arrowutil
