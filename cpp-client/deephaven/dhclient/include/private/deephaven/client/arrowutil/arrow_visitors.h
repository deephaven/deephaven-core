/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <arrow/type.h>
#include <arrow/visitor.h>
#include <string>
#include "deephaven/dhcore/types.h"

namespace deephaven::client::arrowutil {

template<typename InnerType>
class ArrowTypeVisitor final : public arrow::TypeVisitor {
public:
  ArrowTypeVisitor() = default;
  explicit ArrowTypeVisitor(InnerType inner) : inner_(std::move(inner)) {}
  ~ArrowTypeVisitor() final = default;

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
    inner_.template operator()<deephaven::dhcore::DateTime>();
    return arrow::Status::OK();
  }

  InnerType &Inner() { return inner_; }
  const InnerType &Inner() const { return inner_; }

private:
  InnerType inner_;
};

template<typename InnerType>
class ArrowArrayTypeVisitor final : public arrow::ArrayVisitor {
public:
  ArrowArrayTypeVisitor() = default;
  explicit ArrowArrayTypeVisitor(InnerType inner) : inner_(std::move(inner)) {}
  ~ArrowArrayTypeVisitor() final = default;

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
    inner_.template operator()<deephaven::dhcore::DateTime>();
    return arrow::Status::OK();
  }

  InnerType &Inner() { return inner_; }
  const InnerType &Inner() const { return inner_; }

private:
  InnerType inner_;
};
}  // namespace deephaven::client::arrowutil
