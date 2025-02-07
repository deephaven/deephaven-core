/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/arrowutil/arrow_column_source.h"
#include "deephaven/client/utility/arrow_util.h"

using deephaven::client::utility::OkOrThrow;

namespace deephaven::client::arrowutil {
namespace internal {

namespace {
struct NanoScaleFactorVisitor final : public arrow::TypeVisitor {
  size_t result_ = 1;

  arrow::Status Visit(const arrow::Int8Type  &/*type*/) final {
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int16Type &/*type*/) final {
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int32Type &/*type*/) final {
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int64Type &/*type*/) final {
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::FloatType &/*type*/) final {
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType &/*type*/) final {
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanType &/*type*/) final {
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::UInt16Type &/*type*/) final {
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringType &/*type*/) final {
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::TimestampType &type) final {
    result_ = ScaleFromUnit(type.unit());
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Date64Type &/*type*/) final {
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Time64Type &type) final {
    result_ = ScaleFromUnit(type.unit());
    return arrow::Status::OK();
  }

  static size_t ScaleFromUnit(arrow::TimeUnit::type unit) {
    switch (unit) {
      case arrow::TimeUnit::SECOND: return 1'000'000'000;
      case arrow::TimeUnit::MILLI: return 1'000'000;
      case arrow::TimeUnit::MICRO: return 1'000;
      case arrow::TimeUnit::NANO: return 1;
      default: {
        auto message = fmt::format("Unhandled arrow::TimeUnit {}", static_cast<size_t>(unit));
        throw std::runtime_error(DEEPHAVEN_LOCATION_STR(message));
      }
    }
  }
};
}  // namespace

size_t CalcTimeNanoScaleFactor(const arrow::Array &array) {
  NanoScaleFactorVisitor visitor;
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(array.type()->Accept(&visitor)));
  return visitor.result_;
}
}  // namespace internal
}  // namespace deephaven::client::arrowutil
