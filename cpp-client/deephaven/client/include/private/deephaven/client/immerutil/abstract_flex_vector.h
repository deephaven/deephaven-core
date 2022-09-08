/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <immer/flex_vector.hpp>
#include <immer/flex_vector_transient.hpp>
#include <arrow/array.h>
#include "deephaven/client/arrowutil/arrow_value_converter.h"
#include "deephaven/client/column/column_source.h"
#include "deephaven/client/immerutil/immer_column_source.h"
#include "deephaven/client/utility/utility.h"

namespace deephaven::client::immerutil {
namespace internal {
template<typename T>
struct CorrespondingArrowArrayType {};

template<>
struct CorrespondingArrowArrayType<int8_t> {
  typedef arrow::Int8Array type_t;
};

template<>
struct CorrespondingArrowArrayType<int16_t> {
  typedef arrow::Int16Array type_t;
};

template<>
struct CorrespondingArrowArrayType<int32_t> {
  typedef arrow::Int32Array type_t;
};

template<>
struct CorrespondingArrowArrayType<int64_t> {
  typedef arrow::Int64Array type_t;
};

template<>
struct CorrespondingArrowArrayType<float> {
  typedef arrow::FloatArray type_t;
};

template<>
struct CorrespondingArrowArrayType<double> {
  typedef arrow::DoubleArray type_t;
};

template<>
struct CorrespondingArrowArrayType<bool> {
  typedef arrow::BooleanArray type_t;
};

template<>
struct CorrespondingArrowArrayType<std::string> {
  typedef arrow::StringArray type_t;
};

template<>
struct CorrespondingArrowArrayType<deephaven::client::DateTime> {
  typedef arrow::TimestampArray type_t;
};

struct FlexVectorAppender {
  template<typename ARROW_SRC, typename T>
  static void append(const ARROW_SRC &src, immer::flex_vector<T> *destData,
      immer::flex_vector<bool> *optionalDestNulls) {

    auto transientData = destData->transient();
    immer::flex_vector_transient<bool> transientNulls;
    if (optionalDestNulls != nullptr) {
      transientNulls = optionalDestNulls->transient();
    }
    for (auto optValue : src) {
      bool isNull = !optValue.has_value();
      T value = T();
      if (isNull) {
        if constexpr(deephaven::client::arrowutil::isNumericType<T>()) {
          value = deephaven::client::DeephavenConstantsForType<T>::NULL_VALUE;
        }
      } else {
        deephaven::client::arrowutil::ArrowValueConverter::convert(*optValue, &value);
      }
      transientData.push_back(std::move(value));
      if (optionalDestNulls != nullptr) {
        transientNulls.push_back(isNull);
      }
    }
    *destData = transientData.persistent();
    if (optionalDestNulls != nullptr) {
      *optionalDestNulls = transientNulls.persistent();
    }
  }
};
}  // namespace internal

/**
 * This class allows us to manipulate an immer::flex_vector without needing to know what type
 * it's instantiated on.
 */
class AbstractFlexVectorBase {
protected:
  typedef deephaven::client::column::ColumnSource ColumnSource;
public:
  virtual ~AbstractFlexVectorBase();

  virtual std::unique_ptr<AbstractFlexVectorBase> take(size_t n) = 0;
  virtual void inPlaceDrop(size_t n) = 0;
  virtual void inPlaceAppend(std::unique_ptr<AbstractFlexVectorBase> other) = 0;
  virtual void inPlaceAppendArrow(const arrow::Array &data) = 0;

  virtual std::shared_ptr<ColumnSource> makeColumnSource() const = 0;
};

template<typename T>
class NumericAbstractFlexVector final : public AbstractFlexVectorBase {
public:
  NumericAbstractFlexVector() = default;

  explicit NumericAbstractFlexVector(immer::flex_vector<T> vec) : vec_(std::move(vec)) {}

  ~NumericAbstractFlexVector() final = default;

  std::unique_ptr<AbstractFlexVectorBase> take(size_t n) final {
    return std::make_unique<NumericAbstractFlexVector>(vec_.take(n));
  }

  void inPlaceDrop(size_t n) final {
    auto temp = std::move(vec_).drop(n);
    vec_ = std::move(temp);
  }

  void inPlaceAppend(std::unique_ptr<AbstractFlexVectorBase> other) final {
    auto *otherVec = deephaven::client::utility::verboseCast<NumericAbstractFlexVector*>(
        DEEPHAVEN_EXPR_MSG(other.get()));
    auto temp = std::move(vec_) + std::move(otherVec->vec_);
    vec_ = std::move(temp);
  }

  void inPlaceAppendArrow(const arrow::Array &data) final {
    typedef typename internal::CorrespondingArrowArrayType<T>::type_t arrowArrayType_t;
    auto *typedArrow = deephaven::client::utility::verboseCast<const arrowArrayType_t*>(
        DEEPHAVEN_EXPR_MSG(&data));
    internal::FlexVectorAppender::append(*typedArrow, &vec_, nullptr);
  }

  std::shared_ptr<ColumnSource> makeColumnSource() const final {
    return deephaven::client::immerutil::NumericImmerColumnSource<T>::create(vec_);
  }

private:
  immer::flex_vector<T> vec_;
};

template<typename T>
class GenericAbstractFlexVector final : public AbstractFlexVectorBase {
public:
  GenericAbstractFlexVector() = default;

  GenericAbstractFlexVector(immer::flex_vector<T> data, immer::flex_vector<bool> nulls) :
      data_(std::move(data)), nulls_(std::move(nulls)) {}

  ~GenericAbstractFlexVector() final = default;

  std::unique_ptr<AbstractFlexVectorBase> take(size_t n) final {
    return std::make_unique<GenericAbstractFlexVector>(data_.take(n), nulls_.take(n));
  }

  void inPlaceDrop(size_t n) final {
    auto tempData = std::move(data_).drop(n);
    data_ = std::move(tempData);

    auto tempNulls = std::move(nulls_).drop(n);
    nulls_ = std::move(tempNulls);
  }

  void inPlaceAppend(std::unique_ptr<AbstractFlexVectorBase> other) final {
    auto *otherVec = deephaven::client::utility::verboseCast<GenericAbstractFlexVector*>(
        DEEPHAVEN_EXPR_MSG(other.get()));
    auto tempData = std::move(data_) + std::move(otherVec->data_);
    data_ = std::move(tempData);

    auto tempNulls = std::move(nulls_) + std::move(otherVec->nulls_);
    nulls_ = std::move(tempNulls);
  }

  void inPlaceAppendArrow(const arrow::Array &data) final {
    typedef typename internal::CorrespondingArrowArrayType<T>::type_t arrowArrayType_t;
    auto *typedArrow = deephaven::client::utility::verboseCast<const arrowArrayType_t*>(
        DEEPHAVEN_EXPR_MSG(&data));
    internal::FlexVectorAppender::append(*typedArrow, &data_, &nulls_);
  }

  std::shared_ptr<ColumnSource> makeColumnSource() const final {
    return deephaven::client::immerutil::GenericImmerColumnSource<T>::create(data_, nulls_);
  }

private:
  immer::flex_vector<T> data_;
  immer::flex_vector<bool> nulls_;
};
}  // namespace deephaven::client::immerutil
