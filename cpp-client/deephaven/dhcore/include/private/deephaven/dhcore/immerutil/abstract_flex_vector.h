/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <immer/flex_vector.hpp>
#include <immer/flex_vector_transient.hpp>
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/chunk/chunk_traits.h"
#include "deephaven/dhcore/column/column_source.h"
#include "deephaven/dhcore/immerutil/immer_column_source.h"
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::dhcore::immerutil {
namespace internal {
class FlexVectorAppender {
  using AnyChunk = deephaven::dhcore::chunk::AnyChunk;
  using ColumnSource = deephaven::dhcore::column::ColumnSource;

public:
  template<typename ColumnSource, typename T>
  static void Append(const ColumnSource &src, size_t begin, size_t end,
      immer::flex_vector<T> *dest_data, immer::flex_vector<bool> *optional_dest_nulls) {
    auto chunk_data = AppendHelper(src, begin, end, optional_dest_nulls);

    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    auto *typed_chunk_data = deephaven::dhcore::utility::VerboseCast<const chunkType_t *>(
        DEEPHAVEN_LOCATION_EXPR(&chunk_data.Unwrap()));
    auto transient_data = dest_data->transient();
    for (auto v : *typed_chunk_data) {
      transient_data.push_back(v);
    }
    *dest_data = transient_data.persistent();
  }


private:
  static AnyChunk AppendHelper(const ColumnSource &src, size_t begin, size_t end,
      immer::flex_vector<bool> *optional_dest_nulls);
};
}  // namespace internal

/**
 * This class allows us to manipulate an immer::flex_vector without needing to know what type
 * it's instantiated on.
 */
class AbstractFlexVectorBase {
protected:
  using Chunk = deephaven::dhcore::chunk::Chunk;
  using ColumnSource = deephaven::dhcore::column::ColumnSource;
public:
  virtual ~AbstractFlexVectorBase();

  [[nodiscard]] virtual std::unique_ptr<AbstractFlexVectorBase> Take(size_t n) = 0;
  virtual void InPlaceDrop(size_t n) = 0;
  virtual void InPlaceAppend(std::unique_ptr<AbstractFlexVectorBase> other) = 0;
  virtual void InPlaceAppendSource(const ColumnSource &source, size_t begin, size_t end) = 0;

  [[nodiscard]] virtual std::shared_ptr<ColumnSource> MakeColumnSource() const = 0;
};

template<typename T>
class NumericAbstractFlexVector final : public AbstractFlexVectorBase {
public:
  NumericAbstractFlexVector() = default;

  explicit NumericAbstractFlexVector(immer::flex_vector<T> vec) : vec_(std::move(vec)) {}

  ~NumericAbstractFlexVector() final = default;

  [[nodiscard]]
  std::unique_ptr<AbstractFlexVectorBase> Take(size_t n) final {
    return std::make_unique<NumericAbstractFlexVector>(vec_.take(n));
  }

  void InPlaceDrop(size_t n) final {
    auto temp = std::move(vec_).drop(n);
    vec_ = std::move(temp);
  }

  void InPlaceAppend(std::unique_ptr<AbstractFlexVectorBase> other) final {
    auto *other_vec = deephaven::dhcore::utility::VerboseCast<NumericAbstractFlexVector *>(
        DEEPHAVEN_LOCATION_EXPR(other.get()));
    auto temp = std::move(vec_) + std::move(other_vec->vec_);
    vec_ = std::move(temp);
  }

  void InPlaceAppendSource(const ColumnSource &source, size_t begin, size_t end) final {
    internal::FlexVectorAppender::Append(source, begin, end, &vec_, nullptr);
  }

  [[nodiscard]]
  std::shared_ptr<ColumnSource> MakeColumnSource() const final {
    return NumericImmerColumnSource<T>::Create(vec_);
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

  [[nodiscard]]
  std::unique_ptr<AbstractFlexVectorBase> Take(size_t n) final {
    return std::make_unique<GenericAbstractFlexVector>(data_.take(n), nulls_.take(n));
  }

  void InPlaceDrop(size_t n) final {
    auto temp_data = std::move(data_).drop(n);
    data_ = std::move(temp_data);

    auto temp_nulls = std::move(nulls_).drop(n);
    nulls_ = std::move(temp_nulls);
  }

  void InPlaceAppend(std::unique_ptr<AbstractFlexVectorBase> other) final {
    auto *other_vec = deephaven::dhcore::utility::VerboseCast<GenericAbstractFlexVector *>(
        DEEPHAVEN_LOCATION_EXPR(other.get()));
    auto temp_data = std::move(data_) + std::move(other_vec->data_);
    data_ = std::move(temp_data);

    auto temp_nulls = std::move(nulls_) + std::move(other_vec->nulls_);
    nulls_ = std::move(temp_nulls);
  }

  void InPlaceAppendSource(const ColumnSource &source, size_t begin, size_t end) final {
    internal::FlexVectorAppender::Append(source, begin, end, &data_, &nulls_);
  }

  [[nodiscard]]
  std::shared_ptr<ColumnSource> MakeColumnSource() const final {
    return GenericImmerColumnSource<T>::Create(data_, nulls_);
  }

private:
  immer::flex_vector<T> data_;
  immer::flex_vector<bool> nulls_;
};
}  // namespace deephaven::dhcore::immerutil
