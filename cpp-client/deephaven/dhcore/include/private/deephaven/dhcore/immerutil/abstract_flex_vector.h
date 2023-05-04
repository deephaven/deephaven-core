/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
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
  typedef deephaven::dhcore::chunk::AnyChunk AnyChunk;
  typedef deephaven::dhcore::column::ColumnSource ColumnSource;

public:
  template<typename COLUMN_SOURCE, typename T>
  static void append(const COLUMN_SOURCE &src, size_t begin, size_t end,
      immer::flex_vector<T> *destData, immer::flex_vector<bool> *optionalDestNulls) {
    auto chunkData = appendHelper(src, begin, end, optionalDestNulls);

    typedef typename deephaven::dhcore::chunk::TypeToChunk<T>::type_t chunkType_t;
    auto *typedChunkData = deephaven::dhcore::utility::verboseCast<const chunkType_t*>(
        DEEPHAVEN_EXPR_MSG(&chunkData.unwrap()));
    auto transientData = destData->transient();
    for (auto v : *typedChunkData) {
      transientData.push_back(v);
    }
    *destData = transientData.persistent();
  }

private:
  static AnyChunk appendHelper(const ColumnSource &src, size_t begin, size_t end,
      immer::flex_vector<bool> *optionalDestNulls);
};
}  // namespace internal

/**
 * This class allows us to manipulate an immer::flex_vector without needing to know what type
 * it's instantiated on.
 */
class AbstractFlexVectorBase {
protected:
  typedef deephaven::dhcore::column::ColumnSource ColumnSource;
  typedef deephaven::dhcore::chunk::Chunk Chunk;
public:
  virtual ~AbstractFlexVectorBase();

  virtual std::unique_ptr<AbstractFlexVectorBase> take(size_t n) = 0;
  virtual void inPlaceDrop(size_t n) = 0;
  virtual void inPlaceAppend(std::unique_ptr<AbstractFlexVectorBase> other) = 0;
  virtual void inPlaceAppendSource(const ColumnSource &source, size_t begin, size_t end) = 0;

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
    auto *otherVec = deephaven::dhcore::utility::verboseCast<NumericAbstractFlexVector*>(
        DEEPHAVEN_EXPR_MSG(other.get()));
    auto temp = std::move(vec_) + std::move(otherVec->vec_);
    vec_ = std::move(temp);
  }

  void inPlaceAppendSource(const ColumnSource &source, size_t begin, size_t end) final {
    internal::FlexVectorAppender::append(source, begin, end, &vec_, nullptr);
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
    auto *otherVec = deephaven::dhcore::utility::verboseCast<GenericAbstractFlexVector*>(
        DEEPHAVEN_EXPR_MSG(other.get()));
    auto tempData = std::move(data_) + std::move(otherVec->data_);
    data_ = std::move(tempData);

    auto tempNulls = std::move(nulls_) + std::move(otherVec->nulls_);
    nulls_ = std::move(tempNulls);
  }

  void inPlaceAppendSource(  const ColumnSource &source, size_t begin, size_t end) final {
    internal::FlexVectorAppender::append(source, begin, end, &data_, &nulls_);
  }

  std::shared_ptr<ColumnSource> makeColumnSource() const final {
    return deephaven::client::immerutil::GenericImmerColumnSource<T>::create(data_, nulls_);
  }

private:
  immer::flex_vector<T> data_;
  immer::flex_vector<bool> nulls_;
};
}  // namespace deephaven::client::immerutil
