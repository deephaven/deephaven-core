/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once
#include <immer/algorithm.hpp>
#include <immer/flex_vector.hpp>
#include "deephaven/client/chunk/chunk.h"
#include "deephaven/client/column/column_source.h"
#include "deephaven/client/utility/utility.h"

namespace deephaven::client::immerutil {
namespace internal {
struct ColumnSourceImpls {
  typedef deephaven::client::chunk::Chunk Chunk;
  typedef deephaven::client::chunk::UInt64Chunk UInt64Chunk;
  typedef deephaven::client::container::RowSequence RowSequence;

  template<typename T>
  static void fillChunk(const immer::flex_vector<T> &src, const RowSequence &rows, Chunk *dest) {
    using deephaven::client::chunk::TypeToChunk;
    using deephaven::client::utility::assertLessEq;
    using deephaven::client::utility::verboseCast;
    typedef typename TypeToChunk<T>::type_t chunkType_t;
    auto *typedDest = verboseCast<chunkType_t*>(dest, DEEPHAVEN_PRETTY_FUNCTION);

    assertLessEq(rows.size(), typedDest->size(), DEEPHAVEN_PRETTY_FUNCTION, "rows.size()",
        "typedDest->size()");
    auto *destp = typedDest->data();

    auto copyDataInner = [&destp](const T *dataBegin, const T *dataEnd) {
      for (const T *current = dataBegin; current != dataEnd; ++current) {
        *destp++ = *current;
      }
    };

    auto copyDataOuter = [&src, &copyDataInner](uint64_t srcBegin, uint64_t srcEnd) {
      auto srcBeginp = src.begin() + srcBegin;
      auto srcEndp = src.begin() + srcEnd;
      immer::for_each_chunk(srcBeginp, srcEndp, copyDataInner);
    };
    rows.forEachChunk(copyDataOuter);
  }

  template<typename T>
  static void fillChunkUnordered(const immer::flex_vector<T> &src, const UInt64Chunk &rowKeys,
      Chunk *dest) {
    using deephaven::client::chunk::TypeToChunk;
    using deephaven::client::utility::assertLessEq;
    using deephaven::client::utility::verboseCast;

    typedef typename TypeToChunk<T>::type_t chunkType_t;

    auto *typedDest = verboseCast<chunkType_t*>(dest, DEEPHAVEN_PRETTY_FUNCTION);
    assertLessEq(rowKeys.size(), typedDest->size(), DEEPHAVEN_PRETTY_FUNCTION, "rowKeys.size()",
        "typedDest->size()");
    auto *destp = typedDest->data();

    // Note: Uses random access with Immer, which is rather more expensive than iterating
    // over contiguous Immer ranges.
    for (auto key : rowKeys) {
      *destp++ = src[key];
    }
  }
};
}  // namespace internal

class ImmerColumnSource : public virtual deephaven::client::column::ColumnSource {
public:
  template<typename T>
  static std::shared_ptr<ImmerColumnSource> create(immer::flex_vector<T> vec);

  static std::shared_ptr<ImmerColumnSource> create(immer::flex_vector<std::string> vec);
};

template<typename T>
class ImmerNumericColumnSource final : public ImmerColumnSource,
    public deephaven::client::column::NumericColumnSource<T>,
    std::enable_shared_from_this<ImmerNumericColumnSource<T>> {

  typedef deephaven::client::chunk::Chunk Chunk;
  typedef deephaven::client::chunk::UInt64Chunk UInt64Chunk;
  typedef deephaven::client::column::ColumnSourceVisitor ColumnSourceVisitor;
  typedef deephaven::client::container::RowSequence RowSequence;

public:
  explicit ImmerNumericColumnSource(immer::flex_vector<T> data) : data_(std::move(data)) {}
  ~ImmerNumericColumnSource() final = default;

  void fillChunk(const RowSequence &rows, Chunk *dest) const final {
    internal::ColumnSourceImpls::fillChunk(data_, rows, dest);
  }

  void fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *dest) const final {
    internal::ColumnSourceImpls::fillChunkUnordered(data_, rowKeys, dest);
  }

  void acceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->visit(*this);
  }

  std::any backdoor() const final {
    return &data_;
  }

private:
  immer::flex_vector<T> data_;
};

class ImmerStringColumnSource final : public ImmerColumnSource,
    public deephaven::client::column::StringColumnSource,
    std::enable_shared_from_this<ImmerStringColumnSource> {
  typedef deephaven::client::column::ColumnSourceVisitor ColumnSourceVisitor;
public:
  explicit ImmerStringColumnSource(immer::flex_vector<std::string> data) : data_(std::move(data)) {}
  ~ImmerStringColumnSource() final = default;

  void fillChunk(const RowSequence &rows, Chunk *dest) const final {
    internal::ColumnSourceImpls::fillChunk(data_, rows, dest);
  }

  void fillChunkUnordered(const UInt64Chunk &rowKeys, Chunk *dest) const final {
    internal::ColumnSourceImpls::fillChunkUnordered(data_, rowKeys, dest);
  }

  void acceptVisitor(ColumnSourceVisitor *visitor) const final {
    visitor->visit(*this);
  }

private:
  immer::flex_vector<std::string> data_;
};

template<typename T>
std::shared_ptr<ImmerColumnSource> ImmerColumnSource::create(immer::flex_vector<T> vec) {
  return std::make_shared<ImmerNumericColumnSource<T>>(std::move(vec));
}

inline std::shared_ptr<ImmerColumnSource> ImmerColumnSource::create(
    immer::flex_vector<std::string> vec) {
  return std::make_shared<ImmerStringColumnSource>(std::move(vec));
}
}  // namespace deephaven::client::immerutil
