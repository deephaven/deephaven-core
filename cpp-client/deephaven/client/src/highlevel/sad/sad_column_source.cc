/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/highlevel/sad/sad_column_source.h"
#include "deephaven/client/highlevel/impl/util.h"
#include "deephaven/client/highlevel/sad/sad_chunk.h"

#include "deephaven/client/utility/utility.h"

using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;
using deephaven::client::highlevel::impl::verboseCast;

namespace deephaven::client::highlevel::sad {
namespace {
class MySadLongColumnSourceContext final : public SadColumnSourceContext {
public:
};

class MySadDoubleColumnSourceContext final : public SadColumnSourceContext {
public:
};

void assertFits(size_t size, size_t capacity);
void assertInRange(size_t index, size_t size);
}  // namespace
SadColumnSource::~SadColumnSource() = default;
SadMutableColumnSource::~SadMutableColumnSource() = default;
SadIntColumnSource::~SadIntColumnSource() = default;
SadLongColumnSource::~SadLongColumnSource() = default;
SadDoubleColumnSource::~SadDoubleColumnSource() = default;

std::shared_ptr<SadIntArrayColumnSource> SadIntArrayColumnSource::create() {
  return std::make_shared<SadIntArrayColumnSource>(Private());
}

SadIntArrayColumnSource::SadIntArrayColumnSource(Private) {}
SadIntArrayColumnSource::~SadIntArrayColumnSource() = default;

std::shared_ptr<SadColumnSourceContext> SadIntArrayColumnSource::createContext(size_t chunkSize) const {
  // We're not really using contexts yet.
  return std::make_shared<MySadLongColumnSourceContext>();
}

void SadIntArrayColumnSource::fillChunkUnordered(SadContext *context, const SadLongChunk &rowKeys,
    size_t size, SadChunk *dest) const {
  auto *typedDest = verboseCast<SadIntChunk*>(DEEPHAVEN_PRETTY_FUNCTION, dest);
  assertFits(size, dest->capacity());

  for (size_t i = 0; i < size; ++i) {
    auto srcIndex = rowKeys.data()[i];
    assertInRange(srcIndex, data_.size());
    typedDest->data()[i] = this->data_[srcIndex];
  }
}

void SadIntArrayColumnSource::fillChunk(SadContext *context, const SadRowSequence &rows, SadChunk *dest) const {
  auto *typedDest = verboseCast<SadIntChunk*>(DEEPHAVEN_PRETTY_FUNCTION, dest);
  assertFits(rows.size(), dest->capacity());

  size_t destIndex = 0;
  int64_t srcIndex;
  auto iter = rows.getRowSequenceIterator();
  while (iter->tryGetNext(&srcIndex)) {
    assertInRange(srcIndex, data_.size());
    typedDest->data()[destIndex] = data_[srcIndex];
    ++destIndex;
  }
}

void SadIntArrayColumnSource::fillFromChunk(SadContext *context, const SadChunk &src,
    const SadRowSequence &rows) {
  auto *typedSrc = verboseCast<const SadIntChunk*>(DEEPHAVEN_PRETTY_FUNCTION, &src);
  assertFits(rows.size(), src.capacity());

  size_t srcIndex = 0;
  int64_t destIndex;
  auto iter = rows.getRowSequenceIterator();
  while (iter->tryGetNext(&destIndex)) {
    ensureSize(destIndex + 1);
    data_[destIndex] = typedSrc->data()[srcIndex];
    ++srcIndex;
  }
}

void SadIntArrayColumnSource::fillFromChunkUnordered(SadContext *context, const SadChunk &src,
    const SadLongChunk &rowKeys, size_t size) {
  auto *typedSrc = verboseCast<const SadIntChunk*>(DEEPHAVEN_PRETTY_FUNCTION, &src);
  assertFits(size, src.capacity());

  for (size_t i = 0; i < size; ++i) {
    auto destIndex = rowKeys.data()[i];
    ensureSize(destIndex + 1);
    data_[destIndex] = typedSrc->data()[i];
  }
}

void SadIntArrayColumnSource::ensureSize(size_t size) {
  if (size > data_.size()) {
    data_.resize(size);
  }
}

void SadIntArrayColumnSource::acceptVisitor(SadColumnSourceVisitor *visitor) const {
  visitor->visit(this);
}

std::shared_ptr<SadLongArrayColumnSource> SadLongArrayColumnSource::create() {
  return std::make_shared<SadLongArrayColumnSource>(Private());
}

SadLongArrayColumnSource::SadLongArrayColumnSource(Private) {}
SadLongArrayColumnSource::~SadLongArrayColumnSource() = default;

std::shared_ptr<SadColumnSourceContext> SadLongArrayColumnSource::createContext(size_t chunkSize) const {
  // We're not really using contexts yet.
  return std::make_shared<MySadLongColumnSourceContext>();
}

void SadLongArrayColumnSource::fillChunkUnordered(SadContext *context, const SadLongChunk &rowKeys,
    size_t size, SadChunk *dest) const {
  auto *typedDest = verboseCast<SadLongChunk*>(DEEPHAVEN_PRETTY_FUNCTION, dest);
  assertFits(size, dest->capacity());

  for (size_t i = 0; i < size; ++i) {
    auto srcIndex = rowKeys.data()[i];
    assertInRange(srcIndex, data_.size());
    typedDest->data()[i] = this->data_[srcIndex];
  }
}

void SadLongArrayColumnSource::fillChunk(SadContext *context, const SadRowSequence &rows, SadChunk *dest) const {
  auto *typedDest = verboseCast<SadLongChunk*>(DEEPHAVEN_PRETTY_FUNCTION, dest);
  assertFits(rows.size(), dest->capacity());

  size_t destIndex = 0;
  int64_t srcIndex;
  auto iter = rows.getRowSequenceIterator();
  while (iter->tryGetNext(&srcIndex)) {
    assertInRange(srcIndex, data_.size());
    typedDest->data()[destIndex] = data_[srcIndex];
    ++destIndex;
  }
}

void SadLongArrayColumnSource::fillFromChunk(SadContext *context, const SadChunk &src,
    const SadRowSequence &rows) {
  auto *typedSrc = verboseCast<const SadLongChunk*>(DEEPHAVEN_PRETTY_FUNCTION, &src);
  assertFits(rows.size(), src.capacity());

  size_t srcIndex = 0;
  int64_t destIndex;
  auto iter = rows.getRowSequenceIterator();
  while (iter->tryGetNext(&destIndex)) {
    ensureSize(destIndex + 1);
    data_[destIndex] = typedSrc->data()[srcIndex];
    ++srcIndex;
  }
}

void SadLongArrayColumnSource::fillFromChunkUnordered(SadContext *context, const SadChunk &src,
    const SadLongChunk &rowKeys, size_t size) {
  auto *typedSrc = verboseCast<const SadLongChunk*>(DEEPHAVEN_PRETTY_FUNCTION, &src);
  assertFits(size, src.capacity());

  for (size_t i = 0; i < size; ++i) {
    auto destIndex = rowKeys.data()[i];
    ensureSize(destIndex + 1);
    data_[destIndex] = typedSrc->data()[i];
  }
}

void SadLongArrayColumnSource::ensureSize(size_t size) {
  if (size > data_.size()) {
    data_.resize(size);
  }
}

void SadLongArrayColumnSource::acceptVisitor(SadColumnSourceVisitor *visitor) const {
  visitor->visit(this);
}

std::shared_ptr<SadDoubleArrayColumnSource> SadDoubleArrayColumnSource::create() {
  return std::make_shared<SadDoubleArrayColumnSource>(Private());
}

SadDoubleArrayColumnSource::SadDoubleArrayColumnSource(Private) {}
SadDoubleArrayColumnSource::~SadDoubleArrayColumnSource() = default;

std::shared_ptr<SadColumnSourceContext> SadDoubleArrayColumnSource::createContext(size_t chunkSize) const {
  // We're not really using contexts yet.
  return std::make_shared<MySadDoubleColumnSourceContext>();
}

void SadDoubleArrayColumnSource::fillChunkUnordered(SadContext *context, const SadLongChunk &rowKeys,
    size_t size, SadChunk *dest) const {
  auto *typedDest = verboseCast<SadDoubleChunk*>(DEEPHAVEN_PRETTY_FUNCTION, dest);
  assertFits(size, dest->capacity());

  for (size_t i = 0; i < size; ++i) {
    auto srcIndex = rowKeys.data()[i];
    assertInRange(srcIndex, data_.size());
    typedDest->data()[i] = this->data_[srcIndex];
  }
}

void SadDoubleArrayColumnSource::fillChunk(SadContext *context, const SadRowSequence &rows, SadChunk *dest) const {
  auto *typedDest = verboseCast<SadDoubleChunk*>(DEEPHAVEN_PRETTY_FUNCTION, dest);
  assertFits(rows.size(), dest->capacity());

  size_t destIndex = 0;
  int64_t srcIndex;
  auto iter = rows.getRowSequenceIterator();
  while (iter->tryGetNext(&srcIndex)) {
    assertInRange(srcIndex, data_.size());
    typedDest->data()[destIndex] = data_[srcIndex];
    ++destIndex;
  }
}

void SadDoubleArrayColumnSource::fillFromChunk(SadContext *context, const SadChunk &src,
    const SadRowSequence &rows) {
  auto *typedSrc = verboseCast<const SadDoubleChunk*>(DEEPHAVEN_PRETTY_FUNCTION, &src);
  assertFits(rows.size(), src.capacity());

  size_t srcIndex = 0;
  int64_t destIndex;
  auto iter = rows.getRowSequenceIterator();
  while (iter->tryGetNext(&destIndex)) {
    ensureSize(destIndex + 1);
    data_[destIndex] = typedSrc->data()[srcIndex];
    ++srcIndex;
  }
}

void SadDoubleArrayColumnSource::fillFromChunkUnordered(SadContext *context, const SadChunk &src,
    const SadLongChunk &rowKeys, size_t size) {
  auto *typedSrc = verboseCast<const SadDoubleChunk*>(DEEPHAVEN_PRETTY_FUNCTION, &src);
  assertFits(size, src.capacity());

  for (size_t i = 0; i < size; ++i) {
    auto destIndex = rowKeys.data()[i];
    ensureSize(destIndex + 1);
    data_[destIndex] = typedSrc->data()[i];
  }
}

void SadDoubleArrayColumnSource::ensureSize(size_t size) {
  if (size > data_.size()) {
    data_.resize(size);
  }
}

void SadDoubleArrayColumnSource::acceptVisitor(SadColumnSourceVisitor *visitor) const {
  visitor->visit(this);
}

SadColumnSourceContext::~SadColumnSourceContext() = default;

namespace {
void assertFits(size_t size, size_t capacity) {
  if (size > capacity) {
    auto message = stringf("Expected capacity at least %o, have %o", size, capacity);
    throw std::runtime_error(message);
  }
}

void assertInRange(size_t index, size_t size) {
  if (index >= size) {
    auto message = stringf("srcIndex %o >= size %o", index, size);
    throw std::runtime_error(message);
  }
}
}  // namespace
}  // namespace deephaven::client::highlevel::sad
