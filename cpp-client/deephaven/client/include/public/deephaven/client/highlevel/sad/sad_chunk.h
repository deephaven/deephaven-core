/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>

namespace deephaven::client::highlevel::sad {
class SadChunkVisitor;
class SadChunk {
public:
  explicit SadChunk(size_t capacity) : capacity_(capacity) {}
  virtual ~SadChunk();

  size_t capacity() const { return capacity_; }

  virtual void acceptVisitor(const SadChunkVisitor &v) const = 0;

protected:
  size_t capacity_ = 0;
};

template<typename T>
class SadNumericChunk : public SadChunk {
protected:
  explicit SadNumericChunk(size_t capacity);

public:
  T *data() { return data_.get(); }
  const T *data() const { return data_.get(); }

protected:
  std::unique_ptr<T[]> data_;
};

class SadIntChunk;
class SadLongChunk;
class SadDoubleChunk;
class SadSizeTChunk;

class SadChunkVisitor {
public:

  virtual void visit(const SadIntChunk &) const = 0;
  virtual void visit(const SadLongChunk &) const = 0;
  virtual void visit(const SadDoubleChunk &) const = 0;
  virtual void visit(const SadSizeTChunk &) const = 0;
};

class SadIntChunk final : public SadNumericChunk<int32_t> {
    struct Private {};
public:
    static std::shared_ptr<SadIntChunk> create(size_t capacity);

    SadIntChunk(Private, size_t capacity) : SadNumericChunk<int32_t>(capacity) {}

    void acceptVisitor(const SadChunkVisitor &v) const final {
        v.visit(*this);
    }
};

class SadLongChunk final : public SadNumericChunk<int64_t> {
  struct Private {};
public:
  static std::shared_ptr<SadLongChunk> create(size_t capacity);

  SadLongChunk(Private, size_t capacity) : SadNumericChunk<int64_t>(capacity) {}

  void acceptVisitor(const SadChunkVisitor &v) const final {
    v.visit(*this);
  }
};

class SadDoubleChunk final : public SadNumericChunk<double> {
  struct Private {};
public:
  static std::shared_ptr<SadDoubleChunk> create(size_t capacity);

  SadDoubleChunk(Private, size_t capacity) : SadNumericChunk<double>(capacity) {}

  void acceptVisitor(const SadChunkVisitor &v) const final {
    v.visit(*this);
  }
};

class SadSizeTChunk final : public SadNumericChunk<size_t> {
  struct Private {};
public:
  static std::shared_ptr<SadSizeTChunk> create(size_t capacity);

  SadSizeTChunk(Private, size_t capacity) : SadNumericChunk<size_t>(capacity) {}

  void acceptVisitor(const SadChunkVisitor &v) const final {
    v.visit(*this);
  }
};

template<typename T>
SadNumericChunk<T>::SadNumericChunk(size_t capacity) : SadChunk(capacity),
    data_(std::make_unique<T[]>(capacity)) {
}
}  // namespace deephaven::client::highlevel::sad
