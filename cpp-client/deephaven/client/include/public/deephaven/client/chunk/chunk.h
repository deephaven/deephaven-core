/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <iostream>
#include <memory>
#include <string_view>
#include <variant>
#include "deephaven/client/types.h"
#include "deephaven/client/utility/utility.h"

namespace deephaven::client::chunk {
/**
 * Base type for Chunks
 */
class Chunk {
protected:
  Chunk() = default;
  explicit Chunk(size_t size) : size_(size) {}
  Chunk(Chunk &&other) noexcept = default;
  Chunk &operator=(Chunk &&other) noexcept = default;
  virtual ~Chunk() = default;

public:
  size_t size() const { return size_; }

protected:
  void checkSize(size_t proposedSize, std::string_view what) const;

  size_t size_ = 0;
};

template<typename T>
class GenericChunk final : public Chunk {
public:
  static GenericChunk<T> create(size_t size);

  GenericChunk() = default;
  GenericChunk(GenericChunk &&other) noexcept = default;
  GenericChunk &operator=(GenericChunk &&other) noexcept = default;
  ~GenericChunk() final = default;

  GenericChunk take(size_t size) const;
  GenericChunk drop(size_t size) const;

  T *data() { return data_.get(); }
  const T *data() const { return data_.get(); }

  T *begin() { return data_.get(); }
  const T *begin() const { return data_.get(); }

  T *end() { return data_.get() + size_; }
  const T *end() const { return data_.get() + size_; }

private:
  GenericChunk(std::shared_ptr<T[]> data, size_t size);

  friend std::ostream &operator<<(std::ostream &s, const GenericChunk &o) {
    using deephaven::client::utility::separatedList;
    return s << '[' << separatedList(o.begin(), o.end()) << ']';
  }

  std::shared_ptr<T[]> data_;
};

typedef GenericChunk<bool> BooleanChunk;
typedef GenericChunk<int8_t> Int8Chunk;
typedef GenericChunk<int16_t> Int16Chunk;
typedef GenericChunk<int32_t> Int32Chunk;
typedef GenericChunk<int64_t> Int64Chunk;
typedef GenericChunk<uint64_t> UInt64Chunk;
typedef GenericChunk<float> FloatChunk;
typedef GenericChunk<double> DoubleChunk;
typedef GenericChunk<std::string> StringChunk;
typedef GenericChunk<deephaven::client::DateTime> DateTimeChunk;

/**
 * Typesafe union of all the Chunk types.
 */
class AnyChunk {
  typedef std::variant<Int8Chunk, Int16Chunk, Int32Chunk, Int64Chunk, UInt64Chunk, FloatChunk,
    DoubleChunk, BooleanChunk, StringChunk, DateTimeChunk> variant_t;

public:
  template<typename T>
  AnyChunk &operator=(T &&chunk) {
    variant_ = std::forward<T>(chunk);
    return *this;
  }

  template<typename Visitor>
  void visit(Visitor &&visitor) const {
    std::visit(std::forward<Visitor>(visitor), variant_);
  }

  const Chunk &unwrap() const;
  Chunk &unwrap();

  template<typename T>
  const T &get() const {
    return std::get<T>(variant_);
  }

  template<typename T>
  T &get() {
    return std::get<T>(variant_);
  }

private:
  variant_t variant_;
};

template<typename T>
GenericChunk<T> GenericChunk<T>::create(size_t size) {
  // Note: wanted to use make_shared, but std::make_shared<T[]>(size) doesn't DTRT until C++20
  auto data = std::shared_ptr<T[]>(new T[size]);
  return GenericChunk<T>(std::move(data), size);
}

template<typename T>
GenericChunk<T>::GenericChunk(std::shared_ptr<T[]> data, size_t size) : Chunk(size),
  data_(std::move(data)) {}

template<typename T>
GenericChunk<T> GenericChunk<T>::take(size_t size) const {
  checkSize(size, DEEPHAVEN_PRETTY_FUNCTION);
  // Share ownership of data_.
  return GenericChunk<T>(data_, size);
}

template<typename T>
GenericChunk<T> GenericChunk<T>::drop(size_t size) const {
  checkSize(size, DEEPHAVEN_PRETTY_FUNCTION);
  // Share ownership of data_, but the value of the pointer yielded by std::shared_ptr<T>::get()
  // is actually data_.get() + size.
  std::shared_ptr<T[]> newBegin(data_, data_.get() + size);
  auto newSize = size_ - size;
  return GenericChunk<T>(std::move(newBegin), newSize);
}

class ChunkVisitor {
public:
  virtual void visit(const Int8Chunk &) const = 0;
  virtual void visit(const Int16Chunk &) const = 0;
  virtual void visit(const Int32Chunk &) const = 0;
  virtual void visit(const Int64Chunk &) const = 0;
  virtual void visit(const UInt64Chunk &) const = 0;
  virtual void visit(const FloatChunk &) const = 0;
  virtual void visit(const DoubleChunk &) const = 0;
  virtual void visit(const BooleanChunk &) const = 0;
  virtual void visit(const StringChunk &) const = 0;
  virtual void visit(const DateTimeChunk &) const = 0;
};

template<typename T>
struct TypeToChunk {};

template<>
struct TypeToChunk<int8_t> {
  typedef deephaven::client::chunk::Int8Chunk type_t;
};

template<>
struct TypeToChunk<int16_t> {
  typedef deephaven::client::chunk::Int16Chunk type_t;
};

template<>
struct TypeToChunk<int32_t> {
  typedef deephaven::client::chunk::Int32Chunk type_t;
};

template<>
struct TypeToChunk<int64_t> {
  typedef deephaven::client::chunk::Int64Chunk type_t;
};

template<>
struct TypeToChunk<uint64_t> {
  typedef deephaven::client::chunk::UInt64Chunk type_t;
};

template<>
struct TypeToChunk<float> {
  typedef deephaven::client::chunk::FloatChunk type_t;
};

template<>
struct TypeToChunk<double> {
  typedef deephaven::client::chunk::DoubleChunk type_t;
};

template<>
struct TypeToChunk<bool> {
  typedef deephaven::client::chunk::BooleanChunk type_t;
};

template<>
struct TypeToChunk<std::string> {
  typedef deephaven::client::chunk::StringChunk type_t;
};

template<>
struct TypeToChunk<deephaven::client::DateTime> {
  typedef deephaven::client::chunk::DateTimeChunk type_t;
};
}  // namespace deephaven::client::chunk
