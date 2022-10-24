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

class ChunkVisitor;

/**
 * Abstract base class for Deephaven chunks. A Chunk represents a simple typed data buffer.
 * It is used as an argument to methods like ColumnSource::fillChunk() and
 * MutableColumnSource::fillFromChunk.
 */
class Chunk {
protected:
  Chunk() = default;
  explicit Chunk(size_t size) : size_(size) {}
  Chunk(Chunk &&other) noexcept = default;
  Chunk &operator=(Chunk &&other) noexcept = default;
  virtual ~Chunk() = default;

public:
  /**
   * Implementation of the visitor pattern.
   */
  virtual void acceptVisitor(ChunkVisitor *visitor) = 0;

  /**
   * The size of the Chunk.
   */
  size_t size() const { return size_; }

protected:
  void checkSize(size_t proposedSize, std::string_view what) const;

  size_t size_ = 0;
};

/**
 * Concrete implementing class for Deephaven chunks.
 */
template<typename T>
class GenericChunk final : public Chunk {
public:
  /**
   * Factory method. Create a Chunk having the specified size.
   */
  static GenericChunk<T> create(size_t size);

  /**
   * Constructor.
   */
  GenericChunk() = default;
  /**
   * Move constructor.
   */
  GenericChunk(GenericChunk &&other) noexcept = default;
  /**
   * Move assignment operator.
   */
  GenericChunk &operator=(GenericChunk &&other) noexcept = default;
  /**
   * Destructor.
   */
  ~GenericChunk() final = default;

  /**
   * Create a new GenericChunk that is a prefix of the current GenericChunk. The new object shares
   * the same underlying buffer, but has new size 'size'. 'size' is required to be less than or
   * equal to the size of this object.
   */
  GenericChunk take(size_t size) const;
  /**
   * Create a new GenericChunk that is a suffix of the current GenericChunk. The new object shares
   * the same underlying buffer, but has new size (this->size() - size).
   * 'size' is required to be less than or equal to the size of this object.
   */
  GenericChunk drop(size_t size) const;

  void acceptVisitor(ChunkVisitor *visitor) final;

  /**
   * Returns a pointer to the start of the data represented by this GenericChunk.
   */
  T *data() { return data_.get(); }
  /**
   * Returns a pointer to the start of the data represented by this GenericChunk.
   */
  const T *data() const { return data_.get(); }

  /**
   * Returns a pointer to the start of the data represented by this GenericChunk.
   */
  T *begin() { return data_.get(); }
  /**
   * Returns a pointer to the start of the data represented by this GenericChunk.
   */
  const T *begin() const { return data_.get(); }

  /**
   * Returns a pointer to the (exlusive) end of the data represented by this GenericChunk.
   */
  T *end() { return data_.get() + size_; }
  /**
   * Returns a pointer to the (exlusive) end of the data represented by this GenericChunk.
   */
  const T *end() const { return data_.get() + size_; }

  /**
   * Indexing operator.
   */
  T& operator[](size_t index) {
    return data_.get()[index];
  }

  /**
   * Indexing operator.
   */
  const T& operator[](size_t index) const {
    return data_.get()[index];
  }

private:
  GenericChunk(std::shared_ptr<T[]> data, size_t size);

  friend std::ostream &operator<<(std::ostream &s, const GenericChunk &o) {
    using deephaven::client::utility::separatedList;
    return s << '[' << separatedList(o.begin(), o.end()) << ']';
  }

  std::shared_ptr<T[]> data_;
};

/**
 * Convenience typedef.
 */
typedef GenericChunk<int8_t> Int8Chunk;
/**
 * Convenience typedef.
 */
typedef GenericChunk<uint8_t> UInt8Chunk;
/**
 * Convenience typedef.
 */
typedef GenericChunk<int16_t> Int16Chunk;
/**
 * Convenience typedef.
 */
typedef GenericChunk<uint16_t> UInt16Chunk;
/**
 * Convenience typedef.
 */
typedef GenericChunk<int32_t> Int32Chunk;
/**
 * Convenience typedef.
 */
typedef GenericChunk<uint32_t> UInt32Chunk;
/**
 * Convenience typedef.
 */
typedef GenericChunk<int64_t> Int64Chunk;
/**
 * Convenience typedef.
 */
typedef GenericChunk<uint64_t> UInt64Chunk;
/**
 * Convenience typedef.
 */
typedef GenericChunk<float> FloatChunk;
/**
 * Convenience typedef.
 */
typedef GenericChunk<double> DoubleChunk;
/**
 * Convenience typedef.
 */
typedef GenericChunk<bool> BooleanChunk;
/**
 * Convenience typedef.
 */
typedef GenericChunk<std::string> StringChunk;
/**
 * Convenience typedef.
 */
typedef GenericChunk<deephaven::client::DateTime> DateTimeChunk;

/**
 * Abstract base class that implements the visitor pattern for Chunk.
 */
class ChunkVisitor {
public:
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const Int8Chunk &) const = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const Int16Chunk &) const = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const Int32Chunk &) const = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const Int64Chunk &) const = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const UInt64Chunk &) const = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const FloatChunk &) const = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const DoubleChunk &) const = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const BooleanChunk &) const = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const StringChunk &) const = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void visit(const DateTimeChunk &) const = 0;
  /**
   * Implements the visitor pattern.
   */
};

template<typename T>
void GenericChunk<T>::acceptVisitor(ChunkVisitor *visitor) {
  visitor->visit(*this);
}

/**
 * Typesafe union of all the Chunk types. This is used when you need to have a method that creates
 * a Chunk (with a type determined at runtime) or for example you need to have a vector of such
 * Chunk objects.
 */
class AnyChunk {
  typedef std::variant<Int8Chunk, Int16Chunk, Int32Chunk, Int64Chunk, UInt64Chunk, FloatChunk,
    DoubleChunk, BooleanChunk, StringChunk, DateTimeChunk> variant_t;

public:
  /**
   * Move assignment operator.
   */
  template<typename T>
  AnyChunk &operator=(T &&chunk) {
    variant_ = std::forward<T>(chunk);
    return *this;
  }

  /**
   * Implementation of the visitor pattern.
   */
  template<typename Visitor>
  void visit(Visitor &&visitor) const {
    std::visit(std::forward<Visitor>(visitor), variant_);
  }

  /**
   * Gets the abstract base type of the contained Chunk.
   */
  const Chunk &unwrap() const;
  /**
   * Gets the abstract base type of the contained Chunk.
   */
  Chunk &unwrap();

  /**
   * Gets the contained Chunk as specified by T, or throws an exception if the contained Chunk
   * is not of type T.
   */
  template<typename T>
  const T &get() const {
    return std::get<T>(variant_);
  }

  /**
   * Gets the contained Chunk as specified by T, or throws an exception if the contained Chunk
   * is not of type T.
   */
  template<typename T>
  T &get() {
    return std::get<T>(variant_);
  }

private:
  variant_t variant_;
};

template<typename T>
GenericChunk<T> GenericChunk<T>::create(size_t size) {
  // Note: wanted to use make_shared, but std::make_shared<T[]>(size) doesn't do what I want
  // until C++20.
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
  // Make a shared_ptr which manages ownership of the same underlying buffer as data_, but which
  // actually points to the interior of that buffer, namely data_.get() + size.
  std::shared_ptr<T[]> newBegin(data_, data_.get() + size);
  auto newSize = size_ - size;
  return GenericChunk<T>(std::move(newBegin), newSize);
}
}  // namespace deephaven::client::chunk
