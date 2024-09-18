/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <iostream>
#include <memory>
#include <string_view>
#include <variant>
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::dhcore::chunk {

class ChunkVisitor;

/**
 * Abstract base class for Deephaven chunks. A Chunk represents a simple typed data buffer.
 * It is used as an argument to methods like ColumnSource::FillChunk() and
 * MutableColumnSource::FillFromChunk.
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
  virtual void AcceptVisitor(ChunkVisitor *visitor) const = 0;

  /**
   * The size of the Chunk.
   */
  [[nodiscard]]
  size_t Size() const { return size_; }

protected:
  void CheckSize(size_t proposed_size, std::string_view what) const;

  size_t size_ = 0;
};

/**
 * Concrete implementing class for Deephaven chunks.
 */
template<typename T>
class GenericChunk final : public Chunk {
public:
  using value_type = T;

  /**
   * Factory method. Create a Chunk having the specified size, with a privately allocated buffer.
   */
  [[nodiscard]]
  static GenericChunk<T> Create(size_t size);

  /**
   * Factory method. Create a Chunk on a buffer owned by the caller. The buffer must outlive this
   * Chunk and all Chunks derived from this chunk (e.g. via take/drop).
   */
  [[nodiscard]]
  static GenericChunk<T> CreateView(T *data, size_t size);

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
  [[nodiscard]]
  GenericChunk Take(size_t size) const;
  /**
   * Create a new GenericChunk that is a suffix of the current GenericChunk. The new object shares
   * the same underlying buffer, but has new size (this->size() - size).
   * 'size' is required to be less than or equal to the size of this object.
   */
  [[nodiscard]]
  GenericChunk Drop(size_t size) const;

  void AcceptVisitor(ChunkVisitor *visitor) const final;

  /**
   * Returns a pointer to the start of the data represented by this GenericChunk.
   */
  [[nodiscard]]
  T *data() { return data_.get(); }
  /**
   * Returns a pointer to the start of the data represented by this GenericChunk.
   */
  [[nodiscard]]
  const T *data() const { return data_.get(); }

  /**
   * Returns a pointer to the start of the data represented by this GenericChunk.
   */
  [[nodiscard]]
  T *begin() { return data_.get(); }
  /**
   * Returns a pointer to the start of the data represented by this GenericChunk.
   */
  [[nodiscard]]
  const T *begin() const { return data_.get(); }

  /**
   * Returns a pointer to the (exlusive) end of the data represented by this GenericChunk.
   */
  T *end() { return data_.get() + size_; }
  /**
   * Returns a pointer to the (exlusive) end of the data represented by this GenericChunk.
   */
  [[nodiscard]]
  const T *end() const { return data_.get() + size_; }

  /**
   * Indexing operator.
   */
  [[nodiscard]]
  T& operator[](size_t index) {
    return data_.get()[index];
  }

  /**
   * Indexing operator.
   */
  [[nodiscard]]
  const T& operator[](size_t index) const {
    return data_.get()[index];
  }

private:
  GenericChunk(std::shared_ptr<T[]> data, size_t size);

  friend std::ostream &operator<<(std::ostream &s, const GenericChunk &o) {
    using deephaven::dhcore::utility::separatedList;
    return s << '[' << separatedList(o.begin(), o.end()) << ']';
  }

  std::shared_ptr<T[]> data_;
};

/**
 * Convenience using.
 */
using CharChunk = GenericChunk<char16_t>;
/**
 * Convenience using.
 */
using Int8Chunk = GenericChunk<int8_t>;
/**
 * Convenience using.
 */
using UInt8Chunk = GenericChunk<uint8_t>;
/**
 * Convenience using.
 */
using Int16Chunk = GenericChunk<int16_t>;
/**
 * Convenience using.
 */
using UInt16Chunk = GenericChunk<uint16_t>;
/**
 * Convenience using.
 */
using Int32Chunk = GenericChunk<int32_t>;
/**
 * Convenience using.
 */
using UInt32Chunk = GenericChunk<uint32_t>;
/**
 * Convenience using.
 */
using Int64Chunk = GenericChunk<int64_t>;
/**
 * Convenience using.
 */
using UInt64Chunk = GenericChunk<uint64_t>;
/**
 * Convenience using.
 */
using FloatChunk = GenericChunk<float>;
/**
 * Convenience using.
 */
using DoubleChunk = GenericChunk<double>;
/**
 * Convenience using.
 */
using BooleanChunk = GenericChunk<bool>;
/**
 * Convenience using.
 */
using StringChunk = GenericChunk<std::string>;
/**
 * Convenience using.
 */
using DateTimeChunk = GenericChunk<deephaven::dhcore::DateTime>;
/**
 * Convenience using.
 */
using LocalDateChunk = GenericChunk<deephaven::dhcore::LocalDate>;
/**
 * Convenience using.
 */
using LocalTimeChunk = GenericChunk<deephaven::dhcore::LocalTime>;


/**
 * Abstract base class that implements the visitor pattern for Chunk.
 */
class ChunkVisitor {
public:
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const CharChunk &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Int8Chunk &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Int16Chunk &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Int32Chunk &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Int64Chunk &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const UInt16Chunk &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const UInt64Chunk &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const FloatChunk &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const DoubleChunk &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const BooleanChunk &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const StringChunk &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const DateTimeChunk &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const LocalDateChunk &) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const LocalTimeChunk &) = 0;
};

template<typename T>
void GenericChunk<T>::AcceptVisitor(ChunkVisitor *visitor) const {
  visitor->Visit(*this);
}

/**
 * Typesafe union of all the Chunk types. This is used when you need to have a method that creates
 * a Chunk (with a type determined at runtime) or for example you need to have a vector of such
 * Chunk objects.
 */
class AnyChunk {
  using variant_t = std::variant<CharChunk, Int8Chunk, Int16Chunk, Int32Chunk, Int64Chunk,
     UInt64Chunk, FloatChunk, DoubleChunk, BooleanChunk, StringChunk, DateTimeChunk,
     LocalDateChunk, LocalTimeChunk>;

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
  void Visit(Visitor &&visitor) const {
    std::visit(std::forward<Visitor>(visitor), variant_);
  }

  /**
   * Gets the abstract base type of the contained Chunk.
   */
  [[nodiscard]]
  const Chunk &Unwrap() const;
  /**
   * Gets the abstract base type of the contained Chunk.
   */
  [[nodiscard]]
  Chunk &Unwrap();

  /**
   * Gets the contained Chunk as specified by T, or throws an exception if the contained Chunk
   * is not of type T.
   */
  template<typename T>
  [[nodiscard]]
  const T &Get() const {
    return std::get<T>(variant_);
  }

  /**
   * Gets the contained Chunk as specified by T, or throws an exception if the contained Chunk
   * is not of type T.
   */
  template<typename T>
  [[nodiscard]]
  T &Get() {
    return std::get<T>(variant_);
  }

private:
  variant_t variant_;
};

template<typename T>
GenericChunk<T> GenericChunk<T>::Create(size_t size) {
  // Note: wanted to use make_shared, but std::make_shared<T[]>(size) doesn't do what I want
  // until C++20.
  auto data = std::shared_ptr<T[]>(new T[size]);
  return GenericChunk<T>(std::move(data), size);
}

template<typename T>
GenericChunk<T> GenericChunk<T>::CreateView(T *data, size_t size) {
  // GenericChunks allocated by Create() point to an underlying heap-allocated buffer. On the other
  // hand, GenericChunks created by CreateView() point to the caller's buffer. In the former case
  // we own the buffer and need to delete it when there are no more shared_ptrs pointing to it. In
  // the latter case the caller owns the buffer, and we should not try to deallocate it.
  // One might think we have to use two different data structures to handle these two different
  // case, but by using the below trick we are able to use std::shared_ptr<T[]> for both cases.

  // Start with an empty shared pointer. Like all shared pointers it has a reference count and an
  // underlying buffer that it is managing, but in this case the buffer it is managing is null.
  // This variable is just around for the purposes of the next line.
  std::shared_ptr<T[]> empty;
  // Now make an aliasing shared pointer, which shares the same reference count and manages the same
  // underlying buffer (which is null) but which is configured to return an unrelated pointer
  // (namely 'data'). When the reference count eventually reaches zero, the shared ptr will
  // deallocate the buffer it owns (namely the null buffer) but it won't touch the user's buffer.
  std::shared_ptr<T[]> data_sp(empty, data);
  return GenericChunk<T>(std::move(data_sp), size);
}

template<typename T>
GenericChunk<T>::GenericChunk(std::shared_ptr<T[]> data, size_t size) : Chunk(size),
  data_(std::move(data)) {}

template<typename T>
GenericChunk<T> GenericChunk<T>::Take(size_t size) const {
  CheckSize(size, DEEPHAVEN_PRETTY_FUNCTION);
  // Share ownership of data_.
  return GenericChunk<T>(data_, size);
}

template<typename T>
GenericChunk<T> GenericChunk<T>::Drop(size_t size) const {
  CheckSize(size, DEEPHAVEN_PRETTY_FUNCTION);
  // Make a shared_ptr which manages ownership of the same underlying buffer as data_, but which
  // actually points to the interior of that buffer, namely data_.get() + size.
  std::shared_ptr<T[]> new_begin(data_, data_.get() + size);
  auto new_size = size_ - size;
  return GenericChunk<T>(std::move(new_begin), new_size);
}
}  // namespace deephaven::dhcore::chunk
