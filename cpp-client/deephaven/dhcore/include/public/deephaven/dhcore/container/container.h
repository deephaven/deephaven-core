/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <iostream>
#include <string>
#include <utility>
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"

namespace deephaven::dhcore::container {
/**
 * Forward declaration
 * @tparam T
 */
template<typename T>
class Container;

/**
 * Implements the visitor pattern for Container.
 */
class ContainerVisitor {
protected:
  /**
   * Convenience using.
   */
  using DateTime = deephaven::dhcore::DateTime;
  /**
   * Convenience using.
   */
  using LocalDate = deephaven::dhcore::LocalDate;
  /**
   * Convenience using.
   */
  using LocalTime = deephaven::dhcore::LocalTime;

public:
  /**
   * Destructor.
   */
  virtual ~ContainerVisitor() = default;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Container<char16_t> *container) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Container<int8_t> *container) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Container<int16_t> *container) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Container<int32_t> *container) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Container<int64_t> *container) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Container<float> *container) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Container<double> *container) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Container<bool> *container) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Container<std::string> *container) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Container<DateTime> *container) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Container<LocalDate> *container) = 0;
  /**
   * Implements the visitor pattern.
   */
  virtual void Visit(const Container<LocalTime> *container) = 0;
};

/**
 * Abstract class for the Deephaven Container type.
 */
class ContainerBase : public std::enable_shared_from_this<ContainerBase> {
public:
  explicit ContainerBase(size_t size) : size_(size) {}
  virtual ~ContainerBase();

  /**
   * Returns the number of elements in the container.
   * @return The number of elements in the Container
   */
  [[nodiscard]]
  size_t size() const {
    return size_;
  }

  /**
   * If this object is a Container<T>, returns a shared_ptr<const Container<T>> pointing
   * to this object. Otherwise, returns a null shared_ptr.
   * @return A shared_ptr<const Container<T>> if this object is a Container<T>, otherwise
   * a null shared_ptr.
   */
  template<class T>
  [[nodiscard]]
  std::shared_ptr<const Container<T>> AsContainerPtr() const {
    auto self = shared_from_this();
    return std::dynamic_pointer_cast<const Container<T>>(self);
  }

  /**
   * If this object is a Container<T>, returns a const Container<T>& referring to this
   * object. Otherwise, throws an exception.
   * @return A const Container<T>& referring to this object.
   */
  template<class T>
  [[nodiscard]]
  const Container<T> &AsContainer() const {
    return *deephaven::dhcore::utility::VerboseCast<const Container<T>*>(DEEPHAVEN_LOCATION_EXPR(this));
  }

  /**
   * Implements the Visitor pattern.
   */
  virtual void AcceptVisitor(ContainerVisitor *visitor) const = 0;

protected:
  virtual std::ostream &StreamTo(std::ostream &s) const = 0;

  size_t size_ = 0;

  /**
   * Ostream operator.
   */
  friend std::ostream &operator<<(std::ostream &s, const ContainerBase &o) {
    return o.StreamTo(s);
  }
};

template<typename T>
class Container final : public ContainerBase {
  using ElementRenderer = deephaven::dhcore::utility::ElementRenderer;
  struct Private{};
public:
  /**
   * Creates a Container<T> of the specified size from the corresponding data
   * and nulls arrays.
   * @param data A shared_ptr to the underlying data
   * @param nulls A shared_ptr to the nulls flags
   * @param size The size of the container
   * @return A shared_ptr<Container<T>> representing the specified data.
   */
  static std::shared_ptr<Container<T>> Create(std::shared_ptr<T[]> data,
      std::shared_ptr<bool[]> nulls, size_t size) {
    return std::make_shared<Container<T>>(Private(), std::move(data), std::move(nulls),
        size);
  }

  /**
   * Constructor. This constructor is effectively private because outside callers cannot
   * create the dummy "Private" argument.
   */
  Container(Private, std::shared_ptr<T[]> &&data, std::shared_ptr<bool[]> &&nulls, size_t size) :
      ContainerBase(size), data_(std::move(data)), nulls_(std::move(nulls)) {}

  /**
   * Implement the Visitor pattern.
   */
  void AcceptVisitor(ContainerVisitor *visitor) const final {
    visitor->Visit(this);
  }

  /**
   * Indexing operator
   * @param index Index of the specified element
   * @return a const reference to the specified element
   */
  const T &operator[](size_t index) const {
    return data_[index];
  }

  /**
   * Determines if the element at the specified index is null.
   * @param index Index of the specified element
   * @return true if the element at the specified index is null; false otherwise
   */
  bool IsNull(size_t index) const {
    return nulls_[index];
  }

  /**
   * Gets a const pointer to the start of the data
   * @return A const pointer to the start of the data
   */
  const T *data() const {
    return data_.get();
  }

  /**
   * Gets a const pointer to the start of the data.
   * @return A const pointer to the start of the data
   */
  const T *begin() const {
    return data();
  }

  /**
   * Gets a const pointer to one past the end of the data.
   * @return A const pointer to one past the end of the data
   */
  const T *end() const {
    return data() + size();
  }

private:
  std::ostream &StreamTo(std::ostream &s) const final {
    ElementRenderer renderer;
    s << '[';
    const char *sep = "";
    const auto sz = size();
    for (size_t i = 0; i != sz; ++i) {
      s << sep;
      sep = ",";
      if (nulls_[i]) {
        s << "null";
      } else {
        renderer.Render(s, data_[i]);
      }
    }
    return s << ']';
  }

  std::shared_ptr<T[]> data_;
  std::shared_ptr<bool[]> nulls_;
};
}  // namespace deephaven::dhcore::container
