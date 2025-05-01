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

class ContainerVisitor {
protected:
  using DateTime = deephaven::dhcore::DateTime;
  using LocalDate = deephaven::dhcore::LocalDate;
  using LocalTime = deephaven::dhcore::LocalTime;

public:
  virtual ~ContainerVisitor() = default;
  virtual void Visit(const Container<char16_t> *container) = 0;
  virtual void Visit(const Container<int8_t> *container) = 0;
  virtual void Visit(const Container<int16_t> *container) = 0;
  virtual void Visit(const Container<int32_t> *container) = 0;
  virtual void Visit(const Container<int64_t> *container) = 0;
  virtual void Visit(const Container<float> *container) = 0;
  virtual void Visit(const Container<double> *container) = 0;
  virtual void Visit(const Container<bool> *container) = 0;
  virtual void Visit(const Container<std::string> *container) = 0;
  virtual void Visit(const Container<DateTime> *container) = 0;
  virtual void Visit(const Container<LocalDate> *container) = 0;
  virtual void Visit(const Container<LocalTime> *container) = 0;
};

class ContainerBase : public std::enable_shared_from_this<ContainerBase> {
public:
  explicit ContainerBase(size_t size) : size_(size) {}
  virtual ~ContainerBase();

  [[nodiscard]]
  size_t size() const {
    return size_;
  }

  template<class T>
  [[nodiscard]]
  std::shared_ptr<const Container<T>> AsContainerPtr() const {
    auto self = shared_from_this();
    return std::dynamic_pointer_cast<const Container<T>>(self);
  }

  template<class T>
  [[nodiscard]]
  const Container<T> &AsContainer() const {
    return *deephaven::dhcore::utility::VerboseCast<const Container<T>*>(DEEPHAVEN_LOCATION_EXPR(this));
  }

  virtual void AcceptVisitor(ContainerVisitor *visitor) const = 0;

protected:
  virtual std::ostream &StreamTo(std::ostream &s) const = 0;

  size_t size_ = 0;

  friend std::ostream &operator<<(std::ostream &s, const ContainerBase &o) {
    return o.StreamTo(s);
  }
};

template<typename T>
class Container final : public ContainerBase {
  using ElementRenderer = deephaven::dhcore::utility::ElementRenderer;
  struct Private{};
public:
  static std::shared_ptr<Container<T>> Create(std::shared_ptr<T[]> data,
      std::shared_ptr<bool[]> nulls, size_t size) {
    return std::make_shared<Container<T>>(Private(), std::move(data), std::move(nulls),
        size);
  }

  Container(Private, std::shared_ptr<T[]> &&data, std::shared_ptr<bool[]> &&nulls, size_t size) :
      ContainerBase(size), data_(std::move(data)), nulls_(std::move(nulls)) {}

  void AcceptVisitor(ContainerVisitor *visitor) const final {
    visitor->Visit(this);
  }

  const T &operator[](size_t index) const {
    return data_[index];
  }

  bool IsNull(size_t index) const {
    return nulls_[index];
  }

  const T *data() const {
    return data_.get();
  }

  const T *begin() const {
    return data();
  }

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
