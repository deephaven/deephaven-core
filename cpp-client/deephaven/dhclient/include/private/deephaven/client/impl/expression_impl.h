/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <iostream>
#include <memory>

namespace deephaven::client::impl {
class BooleanExpressionImpl;

class IrisRepresentableImpl {
public:
  virtual ~IrisRepresentableImpl();
  virtual void StreamIrisRepresentation(std::ostream &s) const = 0;
};

void StreamIris(std::ostream &s, const std::shared_ptr<IrisRepresentableImpl> &o);

class ExpressionImpl : public virtual IrisRepresentableImpl {
public:
  [[nodiscard]]
  static std::shared_ptr<BooleanExpressionImpl> CreateIsNull(std::shared_ptr<ExpressionImpl> impl);
  ~ExpressionImpl() override;
};
}  // namespace deephaven::client::impl
