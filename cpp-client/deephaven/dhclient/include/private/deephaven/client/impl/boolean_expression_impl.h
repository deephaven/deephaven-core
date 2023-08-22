/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <vector>
#include "deephaven/client/impl/expression_impl.h"

namespace deephaven::client::impl {
class BooleanExpressionImpl : public ExpressionImpl {
public:
  [[nodiscard]]
  static std::shared_ptr<BooleanExpressionImpl> CreateNot(std::shared_ptr<BooleanExpressionImpl> item);
  [[nodiscard]]
  static std::shared_ptr<BooleanExpressionImpl> CreateAnd(std::shared_ptr<BooleanExpressionImpl> lhs,
      std::shared_ptr<BooleanExpressionImpl> rhs);
  [[nodiscard]]
  static std::shared_ptr<BooleanExpressionImpl> CreateOr(std::shared_ptr<BooleanExpressionImpl> lhs,
      std::shared_ptr<BooleanExpressionImpl> rhs);

  [[nodiscard]]
  static std::shared_ptr<BooleanExpressionImpl> CreateBooleanValuedInstanceMethod(
      std::shared_ptr<ExpressionImpl> lhs, std::string method, std::shared_ptr<ExpressionImpl> rhs);

  ~BooleanExpressionImpl() override;
};
}  // namespace deephaven::client::impl
