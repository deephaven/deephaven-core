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
  static std::shared_ptr<BooleanExpressionImpl> createNot(std::shared_ptr<BooleanExpressionImpl> item);
  static std::shared_ptr<BooleanExpressionImpl> createAnd(std::shared_ptr<BooleanExpressionImpl> lhs,
      std::shared_ptr<BooleanExpressionImpl> rhs);
  static std::shared_ptr<BooleanExpressionImpl> createOr(std::shared_ptr<BooleanExpressionImpl> lhs,
      std::shared_ptr<BooleanExpressionImpl> rhs);

  static std::shared_ptr<BooleanExpressionImpl> createBooleanValuedInstanceMethod(
      std::shared_ptr<ExpressionImpl> lhs, std::string method, std::shared_ptr<ExpressionImpl> rhs);

  ~BooleanExpressionImpl() override;
};
}  // namespace deephaven::client::impl
