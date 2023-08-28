/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <vector>
#include "deephaven/client/impl/expression_impl.h"

namespace deephaven::client::impl {
class NumericExpressionImpl : public ExpressionImpl {
public:
  [[nodiscard]]
  static std::shared_ptr<NumericExpressionImpl> CreateUnaryOperator(char op,
      std::shared_ptr<NumericExpressionImpl> child);
  [[nodiscard]]
  static std::shared_ptr<NumericExpressionImpl> CreateBinaryOperator(
      std::shared_ptr<NumericExpressionImpl> lhs, char op,
      std::shared_ptr<NumericExpressionImpl> rhs);
  [[nodiscard]]
  static std::shared_ptr<BooleanExpressionImpl> CreateComparisonOperator(
      std::shared_ptr<NumericExpressionImpl> lhs, const char *op,
      std::shared_ptr<NumericExpressionImpl> rhs);
  [[nodiscard]]
  static std::shared_ptr<NumericExpressionImpl> CreateInt64Literal(int64_t value);
  [[nodiscard]]
  static std::shared_ptr<NumericExpressionImpl> CreateDoubleLiteral(double value);

  ~NumericExpressionImpl() override;
};
}  // namespace deephaven::client::impl
