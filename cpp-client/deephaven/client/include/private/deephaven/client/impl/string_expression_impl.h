/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <vector>
#include "expression_impl.h"

namespace deephaven::client::impl {
class StringExpressionImpl : public ExpressionImpl {
public:
  [[nodiscard]]
  static std::shared_ptr<StringExpressionImpl> CreateLiteral(std::string value);
  [[nodiscard]]
  static std::shared_ptr<StringExpressionImpl> CreateAppend(
      std::shared_ptr<StringExpressionImpl> lhs, std::shared_ptr<StringExpressionImpl> rhs);
  [[nodiscard]]
  static std::shared_ptr<BooleanExpressionImpl> CreateComparison(
      std::shared_ptr<StringExpressionImpl> lhs, const char *op,
      std::shared_ptr<StringExpressionImpl> rhs);

  ~StringExpressionImpl() override;
};
}  // namespace deephaven::client::impl
