/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <vector>
#include "expression_impl.h"

namespace deephaven::client {
namespace impl {
class StringExpressionImpl : public ExpressionImpl {
public:
  static std::shared_ptr<StringExpressionImpl> createLiteral(std::string value);
  static std::shared_ptr<StringExpressionImpl> createAppend(
      std::shared_ptr<StringExpressionImpl> lhs, std::shared_ptr<StringExpressionImpl> rhs);
  static std::shared_ptr<BooleanExpressionImpl> createComparison(
      std::shared_ptr<StringExpressionImpl> lhs, const char *op,
      std::shared_ptr<StringExpressionImpl> rhs);

  ~StringExpressionImpl() override;
};
}  // namespace impl
}  // namespace deephaven::client
