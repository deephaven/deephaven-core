/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <vector>
#include "deephaven/client/columns.h"
#include "deephaven/client/impl/expression_impl.h"

namespace deephaven::client::impl {
class DateTimeExpressionImpl : public ExpressionImpl {
protected:
  typedef deephaven::client::DateTime DateTime;

public:
  static std::shared_ptr<DateTimeExpressionImpl> createFromLiteral(std::string value);
  static std::shared_ptr<DateTimeExpressionImpl> createFromDateTime(const DateTime &value);

  static std::shared_ptr<BooleanExpressionImpl> createComparison(
      std::shared_ptr<DateTimeExpressionImpl> lhs, const char *op,
      std::shared_ptr<DateTimeExpressionImpl> rhs);

  ~DateTimeExpressionImpl() override;
};
}  // namespace deephaven::client::impl
