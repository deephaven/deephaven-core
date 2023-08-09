/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <vector>
#include "deephaven/client/columns.h"
#include "deephaven/client/impl/expression_impl.h"
#include "deephaven/dhcore/types.h"

namespace deephaven::client::impl {
class DateTimeExpressionImpl : public ExpressionImpl {
protected:
  using DateTime = deephaven::dhcore::DateTime;

public:
  [[nodiscard]]
  static std::shared_ptr<DateTimeExpressionImpl> CreateFromLiteral(std::string value);
  [[nodiscard]]
  static std::shared_ptr<DateTimeExpressionImpl> CreateFromDateTime(const DateTime &value);

  [[nodiscard]]
  static std::shared_ptr<BooleanExpressionImpl> CreateComparison(
      std::shared_ptr<DateTimeExpressionImpl> lhs, const char *op,
      std::shared_ptr<DateTimeExpressionImpl> rhs);

  ~DateTimeExpressionImpl() override;
};
}  // namespace deephaven::client::impl
