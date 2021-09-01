#pragma once

#include <memory>
#include <vector>
#include "deephaven/client/highlevel/impl/expression_impl.h"

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {
class NumericExpressionImpl : public ExpressionImpl {
public:
  static std::shared_ptr<NumericExpressionImpl> createUnaryOperator(char op,
      std::shared_ptr<NumericExpressionImpl> child);
  static std::shared_ptr<NumericExpressionImpl> createBinaryOperator(
      std::shared_ptr<NumericExpressionImpl> lhs, char op, std::shared_ptr<NumericExpressionImpl> rhs);
  static std::shared_ptr<BooleanExpressionImpl> createComparisonOperator(
      std::shared_ptr<NumericExpressionImpl> lhs, const char *op,
      std::shared_ptr<NumericExpressionImpl> rhs);
  static std::shared_ptr<NumericExpressionImpl> createInt64Literal(int64_t value);
  static std::shared_ptr<NumericExpressionImpl> createDoubleLiteral(double value);

  ~NumericExpressionImpl() override;
};
}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
