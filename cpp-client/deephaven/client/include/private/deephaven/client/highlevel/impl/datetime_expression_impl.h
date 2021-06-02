#pragma once

#include <memory>
#include <vector>
#include "deephaven/client/highlevel/columns.h"
#include "deephaven/client/highlevel/impl/expression_impl.h"

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {
class DateTimeExpressionImpl : public ExpressionImpl {
protected:
  typedef deephaven::client::highlevel::DBDateTime DBDateTime;

public:
  static std::shared_ptr<DateTimeExpressionImpl> createFromLiteral(std::string value);
  static std::shared_ptr<DateTimeExpressionImpl> createFromDBDateTime(const DBDateTime &value);

  static std::shared_ptr<BooleanExpressionImpl> createComparison(
      std::shared_ptr<DateTimeExpressionImpl> lhs, const char *op,
      std::shared_ptr<DateTimeExpressionImpl> rhs);

  ~DateTimeExpressionImpl() override;
};
}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
