#include "deephaven/client/highlevel/impl/datetime_expression_impl.h"

#include <memory>
#include <vector>
#include "deephaven/client/highlevel/columns.h"
#include "deephaven/client/highlevel/impl/boolean_expression_impl.h"
#include "deephaven/client/highlevel/impl/expression_impl.h"
#include "deephaven/client/highlevel/impl/escape_utils.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::highlevel::DateTime;

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {
namespace {
class DateTimeLiteralImpl final : public DateTimeExpressionImpl {
public:
  explicit DateTimeLiteralImpl(std::string value) : value_(std::move(value)) {}
  ~DateTimeLiteralImpl() final = default;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  std::string value_;
};

class DateTimeDateTimeImpl final : public DateTimeExpressionImpl {
public:
  explicit DateTimeDateTimeImpl(const DateTime &value) : value_(value) {}
  ~DateTimeDateTimeImpl() final = default;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  DateTime value_;
};

class DateTimeComparisonImpl final : public BooleanExpressionImpl {
public:
  DateTimeComparisonImpl(std::shared_ptr<DateTimeExpressionImpl> lhs, const char *compareOp,
      std::shared_ptr<DateTimeExpressionImpl> rhs) : lhs_(std::move(lhs)), compareOp_(compareOp),
      rhs_(std::move(rhs)) {}
  ~DateTimeComparisonImpl() final = default;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  std::shared_ptr<DateTimeExpressionImpl> lhs_;
  const char *compareOp_ = nullptr;
  std::shared_ptr<DateTimeExpressionImpl> rhs_;
};
}  // namespace

std::shared_ptr<DateTimeExpressionImpl> DateTimeExpressionImpl::createFromLiteral(std::string value) {
  return std::make_shared<DateTimeLiteralImpl>(std::move(value));
}

std::shared_ptr<DateTimeExpressionImpl> DateTimeExpressionImpl::createFromDateTime(const DateTime &value) {
  return std::make_shared<DateTimeDateTimeImpl>(value);
}

std::shared_ptr<BooleanExpressionImpl> DateTimeExpressionImpl::createComparison(
    std::shared_ptr<DateTimeExpressionImpl> lhs, const char *op,
    std::shared_ptr<DateTimeExpressionImpl> rhs) {
  return std::make_shared<DateTimeComparisonImpl>(std::move(lhs), op, std::move(rhs));
}

DateTimeExpressionImpl::~DateTimeExpressionImpl() = default;

namespace {
void DateTimeLiteralImpl::streamIrisRepresentation(std::ostream &s) const {
  s << '`';
  s << EscapeUtils::escapeJava(value_);
  s << '`';
}

void DateTimeDateTimeImpl::streamIrisRepresentation(std::ostream &s) const {
  s << '`';
  value_.streamIrisRepresentation(s);
  s << '`';
}

void DateTimeComparisonImpl::streamIrisRepresentation(std::ostream &s) const {
  s << '(';
  lhs_->streamIrisRepresentation(s);
  s << ' ';
  s << compareOp_;
  s << ' ';
  rhs_->streamIrisRepresentation(s);
  s << ')';
}
}   // namespace
}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
