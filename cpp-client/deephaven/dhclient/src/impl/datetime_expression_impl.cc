/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/datetime_expression_impl.h"

#include <memory>
#include <vector>
#include "deephaven/client/columns.h"
#include "deephaven/client/impl/boolean_expression_impl.h"
#include "deephaven/client/impl/expression_impl.h"
#include "deephaven/client/impl/escape_utils.h"

using deephaven::dhcore::DateTime;

namespace deephaven::client {
namespace impl {
namespace {
class DateTimeLiteralImpl final : public DateTimeExpressionImpl {
public:
  explicit DateTimeLiteralImpl(std::string value) : value_(std::move(value)) {}

  ~DateTimeLiteralImpl() final = default;

  void StreamIrisRepresentation(std::ostream &s) const final;

private:
  std::string value_;
};

class DateTimeDateTimeImpl final : public DateTimeExpressionImpl {
public:
  explicit DateTimeDateTimeImpl(const DateTime &value) : value_(value) {}

  ~DateTimeDateTimeImpl() final = default;

  void StreamIrisRepresentation(std::ostream &s) const final;

private:
  DateTime value_;
};

class DateTimeComparisonImpl final : public BooleanExpressionImpl {
public:
  DateTimeComparisonImpl(std::shared_ptr<DateTimeExpressionImpl> lhs, const char *compareOp,
      std::shared_ptr<DateTimeExpressionImpl> rhs) : lhs_(std::move(lhs)), compareOp_(compareOp),
      rhs_(std::move(rhs)) {}

  ~DateTimeComparisonImpl() final = default;

  void StreamIrisRepresentation(std::ostream &s) const final;

private:
  std::shared_ptr<DateTimeExpressionImpl> lhs_;
  const char *compareOp_ = nullptr;
  std::shared_ptr<DateTimeExpressionImpl> rhs_;
};
}  // namespace

std::shared_ptr<DateTimeExpressionImpl>
DateTimeExpressionImpl::CreateFromLiteral(std::string value) {
  return std::make_shared<DateTimeLiteralImpl>(std::move(value));
}

std::shared_ptr<DateTimeExpressionImpl>
DateTimeExpressionImpl::CreateFromDateTime(const DateTime &value) {
  return std::make_shared<DateTimeDateTimeImpl>(value);
}

std::shared_ptr<BooleanExpressionImpl> DateTimeExpressionImpl::CreateComparison(
    std::shared_ptr<DateTimeExpressionImpl> lhs, const char *op,
    std::shared_ptr<DateTimeExpressionImpl> rhs) {
  return std::make_shared<DateTimeComparisonImpl>(std::move(lhs), op, std::move(rhs));
}

DateTimeExpressionImpl::~DateTimeExpressionImpl() = default;

namespace {
void DateTimeLiteralImpl::StreamIrisRepresentation(std::ostream &s) const {
  s << '`';
  s << EscapeUtils::EscapeJava(value_);
  s << '`';
}

void DateTimeDateTimeImpl::StreamIrisRepresentation(std::ostream &s) const {
  s << '`';
  value_.StreamIrisRepresentation(s);
  s << '`';
}

void DateTimeComparisonImpl::StreamIrisRepresentation(std::ostream &s) const {
  s << '(';
  lhs_->StreamIrisRepresentation(s);
  s << ' ';
  s << compareOp_;
  s << ' ';
  rhs_->StreamIrisRepresentation(s);
  s << ')';
}
}  // namespace
}  // namespace impl
}  // namespace deephaven::client
