/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/expressions.h"

#include "deephaven/client/impl/boolean_expression_impl.h"
#include "deephaven/client/impl/datetime_expression_impl.h"
#include "deephaven/client/impl/expression_impl.h"
#include "deephaven/client/impl/numeric_expression_impl.h"
#include "deephaven/client/impl/string_expression_impl.h"
#include "deephaven/client/columns.h"

namespace deephaven::client {
Expression::Expression(std::shared_ptr<impl::ExpressionImpl> impl) : impl_(std::move(impl)) {}

Expression::~Expression() = default;

AssignedColumn Expression::as(std::string column_name) const {
  return AssignedColumn::Create(std::move(column_name), implAsExpressionImpl());
}

BooleanExpression::BooleanExpression(std::shared_ptr<impl::BooleanExpressionImpl> impl) :
    Expression(std::move(impl)) {}

BooleanExpression::~BooleanExpression() = default;

BooleanExpression Expression::isNull() const {
  auto result_impl = impl::ExpressionImpl::CreateIsNull(implAsExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

std::shared_ptr<impl::BooleanExpressionImpl>
BooleanExpression::implAsBooleanExpressionImpl() const {
  return std::dynamic_pointer_cast<impl::BooleanExpressionImpl>(impl_);
}

BooleanExpression operator!(const BooleanExpression &expr) {
  auto result_impl = impl::BooleanExpressionImpl::CreateNot(expr.implAsBooleanExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator&(const BooleanExpression &lhs, const BooleanExpression &rhs) {
  auto result_impl = impl::BooleanExpressionImpl::CreateAnd(lhs.implAsBooleanExpressionImpl(),
      rhs.implAsBooleanExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator|(const BooleanExpression &lhs, const BooleanExpression &rhs) {
  auto result_impl = impl::BooleanExpressionImpl::CreateOr(lhs.implAsBooleanExpressionImpl(),
      rhs.implAsBooleanExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

NumericExpression::NumericExpression(std::shared_ptr<impl::NumericExpressionImpl> impl) :
    Expression(std::move(impl)) {}

NumericExpression::NumericExpression(int32_t value) :
    Expression(impl::NumericExpressionImpl::CreateInt64Literal(value)) {
}

NumericExpression::NumericExpression(int64_t value) :
    Expression(impl::NumericExpressionImpl::CreateInt64Literal(value)) {
}

NumericExpression::NumericExpression(float value) :
    Expression(impl::NumericExpressionImpl::CreateDoubleLiteral(value)) {
}

NumericExpression::NumericExpression(double value) :
    Expression(impl::NumericExpressionImpl::CreateDoubleLiteral(value)) {
}

NumericExpression::~NumericExpression() = default;

std::shared_ptr<impl::NumericExpressionImpl>
NumericExpression::implAsNumericExpressionImpl() const {
  return std::dynamic_pointer_cast<impl::NumericExpressionImpl>(impl_);
}

NumericExpression operator+(const NumericExpression &item) {
  auto result_impl = impl::NumericExpressionImpl::CreateUnaryOperator('+',
      item.implAsNumericExpressionImpl());
  return NumericExpression(std::move(result_impl));
}

NumericExpression operator-(const NumericExpression &item) {
  auto result_impl = impl::NumericExpressionImpl::CreateUnaryOperator('-',
      item.implAsNumericExpressionImpl());
  return NumericExpression(std::move(result_impl));
}

NumericExpression operator~(const NumericExpression &item) {
  auto result_impl = impl::NumericExpressionImpl::CreateUnaryOperator('~',
      item.implAsNumericExpressionImpl());
  return NumericExpression(std::move(result_impl));
}

NumericExpression operator+(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto result_impl = impl::NumericExpressionImpl::CreateBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '+', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(result_impl));
}

NumericExpression operator-(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto result_impl = impl::NumericExpressionImpl::CreateBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '-', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(result_impl));
}

NumericExpression operator*(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto result_impl = impl::NumericExpressionImpl::CreateBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '*', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(result_impl));
}

NumericExpression operator/(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto result_impl = impl::NumericExpressionImpl::CreateBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '/', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(result_impl));
}

NumericExpression operator%(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto result_impl = impl::NumericExpressionImpl::CreateBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '%', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(result_impl));
}

NumericExpression operator^(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto result_impl = impl::NumericExpressionImpl::CreateBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '^', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(result_impl));
}

NumericExpression operator&(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto result_impl = impl::NumericExpressionImpl::CreateBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '&', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(result_impl));
}

NumericExpression operator|(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto result_impl = impl::NumericExpressionImpl::CreateBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '|', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(result_impl));
}

BooleanExpression operator<(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto result_impl = impl::NumericExpressionImpl::CreateComparisonOperator(
      lhs.implAsNumericExpressionImpl(), "<", rhs.implAsNumericExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator<=(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto result_impl = impl::NumericExpressionImpl::CreateComparisonOperator(
      lhs.implAsNumericExpressionImpl(), "<=", rhs.implAsNumericExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator==(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto result_impl = impl::NumericExpressionImpl::CreateComparisonOperator(
      lhs.implAsNumericExpressionImpl(), "==", rhs.implAsNumericExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator!=(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto result_impl = impl::NumericExpressionImpl::CreateComparisonOperator(
      lhs.implAsNumericExpressionImpl(), "!=", rhs.implAsNumericExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator>=(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto result_impl = impl::NumericExpressionImpl::CreateComparisonOperator(
      lhs.implAsNumericExpressionImpl(), ">=", rhs.implAsNumericExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator>(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto result_impl = impl::NumericExpressionImpl::CreateComparisonOperator(
      lhs.implAsNumericExpressionImpl(), ">", rhs.implAsNumericExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

StringExpression::StringExpression(const char *value) :
    StringExpression(impl::StringExpressionImpl::CreateLiteral(value)) {}

StringExpression::StringExpression(std::string_view value) :
    StringExpression(
        impl::StringExpressionImpl::CreateLiteral(std::string(value.data(), value.size()))) {}

StringExpression::StringExpression(std::string value) :
    StringExpression(impl::StringExpressionImpl::CreateLiteral(std::move(value))) {}

StringExpression::StringExpression(std::shared_ptr<impl::StringExpressionImpl> impl) :
    Expression(std::move(impl)) {}

StringExpression::~StringExpression() = default;

std::shared_ptr<impl::StringExpressionImpl> StringExpression::implAsStringExpressionImpl() const {
  return std::dynamic_pointer_cast<impl::StringExpressionImpl>(impl_);
}

namespace {
BooleanExpression stringMethodHelper(const StringExpression &lhs, const char *method,
    const StringExpression &rhs) {
  auto impl = impl::BooleanExpressionImpl::CreateBooleanValuedInstanceMethod(
      lhs.implAsStringExpressionImpl(),
      method, rhs.implAsExpressionImpl());
  return BooleanExpression(std::move(impl));
}
} // namespace

BooleanExpression StringExpression::startsWith(const StringExpression &other) const {
  return stringMethodHelper(*this, "startsWith", other);
}

BooleanExpression StringExpression::endsWith(const StringExpression &other) const {
  return stringMethodHelper(*this, "endsWith", other);
}

BooleanExpression StringExpression::contains(const StringExpression &other) const {
  return stringMethodHelper(*this, "contains", other);
}

BooleanExpression StringExpression::matches(const StringExpression &other) const {
  return stringMethodHelper(*this, "matches", other);
}

StringExpression operator+(const StringExpression &lhs, const StringExpression &rhs) {
  auto result_impl = impl::StringExpressionImpl::CreateAppend(
      lhs.implAsStringExpressionImpl(), rhs.implAsStringExpressionImpl());
  return StringExpression(std::move(result_impl));
}

BooleanExpression operator<(const StringExpression &lhs, const StringExpression &rhs) {
  auto result_impl = impl::StringExpressionImpl::CreateComparison(lhs.implAsStringExpressionImpl(),
      "<", rhs.implAsStringExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator<=(const StringExpression &lhs, const StringExpression &rhs) {
  auto result_impl = impl::StringExpressionImpl::CreateComparison(
      lhs.implAsStringExpressionImpl(), "<=", rhs.implAsStringExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator==(const StringExpression &lhs, const StringExpression &rhs) {
  auto result_impl = impl::StringExpressionImpl::CreateComparison(
      lhs.implAsStringExpressionImpl(), "==", rhs.implAsStringExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator!=(const StringExpression &lhs, const StringExpression &rhs) {
  auto result_impl = impl::StringExpressionImpl::CreateComparison(
      lhs.implAsStringExpressionImpl(), "!=", rhs.implAsStringExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator>=(const StringExpression &lhs, const StringExpression &rhs) {
  auto result_impl = impl::StringExpressionImpl::CreateComparison(
      lhs.implAsStringExpressionImpl(), ">=", rhs.implAsStringExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator>(const StringExpression &lhs, const StringExpression &rhs) {
  auto result_impl = impl::StringExpressionImpl::CreateComparison(
      lhs.implAsStringExpressionImpl(), ">", rhs.implAsStringExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

DateTimeExpression::DateTimeExpression(const char *value) :
    DateTimeExpression(impl::DateTimeExpressionImpl::CreateFromLiteral(value)) {}

DateTimeExpression::DateTimeExpression(std::string_view value) :
    DateTimeExpression(
            impl::DateTimeExpressionImpl::CreateFromLiteral(std::string(value.data(), value.size()))) {}

DateTimeExpression::DateTimeExpression(std::string value) :
    DateTimeExpression(impl::DateTimeExpressionImpl::CreateFromLiteral(std::move(value))) {}

DateTimeExpression::DateTimeExpression(const DateTime &value) :
    DateTimeExpression(impl::DateTimeExpressionImpl::CreateFromDateTime(value)) {}

DateTimeExpression::DateTimeExpression(std::shared_ptr<impl::DateTimeExpressionImpl> impl) :
    Expression(std::move(impl)) {}

DateTimeExpression::~DateTimeExpression() = default;

std::shared_ptr<impl::DateTimeExpressionImpl>
DateTimeExpression::implAsDateTimeExpressionImpl() const {
  return std::dynamic_pointer_cast<impl::DateTimeExpressionImpl>(impl_);
}

BooleanExpression operator<(const DateTimeExpression &lhs, const DateTimeExpression &rhs) {
  auto result_impl = impl::DateTimeExpressionImpl::CreateComparison(
      lhs.implAsDateTimeExpressionImpl(),
      "<", rhs.implAsDateTimeExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator<=(const DateTimeExpression &lhs, const DateTimeExpression &rhs) {
  auto result_impl = impl::DateTimeExpressionImpl::CreateComparison(
      lhs.implAsDateTimeExpressionImpl(), "<=", rhs.implAsDateTimeExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator==(const DateTimeExpression &lhs, const DateTimeExpression &rhs) {
  auto result_impl = impl::DateTimeExpressionImpl::CreateComparison(
      lhs.implAsDateTimeExpressionImpl(), "==", rhs.implAsDateTimeExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator!=(const DateTimeExpression &lhs, const DateTimeExpression &rhs) {
  auto result_impl = impl::DateTimeExpressionImpl::CreateComparison(
      lhs.implAsDateTimeExpressionImpl(), "!=", rhs.implAsDateTimeExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator>=(const DateTimeExpression &lhs, const DateTimeExpression &rhs) {
  auto result_impl = impl::DateTimeExpressionImpl::CreateComparison(
      lhs.implAsDateTimeExpressionImpl(), ">=", rhs.implAsDateTimeExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}

BooleanExpression operator>(const DateTimeExpression &lhs, const DateTimeExpression &rhs) {
  auto result_impl = impl::DateTimeExpressionImpl::CreateComparison(
      lhs.implAsDateTimeExpressionImpl(), ">", rhs.implAsDateTimeExpressionImpl());
  return BooleanExpression(std::move(result_impl));
}
}  // namespace deephaven::client
