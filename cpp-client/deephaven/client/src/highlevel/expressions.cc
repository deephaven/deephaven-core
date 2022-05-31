#include "deephaven/client/highlevel/expressions.h"

#include "deephaven/client/highlevel/impl/boolean_expression_impl.h"
#include "deephaven/client/highlevel/impl/datetime_expression_impl.h"
#include "deephaven/client/highlevel/impl/expression_impl.h"
#include "deephaven/client/highlevel/impl/numeric_expression_impl.h"
#include "deephaven/client/highlevel/impl/string_expression_impl.h"
#include "deephaven/client/highlevel/columns.h"

namespace deephaven {
namespace client {
namespace highlevel {
Expression::Expression(std::shared_ptr<impl::ExpressionImpl> impl) : impl_(std::move(impl)) {}
Expression::~Expression() = default;

AssignedColumn Expression::as(std::string columnName) const {
  return AssignedColumn::create(std::move(columnName), implAsExpressionImpl());
}

BooleanExpression::BooleanExpression(std::shared_ptr<impl::BooleanExpressionImpl> impl) :
    Expression(std::move(impl)) {}

BooleanExpression::~BooleanExpression() = default;

BooleanExpression Expression::isNull() const {
  auto resultImpl = impl::ExpressionImpl::createIsNull(implAsExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}

std::shared_ptr<impl::BooleanExpressionImpl> BooleanExpression::implAsBooleanExpressionImpl() const {
  return std::dynamic_pointer_cast<impl::BooleanExpressionImpl>(impl_);
}

BooleanExpression operator!(const BooleanExpression &expr) {
  auto resultImpl = impl::BooleanExpressionImpl::createNot(expr.implAsBooleanExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}

BooleanExpression operator&(const BooleanExpression &lhs, const BooleanExpression &rhs) {
  auto resultImpl = impl::BooleanExpressionImpl::createAnd(lhs.implAsBooleanExpressionImpl(),
      rhs.implAsBooleanExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}

BooleanExpression operator|(const BooleanExpression &lhs, const BooleanExpression &rhs) {
  auto resultImpl = impl::BooleanExpressionImpl::createOr(lhs.implAsBooleanExpressionImpl(),
      rhs.implAsBooleanExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}

NumericExpression::NumericExpression(std::shared_ptr<impl::NumericExpressionImpl> impl) :
    Expression(std::move(impl)) {}

NumericExpression::NumericExpression(int value) :
    Expression(impl::NumericExpressionImpl::createInt64Literal(value)) {
}

NumericExpression::NumericExpression(long value) :
    Expression(impl::NumericExpressionImpl::createInt64Literal(value)) {
}

NumericExpression::NumericExpression(float value) :
    Expression(impl::NumericExpressionImpl::createDoubleLiteral(value)) {
}

NumericExpression::NumericExpression(double value) :
    Expression(impl::NumericExpressionImpl::createDoubleLiteral(value)) {
}

NumericExpression::~NumericExpression() = default;

std::shared_ptr<impl::NumericExpressionImpl> NumericExpression::implAsNumericExpressionImpl() const {
  return std::dynamic_pointer_cast<impl::NumericExpressionImpl>(impl_);
}

NumericExpression operator+(const NumericExpression &item) {
  auto resultImpl = impl::NumericExpressionImpl::createUnaryOperator('+',
      item.implAsNumericExpressionImpl());
  return NumericExpression(std::move(resultImpl));
}

NumericExpression operator-(const NumericExpression &item) {
  auto resultImpl = impl::NumericExpressionImpl::createUnaryOperator('-',
      item.implAsNumericExpressionImpl());
  return NumericExpression(std::move(resultImpl));
}
NumericExpression operator~(const NumericExpression &item) {
  auto resultImpl = impl::NumericExpressionImpl::createUnaryOperator('~',
      item.implAsNumericExpressionImpl());
  return NumericExpression(std::move(resultImpl));
}

NumericExpression operator+(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto resultImpl = impl::NumericExpressionImpl::createBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '+', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(resultImpl));
}
NumericExpression operator-(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto resultImpl = impl::NumericExpressionImpl::createBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '-', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(resultImpl));
}
NumericExpression operator*(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto resultImpl = impl::NumericExpressionImpl::createBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '*', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(resultImpl));
}
NumericExpression operator/(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto resultImpl = impl::NumericExpressionImpl::createBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '/', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(resultImpl));
}
NumericExpression operator%(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto resultImpl = impl::NumericExpressionImpl::createBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '%', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(resultImpl));
}
NumericExpression operator^(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto resultImpl = impl::NumericExpressionImpl::createBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '^', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(resultImpl));
}
NumericExpression operator&(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto resultImpl = impl::NumericExpressionImpl::createBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '&', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(resultImpl));
}
NumericExpression operator|(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto resultImpl = impl::NumericExpressionImpl::createBinaryOperator(
      lhs.implAsNumericExpressionImpl(), '|', rhs.implAsNumericExpressionImpl());
  return NumericExpression(std::move(resultImpl));
}

BooleanExpression operator<(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto resultImpl = impl::NumericExpressionImpl::createComparisonOperator(
      lhs.implAsNumericExpressionImpl(), "<", rhs.implAsNumericExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator<=(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto resultImpl = impl::NumericExpressionImpl::createComparisonOperator(
      lhs.implAsNumericExpressionImpl(), "<=", rhs.implAsNumericExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator==(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto resultImpl = impl::NumericExpressionImpl::createComparisonOperator(
      lhs.implAsNumericExpressionImpl(), "==", rhs.implAsNumericExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator!=(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto resultImpl = impl::NumericExpressionImpl::createComparisonOperator(
      lhs.implAsNumericExpressionImpl(), "!=", rhs.implAsNumericExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator>=(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto resultImpl = impl::NumericExpressionImpl::createComparisonOperator(
      lhs.implAsNumericExpressionImpl(), ">=", rhs.implAsNumericExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator>(const NumericExpression &lhs, const NumericExpression &rhs) {
  auto resultImpl = impl::NumericExpressionImpl::createComparisonOperator(
      lhs.implAsNumericExpressionImpl(), ">", rhs.implAsNumericExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}

StringExpression::StringExpression(const char *value) :
    StringExpression(impl::StringExpressionImpl::createLiteral(value)) {}
StringExpression::StringExpression(std::string_view value) :
    StringExpression(impl::StringExpressionImpl::createLiteral(std::string(value.data(), value.size()))) {}
StringExpression::StringExpression(std::string value) :
    StringExpression(impl::StringExpressionImpl::createLiteral(std::move(value))) {}
StringExpression::StringExpression(std::shared_ptr<impl::StringExpressionImpl> impl) :
    Expression(std::move(impl)) {}
StringExpression::~StringExpression() = default;

std::shared_ptr<impl::StringExpressionImpl> StringExpression::implAsStringExpressionImpl() const {
  return std::dynamic_pointer_cast<impl::StringExpressionImpl>(impl_);
}

namespace {
BooleanExpression stringMethodHelper(const StringExpression &lhs, const char *method,
    const StringExpression &rhs) {
  auto impl = impl::BooleanExpressionImpl::createBooleanValuedInstanceMethod(lhs.implAsStringExpressionImpl(),
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
  auto resultImpl = impl::StringExpressionImpl::createAppend(
      lhs.implAsStringExpressionImpl(), rhs.implAsStringExpressionImpl());
  return StringExpression(std::move(resultImpl));
}

BooleanExpression operator<(const StringExpression &lhs, const StringExpression &rhs) {
  auto resultImpl = impl::StringExpressionImpl::createComparison(lhs.implAsStringExpressionImpl(),
      "<", rhs.implAsStringExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator<=(const StringExpression &lhs, const StringExpression &rhs) {
  auto resultImpl = impl::StringExpressionImpl::createComparison(
      lhs.implAsStringExpressionImpl(), "<=", rhs.implAsStringExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator==(const StringExpression &lhs, const StringExpression &rhs) {
  auto resultImpl = impl::StringExpressionImpl::createComparison(
      lhs.implAsStringExpressionImpl(), "==", rhs.implAsStringExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator!=(const StringExpression &lhs, const StringExpression &rhs) {
  auto resultImpl = impl::StringExpressionImpl::createComparison(
      lhs.implAsStringExpressionImpl(), "!=", rhs.implAsStringExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator>=(const StringExpression &lhs, const StringExpression &rhs) {
  auto resultImpl = impl::StringExpressionImpl::createComparison(
      lhs.implAsStringExpressionImpl(), ">=", rhs.implAsStringExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator>(const StringExpression &lhs, const StringExpression &rhs) {
  auto resultImpl = impl::StringExpressionImpl::createComparison(
      lhs.implAsStringExpressionImpl(), ">", rhs.implAsStringExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}

DateTimeExpression::DateTimeExpression(const char *value) :
    DateTimeExpression(impl::DateTimeExpressionImpl::createFromLiteral(value)) {}
DateTimeExpression::DateTimeExpression(std::string_view value) :
    DateTimeExpression(impl::DateTimeExpressionImpl::createFromLiteral(std::string(value.data(), value.size()))) {}
DateTimeExpression::DateTimeExpression(std::string value) :
    DateTimeExpression(impl::DateTimeExpressionImpl::createFromLiteral(std::move(value))) {}
DateTimeExpression::DateTimeExpression(const DateTime &value) :
    DateTimeExpression(impl::DateTimeExpressionImpl::createFromDateTime(value)) {}
DateTimeExpression::DateTimeExpression(std::shared_ptr<impl::DateTimeExpressionImpl> impl) :
    Expression(std::move(impl)) {}
DateTimeExpression::~DateTimeExpression() = default;

std::shared_ptr<impl::DateTimeExpressionImpl> DateTimeExpression::implAsDateTimeExpressionImpl() const {
  return std::dynamic_pointer_cast<impl::DateTimeExpressionImpl>(impl_);
}

BooleanExpression operator<(const DateTimeExpression &lhs, const DateTimeExpression &rhs) {
  auto resultImpl = impl::DateTimeExpressionImpl::createComparison(lhs.implAsDateTimeExpressionImpl(),
      "<", rhs.implAsDateTimeExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator<=(const DateTimeExpression &lhs, const DateTimeExpression &rhs) {
  auto resultImpl = impl::DateTimeExpressionImpl::createComparison(
      lhs.implAsDateTimeExpressionImpl(), "<=", rhs.implAsDateTimeExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator==(const DateTimeExpression &lhs, const DateTimeExpression &rhs) {
  auto resultImpl = impl::DateTimeExpressionImpl::createComparison(
      lhs.implAsDateTimeExpressionImpl(), "==", rhs.implAsDateTimeExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator!=(const DateTimeExpression &lhs, const DateTimeExpression &rhs) {
  auto resultImpl = impl::DateTimeExpressionImpl::createComparison(
      lhs.implAsDateTimeExpressionImpl(), "!=", rhs.implAsDateTimeExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator>=(const DateTimeExpression &lhs, const DateTimeExpression &rhs) {
  auto resultImpl = impl::DateTimeExpressionImpl::createComparison(
      lhs.implAsDateTimeExpressionImpl(), ">=", rhs.implAsDateTimeExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
BooleanExpression operator>(const DateTimeExpression &lhs, const DateTimeExpression &rhs) {
  auto resultImpl = impl::DateTimeExpressionImpl::createComparison(
      lhs.implAsDateTimeExpressionImpl(), ">", rhs.implAsDateTimeExpressionImpl());
  return BooleanExpression(std::move(resultImpl));
}
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
