#pragma once

#include <memory>
#include <string_view>
#include "deephaven/client/highlevel/types.h"

namespace deephaven {
namespace client {
namespace highlevel {
class AssignedColumn;
class BooleanExpression;

namespace impl {
class ExpressionImpl;
class BooleanExpressionImpl;
class NumericExpressionImpl;
class StringExpressionImpl;
class DateTimeExpressionImpl;
}  // namespace impl

/**
 * The root type shared by various leaf expression classes.
 */
class Expression {
public:
  /**
   * Constructor. Used internally.
   */
  explicit Expression(std::shared_ptr<impl::ExpressionImpl> impl);
  Expression(const Expression &) = delete;
  Expression &operator=(const Expression &) = delete;
  /**
   * Move constructor.
   */
  Expression(Expression &&) = default;
  /**
   * Move assignment operator.
   */
  Expression &operator=(Expression &&) = default;
  /**
   * Destructor.
   */
  virtual ~Expression();

  /**
   * Builds a BooleanExpression representing whether this Expression is null.
   * @return The new BooleanExpression
   */
  BooleanExpression isNull() const;
  /**
   * Builds an AssignedColumn expression representing this expression as assigned to a new
   * column name. Used for example in the TableHandle::select() method.
   * @code
   * auto newTable = tableHandle.select(A, B, (C + 5).as("NewCol"));
   * @endcode
   * @return The new AssignedColumn
   */
  AssignedColumn as(std::string columnName) const;

  /**
   * Used internally
   */
  const std::shared_ptr<impl::ExpressionImpl> &implAsExpressionImpl() const {
    return impl_;
  }

protected:
  std::shared_ptr<impl::ExpressionImpl> impl_;
};

/**
 * A type in the "fluent" syntax that represents a boolean expression.
 */
class BooleanExpression final : public Expression {
public:
  /**
   * Constructor. Used internally.
   */
  explicit BooleanExpression(std::shared_ptr<impl::BooleanExpressionImpl> impl);
  /**
   * Move constructor.
   */
  BooleanExpression(BooleanExpression &&) = default;
  /**
   * Move assignment operator.
   */
  BooleanExpression &operator=(BooleanExpression &&) = default;
  /**
   * Destructor.
   */
  ~BooleanExpression() final;

  std::shared_ptr<impl::BooleanExpressionImpl> implAsBooleanExpressionImpl() const;

  friend BooleanExpression operator!(const BooleanExpression &expr);
  friend BooleanExpression operator&(const BooleanExpression &lhs, const BooleanExpression &rhs);
  friend BooleanExpression operator|(const BooleanExpression &lhs, const BooleanExpression &rhs);
  friend BooleanExpression operator&&(const BooleanExpression &lhs, const BooleanExpression &rhs) {
    return lhs & rhs;
  }
  friend BooleanExpression operator||(const BooleanExpression &lhs, const BooleanExpression &rhs) {
    return lhs | rhs;
  }
};

class NumericExpression : public Expression {
public:
  /**
   * Constructor. Used internally.
   */
  explicit NumericExpression(std::shared_ptr<impl::NumericExpressionImpl> impl);
  /**
   * Implicit conversion from int.
   * @param value The value
   */
  // TODO(kosak): more numeric types, or a template maybe
  NumericExpression(int value);  // NOLINT(google-explicit-constructor)
  /**
   * Implicit conversion from long.
   * @param value The value
   */
  NumericExpression(long value);  // NOLINT(google-explicit-constructor)
  /**
   * Implicit conversion from float.
   * @param value The value
   */
  NumericExpression(float value);  // NOLINT(google-explicit-constructor)
  /**
   * Implicit conversion from double.
   * @param value The value
   */
  NumericExpression(double value);  // NOLINT(google-explicit-constructor)
  /**
   * Move constructor.
   */
  NumericExpression(NumericExpression &&) = default;
  /**
   * Move assignment operator.
   */
  NumericExpression &operator=(NumericExpression &&) = default;
  /**
   * Destructor.
   */
  ~NumericExpression() override;

  std::shared_ptr<impl::NumericExpressionImpl> implAsNumericExpressionImpl() const;

  /**
   * Unary operator +. This is effectively a no-op.
   * @param item The incoming NumericExpression
   * @return A NumericExpression representing the same item.
   */
  friend NumericExpression operator+(const NumericExpression &item);
  /**
   * Unary operator -
   * @param item The incoming NumericExpression
   * @return The NumericExpression representing the negation of `item`
   */
  friend NumericExpression operator-(const NumericExpression &item);
  /**
   * Unary operator ~
   * @param item The incoming NumericExpression
   * @return The NumericExpression representing the complement of `item`
   */
  friend NumericExpression operator~(const NumericExpression &item);
  /**
   * Binary operator +
   * @param lhs A NumericExpression representing the left-hand side of the addition
   * @param rhs A NumericExpression representing the right-hand side of the addition
   * @return The NumericExpression representing the sum `lhs + rhs`
   */
  friend NumericExpression operator+(const NumericExpression &lhs, const NumericExpression &rhs);
  /**
   * Binary operator -
   * @param lhs A NumericExpression representing the left-hand side of the subtraction
   * @param rhs A NumericExpression representing the right-hand side of the subtraction
   * @return The NumericExpression representing the difference `lhs - rhs`
   */
  friend NumericExpression operator-(const NumericExpression &lhs, const NumericExpression &rhs);
  /**
   * Binary operator *
   * @param lhs A NumericExpression representing the left-hand side of the multiplication
   * @param rhs A NumericExpression representing the right-hand side of the multiplication
   * @return The NumericExpression representing the product `lhs * rhs`
   */
  friend NumericExpression operator*(const NumericExpression &lhs, const NumericExpression &rhs);
  /**
   * Binary operator /
   * @param lhs A NumericExpression representing the left-hand side of the division
   * @param rhs A NumericExpression representing the right-hand side of the division
   * @return The NumericExpression representing the division `lhs / rhs`
   */
  friend NumericExpression operator/(const NumericExpression &lhs, const NumericExpression &rhs);
  /**
   * Binary operator %
   * @param lhs A NumericExpression representing the left-hand side of the modulus operation
   * @param rhs A NumericExpression representing the right-hand side of the modulus operation
   * @return The NumericExpression representing the operation `lhs % rhs`
   */
  friend NumericExpression operator%(const NumericExpression &lhs, const NumericExpression &rhs);
  /**
   * Binary operator ^
   * @param lhs A NumericExpression representing the left-hand side of the XOR operation
   * @param rhs A NumericExpression representing the right-hand side of the XOR operation
   * @return The NumericExpression representing the exclusive-or operation `lhs ^ rhs`
   */
  friend NumericExpression operator^(const NumericExpression &lhs, const NumericExpression &rhs);
  /**
   * Binary operator |
   * @param lhs A NumericExpression representing the left-hand side of the OR operation
   * @param rhs A NumericExpression representing the right-hand side of the OR operation
   * @return The NumericExpression representing the bitwise-or operation `lhs | rhs`
   */
  friend NumericExpression operator|(const NumericExpression &lhs, const NumericExpression &rhs);
  /**
   * Binary operator &
   * @param lhs A NumericExpression representing the left-hand side of the AND operation
   * @param rhs A NumericExpression representing the right-hand side of the AND operation
   * @return The NumericExpression representing the bitwise-and operation `lhs & rhs`
   */
  friend NumericExpression operator&(const NumericExpression &lhs, const NumericExpression &rhs);
  /**
   * Binary operator <
   * @param lhs A NumericExpression representing the left-hand side of the comparison
   * @param rhs A NumericExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs < rhs`
   */
  friend BooleanExpression operator<(const NumericExpression &lhs, const NumericExpression &rhs);
  /**
   * Binary operator <=
   * @param lhs A NumericExpression representing the left-hand side of the comparison
   * @param rhs A NumericExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs <= rhs`
   */
  friend BooleanExpression operator<=(const NumericExpression &lhs, const NumericExpression &rhs);
  /**
   * Binary operator ==
   * @param lhs A NumericExpression representing the left-hand side of the comparison
   * @param rhs A NumericExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs == rhs`
   */
  friend BooleanExpression operator==(const NumericExpression &lhs, const NumericExpression &rhs);
  /**
   * Binary operator !=
   * @param lhs A NumericExpression representing the left-hand side of the comparison
   * @param rhs A NumericExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs != rhs`
   */
  friend BooleanExpression operator!=(const NumericExpression &lhs, const NumericExpression &rhs);
  /**
   * Binary operator >=
   * @param lhs A NumericExpression representing the left-hand side of the comparison
   * @param rhs A NumericExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs >= rhs`
   */
  friend BooleanExpression operator>=(const NumericExpression &lhs, const NumericExpression &rhs);
  /**
   * Binary operator >
   * @param lhs A NumericExpression representing the left-hand side of the comparison
   * @param rhs A NumericExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs > rhs`
   */
  friend BooleanExpression operator>(const NumericExpression &lhs, const NumericExpression &rhs);
};

class StringExpression : public Expression {
public:
  /**
   * Constructor. Used internally.
   */
  explicit StringExpression(std::shared_ptr<impl::StringExpressionImpl> impl);
  /**
   * Implicit conversion from const char *.
   * @param value The value
   */
  StringExpression(const char *value);  // NOLINT(google-explicit-constructor)
  /**
   * Implicit conversion from std::string_view
   * @param value The value
   */
  StringExpression(std::string_view value);  // NOLINT(google-explicit-constructor)
  /**
   * Implicit conversion from std::string
   * @param value The value
   */
  StringExpression(std::string value);  // NOLINT(google-explicit-constructor)
  /**
   * Move constructor.
   */
  StringExpression(StringExpression &&) = default;
  /**
   * Move assignment operator.
   */
  StringExpression &operator=(StringExpression &&) = default;
  /**
   * Destructor.
   */
  ~StringExpression() override;

  /**
   * Builds a fluent expression representing whether object "starts with" another StringExpression.
   * @param other The other StringExpression.
   * @return a BooleanExpression expression representing this object "starts with" `other`.
   */
  BooleanExpression startsWith(const StringExpression &other) const;
  /**
   * Builds a fluent expression representing whether object "ends with" another StringExpression.
   * @param other The other StringExpression.
   * @return a BooleanExpression expression representing this object "ends with" `other`.
   */
  BooleanExpression endsWith(const StringExpression &other) const;
  /**
   * Builds a fluent expression representing whether object "contains" another StringExpression.
   * @param other The other StringExpression.
   * @return a BooleanExpression expression representing this object "contains" `other`.
   */
  BooleanExpression contains(const StringExpression &other) const;
  /**
   * Builds a fluent expression representing whether object "matches" another StringExpression.
   * @param other The other StringExpression, whose representation should be a Java regular expression.
   * @return a BooleanExpression expression representing this object "matches" `other`.
   */
  BooleanExpression matches(const StringExpression &other) const;

  /**
   * Used internally
   */
  std::shared_ptr<impl::StringExpressionImpl> implAsStringExpressionImpl() const;

  /**
   * Binary operator + (string concatenation)
   * @param lhs A StringExpression representing the left-hand side of the concatenation
   * @param rhs A StringExpression representing the right-hand side of the concatenation
   * @return The StringExpression representing the concatenation `lhs + rhs`
   */
  friend StringExpression operator+(const StringExpression &lhs, const StringExpression &rhs);
  /**
   * Binary operator <
   * @param lhs A StringExpression representing the left-hand side of the comparison
   * @param rhs A StringExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs < rhs`
   */
  friend BooleanExpression operator<(const StringExpression &lhs, const StringExpression &rhs);
  /**
   * Binary operator <=
   * @param lhs A StringExpression representing the left-hand side of the comparison
   * @param rhs A StringExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs <= rhs`
   */
  friend BooleanExpression operator<=(const StringExpression &lhs, const StringExpression &rhs);
  /**
   * Binary operator ==
   * @param lhs A StringExpression representing the left-hand side of the comparison
   * @param rhs A StringExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs == rhs`
   */
  friend BooleanExpression operator==(const StringExpression &lhs, const StringExpression &rhs);
  /**
   * Binary operator !=
   * @param lhs A StringExpression representing the left-hand side of the comparison
   * @param rhs A StringExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs != rhs`
   */
  friend BooleanExpression operator!=(const StringExpression &lhs, const StringExpression &rhs);
  /**
   * Binary operator >=
   * @param lhs A StringExpression representing the left-hand side of the comparison
   * @param rhs A StringExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs >= rhs`
   */
  friend BooleanExpression operator>=(const StringExpression &lhs, const StringExpression &rhs);
  /**
   * Binary operator >
   * @param lhs A StringExpression representing the left-hand side of the comparison
   * @param rhs A StringExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs > rhs`
   */
  friend BooleanExpression operator>(const StringExpression &lhs, const StringExpression &rhs);
};

class DateTimeExpression : public Expression {
  typedef deephaven::client::highlevel::DateTime DateTime;

public:
  /**
   * Constructor. Used internally.
   */
  explicit DateTimeExpression(std::shared_ptr<impl::DateTimeExpressionImpl> impl);
  /**
   * Implicit conversion from const char *.
   * @param value The value
   */
  DateTimeExpression(const char *value);  // NOLINT(google-explicit-constructor)
  /**
   * Implicit conversion from std::string_view
   * @param value The value
   */
  DateTimeExpression(std::string_view value);  // NOLINT(google-explicit-constructor)
  /**
   * Implicit conversion from std::string
   * @param value The value
   */
  DateTimeExpression(std::string value);  // NOLINT(google-explicit-constructor)
  /**
   * Implicit conversion from DateTime
   * @param value The value
   */
  DateTimeExpression(const DateTime &value);  // NOLINT(google-explicit-constructor)
  /**
   * Move constructor.
   */
  DateTimeExpression(DateTimeExpression &&) = default;
  /**
   * Move assignment operator.
   */
  DateTimeExpression &operator=(DateTimeExpression &&) = default;
  /**
   * Destructor.
   */
  ~DateTimeExpression() override;

  /**
   * Used internally.
   */
  std::shared_ptr<impl::DateTimeExpressionImpl> implAsDateTimeExpressionImpl() const;

  /**
   * Binary operator <
   * @param lhs A DateTimeExpression representing the left-hand side of the comparison
   * @param rhs A DateTimeExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs < rhs`
   */
  friend BooleanExpression operator<(const DateTimeExpression &lhs, const DateTimeExpression &rhs);
  /**
   * Binary operator <=
   * @param lhs A DateTimeExpression representing the left-hand side of the comparison
   * @param rhs A DateTimeExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs <= rhs`
   */
  friend BooleanExpression operator<=(const DateTimeExpression &lhs, const DateTimeExpression &rhs);
  /**
   * Binary operator ==
   * @param lhs A DateTimeExpression representing the left-hand side of the comparison
   * @param rhs A DateTimeExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs == rhs`
   */
  friend BooleanExpression operator==(const DateTimeExpression &lhs, const DateTimeExpression &rhs);
  /**
   * Binary operator !=
   * @param lhs A DateTimeExpression representing the left-hand side of the comparison
   * @param rhs A DateTimeExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs != rhs`
   */
  friend BooleanExpression operator!=(const DateTimeExpression &lhs, const DateTimeExpression &rhs);
  /**
   * Binary operator >=
   * @param lhs A DateTimeExpression representing the left-hand side of the comparison
   * @param rhs A DateTimeExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs >= rhs`
   */
  friend BooleanExpression operator>=(const DateTimeExpression &lhs, const DateTimeExpression &rhs);
  /**
   * Binary operator >
   * @param lhs A DateTimeExpression representing the left-hand side of the comparison
   * @param rhs A DateTimeExpression representing the right-hand side of the comparison
   * @return The BooleanExpression representing the result of the comparison `lhs > rhs`
   */
  friend BooleanExpression operator>(const DateTimeExpression &lhs, const DateTimeExpression &rhs);
};
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
