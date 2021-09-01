#pragma once

#include <memory>
#include <absl/strings/string_view.h>
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

class Expression {
public:
  explicit Expression(std::shared_ptr<impl::ExpressionImpl> impl);
  Expression(const Expression &) = delete;
  Expression &operator=(const Expression &) = delete;
  Expression(Expression &&) = default;
  Expression &operator=(Expression &&) = default;
  virtual ~Expression();

  BooleanExpression isNull() const;
  AssignedColumn as(std::string columnName) const;

  const std::shared_ptr<impl::ExpressionImpl> &implAsExpressionImpl() const {
    return impl_;
  }

protected:
  std::shared_ptr<impl::ExpressionImpl> impl_;
};

class BooleanExpression final : public Expression {
public:
  explicit BooleanExpression(std::shared_ptr<impl::BooleanExpressionImpl> impl);
  BooleanExpression(BooleanExpression &&) = default;
  BooleanExpression &operator=(BooleanExpression &&) = default;
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
  explicit NumericExpression(std::shared_ptr<impl::NumericExpressionImpl> impl);
  // TODO(kosak): more numeric types, or a template maybe
  NumericExpression(int value);  // NOLINT(google-explicit-constructor)
  NumericExpression(long value);  // NOLINT(google-explicit-constructor)
  NumericExpression(float value);  // NOLINT(google-explicit-constructor)
  NumericExpression(double value);  // NOLINT(google-explicit-constructor)
  NumericExpression(NumericExpression &&) = default;
  NumericExpression &operator=(NumericExpression &&) = default;
  ~NumericExpression() override;

  std::shared_ptr<impl::NumericExpressionImpl> implAsNumericExpressionImpl() const;

  // Unary operators
  friend NumericExpression operator+(const NumericExpression &item);
  friend NumericExpression operator-(const NumericExpression &item);
  friend NumericExpression operator~(const NumericExpression &item);
  // Binary operators
  friend NumericExpression operator+(const NumericExpression &lhs, const NumericExpression &rhs);
  friend NumericExpression operator-(const NumericExpression &lhs, const NumericExpression &rhs);
  friend NumericExpression operator*(const NumericExpression &lhs, const NumericExpression &rhs);
  friend NumericExpression operator/(const NumericExpression &lhs, const NumericExpression &rhs);
  friend NumericExpression operator%(const NumericExpression &lhs, const NumericExpression &rhs);
  friend NumericExpression operator^(const NumericExpression &lhs, const NumericExpression &rhs);
  friend NumericExpression operator|(const NumericExpression &lhs, const NumericExpression &rhs);
  friend NumericExpression operator&(const NumericExpression &lhs, const NumericExpression &rhs);
  friend BooleanExpression operator<(const NumericExpression &lhs, const NumericExpression &rhs);
  friend BooleanExpression operator<=(const NumericExpression &lhs, const NumericExpression &rhs);
  friend BooleanExpression operator==(const NumericExpression &lhs, const NumericExpression &rhs);
  friend BooleanExpression operator!=(const NumericExpression &lhs, const NumericExpression &rhs);
  friend BooleanExpression operator>=(const NumericExpression &lhs, const NumericExpression &rhs);
  friend BooleanExpression operator>(const NumericExpression &lhs, const NumericExpression &rhs);
};

class StringExpression : public Expression {
public:
  explicit StringExpression(std::shared_ptr<impl::StringExpressionImpl> impl);
  StringExpression(const char *value);  // NOLINT(google-explicit-constructor)
  StringExpression(absl::string_view value);  // NOLINT(google-explicit-constructor)
  StringExpression(std::string value);  // NOLINT(google-explicit-constructor)
  StringExpression(StringExpression &&) = default;
  StringExpression &operator=(StringExpression &&) = default;
  ~StringExpression() override;

  BooleanExpression startsWith(const StringExpression &other) const;
  BooleanExpression endsWith(const StringExpression &other) const;
  BooleanExpression contains(const StringExpression &other) const;
  BooleanExpression matches(const StringExpression &other) const;

  std::shared_ptr<impl::StringExpressionImpl> implAsStringExpressionImpl() const;

  friend StringExpression operator+(const StringExpression &lhs, const StringExpression &rhs);
  friend BooleanExpression operator<(const StringExpression &lhs, const StringExpression &rhs);
  friend BooleanExpression operator<=(const StringExpression &lhs, const StringExpression &rhs);
  friend BooleanExpression operator==(const StringExpression &lhs, const StringExpression &rhs);
  friend BooleanExpression operator!=(const StringExpression &lhs, const StringExpression &rhs);
  friend BooleanExpression operator>=(const StringExpression &lhs, const StringExpression &rhs);
  friend BooleanExpression operator>(const StringExpression &lhs, const StringExpression &rhs);
};

class DateTimeExpression : public Expression {
  typedef deephaven::client::highlevel::DBDateTime DBDateTime;

public:
  explicit DateTimeExpression(std::shared_ptr<impl::DateTimeExpressionImpl> impl);
  DateTimeExpression(const char *value);  // NOLINT(google-explicit-constructor)
  DateTimeExpression(absl::string_view value);  // NOLINT(google-explicit-constructor)
  DateTimeExpression(std::string value);  // NOLINT(google-explicit-constructor)
  DateTimeExpression(const DBDateTime &value);  // NOLINT(google-explicit-constructor)
  DateTimeExpression(DateTimeExpression &&) = default;
  DateTimeExpression &operator=(DateTimeExpression &&) = default;
  ~DateTimeExpression() override;

  std::shared_ptr<impl::DateTimeExpressionImpl> implAsDateTimeExpressionImpl() const;

  friend BooleanExpression operator<(const DateTimeExpression &lhs, const DateTimeExpression &rhs);
  friend BooleanExpression operator<=(const DateTimeExpression &lhs, const DateTimeExpression &rhs);
  friend BooleanExpression operator==(const DateTimeExpression &lhs, const DateTimeExpression &rhs);
  friend BooleanExpression operator!=(const DateTimeExpression &lhs, const DateTimeExpression &rhs);
  friend BooleanExpression operator>=(const DateTimeExpression &lhs, const DateTimeExpression &rhs);
  friend BooleanExpression operator>(const DateTimeExpression &lhs, const DateTimeExpression &rhs);
};
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
