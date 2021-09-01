#include "deephaven/client/highlevel/impl/numeric_expression_impl.h"

#include <iomanip>
#include "deephaven/client/highlevel/impl/boolean_expression_impl.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::utility::SimpleOstringstream;

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {
namespace {
class NumericUnaryOperatorImpl final : public NumericExpressionImpl {
public:
  NumericUnaryOperatorImpl(char op, std::shared_ptr<NumericExpressionImpl> &&child) : op_(op),
      child_(std::move(child)) {}
  ~NumericUnaryOperatorImpl() final;

  void streamIrisRepresentation(std::ostream &s) const final;

  char op() const { return op_; }
  const std::shared_ptr<NumericExpressionImpl> &child() const { return child_; }

private:
  char op_ = 0;
  std::shared_ptr<NumericExpressionImpl> child_;
};

class NumericBinaryOperatorImpl final : public NumericExpressionImpl {
public:
  NumericBinaryOperatorImpl(std::shared_ptr<NumericExpressionImpl> &&lhs, char op,
      std::shared_ptr<NumericExpressionImpl> &&rhs) : lhs_(std::move(lhs)), op_(op), rhs_(std::move(rhs)) {}
  ~NumericBinaryOperatorImpl() final;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  std::shared_ptr<NumericExpressionImpl> lhs_;
  char op_ = 0;
  std::shared_ptr<NumericExpressionImpl> rhs_;
};

class NumericComparisonOperatorImpl final : public BooleanExpressionImpl {
public:
  NumericComparisonOperatorImpl(std::shared_ptr<NumericExpressionImpl> &&lhs, const char *op,
      std::shared_ptr<NumericExpressionImpl> &&rhs) : lhs_(std::move(lhs)), op_(op), rhs_(std::move(rhs)) {}
  ~NumericComparisonOperatorImpl() final;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  std::shared_ptr<NumericExpressionImpl> lhs_;
  const char *op_ = nullptr;
  std::shared_ptr<NumericExpressionImpl> rhs_;
};

class Int64LiteralImpl final : public NumericExpressionImpl {
public:
  Int64LiteralImpl(int64_t value) : value_(value) {}
  ~Int64LiteralImpl() final;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  int64_t value_ = 0;
};

class DoubleLiteralImpl final : public NumericExpressionImpl {
public:
  DoubleLiteralImpl(double value) : value_(value) {}
  ~DoubleLiteralImpl() final;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  double value_ = 0;
};
}  // namespace

std::shared_ptr<NumericExpressionImpl> NumericExpressionImpl::createUnaryOperator(char op,
    std::shared_ptr<NumericExpressionImpl> child) {
  if (op == '+') {
    // Strip leading unary +
    return child;
  }

  if (op == '-') {
    const auto *childAsUnary = dynamic_cast<const NumericUnaryOperatorImpl*>(child.get());
    if (childAsUnary != nullptr && childAsUnary->op() == '-') {
      return childAsUnary->child();
    }
  }

  return std::make_shared<NumericUnaryOperatorImpl>(op, std::move(child));
}

std::shared_ptr<NumericExpressionImpl> NumericExpressionImpl::createBinaryOperator(
    std::shared_ptr<NumericExpressionImpl> lhs, char op, std::shared_ptr<NumericExpressionImpl> rhs) {
  return std::make_shared<NumericBinaryOperatorImpl>(std::move(lhs), op, std::move(rhs));
}

std::shared_ptr<BooleanExpressionImpl> NumericExpressionImpl::createComparisonOperator(
    std::shared_ptr<NumericExpressionImpl> lhs, const char *op, std::shared_ptr<NumericExpressionImpl> rhs) {
  return std::make_shared<NumericComparisonOperatorImpl>(std::move(lhs), op, std::move(rhs));
}

std::shared_ptr<NumericExpressionImpl> NumericExpressionImpl::createInt64Literal(int64_t value) {
  return std::make_shared<Int64LiteralImpl>(value);
}

std::shared_ptr<NumericExpressionImpl> NumericExpressionImpl::createDoubleLiteral(double value) {
  return std::make_shared<DoubleLiteralImpl>(value);
}

NumericExpressionImpl::~NumericExpressionImpl() = default;

namespace {
NumericUnaryOperatorImpl::~NumericUnaryOperatorImpl() = default;
void NumericUnaryOperatorImpl::streamIrisRepresentation(std::ostream &s) const {
  s << op_;
  child_->streamIrisRepresentation(s);
}

NumericBinaryOperatorImpl::~NumericBinaryOperatorImpl() = default;
void NumericBinaryOperatorImpl::streamIrisRepresentation(std::ostream &s) const {
  lhs_->streamIrisRepresentation(s);
  s << op_;
  rhs_->streamIrisRepresentation(s);
}

NumericComparisonOperatorImpl::~NumericComparisonOperatorImpl() = default;
void NumericComparisonOperatorImpl::streamIrisRepresentation(std::ostream &s) const {
  lhs_->streamIrisRepresentation(s);
  s << op_;
  rhs_->streamIrisRepresentation(s);
}

Int64LiteralImpl::~Int64LiteralImpl() = default;
void Int64LiteralImpl::streamIrisRepresentation(std::ostream &s) const {
  s << value_;
}

DoubleLiteralImpl::~DoubleLiteralImpl() = default;
void DoubleLiteralImpl::streamIrisRepresentation(std::ostream &s) const {
  // To avoid messing with the state flags of 's'
  SimpleOstringstream oss;
  oss << std::setprecision(std::numeric_limits<double>::max_digits10) << value_;
  s << oss.str();
}
}  // namespace
}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
