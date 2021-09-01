#include "deephaven/client/highlevel/impl/string_expression_impl.h"

#include <memory>
#include <vector>
#include "deephaven/client/highlevel/impl/boolean_expression_impl.h"
#include "deephaven/client/highlevel/impl/escape_utils.h"
#include "deephaven/client/highlevel/impl/expression_impl.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::utility::separatedList;

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {
namespace {
class StringLiteralImpl final : public StringExpressionImpl {
  struct Private {};
public:
  explicit StringLiteralImpl(std::string value) : value_(std::move(value)) {}
  ~StringLiteralImpl() final = default;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  std::string value_;
};

class StringConcatImpl final : public StringExpressionImpl {
public:
  static std::shared_ptr<StringExpressionImpl> create(std::shared_ptr<StringExpressionImpl> lhs,
      std::shared_ptr<StringExpressionImpl> rhs);

  explicit StringConcatImpl(std::vector<std::shared_ptr<StringExpressionImpl>> children) :
      children_(std::move(children)) {}
  ~StringConcatImpl() final = default;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  std::vector<std::shared_ptr<StringExpressionImpl>> children_;
};

class StringComparisonImpl final : public BooleanExpressionImpl {
public:
  StringComparisonImpl(std::shared_ptr<StringExpressionImpl> lhs, const char *compareOp,
      std::shared_ptr<StringExpressionImpl> rhs) : lhs_(std::move(lhs)), compareOp_(compareOp),
      rhs_(std::move(rhs)) {}
  ~StringComparisonImpl() final = default;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  std::shared_ptr<StringExpressionImpl> lhs_;
  const char *compareOp_ = nullptr;
  std::shared_ptr<StringExpressionImpl> rhs_;
};
}  // namespace

std::shared_ptr<StringExpressionImpl> StringExpressionImpl::createLiteral(std::string value) {
  return std::make_shared<StringLiteralImpl>(std::move(value));
}

std::shared_ptr<StringExpressionImpl> StringExpressionImpl::createAppend(
    std::shared_ptr<StringExpressionImpl> lhs, std::shared_ptr<StringExpressionImpl> rhs) {
  return StringConcatImpl::create(std::move(lhs), std::move(rhs));
}

std::shared_ptr<BooleanExpressionImpl> StringExpressionImpl::createComparison(
    std::shared_ptr<StringExpressionImpl> lhs, const char *op,
     std::shared_ptr<StringExpressionImpl> rhs) {
  return std::make_shared<StringComparisonImpl>(std::move(lhs), op, std::move(rhs));
}

StringExpressionImpl::~StringExpressionImpl() = default;

namespace {
void StringLiteralImpl::streamIrisRepresentation(std::ostream &s) const {
  s << '`';
  s << EscapeUtils::escapeJava(value_);
  s << '`';
}

std::shared_ptr<StringExpressionImpl> StringConcatImpl::create(
    std::shared_ptr<StringExpressionImpl> lhs, std::shared_ptr<StringExpressionImpl> rhs) {
  std::vector<std::shared_ptr<StringExpressionImpl>> children;
  const auto *lhsAsConcat = dynamic_cast<const StringConcatImpl*>(lhs.get());
  if (lhsAsConcat != nullptr) {
    children.insert(children.end(), lhsAsConcat->children_.begin(), lhsAsConcat->children_.end());
  } else {
    children.push_back(std::move(lhs));
  }

  const auto *rhsAsAnd = dynamic_cast<const StringConcatImpl*>(rhs.get());
  if (rhsAsAnd != nullptr) {
    children.insert(children.end(), rhsAsAnd->children_.begin(), rhsAsAnd->children_.end());
  } else {
    children.push_back(std::move(rhs));
  }

  return std::make_shared<StringConcatImpl>(std::move(children));
}

void StringConcatImpl::streamIrisRepresentation(std::ostream &s) const {
  s << separatedList(children_.begin(), children_.end(), " + ", &streamIris);
}

void StringComparisonImpl::streamIrisRepresentation(std::ostream &s) const {
  s << '(';
  lhs_->streamIrisRepresentation(s);
  s << ' ';
  s << compareOp_;
  s << ' ';
  rhs_->streamIrisRepresentation(s);
  s << ')';
}
}  // namespace
}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
