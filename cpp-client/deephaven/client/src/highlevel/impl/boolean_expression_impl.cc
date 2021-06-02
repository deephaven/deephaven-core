#include "deephaven/client/highlevel/impl/boolean_expression_impl.h"

#include <memory>
#include <vector>
#include "deephaven/client/highlevel/impl/expression_impl.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::utility::separatedList;

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {
namespace {
class NotExpressionImpl final : public BooleanExpressionImpl {
  struct Private {};
public:
  static std::shared_ptr<BooleanExpressionImpl> create(std::shared_ptr<BooleanExpressionImpl> child);
  NotExpressionImpl(Private, std::shared_ptr<BooleanExpressionImpl> &&child);
  ~NotExpressionImpl() final;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  std::shared_ptr<BooleanExpressionImpl> child_;
};

class AndExpressionImpl final : public BooleanExpressionImpl {
  struct Private {};
public:
  static std::shared_ptr<BooleanExpressionImpl> create(std::shared_ptr<BooleanExpressionImpl> lhs,
      std::shared_ptr<BooleanExpressionImpl> rhs);
  AndExpressionImpl(Private, std::vector<std::shared_ptr<BooleanExpressionImpl>> &&children);
  ~AndExpressionImpl() final;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  std::vector<std::shared_ptr<BooleanExpressionImpl>> children_;
};

class OrExpressionImpl final : public BooleanExpressionImpl {
  struct Private {};
public:
  static std::shared_ptr<BooleanExpressionImpl> create(std::shared_ptr<BooleanExpressionImpl> lhs,
      std::shared_ptr<BooleanExpressionImpl> rhs);
  OrExpressionImpl(Private, std::vector<std::shared_ptr<BooleanExpressionImpl>> &&children);
  ~OrExpressionImpl() final;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  std::vector<std::shared_ptr<BooleanExpressionImpl>> children_;
};


class BooleanValuedInstanceMethod final : public BooleanExpressionImpl {
  struct Private {};
public:
  static std::shared_ptr<BooleanExpressionImpl> create(std::shared_ptr<ExpressionImpl> lhs,
      std::string method, std::shared_ptr<ExpressionImpl> rhs);

  BooleanValuedInstanceMethod(Private, std::shared_ptr<ExpressionImpl> &&lhs,
      std::string &&method, std::shared_ptr<ExpressionImpl> &&rhs);
  ~BooleanValuedInstanceMethod() final;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  std::shared_ptr<ExpressionImpl> lhs_;
  std::string method_;
  std::shared_ptr<ExpressionImpl> rhs_;
};
}  // namespace

std::shared_ptr<BooleanExpressionImpl> BooleanExpressionImpl::createNot(
    std::shared_ptr<BooleanExpressionImpl> item) {
  return NotExpressionImpl::create(std::move(item));
}

std::shared_ptr<BooleanExpressionImpl> BooleanExpressionImpl::createAnd(
    std::shared_ptr<BooleanExpressionImpl> lhs, std::shared_ptr<BooleanExpressionImpl> rhs) {
  return AndExpressionImpl::create(std::move(lhs), std::move(rhs));
}

std::shared_ptr<BooleanExpressionImpl> BooleanExpressionImpl::createOr(
    std::shared_ptr<BooleanExpressionImpl> lhs, std::shared_ptr<BooleanExpressionImpl> rhs) {
  return OrExpressionImpl::create(std::move(lhs), std::move(rhs));
}

std::shared_ptr<BooleanExpressionImpl> BooleanExpressionImpl::createBooleanValuedInstanceMethod(
    std::shared_ptr<ExpressionImpl> lhs, std::string method, std::shared_ptr<ExpressionImpl> rhs) {
  return BooleanValuedInstanceMethod::create(std::move(lhs), std::move(method), std::move(rhs));
}

BooleanExpressionImpl::~BooleanExpressionImpl() = default;

namespace {
std::shared_ptr<BooleanExpressionImpl> NotExpressionImpl::create(
    std::shared_ptr<BooleanExpressionImpl> child) {
  // not(not(X)) == X
  const auto *notChild = dynamic_cast<const NotExpressionImpl *>(child.get());
  if (notChild != nullptr) {
    return notChild->child_;
  }
  return std::make_shared<NotExpressionImpl>(Private(), std::move(child));
}

NotExpressionImpl::NotExpressionImpl(Private, std::shared_ptr<BooleanExpressionImpl> &&child) :
    child_(std::move(child)) {}
NotExpressionImpl::~NotExpressionImpl() = default;

void NotExpressionImpl::streamIrisRepresentation(std::ostream &s) const {
  s << '!';
  child_->streamIrisRepresentation(s);
}

std::shared_ptr<BooleanExpressionImpl> AndExpressionImpl::create(
    std::shared_ptr<BooleanExpressionImpl> lhs, std::shared_ptr<BooleanExpressionImpl> rhs) {
  std::vector<std::shared_ptr<BooleanExpressionImpl>> children;
  const auto *lhsAsAnd = dynamic_cast<const AndExpressionImpl*>(lhs.get());
  if (lhsAsAnd != nullptr) {
    children.insert(children.end(), lhsAsAnd->children_.begin(), lhsAsAnd->children_.end());
  } else {
    children.push_back(std::move(lhs));
  }

  const auto *rhsAsAnd = dynamic_cast<const AndExpressionImpl*>(rhs.get());
  if (rhsAsAnd != nullptr) {
    children.insert(children.end(), rhsAsAnd->children_.begin(), rhsAsAnd->children_.end());
  } else {
    children.push_back(std::move(rhs));
  }

  return std::make_shared<AndExpressionImpl>(Private(), std::move(children));
}

AndExpressionImpl::AndExpressionImpl(Private,
    std::vector<std::shared_ptr<BooleanExpressionImpl>> &&children) : children_(std::move(children)) {}
AndExpressionImpl::~AndExpressionImpl() = default;

void AndExpressionImpl::streamIrisRepresentation(std::ostream &s) const {
  s << '(';
  s << separatedList(children_.begin(), children_.end(), " && ", &streamIris);
  s << ')';
}

std::shared_ptr<BooleanExpressionImpl> OrExpressionImpl::create(
    std::shared_ptr<BooleanExpressionImpl> lhs, std::shared_ptr<BooleanExpressionImpl> rhs) {
  std::vector<std::shared_ptr<BooleanExpressionImpl>> children;
  const auto *lhsAsOr = dynamic_cast<const OrExpressionImpl*>(lhs.get());
  if (lhsAsOr != nullptr) {
    children.insert(children.end(), lhsAsOr->children_.begin(), lhsAsOr->children_.end());
  } else {
    children.push_back(std::move(lhs));
  }

  const auto *rhsAsOr = dynamic_cast<const OrExpressionImpl*>(lhs.get());
  if (rhsAsOr != nullptr) {
    children.insert(children.end(), rhsAsOr->children_.begin(), rhsAsOr->children_.end());
  } else {
    children.push_back(std::move(rhs));
  }

  return std::make_shared<OrExpressionImpl>(Private(), std::move(children));
}

OrExpressionImpl::OrExpressionImpl(Private,
    std::vector<std::shared_ptr<BooleanExpressionImpl>> &&children) : children_(std::move(children)) {}
OrExpressionImpl::~OrExpressionImpl() = default;

void OrExpressionImpl::streamIrisRepresentation(std::ostream &s) const {
  s << '(';
  s << separatedList(children_.begin(), children_.end(), " || ", &streamIris);
  s << ')';
}

std::shared_ptr<BooleanExpressionImpl> BooleanValuedInstanceMethod::create(
    std::shared_ptr<ExpressionImpl> lhs, std::string method, std::shared_ptr<ExpressionImpl> rhs) {
  return std::make_shared<BooleanValuedInstanceMethod>(Private(), std::move(lhs), std::move(method),
      std::move(rhs));
}

BooleanValuedInstanceMethod::BooleanValuedInstanceMethod(Private,
    std::shared_ptr<ExpressionImpl> &&lhs, std::string &&method,
    std::shared_ptr<ExpressionImpl> &&rhs) : lhs_(std::move(lhs)), method_(std::move(method)),
    rhs_(std::move(rhs)) {}
BooleanValuedInstanceMethod::~BooleanValuedInstanceMethod() = default;

void BooleanValuedInstanceMethod::streamIrisRepresentation(std::ostream &s) const {
  s << '(';
  lhs_->streamIrisRepresentation(s);
  s << '.' << method_ << '(';
  rhs_->streamIrisRepresentation(s);
  s << "))";
}
}  // namespace
}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
