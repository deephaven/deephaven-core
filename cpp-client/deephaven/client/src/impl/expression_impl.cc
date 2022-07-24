/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/expression_impl.h"

#include <memory>
#include "deephaven/client/impl/boolean_expression_impl.h"

namespace deephaven::client::impl {
namespace {
class IsNullExpressionImpl final : public BooleanExpressionImpl {
public:
  explicit IsNullExpressionImpl(std::shared_ptr<impl::ExpressionImpl> impl) : impl_(std::move(impl)) {}
  IsNullExpressionImpl(const IsNullExpressionImpl &) = delete;
  IsNullExpressionImpl &operator=(const IsNullExpressionImpl &) = delete;
  ~IsNullExpressionImpl() final;

  void streamIrisRepresentation(std::ostream &s) const final;

private:
  std::shared_ptr<impl::ExpressionImpl> impl_;
};
}  // namespace

IrisRepresentableImpl::~IrisRepresentableImpl() = default;
//std::string IrisRepresentableImpl::toIrisRepresentation() const {
//  std::string result;
//  appendIrisRepresentation(&result);
//  return result;
//}

std::shared_ptr<BooleanExpressionImpl> ExpressionImpl::createIsNull(
    std::shared_ptr<ExpressionImpl> impl) {
  return std::make_shared<IsNullExpressionImpl>(std::move(impl));
}

ExpressionImpl::~ExpressionImpl() = default;

namespace {
IsNullExpressionImpl::~IsNullExpressionImpl() = default;
void IsNullExpressionImpl::streamIrisRepresentation(std::ostream &s) const {
  s << "isNull(";
  impl_->streamIrisRepresentation(s);
  s << ')';
}
}  // namespace

void streamIris(std::ostream &s, const std::shared_ptr<IrisRepresentableImpl> &o) {
  o->streamIrisRepresentation(s);
}
}  // namespace deephaven::client::impl
