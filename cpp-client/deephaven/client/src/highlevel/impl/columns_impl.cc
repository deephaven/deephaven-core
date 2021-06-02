#include "deephaven/client/highlevel/impl/columns_impl.h"

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {
std::shared_ptr<ColumnImpl> ColumnImpl::create(std::string name) {
  return std::make_shared<ColumnImpl>(Private(), std::move(name));
}

ColumnImpl::ColumnImpl(Private, std::string name) : name_(std::move(name)) {}
ColumnImpl::~ColumnImpl() = default;

void ColumnImpl::streamIrisRepresentation(std::ostream &s) const {
  s << name_;
}

std::shared_ptr<NumColImpl> NumColImpl::create(std::string name) {
  return std::make_shared<NumColImpl>(Private(), std::move(name));
}

NumColImpl::NumColImpl(Private, std::string name) : ColumnImpl(Private(), std::move(name)) {}
NumColImpl::~NumColImpl() = default;

std::shared_ptr<StrColImpl> StrColImpl::create(std::string name) {
  return std::make_shared<StrColImpl>(Private(), std::move(name));
}

StrColImpl::StrColImpl(Private, std::string name) : ColumnImpl(Private(), std::move(name)) {}
StrColImpl::~StrColImpl() = default;

std::shared_ptr<DateTimeColImpl> DateTimeColImpl::create(std::string name) {
  return std::make_shared<DateTimeColImpl>(Private(), std::move(name));
}

DateTimeColImpl::DateTimeColImpl(Private, std::string name) : ColumnImpl(Private(), std::move(name)) {}
DateTimeColImpl::~DateTimeColImpl() = default;

std::shared_ptr<AssignedColumnImpl> AssignedColumnImpl::create(std::string name,
    std::shared_ptr<ExpressionImpl> expr) {
  return std::make_shared<AssignedColumnImpl>(Private(), std::move(name), std::move(expr));
}

AssignedColumnImpl::AssignedColumnImpl(Private, std::string name, std::shared_ptr<ExpressionImpl> expr)
    : name_(std::move(name)), expr_(std::move(expr)) {}
AssignedColumnImpl::~AssignedColumnImpl() = default;

void AssignedColumnImpl::streamIrisRepresentation(std::ostream &s) const {
  s << name_;
  s << " = ";
  expr_->streamIrisRepresentation(s);
}
}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
