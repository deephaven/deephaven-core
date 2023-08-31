/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/columns_impl.h"

namespace deephaven::client {
namespace impl {
std::shared_ptr<ColumnImpl> ColumnImpl::Create(std::string name) {
  return std::make_shared<ColumnImpl>(Private(), std::move(name));
}

ColumnImpl::ColumnImpl(Private, std::string name) : name_(std::move(name)) {}

ColumnImpl::~ColumnImpl() = default;

void ColumnImpl::StreamIrisRepresentation(std::ostream &s) const {
  s << name_;
}

std::shared_ptr<NumColImpl> NumColImpl::Create(std::string name) {
  return std::make_shared<NumColImpl>(Private(), std::move(name));
}

NumColImpl::NumColImpl(Private, std::string name) : ColumnImpl(Private(), std::move(name)) {}

NumColImpl::~NumColImpl() = default;

std::shared_ptr<StrColImpl> StrColImpl::Create(std::string name) {
  return std::make_shared<StrColImpl>(Private(), std::move(name));
}

StrColImpl::StrColImpl(Private, std::string name) : ColumnImpl(Private(), std::move(name)) {}

StrColImpl::~StrColImpl() = default;

std::shared_ptr<DateTimeColImpl> DateTimeColImpl::Create(std::string name) {
  return std::make_shared<DateTimeColImpl>(Private(), std::move(name));
}

DateTimeColImpl::DateTimeColImpl(Private, std::string name) : ColumnImpl(Private(),
    std::move(name)) {}

DateTimeColImpl::~DateTimeColImpl() = default;

std::shared_ptr<AssignedColumnImpl> AssignedColumnImpl::Create(std::string name,
    std::shared_ptr<ExpressionImpl> expr) {
  return std::make_shared<AssignedColumnImpl>(Private(), std::move(name), std::move(expr));
}

AssignedColumnImpl::AssignedColumnImpl(Private, std::string name,
    std::shared_ptr<ExpressionImpl> expr)
    : name_(std::move(name)), expr_(std::move(expr)) {}

AssignedColumnImpl::~AssignedColumnImpl() = default;

void AssignedColumnImpl::StreamIrisRepresentation(std::ostream &s) const {
  s << name_;
  s << " = ";
  expr_->StreamIrisRepresentation(s);
}
}  // namespace impl
}  // namespace deephaven::client
