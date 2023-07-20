/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/update_by_operation_impl.h"

namespace deephaven::client::impl {
UpdateByOperationImpl::UpdateByOperationImpl(GrpcUpdateByOperation grpcOp) :
    grpcOp_(std::move(grpcOp)) {}
UpdateByOperationImpl::UpdateByOperationImpl(UpdateByOperationImpl &&other) noexcept = default;
UpdateByOperationImpl &UpdateByOperationImpl::operator=(UpdateByOperationImpl &&other) noexcept = default;
UpdateByOperationImpl::~UpdateByOperationImpl() = default;
}  // namespace deephaven::client::impl
