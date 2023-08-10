/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/update_by_operation_impl.h"

namespace deephaven::client::impl {
UpdateByOperationImpl::UpdateByOperationImpl(UpdateByOperationProto grpc_op) :
    updateByProto_(std::move(grpc_op)) {}
UpdateByOperationImpl::UpdateByOperationImpl(UpdateByOperationImpl &&other) noexcept = default;
UpdateByOperationImpl &UpdateByOperationImpl::operator=(UpdateByOperationImpl &&other) noexcept = default;
UpdateByOperationImpl::~UpdateByOperationImpl() = default;
}  // namespace deephaven::client::impl
