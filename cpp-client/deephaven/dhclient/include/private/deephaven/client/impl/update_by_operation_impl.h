/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include "deephaven_core/proto/table.pb.h"
#include "deephaven_core/proto/table.grpc.pb.h"

namespace deephaven::client::impl {
class UpdateByOperationImpl {
  using UpdateByOperationProto = io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation;
public:
  explicit UpdateByOperationImpl(UpdateByOperationProto grpc_op);
  UpdateByOperationImpl(UpdateByOperationImpl &&other) noexcept;
  UpdateByOperationImpl &operator=(UpdateByOperationImpl &&other) noexcept;
  ~UpdateByOperationImpl();

  [[nodiscard]] const UpdateByOperationProto &UpdateByProto() const { return updateByProto_; }

private:
  UpdateByOperationProto updateByProto_;
};
}  // namespace deephaven::client::impl
