/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include "deephaven/proto/table.pb.h"
#include "deephaven/proto/table.grpc.pb.h"

namespace deephaven::client::impl {
class UpdateByOperationImpl {
  typedef io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation GrpcUpdateByOperation;
public:
  explicit UpdateByOperationImpl(GrpcUpdateByOperation grpcOp);
  UpdateByOperationImpl(UpdateByOperationImpl &&other) noexcept;
  UpdateByOperationImpl &operator=(UpdateByOperationImpl &&other) noexcept;
  ~UpdateByOperationImpl();

  const GrpcUpdateByOperation &grpcOp() const { return grpcOp_; }

private:
  GrpcUpdateByOperation grpcOp_;
};
}  // namespace deephaven::client::impl
