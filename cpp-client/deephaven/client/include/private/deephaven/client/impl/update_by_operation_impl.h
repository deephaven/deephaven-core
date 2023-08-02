/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include "deephaven/proto/table.pb.h"
#include "deephaven/proto/table.grpc.pb.h"

namespace deephaven::client::impl {
class UpdateByOperationImpl {
  typedef io::deephaven::proto::backplane::grpc::UpdateByRequest::UpdateByOperation UpdateByOperationProto;
public:
  explicit UpdateByOperationImpl(UpdateByOperationProto grpcOp);
  UpdateByOperationImpl(UpdateByOperationImpl &&other) noexcept;
  UpdateByOperationImpl &operator=(UpdateByOperationImpl &&other) noexcept;
  ~UpdateByOperationImpl();

  [[nodiscard]] const UpdateByOperationProto &updateByProto() const { return updateByProto_; }

private:
  UpdateByOperationProto updateByProto_;
};
}  // namespace deephaven::client::impl
