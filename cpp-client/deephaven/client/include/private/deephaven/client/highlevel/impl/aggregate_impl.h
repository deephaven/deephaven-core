/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include "deephaven/proto/session.pb.h"
#include "deephaven/proto/session.grpc.pb.h"
#include "deephaven/proto/table.pb.h"
#include "deephaven/proto/table.grpc.pb.h"

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {

class AggregateImpl {
  typedef io::deephaven::proto::backplane::grpc::ComboAggregateRequest ComboAggregateRequest;
  struct Private {};

public:
  static std::shared_ptr<AggregateImpl> create(ComboAggregateRequest::Aggregate descriptor);
  AggregateImpl(Private, ComboAggregateRequest::Aggregate descriptor);

  ComboAggregateRequest::Aggregate &descriptor() {
    return descriptor_;
  }

  const ComboAggregateRequest::Aggregate &descriptor() const {
    return descriptor_;
  }

private:
  ComboAggregateRequest::Aggregate descriptor_;
};

class AggregateComboImpl {
  typedef io::deephaven::proto::backplane::grpc::ComboAggregateRequest ComboAggregateRequest;
  struct Private {};

public:
  static std::shared_ptr<AggregateComboImpl> create(
      std::vector<ComboAggregateRequest::Aggregate> aggregates);
  AggregateComboImpl(Private, std::vector<ComboAggregateRequest::Aggregate> aggregates);

  const std::vector<ComboAggregateRequest::Aggregate> &aggregates() const {
    return aggregates_;
  }

private:
  std::vector<ComboAggregateRequest::Aggregate> aggregates_;
};

}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
