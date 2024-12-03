/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include "deephaven_core/proto/session.pb.h"
#include "deephaven_core/proto/session.grpc.pb.h"
#include "deephaven_core/proto/table.pb.h"
#include "deephaven_core/proto/table.grpc.pb.h"

namespace deephaven::client::impl {
class AggregateImpl {
  using ComboAggregateRequest = io::deephaven::proto::backplane::grpc::ComboAggregateRequest;
  struct Private {
  };

public:
  [[nodiscard]]
  static std::shared_ptr<AggregateImpl> Create(ComboAggregateRequest::Aggregate descriptor);
  AggregateImpl(Private, ComboAggregateRequest::Aggregate descriptor);

  [[nodiscard]]
  ComboAggregateRequest::Aggregate &Descriptor() {
    return descriptor_;
  }

  [[nodiscard]]
  const ComboAggregateRequest::Aggregate &Descriptor() const {
    return descriptor_;
  }

private:
  ComboAggregateRequest::Aggregate descriptor_;
};

class AggregateComboImpl {
  using ComboAggregateRequest = io::deephaven::proto::backplane::grpc::ComboAggregateRequest;
  struct Private {
  };

public:
  [[nodiscard]]
  static std::shared_ptr<AggregateComboImpl> Create(
      std::vector<ComboAggregateRequest::Aggregate> aggregates);
  AggregateComboImpl(Private, std::vector<ComboAggregateRequest::Aggregate> aggregates);

  [[nodiscard]]
  const std::vector<ComboAggregateRequest::Aggregate> &Aggregates() const {
    return aggregates_;
  }

private:
  std::vector<ComboAggregateRequest::Aggregate> aggregates_;
};
}  // namespace deephaven::client::impl
