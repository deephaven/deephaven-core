/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/impl/aggregate_impl.h"

namespace deephaven::client {
namespace impl {
std::shared_ptr<AggregateImpl> AggregateImpl::Create(
    ComboAggregateRequest::Aggregate descriptor) {
  return std::make_shared<AggregateImpl>(Private(), std::move(descriptor));
}

AggregateImpl::AggregateImpl(Private, ComboAggregateRequest::Aggregate descriptor) :
    descriptor_(std::move(descriptor)) {}

std::shared_ptr<AggregateComboImpl> AggregateComboImpl::Create(
    std::vector<ComboAggregateRequest::Aggregate> aggregates) {
  return std::make_shared<AggregateComboImpl>(Private(), std::move(aggregates));
}

AggregateComboImpl::AggregateComboImpl(Private,
    std::vector<ComboAggregateRequest::Aggregate> aggregates) :
    aggregates_(std::move(aggregates)) {}
}  // namespace impl
}  // namespace deephaven::client
