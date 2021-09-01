#include "deephaven/client/highlevel/impl/aggregate_impl.h"

namespace deephaven {
namespace client {
namespace highlevel {
namespace impl {

std::shared_ptr<AggregateImpl> AggregateImpl::create(
    ComboAggregateRequest::Aggregate descriptor) {
  return std::make_shared<AggregateImpl>(Private(), std::move(descriptor));
}

AggregateImpl::AggregateImpl(Private, ComboAggregateRequest::Aggregate descriptor) :
    descriptor_(std::move(descriptor)) {}

std::shared_ptr<AggregateComboImpl> AggregateComboImpl::create(
    std::vector<ComboAggregateRequest::Aggregate> aggregates) {
  return std::make_shared<AggregateComboImpl>(Private(), std::move(aggregates));
}

AggregateComboImpl::AggregateComboImpl(Private,
    std::vector<ComboAggregateRequest::Aggregate> aggregates) :
    aggregates_(std::move(aggregates)) {}
}  // namespace impl
}  // namespace highlevel
}  // namespace client
}  // namespace deephaven
