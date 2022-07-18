#pragma once

namespace deephaven::client::subscription {
class SubscriptionHandle {
public:
  virtual ~SubscriptionHandle() = default;
  virtual void cancel() = 0;
};
}  // namespace deephaven::client::subscription
