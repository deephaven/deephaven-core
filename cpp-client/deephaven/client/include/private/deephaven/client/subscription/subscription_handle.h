/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

namespace deephaven::client::subscription {
class SubscriptionHandle {
public:
  virtual ~SubscriptionHandle() = default;
  /**
   * Cancels the subscription.
   * Waits for the internal subcription thread to be torn down.
   */
  virtual void cancel() = 0;
};
}  // namespace deephaven::client::subscription
