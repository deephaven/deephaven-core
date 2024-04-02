/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

namespace deephaven::client::subscription {
class SubscriptionHandle {
public:
  virtual ~SubscriptionHandle() = default;
  /**
   * Cancels the subscription.
   * @param wait If true, waits for the internal subcription thread to be torn down. Use 'true'
   * if you want to be sure that your callback will not be invoked after this call returns.
   */
  virtual void Cancel() = 0;
};
}  // namespace deephaven::client::subscription
