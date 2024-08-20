/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

namespace deephaven::client::subscription {
class SubscriptionHandle {
public:
  virtual ~SubscriptionHandle() = default;
  /**
   * Cancels the subscription and waits for the corresponding thread to die.
   */
  virtual void Cancel() = 0;
};
}  // namespace deephaven::client::subscription
