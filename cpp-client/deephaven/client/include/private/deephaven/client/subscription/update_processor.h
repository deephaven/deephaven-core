/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <arrow/flight/client.h>
#include "deephaven/client/ticking.h"
#include "deephaven/client/utility/misc.h"

namespace deephaven::client::subscription {
class UpdateProcessor final {
  typedef deephaven::client::utility::ColumnDefinitions ColumnDefinitions;

  struct Private {};
public:
  static std::shared_ptr<UpdateProcessor> startThread(
      std::unique_ptr<arrow::flight::FlightStreamWriter> fsw,
      std::unique_ptr<arrow::flight::FlightStreamReader> fsr,
      std::shared_ptr<ColumnDefinitions> colDefs,
      std::shared_ptr<TickingCallback> callback);

  UpdateProcessor(Private,
      std::unique_ptr<arrow::flight::FlightStreamWriter> fsw,
      std::unique_ptr<arrow::flight::FlightStreamReader> fsr,
    std::shared_ptr<ColumnDefinitions> colDefs, std::shared_ptr<TickingCallback> callback);
  ~UpdateProcessor();

  void cancel(bool wait);

private:
  static void runForever(const std::shared_ptr <UpdateProcessor> &self);
  void runForeverHelper();

public:
  std::unique_ptr<arrow::flight::FlightStreamWriter> fsw_;
  std::unique_ptr<arrow::flight::FlightStreamReader> fsr_;
  std::shared_ptr<ColumnDefinitions> colDefs_;
  std::shared_ptr<TickingCallback> callback_;
  std::atomic<bool> cancelled_;
  std::mutex mutex_;
  std::condition_variable condVar_;
  bool threadAlive_ = false;
};
}  // namespace deephaven::client::subscription
