/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <arrow/flight/client.h>
#include "deephaven/client/ticking.h"
#include "deephaven/client/utility/misc.h"

namespace deephaven::client::subscription {
class UpdateProcessor final {
  typedef deephaven::client::utility::ColumnDefinitions ColumnDefinitions;

  struct Private {};
public:
  static std::shared_ptr<UpdateProcessor> startThread(
      std::unique_ptr<arrow::flight::FlightStreamReader> fsr,
      std::shared_ptr<ColumnDefinitions> colDefs,
      std::shared_ptr<TickingCallback> callback,
      bool wantImmer);

  UpdateProcessor(std::unique_ptr<arrow::flight::FlightStreamReader> fsr,
    std::shared_ptr<ColumnDefinitions> colDefs, std::shared_ptr<TickingCallback> callback);
  ~UpdateProcessor();

  void cancel();

private:
  static void runForever(const std::shared_ptr <UpdateProcessor> &self, bool wantImmer);
  void classicRunForeverHelper();
  void immerRunForeverHelper();

public:
  std::unique_ptr<arrow::flight::FlightStreamReader> fsr_;
  std::shared_ptr<ColumnDefinitions> colDefs_;
  std::shared_ptr<TickingCallback> callback_;
};
}  // namespace deephaven::client::subscription
