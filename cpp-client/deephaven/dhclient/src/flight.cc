/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/flight.h"
#include "deephaven/client/impl/table_handle_impl.h"
#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/client/utility/arrow_util.h"

using deephaven::client::utility::OkOrThrow;

namespace deephaven::client {
FlightWrapper TableHandleManager::CreateFlightWrapper() const {
  return FlightWrapper(impl_);
}

FlightWrapper::FlightWrapper(std::shared_ptr<impl::TableHandleManagerImpl> impl) : impl_(std::move(impl)) {}
FlightWrapper::~FlightWrapper() = default;

std::shared_ptr<arrow::flight::FlightStreamReader> FlightWrapper::GetFlightStreamReader(
    const TableHandle &table) const {
  arrow::flight::FlightCallOptions options;
  AddHeaders(&options);

  std::unique_ptr<arrow::flight::FlightStreamReader> fsr;
  arrow::flight::Ticket tkt;
  tkt.ticket = table.Impl()->Ticket().ticket();

  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(impl_->Server()->FlightClient()->DoGet(options, tkt, &fsr)));
  return fsr;
}

void FlightWrapper::AddHeaders(arrow::flight::FlightCallOptions *options) const {
  impl_->Server()->ForEachHeaderNameAndValue(
      [&options](const std::string &name, const std::string &value) {
        options->headers.emplace_back(name, value);
      }
  );
}

arrow::flight::FlightClient *FlightWrapper::FlightClient() const {
  const auto *server = impl_->Server().get();
  return server->FlightClient();
}
}  // namespace deephaven::client
