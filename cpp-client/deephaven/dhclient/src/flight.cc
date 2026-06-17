/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/flight.h"
#include "deephaven/client/impl/table_handle_impl.h"
#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/client/utility/arrow_util.h"
#include "deephaven/client/client_options.h"

using deephaven::client::utility::OkOrThrow;

namespace deephaven::client {
FlightWrapper TableHandleManager::CreateFlightWrapper() const {
  return FlightWrapper(impl_);
}

FlightWrapper::FlightWrapper(std::shared_ptr<impl::TableHandleManagerImpl> impl) : impl_(std::move(impl)) {}
FlightWrapper::~FlightWrapper() = default;

std::unique_ptr<arrow::flight::FlightStreamReader> FlightWrapper::GetFlightStreamReader(
    const TableHandle &table) const {
  arrow::flight::FlightCallOptions options;
  AddHeaders(&options);

  arrow::flight::Ticket tkt;
  tkt.ticket = table.Impl()->Ticket().ticket();

  auto fsr_result = impl_->Server()->FlightClient()->DoGet(options, tkt);
  OkOrThrow(DEEPHAVEN_LOCATION_EXPR(fsr_result));
  return std::move(*fsr_result);
}

void FlightWrapper::AddHeaders(arrow::flight::FlightCallOptions *options) const {
  // Note: Authorization and envoy-prefix headers are automatically added by BearerMiddleware
  // This method now only adds OTHER extra headers (if any)
  impl_->Server()->ForEachHeaderNameAndValue(
      [&options](const std::string &name, const std::string &value) {
        // Skip authorization (handled by middleware)
        if (name == kAuthorizationHeader) {
          return;
        }
        // Skip envoy-prefix (handled by middleware)
        if (name == kEnvoyPrefixHeader) {
          return;
        }
        // Add any other extra headers
        options->headers.emplace_back(name, value);
      }
  );
}

arrow::flight::FlightClient *FlightWrapper::FlightClient() const {
  const auto *server = impl_->Server().get();
  return server->FlightClient();
}
}  // namespace deephaven::client
