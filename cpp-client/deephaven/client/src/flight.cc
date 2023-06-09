/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/flight.h"
#include "deephaven/client/impl/table_handle_impl.h"
#include "deephaven/client/impl/table_handle_manager_impl.h"
#include "deephaven/client/utility/arrow_util.h"

using deephaven::client::utility::okOrThrow;

namespace deephaven::client {
FlightWrapper TableHandleManager::createFlightWrapper() const {
  return FlightWrapper(impl_);
}

FlightWrapper::FlightWrapper(std::shared_ptr<impl::TableHandleManagerImpl> impl) : impl_(std::move(impl)) {}
FlightWrapper::~FlightWrapper() = default;

std::shared_ptr<arrow::flight::FlightStreamReader> FlightWrapper::getFlightStreamReader(
    const TableHandle &table) const {
  arrow::flight::FlightCallOptions options;
  addHeaders(&options);

  std::unique_ptr<arrow::flight::FlightStreamReader> fsr;
  arrow::flight::Ticket tkt;
  tkt.ticket = table.impl()->ticket().ticket();

  okOrThrow(DEEPHAVEN_EXPR_MSG(impl_->server()->flightClient()->DoGet(options, tkt, &fsr)));
  return fsr;
}

void FlightWrapper::addHeaders(arrow::flight::FlightCallOptions *options) const {
  options->headers.push_back(impl_->server()->getAuthHeader());
  for (auto const & header : impl_->server()->getExtraHeaders()) {
    options->headers.push_back(header);
  }
}

arrow::flight::FlightClient *FlightWrapper::flightClient() const {
  const auto *server = impl_->server().get();
  return server->flightClient();
}

TableHandleAndFlightDescriptor::TableHandleAndFlightDescriptor(TableHandle tableHandle,
    arrow::flight::FlightDescriptor flightDescriptor) : tableHandle_(std::move(tableHandle)),
    flightDescriptor_(std::move(flightDescriptor)) {}
TableHandleAndFlightDescriptor::~TableHandleAndFlightDescriptor() = default;

TableHandleAndFlightDescriptor::TableHandleAndFlightDescriptor(TableHandleAndFlightDescriptor &&other) noexcept = default;
TableHandleAndFlightDescriptor &TableHandleAndFlightDescriptor::operator=(TableHandleAndFlightDescriptor &&other) noexcept = default;
}  // namespace deephaven::client
