/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <memory>
#include <string_view>
#include <arrow/flight/client.h>
#include "deephaven/client/client.h"

namespace deephaven::client {
/**
 * This class provides an interface to Arrow Flight, which is the main way to push data into and
 * get data out of the system.
 */
class FlightWrapper {
public:
  /**
   * Constructor. Used internally.
   */
  explicit FlightWrapper(std::shared_ptr<impl::TableHandleManagerImpl> impl);
  /**
   * Destructor
   */
  ~FlightWrapper();

  /**
   * Construct an Arrow FlightStreamReader that is set up to read the given TableHandle.
   * @param table The table to read from.
   * @return An Arrow FlightStreamReader
   */
  std::shared_ptr<arrow::flight::FlightStreamReader> getFlightStreamReader(
      const TableHandle &table) const;

  /**
   * Add Deephaven authentication headers to Arrow FlightCallOptions.
   * This is a bit of a hack, and is used in the scenario where the caller is rolling
   * their own Arrow Flight `DoPut` operation. Example code might look like this:
   * @code
   *   // Get a FlightWrapper
   *   auto wrapper = manager.createFlightWrapper();
   *   // Get a
   *   auto [result, fd] = manager.newTableHandleAndFlightDescriptor();
   *   // Empty FlightCallOptions
   *   arrow::flight::FlightCallOptions options;
   *   // add Deephaven auth headers to the FlightCallOptions
   *   wrapper.addAuthHeaders(&options);
   *   std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
   *   std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;
   *   auto status = wrapper.flightClient()->DoPut(options, fd, schema, &fsw, &fmr);
   * @endcode
   * @param options Destination object where the authentication headers should be written.
   */
  void addAuthHeaders(arrow::flight::FlightCallOptions *options) const;

  /**
   * Gets the underlying FlightClient
   * @return A pointer to the FlightClient.
   */
  arrow::flight::FlightClient *flightClient() const;

private:
  std::shared_ptr<impl::TableHandleManagerImpl> impl_;
};

/**
 * The return type for TableHandleManager::newTableHandleAndFlightDescriptor(), defined in
 * deephaven/client/client.h.
 */
class TableHandleAndFlightDescriptor {
public:
  TableHandleAndFlightDescriptor(TableHandle tableHandle,
      arrow::flight::FlightDescriptor flightDescriptor);
  TableHandleAndFlightDescriptor(TableHandleAndFlightDescriptor &&other) noexcept;
  TableHandleAndFlightDescriptor &operator=(TableHandleAndFlightDescriptor &&other) noexcept;
  ~TableHandleAndFlightDescriptor();

  TableHandle &tableHandle() { return tableHandle_; }
  const TableHandle &tableHandle() const { return tableHandle_; }

  arrow::flight::FlightDescriptor &flightDescriptor() { return flightDescriptor_; }
  const arrow::flight::FlightDescriptor &flightDescriptor() const { return flightDescriptor_; }

  TableHandle tableHandle_;
  arrow::flight::FlightDescriptor flightDescriptor_;
};
}  // namespace deephaven::client
