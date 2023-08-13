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
  [[nodiscard]]
  std::shared_ptr<arrow::flight::FlightStreamReader> GetFlightStreamReader(
      const TableHandle &table) const;

  /**
   * Add Deephaven authentication headers, and any other extra headers
   * request at session creation, to Arrow FlightCallOptions.
   *
   * This is a bit of a hack, and is used in the scenario Where the caller is rolling
   * their own Arrow Flight `DoPut` operation. Example code might look like this:
   * @code
   *   // Get a FlightWrapper
   *   auto wrapper = manager.CreateFlightWrapper();
   *   // Get a
   *   auto [result, fd] = manager.newTableHandleAndFlightDescriptor();
   *   // Empty FlightCallOptions
   *   arrow::flight::FlightCallOptions options;
   *   // add Deephaven auth headers to the FlightCallOptions
   *   wrapper.AddHeaders(&options);
   *   std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
   *   std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;
   *   auto status = wrapper.FlightClient()->DoPut(options, fd, Schema, &fsw, &fmr);
   * @endcode
   * @param options Destination object Where the authentication headers should be written.
   */
  void AddHeaders(arrow::flight::FlightCallOptions *options) const;

  /**
   * Gets the underlying FlightClient
   * @return A pointer to the FlightClient.
   */
  [[nodiscard]]
  arrow::flight::FlightClient *FlightClient() const;

private:
  std::shared_ptr<impl::TableHandleManagerImpl> impl_;
};
}  // namespace deephaven::client
