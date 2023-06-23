/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/arrow_util.h"

#include <ostream>
#include <vector>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/flight/types.h>
#include "deephaven/dhcore/utility/utility.h"

using namespace std;

namespace deephaven::client::utility {
void okOrThrow(const deephaven::dhcore::utility::DebugInfo &debugInfo,
    const arrow::Status &status) {
  if (status.ok()) {
    return;
  }

  auto msg = stringf("Status: %o. Caller: %o", status, debugInfo);
  throw std::runtime_error(msg);
}

arrow::flight::FlightDescriptor convertTicketToFlightDescriptor(const std::string &ticket) {
  if (ticket.length() != 5 || ticket[0] != 'e') {
    const char *message = "Ticket is not in correct format for export";
    throw std::runtime_error(DEEPHAVEN_DEBUG_MSG(message));
  }
  uint32_t value;
  memcpy(&value, ticket.data() + 1, sizeof(uint32_t));
  return arrow::flight::FlightDescriptor::Path({"export", std::to_string(value)});
};
}  // namespace deephaven::client::utility
