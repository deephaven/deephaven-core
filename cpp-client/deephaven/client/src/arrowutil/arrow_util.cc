/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/arrowutil/arrow_util.h"

namespace deephaven::client::arrowutil {
bool ArrowUtil::tryConvertTicketToFlightDescriptor(const std::string &ticket,
    arrow::flight::FlightDescriptor *fd) {
  if (ticket.length() != 5 || ticket[0] != 'e') {
    return false;
  }
  uint32_t value;
  memcpy(&value, ticket.data() + 1, sizeof(uint32_t));
  *fd = arrow::flight::FlightDescriptor::Path({"export", std::to_string(value)});
  return true;
};
}  // namespace deephaven::client::arrowutil
