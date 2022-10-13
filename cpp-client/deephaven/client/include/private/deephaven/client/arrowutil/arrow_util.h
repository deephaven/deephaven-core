/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#pragma once
#include <string>
#include <arrow/type.h>
#include <arrow/flight/types.h>

#include "deephaven/client/types.h"

namespace deephaven::client::arrowutil {
class ArrowUtil {
public:
  static bool tryConvertTicketToFlightDescriptor(const std::string &ticket,
      arrow::flight::FlightDescriptor *fd);
};
}  // namespace deephaven::client::arrowutil
