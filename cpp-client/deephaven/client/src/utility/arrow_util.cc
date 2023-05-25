/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/arrow_util.h"

#include <ostream>
#include <vector>
#include <arrow/status.h>
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
}  // namespace deephaven::client::utility
