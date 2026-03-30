/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/utility/logging.h"

#include <cstdlib>
#include <mutex>

#include <absl/log/globals.h>
#include <absl/log/log.h>
#include <absl/strings/match.h>
#include <absl/strings/string_view.h>

namespace deephaven::client::utility {
void log_verbosity_init() {
  static std::once_flag once;
  std::call_once(once, []() {
    const char *verbosity_env = std::getenv("GRPC_VERBOSITY");
    absl::string_view verbosity = verbosity_env != nullptr ? verbosity_env : "";
    if (absl::EqualsIgnoreCase(verbosity, "DEBUG")) {
      absl::SetVLogLevel("*", 2);
      absl::SetMinLogLevel(absl::LogSeverityAtLeast::kInfo);
    } else if (absl::EqualsIgnoreCase(verbosity, "INFO")) {
      absl::SetVLogLevel("*", -1);
      absl::SetMinLogLevel(absl::LogSeverityAtLeast::kInfo);
    } else if (absl::EqualsIgnoreCase(verbosity, "ERROR")) {
      absl::SetVLogLevel("*", -1);
      absl::SetMinLogLevel(absl::LogSeverityAtLeast::kError);
    } else if (absl::EqualsIgnoreCase(verbosity, "NONE")) {
      absl::SetVLogLevel("*", -1);
      absl::SetMinLogLevel(absl::LogSeverityAtLeast::kInfinity);
    } else if (verbosity.empty() || absl::EqualsIgnoreCase(verbosity, "WARNING")) {
      // Default: WARNING when GRPC_VERBOSITY is not set.
      absl::SetVLogLevel("*", -1);
      absl::SetMinLogLevel(absl::LogSeverityAtLeast::kWarning);
    } else {
      LOG(ERROR) << "Unknown GRPC_VERBOSITY value: " << verbosity;
      absl::SetVLogLevel("*", -1);
      absl::SetMinLogLevel(absl::LogSeverityAtLeast::kWarning);
    }
  });
}
}  // namespace deephaven::client::utility

