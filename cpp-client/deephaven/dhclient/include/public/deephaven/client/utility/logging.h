/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#pragma once

namespace deephaven::client::utility {
/**
 * Initializes Abseil log verbosity based on the GRPC_VERBOSITY environment variable,
 * applying to all source files in the calling project. Recognized values are DEBUG, INFO,
 * WARNING, ERROR, and NONE (case-insensitive). Defaults to WARNING if unset. Safe to call
 * multiple times; initialization runs only once. Automatically called by Client::Connect(),
 * but downstream libraries may call it earlier for finer control.
 *
 * Note: if the application has not called absl::InitializeLog() before the first log message
 * is emitted, Abseil will print a one-time notice to stderr:
 *   "WARNING: All log messages before absl::InitializeLog() is called are written to STDERR"
 * This is harmless -- log output is unaffected. To suppress it, call absl::InitializeLog()
 * once from main() before any library initialization (following Abseil's intended usage).
 * This library intentionally does not call it, following the same convention as gRPC.
 */
void log_verbosity_init();

/**
* Putting this static variable here means that anyone who includes logging.h will automatically
* call log_verbosity_init() at static initialization time. Each translation unit that includes this file
* will include its own copy of this static variable. However, this is fine because log_verbosity_init()
* has a call_once style of implementation.
*/
static bool log_verbosity_init_flag = (log_verbosity_init(), true);

}  // namespace deephaven::client::utility

