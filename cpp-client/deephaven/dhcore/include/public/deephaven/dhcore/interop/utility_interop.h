/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include "deephaven/dhcore/interop/interop_util.h"

extern "C" {
void deephaven_dhcore_utility_GetEnv(const char *envname,
    deephaven::dhcore::interop::StringHandle *string_handle,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_dhcore_utility_SetEnv(const char *envname, const char *value,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_dhcore_utility_UnsetEnv(const char *envname,
    deephaven::dhcore::interop::ErrorStatus *status);
}  // extern "C"
