/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/interop/utility_interop.h"

#include "deephaven/dhcore/interop/interop_util.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::dhcore::interop::ErrorStatus;
using deephaven::dhcore::interop::StringHandle;
using deephaven::dhcore::interop::StringPoolBuilder;
using deephaven::dhcore::interop::StringPoolHandle;
using deephaven::dhcore::utility::GetEnv;
using deephaven::dhcore::utility::SetEnv;
using deephaven::dhcore::utility::UnsetEnv;

extern "C" {
void deephaven_dhcore_utility_GetEnv(const char *envname,
    StringHandle *string_handle, StringPoolHandle *string_pool_handle, ErrorStatus *status) {
  status->Run([=]() {
    StringPoolBuilder builder;
    auto result = GetEnv(envname);
    if (result.has_value()) {
      *string_handle = builder.Add(*result);
    }
    // If envname is not found in the environment, caller will see an empty builder
    *string_pool_handle = builder.Build();
  });
}

void deephaven_dhcore_utility_SetEnv(const char *envname, const char *value, ErrorStatus *status) {
  status->Run([=]() {
    SetEnv(envname, value);
  });
}

void deephaven_dhcore_utility_UnsetEnv(const char *envname, ErrorStatus *status) {
  status->Run([=]() {
    UnsetEnv(envname);
  });
}
}  // extern "C"
