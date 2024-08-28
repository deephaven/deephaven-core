/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/dhcore/interop/testapi/basic_interop_interactions.h"

#include <string>
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/format.h"

using deephaven::dhcore::interop::ErrorStatus;
using deephaven::dhcore::interop::InteropBool;
using deephaven::dhcore::interop::StringHandle;
using deephaven::dhcore::interop::StringPool;
using deephaven::dhcore::interop::StringPoolHandle;
using deephaven::dhcore::interop::StringPoolBuilder;
using deephaven::dhcore::interop::testapi::BasicStruct;
using deephaven::dhcore::interop::testapi::NestedStruct;
using deephaven::dhcore::utility::MakeReservedVector;

extern "C" {
void deephaven_dhcore_interop_testapi_BasicInteropInteractions_Add(
    int32_t a, int32_t b, int32_t *result) {
  *result = a + b;
}

void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddArrays(
    const int32_t *a, const int32_t *b, int32_t length, int32_t *result) {
  for (int32_t i = 0; i != length; ++i) {
    result[i] = a[i] + b[i];
  }
}

void deephaven_dhcore_interop_testapi_BasicInteropInteractions_Xor(
    InteropBool a, InteropBool b, InteropBool *result) {
  *result = InteropBool((bool)a ^ (bool)b);
}

void deephaven_dhcore_interop_testapi_BasicInteropInteractions_XorArrays(
    const InteropBool *a, const InteropBool *b, int32_t length, InteropBool *result) {
  for (int32_t i = 0; i != length; ++i) {
    result[i] = InteropBool((bool)a[i] ^ (bool)b[i]);
  }
}

void deephaven_dhcore_interop_testapi_BasicInteropInteractions_Concat(
    const char *a, const char *b,
    StringHandle *result_handle, StringPoolHandle *string_pool_handle) {
  StringPoolBuilder builder;
  auto text = std::string(a) + b;
  *result_handle = builder.Add(text);
  *string_pool_handle = builder.Build();
}

void deephaven_dhcore_interop_testapi_BasicInteropInteractions_ConcatArrays(
    const char **a, const char **b, int32_t length,
    deephaven::dhcore::interop::StringHandle *result_handles,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle) {
  StringPoolBuilder builder;
  for (int32_t i = 0; i != length; ++i) {
    auto text = std::string(a[i]) + b[i];
    result_handles[i] = builder.Add(text);
  }
  *string_pool_handle = builder.Build();
}

void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddBasicStruct(
    const BasicStruct *a, const BasicStruct *b, BasicStruct *result) {
  *result = a->Add(*b);
}

void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddBasicStructArrays(
    const BasicStruct *a, const BasicStruct *b, int32_t length, BasicStruct *result) {
  for (int32_t i = 0; i != length; ++i) {
    result[i] = a[i].Add(b[i]);
  }
}

void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddNestedStruct(
    const NestedStruct *a, const NestedStruct *b, NestedStruct *result) {
  *result = a->Add(*b);
}

void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddNestedStructArrays(
    const NestedStruct *a, const NestedStruct *b, int32_t length, NestedStruct *result) {
  for (int32_t i = 0; i != length; ++i) {
    result[i] = a[i].Add(b[i]);
  }
}

void deephaven_dhcore_interop_testapi_BasicInteropInteractions_SetErrorIfLessThan(
    int32_t a, int32_t b, ErrorStatus *error_status) {
  error_status->Run([=]() {
    if (a < b) {
      auto message = fmt::format("{} < {}, which is not allowed", a, b);
      throw std::runtime_error(message);
    }
  });
}
}  // extern "C"
