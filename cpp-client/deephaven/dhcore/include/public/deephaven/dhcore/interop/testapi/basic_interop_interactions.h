/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <cstdint>
#include "deephaven/dhcore/interop/interop_util.h"

namespace deephaven::dhcore::interop::testapi {
/**
 * A simple struct that we use to confirm we can pass between C++ and C#.
 */
struct BasicStruct {
  BasicStruct(int32_t i, double d) : i_(i), d_(d) {}

  [[nodiscard]]
  BasicStruct Add(const BasicStruct &other) const {
    return BasicStruct(i_ + other.i_, d_ + other.d_);
  }

  int32_t i_;
  double d_;
};

/**
 * A nested struct that we use to confirm we can pass between C++ and C#.
 */
struct NestedStruct {
  NestedStruct(const BasicStruct &a, const BasicStruct &b) : a_(a), b_(b) {}

  [[nodiscard]]
  NestedStruct Add(const NestedStruct &other) const {
    return NestedStruct(a_.Add(other.a_), b_.Add(other.b_));
  }

  BasicStruct a_;
  BasicStruct b_;
};
}

extern "C" {
/**
 * Adds 'a' and 'b', stores a result in *result.
 * Tests passing int32 back and forth.
 * Note: although interop supports returning a value, we typically always
 * return void and use "out" parameters for everything. We do this for simplicity
 * and consistency: our style is to have zero or more 'out' parameters of simple types
 * and typically one more 'out' parameter for the ErrorStatus struct which contains
 * our exception/error information.
 */
void deephaven_dhcore_interop_testapi_BasicInteropInteractions_Add(
    int32_t a, int32_t b, int32_t *result);

/**
 * Adds the arrays 'a' and 'b' elementwise, stores the results in *result. The caller
 * needs to allocate an array of 'length' elements for 'result'.
 */
void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddArrays(
    const int32_t *a, const int32_t *b, int32_t length, int32_t *result);

/**
 * Performs a XOR b, stores result in *result. Demonstrates our use of InteropBool,
 * because regular C# bool doesn't interop with C++ bool.
 */
void deephaven_dhcore_interop_testapi_BasicInteropInteractions_Xor(
    deephaven::dhcore::interop::InteropBool a,
    deephaven::dhcore::interop::InteropBool b,
    deephaven::dhcore::interop::InteropBool *result);

/**
 * Performs an elementwise XOR of the a and b arrays, stores result in *result.
 * The caller needs to allocate an array of 'length' elements for 'result'.
 */
void deephaven_dhcore_interop_testapi_BasicInteropInteractions_XorArrays(
    const deephaven::dhcore::interop::InteropBool *a,
    const deephaven::dhcore::interop::InteropBool *b,
    int32_t length,
    deephaven::dhcore::interop::InteropBool *result);

/**
 * Concats strings a and b, stores the result in string_pool_handle and result_handle.
 * Demonstrates our StringPool protocol.
 */
void deephaven_dhcore_interop_testapi_BasicInteropInteractions_Concat(
    const char *a, const char *b,
    deephaven::dhcore::interop::StringHandle *result_handle,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle);

/**
 * Concats arrays of strings a and b, elementwise, and stores the results in
 * string_pool_handle and result_handles. The caller needs to allocate an array
 * of 'length' elements for 'result_handles'.
 */
void deephaven_dhcore_interop_testapi_BasicInteropInteractions_ConcatArrays(const char **a,
    const char **b, int32_t length,
    deephaven::dhcore::interop::StringHandle *result_handles,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle);

/**
 * Adds the structs a and b, fieldwise, and stores the result in *result.
 */
void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddBasicStruct(
    const deephaven::dhcore::interop::testapi::BasicStruct *a,
    const deephaven::dhcore::interop::testapi::BasicStruct *b,
    deephaven::dhcore::interop::testapi::BasicStruct *result);

/**
 * Adds arrays of the structs a and b, elementwise, and then fieldwise, and stores the result in
 * *result. The caller needs to allocate an array of 'length' elements for 'result'.
 */
void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddBasicStructArrays(
    const deephaven::dhcore::interop::testapi::BasicStruct *a,
    const deephaven::dhcore::interop::testapi::BasicStruct *b,
    int32_t length,
    deephaven::dhcore::interop::testapi::BasicStruct *result);

/**
 * Adds the structs a and b, fieldwise, and stores the result in *result.
 * a and b are "nested structs" containing other structs.
 */
void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddNestedStruct(
    const deephaven::dhcore::interop::testapi::NestedStruct *a,
    const deephaven::dhcore::interop::testapi::NestedStruct *b,
    deephaven::dhcore::interop::testapi::NestedStruct *result);

/**
 * Adds arrays of the structs a and b, elementwise, and then fieldwise, and stores the result in
 * *result. * a and b are "nested structs" containing other structs. The caller needs to
 * allocate an array of 'length' elements for 'result'.
 */
void deephaven_dhcore_interop_testapi_BasicInteropInteractions_AddNestedStructArrays(
    const deephaven::dhcore::interop::testapi::NestedStruct *a,
    const deephaven::dhcore::interop::testapi::NestedStruct *b,
    int32_t length,
    deephaven::dhcore::interop::testapi::NestedStruct *result);

/**
 * Tests our error protocol. Sets an error if a < b.
 */
void deephaven_dhcore_interop_testapi_BasicInteropInteractions_SetErrorIfLessThan(
    int32_t a, int32_t b,
    deephaven::dhcore::interop::ErrorStatus *error_status);
}  // extern "C"
