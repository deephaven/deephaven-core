/*
 * Copyright 2015 Brockmann Consult GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef JPY_JARRAY_H
#define JPY_JARRAY_H

#ifdef __cplusplus
extern "C" {
#endif

#include "jpy_compat.h"

/**
 * The Java primitive array representation in Python.
 *
 * IMPORTANT: JPy_JArray must only differ from the JPy_JObj structure by the 'bufferExportCount' member
 * since we use the same basic type, name JPy_JType for it. DON'T ever change member positions!
 * @see JPy_JObj
 */
typedef struct JPy_JArray
{
    PyObject_HEAD
    jobject objectRef;
    jint bufferExportCount;
    void *buf;
    char javaType;
    jint bufReadonly;
    jint isCopy;
}
JPy_JArray;

extern PyBufferProcs JArray_as_buffer_boolean;
extern PyBufferProcs JArray_as_buffer_char;
extern PyBufferProcs JArray_as_buffer_byte;
extern PyBufferProcs JArray_as_buffer_short;
extern PyBufferProcs JArray_as_buffer_int;
extern PyBufferProcs JArray_as_buffer_long;
extern PyBufferProcs JArray_as_buffer_float;
extern PyBufferProcs JArray_as_buffer_double;

extern void JArray_ReleaseJavaArrayElements(JPy_JArray* self, char javaType);

#ifdef __cplusplus
}  /* extern "C" */
#endif
#endif /* !JPY_JARRAY_H */
