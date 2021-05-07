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
 *
 * This file was modified by Deephaven Data Labs.
 *
 */

#ifndef JPY_JOBJ_H
#define JPY_JOBJ_H

#ifdef __cplusplus
extern "C" {
#endif

#include "jpy_compat.h"

/**
 * The Java Object representation in Python.
 * @see JPy_JArray
 */
typedef struct JPy_JObj
{
    PyObject_HEAD
    jobject objectRef;
}
JPy_JObj;


int JObj_Check(PyObject* arg);

PyObject* JObj_New(JNIEnv* jenv, jobject objectRef);
PyObject* JObj_FromType(JNIEnv* jenv, JPy_JType* type, jobject objectRef);

int JObj_InitTypeSlots(PyTypeObject* type, const char* typeName, PyTypeObject* superType);


#ifdef __cplusplus
}  /* extern "C" */
#endif
#endif /* !JPY_JOBJ_H */