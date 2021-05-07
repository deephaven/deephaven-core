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

#ifndef JPY_JFIELD_H
#define JPY_JFIELD_H

#ifdef __cplusplus
extern "C" {
#endif

#include "jpy_compat.h"

/**
 * Python object representing a Java method. It's type is 'JMethod'.
 */
typedef struct
{
    PyObject_HEAD

    // The declaring class.
    JPy_JType* declaringClass;
    // Field name.
    PyObject* name;
    // Field type.
    JPy_JType* type;
    // Method is static?
    char isStatic;
    // Method is final?
    char isFinal;
    // Field ID retrieved from JNI.
    jfieldID fid;
}
JPy_JField;

/**
 * The Python 'JMethod' type singleton.
 */
extern PyTypeObject JField_Type;

JPy_JField* JField_New(JPy_JType* declaringType, PyObject* fieldKey, JPy_JType* fieldType, jboolean isStatic, jboolean isFinal, jfieldID fid);
void JField_Del(JPy_JField* field);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* !JPY_JFIELD_H */