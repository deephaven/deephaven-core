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
 */

#ifndef JPY_JMETHOD_H
#define JPY_JMETHOD_H

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
    // Method name.
    PyObject* name;
    // Method parameter count.
    int paramCount;
    // Method is static?
    char isStatic;
    // Method is varargs?
    char isVarArgs;
    // Method parameter types. Will be NULL, if parameter_count == 0.
    JPy_ParamDescriptor* paramDescriptors;
    // Method return type. Will be NULL for constructors.
    JPy_ReturnDescriptor* returnDescriptor;
    // The JNI method ID obtained from the declaring class.
    jmethodID mid;
}
JPy_JMethod;

/**
 * The Python 'JMethod' type singleton.
 */
extern PyTypeObject JMethod_Type;

/**
 * Python object representing an overloaded Java method. It's type is 'JOverloadedMethod'.
 */
typedef struct
{
    PyObject_HEAD

    // The declaring class.
    JPy_JType* declaringClass;
    // Method name.
    PyObject* name;
    // List of method overloads (a PyList with items of type JPy_JMethod).
    PyObject* methodList;
}
JPy_JOverloadedMethod;

/**
 * The Python 'JOverloadedMethod' type singleton.
 */
extern PyTypeObject JOverloadedMethod_Type;

JPy_JMethod*           JOverloadedMethod_FindMethod(JNIEnv* jenv, JPy_JOverloadedMethod* overloadedMethod, PyObject* argTuple, jboolean visitSuperClass, int *isVarArgsArray);
JPy_JMethod*           JOverloadedMethod_FindStaticMethod(JPy_JOverloadedMethod* overloadedMethod, PyObject* argTuple);
JPy_JOverloadedMethod* JOverloadedMethod_New(JPy_JType* declaringClass, PyObject* name, JPy_JMethod* method);
int                    JOverloadedMethod_AddMethod(JPy_JOverloadedMethod* overloadedMethod, JPy_JMethod* method);

JPy_JMethod* JMethod_New(JPy_JType* declaringClass,
                         PyObject* name,
                         int paramCount,
                         JPy_ParamDescriptor* paramDescriptors,
                         JPy_ReturnDescriptor* returnDescriptor,
                         jboolean isStatic,
                         jboolean isVarArgs,
                         jmethodID mid);

void JMethod_Del(JPy_JMethod* method);

int JMethod_ConvertToJavaValues(JNIEnv* jenv, JPy_JMethod* jMethod, int argCount, PyObject* argTuple, jvalue* jArgs);

int  JMethod_CreateJArgs(JNIEnv* jenv, JPy_JMethod* jMethod, PyObject* argTuple, jvalue** jValues, JPy_ArgDisposer** jDisposers, int isVarArgsArray);
void JMethod_DisposeJArgs(JNIEnv* jenv, int paramCount, jvalue* jValues, JPy_ArgDisposer* jDisposers);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* !JPY_JMETHOD_H */