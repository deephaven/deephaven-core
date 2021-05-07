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

#ifndef JPY_CONV_H
#define JPY_CONV_H

#include "jpy_compat.h"

#ifdef __cplusplus
extern "C" {
#endif

#define JPy_AS_JBOOLEAN(pyArg)   (jboolean) (pyArg == Py_True ? 1 : (pyArg == Py_False || pyArg == Py_None) ? 0 : (JPy_AS_CLONG(pyArg)) != 0)
#define JPy_AS_JCHAR(pyArg)      (jchar) (pyArg == Py_None ? 0 : JPy_AS_CLONG(pyArg))
#define JPy_AS_JBYTE(pyArg)      (jbyte) (pyArg == Py_None ? 0 : JPy_AS_CLONG(pyArg))
#define JPy_AS_JSHORT(pyArg)     (jshort) (pyArg == Py_None ? 0 : JPy_AS_CLONG(pyArg))
#define JPy_AS_JINT(pyArg)       (jint) (pyArg == Py_None ? 0 : JPy_AS_CLONG(pyArg))
#define JPy_AS_JLONG(pyArg)      (jlong) (pyArg == Py_None ? 0 : JPy_AS_CLONGLONG(pyArg))
#define JPy_AS_JFLOAT(pyArg)     (jfloat) (pyArg == Py_None ? 0 : PyFloat_AsDouble(pyArg))
#define JPy_AS_JDOUBLE(pyArg)    (jdouble) (pyArg == Py_None ? 0 : PyFloat_AsDouble(pyArg))

#if defined(JPY_COMPAT_33P)

#define JPy_FROM_JBOOLEAN(jArg)  PyBool_FromLong(jArg)
#define JPy_FROM_JCHAR(jArg)     PyLong_FromLong(jArg)
#define JPy_FROM_JBYTE(jArg)     PyLong_FromLong(jArg)
#define JPy_FROM_JSHORT(jArg)    PyLong_FromLong(jArg)
#define JPy_FROM_JINT(jArg)      PyLong_FromLong(jArg)
#define JPy_FROM_JLONG(jArg)     PyLong_FromLongLong(jArg)
#define JPy_FROM_JFLOAT(jArg)    PyFloat_FromDouble(jArg)
#define JPy_FROM_JDOUBLE(jArg)   PyFloat_FromDouble(jArg)

#elif defined(JPY_COMPAT_27)

#define JPy_FROM_JBOOLEAN(jArg)  PyBool_FromLong(jArg)
#define JPy_FROM_JCHAR(jArg)     PyInt_FromLong(jArg)
#define JPy_FROM_JBYTE(jArg)     PyInt_FromLong(jArg)
#define JPy_FROM_JSHORT(jArg)    PyInt_FromLong(jArg)
#define JPy_FROM_JINT(jArg)      PyLong_FromLong(jArg)
#define JPy_FROM_JLONG(jArg)     PyLong_FromLongLong(jArg)
#define JPy_FROM_JFLOAT(jArg)    PyFloat_FromDouble(jArg)
#define JPy_FROM_JDOUBLE(jArg)   PyFloat_FromDouble(jArg)

#else

#error JPY_VERSION_ERROR

#endif

#define JPy_FROM_JVOID()         Py_BuildValue("")
#define JPy_FROM_JNULL()         Py_BuildValue("")


/**
 * Convert Java string to Python string/unicode object.
 */
PyObject* JPy_FromJString(JNIEnv* jenv, jstring stringRef);

/**
 * Convert any Java Object to Python Object.
 */
PyObject* JPy_FromJObject(JNIEnv* jenv, jobject objectRef);

/**
 * Convert any Java Object of known type to Python Object.
 */
PyObject* JPy_FromJObjectWithType(JNIEnv* jenv, jobject objectRef, JPy_JType* type);

/**
 * Convert Python unicode object to Java String.
 */
int JPy_AsJString(JNIEnv* jenv, PyObject* pyObj, jstring* stringRef);

/**
 * Convert any Python objects to Java object.
 *
 * @param allowObjectWrapping if true, may return a PyObject for unrecognized object types
 */
int JPy_AsJObject(JNIEnv* jenv, PyObject* pyObj, jobject* objectRef, jboolean allowObjectWrapping);

/**
 * Convert Python objects to Java object with known type.
 */
int JPy_AsJObjectWithType(JNIEnv* jenv, PyObject* pyObj, jobject* objectRef, JPy_JType* type);

/**
 * Convert Python objects to Java object with known type.
 */
int JPy_AsJObjectWithClass(JNIEnv* jenv, PyObject* pyObj, jobject* objectRef, jclass classRef);


/**
 * Creates a Python unicode object representing the name of the given class.
 * Returns a new reference.
 */
PyObject* JPy_FromTypeName(JNIEnv* jenv, jclass classRef);

/**
 * Gets the UTF8-encoded name of the given Java type.
 * Caller is responsible for freeing the returned string using PyMem_Del().
 */
char* JPy_GetTypeName(JNIEnv* jenv, jclass classRef);


#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* !JPY_CONV_H */