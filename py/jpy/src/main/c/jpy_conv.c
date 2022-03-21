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

#include "jpy_module.h"
#include "jpy_diag.h"
#include "jpy_jtype.h"
#include "jpy_jobj.h"
#include "jpy_conv.h"
#include "jpy_compat.h"



int JPy_AsJObject(JNIEnv* jenv, PyObject* pyObj, jobject* objectRef, jboolean allowJavaWrapping)
{
    return JType_ConvertPythonToJavaObject(jenv, JPy_JObject, pyObj, objectRef, allowJavaWrapping);
}

int JPy_AsJObjectWithType(JNIEnv* jenv, PyObject* pyObj, jobject* objectRef, JPy_JType* type)
{
    return JType_ConvertPythonToJavaObject(jenv, type, pyObj, objectRef, JNI_FALSE);
}

int JPy_AsJObjectWithClass(JNIEnv* jenv, PyObject* pyObj, jobject* objectRef, jclass classRef)
{
    *objectRef = NULL;

    if (pyObj == Py_None) {
        return 0;
    }

    if (classRef != NULL) {
        JPy_JType* valueType;

        valueType = JType_GetType(jenv, classRef, JNI_FALSE); // TODO: we need to xdecref this
        if (valueType == NULL) {
            return -1;
        }
        if (JPy_AsJObjectWithType(jenv, pyObj, objectRef, valueType) < 0) {
            return -1;
        }
    } else {
        if (JPy_AsJObject(jenv, pyObj, objectRef, JNI_FALSE) < 0) {
            return -1;
        }
    }

    return 0;
}



PyObject* JPy_FromJObject(JNIEnv* jenv, jobject objectRef)
{
    JPy_JType* type;
    type = JType_GetTypeForObject(jenv, objectRef, JNI_FALSE);
    if (type == NULL) {
        return NULL;
    }
    return JPy_FromJObjectWithType(jenv, objectRef, type);
}

PyObject* JPy_FromJObjectWithType(JNIEnv* jenv, jobject objectRef, JPy_JType* type)
{
    return JType_ConvertJavaToPythonObject(jenv, type, objectRef);
}


/**
 * Copies the UTF, zero-terminated C-string.
 * Caller is responsible for freeing the returned string using PyMem_Del().
 */
char* JPy_CopyUTFString(const char* utfChars)
{
    char* utfCharsCopy;

    utfCharsCopy = PyMem_New(char, strlen(utfChars) + 1);
    if (utfCharsCopy == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    strcpy(utfCharsCopy, utfChars);
    return utfCharsCopy;
}

/**
 * Copies the given jchar string used by Java into a wchar_t string used by Python.
 * Caller is responsible for freeing the returned string using PyMem_Del().
 */
wchar_t* JPy_ConvertToWCharString(const jchar* jChars, jint length)
{
    wchar_t* wChars;
    jint i;

    wChars = PyMem_New(wchar_t, length + 1);
    if (wChars == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    for (i = 0; i < length; i++) {
        wChars[i] = (wchar_t) jChars[i];
    }
    wChars[length] = 0;

    return wChars;
}

/**
 * Copies the given wchar_t string used by Python into a jchar string used by Python.
 * Caller is responsible for freeing the returned string using PyMem_Del().
 */
jchar* JPy_ConvertToJCharString(const wchar_t* wChars, jint length)
{
    jchar* jChars;
    jint i;

    jChars = PyMem_New(jchar, length + 1);
    if (jChars == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    for (i = 0; i < length; i++) {
        jChars[i] = (jchar) wChars[i];
    }
    jChars[length] = (jchar) 0;

    return jChars;
}

/**
 * Gets the UTF name of thie given class.
 * Caller is responsible for freeing the returned string using Py_Del().
 */
char* JPy_GetTypeName(JNIEnv* jenv, jclass classRef)
{
    jstring jTypeName;
    const char* jTypeNameChars;
    char* typeNameCopy;

    jTypeName = (*jenv)->CallObjectMethod(jenv, classRef, JPy_Class_GetName_MID);
    JPy_ON_JAVA_EXCEPTION_RETURN(NULL);

    jTypeNameChars = (*jenv)->GetStringUTFChars(jenv, jTypeName, NULL);
    if (jTypeNameChars == NULL) {
        PyErr_NoMemory();
        typeNameCopy = NULL;
    } else {
        typeNameCopy = JPy_CopyUTFString(jTypeNameChars);
        (*jenv)->ReleaseStringUTFChars(jenv, jTypeName, jTypeNameChars);
    }
    JPy_DELETE_LOCAL_REF(jTypeName);
    return typeNameCopy;
}

/**
 * Gets a string object representing the name of the given class.
 * Returns a new reference.
 */
PyObject* JPy_FromTypeName(JNIEnv* jenv, jclass classRef)
{
    PyObject* pyTypeName;
    jstring jTypeName;
    const char* jTypeNameChars;

    jTypeName = (*jenv)->CallObjectMethod(jenv, classRef, JPy_Class_GetName_MID);
    JPy_ON_JAVA_EXCEPTION_RETURN(NULL);

    jTypeNameChars = (*jenv)->GetStringUTFChars(jenv, jTypeName, NULL);
    JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JPy_FromTypeName: classRef=%p, jTypeNameChars=\"%s\"\n", classRef, jTypeNameChars);

    if (jTypeNameChars == NULL) {
        PyErr_NoMemory();
        pyTypeName = NULL;
    } else {
        pyTypeName = Py_BuildValue("s", jTypeNameChars);
        (*jenv)->ReleaseStringUTFChars(jenv, jTypeName, jTypeNameChars);
    }
    JPy_DELETE_LOCAL_REF(jTypeName);
    return pyTypeName;
}


PyObject* JPy_FromJString(JNIEnv* jenv, jstring stringRef)
{
    PyObject* returnValue;

#if defined(JPY_COMPAT_33P)

    const jchar* jChars;
    jint length;

    if (stringRef == NULL) {
        return Py_BuildValue("");
    }

    length = (*jenv)->GetStringLength(jenv, stringRef);
    if (length == 0) {
        return Py_BuildValue("s", "");
    }

    jChars = (*jenv)->GetStringChars(jenv, stringRef, NULL);
    if (jChars == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    returnValue = JPy_FROM_WIDE_CHAR_STR(jChars, length);
    (*jenv)->ReleaseStringChars(jenv, stringRef, jChars);

#elif defined(JPY_COMPAT_27)

    const char* utfChars;

    if (stringRef == NULL) {
        return Py_BuildValue("");
    }

    utfChars = (*jenv)->GetStringUTFChars(jenv, stringRef, NULL);
    if (utfChars != NULL) {
        returnValue = Py_BuildValue("s", utfChars);
        (*jenv)->ReleaseStringUTFChars(jenv, stringRef, utfChars);
    } else {
        PyErr_NoMemory();
        returnValue = NULL;
    }
#else
    #error JPY_VERSION_ERROR
#endif
    return returnValue;
}

/**
 * Returns a new Java string (a local reference).
 */
int JPy_AsJString(JNIEnv* jenv, PyObject* arg, jstring* stringRef)
{
    Py_ssize_t length;
    wchar_t* wChars;

    if (arg == Py_None) {
        *stringRef = NULL;
        return 0;
    }

#if defined(JPY_COMPAT_27)
    if (PyString_Check(arg)) {
        char* cstr = PyString_AsString(arg);
        *stringRef = (*jenv)->NewStringUTF(jenv, cstr);
        return *stringRef != NULL ? 0 : -1;
    }
#endif

    wChars = JPy_AS_WIDE_CHAR_STR(arg, &length);
    if (wChars == NULL) {
        *stringRef = NULL;
        return -1;
    }

    if (sizeof(wchar_t) == sizeof(jchar)) {
        *stringRef = (*jenv)->NewString(jenv, (const jchar*) wChars, length);
    } else {
        jchar* jChars;
        jChars = JPy_ConvertToJCharString(wChars, length);
        if (jChars == NULL) {
            goto error;
        }
        *stringRef = (*jenv)->NewString(jenv, jChars, length);
        PyMem_Del(jChars);
    }
    if (*stringRef == NULL) {
        PyMem_Del(wChars);
        PyErr_NoMemory();
        return -1;
    }

error:
    PyMem_Del(wChars);

    return 0;
}

