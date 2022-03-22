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

#include "jpy_module.h"
#include "jpy_diag.h"
#include "jpy_verboseexcept.h"
#include "jpy_jtype.h"
#include "jpy_jmethod.h"
#include "jpy_jfield.h"
#include "jpy_jobj.h"
#include "jpy_conv.h"
#include "jpy_compat.h"


#include <stdlib.h>
#include <string.h>


PyObject* JPy_has_jvm(PyObject* self);
PyObject* JPy_create_jvm(PyObject* self, PyObject* args, PyObject* kwds);
PyObject* JPy_destroy_jvm(PyObject* self, PyObject* args);
PyObject* JPy_get_type(PyObject* self, PyObject* args, PyObject* kwds);
PyObject* JPy_cast(PyObject* self, PyObject* args);
PyObject* JPy_array(PyObject* self, PyObject* args);


static PyMethodDef JPy_Functions[] = {

    {"has_jvm",     (PyCFunction) JPy_has_jvm, METH_NOARGS,
                    "has_jvm() - Check if the JVM is available."},

    {"create_jvm",  (PyCFunction) JPy_create_jvm, METH_VARARGS|METH_KEYWORDS,
                    "create_jvm(options) - Create the Java VM from the given list of options."},

    {"destroy_jvm", JPy_destroy_jvm, METH_VARARGS,
                    "destroy_jvm() - Destroy the current Java VM."},

    {"get_type",    (PyCFunction) JPy_get_type, METH_VARARGS|METH_KEYWORDS,
                    "get_type(name, resolve=True) - Return the Java class with the given name, e.g. 'java.io.File'. "
                    "Loads the Java class from the JVM if not already done. Optionally avoids resolving the class' methods."},

    {"cast",        JPy_cast, METH_VARARGS,
                    "cast(obj, type) - Cast the given Java object to the given Java type (type name or type object). "
                    "Returns None if the cast is not possible."},

    {"array",       JPy_array, METH_VARARGS,
                    "array(name, init) - Return a new Java array of given Java type (type name or type object) and initializer (array length or sequence). "
                    "Possible primitive types are 'boolean', 'byte', 'char', 'short', 'int', 'long', 'float', and 'double'."},

    {NULL, NULL, 0, NULL} /*Sentinel*/
};

void JPy_free(void* unused);

#define JPY_MODULE_NAME "jpy"
#define JPY_MODULE_DOC  "Bi-directional Python-Java Bridge"

#if defined(JPY_COMPAT_33P)
static struct PyModuleDef JPy_ModuleDef =
{
    PyModuleDef_HEAD_INIT,
    JPY_MODULE_NAME,   /* Name of the Python JPy_Module */
    JPY_MODULE_DOC,    /* Module documentation */
    -1,                /* Size of per-interpreter state of the JPy_Module, or -1 if the JPy_Module keeps state in global variables. */
    JPy_Functions,     /* Structure containing global jpy-functions */
    NULL,     // m_reload
    NULL,     // m_traverse
    NULL,     // m_clear
    JPy_free  // m_free
};
#endif

PyObject* JPy_Module = NULL;
PyObject* JPy_Types = NULL;
PyObject* JPy_Type_Callbacks = NULL;
PyObject* JPy_Type_Translations = NULL;
PyObject* JException_Type = NULL;

// A global reference to a Java VM singleton.
JavaVM* JPy_JVM = NULL;

// If true, this JVM structure has been initialised from Python jpy.create_jvm()
jboolean JPy_MustDestroyJVM = JNI_FALSE;


// Global VM Information (maybe better place this in the JPy_JVM structure later)
// {{{

JPy_JType* JPy_JBoolean = NULL;
JPy_JType* JPy_JChar = NULL;
JPy_JType* JPy_JByte = NULL;
JPy_JType* JPy_JShort = NULL;
JPy_JType* JPy_JInt = NULL;
JPy_JType* JPy_JLong = NULL;
JPy_JType* JPy_JFloat = NULL;
JPy_JType* JPy_JDouble = NULL;
JPy_JType* JPy_JVoid = NULL;
JPy_JType* JPy_JBooleanObj = NULL;
JPy_JType* JPy_JCharacterObj = NULL;
JPy_JType* JPy_JByteObj = NULL;
JPy_JType* JPy_JShortObj = NULL;
JPy_JType* JPy_JIntegerObj = NULL;
JPy_JType* JPy_JLongObj = NULL;
JPy_JType* JPy_JFloatObj = NULL;
JPy_JType* JPy_JDoubleObj = NULL;
JPy_JType* JPy_JObject = NULL;
JPy_JType* JPy_JClass = NULL;
JPy_JType* JPy_JString = NULL;
JPy_JType* JPy_JPyObject = NULL;
JPy_JType* JPy_JPyModule = NULL;
JPy_JType* JPy_JThrowable = NULL;
JPy_JType* JPy_JStackTraceElement = NULL;


// java.lang.Comparable
jclass JPy_Comparable_JClass = NULL;
jmethodID JPy_Comparable_CompareTo_MID = NULL;

// java.lang.Object
jclass JPy_Object_JClass = NULL;
jmethodID JPy_Object_ToString_MID = NULL;
jmethodID JPy_Object_HashCode_MID = NULL;
jmethodID JPy_Object_Equals_MID = NULL;

// java.lang.Class
jclass JPy_Class_JClass = NULL;
jmethodID JPy_Class_GetName_MID = NULL;
jmethodID JPy_Class_GetDeclaredConstructors_MID = NULL;
jmethodID JPy_Class_GetDeclaredFields_MID = NULL;
jmethodID JPy_Class_GetDeclaredMethods_MID = NULL;
jmethodID JPy_Class_GetFields_MID = NULL;
jmethodID JPy_Class_GetMethods_MID = NULL;
jmethodID JPy_Class_GetComponentType_MID = NULL;
jmethodID JPy_Class_IsPrimitive_MID = NULL;
jmethodID JPy_Class_IsInterface_MID = NULL;

// java.lang.reflect.Constructor
jclass JPy_Constructor_JClass = NULL;
jmethodID JPy_Constructor_GetModifiers_MID = NULL;
jmethodID JPy_Constructor_GetParameterTypes_MID = NULL;

// java.lang.reflect.Method
jclass JPy_Method_JClass = NULL;
jmethodID JPy_Method_GetName_MID = NULL;
jmethodID JPy_Method_GetReturnType_MID = NULL;
jmethodID JPy_Method_GetParameterTypes_MID = NULL;
jmethodID JPy_Method_GetModifiers_MID = NULL;

// java.lang.reflect.Field
jclass JPy_Field_JClass = NULL;
jmethodID JPy_Field_GetName_MID = NULL;
jmethodID JPy_Field_GetModifiers_MID = NULL;
jmethodID JPy_Field_GetType_MID = NULL;

// java.util.Map
jclass JPy_Map_JClass = NULL;
jclass JPy_Map_Entry_JClass = NULL;
jmethodID JPy_Map_entrySet_MID = NULL;
jmethodID JPy_Map_put_MID = NULL;
jmethodID JPy_Map_clear_MID = NULL;
jmethodID JPy_Map_Entry_getKey_MID = NULL;
jmethodID JPy_Map_Entry_getValue_MID = NULL;
// java.util.Set
jclass JPy_Set_JClass = NULL;
jmethodID JPy_Set_Iterator_MID = NULL;
// java.util.Iterator
jclass JPy_Iterator_JClass = NULL;
jmethodID JPy_Iterator_next_MID = NULL;
jmethodID JPy_Iterator_hasNext_MID = NULL;

jclass JPy_RuntimeException_JClass = NULL;
jclass JPy_OutOfMemoryError_JClass = NULL;
jclass JPy_UnsupportedOperationException_JClass = NULL;
jclass JPy_FileNotFoundException_JClass = NULL;
jclass JPy_KeyError_JClass = NULL;
jclass JPy_StopIteration_JClass = NULL;

// java.lang.Boolean
jclass JPy_Boolean_JClass = NULL;
jmethodID JPy_Boolean_Init_MID = NULL;
jmethodID JPy_Boolean_BooleanValue_MID = NULL;

jclass JPy_Character_JClass = NULL;
jmethodID JPy_Character_Init_MID;
jmethodID JPy_Character_CharValue_MID = NULL;

jclass JPy_Byte_JClass = NULL;
jmethodID JPy_Byte_Init_MID = NULL;

jclass JPy_Short_JClass = NULL;
jmethodID JPy_Short_Init_MID = NULL;

jclass JPy_Integer_JClass = NULL;
jmethodID JPy_Integer_Init_MID = NULL;

jclass JPy_Long_JClass = NULL;
jmethodID JPy_Long_Init_MID = NULL;

jclass JPy_Float_JClass = NULL;
jmethodID JPy_Float_Init_MID = NULL;

jclass JPy_Double_JClass = NULL;
jmethodID JPy_Double_Init_MID = NULL;

// java.lang.Number
jclass JPy_Number_JClass = NULL;
jmethodID JPy_Number_IntValue_MID = NULL;
jmethodID JPy_Number_LongValue_MID = NULL;
jmethodID JPy_Number_DoubleValue_MID = NULL;

jclass JPy_Void_JClass = NULL;
jclass JPy_String_JClass = NULL;
jclass JPy_PyObject_JClass = NULL;
jclass JPy_PyDictWrapper_JClass = NULL;

jmethodID JPy_PyObject_GetPointer_MID = NULL;
jmethodID JPy_PyObject_UnwrapProxy_SMID = NULL;
jmethodID JPy_PyObject_Init_MID = NULL;
jmethodID JPy_PyModule_Init_MID = NULL;

jmethodID JPy_PyDictWrapper_GetPointer_MID = NULL;

// java.lang.Throwable
jclass JPy_Throwable_JClass = NULL;
jmethodID JPy_Throwable_getStackTrace_MID = NULL;
jmethodID JPy_Throwable_getCause_MID = NULL;

// stack trace element
jclass JPy_StackTraceElement_JClass = NULL;

// }}}


JNIEnv* JPy_GetJNIEnv(void)
{
    JavaVM* jvm;
    JNIEnv* jenv;
    jint status;

    jvm = JPy_JVM;
    if (jvm == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "jpy: No JVM available.");
        return NULL;
    }

    status = (*jvm)->GetEnv(jvm, (void**) &jenv, JPY_JNI_VERSION);
    if (status == JNI_EDETACHED) {
        if ((*jvm)->AttachCurrentThread(jvm, (void**) &jenv, NULL) == 0) {
            JPy_DIAG_PRINT(JPy_DIAG_F_JVM, "JPy_GetJNIEnv: Attached current thread to JVM: jenv=%p\n", jenv);
        } else {
            PyErr_SetString(PyExc_RuntimeError, "jpy: Failed to attach current thread to JVM.");
            return NULL;
        }
    } else if (status == JNI_EVERSION) {
        PyErr_SetString(PyExc_RuntimeError, "jpy: Failed to attach current thread to JVM: Java version not supported.");
        return NULL;
    } else if (status == JNI_OK) {
        // ok!
        JPy_DIAG_PRINT(JPy_DIAG_F_JVM, "JPy_GetJNIEnv: jenv=%p\n", jenv);
    } else {
        JPy_DIAG_PRINT(JPy_DIAG_F_JVM + JPy_DIAG_F_ERR, "JPy_GetJNIEnv: Received unhandled status code from JVM GetEnv(): status=%d\n", status);
    }

    return jenv;
}

#if defined(JPY_COMPAT_33P)
#define JPY_RETURN(V) return V
#define JPY_MODULE_INIT_FUNC PyInit_jpy
#elif defined(JPY_COMPAT_27)
#define JPY_RETURN(V) return
#define JPY_MODULE_INIT_FUNC initjpy
#else
#error JPY_VERSION_ERROR
#endif

/**
 * Called by the Python interpreter's import machinery, e.g. using 'import jpy'.
 */
PyMODINIT_FUNC JPY_MODULE_INIT_FUNC(void)
{
    //printf("PyInit_jpy: JPy_JVM=%p\n", JPy_JVM);

    /////////////////////////////////////////////////////////////////////////

#if defined(JPY_COMPAT_33P)
    JPy_Module = PyModule_Create(&JPy_ModuleDef);
    if (JPy_Module == NULL) {
        JPY_RETURN(NULL);
    }
#elif defined(JPY_COMPAT_27)
    JPy_Module = Py_InitModule3(JPY_MODULE_NAME, JPy_Functions, JPY_MODULE_DOC);
    if (JPy_Module == NULL) {
        JPY_RETURN(NULL);
    }
#else
    #error JPY_VERSION_ERROR
#endif

    /////////////////////////////////////////////////////////////////////////

    if (PyType_Ready(&JType_Type) < 0) {
        JPY_RETURN(NULL);
    }
    Py_INCREF(&JType_Type);
    PyModule_AddObject(JPy_Module, "JType", (PyObject*) &JType_Type);

    /////////////////////////////////////////////////////////////////////////

    if (PyType_Ready(&JMethod_Type) < 0) {
        JPY_RETURN(NULL);
    }
    Py_INCREF(&JMethod_Type);
    PyModule_AddObject(JPy_Module, "JMethod", (PyObject*) &JMethod_Type);

    /////////////////////////////////////////////////////////////////////////

    if (PyType_Ready(&JOverloadedMethod_Type) < 0) {
        JPY_RETURN(NULL);
    }
    Py_INCREF(&JOverloadedMethod_Type);
    PyModule_AddObject(JPy_Module, "JOverloadedMethod", (PyObject*) &JOverloadedMethod_Type);

    /////////////////////////////////////////////////////////////////////////

    if (PyType_Ready(&JField_Type) < 0) {
        JPY_RETURN(NULL);
    }
    Py_INCREF(&JField_Type);
    PyModule_AddObject(JPy_Module, "JField", (PyObject*) &JField_Type);

    /////////////////////////////////////////////////////////////////////////

    JException_Type = PyErr_NewException("jpy.JException", NULL, NULL);
    Py_INCREF(JException_Type);
    PyModule_AddObject(JPy_Module, "JException", JException_Type);

    /////////////////////////////////////////////////////////////////////////

    JPy_Types = PyDict_New();
    Py_INCREF(JPy_Types);
    PyModule_AddObject(JPy_Module, JPy_MODULE_ATTR_NAME_TYPES, JPy_Types);

    /////////////////////////////////////////////////////////////////////////

    JPy_Type_Callbacks = PyDict_New();
    Py_INCREF(JPy_Type_Callbacks);
    PyModule_AddObject(JPy_Module, JPy_MODULE_ATTR_NAME_TYPE_CALLBACKS, JPy_Type_Callbacks);

    /////////////////////////////////////////////////////////////////////////

    JPy_Type_Translations = PyDict_New();
    Py_INCREF(JPy_Type_Translations);
    PyModule_AddObject(JPy_Module, JPy_MODULE_ATTR_NAME_TYPE_TRANSLATIONS, JPy_Type_Translations);

    /////////////////////////////////////////////////////////////////////////

    if (PyType_Ready(&Diag_Type) < 0) {
        JPY_RETURN(NULL);
    }
    //Py_INCREF(&DiagFlags_Type);
    {
        PyObject* pyDiag = Diag_New();
        Py_INCREF(pyDiag);
        PyModule_AddObject(JPy_Module, "diag", pyDiag);
    }

    if (PyType_Ready(&VerboseExceptions_Type) < 0) {
        JPY_RETURN(NULL);
    }
    {
        PyObject* pyVerboseExceptions = VerboseExceptions_New();
        Py_INCREF(pyVerboseExceptions);
        PyModule_AddObject(JPy_Module, "VerboseExceptions", pyVerboseExceptions);
    }

    /////////////////////////////////////////////////////////////////////////

    if (JPy_JVM != NULL) {
        JNIEnv* jenv;
        jenv = JPy_GetJNIEnv();
        if (jenv == NULL) {
            JPY_RETURN(NULL);
        }
        // If we have already a running VM, initialize global variables
        if (JPy_InitGlobalVars(jenv) < 0) {
            JPY_RETURN(NULL);
        }
    }

    /////////////////////////////////////////////////////////////////////////

    //printf("PyInit_jpy: exit\n");

    JPY_RETURN(JPy_Module);
}

PyObject* JPy_has_jvm(PyObject* self)
{
    return PyBool_FromLong(JPy_JVM != NULL);
}

PyObject* JPy_create_jvm(PyObject* self, PyObject* args, PyObject* kwds)
{
    static char* keywords[] = {"options", NULL};
    PyObject*   options;
    Py_ssize_t  optionCount;
    PyObject*   option;
    JavaVMOption* jvmOptions;
    JavaVMInitArgs jvmInitArgs;
    jint        jvmErrorCode;
    JNIEnv*     jenv;
    Py_ssize_t  i;

    //printf("JPy_create_jvm: JPy_JVM=%p\n", JPy_JVM);

    options = NULL;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O:create_jvm", keywords, &options)) {
        return NULL;
    }

    if (JPy_JVM != NULL) {
        JPy_DIAG_PRINT(JPy_DIAG_F_JVM + JPy_DIAG_F_ERR, "JPy_create_jvm: WARNING: Java VM is already running.\n");
        Py_DECREF(options);
        return Py_BuildValue("");
    }

    if (!PySequence_Check(options)) {
        PyErr_SetString(PyExc_ValueError, "create_jvm: argument 1 (options) must be a sequence of Java VM option strings");
        return NULL;
    }

    optionCount = PySequence_Length(options);
    if (optionCount == -1) {
        PyErr_SetString(PyExc_ValueError, "create_jvm: failed to determine sequence length of argument 1 (options)");
        return NULL;
    }

    jvmOptions = PyMem_New(JavaVMOption, optionCount);
    if (jvmOptions == NULL) {
        return PyErr_NoMemory();
    }

    for (i = 0; i < optionCount; i++) {
        option = PySequence_GetItem(options, i);
        if (option == NULL) {
            PyMem_Del(jvmOptions);
            return NULL;
        }
        jvmOptions[i].optionString = (char*) JPy_AS_UTF8(option);
        jvmOptions[i].extraInfo = NULL;
        JPy_DIAG_PRINT(JPy_DIAG_F_JVM, "JPy_create_jvm: jvmOptions[%d].optionString = '%s'\n", i, jvmOptions[i].optionString);
        if (jvmOptions[i].optionString == NULL) {
            PyMem_Del(jvmOptions);
            return NULL;
        }
        Py_DECREF(option);
    }

    jvmInitArgs.version = JPY_JNI_VERSION;
    jvmInitArgs.options = jvmOptions;
    jvmInitArgs.nOptions = (size_t) optionCount;
    jvmInitArgs.ignoreUnrecognized = 0;
    jvmErrorCode = JNI_CreateJavaVM(&JPy_JVM, (void**) &jenv, &jvmInitArgs);
    JPy_MustDestroyJVM = JNI_TRUE;

    JPy_DIAG_PRINT(JPy_DIAG_F_JVM, "JPy_create_jvm: res=%d, JPy_JVM=%p, jenv=%p, JPy_MustDestroyJVM=%d\n", jvmErrorCode, JPy_JVM, jenv, JPy_MustDestroyJVM);

    PyMem_Del(jvmOptions);

    if (jvmErrorCode != JNI_OK) {
        JPy_DIAG_PRINT(JPy_DIAG_F_JVM + JPy_DIAG_F_ERR,
                       "JPy_create_jvm: INTERNAL ERROR: Failed to create Java VM (JNI error code %d). Possible reasons are:\n"
                       "* The Java heap space setting is too high (option -Xmx). Try '256M' first, then increment.\n"
                       "* The JVM shared library (Unix: libjvm.so, Windows: jvm.dll) cannot be found or cannot be loaded.\n"
                       "  Make sure the shared library can be found via the 'PATH' environment variable.\n"
                       "  Also make sure that the JVM is compiled for the same architecture as Python.\n",
                       jvmErrorCode);
        PyErr_SetString(PyExc_RuntimeError, "jpy: failed to create Java VM");
        return NULL;
    }

    if (JPy_InitGlobalVars(jenv) < 0) {
        return NULL;
    }

    return Py_BuildValue("");
}

PyObject* JPy_destroy_jvm(PyObject* self, PyObject* args)
{
    JPy_DIAG_PRINT(JPy_DIAG_F_JVM, "JPy_destroy_jvm: JPy_JVM=%p, JPy_MustDestroyJVM=%d\n", JPy_JVM, JPy_MustDestroyJVM);

    if (JPy_JVM != NULL && JPy_MustDestroyJVM) {
        JPy_ClearGlobalVars(JPy_GetJNIEnv());
        (*JPy_JVM)->DestroyJavaVM(JPy_JVM);
        JPy_JVM = NULL;
    }

    return Py_BuildValue("");
}

PyObject* JPy_get_type_internal(JNIEnv* jenv, PyObject* self, PyObject* args, PyObject* kwds)
{
    static char* keywords[] = {"name", "resolve", NULL};
    const char* className;
    int resolve;

    resolve = 1; // True
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s|i:get_type", keywords, &className, &resolve)) {
        return NULL;
    }

    return (PyObject*) JType_GetTypeForName(jenv, className, (jboolean) (resolve != 0 ? JNI_TRUE : JNI_FALSE));
}

PyObject* JPy_get_type(PyObject* self, PyObject* args, PyObject* kwds)
{
    JPy_FRAME(PyObject*, NULL, JPy_get_type_internal(jenv, self, args, kwds), 16)
}

PyObject* JPy_cast_internal(JNIEnv* jenv, PyObject* self, PyObject* args)
{
    PyObject* obj;
    PyObject* objType;
    JPy_JType* type;
    jboolean inst;

    if (!PyArg_ParseTuple(args, "OO:cast", &obj, &objType)) {
        return NULL;
    }

    if (obj == Py_None) {
        return Py_BuildValue("");
    }

    if (!JObj_Check(obj)) {
        PyErr_SetString(PyExc_ValueError, "cast: argument 1 (obj) must be a Java object");
        return NULL;
    }

    if (JPy_IS_STR(objType)) {
        const char* typeName = JPy_AS_UTF8(objType);
        type = JType_GetTypeForName(jenv, typeName, JNI_FALSE);
        if (type == NULL) {
            return NULL;
        }
    } else if (JType_Check(objType)) {
        type = (JPy_JType*) objType;
    } else {
        PyErr_SetString(PyExc_ValueError, "cast: argument 2 (obj_type) must be a Java type name or Java type object");
        return NULL;
    }

    inst = (*jenv)->IsInstanceOf(jenv, ((JPy_JObj*) obj)->objectRef, type->classRef);
    if (inst) {
        return (PyObject*) JObj_FromType(jenv, (JPy_JType*) objType, ((JPy_JObj*) obj)->objectRef);
    } else {
        return Py_BuildValue("");
    }
}

PyObject* JPy_cast(PyObject* self, PyObject* args)
{
    JPy_FRAME(PyObject*, NULL, JPy_cast_internal(jenv, self, args), 16)
}

PyObject* JPy_array_internal(JNIEnv* jenv, PyObject* self, PyObject* args)
{
    JPy_JType* componentType;
    jarray arrayRef;
    PyObject* objType;
    PyObject* objInit;

    if (!PyArg_ParseTuple(args, "OO:array", &objType, &objInit)) {
        return NULL;
    }

    if (JPy_IS_STR(objType)) {
        const char* typeName;
        typeName = JPy_AS_UTF8(objType);
        componentType = JType_GetTypeForName(jenv, typeName, JNI_FALSE);
        if (componentType == NULL) {
            return NULL;
        }
    } else if (JType_Check(objType)) {
        componentType = (JPy_JType*) objType;
    } else {
        PyErr_SetString(PyExc_ValueError, "array: argument 1 (type) must be a type name or Java type object");
        return NULL;
    }

    if (componentType == JPy_JVoid) {
        PyErr_SetString(PyExc_ValueError, "array: argument 1 (type) must not be 'void'");
        return NULL;
    }

    if (JPy_IS_CLONG(objInit)) {
        jint length = JPy_AS_CLONG(objInit);
        if (length < 0) {
            PyErr_SetString(PyExc_ValueError, "array: argument 2 (init) must be either an integer array length or any sequence");
            return NULL;
        }
        if (componentType == JPy_JBoolean) {
            arrayRef = (*jenv)->NewBooleanArray(jenv, length);
        } else if (componentType == JPy_JChar) {
            arrayRef = (*jenv)->NewCharArray(jenv, length);
        } else if (componentType == JPy_JByte) {
            arrayRef = (*jenv)->NewByteArray(jenv, length);
        } else if (componentType == JPy_JShort) {
            arrayRef = (*jenv)->NewShortArray(jenv, length);
        } else if (componentType == JPy_JInt) {
            arrayRef = (*jenv)->NewIntArray(jenv, length);
        } else if (componentType == JPy_JLong) {
            arrayRef = (*jenv)->NewLongArray(jenv, length);
        } else if (componentType == JPy_JFloat) {
            arrayRef = (*jenv)->NewFloatArray(jenv, length);
        } else if (componentType == JPy_JDouble) {
            arrayRef = (*jenv)->NewDoubleArray(jenv, length);
        } else {
            arrayRef = (*jenv)->NewObjectArray(jenv, length, componentType->classRef, NULL);
        }
        if (arrayRef == NULL) {
            return PyErr_NoMemory();
        }
        return JObj_New(jenv, arrayRef);
    } else if (PySequence_Check(objInit)) {
        if (JType_CreateJavaArray(jenv, componentType, objInit, &arrayRef, JNI_FALSE) < 0) {
            return NULL;
        }
        return JObj_New(jenv, arrayRef);
    } else {
        PyErr_SetString(PyExc_ValueError, "array: argument 2 (init) must be either an integer array length or any sequence");
        return NULL;
    }
}

PyObject* JPy_array(PyObject* self, PyObject* args)
{
    JPy_FRAME(PyObject*, NULL, JPy_array_internal(jenv, self, args), 16)
}

JPy_JType* JPy_GetNonObjectJType(JNIEnv* jenv, jclass classRef)
{
    jclass primClassRef;
    jfieldID fid;
    JPy_JType* type;

    if (classRef == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "jpy: internal error: classRef == NULL");
    }

    fid = (*jenv)->GetStaticFieldID(jenv, classRef, "TYPE", "Ljava/lang/Class;");
    if (fid == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "field 'TYPE' not found");
        return NULL;
    }

    primClassRef = (*jenv)->GetStaticObjectField(jenv, classRef, fid);
    if (primClassRef == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "failed to access field 'TYPE'");
        return NULL;
    }

    type = JType_GetType(jenv, primClassRef, JNI_FALSE);
    if (type == NULL) {
        return NULL;
    }

    type->isResolved = JNI_TRUE; // Primitive types are always resolved.
    Py_INCREF((PyObject*) type);

    return type;
}

jclass JPy_GetClass(JNIEnv* jenv, const char* name)
{
    jclass localClassRef;
    jclass globalClassRef;

    localClassRef = (*jenv)->FindClass(jenv, name);
    if (localClassRef == NULL) {
        PyErr_Format(PyExc_RuntimeError, "jpy: internal error: Java class '%s' not found", name);
        return NULL;
    }

    globalClassRef = (*jenv)->NewGlobalRef(jenv, localClassRef);
    //(*jenv)->DeleteLocalRef(jenv, localClassRef);
    if (globalClassRef == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    return globalClassRef;
}

jmethodID JPy_GetMethod(JNIEnv* jenv, jclass classRef, const char* name, const char* sig)
{
    jmethodID methodID;
    methodID = (*jenv)->GetMethodID(jenv, classRef, name, sig);
    if (methodID == NULL) {
        PyErr_Format(PyExc_RuntimeError, "jpy: internal error: method not found: %s%s", name, sig);
        return NULL;
    }
    return methodID;
}

jmethodID JPy_GetStaticMethod(JNIEnv *jenv, jclass classRef, const char *name, const char *sig)
{
    jmethodID methodID;
    methodID = (*jenv)->GetStaticMethodID(jenv, classRef, name, sig);
    if (methodID == NULL) {
        PyErr_Format(PyExc_RuntimeError, "jpy: internal error: static method not found: %s%s", name, sig);
        return NULL;
    }
    return methodID;
}

#define DEFINE_CLASS(C, N) \
    C = JPy_GetClass(jenv, N); \
    if (C == NULL) { \
        return -1; \
    }


#define DEFINE_METHOD(M, C, N, S) \
    M = JPy_GetMethod(jenv, C, N, S); \
    if (M == NULL) { \
        return -1; \
    }


#define DEFINE_STATIC_METHOD(M, C, N, S) \
    M = JPy_GetStaticMethod(jenv, C, N, S); \
    if (M == NULL) { \
        return -1; \
    }


#define DEFINE_NON_OBJECT_TYPE(T, C) \
    T = JPy_GetNonObjectJType(jenv, C); \
    if (T == NULL) { \
        return -1; \
    }


#define DEFINE_OBJECT_TYPE(T, C) \
    T = JType_GetType(jenv, C, JNI_FALSE); \
    if (T == NULL) { \
        return -1; \
    }


int initGlobalPyObjectVars(JNIEnv* jenv)
{
    JPy_JType *dictType;
    JPy_JType *keyErrorType;
    JPy_JType *stopIterationType;

    JPy_JPyObject = JType_GetTypeForName(jenv, "org.jpy.PyObject", JNI_FALSE);
    if (JPy_JPyObject == NULL) {
        // org.jpy.PyObject may not be on the classpath, which is ok
        PyErr_Clear();
        return -1;
    } else {
        JPy_PyObject_JClass = JPy_JPyObject->classRef;
        DEFINE_METHOD(JPy_PyObject_GetPointer_MID, JPy_PyObject_JClass, "getPointer", "()J");
        DEFINE_STATIC_METHOD(JPy_PyObject_UnwrapProxy_SMID, JPy_PyObject_JClass, "unwrapProxy", "(Ljava/lang/Object;)Lorg/jpy/PyObject;");
        DEFINE_METHOD(JPy_PyObject_Init_MID, JPy_PyObject_JClass, "<init>", "(JZ)V");
    }

    JPy_JPyModule = JType_GetTypeForName(jenv, "org.jpy.PyModule", JNI_FALSE);
    if (JPy_JPyModule == NULL) {
        // org.jpy.PyModule may not be on the classpath, which is ok
        PyErr_Clear();
        return -1;
    }

    dictType = JType_GetTypeForName(jenv, "org.jpy.PyDictWrapper", JNI_FALSE);
    if (dictType == NULL) {
        PyErr_Clear();
        return -1;
    } else {
        JPy_PyDictWrapper_JClass = dictType->classRef;
        DEFINE_METHOD(JPy_PyDictWrapper_GetPointer_MID, JPy_PyDictWrapper_JClass, "getPointer", "()J");
    }

    keyErrorType = JType_GetTypeForName(jenv, "org.jpy.KeyError", JNI_FALSE);
    if (keyErrorType == NULL) {
        PyErr_Clear();
        return -1;
    } else {
        JPy_KeyError_JClass = keyErrorType->classRef;
    }

    stopIterationType = JType_GetTypeForName(jenv, "org.jpy.StopIteration", JNI_FALSE);
    if (stopIterationType == NULL) {
        PyErr_Clear();
        return -1;
    } else {
        JPy_StopIteration_JClass = stopIterationType->classRef;
    }

    return 0;
}


int JPy_InitGlobalVars(JNIEnv* jenv)
{
    if (JPy_Comparable_JClass != NULL) {
        return 0;
    }

    DEFINE_CLASS(JPy_Comparable_JClass, "java/lang/Comparable");
    DEFINE_METHOD(JPy_Comparable_CompareTo_MID, JPy_Comparable_JClass, "compareTo", "(Ljava/lang/Object;)I");

    DEFINE_CLASS(JPy_Object_JClass, "java/lang/Object");
    DEFINE_METHOD(JPy_Object_ToString_MID, JPy_Object_JClass, "toString", "()Ljava/lang/String;");
    DEFINE_METHOD(JPy_Object_HashCode_MID, JPy_Object_JClass, "hashCode", "()I");
    DEFINE_METHOD(JPy_Object_Equals_MID, JPy_Object_JClass, "equals", "(Ljava/lang/Object;)Z");

    DEFINE_CLASS(JPy_Class_JClass, "java/lang/Class");
    DEFINE_METHOD(JPy_Class_GetName_MID, JPy_Class_JClass, "getName", "()Ljava/lang/String;");
    DEFINE_METHOD(JPy_Class_GetDeclaredConstructors_MID, JPy_Class_JClass, "getDeclaredConstructors", "()[Ljava/lang/reflect/Constructor;");
    DEFINE_METHOD(JPy_Class_GetDeclaredMethods_MID, JPy_Class_JClass, "getDeclaredMethods", "()[Ljava/lang/reflect/Method;");
    DEFINE_METHOD(JPy_Class_GetDeclaredFields_MID, JPy_Class_JClass, "getDeclaredFields", "()[Ljava/lang/reflect/Field;");
    DEFINE_METHOD(JPy_Class_GetMethods_MID, JPy_Class_JClass, "getMethods", "()[Ljava/lang/reflect/Method;");
    DEFINE_METHOD(JPy_Class_GetFields_MID, JPy_Class_JClass, "getFields", "()[Ljava/lang/reflect/Field;");
    DEFINE_METHOD(JPy_Class_GetComponentType_MID, JPy_Class_JClass, "getComponentType", "()Ljava/lang/Class;");
    DEFINE_METHOD(JPy_Class_IsPrimitive_MID, JPy_Class_JClass, "isPrimitive", "()Z");
    DEFINE_METHOD(JPy_Class_IsInterface_MID, JPy_Class_JClass, "isInterface", "()Z");

    DEFINE_CLASS(JPy_Constructor_JClass, "java/lang/reflect/Constructor");
    DEFINE_METHOD(JPy_Constructor_GetModifiers_MID, JPy_Constructor_JClass, "getModifiers", "()I");
    DEFINE_METHOD(JPy_Constructor_GetParameterTypes_MID, JPy_Constructor_JClass, "getParameterTypes", "()[Ljava/lang/Class;");

    DEFINE_CLASS(JPy_Field_JClass, "java/lang/reflect/Field");
    DEFINE_METHOD(JPy_Field_GetName_MID, JPy_Field_JClass, "getName", "()Ljava/lang/String;");
    DEFINE_METHOD(JPy_Field_GetModifiers_MID, JPy_Field_JClass, "getModifiers", "()I");
    DEFINE_METHOD(JPy_Field_GetType_MID, JPy_Field_JClass, "getType", "()Ljava/lang/Class;");

    DEFINE_CLASS(JPy_Method_JClass, "java/lang/reflect/Method");
    DEFINE_METHOD(JPy_Method_GetName_MID, JPy_Method_JClass, "getName", "()Ljava/lang/String;");
    DEFINE_METHOD(JPy_Method_GetModifiers_MID, JPy_Method_JClass, "getModifiers", "()I");
    DEFINE_METHOD(JPy_Method_GetParameterTypes_MID, JPy_Method_JClass, "getParameterTypes", "()[Ljava/lang/Class;");
    DEFINE_METHOD(JPy_Method_GetReturnType_MID, JPy_Method_JClass, "getReturnType", "()Ljava/lang/Class;");

    DEFINE_CLASS(JPy_Map_JClass, "java/util/Map");
    DEFINE_METHOD(JPy_Map_entrySet_MID, JPy_Map_JClass, "entrySet", "()Ljava/util/Set;");
    DEFINE_METHOD(JPy_Map_put_MID, JPy_Map_JClass, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    DEFINE_METHOD(JPy_Map_clear_MID, JPy_Map_JClass, "clear", "()V");

    DEFINE_CLASS(JPy_Map_Entry_JClass, "java/util/Map$Entry");
    DEFINE_METHOD(JPy_Map_Entry_getKey_MID, JPy_Map_Entry_JClass, "getKey", "()Ljava/lang/Object;");
    DEFINE_METHOD(JPy_Map_Entry_getValue_MID, JPy_Map_Entry_JClass, "getValue", "()Ljava/lang/Object;");


    // java.util.Set
    DEFINE_CLASS(JPy_Set_JClass, "java/util/Set");
    DEFINE_METHOD(JPy_Set_Iterator_MID, JPy_Set_JClass, "iterator", "()Ljava/util/Iterator;");
    // java.util.Iterator
    DEFINE_CLASS(JPy_Iterator_JClass, "java/util/Iterator");
    DEFINE_METHOD(JPy_Iterator_next_MID, JPy_Iterator_JClass, "next", "()Ljava/lang/Object;");
    DEFINE_METHOD(JPy_Iterator_hasNext_MID, JPy_Iterator_JClass, "hasNext", "()Z");

    DEFINE_CLASS(JPy_RuntimeException_JClass, "java/lang/RuntimeException");
    DEFINE_CLASS(JPy_OutOfMemoryError_JClass, "java/lang/OutOfMemoryError");
    DEFINE_CLASS(JPy_FileNotFoundException_JClass, "java/io/FileNotFoundException");
    DEFINE_CLASS(JPy_UnsupportedOperationException_JClass, "java/lang/UnsupportedOperationException");

    DEFINE_CLASS(JPy_Boolean_JClass, "java/lang/Boolean");
    DEFINE_METHOD(JPy_Boolean_Init_MID, JPy_Boolean_JClass, "<init>", "(Z)V");
    DEFINE_METHOD(JPy_Boolean_BooleanValue_MID, JPy_Boolean_JClass, "booleanValue", "()Z");

    DEFINE_CLASS(JPy_Character_JClass, "java/lang/Character");
    DEFINE_METHOD(JPy_Character_Init_MID, JPy_Character_JClass, "<init>", "(C)V");
    DEFINE_METHOD(JPy_Character_CharValue_MID, JPy_Character_JClass, "charValue", "()C");

    DEFINE_CLASS(JPy_Number_JClass, "java/lang/Number");

    DEFINE_CLASS(JPy_Byte_JClass, "java/lang/Byte");
    DEFINE_METHOD(JPy_Byte_Init_MID, JPy_Byte_JClass, "<init>", "(B)V");

    DEFINE_CLASS(JPy_Short_JClass, "java/lang/Short");
    DEFINE_METHOD(JPy_Short_Init_MID, JPy_Short_JClass, "<init>", "(S)V");

    DEFINE_CLASS(JPy_Integer_JClass, "java/lang/Integer");
    DEFINE_METHOD(JPy_Integer_Init_MID, JPy_Integer_JClass, "<init>", "(I)V");

    DEFINE_CLASS(JPy_Long_JClass, "java/lang/Long");
    DEFINE_METHOD(JPy_Long_Init_MID, JPy_Long_JClass, "<init>", "(J)V");

    DEFINE_CLASS(JPy_Float_JClass, "java/lang/Float");
    DEFINE_METHOD(JPy_Float_Init_MID, JPy_Float_JClass, "<init>", "(F)V");

    DEFINE_CLASS(JPy_Double_JClass, "java/lang/Double");
    DEFINE_METHOD(JPy_Double_Init_MID, JPy_Double_JClass, "<init>", "(D)V");

    DEFINE_CLASS(JPy_Number_JClass, "java/lang/Number");
    DEFINE_METHOD(JPy_Number_IntValue_MID, JPy_Number_JClass, "intValue", "()I");
    DEFINE_METHOD(JPy_Number_LongValue_MID , JPy_Number_JClass, "longValue", "()J");
    DEFINE_METHOD(JPy_Number_DoubleValue_MID, JPy_Number_JClass, "doubleValue", "()D");

    DEFINE_CLASS(JPy_Void_JClass, "java/lang/Void");

    DEFINE_CLASS(JPy_String_JClass, "java/lang/String");
    DEFINE_CLASS(JPy_Throwable_JClass, "java/lang/Throwable");
    DEFINE_CLASS(JPy_StackTraceElement_JClass, "java/lang/StackTraceElement");

    // Non-Object types: Primitive types and void.
    DEFINE_NON_OBJECT_TYPE(JPy_JBoolean, JPy_Boolean_JClass);
    DEFINE_NON_OBJECT_TYPE(JPy_JChar, JPy_Character_JClass);
    DEFINE_NON_OBJECT_TYPE(JPy_JByte, JPy_Byte_JClass);
    DEFINE_NON_OBJECT_TYPE(JPy_JShort, JPy_Short_JClass);
    DEFINE_NON_OBJECT_TYPE(JPy_JInt, JPy_Integer_JClass);
    DEFINE_NON_OBJECT_TYPE(JPy_JLong, JPy_Long_JClass);
    DEFINE_NON_OBJECT_TYPE(JPy_JFloat, JPy_Float_JClass);
    DEFINE_NON_OBJECT_TYPE(JPy_JDouble, JPy_Double_JClass);
    DEFINE_NON_OBJECT_TYPE(JPy_JVoid, JPy_Void_JClass);
    // The Java root object. Make sure, JPy_JObject is the first object type to be initialized.
    DEFINE_OBJECT_TYPE(JPy_JObject, JPy_Object_JClass);
    // Immediately after JPy_JObject, we define JPy_JClass. From now on JType_AddClassAttribute() works.
    DEFINE_OBJECT_TYPE(JPy_JClass, JPy_Class_JClass);
    // Primitive-Wrapper Objects.
    DEFINE_OBJECT_TYPE(JPy_JBooleanObj, JPy_Boolean_JClass);
    DEFINE_OBJECT_TYPE(JPy_JCharacterObj, JPy_Character_JClass);
    DEFINE_OBJECT_TYPE(JPy_JByteObj, JPy_Byte_JClass);
    DEFINE_OBJECT_TYPE(JPy_JShortObj, JPy_Short_JClass);
    DEFINE_OBJECT_TYPE(JPy_JIntegerObj, JPy_Integer_JClass);
    DEFINE_OBJECT_TYPE(JPy_JLongObj, JPy_Long_JClass);
    DEFINE_OBJECT_TYPE(JPy_JFloatObj, JPy_Float_JClass);
    DEFINE_OBJECT_TYPE(JPy_JDoubleObj, JPy_Double_JClass);
    // Other objects.
    DEFINE_OBJECT_TYPE(JPy_JString, JPy_String_JClass);
    DEFINE_OBJECT_TYPE(JPy_JThrowable, JPy_Throwable_JClass);
    DEFINE_OBJECT_TYPE(JPy_JStackTraceElement, JPy_StackTraceElement_JClass);
    DEFINE_METHOD(JPy_Throwable_getCause_MID, JPy_Throwable_JClass, "getCause", "()Ljava/lang/Throwable;");
    DEFINE_METHOD(JPy_Throwable_getStackTrace_MID, JPy_Throwable_JClass, "getStackTrace", "()[Ljava/lang/StackTraceElement;");

    // JType_AddClassAttribute is actually called from within JType_GetType(), but not for
    // JPy_JObject and JPy_JClass for an obvious reason. So we do it now:
    JType_AddClassAttribute(jenv, JPy_JObject);
    JType_AddClassAttribute(jenv, JPy_JClass);

    if (initGlobalPyObjectVars(jenv) < 0) {
        JPy_DIAG_PRINT(JPy_DIAG_F_ALL, "JPy_InitGlobalVars: JPy_JPyObject=%p, JPy_JPyModule=%p\n", JPy_JPyObject, JPy_JPyModule);
    }

    return 0;
}

void JPy_ClearGlobalVars(JNIEnv* jenv)
{
    if (jenv != NULL) {
        (*jenv)->DeleteGlobalRef(jenv, JPy_Comparable_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Object_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Class_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Constructor_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Method_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Field_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_RuntimeException_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Boolean_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Character_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Byte_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Short_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Integer_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Long_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Float_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Double_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Number_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_Void_JClass);
        (*jenv)->DeleteGlobalRef(jenv, JPy_String_JClass);
    }

    JPy_Comparable_JClass = NULL;
    JPy_Object_JClass = NULL;
    JPy_Class_JClass = NULL;
    JPy_Constructor_JClass = NULL;
    JPy_Method_JClass = NULL;
    JPy_Field_JClass = NULL;
    JPy_RuntimeException_JClass = NULL;
    JPy_Boolean_JClass = NULL;
    JPy_Character_JClass = NULL;
    JPy_Byte_JClass = NULL;
    JPy_Short_JClass = NULL;
    JPy_Integer_JClass = NULL;
    JPy_Long_JClass = NULL;
    JPy_Float_JClass = NULL;
    JPy_Double_JClass = NULL;
    JPy_Number_JClass = NULL;
    JPy_Void_JClass = NULL;
    JPy_String_JClass = NULL;

    JPy_Object_ToString_MID = NULL;
    JPy_Object_HashCode_MID = NULL;
    JPy_Object_Equals_MID = NULL;
    JPy_Class_GetName_MID = NULL;
    JPy_Class_GetDeclaredConstructors_MID = NULL;
    JPy_Class_GetDeclaredFields_MID = NULL;
    JPy_Class_GetDeclaredMethods_MID = NULL;
    JPy_Class_GetFields_MID = NULL;
    JPy_Class_GetMethods_MID = NULL;
    JPy_Class_GetComponentType_MID = NULL;
    JPy_Class_IsPrimitive_MID = NULL;
    JPy_Class_IsInterface_MID = NULL;
    JPy_Constructor_GetModifiers_MID = NULL;
    JPy_Constructor_GetParameterTypes_MID = NULL;
    JPy_Method_GetName_MID = NULL;
    JPy_Method_GetReturnType_MID = NULL;
    JPy_Method_GetParameterTypes_MID = NULL;
    JPy_Method_GetModifiers_MID = NULL;
    JPy_Field_GetName_MID = NULL;
    JPy_Field_GetModifiers_MID = NULL;
    JPy_Field_GetType_MID = NULL;
    JPy_Boolean_Init_MID = NULL;
    JPy_Boolean_BooleanValue_MID = NULL;
    JPy_Character_Init_MID = NULL;
    JPy_Character_CharValue_MID = NULL;
    JPy_Byte_Init_MID = NULL;
    JPy_Short_Init_MID = NULL;
    JPy_Integer_Init_MID = NULL;
    JPy_Long_Init_MID = NULL;
    JPy_Float_Init_MID = NULL;
    JPy_Double_Init_MID = NULL;
    JPy_Number_IntValue_MID = NULL;
    JPy_Number_LongValue_MID = NULL;
    JPy_Number_DoubleValue_MID = NULL;
    JPy_PyObject_GetPointer_MID = NULL;
    JPy_PyObject_UnwrapProxy_SMID = NULL;

    Py_XDECREF(JPy_JBoolean);
    Py_XDECREF(JPy_JChar);
    Py_XDECREF(JPy_JByte);
    Py_XDECREF(JPy_JShort);
    Py_XDECREF(JPy_JInt);
    Py_XDECREF(JPy_JLong);
    Py_XDECREF(JPy_JFloat);
    Py_XDECREF(JPy_JDouble);
    Py_XDECREF(JPy_JVoid);
    Py_XDECREF(JPy_JBooleanObj);
    Py_XDECREF(JPy_JCharacterObj);
    Py_XDECREF(JPy_JByteObj);
    Py_XDECREF(JPy_JShortObj);
    Py_XDECREF(JPy_JIntegerObj);
    Py_XDECREF(JPy_JLongObj);
    Py_XDECREF(JPy_JFloatObj);
    Py_XDECREF(JPy_JDoubleObj);
    Py_XDECREF(JPy_JPyObject);
    Py_XDECREF(JPy_JPyModule);

    JPy_JBoolean = NULL;
    JPy_JChar = NULL;
    JPy_JByte = NULL;
    JPy_JShort = NULL;
    JPy_JInt = NULL;
    JPy_JLong = NULL;
    JPy_JFloat = NULL;
    JPy_JDouble = NULL;
    JPy_JVoid = NULL;
    JPy_JString = NULL;
    JPy_JBooleanObj = NULL;
    JPy_JCharacterObj = NULL;
    JPy_JByteObj = NULL;
    JPy_JShortObj = NULL;
    JPy_JIntegerObj = NULL;
    JPy_JLongObj = NULL;
    JPy_JFloatObj = NULL;
    JPy_JDoubleObj = NULL;
    JPy_JPyObject = NULL;
    JPy_JPyModule = NULL;
}

#define AT_STRING "\tat "
#define AT_STRLEN 4
#define CAUSED_BY_STRING "caused by "
#define CAUSED_BY_STRLEN 10
#define ELIDED_STRING_MAX_SIZE 30

void JPy_HandleJavaException(JNIEnv* jenv)
{
    jthrowable error = (*jenv)->ExceptionOccurred(jenv);
    if (error != NULL) {
        jstring message;
        int allocError = 0;

        if (JPy_DiagFlags != 0) {
            (*jenv)->ExceptionDescribe(jenv);
        }

        if (JPy_VerboseExceptions) {
            char *stackTraceString;
            size_t stackTraceLength = 0;
            jthrowable cause = error;
            jarray enclosingElements = NULL;
            jint enclosingSize = 0;

            stackTraceString = strdup("");

            do {
                /* We want the type and the detail string, which is actually what a Throwable toString() does by
                 * default, as does the default printStackTrace(). */
                jint ii;

                jarray stackTrace;
                jint stackTraceElements;
                jint lastElementToPrint;
                jint enclosingIndex;

                if (stackTraceLength > 0) {
                    char *newStackString;

                    newStackString = realloc(stackTraceString, CAUSED_BY_STRLEN + 1 + stackTraceLength);
                    if (newStackString == NULL) {
                        allocError = 1;
                        break;
                    }
                    stackTraceString = newStackString;
                    strcat(stackTraceString, CAUSED_BY_STRING);
                    stackTraceLength += CAUSED_BY_STRLEN;
                }

                message = (jstring) (*jenv)->CallObjectMethod(jenv, cause, JPy_Object_ToString_MID);
                if (message != NULL) {
                    const char *messageChars = (*jenv)->GetStringUTFChars(jenv, message, NULL);
                    if (messageChars != NULL) {
                        char *newStackString;
                        size_t len = strlen(messageChars);

                        newStackString = realloc(stackTraceString, len + 2 + stackTraceLength);
                        if (newStackString == NULL) {
                            (*jenv)->ReleaseStringUTFChars(jenv, message, messageChars);
                            allocError = 1;
                            break;
                        }

                        stackTraceString = newStackString;
                        strcat(stackTraceString, messageChars);
                        stackTraceString[stackTraceLength + len] = '\n';
                        stackTraceString[stackTraceLength + len + 1] = '\0';
                        stackTraceLength += (len + 1);

                        (*jenv)->ReleaseStringUTFChars(jenv, message, messageChars);
                    } else {
                        allocError = 1;
                        break;
                    }
                    (*jenv)->DeleteLocalRef(jenv, message);
                }

                /* We should assemble a string based on the stack trace. */
                stackTrace = (*jenv)->CallObjectMethod(jenv, cause, JPy_Throwable_getStackTrace_MID);
                stackTraceElements = (*jenv)->GetArrayLength(jenv, stackTrace);
                lastElementToPrint = stackTraceElements - 1;
                enclosingIndex = enclosingSize - 1;

                while (lastElementToPrint >= 0 && enclosingIndex >= 0) {
                    jobject thisElement = (*jenv)->GetObjectArrayElement(jenv, stackTrace, lastElementToPrint);
                    jobject thatElement = (*jenv)->GetObjectArrayElement(jenv, enclosingElements, enclosingIndex);

                    // if they are equal, let's decrement, otherwise we break
                    jboolean  equal = (*jenv)->CallBooleanMethod(jenv, thisElement, JPy_Object_Equals_MID, thatElement);
                    if (!equal) {
                        break;
                    }

                    lastElementToPrint--;
                    enclosingIndex--;
                }

                for (ii = 0; ii <= lastElementToPrint; ++ii) {
                    jobject traceElement = (*jenv)->GetObjectArrayElement(jenv, stackTrace, ii);
                    if (traceElement != NULL) {
                        message = (jstring) (*jenv)->CallObjectMethod(jenv, traceElement, JPy_Object_ToString_MID);
                        if (message != NULL) {
                            size_t len;
                            char *newStackString;
                            const char *messageChars = (*jenv)->GetStringUTFChars(jenv, message, NULL);
                            if (messageChars == NULL) {
                                allocError = 1;
                                break;
                            }

                            len = strlen(messageChars);

                            newStackString = realloc(stackTraceString, len + 2 + AT_STRLEN + stackTraceLength);
                            if (newStackString == NULL) {
                                (*jenv)->ReleaseStringUTFChars(jenv, message, messageChars);
                                allocError = 1;
                                break;
                            }

                            stackTraceString = newStackString;
                            strcat(stackTraceString, AT_STRING);
                            strcat(stackTraceString, messageChars);
                            stackTraceString[stackTraceLength + len + AT_STRLEN] = '\n';
                            stackTraceString[stackTraceLength + len + AT_STRLEN + 1] = '\0';
                            stackTraceLength += (len + 1 + AT_STRLEN);

                            (*jenv)->ReleaseStringUTFChars(jenv, message, messageChars);
                        }

                    }
                }

                if (lastElementToPrint < stackTraceElements - 1) {
                    int written;
                    char *newStackString = realloc(stackTraceString, stackTraceLength + ELIDED_STRING_MAX_SIZE);
                    if (newStackString == NULL) {
                        allocError = 1;
                        break;
                    }

                    stackTraceString = newStackString;
                    stackTraceString[stackTraceLength + ELIDED_STRING_MAX_SIZE - 1] = '\0';

                    written = snprintf(stackTraceString + stackTraceLength, ELIDED_STRING_MAX_SIZE - 1, "\t... %d more\n", (stackTraceElements - lastElementToPrint) - 1);
                    if (written > (ELIDED_STRING_MAX_SIZE - 1)) {
                        stackTraceLength += (ELIDED_STRING_MAX_SIZE - 1);
                    } else {
                        stackTraceLength += written;
                    }
                }

                /** So we can eliminate extra entries. */
                enclosingElements = stackTrace;
                enclosingSize = stackTraceElements;

                /** Now the next cause. */
                cause = (*jenv)->CallObjectMethod(jenv, cause, JPy_Throwable_getCause_MID);
            } while (cause != NULL && !allocError);

            if (allocError == 0 && stackTraceString != NULL) {
                PyErr_Format(PyExc_RuntimeError, "%s", stackTraceString);
            } else {
                PyErr_SetString(PyExc_RuntimeError,
                                "Java VM exception occurred, but failed to allocate message text");
            }
            free(stackTraceString);
        } else {
            message = (jstring) (*jenv)->CallObjectMethod(jenv, error, JPy_Object_ToString_MID);
            if (message != NULL) {
                const char *messageChars;

                messageChars = (*jenv)->GetStringUTFChars(jenv, message, NULL);
                if (messageChars != NULL) {
                    PyErr_Format(PyExc_RuntimeError, "%s", messageChars);
                    (*jenv)->ReleaseStringUTFChars(jenv, message, messageChars);
                } else {
                    PyErr_SetString(PyExc_RuntimeError,
                                    "Java VM exception occurred, but failed to allocate message text");
                }
                (*jenv)->DeleteLocalRef(jenv, message);
            } else {
                PyErr_SetString(PyExc_RuntimeError, "Java VM exception occurred, no message");
            }
        }

        (*jenv)->DeleteLocalRef(jenv, error);
        (*jenv)->ExceptionClear(jenv);
    }
}

void JPy_free(void* unused)
{
    JPy_DIAG_PRINT(JPy_DIAG_F_ALL, "JPy_free: freeing module data...\n");
    JPy_ClearGlobalVars(NULL);

    JPy_Module = NULL;
    JPy_Types = NULL;
    JPy_Type_Callbacks = NULL;
    JPy_Type_Translations = NULL;
    JException_Type = NULL;

    JPy_DIAG_PRINT(JPy_DIAG_F_ALL, "JPy_free: done freeing module data\n");
}

