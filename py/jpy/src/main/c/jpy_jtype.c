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
#include "jpy_jtype.h"
#include "jpy_jfield.h"
#include "jpy_jmethod.h"
#include "jpy_jobj.h"
#include "jpy_conv.h"
#include "jpy_compat.h"


JPy_JType* JType_New(JNIEnv* jenv, jclass classRef, jboolean resolve);
int JType_ResolveType(JNIEnv* jenv, JPy_JType* type);
int JType_InitComponentType(JNIEnv* jenv, JPy_JType* type, jboolean resolve);
int JType_InitSuperType(JNIEnv* jenv, JPy_JType* type, jboolean resolve);
int JType_ProcessClassConstructors(JNIEnv* jenv, JPy_JType* type);
int JType_ProcessClassFields(JNIEnv* jenv, JPy_JType* type);
int JType_ProcessClassMethods(JNIEnv* jenv, JPy_JType* type);
int JType_AddMethod(JPy_JType* type, JPy_JMethod* method);
JPy_ReturnDescriptor* JType_CreateReturnDescriptor(JNIEnv* jenv, jclass returnType);
JPy_ParamDescriptor* JType_CreateParamDescriptors(JNIEnv* jenv, int paramCount, jarray paramTypes);
void JType_InitParamDescriptorFunctions(JPy_ParamDescriptor* paramDescriptor, jboolean isLastVarArg);
void JType_InitMethodParamDescriptorFunctions(JPy_JType* type, JPy_JMethod* method);
int JType_ProcessField(JNIEnv* jenv, JPy_JType* declaringType, PyObject* fieldKey, const char* fieldName, jclass fieldClassRef, jboolean isStatic, jboolean isFinal, jfieldID fid);
void JType_DisposeLocalObjectRefArg(JNIEnv* jenv, jvalue* value, void* data);
void JType_DisposeReadOnlyBufferArg(JNIEnv* jenv, jvalue* value, void* data);
void JType_DisposeWritableBufferArg(JNIEnv* jenv, jvalue* value, void* data);


static int JType_MatchVarArgPyArgAsFPType(const JPy_ParamDescriptor *paramDescriptor, PyObject *pyArg, int idx,
                                   struct JPy_JType *expectedType, int floatMatch);

static int JType_MatchVarArgPyArgIntType(const JPy_ParamDescriptor *paramDescriptor, PyObject *pyArg, int idx,
                                  struct JPy_JType *expectedComponentType);

JPy_JType* JType_GetTypeForObject(JNIEnv* jenv, jobject objectRef, jboolean resolve)
{
    JPy_JType* type;
    jclass classRef;
    classRef = (*jenv)->GetObjectClass(jenv, objectRef);
    type = JType_GetType(jenv, classRef, resolve);
    JPy_DELETE_LOCAL_REF(classRef);
    return type;
}


JPy_JType* JType_GetTypeForName(JNIEnv* jenv, const char* typeName, jboolean resolve)
{
    const char* resourceName;
    jclass classRef;
    JPy_JType *result;

    JPy_JType* javaType = NULL;
    if (strcmp(typeName, "boolean") == 0) {
        javaType = JPy_JBoolean;
    } else if (strcmp(typeName, "char") == 0) {
        javaType = JPy_JChar;
    } else if (strcmp(typeName, "byte") == 0) {
        javaType = JPy_JByte;
    } else if (strcmp(typeName, "short") == 0) {
        javaType = JPy_JShort;
    } else if (strcmp(typeName, "int") == 0) {
        javaType = JPy_JInt;
    } else if (strcmp(typeName, "long") == 0) {
        javaType = JPy_JLong;
    } else if (strcmp(typeName, "float") == 0) {
        javaType = JPy_JFloat;
    } else if (strcmp(typeName, "double") == 0) {
        javaType = JPy_JDouble;
    } else if (strcmp(typeName, "void") == 0) {
        javaType = JPy_JVoid;
    }

    if (javaType != NULL) {
        JPy_INCREF(javaType);
        return javaType;
    }

    if (strchr(typeName, '.') != NULL) {
        // resourceName: Replace dots '.' by slashes '/'
        char* c;
        resourceName = PyMem_New(char, strlen(typeName) + 1);
        if (resourceName == NULL) {
            PyErr_NoMemory();
            return NULL;
        }
        strcpy((char*) resourceName, typeName);
        c = (char*) resourceName;
        while ((c = strchr(c, '.')) != NULL) {
            *c = '/';
        }
    } else {
        resourceName = typeName;
    }

    JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JType_GetTypeForName: typeName='%s', resourceName='%s'\n", typeName, resourceName);

    classRef = (*jenv)->FindClass(jenv, resourceName);

    if (typeName != resourceName) {
        PyMem_Del((char*) resourceName);
    }

    if (classRef == NULL || (*jenv)->ExceptionCheck(jenv)) {
        (*jenv)->ExceptionClear(jenv);
        PyErr_Format(PyExc_ValueError, "Java class '%s' not found", typeName);
        return NULL;
    }

    result = JType_GetType(jenv, classRef, resolve);
    JPy_DELETE_LOCAL_REF(classRef);
    return result;
}

/**
 * Returns a new reference.
 */
JPy_JType* JType_GetType(JNIEnv* jenv, jclass classRef, jboolean resolve)
{
    PyObject* typeKey;
    PyObject* typeValue;
    JPy_JType* type;
    jboolean found;

    if (JPy_Types == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "jpy internal error: module 'jpy' not initialized");
        return NULL;
    }

    typeKey = JPy_FromTypeName(jenv, classRef);
    if (typeKey == NULL) {
        return NULL;
    }

    typeValue = PyDict_GetItem(JPy_Types, typeKey);
    if (typeValue == NULL) {

        found = JNI_FALSE;

        // Create a new type instance
        type = JType_New(jenv, classRef, resolve);
        if (type == NULL) {
            JPy_DECREF(typeKey);
            return NULL;
        }

        //printf("T1: type->tp_init=%p\n", ((PyTypeObject*)type)->tp_init);

        // In order to avoid infinite recursion, we have to register the new (but yet incomplete) type first...
        PyDict_SetItem(JPy_Types, typeKey, (PyObject*) type);

        //printf("T2: type->tp_init=%p\n", ((PyTypeObject*)type)->tp_init);

        // ... before we can continue processing the super type ...
        if (JType_InitSuperType(jenv, type, resolve) < 0) {
            PyDict_DelItem(JPy_Types, typeKey);
            return NULL;
        }

        //printf("T3: type->tp_init=%p\n", ((PyTypeObject*)type)->tp_init);

        // ... and processing the component type.
        if (JType_InitComponentType(jenv, type, resolve) < 0) {
            PyDict_DelItem(JPy_Types, typeKey);
            return NULL;
        }

        //printf("T4: type->tp_init=%p\n", ((PyTypeObject*)type)->tp_init);

        // Finally we initialise the type's slots, so that our JObj instances behave pythonic.
        if (JType_InitSlots(type) < 0) {
            JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JType_GetType: error: JType_InitSlots() failed for javaName=\"%s\"\n", type->javaName);
            PyDict_DelItem(JPy_Types, typeKey);
            return NULL;
        }

        JType_AddClassAttribute(jenv, type);

        //printf("T5: type->tp_init=%p\n", ((PyTypeObject*)type)->tp_init);

    } else {
        jboolean isTypeInProgress = typeValue->ob_type == &JType_Type;
        jboolean isFinalizedType = PyType_Check(typeValue);

        found = JNI_TRUE;

        if (!isTypeInProgress && !isFinalizedType) {
            JPy_DIAG_PRINT(JPy_DIAG_F_ALL, "JType_GetType: INTERNAL ERROR: illegal typeValue=%p (type '%s') for typeKey=%p (type '%s') in 'jpy.%s'\n",
                           typeValue, Py_TYPE(typeValue)->tp_name,
                           typeKey, Py_TYPE(typeKey)->tp_name,
                           JPy_MODULE_ATTR_NAME_TYPES);
            PyErr_Format(PyExc_RuntimeError,
                         "jpy internal error: attributes in 'jpy.%s' must be of type '%s', but found a '%s'",
                         JPy_MODULE_ATTR_NAME_TYPES, JType_Type.tp_name, Py_TYPE(typeValue)->tp_name);
            JPy_DECREF(typeKey);
            return NULL;
        }

        JPy_DECREF(typeKey);
        type = (JPy_JType*) typeValue;
    }

    JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JType_GetType: javaName=\"%s\", found=%d, resolve=%d, resolved=%d, type=%p\n", type->javaName, found, resolve, type->isResolved, type);

    if (!type->isResolved && resolve) {
        if (JType_ResolveType(jenv, type) < 0) {
            return NULL;
        }
    }
    
    JPy_INCREF(type);
    return type;
}

/**
 * Creates a type instance of the meta type 'JType_Type'.
 * Such type instances are used as types for Java Objects in Python.
 */
JPy_JType* JType_New(JNIEnv* jenv, jclass classRef, jboolean resolve)
{
    PyTypeObject* metaType;
    JPy_JType* type;

    metaType = &JType_Type;

    type = (JPy_JType*) metaType->tp_alloc(metaType, 0);
    if (type == NULL) {
        return NULL;
    }

    // printf("=============================================== JType_New: type->ob_type=%p\n", ((PyObject*)type)->ob_type);

    type->classRef = NULL;
    type->isResolved = JNI_FALSE;
    type->isResolving = JNI_FALSE;

    type->javaName = JPy_GetTypeName(jenv, classRef);
    if (type->javaName == NULL) {
        metaType->tp_free(type);
        return NULL;
    }
    // The object type's name. Note that the reference is borrowed from type->javaName.
    type->typeObj.tp_name = type->javaName;
    // tp_init is used to identify objects instances of type jpy.JType. Make sure it is initially NULL.
    type->typeObj.tp_init = NULL;

    type->classRef = (*jenv)->NewGlobalRef(jenv, classRef);
    if (type->classRef == NULL) {
        PyMem_Del(type->javaName);
        type->javaName = NULL;
        metaType->tp_free(type);
        PyErr_NoMemory();
        return NULL;
    }

    type->isPrimitive = (*jenv)->CallBooleanMethod(jenv, type->classRef, JPy_Class_IsPrimitive_MID);
    if ((*jenv)->ExceptionCheck(jenv)) {
        (*jenv)->ExceptionClear(jenv);
        PyMem_Del(type->javaName);
        type->javaName = NULL;
        metaType->tp_free(type);
        return NULL;
    }
    type->isInterface = (*jenv)->CallBooleanMethod(jenv, type->classRef, JPy_Class_IsInterface_MID);
    if ((*jenv)->ExceptionCheck(jenv)) {
        (*jenv)->ExceptionClear(jenv);
        PyMem_Del(type->javaName);
        type->javaName = NULL;
        metaType->tp_free(type);
        return NULL;
    }

    JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JType_New: javaName=\"%s\", resolve=%d, type=%p\n", type->javaName, resolve, type);

    return type;
}

PyObject* JType_ConvertJavaToPythonObject(JNIEnv* jenv, JPy_JType* type, jobject objectRef)
{
    if (objectRef == NULL) {
        return JPy_FROM_JNULL();
    }

    if (type->componentType == NULL) {
        // Scalar type, not an array, try to convert to Python equivalent
        if (type == JPy_JBooleanObj || type == JPy_JBoolean) {
            jboolean value = (*jenv)->CallBooleanMethod(jenv, objectRef, JPy_Boolean_BooleanValue_MID);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            return JPy_FROM_JBOOLEAN(value);
        } else if (type == JPy_JCharacterObj || type == JPy_JChar) {
            jchar value = (*jenv)->CallCharMethod(jenv, objectRef, JPy_Character_CharValue_MID);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            return JPy_FROM_JCHAR(value);
        } else if (type == JPy_JByteObj || type == JPy_JShortObj || type == JPy_JIntegerObj || type == JPy_JShort || type == JPy_JInt) {
            jint value = (*jenv)->CallIntMethod(jenv, objectRef, JPy_Number_IntValue_MID);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            return JPy_FROM_JINT(value);
        } else if (type == JPy_JLongObj || type == JPy_JLong) {
            jlong value = (*jenv)->CallLongMethod(jenv, objectRef, JPy_Number_LongValue_MID);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            return JPy_FROM_JLONG(value);
        } else if (type == JPy_JFloatObj || type == JPy_JDoubleObj || type == JPy_JFloat || type == JPy_JDouble) {
            jdouble value = (*jenv)->CallDoubleMethod(jenv, objectRef, JPy_Number_DoubleValue_MID);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            return JPy_FROM_JDOUBLE(value);
        } else if (type == JPy_JPyObject || type == JPy_JPyModule) {
            jlong value = (*jenv)->CallLongMethod(jenv, objectRef, JPy_PyObject_GetPointer_MID);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            PyObject* pyObj = (PyObject*) value;
            JPy_INCREF(pyObj);
            return pyObj;
        } else if (type == JPy_JString) {
            return JPy_FromJString(jenv, objectRef);
        } else if (type == JPy_JObject) {
            type = JType_GetTypeForObject(jenv, objectRef, JNI_FALSE);
            if (type != JPy_JObject) {
                return JType_ConvertJavaToPythonObject(jenv, type, objectRef);
            }
        } else if (JPy_JPyObject != NULL) {
            // Note: JPy_JPyObject == NULL means that org.jpy.PyObject has not been loaded from the
            // java classpath, which is OK. This is common when using jpy from Python.
            // If org.jpy.PyObject has not been loaded, we know the object isn't a proxy.

            // If this object was created by PyObject#createProxy, let's unwrap it and get back original PyObject.
            jobject jPyObject = (*jenv)->CallStaticObjectMethod(jenv, JPy_JPyObject->classRef, JPy_PyObject_UnwrapProxy_SMID, objectRef);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            if (jPyObject != NULL) {
                // We know that jPyObject is of the proper type, no need to check it.
                jlong value = (*jenv)->CallLongMethod(jenv, jPyObject, JPy_PyObject_GetPointer_MID);
                JPy_DELETE_LOCAL_REF(jPyObject);
                JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
                PyObject* pyObj = (PyObject*) value;
                JPy_INCREF(pyObj);
                return pyObj;
            }
        }
    }

    // For all other types than the ones handled above (namely primitive types, PyObject+PyModule, and String),
    // we create a Java object wrapper
    return (PyObject*) JObj_FromType(jenv, type, objectRef);

    // Tried the following in order to return objects that have the actual type instead of the declared return type.
    // Problem occurs if Java collections are used: These return java.lang.Object items and must be explicitly
    // cast to the desired type in Python using jpy.cast(obj, type).
    // Anyway, JObj_New() is 2x slower than JObj_FromType() and it may require definition of Java type wrappers for
    // actually private implementation classes. For example: A java.util.List is filled with java.nio.file.Path objects,
    // but List.get() returns a java.lang.Object. Returning the actual type is platform dependent. E.g. on
    // Windows, it returns an instance of the internal sun.nio.fs.WindowsPath class.
    //
    //return (PyObject*) JObj_New(jenv, objectRef);
}

int JType_PythonToJavaConversionError(JPy_JType* type, PyObject* pyArg)
{
    PyErr_Format(PyExc_ValueError, "cannot convert a Python '%s' to a Java '%s'", Py_TYPE(pyArg)->tp_name, type->javaName);
    return -1;
}

int JType_CreateJavaObject(JNIEnv* jenv, JPy_JType* type, PyObject* pyArg, jclass classRef, jmethodID initMID, jvalue value, jobject* objectRef)
{
    Py_BEGIN_ALLOW_THREADS;
    *objectRef = (*jenv)->NewObjectA(jenv, classRef, initMID, &value);
    Py_END_ALLOW_THREADS;
    if (*objectRef == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    return 0;
}

int JType_CreateJavaObject_2(JNIEnv* jenv, JPy_JType* type, PyObject* pyArg, jclass classRef, jmethodID initMID, jvalue value1, jvalue value2, jobject* objectRef)
{
    Py_BEGIN_ALLOW_THREADS;
    *objectRef = (*jenv)->NewObject(jenv, classRef, initMID, value1, value2);
    Py_END_ALLOW_THREADS;
    if (*objectRef == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    return 0;
}

int JType_CreateJavaBooleanObject(JNIEnv* jenv, JPy_JType* type, PyObject* pyArg, jobject* objectRef)
{
    jvalue value;
    if (PyBool_Check(pyArg) || JPy_IS_CLONG(pyArg)) {
        value.z = JPy_AS_JBOOLEAN(pyArg);
    } else {
        return JType_PythonToJavaConversionError(type, pyArg);
    }
    return JType_CreateJavaObject(jenv, type, pyArg, JPy_Boolean_JClass, JPy_Boolean_Init_MID, value, objectRef);
}

int JType_CreateJavaCharacterObject(JNIEnv* jenv, JPy_JType* type, PyObject* pyArg, jobject* objectRef)
{
    jvalue value;
    if (JPy_IS_CLONG(pyArg)) {
        value.c = JPy_AS_JCHAR(pyArg);
    } else {
        return JType_PythonToJavaConversionError(type, pyArg);
    }
    return JType_CreateJavaObject(jenv, type, pyArg, JPy_Character_JClass, JPy_Character_Init_MID, value, objectRef);
}

int JType_CreateJavaByteObject(JNIEnv* jenv, JPy_JType* type, PyObject* pyArg, jobject* objectRef)
{
    jvalue value;
    if (JPy_IS_CLONG(pyArg)) {
        value.b = JPy_AS_JBYTE(pyArg);
    } else {
        return JType_PythonToJavaConversionError(type, pyArg);
    }
    return JType_CreateJavaObject(jenv, type, pyArg, JPy_Byte_JClass, JPy_Byte_Init_MID, value, objectRef);
}

int JType_CreateJavaShortObject(JNIEnv* jenv, JPy_JType* type, PyObject* pyArg, jobject* objectRef)
{
    jvalue value;
    if (JPy_IS_CLONG(pyArg)) {
        value.s = JPy_AS_JSHORT(pyArg);
    } else {
        return JType_PythonToJavaConversionError(type, pyArg);
    }
    return JType_CreateJavaObject(jenv, type, pyArg, JPy_Short_JClass, JPy_Short_Init_MID, value, objectRef);
}

int JType_CreateJavaNumberFromPythonInt(JNIEnv* jenv, JPy_JType* type, PyObject* pyArg, jobject* objectRef)
{
    jvalue value;
    char b;
    short s;
    long i;
    long long j;
    j = JPy_AS_JLONG(pyArg);
    i = (int) j;
    s = (short) j;
    b = (char) j;

    if (i != j) {
        value.j = j;
        return JType_CreateJavaObject(jenv, type, pyArg, JPy_Long_JClass, JPy_Long_Init_MID, value, objectRef);
    }
    if (s != i) {
        value.i = i;
        return JType_CreateJavaObject(jenv, type, pyArg, JPy_Integer_JClass, JPy_Integer_Init_MID, value,
                                     objectRef);
    }
    if (b != s) {
        value.s = s;
        return JType_CreateJavaObject(jenv, type, pyArg, JPy_Short_JClass, JPy_Short_Init_MID, value,
                                      objectRef);
    }
    value.b = b;
    return JType_CreateJavaObject(jenv, type, pyArg, JPy_Byte_JClass, JPy_Byte_Init_MID, value, objectRef);
}

int JType_CreateJavaIntegerObject(JNIEnv* jenv, JPy_JType* type, PyObject* pyArg, jobject* objectRef)
{
    jvalue value;
    if (JPy_IS_CLONG(pyArg)) {
        value.i = JPy_AS_JINT(pyArg);
    } else {
        return JType_PythonToJavaConversionError(type, pyArg);
    }
    return JType_CreateJavaObject(jenv, type, pyArg, JPy_Integer_JClass, JPy_Integer_Init_MID, value, objectRef);
}

int JType_CreateJavaLongObject(JNIEnv* jenv, JPy_JType* type, PyObject* pyArg, jobject* objectRef)
{
    jvalue value;
    if (JPy_IS_CLONG(pyArg)) {
        value.j = JPy_AS_JLONG(pyArg);
    } else {
        return JType_PythonToJavaConversionError(type, pyArg);
    }
    return JType_CreateJavaObject(jenv, type, pyArg, JPy_Long_JClass, JPy_Long_Init_MID, value, objectRef);
}

int JType_CreateJavaFloatObject(JNIEnv* jenv, JPy_JType* type, PyObject* pyArg, jobject* objectRef)
{
    jvalue value;
    if (JPy_IS_CLONG(pyArg)) {
        value.f = (jfloat) JPy_AS_JLONG(pyArg);
    } else if (PyFloat_Check(pyArg)) {
        value.f = JPy_AS_JFLOAT(pyArg);
    } else {
        return JType_PythonToJavaConversionError(type, pyArg);
    }
    return JType_CreateJavaObject(jenv, type, pyArg, JPy_Float_JClass, JPy_Float_Init_MID, value, objectRef);
}

int JType_CreateJavaDoubleObject(JNIEnv* jenv, JPy_JType* type, PyObject* pyArg, jobject* objectRef)
{
    jvalue value;
    if (JPy_IS_CLONG(pyArg)) {
        value.d = (jdouble) JPy_AS_JLONG(pyArg);
    } else if (PyFloat_Check(pyArg)) {
        value.d = JPy_AS_JDOUBLE(pyArg);
    } else {
        return JType_PythonToJavaConversionError(type, pyArg);
    }
    return JType_CreateJavaObject(jenv, type, pyArg, JPy_Double_JClass, JPy_Double_Init_MID, value, objectRef);
}

int JType_CreateJavaPyObject(JNIEnv* jenv, JPy_JType* type, PyObject* pyArg, jobject* objectRef)
{
    jvalue value1;
    jvalue value2;

    value1.j = (jlong) pyArg;
    value2.z = JNI_TRUE;

    JPy_INCREF(pyArg);
    return JType_CreateJavaObject_2(jenv, type, pyArg, type->classRef, JPy_PyObject_Init_MID, value1, value2, objectRef);
}

int JType_CreateJavaArray(JNIEnv* jenv, JPy_JType* componentType, PyObject* pyArg, jobject* objectRef, jboolean allowObjectWrapping)
{
    jint itemCount;
    jarray arrayRef;
    jint index;
    PyObject* pyItem;

    if (pyArg == Py_None) {
        itemCount = 0;
    } else if (PySequence_Check(pyArg)) {
        itemCount = PySequence_Length(pyArg);
        if (itemCount < 0) {
            return -1;
        }
    } else {
        PyErr_Format(PyExc_ValueError, "cannot convert a Python '%s' to a Java array of type '%s'", Py_TYPE(pyArg)->tp_name, componentType->javaName);
        return -1;
    }

    arrayRef = NULL;

    if (componentType == JPy_JBoolean) {
        arrayRef = (*jenv)->NewBooleanArray(jenv, itemCount);
        if (arrayRef == NULL || (*jenv)->ExceptionCheck(jenv)) {
            JPy_HandleJavaException(jenv);
            return -1;
        }
        if (itemCount > 0) {
            jboolean* items = (*jenv)->GetBooleanArrayElements(jenv, arrayRef, NULL);
            if (items == NULL) {
                JPy_DELETE_LOCAL_REF(arrayRef);
                PyErr_NoMemory();
                return -1;
            }
            for (index = 0; index < itemCount; index++) {
                pyItem = PySequence_GetItem(pyArg, index);
                if (pyItem == NULL) {
                    (*jenv)->ReleaseBooleanArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
                items[index] = JPy_AS_JBOOLEAN(pyItem);
                JPy_DECREF(pyItem);
                if (PyErr_Occurred()) {
                    (*jenv)->ReleaseBooleanArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
            }
            (*jenv)->ReleaseBooleanArrayElements(jenv, arrayRef, items, 0);
        }
    } else if (componentType == JPy_JByte) {
        arrayRef = (*jenv)->NewByteArray(jenv, itemCount);
        if (arrayRef == NULL || (*jenv)->ExceptionCheck(jenv)) {
            JPy_HandleJavaException(jenv);
            return -1;
        }
        if (itemCount > 0) {
            jbyte* items = (*jenv)->GetByteArrayElements(jenv, arrayRef, NULL);
            if (items == NULL) {
                JPy_DELETE_LOCAL_REF(arrayRef);
                PyErr_NoMemory();
                return -1;
            }
            for (index = 0; index < itemCount; index++) {
                pyItem = PySequence_GetItem(pyArg, index);
                if (pyItem == NULL) {
                    (*jenv)->ReleaseByteArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
                items[index] = JPy_AS_JBYTE(pyItem);
                JPy_DECREF(pyItem);
                if (PyErr_Occurred()) {
                    (*jenv)->ReleaseByteArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
            }
            (*jenv)->ReleaseByteArrayElements(jenv, arrayRef, items, 0);
        }
    } else if (componentType == JPy_JChar) {
        arrayRef = (*jenv)->NewCharArray(jenv, itemCount);
        if (arrayRef == NULL || (*jenv)->ExceptionCheck(jenv)) {
            JPy_HandleJavaException(jenv);
            return -1;
        }
        if (itemCount > 0) {
            jchar* items = (*jenv)->GetCharArrayElements(jenv, arrayRef, NULL);
            if (items == NULL) {
                JPy_DELETE_LOCAL_REF(arrayRef);
                PyErr_NoMemory();
                return -1;
            }
            for (index = 0; index < itemCount; index++) {
                pyItem = PySequence_GetItem(pyArg, index);
                if (pyItem == NULL) {
                    (*jenv)->ReleaseCharArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
                items[index] = JPy_AS_JCHAR(pyItem);
                JPy_DECREF(pyItem);
                if (PyErr_Occurred()) {
                    (*jenv)->ReleaseCharArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
            }
            (*jenv)->ReleaseCharArrayElements(jenv, arrayRef, items, 0);
        }
    } else if (componentType == JPy_JShort) {
        arrayRef = (*jenv)->NewShortArray(jenv, itemCount);
        if (arrayRef == NULL || (*jenv)->ExceptionCheck(jenv)) {
            JPy_HandleJavaException(jenv);
            return -1;
        }
        if (itemCount > 0) {
            jshort* items = (*jenv)->GetShortArrayElements(jenv, arrayRef, NULL);
            if (items == NULL) {
                JPy_DELETE_LOCAL_REF(arrayRef);
                PyErr_NoMemory();
                return -1;
            }
            for (index = 0; index < itemCount; index++) {
                pyItem = PySequence_GetItem(pyArg, index);
                if (pyItem == NULL) {
                    (*jenv)->ReleaseShortArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
                items[index] = JPy_AS_JSHORT(pyItem);
                JPy_DECREF(pyItem);
                if (PyErr_Occurred()) {
                    (*jenv)->ReleaseShortArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
            }
            (*jenv)->ReleaseShortArrayElements(jenv, arrayRef, items, 0);
        }
    } else if (componentType == JPy_JInt) {
        arrayRef = (*jenv)->NewIntArray(jenv, itemCount);
        if (arrayRef == NULL || (*jenv)->ExceptionCheck(jenv)) {
            JPy_HandleJavaException(jenv);
            return -1;
        }
        if (itemCount > 0) {
            jint* items = (*jenv)->GetIntArrayElements(jenv, arrayRef, NULL);
            if (items == NULL) {
                JPy_DELETE_LOCAL_REF(arrayRef);
                PyErr_NoMemory();
                return -1;
            }
            for (index = 0; index < itemCount; index++) {
                pyItem = PySequence_GetItem(pyArg, index);
                if (pyItem == NULL) {
                    (*jenv)->ReleaseIntArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
                items[index] = JPy_AS_JINT(pyItem);
                JPy_DECREF(pyItem);
                if (PyErr_Occurred()) {
                    (*jenv)->ReleaseIntArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
            }
            (*jenv)->ReleaseIntArrayElements(jenv, arrayRef, items, 0);
        }
    } else if (componentType == JPy_JLong) {
        arrayRef = (*jenv)->NewLongArray(jenv, itemCount);
        if (arrayRef == NULL || (*jenv)->ExceptionCheck(jenv)) {
            JPy_HandleJavaException(jenv);
            return -1;
        }
        if (itemCount > 0) {
            jlong* items = (*jenv)->GetLongArrayElements(jenv, arrayRef, NULL);
            if (items == NULL) {
                JPy_DELETE_LOCAL_REF(arrayRef);
                PyErr_NoMemory();
                return -1;
            }
            for (index = 0; index < itemCount; index++) {
                pyItem = PySequence_GetItem(pyArg, index);
                if (pyItem == NULL) {
                    (*jenv)->ReleaseLongArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
                items[index] = JPy_AS_JLONG(pyItem);
                JPy_DECREF(pyItem);
                if (PyErr_Occurred()) {
                    (*jenv)->ReleaseLongArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
            }
            (*jenv)->ReleaseLongArrayElements(jenv, arrayRef, items, 0);
        }
    } else if (componentType == JPy_JFloat) {
        arrayRef = (*jenv)->NewFloatArray(jenv, itemCount);
        if (arrayRef == NULL || (*jenv)->ExceptionCheck(jenv)) {
            JPy_HandleJavaException(jenv);
            return -1;
        }
        if (itemCount > 0) {
            jfloat* items = (*jenv)->GetFloatArrayElements(jenv, arrayRef, NULL);
            if (items == NULL) {
                JPy_DELETE_LOCAL_REF(arrayRef);
                PyErr_NoMemory();
                return -1;
            }
            for (index = 0; index < itemCount; index++) {
                pyItem = PySequence_GetItem(pyArg, index);
                if (pyItem == NULL) {
                    (*jenv)->ReleaseFloatArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
                items[index] = JPy_AS_JFLOAT(pyItem);
                JPy_DECREF(pyItem);
                if (PyErr_Occurred()) {
                    (*jenv)->ReleaseFloatArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
            }
            (*jenv)->ReleaseFloatArrayElements(jenv, arrayRef, items, 0);
        }
    } else if (componentType == JPy_JDouble) {
        arrayRef = (*jenv)->NewDoubleArray(jenv, itemCount);
        if (arrayRef == NULL || (*jenv)->ExceptionCheck(jenv)) {
            JPy_HandleJavaException(jenv);
            return -1;
        }
        if (itemCount > 0) {
            jdouble* items = (*jenv)->GetDoubleArrayElements(jenv, arrayRef, NULL);
            if (items == NULL) {
                JPy_DELETE_LOCAL_REF(arrayRef);
                PyErr_NoMemory();
                return -1;
            }
            for (index = 0; index < itemCount; index++) {
                pyItem = PySequence_GetItem(pyArg, index);
                if (pyItem == NULL) {
                    (*jenv)->ReleaseDoubleArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
                items[index] = JPy_AS_JDOUBLE(pyItem);
                JPy_DECREF(pyItem);
                if (PyErr_Occurred()) {
                    (*jenv)->ReleaseDoubleArrayElements(jenv, arrayRef, items, 0);
                    JPy_DELETE_LOCAL_REF(arrayRef);
                    return -1;
                }
            }
            (*jenv)->ReleaseDoubleArrayElements(jenv, arrayRef, items, 0);
        }
    } else if (!componentType->isPrimitive) {
        jobject jItem;
        arrayRef = (*jenv)->NewObjectArray(jenv, itemCount, componentType->classRef, NULL);
        if (arrayRef == NULL || (*jenv)->ExceptionCheck(jenv)) {
            JPy_HandleJavaException(jenv);
            return -1;
        }
        for (index = 0; index < itemCount; index++) {
            pyItem = PySequence_GetItem(pyArg, index);
            if (pyItem == NULL) {
                JPy_DELETE_LOCAL_REF(arrayRef);
                return -1;
            }
            if (JType_ConvertPythonToJavaObject(jenv, componentType, pyItem, &jItem, allowObjectWrapping) < 0) {
                JPy_DELETE_LOCAL_REF(arrayRef);
                JPy_DECREF(pyItem);
                return -1;
            }
            JPy_DECREF(pyItem);
            (*jenv)->SetObjectArrayElement(jenv, arrayRef, index, jItem);
            if ((*jenv)->ExceptionCheck(jenv)) {
                JPy_DELETE_LOCAL_REF(arrayRef);
                JPy_DELETE_LOCAL_REF(jItem);
                JPy_HandleJavaException(jenv);
                return -1;
            }
        }
    } else {
        PyErr_Format(PyExc_ValueError, "illegal Java array component type %s", componentType->javaName);
        return -1;
    }

    *objectRef = arrayRef;
    return 0;
}

int JType_ConvertPythonToJavaObject(JNIEnv* jenv, JPy_JType* type, PyObject* pyArg, jobject* objectRef, jboolean allowObjectWrapping)
{
    // Note: There may be a potential memory leak here.
    // If a new local reference is created in this function and assigned to *objectRef, the reference may escape.
    // If the reference is created for an argument to a JNI call, we already delete the ref (see JMethod_InvokeMethod()).
    // In other cases we actually have to call (*jenv)->DeleteLocalRef(jenv, *objectRef) some time later.

    if (pyArg == Py_None) {
        // None converts into NULL
        *objectRef = NULL;
        // todo: if the return type is PyObject, we should return Py_None instead
        return 0;
    }

    // If it is already a Java object wrapper JObj, and assignable, then we are done
    if (JObj_Check(pyArg)) {
        jobject jobj = ((JPy_JObj*) pyArg)->objectRef;
        jclass jobjclass = (*jenv)->GetObjectClass(jenv, jobj);

        if ((*jenv)->IsAssignableFrom(jenv, jobjclass, type->classRef)) {
            JPy_DELETE_LOCAL_REF(jobjclass);

            JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JType_ConvertPythonToJavaObject: unwrapping JObj into type->javaName=\"%s\"\n", type->javaName);

            jobj = (*jenv)->NewLocalRef(jenv, jobj);  // need a new reference to it
            *objectRef = jobj;
            if (jobj == NULL) {
                PyErr_NoMemory();
                return -1;
            }
            return 0;
        }
        JPy_DELETE_LOCAL_REF(jobjclass);
    }

    // If it is already a Java type wrapper JType, and assignable, then we are done
    if (JType_Check(pyArg)) {
        jclass jobj = ((JPy_JType*)pyArg)->classRef;
        jclass jobjclass = (*jenv)->GetObjectClass(jenv, jobj);

        if ((*jenv)->IsAssignableFrom(jenv, jobjclass, type->classRef)) {
            JPy_DELETE_LOCAL_REF(jobjclass);

            JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JType_ConvertPythonToJavaObject: unwrapping JType into type->javaName=\"%s\"\n", type->javaName);

            jobj = (*jenv)->NewLocalRef(jenv, jobj);  // need a new reference to it
            *objectRef = jobj;
            if (jobj == NULL) {
                PyErr_NoMemory();
                return -1;
            }
            return 0;
        }
        JPy_DELETE_LOCAL_REF(jobjclass);
    }

    if (type->componentType != NULL) {
        // For any other Python argument create a Java object (a new local reference)
        return JType_CreateJavaArray(jenv, type->componentType, pyArg, objectRef, allowObjectWrapping);
    } else if (type == JPy_JBoolean || type == JPy_JBooleanObj) {
        return JType_CreateJavaBooleanObject(jenv, type, pyArg, objectRef);
    } else if (type == JPy_JChar || type == JPy_JCharacterObj) {
        return JType_CreateJavaCharacterObject(jenv, type, pyArg, objectRef);
    } else if (type == JPy_JByte || type == JPy_JByteObj) {
        return JType_CreateJavaByteObject(jenv, type, pyArg, objectRef);
    } else if (type == JPy_JShort || type == JPy_JShortObj) {
        return JType_CreateJavaShortObject(jenv, type, pyArg, objectRef);
    } else if (type == JPy_JInt || type == JPy_JIntegerObj) {
        return JType_CreateJavaIntegerObject(jenv, type, pyArg, objectRef);
    } else if (type == JPy_JLong || type == JPy_JLongObj) {
        return JType_CreateJavaLongObject(jenv, type, pyArg, objectRef);
    } else if (type == JPy_JFloat || type == JPy_JFloatObj) {
        return JType_CreateJavaFloatObject(jenv, type, pyArg, objectRef);
    } else if (type == JPy_JDouble || type == JPy_JDoubleObj) {
        return JType_CreateJavaDoubleObject(jenv, type, pyArg, objectRef);
    } else if (type == JPy_JPyObject) {
        return JType_CreateJavaPyObject(jenv, type, pyArg, objectRef);
    } else if (JPy_IS_STR(pyArg) && (type == JPy_JString || type == JPy_JObject || ((*jenv)->IsAssignableFrom(jenv, JPy_JString->classRef, type->classRef)))) {
        return JPy_AsJString(jenv, pyArg, objectRef);
    } else if (PyBool_Check(pyArg) && (type == JPy_JObject || ((*jenv)->IsAssignableFrom(jenv, JPy_Boolean_JClass, type->classRef)))) {
        return JType_CreateJavaBooleanObject(jenv, type, pyArg, objectRef);
    } else if (JPy_IS_CLONG(pyArg) && (type == JPy_JObject || (*jenv)->IsAssignableFrom(jenv, JPy_Number_JClass, type->classRef))) {
        return JType_CreateJavaNumberFromPythonInt(jenv, type, pyArg, objectRef);
    } else if (JPy_IS_CLONG(pyArg) && ((*jenv)->IsAssignableFrom(jenv, JPy_Integer_JClass, type->classRef))) {
        return JType_CreateJavaIntegerObject(jenv, type, pyArg, objectRef);
    } else if (JPy_IS_CLONG(pyArg) && (type == JPy_JObject || ((*jenv)->IsAssignableFrom(jenv, JPy_Long_JClass, type->classRef)))) {
        return JType_CreateJavaLongObject(jenv, type, pyArg, objectRef);
    } else if (PyFloat_Check(pyArg) && (type == JPy_JObject || ((*jenv)->IsAssignableFrom(jenv, JPy_Double_JClass, type->classRef)))) {
        return JType_CreateJavaDoubleObject(jenv, type, pyArg, objectRef);
    } else if (PyFloat_Check(pyArg) && (type == JPy_JObject || ((*jenv)->IsAssignableFrom(jenv, JPy_Float_JClass, type->classRef)))) {
        return JType_CreateJavaFloatObject(jenv, type, pyArg, objectRef);
    } else if (type == JPy_JObject && allowObjectWrapping) {
        return JType_CreateJavaPyObject(jenv, JPy_JPyObject, pyArg, objectRef);
    }
    return JType_PythonToJavaConversionError(type, pyArg);
}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// The following functions deal with type creation, initialisation, and resolution.


/**
 * Fill the type __dict__ with our Java class constructors and methods.
 * Constructors will be available using the key named __jinit__.
 * Methods will be available using their method name.
 */
int JType_ResolveType(JNIEnv* jenv, JPy_JType* type)
{
    PyTypeObject* typeObj;

    if (type->isResolved || type->isResolving) {
        return 0;
    }

    type->isResolving = JNI_TRUE;

    typeObj = JTYPE_AS_PYTYPE(type);
    if (typeObj->tp_base != NULL && JType_Check((PyObject*) typeObj->tp_base)) {
        JPy_JType* baseType = (JPy_JType*) typeObj->tp_base;
        if (!baseType->isResolved) {
            if (JType_ResolveType(jenv, baseType) < 0) {
                type->isResolving = JNI_FALSE;
                return -1;
            }
        }
    }

    //printf("JType_ResolveType 1\n");
    if (JType_ProcessClassConstructors(jenv, type) < 0) {
        type->isResolving = JNI_FALSE;
        return -1;
    }

    //printf("JType_ResolveType 2\n");
    if (JType_ProcessClassMethods(jenv, type) < 0) {
        type->isResolving = JNI_FALSE;
        return -1;
    }

    //printf("JType_ResolveType 3\n");
    if (JType_ProcessClassFields(jenv, type) < 0) {
        type->isResolving = JNI_FALSE;
        return -1;
    }

    //printf("JType_ResolveType 4\n");
    type->isResolving = JNI_FALSE;
    type->isResolved = JNI_TRUE;
    return 0;
}

jboolean JType_AcceptMethod(JPy_JType* declaringClass, JPy_JMethod* method)
{
    PyObject* callable;
    PyObject* callableResult;

    //printf("JType_AcceptMethod: javaName='%s'\n", overloadedMethod->declaringClass->javaName);

    callable = PyDict_GetItemString(JPy_Type_Callbacks, declaringClass->javaName);
    if (callable != NULL) {
        if (PyCallable_Check(callable)) {
            callableResult = PyObject_CallFunction(callable, "OO", declaringClass, method);
            if (callableResult == Py_None || callableResult == Py_False) {
                return JNI_FALSE;
            } else if (callableResult == NULL) {
                JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JType_AcceptMethod: warning: failed to invoke callback on method addition\n");
                // Ignore this problem and continue
            }
        }
    }

    return JNI_TRUE;
}


int JType_ProcessMethod(JNIEnv* jenv, JPy_JType* type, PyObject* methodKey, const char* methodName, jclass returnType, jarray paramTypes, jboolean isStatic, jboolean isVarArgs, jmethodID mid)
{
    JPy_ParamDescriptor* paramDescriptors = NULL;
    JPy_ReturnDescriptor* returnDescriptor = NULL;
    jint paramCount;
    JPy_JMethod* method;

    paramCount = (*jenv)->GetArrayLength(jenv, paramTypes);
    JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JType_ProcessMethod: methodName=\"%s\", paramCount=%d, isStatic=%d, isVarArgs=%d, mid=%p\n", methodName, paramCount, isStatic, isVarArgs, mid);

    if (paramCount > 0) {
        paramDescriptors = JType_CreateParamDescriptors(jenv, paramCount, paramTypes);
        if (paramDescriptors == NULL) {
            JPy_DIAG_PRINT(JPy_DIAG_F_TYPE + JPy_DIAG_F_ERR, "JType_ProcessMethod: WARNING: Java method '%s' rejected because an error occurred during parameter type processing\n", methodName);
            return -1;
        }
    } else {
        paramDescriptors = NULL;
    }

    if (returnType != NULL) {
        returnDescriptor = JType_CreateReturnDescriptor(jenv, returnType);
        if (returnDescriptor == NULL) {
            PyMem_Del(paramDescriptors);
            JPy_DIAG_PRINT(JPy_DIAG_F_TYPE + JPy_DIAG_F_ERR, "JType_ProcessMethod: WARNING: Java method '%s' rejected because an error occurred during return type processing\n", methodName);
            return -1;
        }
    } else {
        returnDescriptor = NULL;
    }

    method = JMethod_New(type, methodKey, paramCount, paramDescriptors, returnDescriptor, isStatic, isVarArgs, mid);
    if (method == NULL) {
        PyMem_Del(paramDescriptors);
        PyMem_Del(returnDescriptor);
        JPy_DIAG_PRINT(JPy_DIAG_F_TYPE + JPy_DIAG_F_ERR, "JType_ProcessMethod: WARNING: Java method '%s' rejected because an error occurred during method instantiation\n", methodName);
        return -1;
    }

    if (JType_AcceptMethod(type, method)) {
        JType_InitMethodParamDescriptorFunctions(type, method);
        JType_AddMethod(type, method);
    } else {
        JMethod_Del(method);
    }

    return 0;
}

int JType_InitComponentType(JNIEnv* jenv, JPy_JType* type, jboolean resolve)
{
    jclass componentTypeRef;

    componentTypeRef = (jclass) (*jenv)->CallObjectMethod(jenv, type->classRef, JPy_Class_GetComponentType_MID);
    JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    if (componentTypeRef != NULL) {
        type->componentType = JType_GetType(jenv, componentTypeRef, resolve);
        JPy_DELETE_LOCAL_REF(componentTypeRef);
        if (type->componentType == NULL) {
            return -1;
        }
        JPy_INCREF(type->componentType);
    } else {
        type->componentType = NULL;
    }

    return 0;
}

int JType_InitSuperType(JNIEnv* jenv, JPy_JType* type, jboolean resolve)
{
    jclass superClassRef;

    superClassRef = (*jenv)->GetSuperclass(jenv, type->classRef);
    if (superClassRef != NULL) {
        type->superType = JType_GetType(jenv, superClassRef, resolve);
        if (type->superType == NULL) {
            return -1;
        }
        JPy_INCREF(type->superType);
        JPy_DELETE_LOCAL_REF(superClassRef);
    } else if (type->isInterface && JPy_JObject != NULL) {
        // This solves the problems that java.lang.Object methods can not be called on interfaces (https://github.com/bcdev/jpy/issues/57)
        type->superType = JPy_JObject;
        JPy_INCREF(type->superType);
    } else {
        type->superType = NULL;
    }

    return 0;
}


int JType_ProcessClassConstructors(JNIEnv* jenv, JPy_JType* type)
{
    jclass classRef;
    jobject constructors;
    jobject constructor;
    jobject parameterTypes;
    jint modifiers;
    jint constrCount;
    jint i;
    jboolean isPublic;
    jboolean isVarArg;
    jmethodID mid;
    PyObject* methodKey;

    classRef = type->classRef;
    methodKey = Py_BuildValue("s", JPy_JTYPE_ATTR_NAME_JINIT);
    constructors = (*jenv)->CallObjectMethod(jenv, classRef, JPy_Class_GetDeclaredConstructors_MID);
    JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    constrCount = (*jenv)->GetArrayLength(jenv, constructors);

    JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JType_ProcessClassConstructors: constrCount=%d\n", constrCount);

    for (i = 0; i < constrCount; i++) {
        constructor = (*jenv)->GetObjectArrayElement(jenv, constructors, i);
        modifiers = (*jenv)->CallIntMethod(jenv, constructor, JPy_Constructor_GetModifiers_MID);
        JPy_ON_JAVA_EXCEPTION_RETURN(-1);
        isPublic = (modifiers & 0x0001) != 0;
        isVarArg = (modifiers & 0x0080) != 0;
        if (isPublic) {
            parameterTypes = (*jenv)->CallObjectMethod(jenv, constructor, JPy_Constructor_GetParameterTypes_MID);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
            mid = (*jenv)->FromReflectedMethod(jenv, constructor);
            JType_ProcessMethod(jenv, type, methodKey, JPy_JTYPE_ATTR_NAME_JINIT, NULL, parameterTypes, 1, isVarArg, mid);
            JPy_DELETE_LOCAL_REF(parameterTypes);
        }
        JPy_DELETE_LOCAL_REF(constructor);
    }

    JPy_DELETE_LOCAL_REF(constructors);

    return 0;
}


int JType_ProcessClassFields(JNIEnv* jenv, JPy_JType* type)
{
    jclass classRef;
    jobject fields;
    jobject field;
    jobject fieldNameStr;
    jobject fieldTypeObj;
    jint modifiers;
    jint fieldCount;
    jint i;
    jboolean isStatic;
    jboolean isPublic;
    jboolean isFinal;
    const char * fieldName;
    jfieldID fid;
    PyObject* fieldKey;

    classRef = type->classRef;
    if (type->isInterface) {
        fields = (*jenv)->CallObjectMethod(jenv, classRef, JPy_Class_GetFields_MID);
    } else {
        fields = (*jenv)->CallObjectMethod(jenv, classRef, JPy_Class_GetDeclaredFields_MID);
    }
    JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    fieldCount = (*jenv)->GetArrayLength(jenv, fields);

    JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JType_ProcessClassFields: fieldCount=%d\n", fieldCount);

    for (i = 0; i < fieldCount; i++) {
        field = (*jenv)->GetObjectArrayElement(jenv, fields, i);
        modifiers = (*jenv)->CallIntMethod(jenv, field, JPy_Field_GetModifiers_MID);
        JPy_ON_JAVA_EXCEPTION_RETURN(-1);
        // see http://docs.oracle.com/javase/6/docs/api/constant-values.html#java.lang.reflect.Modifier.PUBLIC
        isPublic = (modifiers & 0x0001) != 0;
        isStatic = (modifiers & 0x0008) != 0;
        isFinal  = (modifiers & 0x0010) != 0;
        if (isPublic) {
            fieldNameStr = (*jenv)->CallObjectMethod(jenv, field, JPy_Field_GetName_MID);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
            fieldTypeObj = (*jenv)->CallObjectMethod(jenv, field, JPy_Field_GetType_MID);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
            fid = (*jenv)->FromReflectedField(jenv, field);

            fieldName = (*jenv)->GetStringUTFChars(jenv, fieldNameStr, NULL);
            fieldKey = Py_BuildValue("s", fieldName);
            JType_ProcessField(jenv, type, fieldKey, fieldName, fieldTypeObj, isStatic, isFinal, fid);
            (*jenv)->ReleaseStringUTFChars(jenv, fieldNameStr, fieldName);

            JPy_DELETE_LOCAL_REF(fieldTypeObj);
            JPy_DELETE_LOCAL_REF(fieldNameStr);
        }
        JPy_DELETE_LOCAL_REF(field);
    }
    JPy_DELETE_LOCAL_REF(fields);
    return 0;
}

int JType_ProcessClassMethods(JNIEnv* jenv, JPy_JType* type) {
    jclass classRef;
    jobject methods;
    jobject method;
    jobject methodNameStr;
    jobject returnType;
    jobject parameterTypes;
    jint modifiers;
    jint methodCount;
    jint i;
    jboolean isStatic;
    jboolean isVarArg;
    jboolean isPublic;
    jboolean isBridge;
    const char *methodName;
    jmethodID mid;
    PyObject *methodKey;

    classRef = type->classRef;

    methods = (*jenv)->CallObjectMethod(jenv, classRef, JPy_Class_GetMethods_MID);
    JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    methodCount = (*jenv)->GetArrayLength(jenv, methods);
    JPy_ON_JAVA_EXCEPTION_RETURN(-1);

    JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JType_ProcessClassMethods: methodCount=%d\n", methodCount);

    for (i = 0; i < methodCount; i++) {
        method = (*jenv)->GetObjectArrayElement(jenv, methods, i);
        modifiers = (*jenv)->CallIntMethod(jenv, method, JPy_Method_GetModifiers_MID);
        JPy_ON_JAVA_EXCEPTION_RETURN(-1);

        // see http://docs.oracle.com/javase/6/docs/api/constant-values.html#java.lang.reflect.Modifier.PUBLIC
        isPublic   = (modifiers & 0x0001) != 0;
        isStatic   = (modifiers & 0x0008) != 0;
        isVarArg   = (modifiers & 0x0080) != 0;
        isBridge   = (modifiers & 0x0040) != 0;
        // we exclude bridge methods; as covariant return types will result in bridge methods that cause ambiguity
        if (isPublic && !isBridge) {
            methodNameStr = (*jenv)->CallObjectMethod(jenv, method, JPy_Method_GetName_MID);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
            returnType = (*jenv)->CallObjectMethod(jenv, method, JPy_Method_GetReturnType_MID);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
            parameterTypes = (*jenv)->CallObjectMethod(jenv, method, JPy_Method_GetParameterTypes_MID);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
            mid = (*jenv)->FromReflectedMethod(jenv, method);

            methodName = (*jenv)->GetStringUTFChars(jenv, methodNameStr, NULL);
            methodKey = Py_BuildValue("s", methodName);
            JType_ProcessMethod(jenv, type, methodKey, methodName, returnType, parameterTypes, isStatic, isVarArg, mid);
            (*jenv)->ReleaseStringUTFChars(jenv, methodNameStr, methodName);

            JPy_DELETE_LOCAL_REF(parameterTypes);
            JPy_DELETE_LOCAL_REF(returnType);
            JPy_DELETE_LOCAL_REF(methodNameStr);
        }
        JPy_DELETE_LOCAL_REF(method);
    }
    JPy_DELETE_LOCAL_REF(methods);
    return 0;
}

jboolean JType_AcceptField(JPy_JType* declaringClass, JPy_JField* field)
{
    return JNI_TRUE;
}

int JType_AddField(JPy_JType* declaringClass, JPy_JField* field)
{
    PyObject* typeDict;

    typeDict = declaringClass->typeObj.tp_dict;
    if (typeDict == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "jpy internal error: missing attribute '__dict__' in JType");
        return -1;
    }

    PyDict_SetItem(typeDict, field->name, (PyObject*) field);
    return 0;
}

int JType_AddClassAttribute(JNIEnv* jenv, JPy_JType* declaringClass)
{
    PyObject* typeDict;
    if (JPy_JClass != NULL) {
        typeDict = declaringClass->typeObj.tp_dict;
        if (typeDict == NULL) {
            PyErr_SetString(PyExc_RuntimeError, "jpy internal error: missing attribute '__dict__' in JType");
            return -1;
        }
        PyDict_SetItem(typeDict, Py_BuildValue("s", "jclass"), (PyObject*) JObj_FromType(jenv, JPy_JClass, declaringClass->classRef));
    }
    return 0;
}

int JType_AddFieldAttribute(JNIEnv* jenv, JPy_JType* declaringClass, PyObject* fieldName, JPy_JType* fieldType, jfieldID fid)
{
    PyObject* typeDict;
    PyObject* fieldValue;

    typeDict = declaringClass->typeObj.tp_dict;
    if (typeDict == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "jpy internal error: missing attribute '__dict__' in JType");
        return -1;
    }

    if (fieldType == JPy_JBoolean) {
        jboolean item = (*jenv)->GetStaticBooleanField(jenv, declaringClass->classRef, fid);
        fieldValue = JPy_FROM_JBOOLEAN(item);
    } else if (fieldType == JPy_JChar) {
        jchar item = (*jenv)->GetStaticCharField(jenv, declaringClass->classRef, fid);
        fieldValue = JPy_FROM_JCHAR(item);
    } else if (fieldType == JPy_JByte) {
        jbyte item = (*jenv)->GetStaticByteField(jenv, declaringClass->classRef, fid);
        fieldValue = JPy_FROM_JBYTE(item);
    } else if (fieldType == JPy_JShort) {
        jshort item = (*jenv)->GetStaticShortField(jenv, declaringClass->classRef, fid);
        fieldValue = JPy_FROM_JSHORT(item);
    } else if (fieldType == JPy_JInt) {
        jint item = (*jenv)->GetStaticIntField(jenv, declaringClass->classRef, fid);
        fieldValue = JPy_FROM_JINT(item);
    } else if (fieldType == JPy_JLong) {
        jlong item = (*jenv)->GetStaticLongField(jenv, declaringClass->classRef, fid);
        fieldValue = JPy_FROM_JLONG(item);
    } else if (fieldType == JPy_JFloat) {
        jfloat item = (*jenv)->GetStaticFloatField(jenv, declaringClass->classRef, fid);
        fieldValue = JPy_FROM_JFLOAT(item);
    } else if (fieldType == JPy_JDouble) {
        jdouble item = (*jenv)->GetStaticDoubleField(jenv, declaringClass->classRef, fid);
        fieldValue = JPy_FROM_JDOUBLE(item);
    } else if (fieldType == JPy_JString) {
        jstring objectRef = (*jenv)->GetStaticObjectField(jenv, declaringClass->classRef, fid);
        fieldValue = JPy_FromJString(jenv, objectRef);
        JPy_DELETE_LOCAL_REF(objectRef);
    } else {
        jobject objectRef = (*jenv)->GetStaticObjectField(jenv, declaringClass->classRef, fid);
        fieldValue = JPy_FromJObjectWithType(jenv, objectRef, (JPy_JType*) fieldType);
        JPy_DELETE_LOCAL_REF(objectRef);
    }
    PyDict_SetItem(typeDict, fieldName, fieldValue);
    return 0;
}

int JType_ProcessField(JNIEnv* jenv, JPy_JType* declaringClass, PyObject* fieldKey, const char* fieldName, jclass fieldClassRef, jboolean isStatic, jboolean isFinal, jfieldID fid)
{
    JPy_JField* field;
    JPy_JType* fieldType;

    fieldType = JType_GetType(jenv, fieldClassRef, JNI_FALSE);
    if (fieldType == NULL) {
        JPy_DIAG_PRINT(JPy_DIAG_F_TYPE + JPy_DIAG_F_ERR, "JType_ProcessField: WARNING: Java field '%s' rejected because an error occurred during type processing\n", fieldName);
        return -1;
    }

    if (isStatic && isFinal) {
        // Add static final values to the JPy_JType's tp_dict.
        // todo: Note that this is a workaround only, because the JPy_JType's tp_getattro slot is not called.
        if (JType_AddFieldAttribute(jenv, declaringClass, fieldKey, fieldType, fid) < 0) {
            return -1;
        }
    } else if (!isStatic) {
        // Add instance field accessor to the JPy_JType's tp_dict, this will be evaluated in the JPy_JType's tp_setattro and tp_getattro slots.
        field = JField_New(declaringClass, fieldKey, fieldType, isStatic, isFinal, fid);
        if (field == NULL) {
            JPy_DIAG_PRINT(JPy_DIAG_F_TYPE + JPy_DIAG_F_ERR, "JType_ProcessField: WARNING: Java field '%s' rejected because an error occurred during field instantiation\n", fieldName);
            return -1;
        }

        if (JType_AcceptField(declaringClass, field)) {
            JType_AddField(declaringClass, field);
        } else {
            JField_Del(field);
        }
    } else {
        JPy_DIAG_PRINT(JPy_DIAG_F_TYPE + JPy_DIAG_F_ERR, "JType_ProcessField: WARNING: Java field '%s' rejected because is is static, but not final\n", fieldName);
    }

    return 0;
}


void JType_InitMethodParamDescriptorFunctions(JPy_JType* type, JPy_JMethod* method)
{
    int index;
    for (index = 0; index < method->paramCount; index++) {
        JType_InitParamDescriptorFunctions(method->paramDescriptors + index, index == method->paramCount - 1 && method->isVarArgs);
    }
}

int JType_AddMethod(JPy_JType* type, JPy_JMethod* method)
{
    PyObject* typeDict;
    PyObject* methodValue;
    JPy_JOverloadedMethod* overloadedMethod;

    typeDict = type->typeObj.tp_dict;
    if (typeDict == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "jpy internal error: missing attribute '__dict__' in JType");
        return -1;
    }

    methodValue = PyDict_GetItem(typeDict, method->name);
    if (methodValue == NULL) {
        overloadedMethod = JOverloadedMethod_New(type, method->name, method);
        return PyDict_SetItem(typeDict, method->name, (PyObject*) overloadedMethod);
    } else if (PyObject_TypeCheck(methodValue, &JOverloadedMethod_Type)) {
        overloadedMethod = (JPy_JOverloadedMethod*) methodValue;
        return JOverloadedMethod_AddMethod(overloadedMethod, method);
    } else {
        PyErr_SetString(PyExc_RuntimeError, "jpy internal error: expected type 'JOverloadedMethod' in '__dict__' of a JType");
        return -1;
    }
}

/**
 * Returns NULL (error), Py_None (borrowed ref), or a JPy_JOverloadedMethod* (borrowed ref)
 */
PyObject* JType_GetOverloadedMethod(JNIEnv* jenv, JPy_JType* type, PyObject* methodName, jboolean useSuperClass)
{
    PyObject* typeDict;
    PyObject* methodValue;

    typeDict = type->typeObj.tp_dict;
    if (typeDict == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "internal error: missing attribute '__dict__' in JType");
        return NULL;
    }

    methodValue = PyDict_GetItem(typeDict, methodName);
    if (methodValue == NULL) {
        if (useSuperClass) {
            // type->superType == NULL means that type->superType is either an interface or a java.lang.Object
            // If it is an interface (and not type java.lang.Object) also look for java.lang.Object methods
            // See https://github.com/bcdev/jpy/issues/57.
            if (type->superType != NULL) {
                return JType_GetOverloadedMethod(jenv, type->superType, methodName, JNI_TRUE);
            } else if (type != JPy_JObject && JPy_JObject != NULL) {
                return JType_GetOverloadedMethod(jenv, JPy_JObject, methodName, JNI_FALSE);
            } else {
                return Py_None;
            }
        } else {
            return Py_None;
        }
    }

    if (PyObject_TypeCheck(methodValue, &JOverloadedMethod_Type)) {
        return methodValue;
    } else {
        PyErr_SetString(PyExc_RuntimeError, "internal error: expected type 'JOverloadedMethod' in '__dict__' of a JType");
        return NULL;
    }
}

JPy_ReturnDescriptor* JType_CreateReturnDescriptor(JNIEnv* jenv, jclass returnClass)
{
    JPy_ReturnDescriptor* returnDescriptor;
    JPy_JType* type;

    returnDescriptor = PyMem_New(JPy_ReturnDescriptor, 1);
    if (returnDescriptor == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    type = JType_GetType(jenv, returnClass, JNI_FALSE);
    if (type == NULL) {
        return NULL;
    }

    returnDescriptor->type = type;
    returnDescriptor->paramIndex = -1;
    JPy_INCREF((PyObject*) type);

    JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JType_ProcessReturnType: type->javaName=\"%s\", type=%p\n", type->javaName, type);

    return returnDescriptor;
}


JPy_ParamDescriptor* JType_CreateParamDescriptors(JNIEnv* jenv, int paramCount, jarray paramClasses)
{
    JPy_ParamDescriptor* paramDescriptors;
    JPy_ParamDescriptor* paramDescriptor;
    JPy_JType* type;
    jclass paramClass;
    int i;

    paramDescriptors = PyMem_New(JPy_ParamDescriptor, paramCount);
    if (paramDescriptors == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    for (i = 0; i < paramCount; i++) {
        paramClass = (*jenv)->GetObjectArrayElement(jenv, paramClasses, i);
        paramDescriptor = paramDescriptors + i;

        type = JType_GetType(jenv, paramClass, JNI_FALSE);
        JPy_DELETE_LOCAL_REF(paramClass);
        if (type == NULL) {
            return NULL;
        }

        paramDescriptor->type = type;
        JPy_INCREF((PyObject*) paramDescriptor->type);

        paramDescriptor->isMutable = 0;
        paramDescriptor->isOutput = 0;
        paramDescriptor->isReturn = 0;
        paramDescriptor->MatchPyArg = NULL;
        paramDescriptor->MatchVarArgPyArg = NULL;
        paramDescriptor->ConvertPyArg = NULL;
        paramDescriptor->ConvertVarArgPyArg = NULL;
    }

    return paramDescriptors;
}

int JType_MatchPyArgAsJBooleanParam(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg)
{
    if (PyBool_Check(pyArg)) return 100;
    else if (JPy_IS_CLONG(pyArg)) return 10;
    else return 0;
}

int JType_ConvertPyArgToJBooleanArg(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg, jvalue* value, JPy_ArgDisposer* disposer)
{
    value->z = JPy_AS_JBOOLEAN(pyArg);
    return 0;
}

int JType_MatchPyArgAsJByteParam(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg)
{
    if (JPy_IS_CLONG(pyArg)) return 100;
    else if (PyBool_Check(pyArg)) return 10;
    else return 0;
}

int JType_ConvertPyArgToJByteArg(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg, jvalue* value, JPy_ArgDisposer* disposer)
{
    value->b = JPy_AS_JBYTE(pyArg);
    return 0;
}

int JType_MatchPyArgAsJCharParam(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg)
{
    if (JPy_IS_CLONG(pyArg)) return 100;
    else if (PyBool_Check(pyArg)) return 10;
    else return 0;
}

int JType_ConvertPyArgToJCharArg(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg, jvalue* value, JPy_ArgDisposer* disposer)
{
    value->c = JPy_AS_JCHAR(pyArg);
    return 0;
}

int JType_MatchPyArgAsJShortParam(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg)
{
    if (JPy_IS_CLONG(pyArg)) return 100;
    else if (PyBool_Check(pyArg)) return 10;
    else return 0;
}

int JType_ConvertPyArgToJShortArg(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg, jvalue* value, JPy_ArgDisposer* disposer)
{
    value->s = JPy_AS_JSHORT(pyArg);
    return 0;
}

int JType_MatchPyArgAsJIntParam(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg)
{
    if (JPy_IS_CLONG(pyArg)) return 100;
    else if (PyBool_Check(pyArg)) return 10;
    else return 0;
}

int JType_ConvertPyArgToJIntArg(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg, jvalue* value, JPy_ArgDisposer* disposer)
{
    value->i = JPy_AS_JINT(pyArg);
    return 0;
}

int JType_MatchPyArgAsJLongParam(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg)
{
    if (JPy_IS_CLONG(pyArg)) return 100;
    else if (PyBool_Check(pyArg)) return 10;
    else return 0;
}

int JType_ConvertPyArgToJLongArg(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg, jvalue* value, JPy_ArgDisposer* disposer)
{
    value->j = JPy_AS_JLONG(pyArg);
    return 0;
}

int JType_MatchPyArgAsJFloatParam(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg)
{
    if (PyFloat_Check(pyArg)) return 90; // not 100, in order to give 'double' a chance
    else if (PyNumber_Check(pyArg)) return 50;
    else if (JPy_IS_CLONG(pyArg)) return 10;
    else if (PyBool_Check(pyArg)) return 1;
    else return 0;
}

int JType_ConvertPyArgToJFloatArg(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg, jvalue* value, JPy_ArgDisposer* disposer)
{
    value->f = JPy_AS_JFLOAT(pyArg);
    return 0;
}

int JType_MatchPyArgAsJDoubleParam(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg)
{
    if (PyFloat_Check(pyArg)) return 100;
    else if (PyNumber_Check(pyArg)) return 50;
    else if (JPy_IS_CLONG(pyArg)) return 10;
    else if (PyBool_Check(pyArg)) return 1;
    else return 0;
}

int JType_ConvertPyArgToJDoubleArg(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg, jvalue* value, JPy_ArgDisposer* disposer)
{
    value->d = JPy_AS_JDOUBLE(pyArg);
    return 0;
}

int JType_MatchPyArgAsJStringParam(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg)
{
    if (pyArg == Py_None) {
        // Signal it is possible, but give low priority since we cannot perform any type checks on 'None'
        return 1;
    }
    if (JPy_IS_STR(pyArg)) {
        return 100;
    }
    return 0;
}

int JType_ConvertPyArgToJStringArg(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg, jvalue* value, JPy_ArgDisposer* disposer)
{
    disposer->data = NULL;
    disposer->DisposeArg = JType_DisposeLocalObjectRefArg;
    return JPy_AsJString(jenv, pyArg, &value->l);
}

int JType_ConvertPyArgToJPyObjectArg(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg, jvalue* value, JPy_ArgDisposer* disposer)
{
    disposer->data = NULL;
    disposer->DisposeArg = JType_DisposeLocalObjectRefArg;
    return JType_CreateJavaPyObject(jenv, JPy_JPyObject, pyArg, &value->l);
}


int JType_MatchPyArgAsJPyObjectParam(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg)
{
    // We can always turn a python object into a PyObject
    // But it has a lower matching value to give a more specific type a better chance
    // ie, against:
    // void method1(String s) { ... }
    // void method1(PyObject o) { ... }
    //
    // the Python code: obj.method1('a string') should prefer the String specific version
    return 10; // yeah - EZ PZ
}

int JType_MatchPyArgAsJObjectParam(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg)
{
    return JType_MatchPyArgAsJObject(jenv, paramDescriptor->type, pyArg);
}

int JType_MatchVarArgPyArgAsJObjectParam(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg, int idx)
{
    Py_ssize_t argCount = PyTuple_Size(pyArg);
    Py_ssize_t remaining = (argCount - idx);

    JPy_JType *componentType = paramDescriptor->type->componentType;
    int minMatch = 100;
    int ii;

    if (componentType == NULL) {
        return 0;
    }

    if (remaining == 0) {
        return 10;
    }

    for (ii = 0; ii < remaining; ii++) {
        PyObject *unpack = PyTuple_GetItem(pyArg, idx + ii);
        int matchValue = JType_MatchPyArgAsJObject(jenv, componentType, unpack);
        if (matchValue == 0) {
            return 0;
        }
        minMatch = matchValue < minMatch ? matchValue : minMatch;
    }

    return minMatch;
}

int JType_MatchVarArgPyArgAsJStringParam(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg, int idx)
{
    Py_ssize_t argCount = PyTuple_Size(pyArg);
    Py_ssize_t remaining = (argCount - idx);

    JPy_JType *componentType = paramDescriptor->type->componentType;
    int minMatch = 100;
    int ii;

    if (componentType != JPy_JString) {
        return 0;
    }

    if (remaining == 0) {
        return 10;
    }

    for (ii = 0; ii < remaining; ii++) {
        PyObject *unpack = PyTuple_GetItem(pyArg, idx + ii);
        int matchValue = JType_MatchPyArgAsJStringParam(jenv, paramDescriptor, unpack);
        if (matchValue == 0) {
            return 0;
        }
        minMatch = matchValue < minMatch ? matchValue : minMatch;
    }

    return minMatch;
}

int JType_MatchVarArgPyArgAsJPyObjectParam(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg, int idx)
{
    Py_ssize_t argCount = PyTuple_Size(pyArg);
    Py_ssize_t remaining = (argCount - idx);

    JPy_JType *componentType = paramDescriptor->type->componentType;
    int minMatch = 100;
    int ii;

    if (componentType != JPy_JPyObject) {
        return 0;
    }

    if (remaining == 0) {
        return 10;
    }

    for (ii = 0; ii < remaining; ii++) {
        PyObject *unpack = PyTuple_GetItem(pyArg, idx + ii);
        int matchValue = JType_MatchPyArgAsJPyObjectParam(jenv, paramDescriptor, unpack);
        if (matchValue == 0) {
            return 0;
        }
        minMatch = matchValue < minMatch ? matchValue : minMatch;
    }
    return minMatch;
}

int JType_MatchVarArgPyArgAsJBooleanParam(JNIEnv *jenv, JPy_ParamDescriptor *paramDescriptor, PyObject *pyArg, int idx)
{
    Py_ssize_t argCount = PyTuple_Size(pyArg);
    Py_ssize_t remaining = (argCount - idx);

    JPy_JType *componentType = paramDescriptor->type->componentType;
    int minMatch = 100;
    int ii;

    if (componentType != JPy_JBoolean) {
        // something is horribly wrong here!
        return 0;
    }

    if (remaining == 0) {
        return 10;
    }

    for (ii = 0; ii < remaining; ii++) {
        PyObject *unpack = PyTuple_GetItem(pyArg, idx + ii);

        int matchValue;
        if (PyBool_Check(unpack)) matchValue = 100;
        else if (JPy_IS_CLONG(unpack)) matchValue = 10;
        else return 0;
        minMatch = matchValue < minMatch ? matchValue : minMatch;
    }

    return minMatch;
}

int JType_MatchVarArgPyArgAsJIntParam(JNIEnv *jenv, JPy_ParamDescriptor *paramDescriptor, PyObject *pyArg, int idx)
{
    return JType_MatchVarArgPyArgIntType(paramDescriptor, pyArg, idx, JPy_JInt);
}

int JType_MatchVarArgPyArgAsJLongParam(JNIEnv *jenv, JPy_ParamDescriptor *paramDescriptor, PyObject *pyArg, int idx)
{
    return JType_MatchVarArgPyArgIntType(paramDescriptor, pyArg, idx, JPy_JLong);
}

int JType_MatchVarArgPyArgAsJShortParam(JNIEnv *jenv, JPy_ParamDescriptor *paramDescriptor, PyObject *pyArg, int idx)
{
    return JType_MatchVarArgPyArgIntType(paramDescriptor, pyArg, idx, JPy_JShort);
}

int JType_MatchVarArgPyArgAsJByteParam(JNIEnv *jenv, JPy_ParamDescriptor *paramDescriptor, PyObject *pyArg, int idx)
{
    return JType_MatchVarArgPyArgIntType(paramDescriptor, pyArg, idx, JPy_JByte);
}

int JType_MatchVarArgPyArgAsJCharParam(JNIEnv *jenv, JPy_ParamDescriptor *paramDescriptor, PyObject *pyArg, int idx)
{
    return JType_MatchVarArgPyArgIntType(paramDescriptor, pyArg, idx, JPy_JChar);
}

int JType_MatchVarArgPyArgIntType(const JPy_ParamDescriptor *paramDescriptor, PyObject *pyArg, int idx,
                                  struct JPy_JType *expectedComponentType) {
    Py_ssize_t argCount = PyTuple_Size(pyArg);
    Py_ssize_t remaining = (argCount - idx);

    JPy_JType *componentType = paramDescriptor->type->componentType;
    int minMatch = 100;
    int ii;

    if (componentType != expectedComponentType) {
        // something is horribly wrong here!
        return 0;
    }

    if (remaining == 0) {
        return 10;
    }

    for (ii = 0; ii < remaining; ii++) {
        PyObject *unpack = PyTuple_GetItem(pyArg, idx + ii);

        int matchValue;
        if (JPy_IS_CLONG(unpack)) matchValue = 100;
        else if (PyBool_Check(unpack)) matchValue = 10;
        else return 0;
        minMatch = matchValue < minMatch ? matchValue : minMatch;
    }

    return minMatch;
}

int JType_MatchVarArgPyArgAsJDoubleParam(JNIEnv *jenv, JPy_ParamDescriptor *paramDescriptor, PyObject *pyArg, int idx)
{
    return JType_MatchVarArgPyArgAsFPType(paramDescriptor, pyArg, idx, JPy_JDouble, 100);
}

int JType_MatchVarArgPyArgAsJFloatParam(JNIEnv *jenv, JPy_ParamDescriptor *paramDescriptor, PyObject *pyArg, int idx)
{
    // float gets a match of 90, so that double has a better chance
    return JType_MatchVarArgPyArgAsFPType(paramDescriptor, pyArg, idx, JPy_JFloat, 90);
}

/* The float and double match functions are almost identical, but for the expected componentType and the match value
 * for floating point numbers should give a preference to double over float. */
int JType_MatchVarArgPyArgAsFPType(const JPy_ParamDescriptor *paramDescriptor, PyObject *pyArg, int idx,
                                   struct JPy_JType *expectedType, int floatMatch) {
    Py_ssize_t argCount = PyTuple_Size(pyArg);
    Py_ssize_t remaining = (argCount - idx);

    JPy_JType *componentType = paramDescriptor->type->componentType;
    int minMatch = 100;
    int ii;

    if (componentType != expectedType) {
        // something is horribly wrong here!
        return 0;
    }

    if (remaining == 0) {
        return 10;
    }

    for (ii = 0; ii < remaining; ii++) {
        PyObject *unpack = PyTuple_GetItem(pyArg, idx + ii);

        int matchValue;
        if (PyFloat_Check(unpack)) matchValue = floatMatch;
        else if (PyNumber_Check(unpack)) matchValue = 50;
        else if (JPy_IS_CLONG(unpack)) matchValue = 10;
        else if (PyBool_Check(unpack)) matchValue = 1;
        else return 0;
        minMatch = matchValue < minMatch ? matchValue : minMatch;
    }

    return minMatch;
}

int JType_ConvertVarArgPyArgToJObjectArg(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArgOrig, int offset, jvalue* value, JPy_ArgDisposer* disposer)
{
    Py_ssize_t size = PyTuple_Size(pyArgOrig);
    PyObject *pyArg = PyTuple_GetSlice(pyArgOrig, offset, size);

    if (pyArg == Py_None) {
        // Py_None maps to (Java) NULL
        value->l = NULL;
        disposer->data = NULL;
        disposer->DisposeArg = NULL;
    } else if (JObj_Check(pyArg)) {
        // If it is a wrapped Java object, it is always a global reference, so don't dispose it
        JPy_JObj* obj = (JPy_JObj*) pyArg;
        value->l = obj->objectRef;
        disposer->data = NULL;
        disposer->DisposeArg = NULL;
    } else {
        // For any other Python argument, we first check if the formal parameter is a primitive array
        // and the Python argument is a buffer object

        JPy_JType* paramType = paramDescriptor->type;
        JPy_JType* paramComponentType = paramType->componentType;

        if (paramComponentType != NULL && paramComponentType->isPrimitive && PyObject_CheckBuffer(pyArg)) {
            Py_buffer* pyBuffer;
            int flags;
            Py_ssize_t itemCount;
            jarray jArray;
            void* arrayItems;
            jint itemSize;

            pyBuffer = PyMem_New(Py_buffer, 1);
            if (pyBuffer == NULL) {
                PyErr_NoMemory();
                JPy_DECREF(pyArg);
                return -1;
            }

            flags = paramDescriptor->isMutable ? PyBUF_WRITABLE : PyBUF_SIMPLE;
            if (PyObject_GetBuffer(pyArg, pyBuffer, flags) < 0) {
                PyMem_Del(pyBuffer);
                JPy_DECREF(pyArg);
                return -1;
            }

            itemCount = pyBuffer->len / pyBuffer->itemsize;
            if (itemCount <= 0) {
                PyBuffer_Release(pyBuffer);
                PyMem_Del(pyBuffer);
                JPy_DECREF(pyArg);
                PyErr_Format(PyExc_ValueError, "illegal buffer argument: not a positive item count: %ld", itemCount);
                return -1;
            }

            if (paramComponentType == JPy_JBoolean) {
                jArray = (*jenv)->NewBooleanArray(jenv, itemCount);
                itemSize = sizeof(jboolean);
            } else if (paramComponentType == JPy_JByte) {
                jArray = (*jenv)->NewByteArray(jenv, itemCount);
                itemSize = sizeof(jbyte);
            } else if (paramComponentType == JPy_JChar) {
                jArray = (*jenv)->NewCharArray(jenv, itemCount);
                itemSize = sizeof(jchar);
            } else if (paramComponentType == JPy_JShort) {
                jArray = (*jenv)->NewShortArray(jenv, itemCount);
                itemSize = sizeof(jshort);
            } else if (paramComponentType == JPy_JInt) {
                jArray = (*jenv)->NewIntArray(jenv, itemCount);
                itemSize = sizeof(jint);
            } else if (paramComponentType == JPy_JLong) {
                jArray = (*jenv)->NewLongArray(jenv, itemCount);
                itemSize = sizeof(jlong);
            } else if (paramComponentType == JPy_JFloat) {
                jArray = (*jenv)->NewFloatArray(jenv, itemCount);
                itemSize = sizeof(jfloat);
            } else if (paramComponentType == JPy_JDouble) {
                jArray = (*jenv)->NewDoubleArray(jenv, itemCount);
                itemSize = sizeof(jdouble);
            } else {
                JPy_DECREF(pyArg);
                PyBuffer_Release(pyBuffer);
                PyMem_Del(pyBuffer);
                PyErr_SetString(PyExc_RuntimeError, "internal error: illegal primitive Java type");
                return -1;
            }

            if (pyBuffer->len != itemCount * itemSize) {
                Py_ssize_t bufferLen = pyBuffer->len;
                Py_ssize_t bufferItemSize = pyBuffer->itemsize;
                //printf("%ld, %ld, %ld, %ld\n", pyBuffer->len , pyBuffer->itemsize, itemCount, itemSize);
                JPy_DECREF(pyArg);
                PyBuffer_Release(pyBuffer);
                PyMem_Del(pyBuffer);
                PyErr_Format(PyExc_ValueError,
                             "illegal buffer argument: expected size was %ld bytes, but got %ld (expected item size was %d bytes, got %ld)",
                             itemCount * itemSize, bufferLen, itemSize, bufferItemSize);
                return -1;
            }

            if (jArray == NULL) {
                JPy_DECREF(pyArg);
                PyBuffer_Release(pyBuffer);
                PyMem_Del(pyBuffer);
                PyErr_NoMemory();
                return -1;
            }

            if (!paramDescriptor->isOutput) {
                arrayItems = (*jenv)->GetPrimitiveArrayCritical(jenv, jArray, NULL);
                if (arrayItems == NULL) {
                    JPy_DECREF(pyArg);
                    PyBuffer_Release(pyBuffer);
                    PyMem_Del(pyBuffer);
                    PyErr_NoMemory();
                    return -1;
                }
                JPy_DIAG_PRINT(JPy_DIAG_F_EXEC|JPy_DIAG_F_MEM, "JType_ConvertPyArgToJObjectArg: moving Python buffer into Java array: pyBuffer->buf=%p, pyBuffer->len=%d\n", pyBuffer->buf, pyBuffer->len);
                memcpy(arrayItems, pyBuffer->buf, itemCount * itemSize);
                (*jenv)->ReleasePrimitiveArrayCritical(jenv, jArray, arrayItems, 0);
            }

            value->l = jArray;
            disposer->data = pyBuffer;
            disposer->DisposeArg = paramDescriptor->isMutable ? JType_DisposeWritableBufferArg : JType_DisposeReadOnlyBufferArg;
        } else {
            jobject objectRef;
            if (JType_ConvertPythonToJavaObject(jenv, paramType, pyArg, &objectRef, JNI_FALSE) < 0) {
                JPy_DECREF(pyArg);
                return -1;
            }
            value->l = objectRef;
            disposer->data = NULL;
            disposer->DisposeArg = JType_DisposeLocalObjectRefArg;
        }
    }

    JPy_DECREF(pyArg);

    return 0;
}

int JType_MatchPyArgAsJObject(JNIEnv* jenv, JPy_JType* paramType, PyObject* pyArg)
{
    JPy_JType* argType;
    JPy_JType* paramComponentType;
    JPy_JType* argComponentType;
    JPy_JObj* argValue;

    if (pyArg == Py_None) {
        // Signal it is possible, but give low priority since we cannot perform any type checks on 'None'
        return 1;
    }

    paramComponentType = paramType->componentType;

    if (JObj_Check(pyArg)) {
        // pyArg is a Java object

        argType = (JPy_JType*) Py_TYPE(pyArg);
        if (argType == paramType) {
            // pyArg has same type as the parameter
            return 100;
        }

        argValue = (JPy_JObj*) pyArg;
        if ((*jenv)->IsInstanceOf(jenv, argValue->objectRef, paramType->classRef)) {
            argComponentType = argType->componentType;
            if (argComponentType == paramComponentType) {
                // pyArg is an instance of parameter type, and they both have the same component types (which may be null)
                return 90;
            }
            if (argComponentType != NULL && paramComponentType != NULL) {
                // Determines whether an object of clazz1 can be safely cast to clazz2.
                if ((*jenv)->IsAssignableFrom(jenv, argComponentType->classRef, paramComponentType->classRef)) {
                    // pyArg is an instance of parameter array type, and component types are compatible
                    return 80;
                }
            }
            // Honour that pyArg is compatible with paramType, but better matches may exist.
            return 10;
        }

        // pyArg type does not match parameter type
        return 0;
    }

    // pyArg is not a Java object

    if (paramComponentType != NULL) {
        // The parameter type is an array type

        if (paramComponentType->isPrimitive && PyObject_CheckBuffer(pyArg)) {
            Py_buffer view;

            // The parameter type is a primitive array type, pyArg is a Python buffer object

            if (PyObject_GetBuffer(pyArg, &view, PyBUF_FORMAT) == 0) {
                JPy_JType* type;
                int matchValue;

                //printf("JType_AssessToJObject: buffer len=%d, itemsize=%d, format=%s\n", view.len, view.itemsize, view.format);

                type = paramComponentType;
                matchValue = 0;
                if (view.format != NULL) {
                    char format = *view.format;
                    if (type == JPy_JBoolean) {
                        matchValue = format == 'b' || format == 'B' ? 100
                                   : view.itemsize == 1 ? 10
                                   : 0;
                    } else if (type == JPy_JByte) {
                        matchValue = format == 'b' ? 100
                                   : format == 'B' ? 90
                                   : view.itemsize == 1 ? 10
                                   : 0;
                    } else if (type == JPy_JChar) {
                        matchValue = format == 'u' ? 100
                                   : format == 'H' ? 90
                                   : format == 'h' ? 80
                                   : view.itemsize == 2 ? 10
                                   : 0;
                    } else if (type == JPy_JShort) {
                        matchValue = format == 'h' ? 100
                                   : format == 'H' ? 90
                                   : view.itemsize == 2 ? 10
                                   : 0;
                    } else if (type == JPy_JInt) {
                        matchValue = format == 'i' ? 100
                                   : format == 'I' ? 90
                                   : view.itemsize == 4 ? 10
                                   : 0;
                    } else if (type == JPy_JLong) {
                        matchValue = format == 'q' || format == 'l' ? 100
                                   : format == 'Q' || format == 'L' ? 90
                                   : view.itemsize == 8 ? 10
                                   : 0;
                    } else if (type == JPy_JFloat) {
                        matchValue = format == 'f' ? 100
                                   : view.itemsize == 4 ? 10
                                   : 0;
                    } else if (type == JPy_JDouble) {
                        matchValue = format == 'd' ? 100
                                   : view.itemsize == 8 ? 10
                                   : 0;
                    }
                } else {
                    if (type == JPy_JBoolean) {
                        matchValue = view.itemsize == 1 ? 10 : 0;
                    } else if (type == JPy_JByte) {
                        matchValue = view.itemsize == 1 ? 10 : 0;
                    } else if (type == JPy_JChar) {
                        matchValue = view.itemsize == 2 ? 10 : 0;
                    } else if (type == JPy_JShort) {
                        matchValue = view.itemsize == 2 ? 10 : 0;
                    } else if (type == JPy_JInt) {
                        matchValue = view.itemsize == 4 ? 10 : 0;
                    } else if (type == JPy_JLong) {
                        matchValue = view.itemsize == 8 ? 10 : 0;
                    } else if (type == JPy_JFloat) {
                        matchValue = view.itemsize == 4 ? 10 : 0;
                    } else if (type == JPy_JDouble) {
                        matchValue = view.itemsize == 8 ? 10 : 0;
                    }
                }

                PyBuffer_Release(&view);
                return matchValue;
            }
        } else if (PySequence_Check(pyArg)) {
            // if we know the type of the array is a string, we should preferentially match it
            if ((*jenv)->IsAssignableFrom(jenv, paramComponentType->classRef, JPy_String_JClass)) {
                // it's a string array
                Py_ssize_t len = PySequence_Length(pyArg);
                Py_ssize_t ii;

                for (ii = 0; ii < len; ++ii) {
                    PyObject *element = PySequence_GetItem(pyArg, ii);
                    if (!JPy_IS_STR(element)) {
                        // if the element is not a string, this is not a good match
                        return 0;
                    }
                }

                // a String sequence is a good match for a String array
                return 80;
            }
            return 10;
        }
    } else if (paramType == JPy_JObject) {
        // Parameter type is not an array type, but any other Java object type
        return 10;
    } else if (paramType == JPy_JBooleanObj) {
        if (PyBool_Check(pyArg)) {
            return 100;
        } else if (JPy_IS_CLONG(pyArg)) {
            return 10;
        }
    } else if (paramType == JPy_JCharacterObj
               || paramType == JPy_JByteObj
               || paramType == JPy_JShortObj
               || paramType == JPy_JIntegerObj
               || paramType == JPy_JLongObj) {
        if (JPy_IS_CLONG(pyArg)) {
            return 100;
        } else if (PyBool_Check(pyArg)) {
            return 10;
        }
    } else if (paramType == JPy_JFloatObj
               || paramType == JPy_JDoubleObj) {
        if (PyFloat_Check(pyArg)) {
            return 100;
        } else if (JPy_IS_CLONG(pyArg)) {
            return 90;
        } else if (PyBool_Check(pyArg)) {
            return 10;
        }
    } else {
        if (JPy_IS_STR(pyArg)) {
            if ((*jenv)->IsAssignableFrom(jenv, JPy_JString->classRef, paramType->classRef)) {
                return 80;
            }
        }
        else if (PyBool_Check(pyArg)) {
            if ((*jenv)->IsAssignableFrom(jenv, JPy_Boolean_JClass, paramType->classRef)) {
                return 80;
            }
        }
        else if (JPy_IS_CLONG(pyArg)) {
            if ((*jenv)->IsAssignableFrom(jenv, JPy_Integer_JClass, paramType->classRef)) {
                return 80;
            }
            else if ((*jenv)->IsAssignableFrom(jenv, JPy_Long_JClass, paramType->classRef)) {
                return 80;
            }
        }
        else if (PyFloat_Check(pyArg)) {
            if ((*jenv)->IsAssignableFrom(jenv, JPy_Double_JClass, paramType->classRef)) {
                return 80;
            }
            else if ((*jenv)->IsAssignableFrom(jenv, JPy_Float_JClass, paramType->classRef)) {
                return 80;
            }
        }
    }

    return 0;
}

int JType_ConvertPyArgToJObjectArg(JNIEnv* jenv, JPy_ParamDescriptor* paramDescriptor, PyObject* pyArg, jvalue* value, JPy_ArgDisposer* disposer)
{
    if (pyArg == Py_None) {
        // Py_None maps to (Java) NULL
        value->l = NULL;
        disposer->data = NULL;
        disposer->DisposeArg = NULL;
    } else if (JObj_Check(pyArg)) {
        // If it is a wrapped Java object, it is always a global reference, so don't dispose it
        JPy_JObj* obj = (JPy_JObj*) pyArg;
        value->l = obj->objectRef;
        disposer->data = NULL;
        disposer->DisposeArg = NULL;
    } else {
        // For any other Python argument, we first check if the formal parameter is a primitive array
        // and the Python argument is a buffer object

        JPy_JType* paramType = paramDescriptor->type;
        JPy_JType* paramComponentType = paramType->componentType;

        if (paramComponentType != NULL && paramComponentType->isPrimitive && PyObject_CheckBuffer(pyArg)) {
            Py_buffer* pyBuffer;
            int flags;
            Py_ssize_t itemCount;
            jarray jArray;
            void* arrayItems;
            jint itemSize;

            pyBuffer = PyMem_New(Py_buffer, 1);
            if (pyBuffer == NULL) {
                PyErr_NoMemory();
                return -1;
            }

            flags = paramDescriptor->isMutable ? PyBUF_WRITABLE : PyBUF_SIMPLE;
            if (PyObject_GetBuffer(pyArg, pyBuffer, flags) < 0) {
                PyMem_Del(pyBuffer);
                return -1;
            }

            itemCount = pyBuffer->len / pyBuffer->itemsize;
            if (itemCount <= 0) {
                PyBuffer_Release(pyBuffer);
                PyMem_Del(pyBuffer);
                PyErr_Format(PyExc_ValueError, "illegal buffer argument: not a positive item count: %ld", itemCount);
                return -1;
            }

            if (paramComponentType == JPy_JBoolean) {
                jArray = (*jenv)->NewBooleanArray(jenv, itemCount);
                itemSize = sizeof(jboolean);
            } else if (paramComponentType == JPy_JByte) {
                jArray = (*jenv)->NewByteArray(jenv, itemCount);
                itemSize = sizeof(jbyte);
            } else if (paramComponentType == JPy_JChar) {
                jArray = (*jenv)->NewCharArray(jenv, itemCount);
                itemSize = sizeof(jchar);
            } else if (paramComponentType == JPy_JShort) {
                jArray = (*jenv)->NewShortArray(jenv, itemCount);
                itemSize = sizeof(jshort);
            } else if (paramComponentType == JPy_JInt) {
                jArray = (*jenv)->NewIntArray(jenv, itemCount);
                itemSize = sizeof(jint);
            } else if (paramComponentType == JPy_JLong) {
                jArray = (*jenv)->NewLongArray(jenv, itemCount);
                itemSize = sizeof(jlong);
            } else if (paramComponentType == JPy_JFloat) {
                jArray = (*jenv)->NewFloatArray(jenv, itemCount);
                itemSize = sizeof(jfloat);
            } else if (paramComponentType == JPy_JDouble) {
                jArray = (*jenv)->NewDoubleArray(jenv, itemCount);
                itemSize = sizeof(jdouble);
            } else {
                PyBuffer_Release(pyBuffer);
                PyMem_Del(pyBuffer);
                PyErr_SetString(PyExc_RuntimeError, "internal error: illegal primitive Java type");
                return -1;
            }

            if (pyBuffer->len != itemCount * itemSize) {
                Py_ssize_t bufferLen = pyBuffer->len;
                Py_ssize_t bufferItemSize = pyBuffer->itemsize;
                //printf("%ld, %ld, %ld, %ld\n", pyBuffer->len , pyBuffer->itemsize, itemCount, itemSize);
                PyBuffer_Release(pyBuffer);
                PyMem_Del(pyBuffer);
                PyErr_Format(PyExc_ValueError,
                             "illegal buffer argument: expected size was %ld bytes, but got %ld (expected item size was %d bytes, got %ld)",
                             itemCount * itemSize, bufferLen, itemSize, bufferItemSize);
                return -1;
            }

            if (jArray == NULL) {
                PyBuffer_Release(pyBuffer);
                PyMem_Del(pyBuffer);
                PyErr_NoMemory();
                return -1;
            }

            if (!paramDescriptor->isOutput) {
                arrayItems = (*jenv)->GetPrimitiveArrayCritical(jenv, jArray, NULL);
                if (arrayItems == NULL) {
                    PyBuffer_Release(pyBuffer);
                    PyMem_Del(pyBuffer);
                    PyErr_NoMemory();
                    return -1;
                }
                JPy_DIAG_PRINT(JPy_DIAG_F_EXEC|JPy_DIAG_F_MEM, "JType_ConvertPyArgToJObjectArg: moving Python buffer into Java array: pyBuffer->buf=%p, pyBuffer->len=%d\n", pyBuffer->buf, pyBuffer->len);
                memcpy(arrayItems, pyBuffer->buf, itemCount * itemSize);
                (*jenv)->ReleasePrimitiveArrayCritical(jenv, jArray, arrayItems, 0);
            }

            value->l = jArray;
            disposer->data = pyBuffer;
            disposer->DisposeArg = paramDescriptor->isMutable ? JType_DisposeWritableBufferArg : JType_DisposeReadOnlyBufferArg;
        } else {
            jobject objectRef;
            if (JType_ConvertPythonToJavaObject(jenv, paramType, pyArg, &objectRef, JNI_FALSE) < 0) {
                return -1;
            }
            value->l = objectRef;
            disposer->data = NULL;
            disposer->DisposeArg = JType_DisposeLocalObjectRefArg;
        }
    }

    return 0;
}

void JType_DisposeLocalObjectRefArg(JNIEnv* jenv, jvalue* value, void* data)
{
    jobject objectRef = value->l;
    if (objectRef != NULL) {
        JPy_DIAG_PRINT(JPy_DIAG_F_MEM, "JType_DisposeLocalObjectRefArg: objectRef=%p\n", objectRef);
        JPy_DELETE_LOCAL_REF(objectRef);
    }
}

void JType_DisposeReadOnlyBufferArg(JNIEnv* jenv, jvalue* value, void* data)
{
    Py_buffer* pyBuffer;
    jarray jArray;

    pyBuffer = (Py_buffer*) data;
    jArray = (jarray) value->l;

    JPy_DIAG_PRINT(JPy_DIAG_F_MEM, "JType_DisposeReadOnlyBufferArg: pyBuffer=%p, jArray=%p\n", pyBuffer, jArray);

    if (pyBuffer != NULL) {
        PyBuffer_Release(pyBuffer);
        PyMem_Del(pyBuffer);
    }
    if (jArray != NULL) {
        JPy_DELETE_LOCAL_REF(jArray);
    }
}

void JType_DisposeWritableBufferArg(JNIEnv* jenv, jvalue* value, void* data)
{
    Py_buffer* pyBuffer;
    jarray jArray;
    void* arrayItems;

    pyBuffer = (Py_buffer*) data;
    jArray = (jarray) value->l;

    JPy_DIAG_PRINT(JPy_DIAG_F_MEM, "JType_DisposeWritableBufferArg: pyBuffer=%p, jArray=%p\n", pyBuffer, jArray);

    if (pyBuffer != NULL && jArray != NULL) {
        // Copy modified array content back into buffer view
        arrayItems = (*jenv)->GetPrimitiveArrayCritical(jenv, jArray, NULL);
        if (arrayItems != NULL) {
            JPy_DIAG_PRINT(JPy_DIAG_F_EXEC|JPy_DIAG_F_MEM, "JType_DisposeWritableBufferArg: moving Java array into Python buffer: pyBuffer->buf=%p, pyBuffer->len=%d\n", pyBuffer->buf, pyBuffer->len);
            memcpy(pyBuffer->buf, arrayItems, pyBuffer->len);
            (*jenv)->ReleasePrimitiveArrayCritical(jenv, jArray, arrayItems, 0);
        }
        JPy_DELETE_LOCAL_REF(jArray);
        PyBuffer_Release(pyBuffer);
        PyMem_Del(pyBuffer);
    } else if (pyBuffer != NULL) {
        PyBuffer_Release(pyBuffer);
        PyMem_Del(pyBuffer);
    } else if (jArray != NULL) {
        JPy_DELETE_LOCAL_REF(jArray);
    }
}

void JType_InitParamDescriptorFunctions(JPy_ParamDescriptor* paramDescriptor, jboolean isLastVarArg)
{
    JPy_JType* paramType = paramDescriptor->type;

    if (paramType == JPy_JVoid) {
        paramDescriptor->MatchPyArg = NULL;
        paramDescriptor->ConvertPyArg = NULL;
    } else if (paramType == JPy_JBoolean) {
        paramDescriptor->MatchPyArg = JType_MatchPyArgAsJBooleanParam;
        paramDescriptor->ConvertPyArg = JType_ConvertPyArgToJBooleanArg;
    } else if (paramType == JPy_JByte) {
        paramDescriptor->MatchPyArg = JType_MatchPyArgAsJByteParam;
        paramDescriptor->ConvertPyArg = JType_ConvertPyArgToJByteArg;
    } else if (paramType == JPy_JChar) {
        paramDescriptor->MatchPyArg = JType_MatchPyArgAsJCharParam;
        paramDescriptor->ConvertPyArg = JType_ConvertPyArgToJCharArg;
    } else if (paramType == JPy_JShort) {
        paramDescriptor->MatchPyArg = JType_MatchPyArgAsJShortParam;
        paramDescriptor->ConvertPyArg = JType_ConvertPyArgToJShortArg;
    } else if (paramType == JPy_JInt) {
        paramDescriptor->MatchPyArg = JType_MatchPyArgAsJIntParam;
        paramDescriptor->ConvertPyArg = JType_ConvertPyArgToJIntArg;
    } else if (paramType == JPy_JLong) {
        paramDescriptor->MatchPyArg = JType_MatchPyArgAsJLongParam;
        paramDescriptor->ConvertPyArg = JType_ConvertPyArgToJLongArg;
    } else if (paramType == JPy_JFloat) {
        paramDescriptor->MatchPyArg = JType_MatchPyArgAsJFloatParam;
        paramDescriptor->ConvertPyArg = JType_ConvertPyArgToJFloatArg;
    } else if (paramType == JPy_JDouble) {
        paramDescriptor->MatchPyArg = JType_MatchPyArgAsJDoubleParam;
        paramDescriptor->ConvertPyArg = JType_ConvertPyArgToJDoubleArg;
    } else if (paramType == JPy_JString) {
        paramDescriptor->MatchPyArg = JType_MatchPyArgAsJStringParam;
        paramDescriptor->ConvertPyArg = JType_ConvertPyArgToJStringArg;
    //} else if (paramType == JPy_JMap) {
    //} else if (paramType == JPy_JList) {
    //} else if (paramType == JPy_JSet) {
    } else if (paramType == JPy_JPyObject) {
        paramDescriptor->MatchPyArg = JType_MatchPyArgAsJPyObjectParam;
        paramDescriptor->ConvertPyArg = JType_ConvertPyArgToJPyObjectArg;
    } else {
        paramDescriptor->MatchPyArg = JType_MatchPyArgAsJObjectParam;
        paramDescriptor->ConvertPyArg = JType_ConvertPyArgToJObjectArg;
    }
    if (isLastVarArg) {
        paramDescriptor->ConvertVarArgPyArg = JType_ConvertVarArgPyArgToJObjectArg;

        if (paramType->componentType == JPy_JBoolean) {
            paramDescriptor->MatchVarArgPyArg = JType_MatchVarArgPyArgAsJBooleanParam;
        } else if (paramType->componentType == JPy_JByte) {
            paramDescriptor->MatchVarArgPyArg = JType_MatchVarArgPyArgAsJByteParam;
        } else if (paramType->componentType == JPy_JChar) {
            paramDescriptor->MatchVarArgPyArg = JType_MatchVarArgPyArgAsJCharParam;
        } else if (paramType->componentType == JPy_JShort) {
            paramDescriptor->MatchVarArgPyArg = JType_MatchVarArgPyArgAsJShortParam;
        } else if (paramType->componentType == JPy_JInt) {
            paramDescriptor->MatchVarArgPyArg = JType_MatchVarArgPyArgAsJIntParam;
        } else if (paramType->componentType == JPy_JLong) {
            paramDescriptor->MatchVarArgPyArg = JType_MatchVarArgPyArgAsJLongParam;
        } else if (paramType->componentType == JPy_JFloat) {
            paramDescriptor->MatchVarArgPyArg = JType_MatchVarArgPyArgAsJFloatParam;
        } else if (paramType->componentType == JPy_JDouble) {
            paramDescriptor->MatchVarArgPyArg = JType_MatchVarArgPyArgAsJDoubleParam;
        } else if (paramType->componentType == JPy_JString) {
            paramDescriptor->MatchVarArgPyArg = JType_MatchVarArgPyArgAsJStringParam;
        } else if (paramType->componentType == JPy_JPyObject) {
            paramDescriptor->MatchVarArgPyArg = JType_MatchVarArgPyArgAsJPyObjectParam;
        } else {
            paramDescriptor->MatchVarArgPyArg = JType_MatchVarArgPyArgAsJObjectParam;
        }
    }
}

/**
 * The JType's tp_repr slot.
 */
PyObject* JType_repr(JPy_JType* self)
{
    //printf("JType_repr: self=%p\n", self);
    return JPy_FROM_FORMAT("%s(%p)",
                           self->javaName,
                           self->classRef);
}

/**
 * The JType's tp_str slot.
 */
PyObject* JType_str(JPy_JType* self)
{
    JNIEnv* jenv;
    jstring strJObj;
    PyObject* strPyObj;
    jboolean isCopy;
    const char * utfChars;

    JPy_GET_JNI_ENV_OR_RETURN(jenv, NULL)

    //printf("JType_str: self=%p\n", self);

    strJObj = (*jenv)->CallObjectMethod(jenv, self->classRef, JPy_Object_ToString_MID);
    utfChars = (*jenv)->GetStringUTFChars(jenv, strJObj, &isCopy);
    strPyObj = JPy_FROM_FORMAT("%s", utfChars);
    (*jenv)->ReleaseStringUTFChars(jenv, strJObj, utfChars);
    JPy_DELETE_LOCAL_REF(strJObj);

    return strPyObj;
}

/**
 * The JType's tp_dealloc slot.
 */
void JType_dealloc(JPy_JType* self)
{
    JNIEnv* jenv = JPy_GetJNIEnv();

    //printf("JType_dealloc: self->javaName='%s', self->classRef=%p\n", self->javaName, self->classRef);

    PyMem_Del(self->javaName);
    self->javaName = NULL;

    if (jenv != NULL && self->classRef != NULL) {
        (*jenv)->DeleteGlobalRef(jenv, self->classRef);
        self->classRef = NULL;
    }

    JPy_XDECREF(self->superType);
    self->superType = NULL;

    JPy_XDECREF(self->componentType);
    self->componentType = NULL;

    Py_TYPE(self)->tp_free((PyObject*) self);
}

/**
 * The JType's JType_getattro slot.
 */
PyObject* JType_getattro(JPy_JType* self, PyObject* name)
{
    //printf("JType_getattro: %s.%s\n", Py_TYPE(self)->tp_name, JPy_AS_UTF8(name));

    if (!self->isResolved && !self->isResolving) {
        JNIEnv* jenv;
        JPy_GET_JNI_ENV_OR_RETURN(jenv, NULL);
        JType_ResolveType(jenv, self);
    }

    return PyObject_GenericGetAttr((PyObject*) self, name);
}


/**
 * The jpy.JType singleton.
 */
PyTypeObject JType_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "jpy.JType",                 /* tp_name */
    sizeof (JPy_JType),          /* tp_basicsize */
    0,                           /* tp_itemsize */
    (destructor) JType_dealloc,  /* tp_dealloc */
    NULL,                         /* tp_print */
    NULL,                         /* tp_getattr */
    NULL,                         /* tp_setattr */
    NULL,                         /* tp_reserved */
    (reprfunc) JType_repr,        /* tp_repr */
    NULL,                         /* tp_as_number */
    NULL,                         /* tp_as_sequence */
    NULL,                         /* tp_as_mapping */
    NULL,                         /* tp_hash  */
    NULL,                         /* tp_call */
    (reprfunc) JType_str,         /* tp_str */
    (getattrofunc)JType_getattro, /* tp_getattro */
    NULL,                         /* tp_setattro */
    NULL,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,  /* tp_flags */
    "Java Meta Type",             /* tp_doc */
    NULL,                         /* tp_traverse */
    NULL,                         /* tp_clear */
    NULL,                         /* tp_richcompare */
    0,                            /* tp_weaklistoffset */
    NULL,                         /* tp_iter */
    NULL,                         /* tp_iternext */
    NULL,                         /* tp_methods */
    NULL,                         /* tp_members */
    NULL,                         /* tp_getset */
    NULL,                         /* tp_base */
    NULL,                         /* tp_dict */
    NULL,                         /* tp_descr_get */
    NULL,                         /* tp_descr_set */
    0,                            /* tp_dictoffset */
    (initproc) NULL,              /* tp_init */
    NULL,                         /* tp_alloc */
    (newfunc) NULL,               /* tp_new=NULL --> JType instances cannot be created from Python. */
};
