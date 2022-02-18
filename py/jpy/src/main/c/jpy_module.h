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

#ifndef JPY_MODULE_H
#define JPY_MODULE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <Python.h>
#include <structmember.h>
#include <jni.h>

#include "jpy_compat.h"

#define JPY_JNI_VERSION JNI_VERSION_1_6

extern PyObject* JPy_Module;
extern PyObject* JPy_Types;
extern PyObject* JPy_Type_Callbacks;
extern PyObject* JPy_Type_Translations;
extern PyObject* JException_Type;

extern JavaVM* JPy_JVM;
extern jboolean JPy_MustDestroyJVM;


#define JPy_JTYPE_ATTR_NAME_JINIT "__jinit__"

#define JPy_MODULE_ATTR_NAME_TYPES "types"
#define JPy_MODULE_ATTR_NAME_TYPE_CALLBACKS "type_callbacks"
#define JPy_MODULE_ATTR_NAME_TYPE_TRANSLATIONS "type_translations"


/**
 * Gets the current JNI environment pointer.
 * Returns NULL, if the JVM is down.
 *
 * General jpy design guideline: Use the JPy_GetJNIEnv function only in entry points from Python calls into C.
 * Add a JNIEnv* as first parameter to all functions that require it.
 */
JNIEnv* JPy_GetJNIEnv(void);

int JPy_InitGlobalVars(JNIEnv* jenv);
void JPy_ClearGlobalVars(JNIEnv* jenv);

/**
 * Gets the current JNI environment pointer JENV. If this is NULL, it returns the given RET_VALUE.
 * Warning: This method may immediately return, so make sure there will be no memory leaks in this case.
 *
 * General jpy design guideline: Use the JPy_GET_JNI_ENV_OR_RETURN macro only in entry points from Python calls into C.
 * Add a JNIEnv* as first parameter to all functions that require it.
 */
#define JPy_GET_JNI_ENV_OR_RETURN(JENV, RET_VALUE) \
    if ((JENV = JPy_GetJNIEnv()) == NULL) { \
        return (RET_VALUE); \
    } else { \
    }


/**
 * Fetches the last Java exception occurred and raises a new Python exception.
 */
void JPy_HandleJavaException(JNIEnv* jenv);


#define JPy_ON_JAVA_EXCEPTION_GOTO(LABEL) \
    if ((*jenv)->ExceptionCheck(jenv)) { \
        JPy_HandleJavaException(jenv); \
        goto LABEL; \
    }

#define JPy_ON_JAVA_EXCEPTION_RETURN(VALUE) \
    if ((*jenv)->ExceptionCheck(jenv)) { \
        JPy_HandleJavaException(jenv); \
        return VALUE; \
    }

#define JPy_FRAME(TYPE, ON_ERROR, FUNCTION, FRAME_SIZE) \
  JNIEnv* jenv; \
  TYPE result; \
  if ((jenv = JPy_GetJNIEnv()) == NULL) { \
    return ON_ERROR; \
  } \
  if ((*jenv)->PushLocalFrame(jenv, FRAME_SIZE) < 0) { \
    JPy_HandleJavaException(jenv); \
    return ON_ERROR; \
  } \
  result = FUNCTION; \
  (*jenv)->PopLocalFrame(jenv, NULL); \
  return result;

struct JPy_JType;

extern struct JPy_JType* JPy_JBoolean;
extern struct JPy_JType* JPy_JChar;
extern struct JPy_JType* JPy_JByte;
extern struct JPy_JType* JPy_JShort;
extern struct JPy_JType* JPy_JInt;
extern struct JPy_JType* JPy_JLong;
extern struct JPy_JType* JPy_JFloat;
extern struct JPy_JType* JPy_JDouble;
extern struct JPy_JType* JPy_JVoid;
extern struct JPy_JType* JPy_JBooleanObj;
extern struct JPy_JType* JPy_JCharacterObj;
extern struct JPy_JType* JPy_JByteObj;
extern struct JPy_JType* JPy_JShortObj;
extern struct JPy_JType* JPy_JIntegerObj;
extern struct JPy_JType* JPy_JLongObj;
extern struct JPy_JType* JPy_JFloatObj;
extern struct JPy_JType* JPy_JDoubleObj;
extern struct JPy_JType* JPy_JObject;
extern struct JPy_JType* JPy_JClass;
extern struct JPy_JType* JPy_JString;
extern struct JPy_JType* JPy_JPyObject;
extern struct JPy_JType* JPy_JPyModule;

// java.lang.Comparable
extern jclass JPy_Comparable_JClass;
extern jmethodID JPy_Comparable_CompareTo_MID;
// java.lang.Object
extern jclass JPy_Object_JClass;
extern jmethodID JPy_Object_ToString_MID;
extern jmethodID JPy_Object_HashCode_MID;
extern jmethodID JPy_Object_Equals_MID;
// java.lang.Class
extern jclass JPy_Class_JClass;
extern jmethodID JPy_Class_GetName_MID;
extern jmethodID JPy_Class_GetDeclaredConstructors_MID;
extern jmethodID JPy_Class_GetDeclaredFields_MID;
extern jmethodID JPy_Class_GetDeclaredMethods_MID;
extern jmethodID JPy_Class_GetFields_MID;
extern jmethodID JPy_Class_GetMethods_MID;
extern jmethodID JPy_Class_GetComponentType_MID;
extern jmethodID JPy_Class_IsPrimitive_MID;
extern jmethodID JPy_Class_IsInterface_MID;
// java.lang.reflect.Constructor
extern jclass JPy_Constructor_JClass;
extern jmethodID JPy_Constructor_GetModifiers_MID;
extern jmethodID JPy_Constructor_GetParameterTypes_MID;
// java.lang.reflect.Method
extern jclass JPy_Method_JClass;
extern jmethodID JPy_Method_GetName_MID;
extern jmethodID JPy_Method_GetModifiers_MID;
extern jmethodID JPy_Method_GetParameterTypes_MID;
extern jmethodID JPy_Method_GetReturnType_MID;
// java.lang.reflect.Field
extern jclass JPy_Field_JClass;
extern jmethodID JPy_Field_GetName_MID;
extern jmethodID JPy_Field_GetModifiers_MID;
extern jmethodID JPy_Field_GetType_MID;
// java.util.Map
extern jclass JPy_Map_JClass;
extern jclass JPy_Map_Entry_JClass;
extern jmethodID JPy_Map_entrySet_MID;
extern jmethodID JPy_Map_put_MID;
extern jmethodID JPy_Map_clear_MID;
extern jmethodID JPy_Map_Entry_getKey_MID;
extern jmethodID JPy_Map_Entry_getValue_MID;
// java.util.Set
extern jclass JPy_Set_JClass;
extern jmethodID JPy_Set_Iterator_MID;
// java.util.Iterator
extern jclass JPy_Iterator_JClass;
extern jmethodID JPy_Iterator_next_MID;
extern jmethodID JPy_Iterator_hasNext_MID;

extern jclass JPy_RuntimeException_JClass;
extern jclass JPy_OutOfMemoryError_JClass;
extern jclass JPy_FileNotFoundException_JClass;
extern jclass JPy_UnsupportedOperationException_JClass;
extern jclass JPy_KeyError_JClass;
extern jclass JPy_StopIteration_JClass;

extern jclass JPy_Boolean_JClass;
extern jmethodID JPy_Boolean_Init_MID;
extern jmethodID JPy_Boolean_BooleanValue_MID;

extern jclass JPy_Character_JClass;
extern jmethodID JPy_Character_Init_MID;
extern jmethodID JPy_Character_CharValue_MID;

extern jclass JPy_Number_JClass;

extern jclass JPy_Byte_JClass;
extern jmethodID JPy_Byte_Init_MID;

extern jclass JPy_Short_JClass;
extern jmethodID JPy_Short_Init_MID;

extern jclass JPy_Integer_JClass;
extern jmethodID JPy_Integer_Init_MID;

extern jclass JPy_Long_JClass;
extern jmethodID JPy_Long_Init_MID;

extern jclass JPy_Float_JClass;
extern jmethodID JPy_Float_Init_MID;

extern jclass JPy_Double_JClass;
extern jmethodID JPy_Double_Init_MID;

extern jclass JPy_Number_JClass;
extern jmethodID JPy_Number_IntValue_MID;
extern jmethodID JPy_Number_LongValue_MID;
extern jmethodID JPy_Number_DoubleValue_MID;

extern jclass JPy_String_JClass;
extern jclass JPy_Void_JClass;

extern jclass JPy_PyObject_JClass;
extern jmethodID JPy_PyObject_GetPointer_MID;
extern jmethodID JPy_PyObject_UnwrapProxy_SMID;
extern jmethodID JPy_PyObject_Init_MID;

extern jclass JPy_PyDictWrapper_JClass;
extern jmethodID JPy_PyDictWrapper_GetPointer_MID;

#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* !JPY_MODULE_H */