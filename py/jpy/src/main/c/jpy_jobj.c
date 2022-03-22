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
#include "jpy_jarray.h"
#include "jpy_jtype.h"
#include "jpy_jobj.h"
#include "jpy_jmethod.h"
#include "jpy_jfield.h"
#include "jpy_conv.h"

PyObject* JObj_New(JNIEnv* jenv, jobject objectRef)
{
    JPy_JType* type;
    type = JType_GetTypeForObject(jenv, objectRef, JNI_TRUE);
    if (type == NULL) {
        return NULL;
    }
    return JObj_FromType(jenv, type, objectRef);
}

PyObject* JObj_FromType(JNIEnv* jenv, JPy_JType* type, jobject objectRef)
{
    PyObject* callable;
    PyObject* callableResult;

    JPy_JObj* obj;

    obj = (JPy_JObj*) PyObject_New(JPy_JObj, JTYPE_AS_PYTYPE(type));
    if (obj == NULL) {
        return NULL;
    }

    objectRef = (*jenv)->NewGlobalRef(jenv, objectRef);
    if (objectRef == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    obj->objectRef = objectRef;

    // For special treatment of primitive array refer to JType_InitSlots()
    if (type->componentType != NULL && type->componentType->isPrimitive) {
        JPy_JArray* array;

        array = (JPy_JArray*) obj;
        array->bufferExportCount = 0;
        array->buf = NULL;
    }

    // we check the type translations dictionary for a callable for this java type name,
    // and apply the returned callable to the wrapped object
    callable = PyDict_GetItemString(JPy_Type_Translations, type->javaName);
    if (callable != NULL) {
        if (PyCallable_Check(callable)) {
            callableResult = PyObject_CallFunction(callable, "OO", type, obj);
            if (callableResult == NULL) {
                return Py_None;
            } else {
                return callableResult;
            }
        }
    }

    return (PyObject *)obj;
}

int JObj_init_internal(JNIEnv* jenv, JPy_JObj* self, PyObject* args, PyObject* kwds)
{
    PyTypeObject* type;
    JPy_JType* jType;
    PyObject* constructor;
    JPy_JMethod* jMethod;
    jobject objectRef;
    jvalue* jArgs;
    JPy_ArgDisposer* jDisposers;
    int isVarArgsArray;

    type = ((PyObject*) self)->ob_type;

    constructor = PyDict_GetItemString(type->tp_dict, JPy_JTYPE_ATTR_NAME_JINIT);
    if (constructor == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "no constructor found (missing JType attribute '" JPy_JTYPE_ATTR_NAME_JINIT "')");
        return -1;
    }

    if (!PyObject_TypeCheck(constructor, &JOverloadedMethod_Type)) {
        PyErr_SetString(PyExc_RuntimeError, "invalid JType attribute '"  JPy_JTYPE_ATTR_NAME_JINIT  "': expected type JOverloadedMethod_Type");
        return -1;
    }

    jType = (JPy_JType*) type;
    if (jType->classRef == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "internal error: Java class reference is NULL");
        return -1;
    }

    jMethod = JOverloadedMethod_FindMethod(jenv, (JPy_JOverloadedMethod*) constructor, args, JNI_FALSE, &isVarArgsArray);
    if (jMethod == NULL) {
        return -1;
    }

    if (JMethod_CreateJArgs(jenv, jMethod, args, &jArgs, &jDisposers, isVarArgsArray) < 0) {
        return -1;
    }

    JPy_DIAG_PRINT(JPy_DIAG_F_MEM, "JObj_init: calling Java constructor %s\n", jType->javaName);

    objectRef = (*jenv)->NewObjectA(jenv, jType->classRef, jMethod->mid, jArgs);
    JPy_ON_JAVA_EXCEPTION_RETURN(-1);

    if (objectRef == NULL) {
        PyErr_NoMemory();
        return -1;
    }

    if (jMethod->paramCount > 0) {
        JMethod_DisposeJArgs(jenv, jMethod->paramCount, jArgs, jDisposers);
    }

    objectRef = (*jenv)->NewGlobalRef(jenv, objectRef);
    if (objectRef == NULL) {
        PyErr_NoMemory();
        return -1;
    }

    // Note:  __init__ may be called multiple times, so we have to release the old objectRef
    if (self->objectRef != NULL) {
        (*jenv)->DeleteGlobalRef(jenv, self->objectRef);
    }

    self->objectRef = objectRef;

    JPy_DIAG_PRINT(JPy_DIAG_F_MEM, "JObj_init: self->objectRef=%p\n", self->objectRef);

    return 0;
}

/**
 * The JObj type's tp_init slot. Called when the type is used to create new instances (constructor).
 */
int JObj_init(JPy_JObj* self, PyObject* args, PyObject* kwds)
{
    JPy_FRAME(int, -1, JObj_init_internal(jenv, self, args, kwds), 16)
}

/**
 * The JObj type's tp_dealloc slot. Called when the reference count reaches zero.
 */
void JObj_dealloc(JPy_JObj* self)
{
    JNIEnv* jenv;
    JPy_JType* jtype;

    JPy_DIAG_PRINT(JPy_DIAG_F_MEM, "JObj_dealloc: releasing instance of %s, self->objectRef=%p\n", Py_TYPE(self)->tp_name, self->objectRef);

    jtype = (JPy_JType *)Py_TYPE(self);
    if (jtype->componentType != NULL && jtype->componentType->isPrimitive) {
        JPy_JArray* array;
        array = (JPy_JArray*) self;
        assert(array->bufferExportCount == 0);
        if (array->buf != NULL) {
            JArray_ReleaseJavaArrayElements(array, array->javaType);
        }
  
    }    

    jenv = JPy_GetJNIEnv();
    if (jenv != NULL) {
        if (self->objectRef != NULL) {
            (*jenv)->DeleteGlobalRef(jenv, self->objectRef);
        }
    }

    Py_TYPE(self)->tp_free((PyObject*) self);
}

int JObj_CompareTo(JNIEnv* jenv, JPy_JObj* obj1, JPy_JObj* obj2)
{
    jobject ref1;
    jobject ref2;
    int value;

    ref1 = obj1->objectRef;
    ref2 = obj2->objectRef;

    if (ref1 == ref2 || (*jenv)->IsSameObject(jenv, ref1, ref2)) {
        return 0;
    } else if ((*jenv)->IsInstanceOf(jenv, ref1, JPy_Comparable_JClass)) {
        value = (*jenv)->CallIntMethod(jenv, ref1, JPy_Comparable_CompareTo_MID, ref2);
        (*jenv)->ExceptionClear(jenv); // we can't deal with exceptions here, so clear any
    } else {
        value = (char*) ref1 - (char*) ref2;
    }

    return (value == 0) ? 0 : (value < 0) ? -1 : +1;
}

int JObj_Equals(JNIEnv* jenv, JPy_JObj* obj1, JPy_JObj* obj2)
{
    jobject ref1;
    jobject ref2;
    int returnValue;

    ref1 = obj1->objectRef;
    ref2 = obj2->objectRef;

    if ((*jenv)->IsSameObject(jenv, ref1, ref2)) {
        returnValue = 1;
    } else {
        returnValue = (*jenv)->CallIntMethod(jenv, ref1, JPy_Object_Equals_MID, ref2);
    }
    (*jenv)->ExceptionClear(jenv); // we can't deal with exceptions here, so clear any
    return returnValue;
}

/**
 * The JObj type's tp_richcompare slot. Python: obj1 <opid> obj2
 */
PyObject* JObj_richcompare(PyObject* obj1, PyObject* obj2, int opid)
{
    JNIEnv* jenv;

    if (!JObj_Check(obj1) || !JObj_Check(obj2)) {
        Py_RETURN_FALSE;
    }

    JPy_GET_JNI_ENV_OR_RETURN(jenv, NULL);

    if (opid == Py_LT) {
        int value = JObj_CompareTo(jenv, (JPy_JObj*) obj1, (JPy_JObj*) obj2);
        if (value == -2) {
            return NULL;
        } else if (value == -1) {
            Py_RETURN_TRUE;
        } else {
            Py_RETURN_FALSE;
        }
    } else if (opid == Py_LE) {
        int value = JObj_CompareTo(jenv, (JPy_JObj*) obj1, (JPy_JObj*) obj2);
        if (value == -2) {
            return NULL;
        } else if (value == -1 || value == 0) {
            Py_RETURN_TRUE;
        } else {
            Py_RETURN_FALSE;
        }
    } else if (opid == Py_GT) {
        int value = JObj_CompareTo(jenv, (JPy_JObj*) obj1, (JPy_JObj*) obj2);
        if (value == -2) {
            return NULL;
        } else if (value == +1) {
            Py_RETURN_TRUE;
        } else {
            Py_RETURN_FALSE;
        }
    } else if (opid == Py_GE) {
        int value = JObj_CompareTo(jenv, (JPy_JObj*) obj1, (JPy_JObj*) obj2);
        if (value == -2) {
            return NULL;
        } else if (value == +1 || value == 0) {
            Py_RETURN_TRUE;
        } else {
            Py_RETURN_FALSE;
        }
    } else if (opid == Py_EQ) {
        int value = JObj_Equals(jenv, (JPy_JObj*) obj1, (JPy_JObj*) obj2);
        if (value == -2) {
            return NULL;
        } else if (value) {
            Py_RETURN_TRUE;
        } else {
            Py_RETURN_FALSE;
        }
    } else if (opid == Py_NE) {
        int value = JObj_Equals(jenv, (JPy_JObj*) obj1, (JPy_JObj*) obj2);
        if (value == -2) {
            return NULL;
        } else if (value) {
            Py_RETURN_FALSE;
        } else {
            Py_RETURN_TRUE;
        }
    } else {
        PyErr_SetString(PyExc_RuntimeError, "internal error: unrecognized opid");
        return NULL;
    }
}

/**
 * The JObj type's tp_hash slot. Python: hash(obj)
 */
long JObj_hash(JPy_JObj* self)
{
    JNIEnv* jenv;
    jenv = JPy_GetJNIEnv();
    if (jenv != NULL) {
        int returnValue = (*jenv)->CallIntMethod(jenv, self->objectRef, JPy_Object_HashCode_MID);
        (*jenv)->ExceptionClear(jenv); // we can't deal with exceptions here, so clear any
        return returnValue;
    }
    return -1;
}


/**
 * The JObj type's tp_repr slot. Python: repr(obj))
 */
PyObject* JObj_repr(JPy_JObj* self)
{
    return JPy_FROM_FORMAT("%s(objectRef=%p)", Py_TYPE(self)->tp_name, self->objectRef);
}

/**
 * The JObj type's tp_str slot. Calls Object.toString() on Java Objects. Python: str(obj)
 */
PyObject* JObj_str(JPy_JObj* self)
{
    JNIEnv* jenv;
    jstring stringRef;
    PyObject* returnValue;

    JPy_GET_JNI_ENV_OR_RETURN(jenv, NULL)

    if (self->objectRef == NULL) {
        return Py_BuildValue("");
    }

    returnValue = NULL;
    stringRef = (*jenv)->CallObjectMethod(jenv, self->objectRef, JPy_Object_ToString_MID);
    JPy_ON_JAVA_EXCEPTION_GOTO(error);
    returnValue = JPy_FromJString(jenv, stringRef);

error:
    (*jenv)->DeleteLocalRef(jenv, stringRef);

    return returnValue;
}


/**
 * The JObj type's tp_setattro slot.
 */
int JObj_setattro(JPy_JObj* self, PyObject* name, PyObject* value)
{
    PyObject* oldValue;

    //printf("JObj_setattro: %s.%s\n", Py_TYPE(self)->tp_name, JPy_AS_UTF8(name));

    oldValue = PyObject_GenericGetAttr((PyObject*) self, name);
    if (oldValue != NULL && PyObject_TypeCheck(oldValue, &JField_Type)) {
        JNIEnv* jenv;
        JPy_JField* field;
        JPy_JType* type;

        field = (JPy_JField*) oldValue;
        type = field->type;

        JPy_GET_JNI_ENV_OR_RETURN(jenv, -1)

        if (type == JPy_JBoolean) {
            jboolean item = JPy_AS_JBOOLEAN(value);
            (*jenv)->SetBooleanField(jenv, self->objectRef, field->fid, item);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
        } else if (type == JPy_JChar) {
            jchar item = JPy_AS_JCHAR(value);
            (*jenv)->SetCharField(jenv, self->objectRef, field->fid, item);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
        } else if (type == JPy_JByte) {
            jbyte item = JPy_AS_JBYTE(value);
            (*jenv)->SetByteField(jenv, self->objectRef, field->fid, item);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
        } else if (type == JPy_JShort) {
            jshort item = JPy_AS_JSHORT(value);
            (*jenv)->SetShortField(jenv, self->objectRef, field->fid, item);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
        } else if (type == JPy_JInt) {
            jint item = JPy_AS_JINT(value);
            (*jenv)->SetIntField(jenv, self->objectRef, field->fid, item);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
        } else if (type == JPy_JLong) {
            jlong item = JPy_AS_JLONG(value);
            (*jenv)->SetLongField(jenv, self->objectRef, field->fid, item);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
        } else if (type == JPy_JFloat) {
            jfloat item = JPy_AS_JFLOAT(value);
            (*jenv)->SetFloatField(jenv, self->objectRef, field->fid, item);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
        } else if (type == JPy_JDouble) {
            jdouble item = JPy_AS_JDOUBLE(value);
            (*jenv)->SetDoubleField(jenv, self->objectRef, field->fid, item);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
        } else {
            jobject objectRef;
            if (JPy_AsJObjectWithType(jenv, value, &objectRef, field->type) < 0) {
                return -1;
            }
            (*jenv)->SetObjectField(jenv, self->objectRef, field->fid, objectRef);
            JPy_ON_JAVA_EXCEPTION_RETURN(-1);
        }
        return 0;
    } else {
        return PyObject_GenericSetAttr((PyObject*) self, name, value);
    }
}

/**
 * The JObj type's tp_getattro slot.
 * This is important: wrap callable objects of type JOverloadedMethod_Type into python methods so that
 * a method call to an instance x of class X becomes: x.m() --> X.m(x)
 */
PyObject* JObj_getattro(JPy_JObj* self, PyObject* name)
{
    JPy_JType* selfType;
    PyObject* value;

    //printf("JObj_getattro: %s.%s\n", Py_TYPE(self)->tp_name, JPy_AS_UTF8(name));

    // First make sure that the Java type is resolved, otherwise we won't find any methods at all.
    selfType = (JPy_JType*) Py_TYPE(self);
    if (!selfType->isResolved) {
        JNIEnv* jenv;
        JPy_GET_JNI_ENV_OR_RETURN(jenv, NULL)
        if (JType_ResolveType(jenv, selfType) < 0) {
            return NULL;
        }
    }

    // todo: implement a special lookup: we need to override __getattro__ of JType (--> JType_getattro) as well so that we know if a method
    // is called on a class rather than on an instance. Using PyObject_GenericGetAttr will also call  JType_getattro,
    // but then we loose the information that a method is called on an instance and not on a class.
    value = PyObject_GenericGetAttr((PyObject*) self, name);
    if (value == NULL) {
        //printf("JObj_getattro: not found!\n");
        return NULL;
    }
    if (PyObject_TypeCheck(value, &JOverloadedMethod_Type)) {
        //JPy_JOverloadedMethod* overloadedMethod = (JPy_JOverloadedMethod*) value;
        //printf("JObj_getattro: wrapping JOverloadedMethod, overloadCount=%d\n", PyList_Size(overloadedMethod->methodList));
#if defined(JPY_COMPAT_33P)
        return PyMethod_New(value, (PyObject*) self);
#elif defined(JPY_COMPAT_27)
        // todo: py27: 3rd arg must be class of self, check if NULL is ok here
        return PyMethod_New(value, (PyObject*) self, NULL);
#else
#error JPY_VERSION_ERROR
#endif
    } else if (PyObject_TypeCheck(value, &JField_Type)) {
        JNIEnv* jenv;
        JPy_JField* field;
        JPy_JType* type;

        field = (JPy_JField*) value;
        type = field->type;

        JPy_GET_JNI_ENV_OR_RETURN(jenv, NULL)

        if (type == JPy_JBoolean) {
            jboolean item = (*jenv)->GetBooleanField(jenv, self->objectRef, field->fid);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            return JPy_FROM_JBOOLEAN(item);
        } else if (type == JPy_JChar) {
            jchar item = (*jenv)->GetCharField(jenv, self->objectRef, field->fid);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            return JPy_FROM_JCHAR(item);
        } else if (type == JPy_JByte) {
            jbyte item = (*jenv)->GetByteField(jenv, self->objectRef, field->fid);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            return JPy_FROM_JBYTE(item);
        } else if (type == JPy_JShort) {
            jshort item = (*jenv)->GetShortField(jenv, self->objectRef, field->fid);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            return JPy_FROM_JSHORT(item);
        } else if (type == JPy_JInt) {
            jint item = (*jenv)->GetIntField(jenv, self->objectRef, field->fid);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            return JPy_FROM_JINT(item);
        } else if (type == JPy_JLong) {
            jlong item = (*jenv)->GetLongField(jenv, self->objectRef, field->fid);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            return JPy_FROM_JLONG(item);
        } else if (type == JPy_JFloat) {
            jfloat item = (*jenv)->GetFloatField(jenv, self->objectRef, field->fid);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            return JPy_FROM_JFLOAT(item);
        } else if (type == JPy_JDouble) {
            jdouble item = (*jenv)->GetDoubleField(jenv, self->objectRef, field->fid);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            return JPy_FROM_JDOUBLE(item);
        } else {
            PyObject* returnValue;
            jobject item = (*jenv)->GetObjectField(jenv, self->objectRef, field->fid);
            JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
            returnValue = JPy_FromJObjectWithType(jenv, item, field->type);
            (*jenv)->DeleteLocalRef(jenv, item);
            return returnValue;
        }
    } else {
        //printf("JObj_getattro: passing through\n");
    }
    return value;
}

/**
 * The JObj type's sq_length field of the tp_as_sequence slot. Called if len(obj) is called.
 * Only used for array types (type->componentType != NULL).
 */
Py_ssize_t JObj_sq_length(JPy_JObj* self)
{
    JNIEnv* jenv;
    jsize length;
    JPy_GET_JNI_ENV_OR_RETURN(jenv, -1)
    length = (*jenv)->GetArrayLength(jenv, self->objectRef);
    //printf("JObj_sq_length: length=%d\n", length);
    return (Py_ssize_t) length;
}

/*
 * The JObj type's sq_item field of the tp_as_sequence slot. Called if 'item = obj[index]' is used.
 * Only used for array types (type->componentType != NULL).
 */
PyObject* JObj_sq_item(JPy_JObj* self, Py_ssize_t index)
{
    JNIEnv* jenv;
    JPy_JType* type;
    JPy_JType* componentType;
    jsize length;

    JPy_GET_JNI_ENV_OR_RETURN(jenv, NULL)

    //printf("JObj_sq_item: index=%d\n", index);

    type = (JPy_JType*) Py_TYPE(self);
    componentType = type->componentType;
    if (componentType == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "internal error: object is not an array");
        return NULL;
    }

    // This is annoying and slow in Python 3.3.2: We must have this check, in order to raise an PyExc_IndexError,
    // otherwise Python functions such as list(jarr) will not succeed.
    // Tis is really strange, because n = sq_length() will be called and subsequent sq_item(index=0 ... n) calls will be done.
    length = (*jenv)->GetArrayLength(jenv, self->objectRef);
    if (index < 0 || index >= length) {
        PyErr_SetString(PyExc_IndexError, "Java array index out of bounds");
        return NULL;
    }

    if (componentType == JPy_JBoolean) {
        jboolean item;
        (*jenv)->GetBooleanArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
        return JPy_FROM_JBOOLEAN(item);
    } else if (componentType == JPy_JChar) {
        jchar item;
        (*jenv)->GetCharArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
        return JPy_FROM_JCHAR(item);
    } else if (componentType == JPy_JByte) {
        jbyte item;
        (*jenv)->GetByteArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
        return JPy_FROM_JBYTE(item);
    } else if (componentType == JPy_JShort) {
        jshort item;
        (*jenv)->GetShortArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
        return JPy_FROM_JSHORT(item);
    } else if (componentType == JPy_JInt) {
        jint item;
        (*jenv)->GetIntArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
        return JPy_FROM_JINT(item);
    } else if (componentType == JPy_JLong) {
        jlong item;
        (*jenv)->GetLongArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
        return JPy_FROM_JLONG(item);
    } else if (componentType == JPy_JFloat) {
        jfloat item;
        (*jenv)->GetFloatArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
        return JPy_FROM_JFLOAT(item);
    } else if (componentType == JPy_JDouble) {
        jdouble item;
        (*jenv)->GetDoubleArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
        return JPy_FROM_JDOUBLE(item);
    } else {
        PyObject* returnValue;
        jobject item = (*jenv)->GetObjectArrayElement(jenv, self->objectRef, (jsize) index);
        JPy_ON_JAVA_EXCEPTION_RETURN(NULL);
        returnValue = JPy_FromJObjectWithType(jenv, item, type->componentType);
        (*jenv)->DeleteLocalRef(jenv, item);
        return returnValue;
    }
}

/*
 * The JObj type's sq_ass_item field of the tp_as_sequence slot. Called if 'obj[index] = item' is used.
 * Only used for array types (type->componentType != NULL).
 */
int JObj_sq_ass_item(JPy_JObj* self, Py_ssize_t index, PyObject* pyItem)
{
    JNIEnv* jenv;
    JPy_JType* type;
    JPy_JType* componentType;

    JPy_GET_JNI_ENV_OR_RETURN(jenv, -1)

    type = (JPy_JType*) Py_TYPE(self);
    componentType = type->componentType;
    if (type->componentType == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "object is not a Java array");
        return -1;
    }

    // fixes https://github.com/bcdev/jpy/issues/52
    if (pyItem == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "cannot delete items of Java arrays");
        return -1;
    }

    // Note: the following item assignments are not value range checked
    if (componentType == JPy_JBoolean) {
        jboolean item = JPy_AS_JBOOLEAN(pyItem);
        (*jenv)->SetBooleanArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    } else if (componentType == JPy_JChar) {
        jchar item = JPy_AS_JCHAR(pyItem);
        (*jenv)->SetCharArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    } else if (componentType == JPy_JByte) {
        jbyte item = JPy_AS_JBYTE(pyItem);
        (*jenv)->SetByteArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    } else if (componentType == JPy_JShort) {
        jshort item = JPy_AS_JSHORT(pyItem);
        (*jenv)->SetShortArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    } else if (componentType == JPy_JInt) {
        jint item = JPy_AS_JINT(pyItem);
        (*jenv)->SetIntArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    } else if (componentType == JPy_JLong) {
        jlong item = JPy_AS_JLONG(pyItem);
        (*jenv)->SetLongArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    } else if (componentType == JPy_JFloat) {
        jfloat item = JPy_AS_JFLOAT(pyItem);
        (*jenv)->SetFloatArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    } else if (componentType == JPy_JDouble) {
        jdouble item = JPy_AS_JDOUBLE(pyItem);
        (*jenv)->SetDoubleArrayRegion(jenv, self->objectRef, (jsize) index, 1, &item);
        JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    } else {
        jobject item;
        if (JPy_AsJObjectWithType(jenv, pyItem, &item, type->componentType) < 0) {
            return -1;
        }
        (*jenv)->SetObjectArrayElement(jenv, self->objectRef, (jsize) index, item);
        JPy_ON_JAVA_EXCEPTION_RETURN(-1);
    }
    return 0;
}

/**
 * The JObj type's tp_as_sequence slot.
 * Implements the <sequence> interface for array types (type->componentType != NULL).
 */
static PySequenceMethods JObj_as_sequence = {
    (lenfunc) JObj_sq_length,            /* sq_length */
    NULL,   /* sq_concat */
    NULL,   /* sq_repeat */
    (ssizeargfunc) JObj_sq_item,         /* sq_item */
    NULL,   /* was_sq_slice */
    (ssizeobjargproc) JObj_sq_ass_item,  /* sq_ass_item */
    NULL,   /* was_sq_ass_slice */
    NULL,   /* sq_contains */
    NULL,   /* sq_inplace_concat */
    NULL,   /* sq_inplace_repeat */
};


int JType_InitSlots(JPy_JType* type)
{
    PyTypeObject* typeObj;
    jboolean isArray;
    jboolean isPrimitiveArray;

    isArray = type->componentType != NULL;
    isPrimitiveArray = isArray && type->componentType->isPrimitive;

    typeObj = JTYPE_AS_PYTYPE(type);

    Py_REFCNT(typeObj) = 1;
    Py_TYPE(typeObj) = NULL;
    Py_SIZE(typeObj) = 0;
    // todo: The following lines are actually correct, but setting Py_TYPE(type) = &JType_Type results in an interpreter crash. Why?
    // This is still a problem because all the JType slots are actually never called (especially JType_getattro is
    // needed to resolve unresolved JTypes and to recognize static field and methods access)
    //Py_INCREF(&JType_Type);
    //Py_TYPE(type) = &JType_Type;
    //Py_SIZE(type) = sizeof (JPy_JType);

    typeObj->tp_basicsize = isPrimitiveArray ? sizeof (JPy_JArray) : sizeof (JPy_JObj);
    typeObj->tp_itemsize = 0;
    typeObj->tp_base = type->superType != NULL ? JTYPE_AS_PYTYPE(type->superType) : &JType_Type;
    //typeObj->tp_base = (PyTypeObject*) type->superType;
    typeObj->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    // If I uncomment the following line, I get (unpredictable) interpreter crashes
    // (see also http://stackoverflow.com/questions/8066438/how-to-dynamically-create-a-derived-type-in-the-python-c-api)
    //typeObj->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HEAPTYPE;

    #if defined(JPY_COMPAT_27)
    if (isPrimitiveArray) {
        typeObj->tp_flags |= Py_TPFLAGS_HAVE_NEWBUFFER;
    }
    #endif

    typeObj->tp_getattro = (getattrofunc) JObj_getattro;
    typeObj->tp_setattro = (setattrofunc) JObj_setattro;

    // Note: we may later want to add  <sequence> protocol to 'java.lang.String' and 'java.util.List' types.
    // However, we cannot check directly against these global variable 'JPy_JString' and 'JPy_JList' here because
    // the current function (JType_InitSlots) is called to compute the actual values for the global
    // 'JPy_JString' and 'JPy_JList' variables!
    // So we actually have to check against the Java Object types in order to create and assign slots for the
    // Python protocols: java.lang.String --> sequence, java.util.Map --> dict, java.util.List --> list, java.util.Set --> set.


    // If this type is an array type, add support for the <sequence> protocol
    if (isArray) {
        typeObj->tp_as_sequence = &JObj_as_sequence;
    }

    if (isPrimitiveArray) {
        const char* componentTypeName = type->componentType->javaName;
        if (strcmp(componentTypeName, "boolean") == 0) {
            typeObj->tp_as_buffer = &JArray_as_buffer_boolean;
        } else if (strcmp(componentTypeName, "char") == 0) {
            typeObj->tp_as_buffer = &JArray_as_buffer_char;
        } else if (strcmp(componentTypeName, "byte") == 0) {
            typeObj->tp_as_buffer = &JArray_as_buffer_byte;
        } else if (strcmp(componentTypeName, "short") == 0) {
            typeObj->tp_as_buffer = &JArray_as_buffer_short;
        } else if (strcmp(componentTypeName, "int") == 0) {
            typeObj->tp_as_buffer = &JArray_as_buffer_int;
        } else if (strcmp(componentTypeName, "long") == 0) {
            typeObj->tp_as_buffer = &JArray_as_buffer_long;
        } else if (strcmp(componentTypeName, "float") == 0) {
            typeObj->tp_as_buffer = &JArray_as_buffer_float;
        } else if (strcmp(componentTypeName, "double") == 0) {
            typeObj->tp_as_buffer = &JArray_as_buffer_double;
        }
    }

    //printf("JType_InitSlots: typeObj->tp_as_buffer=%p\n", typeObj->tp_as_buffer);

    typeObj->tp_alloc = PyType_GenericAlloc;
    typeObj->tp_new = PyType_GenericNew;
    typeObj->tp_init = (initproc) JObj_init;
    typeObj->tp_richcompare = (richcmpfunc) JObj_richcompare;
    typeObj->tp_hash = (hashfunc) JObj_hash;
    typeObj->tp_repr = (reprfunc) JObj_repr;
    typeObj->tp_str = (reprfunc) JObj_str;
    typeObj->tp_dealloc = (destructor) JObj_dealloc;

    // Check if we should set type.__module__ to the to the first part (up to the last dot) of the tp_name.
    // See http://docs.python.org/3/c-api/exceptions.html?highlight=pyerr_newexception#PyErr_NewException

    // Note that PyType_Ready() will set our typeObj->ob_type to &PyType_Type, while JType_New() created
    // the typeObj with an typeObj->ob_type set to &JType_Type.
    if (PyType_Ready(typeObj) < 0) {
        JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JType_InitSlots: INTERNAL ERROR: PyType_Ready() failed\n");
        return -1;
    }

    //printf("+++++++++++++++++++++++++++++++++++++++++ typeObj->ob_type=%p\n", ((PyObject*)typeObj)->ob_type);

    JPy_DIAG_PRINT(JPy_DIAG_F_TYPE, "JType_InitSlots: typeObj=%p, Py_TYPE(typeObj)=%p, typeObj->tp_name=\"%s\", typeObj->tp_base=%p, typeObj->tp_init=%p, &JType_Type=%p, &PyType_Type=%p, JObj_init=%p\n",
                   typeObj, Py_TYPE(typeObj), typeObj->tp_name, typeObj->tp_base, typeObj->tp_init, &JType_Type, &PyType_Type, JObj_init);

    return 0;
}

// This is only a good test as long JObj_init() is not used in other types
#define JPY_IS_JTYPE(T)  (JTYPE_AS_PYTYPE(T)->tp_init == (initproc) JObj_init)


int JObj_Check(PyObject* arg)
{
    return JPY_IS_JTYPE(Py_TYPE(arg));
}

int JType_Check(PyObject* arg)
{
    return PyType_Check(arg) && JPY_IS_JTYPE(arg);
}

