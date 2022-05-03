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

#include "jpy_module.h"
#include "jpy_diag.h"
#include "jpy_jtype.h"
#include "jpy_jobj.h"
#include "jpy_jmethod.h"
#include "jpy_conv.h"
#include "jpy_compat.h"


JPy_JMethod* JMethod_New(JPy_JType* declaringClass,
                         PyObject* name,
                         int paramCount,
                         JPy_ParamDescriptor* paramDescriptors,
                         JPy_ReturnDescriptor* returnDescriptor,
                         jboolean isStatic,
                         jboolean isVarArgs,
                         jmethodID mid)
{
    PyTypeObject* type = &JMethod_Type;
    JPy_JMethod* method;

    method = (JPy_JMethod*) type->tp_alloc(type, 0);
    method->declaringClass = declaringClass;
    method->name = name;
    method->paramCount = paramCount;
    method->paramDescriptors = paramDescriptors;
    method->returnDescriptor = returnDescriptor;
    method->isStatic = isStatic;
    method->isVarArgs = isVarArgs;
    method->mid = mid;

    JPy_INCREF(declaringClass);
    JPy_INCREF(method->name);

    return method;
}


/**
 * The JMethod type's tp_dealloc slot. 
 */
void JMethod_dealloc(JPy_JMethod* self)
{
    JNIEnv* jenv;

    JPy_DECREF(self->declaringClass);
    JPy_DECREF(self->name);

    jenv = JPy_GetJNIEnv();
    if (jenv != NULL) {
        int i;
        for (i = 0; i < self->paramCount; i++) {
            JPy_DECREF((self->paramDescriptors + i)->type);
        }
        JPy_DECREF((self->returnDescriptor + i)->type);
    }

    PyMem_Del(self->paramDescriptors);
    PyMem_Del(self->returnDescriptor);
    
    Py_TYPE(self)->tp_free((PyObject*) self);
}

void JMethod_Del(JPy_JMethod* method)
{
    JMethod_dealloc(method);
}

/**
 * Matches the give Python argument tuple against the Java method's formal parameters.
 * Returns the sum of the i-th argument against the i-th Java parameter.
 * The maximum match value returned is 100 * method->paramCount.
 *
 * The isVarArgsArray pointer is set to 1 if this is a varargs match for an object array
 * argument.
 */
int JMethod_MatchPyArgs(JNIEnv* jenv, JPy_JType* declaringClass, JPy_JMethod* method, int argCount, PyObject* pyArgs, int *isVarArgArray)
{
    JPy_ParamDescriptor* paramDescriptor;
    PyObject* pyArg;
    int matchValueSum;
    int matchValue;
    int i;
    int i0;
    int iLast;
    *isVarArgArray = 0;

    if (method->isStatic) {
        if (method->isVarArgs) {
            if(argCount < method->paramCount - 1) {
                JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: var args argument count mismatch java=%d, python=%d (matchValue=0)\n", method->paramCount, argCount);
                // argument count mismatch
                return 0;
            }
            iLast = method->paramCount - 1;
        } else {
            if (method->paramCount != argCount) {
                JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: argument count mismatch (matchValue=0)\n");
                // argument count mismatch
                return 0;
            }
            if (method->paramCount == 0) {
                JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: no-argument static method (matchValue=100)\n");
                // There can't be any other static method overloads with no parameters
                return 100;
            }

            iLast = argCount;
        }
        matchValueSum = 0;
        i0 = 0;
    } else {
        PyObject* self;
        //printf("Non-Static! method->paramCount=%d, argCount=%d, isVarArg=%d\n", method->paramCount, argCount, method->isVarArgs);

        if (method->isVarArgs) {
            if (argCount < method->paramCount) {
                JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: var args argument count mismatch java=%d, python=%d (matchValue=0)\n", method->paramCount, argCount);
                // argument count mismatch
                return 0;
            }
            iLast = method->paramCount;
        }
        else if (method->paramCount != argCount - 1) {
            JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: argument count mismatch (matchValue=0)\n");
            // argument count mismatch
            return 0;
        } else {
            iLast = method->paramCount + 1;
        }

        self = PyTuple_GetItem(pyArgs, 0);
        if (self == Py_None) {
            JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: self argument is None (matchValue=0)\n");
            return 0;
        }
        if (!JObj_Check(self)) {
            JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: self argument is not a Java object (matchValue=0)\n");
            return 0;
        }

        i0 = 1;
        matchValueSum = JType_MatchPyArgAsJObject(jenv, declaringClass, self);
        if (matchValueSum == 0) {
           JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: self argument does not match required Java class (matchValue=0)\n");
           return 0;
        }
        if (method->paramCount == 0) {
            JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: no-argument non-static method (matchValue=%d)\n", matchValueSum);
            // There can't be any other method overloads with no parameters
            return matchValueSum;
        }
    }

    paramDescriptor = method->paramDescriptors;
    for (i = i0; i < iLast; i++) {
        pyArg = PyTuple_GetItem(pyArgs, i);
        matchValue = paramDescriptor->MatchPyArg(jenv, paramDescriptor, pyArg);

        JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: pyArgs[%d]: paramDescriptor->type->javaName='%s', matchValue=%d\n", i, paramDescriptor->type->javaName, matchValue);

        if (matchValue == 0) {
            //printf("JMethod_MatchPyArgs 6\n");
            // current pyArg does not match parameter type at all
            return 0;
        }

        matchValueSum += matchValue;
        paramDescriptor++;
    }
    if (method->isVarArgs) {
        int singleMatchValue = 0;

        JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: isVarArgs, argCount = %d, i=%d\n", argCount, i);

        if (argCount - i == 0) {
            matchValueSum += 10;
            JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: isVarArgs, argCount = %d, paramCount = %d, matchValueSum=%d\n", argCount, method->paramCount, matchValueSum);
        } else if (argCount - i == 1) {
            // if we have exactly one argument, which matches our array type, then we can use that as an array
            pyArg = PyTuple_GetItem(pyArgs, i);
            singleMatchValue = paramDescriptor->MatchPyArg(jenv, paramDescriptor, pyArg);
            JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: isVarArgs, argCount = %d, paramCount = %d, starting singleMatchValue=%d\n", argCount, method->paramCount, singleMatchValue);
        }

        JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: isVarArgs, argCount = %d, paramCount = %d, starting matchValue=%d\n", argCount, method->paramCount, matchValueSum);
        matchValue = paramDescriptor->MatchVarArgPyArg(jenv, paramDescriptor, pyArgs, i);
        JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JMethod_MatchPyArgs: isVarArgs, paramDescriptor->type->javaName='%s', matchValue=%d\n", paramDescriptor->type->javaName, matchValue);
        if (matchValue == 0 && singleMatchValue == 0) {
            return 0;
        }
        if (matchValue > singleMatchValue) {
            matchValueSum += matchValue;
        } else {
            matchValueSum += singleMatchValue;
            *isVarArgArray = 1;
        }
    }

    //printf("JMethod_MatchPyArgs 7\n");
    return matchValueSum;
}

#define JPy_SUPPORT_RETURN_PARAMETER 1

PyObject* JMethod_FromJObject(JNIEnv* jenv, JPy_JMethod* method, PyObject* pyArgs, jvalue* jArgs, int argOffset, JPy_JType* returnType, jobject jReturnValue)
{
    #ifdef JPy_SUPPORT_RETURN_PARAMETER
    if (method->returnDescriptor->paramIndex >= 0) {
        jint paramIndex = method->returnDescriptor->paramIndex;
        PyObject* pyReturnArg = PyTuple_GetItem(pyArgs, paramIndex + argOffset);
        jobject jArg = jArgs[paramIndex].l;
        //printf("JMethod_FromJObject: paramIndex=%d, jArg=%p, isNone=%d\n", paramIndex, jArg, pyReturnArg == Py_None);
        if ((JObj_Check(pyReturnArg) || PyObject_CheckBuffer(pyReturnArg))
            && (*jenv)->IsSameObject(jenv, jReturnValue, jArg)) {
             JPy_INCREF(pyReturnArg);
             return pyReturnArg;
        }
    }
    #endif
    return JPy_FromJObjectWithType(jenv, jReturnValue, returnType);
}

/**
 * Invoke a method. We have already ensured that the Python arguments and expected Java parameters match.
 */
PyObject* JMethod_InvokeMethod(JNIEnv* jenv, JPy_JMethod* method, PyObject* pyArgs, int isVarArgsArray)
{
    jvalue* jArgs;
    JPy_ArgDisposer* argDisposers;
    PyObject* returnValue;
    JPy_JType* declaringClass;
    JPy_JType* returnType;
    jclass classRef;

    //printf("JMethod_InvokeMethod 1: typeCode=%c\n", typeCode);
    if (JMethod_CreateJArgs(jenv, method, pyArgs, &jArgs, &argDisposers, isVarArgsArray) < 0) {
        return NULL;
    }

    //printf("JMethod_InvokeMethod 2: typeCode=%c\n", typeCode);

    returnType = method->returnDescriptor->type;
    returnValue = NULL;
    declaringClass = method->declaringClass;
    classRef = declaringClass->classRef;

    if (method->isStatic) {

        JPy_DIAG_PRINT(JPy_DIAG_F_EXEC, "JMethod_InvokeMethod: calling static Java method %s#%s\n", declaringClass->javaName, JPy_AS_UTF8(method->name));

        if (returnType == JPy_JVoid) {
            Py_BEGIN_ALLOW_THREADS;
            (*jenv)->CallStaticVoidMethodA(jenv, classRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JVOID();
        } else if (returnType == JPy_JBoolean) {
            jboolean v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallStaticBooleanMethodA(jenv, classRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JBOOLEAN(v);
        } else if (returnType == JPy_JChar) {
            jchar v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallStaticCharMethodA(jenv, classRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JCHAR(v);
        } else if (returnType == JPy_JByte) {
            jbyte v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallStaticByteMethodA(jenv, classRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JBYTE(v);
        } else if (returnType == JPy_JShort) {
            jshort v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallStaticShortMethodA(jenv, classRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JSHORT(v);
        } else if (returnType == JPy_JInt) {
            jint v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallStaticIntMethodA(jenv, classRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JINT(v);
        } else if (returnType == JPy_JLong) {
            jlong v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallStaticLongMethodA(jenv, classRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JLONG(v);
        } else if (returnType == JPy_JFloat) {
            jfloat v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallStaticFloatMethodA(jenv, classRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JFLOAT(v);
        } else if (returnType == JPy_JDouble) {
            jdouble v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallStaticDoubleMethodA(jenv, classRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JDOUBLE(v);
        } else if (returnType == JPy_JString) {
            jstring v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallStaticObjectMethodA(jenv, classRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FromJString(jenv, v);
            JPy_DELETE_LOCAL_REF(v);
        } else {
            jobject v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallStaticObjectMethodA(jenv, classRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JMethod_FromJObject(jenv, method, pyArgs, jArgs, 0, returnType, v);
            JPy_DELETE_LOCAL_REF(v);
        }

    } else {
        jobject objectRef;
        PyObject* self;

        JPy_DIAG_PRINT(JPy_DIAG_F_EXEC, "JMethod_InvokeMethod: calling Java method %s#%s\n", declaringClass->javaName, JPy_AS_UTF8(method->name));

        self = PyTuple_GetItem(pyArgs, 0);
        // Note it is already ensured that self is a JPy_JObj*
        objectRef = ((JPy_JObj*) self)->objectRef;

        if (returnType == JPy_JVoid) {
            Py_BEGIN_ALLOW_THREADS;
            (*jenv)->CallVoidMethodA(jenv, objectRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JVOID();
        } else if (returnType == JPy_JBoolean) {
            jboolean v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallBooleanMethodA(jenv, objectRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JBOOLEAN(v);
        } else if (returnType == JPy_JChar) {
            jchar v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallCharMethodA(jenv, objectRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JCHAR(v);
        } else if (returnType == JPy_JByte) {
            jbyte v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallByteMethodA(jenv, objectRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JBYTE(v);
        } else if (returnType == JPy_JShort) {
            jshort v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallShortMethodA(jenv, objectRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JSHORT(v);
        } else if (returnType == JPy_JInt) {
            jint v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallIntMethodA(jenv, objectRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JINT(v);
        } else if (returnType == JPy_JLong) {
            jlong v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallLongMethodA(jenv, objectRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JLONG(v);
        } else if (returnType == JPy_JFloat) {
            jfloat v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallFloatMethodA(jenv, objectRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JFLOAT(v);
        } else if (returnType == JPy_JDouble) {
            jdouble v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallDoubleMethodA(jenv, objectRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FROM_JDOUBLE(v);
        } else if (returnType == JPy_JString) {
            jstring v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallObjectMethodA(jenv, objectRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JPy_FromJString(jenv, v);
            JPy_DELETE_LOCAL_REF(v);
        } else {
            jobject v;
            Py_BEGIN_ALLOW_THREADS;
            v = (*jenv)->CallObjectMethodA(jenv, objectRef, method->mid, jArgs);
            Py_END_ALLOW_THREADS;
            JPy_ON_JAVA_EXCEPTION_GOTO(error);
            returnValue = JMethod_FromJObject(jenv, method, pyArgs, jArgs, 1, returnType, v);
            JPy_DELETE_LOCAL_REF(v);
        }
    }

error:
    if (jArgs != NULL) {
        JMethod_DisposeJArgs(jenv, method->paramCount, jArgs, argDisposers);
    }

    return returnValue;
}

int JMethod_CreateJArgs(JNIEnv* jenv, JPy_JMethod* method, PyObject* pyArgs, jvalue** argValuesRet, JPy_ArgDisposer** argDisposersRet, int isVarArgsArray)
{
    JPy_ParamDescriptor* paramDescriptor;
    Py_ssize_t i, i0, iLast;
    Py_ssize_t argCount;
    PyObject* pyArg;
    jvalue* jValue;
    jvalue* jValues;
    JPy_ArgDisposer* argDisposer;
    JPy_ArgDisposer* argDisposers;

    if (method->paramCount == 0) {
        *argValuesRet = NULL;
        *argDisposersRet = NULL;
        return 0;
    }

    argCount = PyTuple_Size(pyArgs);

    if (method->isVarArgs) {
        // need to know if we expect a self parameter
        i0 = method->isStatic ? 0 : 1;
        iLast = method->isStatic ? method->paramCount - 1 : method->paramCount;
    } else {
        i0 = argCount - method->paramCount;
        if (!(i0 == 0 || i0 == 1)) {
            PyErr_SetString(PyExc_RuntimeError, "internal error");
            return -1;
        }
        iLast = argCount;
    }

    jValues = PyMem_New(jvalue, method->paramCount);
    if (jValues == NULL) {
        PyErr_NoMemory();
        return -1;
    }

    argDisposers = PyMem_New(JPy_ArgDisposer, method->paramCount);
    if (argDisposers == NULL) {
        PyMem_Del(jValues);
        PyErr_NoMemory();
        return -1;
    }

    paramDescriptor = method->paramDescriptors;
    jValue = jValues;
    argDisposer = argDisposers;
    for (i = i0; i < iLast; i++) {
        pyArg = PyTuple_GetItem(pyArgs, i);
        jValue->l = 0;
        argDisposer->data = NULL;
        argDisposer->DisposeArg = NULL;
        if (paramDescriptor->ConvertPyArg(jenv, paramDescriptor, pyArg, jValue, argDisposer) < 0) {
            PyMem_Del(jValues);
            PyMem_Del(argDisposers);
            return -1;
        }
        paramDescriptor++;
        jValue++;
        argDisposer++;
    }
    if (method->isVarArgs) {
        if (isVarArgsArray) {
            pyArg = PyTuple_GetItem(pyArgs, i);
            jValue->l = 0;
            argDisposer->data = NULL;
            argDisposer->DisposeArg = NULL;
            if (paramDescriptor->ConvertPyArg(jenv, paramDescriptor, pyArg, jValue, argDisposer) < 0) {
                PyMem_Del(jValues);
                PyMem_Del(argDisposers);
                return -1;
            }
        } else {
            jValue->l = 0;
            argDisposer->data = NULL;
            argDisposer->DisposeArg = NULL;
            if (paramDescriptor->ConvertVarArgPyArg(jenv, paramDescriptor, pyArgs, i, jValue, argDisposer) < 0) {
                PyMem_Del(jValues);
                PyMem_Del(argDisposers);
                return -1;
            }
        }
    }

    *argValuesRet = jValues;
    *argDisposersRet = argDisposers;
    return 0;
}

void JMethod_DisposeJArgs(JNIEnv* jenv, int paramCount, jvalue* jArgs, JPy_ArgDisposer* argDisposers)
{
    jvalue* jArg;
    JPy_ArgDisposer* argDisposer;
    int index;

    jArg = jArgs;
    argDisposer = argDisposers;

    for (index = 0; index < paramCount; index++) {
        if (argDisposer->DisposeArg != NULL) {
            argDisposer->DisposeArg(jenv, jArg, argDisposer->data);
        }
        jArg++;
        argDisposer++;
    }

    PyMem_Del(jArgs);
    PyMem_Del(argDisposers);
}


PyObject* JMethod_repr(JPy_JMethod* self)
{
    const char* name = JPy_AS_UTF8(self->name);
    return JPy_FROM_FORMAT("%s(name='%s', param_count=%d, is_static=%d, mid=%p)",
                           ((PyObject*)self)->ob_type->tp_name,
                           name,
                           self->paramCount,
                           self->isStatic,
                           self->mid);
}

PyObject* JMethod_str(JPy_JMethod* self)
{
    JPy_INCREF(self->name);
    return self->name;
}


static PyMemberDef JMethod_members[] =
{
    {"name",        T_OBJECT_EX, offsetof(JPy_JMethod, name),       READONLY, "Method name"},
    {"param_count", T_INT,       offsetof(JPy_JMethod, paramCount), READONLY, "Number of method parameters"},
    {"is_static",   T_BOOL,      offsetof(JPy_JMethod, isStatic),   READONLY, "Tests if this is a static method"},
    {NULL}  /* Sentinel */
};

#define JMethod_CHECK_PARAMETER_INDEX(self, index) \
    if (index < 0 || index >= self->paramCount) { \
        PyErr_SetString(PyExc_IndexError, "invalid parameter index"); \
        return NULL; \
    }


PyObject* JMethod_get_param_type(JPy_JMethod* self, PyObject* args)
{
    PyObject* type;
    int index;
    if (!PyArg_ParseTuple(args, "i:get_param_type", &index)) {
        return NULL;
    }
    JMethod_CHECK_PARAMETER_INDEX(self, index);
    type = (PyObject*) self->paramDescriptors[index].type;
    JPy_INCREF(type);
    return type;
}

PyObject* JMethod_is_param_mutable(JPy_JMethod* self, PyObject* args)
{
    int index;
    int value;
    if (!PyArg_ParseTuple(args, "i:is_param_mutable", &index)) {
        return NULL;
    }
    JMethod_CHECK_PARAMETER_INDEX(self, index);
    value = self->paramDescriptors[index].isMutable;
    return PyBool_FromLong(value);
}

PyObject* JMethod_set_param_mutable(JPy_JMethod* self, PyObject* args)
{
    int index;
    int value;
#if defined(JPY_COMPAT_33P)
    if (!PyArg_ParseTuple(args, "ip:set_param_mutable", &index, &value)) {
#elif defined(JPY_COMPAT_27)
    if (!PyArg_ParseTuple(args, "ii:set_param_mutable", &index, &value)) {
#else
#error JPY_VERSION_ERROR
#endif
        return NULL;
    }
    JMethod_CHECK_PARAMETER_INDEX(self, index);
    self->paramDescriptors[index].isMutable = value;
    return Py_BuildValue("");
}

PyObject* JMethod_is_param_output(JPy_JMethod* self, PyObject* args)
{
    int index = 0;
    int value = 0;
    if (!PyArg_ParseTuple(args, "i:is_param_output", &index)) {
        return NULL;
    }
    JMethod_CHECK_PARAMETER_INDEX(self, index);
    value = self->paramDescriptors[index].isOutput;
    return PyBool_FromLong(value);
}

PyObject* JMethod_set_param_output(JPy_JMethod* self, PyObject* args)
{
    int index = 0;
    int value = 0;
#if defined(JPY_COMPAT_33P)
    if (!PyArg_ParseTuple(args, "ip:set_param_output", &index, &value)) {
#elif defined(JPY_COMPAT_27)
    if (!PyArg_ParseTuple(args, "ii:set_param_output", &index, &value)) {
#else
#error JPY_VERSION_ERROR
#endif
        return NULL;
    }
    JMethod_CHECK_PARAMETER_INDEX(self, index);
    self->paramDescriptors[index].isOutput = value;
    return Py_BuildValue("");
}

PyObject* JMethod_is_param_return(JPy_JMethod* self, PyObject* args)
{
    int index = 0;
    int value = 0;
    if (!PyArg_ParseTuple(args, "i:is_param_return", &index)) {
        return NULL;
    }
    JMethod_CHECK_PARAMETER_INDEX(self, index);
    value = self->paramDescriptors[index].isReturn;
    return PyBool_FromLong(value);
}

PyObject* JMethod_set_param_return(JPy_JMethod* self, PyObject* args)
{
    int index = 0;
    int value = 0;
#if defined(JPY_COMPAT_33P)
    if (!PyArg_ParseTuple(args, "ip:set_param_return", &index, &value)) {
#elif defined(JPY_COMPAT_27)
    if (!PyArg_ParseTuple(args, "ii:set_param_return", &index, &value)) {
#else
#error JPY_VERSION_ERROR
#endif
        return NULL;
    }
    JMethod_CHECK_PARAMETER_INDEX(self, index);
    self->paramDescriptors[index].isReturn = value;
    if (value) {
        self->returnDescriptor->paramIndex = index;
    }
    return Py_BuildValue("");
}


static PyMethodDef JMethod_methods[] =
{
    {"get_param_type",    (PyCFunction) JMethod_get_param_type,    METH_VARARGS, "Gets the type of the parameter given by index"},
    {"is_param_mutable",  (PyCFunction) JMethod_is_param_mutable,  METH_VARARGS, "Tests if the method parameter given by index is mutable"},
    {"is_param_output",   (PyCFunction) JMethod_is_param_output,   METH_VARARGS, "Tests if the method parameter given by index is a mere output value (and not read from)"},
    {"is_param_return",   (PyCFunction) JMethod_is_param_return,   METH_VARARGS, "Tests if the method parameter given by index is the return value"},
    {"set_param_mutable", (PyCFunction) JMethod_set_param_mutable, METH_VARARGS, "Sets whether the method parameter given by index is mutable"},
    {"set_param_output",  (PyCFunction) JMethod_set_param_output,  METH_VARARGS, "Sets whether the method parameter given by index is a mere output value (and not read from)"},
    {"set_param_return",  (PyCFunction) JMethod_set_param_return,  METH_VARARGS, "Sets whether the method parameter given by index is the return value"},
    {NULL}  /* Sentinel */
};

/**
 * Implements the BeamPy_JObjectType class singleton.
 */
PyTypeObject JMethod_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "jpy.JMethod",                /* tp_name */
    sizeof (JPy_JMethod),         /* tp_basicsize */
    0,                            /* tp_itemsize */
    (destructor)JMethod_dealloc,  /* tp_dealloc */
    NULL,                         /* tp_print */
    NULL,                         /* tp_getattr */
    NULL,                         /* tp_setattr */
    NULL,                         /* tp_reserved */
    (reprfunc)JMethod_repr,       /* tp_repr */
    NULL,                         /* tp_as_number */
    NULL,                         /* tp_as_sequence */
    NULL,                         /* tp_as_mapping */
    NULL,                         /* tp_hash  */
    NULL,                         /* tp_call */
    (reprfunc)JMethod_str,        /* tp_str */
    NULL,                         /* tp_getattro */
    NULL,                         /* tp_setattro */
    NULL,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,           /* tp_flags */
    "Java Method Wrapper",        /* tp_doc */
    NULL,                         /* tp_traverse */
    NULL,                         /* tp_clear */
    NULL,                         /* tp_richcompare */
    0,                            /* tp_weaklistoffset */
    NULL,                         /* tp_iter */
    NULL,                         /* tp_iternext */
    JMethod_methods,              /* tp_methods */
    JMethod_members,              /* tp_members */
    NULL,                         /* tp_getset */
    NULL,                         /* tp_base */
    NULL,                         /* tp_dict */
    NULL,                         /* tp_descr_get */
    NULL,                         /* tp_descr_set */
    0,                            /* tp_dictoffset */
    NULL,                         /* tp_init */
    NULL,                         /* tp_alloc */
    NULL,                         /* tp_new */
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  JOverloadedMethod

typedef struct JPy_MethodFindResult
{
    JPy_JMethod* method;
    int matchValue;
    int matchCount;
    int isVarArgsArray;
}
JPy_MethodFindResult;

JPy_JMethod* JOverloadedMethod_FindMethod0(JNIEnv* jenv, JPy_JOverloadedMethod* overloadedMethod, PyObject* pyArgs, JPy_MethodFindResult* result)
{
    Py_ssize_t overloadCount;
    Py_ssize_t argCount;
    int matchCount;
    int matchValue;
    int matchValueMax;
    JPy_JMethod* currMethod;
    JPy_JMethod* bestMethod;
    int i;
    int currentIsVarArgsArray;
    int bestIsVarArgsArray;

    result->method = NULL;
    result->matchValue = 0;
    result->matchCount = 0;

    overloadCount = PyList_Size(overloadedMethod->methodList);
    if (overloadCount <= 0) {
        PyErr_SetString(PyExc_RuntimeError, "internal error: invalid overloadedMethod->methodList");
        return NULL;
    }

    argCount = PyTuple_Size(pyArgs);
    matchCount = 0;
    matchValueMax = -1;
    bestMethod = NULL;
    bestIsVarArgsArray = 0;

    JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JOverloadedMethod_FindMethod0: method '%s#%s': overloadCount=%d, argCount=%d\n",
                              overloadedMethod->declaringClass->javaName, JPy_AS_UTF8(overloadedMethod->name), overloadCount, argCount);

    for (i = 0; i < overloadCount; i++) {
        currMethod = (JPy_JMethod*) PyList_GetItem(overloadedMethod->methodList, i);

        if (currMethod->isVarArgs && matchValueMax > 0 && !bestMethod->isVarArgs) {
            // we should not process varargs if we have already found a suitable fixed arity method
            break;
        }

        matchValue = JMethod_MatchPyArgs(jenv, overloadedMethod->declaringClass, currMethod, argCount, pyArgs, &currentIsVarArgsArray);

        JPy_DIAG_PRINT(JPy_DIAG_F_METH, "JOverloadedMethod_FindMethod0: methodList[%d]: paramCount=%d, matchValue=%d, isVarArgs=%d\n", i,
                                  currMethod->paramCount, matchValue, currMethod->isVarArgs);

        if (matchValue > 0) {
            if (matchValue > matchValueMax) {
                matchValueMax = matchValue;
                bestMethod = currMethod;
                matchCount = 1;
                bestIsVarArgsArray = currentIsVarArgsArray;
            } else if (matchValue == matchValueMax) {
                matchCount++;
            }
            if (!currMethod->isVarArgs && (matchValue >= 100 * argCount)) {
                // we can't get any better (if so, we have an internal problem)
                break;
            }
        }
    }

    if (bestMethod == NULL) {
        matchValueMax = 0;
        matchCount = 0;
        bestIsVarArgsArray = 0;
    }

    result->method = bestMethod;
    result->matchValue = matchValueMax;
    result->matchCount = matchCount;
    result->isVarArgsArray = bestIsVarArgsArray;

    return bestMethod;
}

JPy_JMethod* JOverloadedMethod_FindMethod(JNIEnv* jenv, JPy_JOverloadedMethod* overloadedMethod, PyObject* pyArgs, jboolean visitSuperClass, int *isVarArgsArray)
{
    JPy_JOverloadedMethod* currentOM;
    JPy_MethodFindResult result;
    JPy_MethodFindResult bestResult;
    JPy_JType* superClass;
    PyObject* superOM;
    int argCount;

    argCount = PyTuple_Size(pyArgs);

    if ((JPy_DiagFlags & JPy_DIAG_F_METH) != 0) {
        int i;
        printf("JOverloadedMethod_FindMethod: argCount=%d, visitSuperClass=%d\n", argCount, visitSuperClass);
        for (i = 0; i < argCount; i++) {
            PyObject* pyArg = PyTuple_GetItem(pyArgs, i);
            printf("\tPy_TYPE(pyArgs[%d])->tp_name = %s\n", i, Py_TYPE(pyArg)->tp_name);
        }
    }

    bestResult.method = NULL;
    bestResult.matchValue = 0;
    bestResult.matchCount = 0;
    bestResult.isVarArgsArray = 0;

    currentOM = overloadedMethod;
    while (1) {
        if (JOverloadedMethod_FindMethod0(jenv, currentOM, pyArgs, &result) < 0) {
            // oops, error
            return NULL;
        }
        if (result.method != NULL) {
            // in the case where we have a match count that is perfect, but more than one match; the super class might
            // have a better match count, because varargs can have fewer arguments than actual parameters.
            if (result.matchValue >= 100 * argCount && result.matchCount == 1) {
                // We can't get any better.
                *isVarArgsArray = result.isVarArgsArray;
                return result.method;
            } else if (result.matchValue > 0 && result.matchValue > bestResult.matchValue) {
                // We may have better matching methods overloads in the super class (if any)
                bestResult = result;
            }
        }

        if (visitSuperClass) {
            superClass = currentOM->declaringClass->superType;
            if (superClass != NULL) {
                superOM = JType_GetOverloadedMethod(jenv, superClass, currentOM->name, JNI_TRUE);
            } else {
                superOM = Py_None;
            }
        } else {
            superOM = Py_None;
        }

        if (superOM == NULL) {
            // oops, error
            return NULL;
        } else if (superOM == Py_None) {
            // no overloaded methods found in super class, so return best result found so far
            if (bestResult.method == NULL) {
                PyErr_SetString(PyExc_RuntimeError, "no matching Java method overloads found");
                return NULL;
            } else if (bestResult.matchCount > 1) {
                PyErr_SetString(PyExc_RuntimeError, "ambiguous Java method call, too many matching method overloads found");
                return NULL;
            } else {
                *isVarArgsArray = bestResult.isVarArgsArray;
                return bestResult.method;
            }
        } else {
            // Continue trying with overloads from super type
            currentOM = (JPy_JOverloadedMethod*) superOM;
        }
    }

    // Should never come here
    PyErr_SetString(PyExc_RuntimeError, "internal error");
    return NULL;
}

JPy_JOverloadedMethod* JOverloadedMethod_New(JPy_JType* declaringClass, PyObject* name, JPy_JMethod* method)
{
    PyTypeObject* methodType = &JOverloadedMethod_Type;
    JPy_JOverloadedMethod* overloadedMethod;

    overloadedMethod = (JPy_JOverloadedMethod*) methodType->tp_alloc(methodType, 0);
    overloadedMethod->declaringClass = declaringClass;
    overloadedMethod->name = name;
    overloadedMethod->methodList = PyList_New(0);

    JPy_INCREF((PyObject*) overloadedMethod->declaringClass);
    JPy_INCREF((PyObject*) overloadedMethod->name);
    JPy_INCREF((PyObject*) overloadedMethod);

    JOverloadedMethod_AddMethod(overloadedMethod, method);

    return overloadedMethod;
}

int JOverloadedMethod_AddMethod(JPy_JOverloadedMethod* overloadedMethod, JPy_JMethod* method)
{
    Py_ssize_t destinationIndex = -1;

    if (!method->isVarArgs) {
        Py_ssize_t ii;
        // we need to insert this before the first varargs method
        Py_ssize_t size = PyList_Size(overloadedMethod->methodList);
        for (ii = 0; ii < size; ii++) {
            PyObject *check = PyList_GetItem(overloadedMethod->methodList, ii);
            if (((JPy_JMethod *) check)->isVarArgs) {
                // this is the first varargs method, so we should go before it
                destinationIndex = ii;
                break;
            }
        }
    }

    if (destinationIndex >= 0) {
        return PyList_Insert(overloadedMethod->methodList, destinationIndex, (PyObject *) method);
    } else {
        return PyList_Append(overloadedMethod->methodList, (PyObject *) method);
    }
}

/**
 * The 'JOverloadedMethod' type's tp_dealloc slot.
 */
void JOverloadedMethod_dealloc(JPy_JOverloadedMethod* self)
{
    JPy_DECREF((PyObject*) self->declaringClass);
    JPy_DECREF((PyObject*) self->name);
    JPy_DECREF((PyObject*) self->methodList);
    Py_TYPE(self)->tp_free((PyObject*) self);
}

PyObject* JOverloadedMethod_call_internal(JNIEnv* jenv, JPy_JOverloadedMethod* self, PyObject *args, PyObject *kw)
{
    JPy_JMethod* method;
    int isVarArgsArray;

    method = JOverloadedMethod_FindMethod(jenv, self, args, JNI_TRUE, &isVarArgsArray);
    if (method == NULL) {
        return NULL;
    }

    return JMethod_InvokeMethod(jenv, method, args, isVarArgsArray);
}

/**
 * The 'JOverloadedMethod' type's tp_call slot. Makes instances of the 'JOverloadedMethod' type callable.
 */
PyObject* JOverloadedMethod_call(JPy_JOverloadedMethod* self, PyObject *args, PyObject *kw)
{
    JPy_FRAME(PyObject*, NULL, JOverloadedMethod_call_internal(jenv, self, args, kw), 16)
}

/**
 * The 'JOverloadedMethod' type's tp_repr slot.
 */
PyObject* JOverloadedMethod_repr(JPy_JOverloadedMethod* self)
{
    const char* className = self->declaringClass->javaName;
    const char* name = JPy_AS_UTF8(self->name);
    int methodCount = PyList_Size(self->methodList);
    return JPy_FROM_FORMAT("%s(class='%s', name='%s', methodCount=%d)",
                           ((PyObject*)self)->ob_type->tp_name,
                           className,
                           name,
                           methodCount);
}

static PyMemberDef JOverloadedMethod_members[] =
{
    {"decl_class",   T_OBJECT_EX, offsetof(JPy_JOverloadedMethod, declaringClass), READONLY, "Declaring Java class"},
    {"name",         T_OBJECT_EX, offsetof(JPy_JOverloadedMethod, name),           READONLY, "Overloaded method name"},
    {"methods",      T_OBJECT_EX, offsetof(JPy_JOverloadedMethod, methodList),     READONLY, "List of methods"},
    {NULL}  /* Sentinel */
};

/**
 * The 'JOverloadedMethod' type's tp_str slot.
 */
PyObject* JOverloadedMethod_str(JPy_JOverloadedMethod* self)
{
    JPy_INCREF(self->name);
    return self->name;
}

PyTypeObject JOverloadedMethod_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "jpy.JOverloadedMethod",                /* tp_name */
    sizeof (JPy_JOverloadedMethod),         /* tp_basicsize */
    0,                            /* tp_itemsize */
    (destructor)JOverloadedMethod_dealloc,  /* tp_dealloc */
    NULL,                         /* tp_print */
    NULL,                         /* tp_getattr */
    NULL,                         /* tp_setattr */
    NULL,                         /* tp_reserved */
    (reprfunc)JOverloadedMethod_repr,       /* tp_repr */
    NULL,                         /* tp_as_number */
    NULL,                         /* tp_as_sequence */
    NULL,                         /* tp_as_mapping */
    NULL,                         /* tp_hash  */
    (ternaryfunc)JOverloadedMethod_call,    /* tp_call */
    (reprfunc)JOverloadedMethod_str,        /* tp_str */
    NULL,                         /* tp_getattro */
    NULL,                         /* tp_setattro */
    NULL,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,           /* tp_flags */
    "Java Overloaded Method",     /* tp_doc */
    NULL,                         /* tp_traverse */
    NULL,                         /* tp_clear */
    NULL,                         /* tp_richcompare */
    0,                            /* tp_weaklistoffset */
    NULL,                         /* tp_iter */
    NULL,                         /* tp_iternext */
    NULL,                         /* tp_methods */
    JOverloadedMethod_members,    /* tp_members */
    NULL,                         /* tp_getset */
    NULL,                         /* tp_base */
    NULL,                         /* tp_dict */
    NULL,                         /* tp_descr_get */
    NULL,                         /* tp_descr_set */
    0,                            /* tp_dictoffset */
    NULL,                         /* tp_init */
    NULL,                         /* tp_alloc */
    NULL,                         /* tp_new */
};
