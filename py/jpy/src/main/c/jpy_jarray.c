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
#include "jpy_jarray.h"


#define PRINT_FLAG(F) printf("JArray_GetBufferProc: %s = %d\n", #F, (flags & F) != 0);
#define PRINT_MEMB(F, M) printf("JArray_GetBufferProc: %s = " ## F ## "\n", #M, M);

//#define JPy_USE_GET_PRIMITIVE_ARRAY_CRITICAL 1


/*
 * Implements the getbuffer() method of the buffer protocol for JPy_JArray objects.
 * Regarding the format parameter, refer to the Python 'struct' module documentation:
 * http://docs.python.org/2/library/struct.html#module-struct
 */
int JArray_GetBufferProc(JPy_JArray* self, Py_buffer* view, int flags, char javaType, jint itemSize, const char* format)
{
    JNIEnv* jenv;
    jint itemCount;
    jboolean isCopy;
    void* buf;

    JPy_GET_JNI_ENV_OR_RETURN(jenv, -1)

    /*
    printf("JArray_GetBufferProc:\n");
    PRINT_FLAG(PyBUF_ANY_CONTIGUOUS);
    PRINT_FLAG(PyBUF_CONTIG);
    PRINT_FLAG(PyBUF_CONTIG_RO);
    PRINT_FLAG(PyBUF_C_CONTIGUOUS);
    PRINT_FLAG(PyBUF_FORMAT);
    PRINT_FLAG(PyBUF_FULL);
    PRINT_FLAG(PyBUF_FULL_RO);
    PRINT_FLAG(PyBUF_F_CONTIGUOUS);
    PRINT_FLAG(PyBUF_INDIRECT);
    PRINT_FLAG(PyBUF_ND);
    PRINT_FLAG(PyBUF_READ);
    PRINT_FLAG(PyBUF_RECORDS);
    PRINT_FLAG(PyBUF_RECORDS_RO);
    PRINT_FLAG(PyBUF_SIMPLE);
    PRINT_FLAG(PyBUF_STRIDED);
    PRINT_FLAG(PyBUF_STRIDED_RO);
    PRINT_FLAG(PyBUF_STRIDES);
    PRINT_FLAG(PyBUF_WRITE);
    PRINT_FLAG(PyBUF_WRITEABLE);
    */

    itemCount = (*jenv)->GetArrayLength(jenv, self->objectRef);

    // According to Python documentation,
    // buffer allocation shall be done in the 5 following steps;

    // Step 1/5
#ifdef JPy_USE_GET_PRIMITIVE_ARRAY_CRITICAL
    buf = (*jenv)->GetPrimitiveArrayCritical(jenv, self->objectRef, &isCopy);
#else
    if (self->buf == NULL) {
        if (javaType == 'Z') {
            buf = (*jenv)->GetBooleanArrayElements(jenv, self->objectRef, &isCopy);
        } else if (javaType == 'C') {
            buf = (*jenv)->GetCharArrayElements(jenv, self->objectRef, &isCopy);
        } else if (javaType == 'B') {
            buf = (*jenv)->GetByteArrayElements(jenv, self->objectRef, &isCopy);
        } else if (javaType == 'S') {
            buf = (*jenv)->GetShortArrayElements(jenv, self->objectRef, &isCopy);
        } else if (javaType == 'I') {
            buf = (*jenv)->GetIntArrayElements(jenv, self->objectRef, &isCopy);
        } else if (javaType == 'J') {
            buf = (*jenv)->GetLongArrayElements(jenv, self->objectRef, &isCopy);
        } else if (javaType == 'F') {
            buf = (*jenv)->GetFloatArrayElements(jenv, self->objectRef, &isCopy);
        } else if (javaType == 'D') {
            buf = (*jenv)->GetDoubleArrayElements(jenv, self->objectRef, &isCopy);
        } else {
            PyErr_Format(PyExc_RuntimeError, "internal error: illegal Java array type '%c'", javaType);
            return -1;
        }
        self->buf = buf;
        self->javaType = javaType;
        self->isCopy = isCopy;
        self->bufReadonly = (flags & (PyBUF_WRITE | PyBUF_WRITEABLE)) == 0;
    } else {
        buf = self->buf;
    }
#endif
    if (buf == NULL) {
        PyErr_NoMemory();
        return -1;
    }

    JPy_DIAG_PRINT(JPy_DIAG_F_MEM, "JArray_GetBufferProc: buf=%p, bufferExportCount=%d, type='%s', format='%s', itemSize=%d, itemCount=%d, isCopy=%d\n", buf, self->bufferExportCount, Py_TYPE(self)->tp_name, format, itemSize, itemCount, isCopy);

    // Step 2/5
    view->buf = buf;
    view->len = itemCount * itemSize;
    view->itemsize = itemSize;
    view->readonly = (flags & (PyBUF_WRITE | PyBUF_WRITEABLE)) == 0;
    self->bufReadonly &= view->readonly;
    view->ndim = 1;
    view->shape = PyMem_New(Py_ssize_t, 1);
    *view->shape = itemCount;
    view->strides = PyMem_New(Py_ssize_t, 1);
    *view->strides = itemSize;
    view->suboffsets = NULL;
    if ((flags & PyBUF_FORMAT) != 0) {
        view->format = (char*) format;
    } else {
        view->format = (char*) "B";
    }

    /*
    PRINT_MEMB("%d", view->len);
    PRINT_MEMB("%d", view->ndim);
    PRINT_MEMB("%s", view->format);
    PRINT_MEMB("%d", view->itemsize);
    PRINT_MEMB("%d", view->readonly);
    PRINT_MEMB("%d", view->shape[0]);
    PRINT_MEMB("%d", view->strides[0]);
    */

    // Step 3/5
    self->bufferExportCount++;

    // Step 4/5
    view->obj = (PyObject*) self;
    JPy_INCREF(view->obj);

    // Step 5/5
    return 0;
}

int JArray_getbufferproc_boolean(JPy_JArray* self, Py_buffer* view, int flags)
{
    return JArray_GetBufferProc(self, view, flags, 'Z', 1, "B");
}

int JArray_getbufferproc_char(JPy_JArray* self, Py_buffer* view, int flags)
{
    return JArray_GetBufferProc(self, view, flags, 'C', 2, "H");
}

int JArray_getbufferproc_byte(JPy_JArray* self, Py_buffer* view, int flags)
{
    return JArray_GetBufferProc(self, view, flags, 'B', 1, "b");
}

int JArray_getbufferproc_short(JPy_JArray* self, Py_buffer* view, int flags)
{
    return JArray_GetBufferProc(self, view, flags, 'S', 2, "h");
}

int JArray_getbufferproc_int(JPy_JArray* self, Py_buffer* view, int flags)
{
    return JArray_GetBufferProc(self, view, flags, 'I', 4, "i");
}

int JArray_getbufferproc_long(JPy_JArray* self, Py_buffer* view, int flags)
{
    return JArray_GetBufferProc(self, view, flags, 'J', 8, "q");
}

int JArray_getbufferproc_float(JPy_JArray* self, Py_buffer* view, int flags)
{
    return JArray_GetBufferProc(self, view, flags, 'F', 4, "f");
}

int JArray_getbufferproc_double(JPy_JArray* self, Py_buffer* view, int flags)
{
    return JArray_GetBufferProc(self, view, flags, 'D', 8, "d");
}


/*
 *
 */
void JArray_ReleaseJavaArrayElements(JPy_JArray* self, char javaType)
{
    JNIEnv* jenv = JPy_GetJNIEnv();
    if (!self->buf) 
        return;
    
    if (jenv != NULL) {
    #ifdef JPy_USE_GET_PRIMITIVE_ARRAY_CRITICAL
        (*jenv)->ReleasePrimitiveArrayCritical(jenv, self->objectRef, view->buf, view->readonly ? JNI_ABORT : 0);
    #else
        if (javaType == 'Z') {
            (*jenv)->ReleaseBooleanArrayElements(jenv, self->objectRef, (jboolean*) self->buf, self->bufReadonly ? JNI_ABORT : 0);
        } else if (javaType == 'C') {
            (*jenv)->ReleaseCharArrayElements(jenv, self->objectRef, (jchar*) self->buf, self->bufReadonly ? JNI_ABORT : 0);
        } else if (javaType == 'B') {
            (*jenv)->ReleaseByteArrayElements(jenv, self->objectRef, (jbyte*) self->buf, self->bufReadonly ? JNI_ABORT : 0);
        } else if (javaType == 'S') {
            (*jenv)->ReleaseShortArrayElements(jenv, self->objectRef, (jshort*) self->buf, self->bufReadonly ? JNI_ABORT : 0);
        } else if (javaType == 'I') {
            (*jenv)->ReleaseIntArrayElements(jenv, self->objectRef, (jint*) self->buf, self->bufReadonly ? JNI_ABORT : 0);
        } else if (javaType == 'J') {
            (*jenv)->ReleaseLongArrayElements(jenv, self->objectRef, (jlong*) self->buf, self->bufReadonly ? JNI_ABORT : 0);
        } else if (javaType == 'F') {
            (*jenv)->ReleaseFloatArrayElements(jenv, self->objectRef, (jfloat*) self->buf, self->bufReadonly ? JNI_ABORT : 0);
        } else if (javaType == 'D') {
            (*jenv)->ReleaseDoubleArrayElements(jenv, self->objectRef, (jdouble*) self->buf, self->bufReadonly ? JNI_ABORT : 0);
        }
    #endif
    }

} 

/*
 * Implements the releasebuffer() method the buffer protocol for JPy_JArray objects
 */
void JArray_ReleaseBufferProc(JPy_JArray* self, Py_buffer* view, char javaType)
{

    // Step 1
    self->bufferExportCount--;

    JPy_DIAG_PRINT(JPy_DIAG_F_MEM, "JArray_ReleaseBufferProc: buf=%p, bufferExportCount=%d\n", view->buf, self->bufferExportCount);

    // Step 2
    // defer the release of Java buffer to dealloc

    // Note: this function is *not* responsible for PyDECREF of view->obj
    // https://docs.python.org/3/c-api/typeobj.html#c.PyBufferProcs.bf_releasebuffer
}

// todo: py27: fix all releasebufferproc() functions which have different parameter types in 2.7

void JArray_releasebufferproc_boolean(JPy_JArray* self, Py_buffer* view)
{
    JArray_ReleaseBufferProc(self, view, 'Z');
}

void JArray_releasebufferproc_char(JPy_JArray* self, Py_buffer* view)
{
    JArray_ReleaseBufferProc(self, view, 'C');
}

void JArray_releasebufferproc_byte(JPy_JArray* self, Py_buffer* view)
{
    JArray_ReleaseBufferProc(self, view, 'B');
}

void JArray_releasebufferproc_short(JPy_JArray* self, Py_buffer* view)
{
    JArray_ReleaseBufferProc(self, view, 'S');
}

void JArray_releasebufferproc_int(JPy_JArray* self, Py_buffer* view)
{
    JArray_ReleaseBufferProc(self, view, 'I');
}

void JArray_releasebufferproc_long(JPy_JArray* self, Py_buffer* view)
{
    JArray_ReleaseBufferProc(self, view, 'J');
}

void JArray_releasebufferproc_float(JPy_JArray* self, Py_buffer* view)
{
    JArray_ReleaseBufferProc(self, view, 'F');
}

void JArray_releasebufferproc_double(JPy_JArray* self, Py_buffer* view)
{
    JArray_ReleaseBufferProc(self, view, 'D');
}

// PyBufferProcs 3.x
//
// struct PyBufferProcs {
//    getbufferproc bf_getbuffer;
//    releasebufferproc bf_releasebuffer;
// }
//
// PyBufferProcs 2.6 and 2.7 (3.x backport)
//
// struct PyBufferProcs {
//    readbufferproc bf_getreadbuffer;
//    writebufferproc bf_getwritebuffer;
//    segcountproc bf_getsegcount;
//    charbufferproc bf_getcharbuffer;
//    getbufferproc bf_getbuffer;
//    releasebufferproc bf_releasebuffer;
// }
//
// PyBufferProcs <= 2.5 (not supported by jpy)
//
// struct PyBufferProcs {
//    readbufferproc bf_getreadbuffer;
//    writebufferproc bf_getwritebuffer;
//    segcountproc bf_getsegcount;
//    charbufferproc bf_getcharbuffer;
// }

#if defined(JPY_COMPAT_33P)

#define JPY_PY27_OLD_BUFFER_PROCS

#elif defined(JPY_COMPAT_27)

#define JPY_PY27_OLD_BUFFER_PROCS \
    (readbufferproc) NULL, \
    (writebufferproc) NULL, \
    (segcountproc) NULL, \
    (charbufferproc) NULL,

#else

#error JPY_VERSION_ERROR

#endif


PyBufferProcs JArray_as_buffer_boolean = {
    JPY_PY27_OLD_BUFFER_PROCS
    (getbufferproc) JArray_getbufferproc_boolean,
    (releasebufferproc) JArray_releasebufferproc_boolean
};

PyBufferProcs JArray_as_buffer_char = {
    JPY_PY27_OLD_BUFFER_PROCS
    (getbufferproc) JArray_getbufferproc_char,
    (releasebufferproc) JArray_releasebufferproc_char
};

PyBufferProcs JArray_as_buffer_byte = {
    JPY_PY27_OLD_BUFFER_PROCS
    (getbufferproc) JArray_getbufferproc_byte,
    (releasebufferproc) JArray_releasebufferproc_byte
};

PyBufferProcs JArray_as_buffer_short = {
    JPY_PY27_OLD_BUFFER_PROCS
    (getbufferproc) JArray_getbufferproc_short,
    (releasebufferproc) JArray_releasebufferproc_short
};

PyBufferProcs JArray_as_buffer_int = {
    JPY_PY27_OLD_BUFFER_PROCS
    (getbufferproc) JArray_getbufferproc_int,
    (releasebufferproc) JArray_releasebufferproc_int
};

PyBufferProcs JArray_as_buffer_long = {
    JPY_PY27_OLD_BUFFER_PROCS
    (getbufferproc) JArray_getbufferproc_long,
    (releasebufferproc) JArray_releasebufferproc_long
};

PyBufferProcs JArray_as_buffer_float = {
    JPY_PY27_OLD_BUFFER_PROCS
    (getbufferproc) JArray_getbufferproc_float,
    (releasebufferproc) JArray_releasebufferproc_float
};

PyBufferProcs JArray_as_buffer_double = {
    JPY_PY27_OLD_BUFFER_PROCS
    (getbufferproc) JArray_getbufferproc_double,
    (releasebufferproc) JArray_releasebufferproc_double
};
