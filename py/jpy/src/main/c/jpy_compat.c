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

#include "jpy_compat.h"


#ifdef JPY_COMPAT_27

const char* JPy_AsUTF8_PriorToPy33(PyObject* pyStr)
{
    if (PyUnicode_Check(pyStr)) {
        pyStr = PyUnicode_AsUTF8String(pyStr);
        if (pyStr == NULL) {
            return NULL;
        }
    }
    return PyString_AsString(pyStr);
}

wchar_t* JPy_AsWideCharString_PriorToPy33(PyObject* pyUnicode, Py_ssize_t* size)
{
    wchar_t* buffer = NULL;
    PyObject* pyNewRef = NULL;

    if (!PyUnicode_Check(pyUnicode)) {
        pyNewRef = PyUnicode_FromObject(pyUnicode);
        if (pyNewRef == NULL) {
            goto error;
        }
        pyUnicode = pyNewRef;
    }

    *size = PyUnicode_GET_SIZE(pyUnicode);
    if (*size < 0) {
        goto error;
    }

    buffer = PyMem_New(wchar_t, *size);
    if (buffer == NULL) {
        goto error;
    }

    *size = PyUnicode_AsWideChar((PyUnicodeObject*) pyUnicode, buffer, *size);
    if (*size < 0) {
        PyMem_Free(buffer);
        buffer = NULL;
    }

error:

    JPy_XDECREF(pyNewRef);

    return buffer;
}

#endif
