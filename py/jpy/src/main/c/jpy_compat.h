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

#ifndef JPY_COMPAT_H
#define JPY_COMPAT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <Python.h>

#define JPY_VERSION_ERROR "jpy requires either Python 2.7 or Python 3.3+"

#if PY_MAJOR_VERSION == 2 && PY_MINOR_VERSION == 7
#define JPY_COMPAT_27 1
#undef JPY_COMPAT_33P
#elif PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION >= 3
#define JPY_COMPAT_33P 1
#if PY_MINOR_VERSION >= 5
#define JPY_COMPAT_35P 1
#endif
#undef JPY_COMPAT_27
#else
#error JPY_VERSION_ERROR
#endif


#if defined(JPY_COMPAT_33P)

#define JPy_IS_CLONG(pyArg)      PyLong_Check(pyArg)
#define JPy_AS_CLONG(pyArg)      PyLong_AsLong(pyArg)
#define JPy_AS_CLONGLONG(pyArg)  PyLong_AsLongLong(pyArg)
#define JPy_FROM_CLONG(cl)       PyLong_FromLong(cl)

#define JPy_IS_STR(pyArg)        PyUnicode_Check(pyArg)
#define JPy_FROM_CSTR(cstr)      PyUnicode_FromString(cstr)
#define JPy_FROM_FORMAT          PyUnicode_FromFormat

#define JPy_AS_UTF8(unicode)                 PyUnicode_AsUTF8(unicode)
#define JPy_AS_WIDE_CHAR_STR(unicode, size)  PyUnicode_AsWideCharString(unicode, size)
#define JPy_FROM_WIDE_CHAR_STR(wc, size)     PyUnicode_FromKindAndData(PyUnicode_2BYTE_KIND, wc, size)

#elif defined(JPY_COMPAT_27)

#define JPy_IS_CLONG(pyArg)      (PyInt_Check(pyArg) || PyLong_Check(pyArg))
#define JPy_AS_CLONG(pyArg)      (PyInt_Check(pyArg) ? PyInt_AsLong(pyArg) : PyLong_AsLong(pyArg))
#define JPy_AS_CLONGLONG(pyArg)  (PyInt_Check(pyArg) ? PyInt_AsLong(pyArg) : PyLong_AsLongLong(pyArg))
#define JPy_FROM_CLONG(cl)        PyInt_FromLong(cl)

#define JPy_IS_STR(pyArg)        (PyString_Check(pyArg) || PyUnicode_Check(pyArg))
#define JPy_FROM_CSTR(cstr)      PyString_FromString(cstr)
#define JPy_FROM_FORMAT          PyString_FromFormat

// Implement conversion rules from Python 2 to 3 as given here:
// https://docs.python.org/3.3/howto/cporting.html
// http://lucumr.pocoo.org/2011/1/22/forwards-compatible-python/

const char* JPy_AsUTF8_PriorToPy33(PyObject* unicode);
wchar_t* JPy_AsWideCharString_PriorToPy33(PyObject *unicode, Py_ssize_t *size);

#define JPy_AS_UTF8(unicode)                 JPy_AsUTF8_PriorToPy33(unicode)
#define JPy_AS_WIDE_CHAR_STR(unicode, size)  JPy_AsWideCharString_PriorToPy33(unicode, size)
#define JPy_FROM_WIDE_CHAR_STR(wc, size)     PyUnicode_FromWideChar(wc, size)

#endif


#ifdef __cplusplus
} /* extern "C" */
#endif
#endif /* !JPY_COMPAT_H */
