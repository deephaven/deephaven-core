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

#include <Python.h>
#include "jpy_verboseexcept.h"

int JPy_VerboseExceptions = 0;

PyObject* VerboseExceptions_New(void)
{
    return PyObject_New(PyObject, &VerboseExceptions_Type);
}


PyObject* VerboseExceptions_getattro(PyObject* self, PyObject *attr_name)
{
    if (strcmp(JPy_AS_UTF8(attr_name), "enabled") == 0) {
        return PyBool_FromLong(JPy_VerboseExceptions);
    } else {
        return PyObject_GenericGetAttr(self, attr_name);
    }
}


int VerboseExceptions_setattro(PyObject* self, PyObject *attr_name, PyObject *v)
{
    if (strcmp(JPy_AS_UTF8(attr_name), "enabled") == 0) {
        if (PyBool_Check(v)) {
            JPy_VerboseExceptions =  v == Py_True;
        } else {
            PyErr_SetString(PyExc_ValueError, "value for 'flags' must be a boolean");
            return -1;
        }
        return 0;
    } else {
        return PyObject_GenericSetAttr(self, attr_name, v);
    }
}


PyTypeObject VerboseExceptions_Type =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "jpy.VerboseExceptions",                   /* tp_name */
    sizeof (VerboseExceptions_Type),            /* tp_basicsize */
    0,                            /* tp_itemsize */
    NULL,                         /* tp_dealloc */
    NULL,                         /* tp_print */
    NULL,                         /* tp_getattr */
    NULL,                         /* tp_setattr */
    NULL,                         /* tp_reserved */
    NULL,                         /* tp_repr */
    NULL,                         /* tp_as_number */
    NULL,                         /* tp_as_sequence */
    NULL,                         /* tp_as_mapping */
    NULL,                         /* tp_hash  */
    NULL,                         /* tp_call */
    NULL,                         /* tp_str */
    (getattrofunc) VerboseExceptions_getattro, /* tp_getattro */
    (setattrofunc) VerboseExceptions_setattro, /* tp_setattro */
    NULL,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,           /* tp_flags */
    "Controls python exception verbosity",   /* tp_doc */
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
    NULL,                         /* tp_new */
};
