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

#include <Python.h>
#include "structmember.h"
#include "jpy_diag.h"
#include "jpy_compat.h"

int JPy_DiagFlags = JPy_DIAG_F_OFF;


void JPy_DiagPrint(int diagFlags, const char * format, ...)
{
    if ((JPy_DiagFlags & diagFlags) != 0) {
        va_list args;
        va_start(args, format);
        vfprintf(stdout, format, args);
        fflush(stdout);
        va_end(args);
    }
}


PyObject* Diag_New(void)
{
    JPy_Diag* self;

    self = (JPy_Diag*) PyObject_New(PyObject, &Diag_Type);

    self->F_OFF   = JPy_DIAG_F_OFF;
    self->F_TYPE  = JPy_DIAG_F_TYPE;
    self->F_METH  = JPy_DIAG_F_METH;
    self->F_EXEC  = JPy_DIAG_F_EXEC;
    self->F_MEM   = JPy_DIAG_F_MEM;
    self->F_JVM   = JPy_DIAG_F_JVM;
    self->F_ERR   = JPy_DIAG_F_ERR;
    self->F_ALL   = JPy_DIAG_F_ALL;

    return (PyObject*) self;
}


PyObject* Diag_getattro(JPy_Diag* self, PyObject *attr_name)
{
    //printf("Diag_getattro: attr_name=%s\n", JPy_AS_UTF8(attr_name));
    if (strcmp(JPy_AS_UTF8(attr_name), "flags") == 0) {
        return JPy_FROM_CLONG(JPy_DiagFlags);
    } else {
        return PyObject_GenericGetAttr((PyObject*) self, attr_name);
    }
}


int Diag_setattro(JPy_Diag* self, PyObject *attr_name, PyObject *v)
{
    //printf("Diag_setattro: attr_name=%s\n", JPy_AS_UTF8(attr_name));
    if (strcmp(JPy_AS_UTF8(attr_name), "flags") == 0) {
        if (JPy_IS_CLONG(v)) {
            JPy_DiagFlags = self->flags = (int) JPy_AS_CLONG(v);
        } else {
            PyErr_SetString(PyExc_ValueError, "value for 'flags' must be an integer number");
            return -1;
        }
        return 0;
    } else {
        return PyObject_GenericSetAttr((PyObject*) self, attr_name, v);
    }
}


static PyMemberDef Diag_members[] =
{
    {"flags",    T_INT, offsetof(JPy_Diag, flags),   READONLY, "Combination of diagnostic flags (F_* constants). If != 0, diagnostic messages are printed out."},
    {"F_OFF",    T_INT, offsetof(JPy_Diag, F_OFF),   READONLY, "Don't print any diagnostic messages"},
    {"F_TYPE",   T_INT, offsetof(JPy_Diag, F_TYPE),  READONLY, "Type resolution: print diagnostic messages while generating Python classes from Java classes"},
    {"F_METH",   T_INT, offsetof(JPy_Diag, F_METH),  READONLY, "Method resolution: print diagnostic messages while resolving Java overloaded methods"},
    {"F_EXEC",   T_INT, offsetof(JPy_Diag, F_EXEC),  READONLY, "Execution: print diagnostic messages when Java code is executed"},
    {"F_MEM",    T_INT, offsetof(JPy_Diag, F_MEM),   READONLY, "Memory: print diagnostic messages when wrapped Java objects are allocated/deallocated"},
    {"F_JVM",    T_INT, offsetof(JPy_Diag, F_JVM),   READONLY, "JVM: print diagnostic information usage of the Java VM Invocation API"},
    {"F_ERR",    T_INT, offsetof(JPy_Diag, F_ERR),   READONLY, "Errors: print diagnostic information when erroneous states are detected"},
    {"F_ALL",    T_INT, offsetof(JPy_Diag, F_ALL),   READONLY, "Print any diagnostic messages"},
    {NULL}  /* Sentinel */
};


PyTypeObject Diag_Type =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "jpy.Diag",                   /* tp_name */
    sizeof (JPy_Diag),            /* tp_basicsize */
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
    (getattrofunc) Diag_getattro, /* tp_getattro */
    (setattrofunc) Diag_setattro, /* tp_setattro */
    NULL,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,           /* tp_flags */
    "Controls output of diagnostic information for debugging",   /* tp_doc */
    NULL,                         /* tp_traverse */
    NULL,                         /* tp_clear */
    NULL,                         /* tp_richcompare */
    0,                            /* tp_weaklistoffset */
    NULL,                         /* tp_iter */
    NULL,                         /* tp_iternext */
    NULL,                         /* tp_methods */
    Diag_members,                 /* tp_members */
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
