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
#include "jpy_jtype.h"
#include "jpy_jobj.h"
#include "jpy_jfield.h"
#include "jpy_conv.h"
#include "jpy_compat.h"


JPy_JField* JField_New(JPy_JType* declaringClass, PyObject* fieldName, JPy_JType* fieldType, jboolean isStatic, jboolean isFinal, jfieldID fid)
{
    PyTypeObject* type = &JField_Type;
    JPy_JField* field;

    field = (JPy_JField*) type->tp_alloc(type, 0);
    field->declaringClass = declaringClass;
    field->name = fieldName;
    field->type = fieldType;
    field->isStatic = isStatic;
    field->isFinal = isFinal;
    field->fid = fid;

    JPy_INCREF(field->name);
    JPy_INCREF(field->type);

    return field;
}

/**
 * The JField type's tp_dealloc slot.
 */
void JField_dealloc(JPy_JField* self)
{
    JPy_DECREF(self->name);
    JPy_DECREF(self->type);
    Py_TYPE(self)->tp_free((PyObject*) self);
}

void JField_Del(JPy_JField* field)
{
    JField_dealloc(field);
}


/**
 * The JField type's tp_repr slot.
 */
PyObject* JField_repr(JPy_JField* self)
{
    const char* name = JPy_AS_UTF8(self->name);
    return JPy_FROM_FORMAT("%s(name='%s', is_static=%d, is_final=%d, fid=%p)",
                           ((PyObject*)self)->ob_type->tp_name,
                           name,
                           self->isStatic,
                           self->isFinal,
                           self->fid);
}

/**
 * The JField type's tp_str slot.
 */
PyObject* JField_str(JPy_JField* self)
{
    JPy_INCREF(self->name);
    return self->name;
}


static PyMemberDef JField_members[] =
{
    {"name",        T_OBJECT_EX, offsetof(JPy_JField, name),       READONLY, "Field name"},
    {"is_static",   T_BOOL,      offsetof(JPy_JField, isStatic),   READONLY, "Tests if this is a static field"},
    {"is_final",    T_BOOL,      offsetof(JPy_JField, isFinal),    READONLY, "Tests if this is a final field"},
    {NULL}  /* Sentinel */
};


/**
 * Implements the BeamPy_JObjectType class singleton.
 */
PyTypeObject JField_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "jpy.JField",                 /* tp_name */
    sizeof (JPy_JField),          /* tp_basicsize */
    0,                            /* tp_itemsize */
    (destructor)JField_dealloc,   /* tp_dealloc */
    NULL,                         /* tp_print */
    NULL,                         /* tp_getattr */
    NULL,                         /* tp_setattr */
    NULL,                         /* tp_reserved */
    (reprfunc)JField_repr,        /* tp_repr */
    NULL,                         /* tp_as_number */
    NULL,                         /* tp_as_sequence */
    NULL,                         /* tp_as_mapping */
    NULL,                         /* tp_hash  */
    NULL,                         /* tp_call */
    (reprfunc)JField_str,         /* tp_str */
    NULL,                         /* tp_getattro */
    NULL,                         /* tp_setattro */
    NULL,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,           /* tp_flags */
    "Java Field Wrapper",         /* tp_doc */
    NULL,                         /* tp_traverse */
    NULL,                         /* tp_clear */
    NULL,                         /* tp_richcompare */
    0,                            /* tp_weaklistoffset */
    NULL,                         /* tp_iter */
    NULL,                         /* tp_iternext */
    NULL,                         /* tp_methods */
    JField_members,               /* tp_members */
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