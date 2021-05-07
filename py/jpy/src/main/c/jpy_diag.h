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

#ifndef JPY_DIAG_H
#define JPY_DIAG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "jpy_compat.h"

typedef struct JPy_Diag
{
    PyObject_HEAD
    int flags;
    int F_OFF;
    int F_TYPE;
    int F_METH;
    int F_EXEC;
    int F_MEM;
    int F_JVM;
    int F_ERR;
    int F_ALL;
}
JPy_Diag;


#define JPy_DIAG_F_OFF    0x00
#define JPy_DIAG_F_TYPE   0x01
#define JPy_DIAG_F_METH   0x02
#define JPy_DIAG_F_EXEC   0x04
#define JPy_DIAG_F_MEM    0x08
#define JPy_DIAG_F_JVM    0x10
#define JPy_DIAG_F_ERR    0x20
#define JPy_DIAG_F_ALL    0xff

extern PyTypeObject Diag_Type;
extern int JPy_DiagFlags;

PyObject* Diag_New(void);

void JPy_DiagPrint(int diagFlags, const char * format, ...);

#define JPy_DIAG_PRINT if (JPy_DiagFlags != 0) JPy_DiagPrint


#ifdef __cplusplus
}  /* extern "C" */
#endif
#endif /* !JPY_DIAG_H */