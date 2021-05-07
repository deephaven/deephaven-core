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

#include "org_jpy_DL.h"

#if !defined(_WIN32) && !defined(__CYGWIN__)

#include <dlfcn.h>

/*
 * Class:     org_jpy_DL
 * Method:    dlopen
 * Signature: (Ljava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL Java_org_jpy_DL_dlopen
  (JNIEnv *jenv, jclass dlClass, jstring jPath, jint mode)
{
    const char* path;
    int flags;

    if (jPath == NULL) {
        return 0;
    }

    path = (*jenv)->GetStringUTFChars(jenv, jPath, NULL);
    if (path == NULL) {
        return 0;
    }

    flags = 0;
    if ((mode & org_jpy_DL_RTLD_LAZY) != 0) {
        flags |= RTLD_LAZY;
    }
    if ((mode & org_jpy_DL_RTLD_NOW) != 0) {
        flags |= RTLD_NOW;
    }
    if ((mode & org_jpy_DL_RTLD_LOCAL) != 0) {
        flags |= RTLD_LOCAL;
    }
    if ((mode & org_jpy_DL_RTLD_GLOBAL) != 0) {
        flags |= RTLD_GLOBAL;
    }

    return (jlong) dlopen(path, flags);
}

/*
 * Class:     org_jpy_DL
 * Method:    dlclose
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_jpy_DL_dlclose
  (JNIEnv *jenv, jclass dlClass, jlong handle)
{
    return dlclose((void*) handle);
}

/*
 * Class:     org_jpy_DL
 * Method:    dlerror
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_jpy_DL_dlerror
  (JNIEnv *jenv, jclass dlClass)
{
    const char* message;

    message = dlerror();
    if (message != NULL) {
        return (*jenv)->NewStringUTF(jenv, message);
    } else {
        return NULL;
    }
}

#else /* !defined(_WIN32) && !defined(__CYGWIN__) */

// Dummy DLL entry point for Python 2.7 (Windows requires it)
__declspec(dllexport) void initjdl(void) { }

// Dummy DLL entry point for Python 3.3+ (Windows requires it)
__declspec(dllexport) void* PyInit_jdl(void) { return NULL; }

/*
 * Class:     org_jpy_DL
 * Method:    dlopen
 * Signature: (Ljava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL Java_org_jpy_DL_dlopen
  (JNIEnv *jenv, jclass dlClass, jstring jPath, jint mode)
{
    return 0;
}

/*
 * Class:     org_jpy_DL
 * Method:    dlclose
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_jpy_DL_dlclose
  (JNIEnv *jenv, jclass dlClass, jlong handle)
{
    return 0;
}

/*
 * Class:     org_jpy_DL
 * Method:    dlerror
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_jpy_DL_dlerror
  (JNIEnv *jenv, jclass dlClass)
{
    return NULL;
}

#endif /* !defined(_WIN32) && !defined(__CYGWIN__) */

