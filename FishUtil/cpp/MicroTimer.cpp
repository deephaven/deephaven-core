#include "MicroTimer.h"

#ifdef _WIN32
#include <windows.h>
#include <sys/timeb.h>
static jdouble scale;
LARGE_INTEGER startupTick;
LARGE_INTEGER startupTime;
const double MICROS_IN_SEC = 1000000.0;

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM * vm, void * reserved) {
    LARGE_INTEGER freq;
    QueryPerformanceFrequency (&freq);
    scale = freq.QuadPart / MICROS_IN_SEC;

    QueryPerformanceCounter(&startupTick);

    struct timeb startupTimeMillis;
    ftime(&startupTimeMillis);
    startupTime.QuadPart = startupTimeMillis.time;
    startupTime.QuadPart *= 1000;
    startupTime.QuadPart += startupTimeMillis.millitm;
    startupTime.QuadPart *= 1000;

    return JNI_VERSION_1_2;
}

JNIEXPORT jlong JNICALL Java_io_deephaven_util_clock_MicroTimer_currentTimeMicrosNative
  (JNIEnv * env, jclass cls) {
    LARGE_INTEGER now;
    QueryPerformanceCounter (&now);
    LARGE_INTEGER diff;
    diff.QuadPart = (now.QuadPart - startupTick.QuadPart) / scale;
    return startupTime.QuadPart + diff.QuadPart;
}

extern "C" JNIEXPORT jlong JNICALL Java_io_deephaven_util_clock_MicroTimer_clockRealtimeNative
  (JNIEnv * env, jclass cls) {
    jlong micros = Java_io_deephaven_util_clock_MicroTimer_currentTimeMicrosNative(env, cls);
    return micros * 1000L;
}

extern "C" JNIEXPORT jlong JNICALL Java_io_deephaven_util_clock_MicroTimer_clockMonotonicNative
  (JNIEnv * env, jclass cls) {
    jlong micros = Java_io_deephaven_util_clock_MicroTimer_currentTimeMicrosNative(env, cls);
    return micros * 1000L;
}


#else
#include <stdint.h>
#include <sys/time.h>
#include <time.h>
const uint64_t MICROS_IN_SEC = 1000000L;

JNIEXPORT jlong JNICALL Java_io_deephaven_util_clock_MicroTimer_currentTimeMicrosNative
  (JNIEnv * env, jclass cls) {
    timeval now;
    gettimeofday(&now, NULL);
    return ((uint64_t) now.tv_sec * 1000000L) + now.tv_usec;
}

extern "C" JNIEXPORT jlong JNICALL Java_io_deephaven_util_clock_MicroTimer_clockRealtimeNative
  (JNIEnv * env, jclass cls) {
    timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    return ((uint64_t) now.tv_sec * 1000000000L) + now.tv_nsec;
}

extern "C" JNIEXPORT jlong JNICALL Java_io_deephaven_util_clock_MicroTimer_clockMonotonicNative
  (JNIEnv * env, jclass cls) {
    timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return ((uint64_t) now.tv_sec * 1000000000L) + now.tv_nsec;
}

#endif

static __inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo)|(((unsigned long long)hi)<<32);
}

extern "C" JNIEXPORT jlong JNICALL Java_io_deephaven_util_clock_MicroTimer_rdtscNative
  (JNIEnv * env, jclass cls) {
    return (jlong) rdtsc();
}

