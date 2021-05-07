#include "SignalUtils.h"

#include <sys/types.h>
#include <signal.h>

extern "C" JNIEXPORT jint JNICALL Java_io_deephaven_util_signals_SignalUtils_sendSignalNative
  (JNIEnv * env, jclass cls, jint pid, jint sig) {
    return kill(pid, sig);
}
