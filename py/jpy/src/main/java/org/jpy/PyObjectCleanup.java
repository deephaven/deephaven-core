package org.jpy;

interface PyObjectCleanup {
    int cleanupOnlyUseFromGIL();
}
