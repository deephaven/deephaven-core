//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jpy.PyLib;

@InternalUseOnly
public class IsPythonFreeThreaded {
    private static final Logger logger = LoggerFactory.getLogger(IsPythonFreeThreaded.class);
    private static Boolean isFreeThreaded = null;

    /**
     * Return true if the version of Python we are executing is free threaded.
     *
     * <p>
     * This must only be called after Python has already been initialized. <b>This class is not considered part of the
     * public Deephaven API and may change at any time.</b>
     * </p>
     *
     * @return true if the version of Python we are executing is free threaded.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean isPythonFreeThreaded() {
        if (isFreeThreaded != null) {
            return isFreeThreaded;
        }
        synchronized (IsPythonFreeThreaded.class) {
            if (isFreeThreaded != null) {
                return isFreeThreaded;
            }
            PyLib.assertPythonRuns();
            final String version = PyLib.getPythonVersion();
            isFreeThreaded = version.contains("free-threading");
            logger.info().append("Python ").append(isFreeThreaded ? "is" : "is not").append(" free threaded: ")
                    .append(version).endl();
            return isFreeThreaded;
        }
    }
}
