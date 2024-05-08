//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.thread;

import io.deephaven.configuration.Configuration;

public class ThreadHelpers {
    public static int getNumThreadsFromConfig(final String configKey) {
        final int numThreads = Configuration.getInstance().getIntegerWithDefault(configKey, -1);
        if (numThreads <= 0) {
            return Runtime.getRuntime().availableProcessors();
        } else {
            return numThreads;
        }
    }
}
