//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.thread;

import io.deephaven.configuration.Configuration;

public class ThreadHelpers {
    /**
     * Get the number of threads to use for a given configuration key, defaulting to the number of available processors
     * if the configuration key is set to a non-positive value, or the configuration key is not set and the provided
     * default is non-positive.
     *
     * @param configKey The configuration key to look up
     * @param defaultValue The default value to use if the configuration key is not set
     * @return The number of threads to use
     */
    public static int getOrComputeThreadCountProperty(final String configKey, final int defaultValue) {
        final int numThreads = Configuration.getInstance().getIntegerWithDefault(configKey, defaultValue);
        if (numThreads <= 0) {
            return Runtime.getRuntime().availableProcessors();
        } else {
            return numThreads;
        }
    }
}
