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

    /**
     * Get the number of threads to use for a given configuration key, defaulting the provided thread count if the
     * configuration key is set to a non-positive value, or the configuration key is not set and the provided default
     * config value is non-positive.
     *
     * @param configKey The configuration key to look up
     * @param defaultConfigValue The default value to use if the configuration key is not set
     * @param defaultNumThreads The default number of threads to use
     * @return The number of threads to use
     */
    public static int getOrComputeThreadCountProperty(
            final String configKey,
            final int defaultConfigValue,
            final int defaultNumThreads) {
        final int numThreads = Configuration.getInstance().getIntegerWithDefault(configKey, defaultConfigValue);
        if (numThreads <= 0) {
            return defaultNumThreads;
        } else {
            return numThreads;
        }
    }
}
