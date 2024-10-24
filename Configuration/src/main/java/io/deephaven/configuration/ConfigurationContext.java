//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.configuration;

import java.util.Collection;
import java.util.List;

/**
 * A class for determining the environment that a configuration is running within.
 */
public interface ConfigurationContext {
    /**
     * Check whether the current system context matches one of the requested values in the specified scope
     *
     * @param token The name of the property to check
     * @param targetValues A list of possible values for the specified property, such as 'process.name=foo'
     * @return True if the specified property currently has a value equal to one of the target values, false otherwise.
     */
    boolean matches(String token, List<String> targetValues);

    /**
     * Return the configuration contexts. This is the list of properties that may have been used to parse the
     * configuration file. This collection is immutable.
     *
     * @return the configuration contexts.
     */
    Collection<String> getContextKeyValues();
}
