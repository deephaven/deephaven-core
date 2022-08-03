/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util.scripts;

import java.io.Serializable;

/**
 * Tagging interface to be used to transmit {@link ScriptPathLoader} states.
 */
public interface ScriptPathLoaderState extends Serializable {
    ScriptPathLoaderState NONE = null;

    default String toAbbreviatedString() {
        return toString();
    }
}
