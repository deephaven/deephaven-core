package io.deephaven.appmode;

import java.util.Optional;

public interface Field<T> {
    /**
     * A human readable name for this field. Often used to label the web-ui tab.
     */
    String name();

    /**
     * Retrieve the instance that this field references.
     */
    T value();

    /**
     * An optional description for users who want to improve exploration of an existing application state.
     */
    Optional<String> description();
}
