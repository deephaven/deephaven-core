/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

/**
 * The {@link JsType} specific registration.
 */
public interface JsTypeRegistration {

    /**
     * Register {@code jsType}.
     *
     * @param jsType the js type
     */
    void register(JsType jsType);
}
