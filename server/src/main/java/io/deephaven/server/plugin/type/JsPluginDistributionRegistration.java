/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.type;

import io.deephaven.plugin.Registration;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;

/**
 * A {@link Registration} that sources {@link JsPluginDistribution#fromPackageJsonDistribution(Path)} from the system
 * property {@value PROPERTY}. Multiple values can be specified by using a comma as a separator.
 *
 * <p>
 * Potentially useful for development purposes while developing JS plugins.
 */
public final class JsPluginDistributionRegistration implements Registration {

    private static final String PROPERTY = "io.deephaven.server.plugin.type.JsPluginDistribution";

    @Inject
    public JsPluginDistributionRegistration() {}

    @Override
    public void registerInto(Callback callback) {
        final String values = System.getProperty(PROPERTY, null);
        if (values == null) {
            return;
        }
        final Iterator<String> it = Arrays.stream(values.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .iterator();
        while (it.hasNext()) {
            final String distributionDir = it.next();
            try {
                callback.register(JsPluginDistribution.fromPackageJsonDistribution(Path.of(distributionDir)));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
