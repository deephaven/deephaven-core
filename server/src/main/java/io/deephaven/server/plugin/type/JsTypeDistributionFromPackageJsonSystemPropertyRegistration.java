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
 * A {@link Registration} that sources {@link JsTypeDistribution#fromPackageJson(Path)} from the system property
 * {@value FROM_PACKAGE_JSON}. Multiple values can be specified by using a comma as a separator.
 *
 * <p>
 * Potentially useful for development purposes while developing an NPM js plugin.
 */
public final class JsTypeDistributionFromPackageJsonSystemPropertyRegistration implements Registration {

    private static final String FROM_PACKAGE_JSON =
            "io.deephaven.server.plugin.type.JsTypeDistribution.fromPackageJson";

    @Inject
    public JsTypeDistributionFromPackageJsonSystemPropertyRegistration() {}

    @Override
    public void registerInto(Callback callback) {
        final String values = System.getProperty(FROM_PACKAGE_JSON, null);
        if (values == null) {
            return;
        }
        final Iterator<String> it = Arrays.stream(values.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .iterator();
        while (it.hasNext()) {
            final String packageJson = it.next();
            try {
                callback.register(JsTypeDistribution.fromPackageJson(Path.of(packageJson)));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
