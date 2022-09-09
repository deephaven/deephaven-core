/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.type;

import io.deephaven.plugin.Registration;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * A {@link Registration} that sources {@link JsTypePackageJson} from the system property {@value JS_TYPE_PROP}.
 * Multiple values can be specified by using a comma as a separator.
 *
 * <p>
 * Potentially useful for development purposes while developing a js plugin.
 */
public final class JsTypePackageJsonSystemPropertyRegistration implements Registration {

    private static final String JS_TYPE_PROP = "io.deephaven.server.plugin.type.JsTypePackageJson";

    @Inject
    public JsTypePackageJsonSystemPropertyRegistration() {}

    @Override
    public void registerInto(Callback callback) {
        final String values = System.getProperty(JS_TYPE_PROP, null);
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
                callback.register(JsTypePackageJson.of(packageJson));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
