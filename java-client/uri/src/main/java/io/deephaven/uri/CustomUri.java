package io.deephaven.uri;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.net.URI;

@Immutable
@SimpleStyle
public abstract class CustomUri extends StructuredUriBase {

    public static boolean isValidScheme(String scheme) {
        return scheme != null && !DeephavenUri.isValidScheme(scheme);
    }

    public static CustomUri of(URI uri) {
        return ImmutableCustomUri.of(uri);
    }

    @Parameter
    public abstract URI uri();

    @Override
    public final URI toURI() {
        return uri();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(uri());
        return visitor;
    }

    @Override
    public final String toString() {
        return uri().toString();
    }

    @Check
    final void checkScheme() {
        if (!isValidScheme(uri().getScheme())) {
            throw new IllegalArgumentException(String.format("Invalid custom URI '%s'", uri()));
        }
    }
}
