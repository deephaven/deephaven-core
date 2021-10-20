package io.deephaven.uri;

public abstract class AdHocTableResolverBase implements TableResolver {

    @Override
    public final ResolvableUri create(String scheme, String rest) {
        if (!schemes().contains(scheme)) {
            throw new IllegalArgumentException(String.format("Invalid scheme '%s'", scheme));
        }
        return RawUri.fromPath(scheme, rest);
    }
}
