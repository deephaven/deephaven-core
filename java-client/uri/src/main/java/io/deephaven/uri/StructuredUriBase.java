package io.deephaven.uri;

public abstract class StructuredUriBase implements StructuredUri {

    @Override
    public final RemoteUri target(DeephavenTarget target) {
        return RemoteUri.of(target, this);
    }
}
