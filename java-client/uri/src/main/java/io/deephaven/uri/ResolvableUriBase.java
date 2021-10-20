package io.deephaven.uri;

public abstract class ResolvableUriBase implements ResolvableUri {

    @Override
    public final RemoteUri target(DeephavenTarget target) {
        return RemoteUri.of(target, this);
    }
}
