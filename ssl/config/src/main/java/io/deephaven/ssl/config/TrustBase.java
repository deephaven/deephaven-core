package io.deephaven.ssl.config;

public abstract class TrustBase implements Trust {

    @Override
    public boolean contains(Trust trust) {
        return equals(trust);
    }

    @Override
    public Trust or(Trust other) {
        if (contains(other)) {
            return this;
        }
        if (other.contains(this)) {
            return other;
        }
        return TrustList.of(this, other);
    }
}
