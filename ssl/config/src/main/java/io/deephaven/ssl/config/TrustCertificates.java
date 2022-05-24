package io.deephaven.ssl.config;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * The trust material from a certificate(s).
 */
@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableTrustCertificates.class)
public abstract class TrustCertificates implements Trust {

    public static TrustCertificates of(String... path) {
        return ImmutableTrustCertificates.builder().addPath(path).build();
    }

    /**
     * The certificate paths.
     */
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    public abstract List<String> path();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Check
    final void checkPath() {
        if (path().isEmpty()) {
            throw new IllegalArgumentException("path must have at least one element");
        }
    }
}
