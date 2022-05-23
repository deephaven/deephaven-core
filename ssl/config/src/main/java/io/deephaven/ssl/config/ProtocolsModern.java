package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

import java.util.Arrays;
import java.util.List;

/**
 * Includes the protocols {@value #TLSV_1_2} and {@value #TLSV_1_3}.
 */
@Immutable
@SimpleStyle
@JsonDeserialize(as = ImmutableProtocolsModern.class)
public abstract class ProtocolsModern implements Protocols {

    public static final String TLSV_1_2 = "TLSv1.2";

    public static final String TLSV_1_3 = "TLSv1.3";

    public static ProtocolsModern of() {
        return ImmutableProtocolsModern.of();
    }

    public final List<String> protocols() {
        return Arrays.asList(TLSV_1_2, TLSV_1_3);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
