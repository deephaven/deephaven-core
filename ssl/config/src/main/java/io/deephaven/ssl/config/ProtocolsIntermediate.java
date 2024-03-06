//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

import java.util.Arrays;
import java.util.List;

/**
 * Includes the protocols {@value #TLSV_1_3} and {@value #TLSV_1_2}.
 *
 * @see <a href=
 *      "https://wiki.mozilla.org/Security/Server_Side_TLS#Intermediate_compatibility_.28recommended.29">Intermediate
 *      compatibility</a>
 */
@Immutable
@SingletonStyle
@JsonDeserialize(as = ImmutableProtocolsIntermediate.class)
public abstract class ProtocolsIntermediate implements Protocols {

    public static final String TLSV_1_3 = "TLSv1.3";

    public static final String TLSV_1_2 = "TLSv1.2";

    public static ProtocolsIntermediate of() {
        return ImmutableProtocolsIntermediate.of();
    }

    public final List<String> protocols() {
        return Arrays.asList(TLSV_1_3, TLSV_1_2);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
