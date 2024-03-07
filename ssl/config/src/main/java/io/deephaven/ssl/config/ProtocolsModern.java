//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

import java.util.Collections;
import java.util.List;

/**
 * Includes the protocol {@value #TLSV_1_3}.
 *
 * @see <a href="https://wiki.mozilla.org/Security/Server_Side_TLS#Modern_compatibility">Modern compatibility</a>
 */
@Immutable
@SingletonStyle
@JsonDeserialize(as = ImmutableProtocolsModern.class)
public abstract class ProtocolsModern implements Protocols {

    public static final String TLSV_1_3 = "TLSv1.3";

    public static ProtocolsModern of() {
        return ImmutableProtocolsModern.of();
    }

    public final List<String> protocols() {
        return Collections.singletonList(TLSV_1_3);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
