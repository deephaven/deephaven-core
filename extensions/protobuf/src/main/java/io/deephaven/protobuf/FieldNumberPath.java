/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Arrays;

@Immutable
@SimpleStyle
public abstract class FieldNumberPath {

    public static FieldNumberPath of(int... path) {
        return ImmutableFieldNumberPath.of(path);
    }

    @Parameter
    public abstract int[] path();

    public final boolean startsWith(FieldNumberPath prefix) {
        final int[] prefixPath = prefix.path();
        return Arrays.equals(path(), 0, Math.min(path().length, prefixPath.length), prefixPath, 0, prefixPath.length);
    }
}
