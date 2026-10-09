//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.join;

import com.palantir.javapoet.CodeBlock;
import io.deephaven.engine.table.impl.by.typed.HasherConfig;

/**
 * The code fragments {@link io.deephaven.engine.table.impl.by.typed.TypedHasherFactory} uses to generate the
 * {@link StaticKeyLongValueHasherTypedBase} implementations.
 */
public class TypedKeyValueFactory {
    public static void buildFound(HasherConfig<?> hasherConfig, boolean alternate, CodeBlock.Builder builder) {
        builder.addStatement("final long repeated = repeatedValue(value, values.get(chunkPosition))");
        builder.beginControlFlow("if (repeated != value)");
        builder.addStatement("mainValue.set(tableLocation, repeated)");
        builder.endControlFlow();
    }

    public static void insert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("mainValue.set(tableLocation, values.get(chunkPosition))");
        builder.addStatement("growIfFull()");
    }

    public static void probeFound(HasherConfig<?> hasherConfig, boolean alternate, CodeBlock.Builder builder) {
        builder.addStatement("values.set(chunkPosition, value)");
    }

    public static void probeMissing(CodeBlock.Builder builder) {
        builder.addStatement("values.set(chunkPosition, missingValue)");
    }
}
