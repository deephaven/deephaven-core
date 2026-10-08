//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.join;

import com.palantir.javapoet.CodeBlock;
import io.deephaven.engine.table.impl.by.typed.HasherConfig;

/**
 * The code fragments {@link io.deephaven.engine.table.impl.by.typed.TypedHasherFactory} uses to generate the
 * {@link KeyIdHasherTypedBase} and {@link IncrementalKeyIdHasherTypedBase} implementations.
 */
public class TypedKeyIdFactory {
    public static void found(HasherConfig<?> hasherConfig, boolean alternate, CodeBlock.Builder builder) {
        builder.addStatement("ids.set(chunkPosition, idValue)");
        builder.addStatement("statuses.set(chunkPosition, FOUND)");
    }

    public static void insert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("final int id = allocateId(tableLocation)");
        builder.addStatement("mainId.set(tableLocation, id)");
        builder.addStatement("ids.set(chunkPosition, id)");
        builder.addStatement("statuses.set(chunkPosition, ADDED)");
    }

    public static void probeMissing(CodeBlock.Builder builder) {
        builder.addStatement("ids.set(chunkPosition, NULL_ID)");
        builder.addStatement("statuses.set(chunkPosition, MISSING)");
    }

    public static void moveMain(CodeBlock.Builder builder) {
        builder.addStatement("idToSlot.set(currentStateValue, destinationTableLocation)");
    }
}
