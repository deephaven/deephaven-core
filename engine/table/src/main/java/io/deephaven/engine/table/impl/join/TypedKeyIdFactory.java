//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.join;

import com.palantir.javapoet.CodeBlock;
import io.deephaven.engine.table.impl.by.typed.HasherConfig;

/**
 * The code fragments {@link io.deephaven.engine.table.impl.by.typed.TypedHasherFactory} uses to generate the
 * {@link StaticKeyIdHasherTypedBase} and {@link IncrementalKeyIdHasherTypedBase} implementations.
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

    /**
     * The insertion for a {@link StaticKeyIdHasherTypedBase}, which grows as soon as the table is over its load factor.
     */
    public static void staticInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        insert(hasherConfig, builder);
        builder.addStatement("growIfFull()");
    }

    public static void probeMissing(CodeBlock.Builder builder) {
        builder.addStatement("ids.set(chunkPosition, NULL_ID)");
        builder.addStatement("statuses.set(chunkPosition, MISSING)");
    }

    public static void moveMain(CodeBlock.Builder builder) {
        builder.addStatement("idToSlot.set(currentStateValue, destinationTableLocation)");
    }
}
