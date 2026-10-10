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
    /**
     * The state match for a {@link StaticKeyIdHasherTypedBase}: compare a slot's key only when the slot's tag is the
     * key's tag.
     */
    public static String tagMatches(final String tagValueName) {
        return tagValueName + " == tagFor(hash)";
    }

    /**
     * The state match for an {@link IncrementalKeyIdHasherTypedBase}: compare a slot's key when the slot's tag is the
     * key's tag, or when the slot is a tombstone, whose key a search must still recognize.
     */
    public static String tagOrTombstoneMatches(final String tagValueName) {
        return tagValueName + " == tagFor(hash) || isStateDeleted(" + tagValueName + ")";
    }

    public static void found(HasherConfig<?> hasherConfig, boolean alternate, CodeBlock.Builder builder) {
        builder.addStatement("ids.set(chunkPosition, $L)", idAtLocation(alternate));
        builder.addStatement("statuses.set(chunkPosition, FOUND)");
    }

    public static void insert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("final int id = allocateId(tableLocation)");
        builder.addStatement("mainTag.set(tableLocation, tagFor(hash))");
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

    public static void probeFound(HasherConfig<?> hasherConfig, boolean alternate, CodeBlock.Builder builder) {
        builder.addStatement("ids.set(chunkPosition, $L)", idAtLocation(alternate));
    }

    public static void probeMissing(CodeBlock.Builder builder) {
        builder.addStatement("ids.set(chunkPosition, NULL_ID)");
    }

    /**
     * Before a full rehash moves the tags, replace the ids with a new array for it to move them into as well.
     */
    public static void rehashFullSetup(CodeBlock.Builder builder) {
        builder.addStatement("final int[] destIds = new int[tableSize]");
        builder.addStatement("final int[] originalIds = mainId.getArray()");
        builder.addStatement("mainId.setArray(destIds)");
    }

    public static void staticMoveMainFull(CodeBlock.Builder builder) {
        builder.addStatement("destIds[destinationTableLocation] = originalIds[sourceBucket]");
    }

    public static void incrementalMoveMainFull(CodeBlock.Builder builder) {
        builder.addStatement("final int id = originalIds[sourceBucket]");
        builder.addStatement("destIds[destinationTableLocation] = id");
        builder.addStatement("idToSlot.set(id, destinationTableLocation)");
    }

    public static void incrementalMoveMainAlternate(CodeBlock.Builder builder) {
        builder.addStatement("final int id = alternateId.getUnsafe(locationToMigrate)");
        builder.addStatement("mainId.set(destinationTableLocation, id)");
        builder.addStatement("idToSlot.set(id, destinationTableLocation)");
    }

    private static String idAtLocation(final boolean alternate) {
        return alternate ? "alternateId.getUnsafe(alternateTableLocation)" : "mainId.getUnsafe(tableLocation)";
    }
}
