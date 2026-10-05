//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import com.palantir.javapoet.CodeBlock;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.impl.by.typed.HasherConfig;

/**
 * The code generation hooks for the {@link DistinctKeySet} hashers, whose slot state is the number of set rows holding
 * the slot's key.
 */
public class TypedDistinctSetFactory {

    private static String stateSource(final boolean alternate) {
        return alternate ? "alternateCount" : "mainCount";
    }

    private static String tableLocation(final boolean alternate) {
        return alternate ? "alternateTableLocation" : "tableLocation";
    }

    public static void addFound(HasherConfig<?> hasherConfig, boolean alternate, CodeBlock.Builder builder) {
        builder.beginControlFlow("if (count == 0)");
        builder.addStatement("++revivedKeys");
        builder.endControlFlow();
        builder.addStatement("$L.set($L, count + 1)", stateSource(alternate), tableLocation(alternate));
    }

    public static void addInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
        builder.addStatement("mainCount.set(tableLocation, 1L)");
        builder.addStatement("++insertedKeys");
    }

    public static void removeFound(HasherConfig<?> hasherConfig, boolean alternate, CodeBlock.Builder builder) {
        builder.addStatement("$T.gtZero(count, \"count\")", Assert.class);
        builder.addStatement("$L.set($L, count - 1)", stateSource(alternate), tableLocation(alternate));
        builder.beginControlFlow("if (count == 1)");
        builder.addStatement("++emptiedKeys");
        builder.endControlFlow();
    }

    public static void removeMissing(CodeBlock.Builder builder) {
        builder.addStatement("throw new $T($S)", IllegalStateException.class, "Removed key is not in the set");
    }

    public static void tombstoneFound(HasherConfig<?> hasherConfig, boolean alternate, CodeBlock.Builder builder) {
        builder.beginControlFlow("if (count == 0)");
        builder.addStatement("$L.set($L, $L)", stateSource(alternate), tableLocation(alternate),
                hasherConfig.tombstoneStateName);
        builder.addStatement("--liveEntries");
        builder.endControlFlow();
    }

    public static void tombstoneMissing(CodeBlock.Builder builder) {
        builder.addStatement("// A key removed by several rows is replaced when the first of them is probed");
    }

    public static void matchFound(HasherConfig<?> hasherConfig, boolean alternate, CodeBlock.Builder builder) {
        builder.beginControlFlow("if ((count > 0) == inclusion)");
        builder.addStatement("results.add(rowKeys.get(chunkPosition))");
        builder.endControlFlow();
    }

    public static void matchMissing(CodeBlock.Builder builder) {
        builder.beginControlFlow("if (!inclusion)");
        builder.addStatement("results.add(rowKeys.get(chunkPosition))");
        builder.endControlFlow();
    }

    public static void moveMain(CodeBlock.Builder builder) {
        // The state is the key's count, which moves with the key; there is nothing else to move.
    }
}
