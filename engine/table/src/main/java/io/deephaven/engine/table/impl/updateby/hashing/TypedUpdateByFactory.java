/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.updateby.hashing;

import com.squareup.javapoet.CodeBlock;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.by.typed.HasherConfig;

public class TypedUpdateByFactory {
    public static void addOnlyBuildLeftFound(HasherConfig<?> hasherConfig, boolean alternate,
                                             CodeBlock.Builder builder) {
//        builder.addStatement("addLeftIndex(tableLocation, rowKeyChunk.get(chunkPosition))");
    }

    public static void addOnlyBuildLeftInsert(HasherConfig<?> hasherConfig, CodeBlock.Builder builder) {
//        builder.addStatement("addLeftIndex(tableLocation, rowKeyChunk.get(chunkPosition))");
//        builder.addStatement("rightRowSetSource.set(tableLocation, $T.builderSequential())", RowSetFactory.class);
    }
    public static void addOnlyRehashSetup(CodeBlock.Builder builder) {
//        builder.addStatement("final Object [] oldLeftState = leftRowSetSource.getArray()");
//        builder.addStatement("final Object [] destLeftState = new Object[tableSize]");
//        builder.addStatement("leftRowSetSource.setArray(destLeftState)");
    }

    public static void addOnlyMoveMainFull(CodeBlock.Builder builder) {
//        builder.addStatement("destLeftState[destinationTableLocation] = oldLeftState[sourceBucket]");
    }

    public static void addOnlyMoveMainAlternate(CodeBlock.Builder builder) {
//        builder.addStatement(
//                "leftRowSetSource.set(destinationTableLocation, alternateLeftRowSetSource.getUnsafe(locationToMigrate))");
//        builder.addStatement("alternateLeftRowSetSource.set(locationToMigrate, null)");
//        builder.addStatement(
//                "rightRowSetSource.set(destinationTableLocation, alternateRightRowSetSource.getUnsafe(locationToMigrate))");
//        builder.addStatement("alternateRightRowSetSource.set(locationToMigrate, null)");
//
//        builder.addStatement("final long cookie  = alternateCookieSource.getUnsafe(locationToMigrate)");
//        builder.addStatement("migrateCookie(cookie, destinationTableLocation, hashSlots)");
    }
}
