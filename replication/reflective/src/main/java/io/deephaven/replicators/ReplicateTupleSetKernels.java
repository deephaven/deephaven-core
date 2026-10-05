//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import com.palantir.javapoet.JavaFile;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.impl.select.TupleSetKernelFactory;

import java.io.File;
import java.io.IOException;

/**
 * Generates the {@code TupleMapSetKernel} subclasses for every two column key; keys of more columns have theirs
 * generated when first needed.
 */
public class ReplicateTupleSetKernels {
    public static void main(String[] args) throws IOException {
        final File sourceRoot = new File("engine/table/src/main/java/");
        final ChunkType[] chunkTypes = new ChunkType[2];
        for (final ChunkType first : ChunkType.values()) {
            if (first == ChunkType.Boolean) {
                continue;
            }
            chunkTypes[0] = first;
            for (final ChunkType second : ChunkType.values()) {
                if (second == ChunkType.Boolean) {
                    continue;
                }
                chunkTypes[1] = second;
                final String className = TupleSetKernelFactory.className(chunkTypes);
                final JavaFile javaFile = TupleSetKernelFactory.generate(chunkTypes, className);
                System.out.println("Generating " + className + " to " + sourceRoot);
                javaFile.writeTo(sourceRoot);
            }
        }
    }
}
