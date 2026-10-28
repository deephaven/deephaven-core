//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

/**
 * Marks the {@link CompressionCodecProvider}s that this module includes, the built-in types, which a deployment's
 * provider of the same compression type replaces. Package-private, so that only this module's providers are built-in.
 */
interface DeephavenCompressionCodecProvider extends CompressionCodecProvider {
}
