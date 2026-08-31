//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh.updateby")
public class UpdateByControl {
    /**
     * If redirections should be used for output sources instead of sparse array sources. If unset, defaults to
     * server-provided defaults.
     */
    @JsNullable
    public Boolean useRedirection;

    /**
     * The maximum chunk capacity. If unset, defaults to server-provided defaults.
     */
    @JsNullable
    public Double chunkCapacity;
    /**
     * The maximum fractional memory overhead allowable for sparse redirections as a fraction (e.g. 1.1 is 10%
     * overhead). Values less than zero disable overhead checking, and result in always using the sparse structure. A
     * value of zero results in never using the sparse structure. If unset, defaults to server-provided defaults.
     */
    @JsNullable
    public Double maxStaticSparseMemoryOverhead;
    /**
     * The initial hash table size. If unset, defaults to server-provided defaults.
     */
    @JsNullable
    public Double initialHashTableSize;
    /**
     * The maximum load factor for the hash table. If unset, defaults to server-provided defaults.
     */
    @JsNullable
    public Double maximumLoadFactor;
    /**
     * The target load factor for the hash table. If unset, defaults to server-provided defaults.
     */
    @JsNullable
    public Double targetLoadFactor;

    /**
     * The math context.
     */
    @JsNullable
    public MathContext mathContext;
}
