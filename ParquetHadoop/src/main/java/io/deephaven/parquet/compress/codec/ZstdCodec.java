/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.deephaven.parquet.compress.codec;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.NoPool;
import com.github.luben.zstd.RecyclingBufferPool;
import io.deephaven.parquet.compress.codec.zstd.ZstdCompressorStream;
import io.deephaven.parquet.compress.codec.zstd.ZstdDecompressorStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.parquet.hadoop.codec.ZstandardCodec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Provides an alternative codec name of "ZSTD" instead of the superclass's "ZSTANDARD". These streams are also modified
 * to use the "no finalizer" variant of the underlying streams, so that GC picking up the streams doesn't close the
 * underlying file.
 */
public class ZstdCodec extends ZstandardCodec {
    @Override
    public CompressionInputStream createInputStream(InputStream stream) throws IOException {
        BufferPool pool;
        if (getConf().getBoolean(PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED,
                DEFAULT_PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED)) {
            pool = RecyclingBufferPool.INSTANCE;
        } else {
            pool = NoPool.INSTANCE;
        }
        return new ZstdDecompressorStream(stream, pool);
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream stream) throws IOException {
        BufferPool pool;
        if (getConf().getBoolean(PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED,
                DEFAULT_PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED)) {
            pool = RecyclingBufferPool.INSTANCE;
        } else {
            pool = NoPool.INSTANCE;
        }
        return new ZstdCompressorStream(stream, pool,
                getConf().getInt(PARQUET_COMPRESS_ZSTD_LEVEL, DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL),
                getConf().getInt(PARQUET_COMPRESS_ZSTD_WORKERS, DEFAULTPARQUET_COMPRESS_ZSTD_WORKERS));
    }
}
