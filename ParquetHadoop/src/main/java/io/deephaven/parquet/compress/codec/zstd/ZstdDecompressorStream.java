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
package io.deephaven.parquet.compress.codec.zstd;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Modified version of {@link org.apache.parquet.hadoop.codec.ZstdDecompressorStream} but with the no-finalizer
 * version of the input stream to avoid closing the underlying stream when GC runs.
 */
public class ZstdDecompressorStream extends CompressionInputStream {

    private final ZstdInputStreamNoFinalizer zstdInputStream;

    public ZstdDecompressorStream(InputStream stream) throws IOException {
        super(stream);
        zstdInputStream = new ZstdInputStreamNoFinalizer(stream);
    }

    public ZstdDecompressorStream(InputStream stream, BufferPool pool) throws IOException {
        super(stream);
        zstdInputStream = new ZstdInputStreamNoFinalizer(stream, pool);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return zstdInputStream.read(b, off, len);
    }

    public int read() throws IOException {
        return zstdInputStream.read();
    }

    public void resetState() throws IOException {
        // no-op, doesn't apply to ZSTD
    }

    @Override
    public void close() throws IOException {
        try {
            zstdInputStream.close();
        } finally {
            super.close();
        }
    }
}
