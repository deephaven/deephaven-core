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
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Modified version of {@link org.apache.parquet.hadoop.codec.ZstdCompressorStream} but with the no-finalizer
 * version of the output stream to avoid closing the underlying stream when GC runs.
 */
public class ZstdCompressorStream extends CompressionOutputStream {

    private final ZstdOutputStreamNoFinalizer zstdOutputStream;

    public ZstdCompressorStream(OutputStream stream, int level, int workers) throws IOException {
        super(stream);
        zstdOutputStream = new ZstdOutputStreamNoFinalizer(stream, level);
        zstdOutputStream.setWorkers(workers);
    }

    public ZstdCompressorStream(OutputStream stream, BufferPool pool, int level, int workers) throws IOException {
        super(stream);
        zstdOutputStream = new ZstdOutputStreamNoFinalizer(stream, pool);
        zstdOutputStream.setLevel(level);
        zstdOutputStream.setWorkers(workers);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        zstdOutputStream.write(b, off, len);
    }

    public void write(int b) throws IOException {
        zstdOutputStream.write(b);
    }

    public void finish() throws IOException {
        // no-op, doesn't apply to ZSTD
    }

    public void resetState() throws IOException {
        // no-op, doesn't apply to ZSTD
    }

    @Override
    public void flush() throws IOException {
        zstdOutputStream.flush();
    }

    @Override
    public void close() throws IOException {
        zstdOutputStream.close();
    }
}
