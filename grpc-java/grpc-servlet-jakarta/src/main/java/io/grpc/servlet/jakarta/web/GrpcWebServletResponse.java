/*
 * Copyright 2019 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.grpc.servlet.jakarta.web;

import io.grpc.internal.GrpcUtil;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Wraps the outgoing gRPC-web response in a way that lets the gRPC servlet/filter write normal gRPC payloads to it, and
 * let them be translated to gRPC-web. Presently, this only means changing the content-type, and writing trailers as a
 * streamed response.
 */
public class GrpcWebServletResponse extends HttpServletResponseWrapper {
    private Supplier<Map<String, String>> trailers;
    private GrpcWebOutputStream outputStream;

    public GrpcWebServletResponse(HttpServletResponse response) {
        super(response);
    }

    @Override
    public void setContentType(String type) {
        // Adapt the content-type to be grpc-web
        super.setContentType(
                type.replaceFirst(Pattern.quote(GrpcUtil.CONTENT_TYPE_GRPC), GrpcWebFilter.CONTENT_TYPE_GRPC_WEB));
    }

    @Override
    public void setTrailerFields(Supplier<Map<String, String>> supplier) {
        // intercept trailers and write them out as a message just before we complete
        trailers = supplier;
    }

    @Override
    public Supplier<Map<String, String>> getTrailerFields() {
        return trailers;
    }

    @Override
    public synchronized GrpcWebOutputStream getOutputStream() throws IOException {
        if (outputStream == null) {
            // Provide our own output stream instance, so we can control/monitor the write listener
            outputStream = new GrpcWebOutputStream(super.getOutputStream(), this);
        }
        return outputStream;
    }

    @Override
    public PrintWriter getWriter() {
        // As the GrpcAdapter will only use the getOutputStream method, we will always throw if this is used
        throw new UnsupportedOperationException("getWriter()");
    }

    public void writeTrailers(Runnable safelyComplete) {
        if (outputStream == null) {
            throw new IllegalStateException("outputStream");
        }
        if (trailers != null) {
            Map<String, String> map = trailers.get();
            if (map != null) {
                // Write any trailers out to the output stream as a payload, since grpc-web doesn't
                // use proper trailers.
                ByteBuffer payload = createTrailerPayload(map);

                if (payload.hasRemaining()) {
                    // Normally we must not throw, but this is an exceptional case. Complete the stream, _then_ throw
                    safelyComplete.run();
                    throw new IllegalStateException("Incorrectly sized buffer, trailer payload will be sized wrong");
                }

                // Write the payload to the wire, then complete the stream. This may complete asynchronously, so we
                // won't call super.complete() here.
                outputStream.writeAndCloseWhenReady(payload.array(), safelyComplete);
                return;
            }
        }

        // If the map was empty or absent, we wrote nothing, so call super.complete(). This is likely an error from the
        // client's perspective, but we don't have anything more to tell them.
        safelyComplete.run();
    }

    private ByteBuffer createTrailerPayload(Map<String, String> trailers) {
        // write a payload, even for an empty set of trailers, but not for
        // the absence of trailers.
        int trailerLength = trailers.entrySet().stream()
                .mapToInt(e -> e.getKey().length() + e.getValue().length() + 4).sum();
        ByteBuffer payload = ByteBuffer.allocate(5 + trailerLength);
        payload.put((byte) 0x80);
        payload.putInt(trailerLength);
        for (Map.Entry<String, String> entry : trailers.entrySet()) {
            payload.put(entry.getKey().getBytes(StandardCharsets.US_ASCII));
            payload.put((byte) ':');
            payload.put((byte) ' ');
            payload.put(entry.getValue().getBytes(StandardCharsets.US_ASCII));
            payload.put((byte) '\r');
            payload.put((byte) '\n');
        }
        return payload;
    }


}
