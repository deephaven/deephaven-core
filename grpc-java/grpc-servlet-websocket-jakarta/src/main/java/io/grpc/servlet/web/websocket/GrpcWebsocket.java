/*
 * Copyright 2022 Deephaven Data Labs
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

package io.grpc.servlet.web.websocket;

import jakarta.websocket.CloseReason;
import jakarta.websocket.Endpoint;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.Session;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Supports multiple endpoints, based on the negotiated sub-protocol. If a protocol isn't supported, an error will be
 * sent to the client.
 */
public class GrpcWebsocket extends Endpoint {
    private final Map<String, Supplier<Endpoint>> endpointFactories = new HashMap<>();
    private Endpoint endpoint;

    public GrpcWebsocket(Map<String, Supplier<Endpoint>> endpoints) {
        endpointFactories.putAll(endpoints);
    }

    public void onOpen(Session session, EndpointConfig endpointConfig) {
        Supplier<Endpoint> supplier = endpointFactories.get(session.getNegotiatedSubprotocol());
        if (supplier == null) {
            try {
                session.close(new CloseReason(CloseReason.CloseCodes.PROTOCOL_ERROR, "Unsupported subprotocol"));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return;
        }

        endpoint = supplier.get();
        endpoint.onOpen(session, endpointConfig);
    }

    @Override
    public void onClose(Session session, CloseReason closeReason) {
        endpoint.onClose(session, closeReason);
    }

    @Override
    public void onError(Session session, Throwable thr) {
        endpoint.onError(session, thr);
    }
}
