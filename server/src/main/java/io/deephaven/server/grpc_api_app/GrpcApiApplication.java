/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.grpc_api_app;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.sources.ring.RingTableTools;
import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.server.session.SessionState;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.util.annotations.ScriptApi;
import io.grpc.stub.StreamObserver;

/**
 * The {@value APP_NAME}, application id {@value APP_ID}, produces stream {@link io.deephaven.engine.table.Table tables}
 * {@value NOTIFICATION_INFO}; and derived table {@value NOTIFICATION_INFO_RING}. This data is modeled after the
 * {@link ExportNotification} event information from {@link SessionState#addExportListener(StreamObserver)}.
 *
 * @see #ENABLED
 * @see #RING_SIZE
 */
public final class GrpcApiApplication implements ApplicationState.Factory {
    private static final String APP_ID = "io.deephaven.server.grpc_api_app.GrpcApiApplication";
    private static final String APP_NAME = "GRPC API Application";
    private static final String NOTIFICATION_INFO = "session_export_notification_info";
    private static final String NOTIFICATION_INFO_RING = "session_export_notification_info_ring";

    private static final String ENABLED = "enabled";
    private static final String RING_SIZE = "ringSize";

    private static final Object MEMO_KEY = new Object();
    private static Table blinkTable = null;

    @ScriptApi
    public static Table exportStateChangeLog() {
        if (blinkTable == null) {
            throw new IllegalStateException("GrpcApiApplication not initialized");
        }
        return BlinkTableTools.blinkToAppendOnly(blinkTable, MEMO_KEY);
    }

    /**
     * Looks up the system property {@value ENABLED}, defaults to {@code false}.
     *
     * @return if the gRPC API application is enabled
     */
    private static boolean enabled() {
        return "true".equals(System.getProperty(APP_ID + "." + ENABLED));
    }

    /**
     * Looks up the system property {@value RING_SIZE}, defaults to {@code 1024}. The {@value NOTIFICATION_INFO_RING}
     * table is disabled when {@code 0} or less.
     *
     * @return the {@value NOTIFICATION_INFO_RING} table size
     */
    private static int ringSize() {
        return Integer.getInteger(APP_ID + "." + RING_SIZE, 1024);
    }

    @Override
    public ApplicationState create(final ApplicationState.Listener listener) {
        final ApplicationState state = new ApplicationState(listener, APP_ID, APP_NAME);
        if (!enabled()) {
            return state;
        }

        final SessionStateExportObjectUpdateStreamPublisher updateStream =
                new SessionStateExportObjectUpdateStreamPublisher();
        SessionState.setStateChangeListener(updateStream::onExportObjectStateUpdate);

        // noinspection resource
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(
                SessionStateExportObjectUpdateStreamPublisher.definition(), updateStream,
                ExecutionContext.getContext().getUpdateGraph(), NOTIFICATION_INFO);
        blinkTable = adapter.table();
        state.setField(NOTIFICATION_INFO, blinkTable);

        final int ringSize = ringSize();
        if (ringSize > 0) {
            state.setField(NOTIFICATION_INFO_RING, RingTableTools.of(blinkTable, ringSize));
        }
        return state;
    }
}
