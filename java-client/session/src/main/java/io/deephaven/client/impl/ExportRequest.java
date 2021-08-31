package io.deephaven.client.impl;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.qst.table.TableSpec;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A request to export a {@link #table() table}.
 *
 * @see Session#export(ExportsRequest)
 */
@Immutable
@SimpleStyle
public abstract class ExportRequest {

    public interface Listener {
        /**
         * Creates a stateful listener that warns on each creation responses that is not successful, and logs once on
         * error.
         *
         * @return the listener
         */
        static Listener logging() {
            return new LoggingListener();
        }

        void onNext(ExportedTableCreationResponse response);

        void onError(Throwable t);

        void onCompleted();
    }

    public static ExportRequest of(TableSpec table, Listener listener) {
        return ImmutableExportRequest.of(table, listener);
    }

    /**
     * The table.
     *
     * @return the table
     */
    @Parameter
    public abstract TableSpec table();

    /**
     * The listener.
     *
     * @return the listener
     */
    @Parameter
    public abstract Listener listener();

    private static class LoggingListener implements Listener {

        private static final Logger log = LoggerFactory.getLogger(LoggingListener.class);

        private final AtomicBoolean onErrorNotified;

        private LoggingListener() {
            this(new AtomicBoolean(false));
        }

        private LoggingListener(AtomicBoolean onErrorNotified) {
            this.onErrorNotified = Objects.requireNonNull(onErrorNotified);
        }

        @Override
        public void onNext(ExportedTableCreationResponse response) {
            if (response.getSuccess()) {
                String reference = ExportTicketHelper.toReadableString(response.getResultId(), "resultId");
                log.debug("ExportedTableCreationResponse for '{}' was successful", reference);
                return;
            }
            String reference = ExportTicketHelper.toReadableString(response.getResultId(), "resultId");
            log.warn("ExportedTableCreationResponse for '{}' was not successful: {}", reference,
                    response.getErrorInfo());
        }

        @Override
        public void onError(Throwable t) {
            if (onErrorNotified.compareAndSet(false, true)) {
                log.error("LoggingListener onError", t);
            }
        }

        @Override
        public void onCompleted() {
            log.debug("LoggingListener onCompleted");
        }
    }
}
