package io.deephaven.grpc_api.util;

import com.google.rpc.Code;
import io.deephaven.base.RAPriQueue;
import io.deephaven.base.verify.Assert;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.io.Closeable;

public class BrowserStream<T extends BrowserStream.MessageBase> implements Closeable {
    public enum Mode {
        IN_ORDER, MOST_RECENT
    }

    public static class MessageBase {
        private int pos;
        protected long sequence;
        protected boolean isHalfClosed;

        public MessageBase() {
            this(-1, false);
        }

        public MessageBase(final long sequence, final boolean isHalfClosed) {
            setSequenceInfo(sequence, isHalfClosed);
        }

        public void setSequenceInfo(long sequence, boolean isHalfClosed) {
            this.sequence = sequence;
            this.isHalfClosed = isHalfClosed;
        }
    }

    public interface Marshaller<T> {
        void onMessageReceived(T message);

        void onCancel();

        void onError(Throwable err);

        void onCompleted();
    }

    private static final Logger log = LoggerFactory.getLogger(BrowserStream.class);

    /** represents the sequence that the listener will process next */
    private long nextSeq = 0;
    /** represents whether or not an item is currently being processed */
    private boolean processingMessage = false;
    /** represents the sequence that the client-stream should be considered closed */
    private long halfClosedSeq = -1;

    private final Mode mode;
    private final String logIdentity;
    private final SessionState session;
    private final Marshaller<T> marshaller;

    /** priority queue for all pending seq when mode is Mode.IN_ORDER */
    private RAPriQueue<T> pendingSeq;

    /** most recent queued msg for when mode is Mode.MOST_RECENT */
    private T queuedMessage;

    public BrowserStream(final Mode mode, final SessionState session,
        final Marshaller<T> marshaller) {
        this.mode = mode;
        this.logIdentity =
            "BrowserStream(" + Integer.toHexString(System.identityHashCode(this)) + "): ";
        this.session = session;
        this.marshaller = marshaller;

        this.session.addOnCloseCallback(this);
    }

    public void onMessageReceived(T message) {
        synchronized (this) {
            if (halfClosedSeq != -1 && message.sequence > halfClosedSeq) {
                throw GrpcUtil.statusRuntimeException(Code.ABORTED,
                    "Sequence sent after half close: closed seq=" + halfClosedSeq + " recv seq="
                        + message.sequence);
            }

            if (message.isHalfClosed) {
                if (halfClosedSeq != -1) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Already half closed: closed seq=" + halfClosedSeq + " recv seq="
                            + message.sequence);
                }
                halfClosedSeq = message.sequence;
            }

            if (mode == Mode.IN_ORDER) {
                if (message.sequence < nextSeq) {
                    throw GrpcUtil.statusRuntimeException(Code.OUT_OF_RANGE,
                        "Duplicate sequence sent: next seq=" + nextSeq + " recv seq="
                            + message.sequence);
                }
                boolean queueMsg = false;
                if (processingMessage) {
                    queueMsg = true;
                    log.debug().append(logIdentity).append("queueing; next seq=").append(nextSeq)
                        .append(" recv seq=").append(message.sequence).endl();
                } else if (message.sequence != nextSeq) {
                    queueMsg = true;
                    log.debug().append(logIdentity).append("queueing; waiting seq=").append(nextSeq)
                        .append(" recv seq=").append(message.sequence).endl();
                }
                if (queueMsg) {
                    if (pendingSeq == null) {
                        pendingSeq = new RAPriQueue<>(1, MessageInfoQueueAdapter.getInstance(),
                            MessageBase.class);
                    }
                    pendingSeq.enter(message);
                    return;
                }
            } else { // Mode.MOST_RECENT
                if (message.sequence < nextSeq
                    || (message.sequence == nextSeq && processingMessage) // checks for duplicate
                    || (queuedMessage != null && message.sequence < queuedMessage.sequence)) {
                    // this message is too old
                    log.debug().append(logIdentity).append("dropping; next seq=").append(nextSeq)
                        .append(" queued seq=")
                        .append(queuedMessage != null ? queuedMessage.sequence : -1)
                        .append(" recv seq=").append(message.sequence).endl();
                    return;
                }
                // is most recent msg seen
                if (processingMessage) {
                    log.debug().append(logIdentity).append("queueing; processing seq=")
                        .append(nextSeq)
                        .append(" recv seq=").append(message.sequence).endl();
                    queuedMessage = message;
                    return;
                }
            }

            nextSeq = message.sequence + 1;
            processingMessage = true;
        }

        do {
            try {
                marshaller.onMessageReceived(message);
            } catch (final RuntimeException e) {
                onError(e);
                return;
            }

            synchronized (this) {
                if (message.isHalfClosed) {
                    onComplete();
                    processingMessage = false;
                    return;
                }
                if (mode == Mode.IN_ORDER) {
                    message = pendingSeq == null ? null : pendingSeq.top();
                    if (message == null || message.sequence != nextSeq) {
                        processingMessage = false;
                        break;
                    }
                    Assert.eq(pendingSeq.removeTop(), "pendingSeq.remoteTop()", message, "message");
                } else { // Mode.MOST_RECENT
                    message = queuedMessage;
                    if (message == null) {
                        processingMessage = false;
                        break;
                    }
                    queuedMessage = null;
                }

                log.debug().append(logIdentity).append("processing queued seq=")
                    .append(message.sequence).endl();
                nextSeq = message.sequence + 1;
            }
        } while (true);

    }

    public void onError(final RuntimeException e) {
        if (session.removeOnCloseCallback(this) != null) {
            log.error().append(logIdentity)
                .append("closing browser stream on unexpected exception: ").append(e).endl();
            this.marshaller.onError(e);
        }
    }

    @Override
    public void close() {
        this.marshaller.onCancel();
    }

    private void onComplete() {
        if (session.removeOnCloseCallback(this) != null) {
            log.info().append(logIdentity).append("browser stream completed").endl();
            this.marshaller.onCompleted();
        }
    }

    private static class MessageInfoQueueAdapter implements RAPriQueue.Adapter<MessageBase> {
        private static final MessageInfoQueueAdapter INSTANCE = new MessageInfoQueueAdapter();

        private static <T extends MessageBase> RAPriQueue.Adapter<T> getInstance() {
            // noinspection unchecked
            return (RAPriQueue.Adapter<T>) INSTANCE;
        }

        @Override
        public boolean less(MessageBase a, MessageBase b) {
            return a.sequence < b.sequence;
        }

        @Override
        public void setPos(MessageBase mi, int pos) {
            mi.pos = pos;
        }

        @Override
        public int getPos(MessageBase mi) {
            return mi.pos;
        }
    }
}
