package io.deephaven.web.client.api;

import elemental2.dom.DomGlobal;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.shared.fu.JsRunnable;

/**
 * Util class using exponential backoff to keep trying to connect to a server. Any disconnect should call failed(), and
 * the given retry callback will be invoked as appropriate. Once connection is established, success() should be invoked
 * to clear the reset tries attempted.
 *
 * Max backoff is doubled each failure, to a max of one minute. A random value between 0 and that backoff is selected to
 * wait before trying again, to avoid clients all attempting to simultaneously reconnect.
 *
 * A maximum number of times to try can be specified, otherwise defaults to MAX_VALUE.
 */
public class ReconnectState {
    private static final int MIN_BACKOFF_MILLIS = 100;// 0.1 seconds
    private static final int MAX_BACKOFF_MILLIS = 1000 * 60;// 1 minute

    private double cancel;

    public enum State {
        Disconnected, Connecting, Connected, Failed, Reconnecting
    }

    private final int maxTries;
    private final JsRunnable retry;

    private State state = State.Disconnected;
    private int currentTry;

    public ReconnectState(JsRunnable retry) {
        this(Integer.MAX_VALUE, retry);
    }

    public ReconnectState(int maxTries, JsRunnable retry) {
        this.maxTries = maxTries;
        this.retry = retry;
    }

    /**
     * Call once it is time to connect for the first time, will invoke the retry function. Can also be called after
     * failure to start trying to connect fresh.
     */
    public void initialConnection() {
        if (state == State.Connecting || state == State.Reconnecting || state == State.Connected) {
            // already connected, or attempting a connection, ignore
            JsLog.debug("Already connected, not attempting initial connection", this);
            return;
        }

        JsLog.debug("Beginning initial connection", this);
        state = State.Connecting;
        retry.run();
    }

    /**
     * After a successful connection is established, should be called.
     */
    public void success() {
        assert state == State.Connecting || state == State.Reconnecting;
        state = State.Connected;
        // reset attempt count for next failure
        currentTry = 0;
    }

    /**
     * After the connection closes or attempt fails, should be called.
     */
    public void failed() {
        if (state == State.Failed) {
            // dup call, we haven't yet tried reconnect yet or reached max tries, ignore
            JsLog.debug("Already queued new connection attempt", this);
            return;
        }
        state = State.Failed;
        if (currentTry > maxTries) {
            // give up (notify of failure?)
            JsLog.debug("Notified of connection failure, but unable to try again", this);
            return;
        }

        currentTry++;

        // randomly pick a delay, exponentially back off based on number of tries
        // https://en.wikipedia.org/wiki/Exponential_backoff
        double delay = Math.random() * Math.min(
                MIN_BACKOFF_MILLIS * Math.pow(2, currentTry - 1),
                MAX_BACKOFF_MILLIS // don't go above the max delay
        );
        JsLog.debug("Attempting reconnect in ", delay, "ms", this);
        cancel = DomGlobal.setTimeout(ignore -> {
            assert state == State.Failed;
            state = State.Reconnecting;
            retry.run();
        }, delay);
    }

    /**
     * After the connection has been deliberately closed, call this to prepare for a later connection.
     */
    public void disconnected() {
        state = State.Disconnected;
        DomGlobal.clearTimeout(cancel);
    }

    public State getState() {
        return state;
    }
}
