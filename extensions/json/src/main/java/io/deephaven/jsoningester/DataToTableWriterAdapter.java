    package io.deephaven.jsoningester;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface DataToTableWriterAdapter {

    /**
     * Wait for all enqueued messages to finish processing. If additional messages are queued after this function is
     * called, those messages may not be processed prior to returning. Additionally, this does not guarantee that all
     * processed messages are written. {@link #cleanup()} should be called after this function to ensure that all
     * data has been written.
     *
     * @param timeoutMillis maximum time to wait in milliseconds
     * @throws InterruptedException if wait was interrupted
     */
    void waitForProcessing(long timeoutMillis) throws InterruptedException, TimeoutException;

    /**
     * Do any 'cleanup' actions that should happen periodically but not at every message, such as flushing data.
     * @throws IOException If any underlying actions cause it to be thrown.
     */
    void cleanup() throws IOException;

    /**
     * Shut down the adapter. This <b>must not run {@link #cleanup cleanup}</b>; that is handled
     * by the StreamContext.
     */
    void shutdown();

}
