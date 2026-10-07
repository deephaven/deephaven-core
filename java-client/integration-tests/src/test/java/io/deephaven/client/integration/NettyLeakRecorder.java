//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.netty.buffer.PooledByteBufAllocator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Records netty's buffer leak reports. Netty's {@code ResourceLeakDetector} only logs a leak, at ERROR, when the
 * garbage collector reclaims a buffer that was never released; {@code logback-test.xml} attaches this appender to that
 * logger so a test can fail on what was reported.
 */
public final class NettyLeakRecorder extends AppenderBase<ILoggingEvent> {

    private static final List<String> LEAKS = Collections.synchronizedList(new ArrayList<>());

    static List<String> leaks() {
        synchronized (LEAKS) {
            return new ArrayList<>(LEAKS);
        }
    }

    /**
     * Fails if netty has reported a leak so far in this JVM. Every test class that opens a channel calls this from its
     * {@code @AfterAll}, so a leak from any class is caught by that class or by whichever runs after it, in any order.
     * Netty reports a leak only once the garbage collector finds an unreleased buffer, and only when the next tracked
     * allocation polls the detector's reference queue, so this does both before looking.
     */
    static void assertNoLeaks() throws InterruptedException {
        // A hint, not a guarantee. If the collector does nothing, or does not reach the leaked buffer, netty never
        // learns the buffer is gone and this assertion passes with the leak unreported. A missed collection can
        // only hide a leak, never produce a false failure.
        System.gc();
        Thread.sleep(200);
        // Tracking an allocation reports whatever the GC just enqueued. The pooled allocator tracks every buffer at
        // this level; the unpooled one tracks only direct buffers, so an Unpooled heap buffer would not do.
        PooledByteBufAllocator.DEFAULT.heapBuffer(1).release();
        assertThat(leaks()).as("netty buffer leaks reported so far in this JVM").isEmpty();
    }

    @Override
    protected void append(ILoggingEvent event) {
        final String message = event.getFormattedMessage();
        if (message != null && message.contains("LEAK:")) {
            LEAKS.add(message);
        }
    }
}
