//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

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
     * Fails if netty has reported a leak so far in this JVM. Every test class calls this from its {@code @AfterAll}, so
     * a leak from any class is caught by that class or by whichever runs after it, in any order. Netty reports a leak
     * only once the garbage collector finds an unreleased buffer, so this gives it a chance first.
     */
    static void assertNoLeaks() throws InterruptedException {
        System.gc();
        Thread.sleep(200);
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
