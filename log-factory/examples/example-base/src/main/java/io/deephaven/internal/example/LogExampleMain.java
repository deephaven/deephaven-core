/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.internal.example;

import io.deephaven.io.logger.Logger;
import io.deephaven.internal.log.LoggerFactory;
import io.example.noisy.ThirdPartyLibrary;

public class LogExampleMain {

    public static final Logger log = LoggerFactory.getLogger(LogExampleMain.class);

    public static void main(String[] args) throws InterruptedException {
        Runtime.getRuntime().addShutdownHook(new Thread(log::shutdown));

        final Thread thread = new Thread(ThirdPartyLibrary::stuff);
        thread.setName("ThirdParty-thread");
        thread.start();
        thread.join();

        for (int i = 0; i < 10; ++i) {
            log.info().append(i).append(' ').appendDouble(i / 1000.0).endl();
        }
        log.trace().append("A trace").endl();
        log.debug().append("A debug").endl();
        log.info().append("An info").endl();
        log.warn().append("A warn").endl();
        log.error(new RuntimeException("For example")).append("An error with stacktrace").endl();
    }
}
