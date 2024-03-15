//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.example.noisy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThirdPartyLibrary {

    private static final Logger log = LoggerFactory.getLogger(ThirdPartyLibrary.class);

    public static void stuff() {
        for (int i = 0; i < 1000; ++i) {
            log.info("this is a lot of stuff at info level {}", i);
        }
        log.debug("external debug");
        log.info("external info");
        log.warn("external warn");
        log.error("external error with stacktrace", new RuntimeException("expected"));
    }
}
