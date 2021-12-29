/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class DaemonThreadFactory implements ThreadFactory {

    private final ThreadFactory wrappedFactory = Executors.defaultThreadFactory();

    @Override
    public Thread newThread(Runnable r) {
        Thread t = wrappedFactory.newThread(r);
        t.setDaemon(true);
        return t;
    }
}
