/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.logback;

import ch.qos.logback.core.OutputStreamAppender;
import io.deephaven.base.system.PrintStreamGlobals;

/**
 * Logs events out to {@link PrintStreamGlobals#getOut()}.
 */
public class PrintStreamGlobalsConsole<E> extends OutputStreamAppender<E> {

    @Override
    public void start() {
        setOutputStream(PrintStreamGlobals.getOut());
        super.start();
    }
}
