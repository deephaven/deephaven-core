/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.loggers;

import org.apache.log4j.spi.*;

public class AsyncAppender extends org.apache.log4j.AsyncAppender {

    public void append(LoggingEvent event) {
        Object message = event.getMessage();

        if (!(message instanceof String)) {
            event = new LoggingEvent(event.getFQNOfLoggerClass(),
                    event.getLogger(),
                    event.getTimeStamp(),
                    event.getLevel(),
                    message.toString(),
                    event.getThreadName(),
                    event.getThrowableInformation(),
                    event.getNDC(),
                    event.getLocationInformation(),
                    event.getProperties());
        }

        super.append(event);
    }

}
