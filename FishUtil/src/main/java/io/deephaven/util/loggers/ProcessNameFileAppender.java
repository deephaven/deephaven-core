/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.loggers;

import io.deephaven.util.ProcessNameUtil;
import io.deephaven.io.logger.CustomLog4jLevel;
import org.apache.log4j.*;
import org.apache.log4j.spi.*;

import java.io.*;
import java.util.*;

import static io.deephaven.configuration.Configuration.LOGDIR_PROPERTY;

/**
 * Log4j FileAppender implementation for use in Deephaven log4j logs.
 */
public class ProcessNameFileAppender extends DailyRollingFileAppender {

    private boolean first = false;

    @SuppressWarnings("unused")
    public ProcessNameFileAppender() {
        super();
    }

    @SuppressWarnings("unused")
    public ProcessNameFileAppender(Layout layout, String filename, String datePattern)
        throws IOException {
        super(layout, filename, datePattern);
    }

    public void activateOptions() {
        String mainClass = System.getProperty("processname.main");

        if (mainClass == null) {
            StackTraceElement trace[] = Thread.currentThread().getStackTrace();

            String main[] = trace[trace.length - 1].getClassName().split("\\.");

            mainClass = main[main.length - 1];
        }

        final String suffix = ProcessNameUtil.getSuffix();

        final String logDirJvmProp = System.getProperty(LOGDIR_PROPERTY);
        if (logDirJvmProp != null) {
            setFile(logDirJvmProp + File.separator + mainClass + suffix + ".log");
        } else {
            setFile(System.getProperty("workspace") + "/../logs/" + mainClass + suffix + ".log"); // can't
                                                                                                  // use
                                                                                                  // a
                                                                                                  // property
                                                                                                  // here
                                                                                                  // since
                                                                                                  // configuration
                                                                                                  // needs
                                                                                                  // log4j
                                                                                                  // and
                                                                                                  // log4j
                                                                                                  // would
                                                                                                  // need
                                                                                                  // configuration
        }

        super.activateOptions();
    }

    // the append below is a hack. we grep out std out logs to send an email when the process is
    // completed. since we are now logging to a file we also need the email messages to go to the
    // console
    public void append(LoggingEvent event) {
        if (!first) {
            super.append(new LoggingEvent(event.getFQNOfLoggerClass(), event.getLogger(),
                Level.INFO, "********************************  " + new Date()
                    + " *******************************",
                null));

            first = true;
        }

        super.append(event);

        if (event.getLevel().equals(CustomLog4jLevel.EMAIL)) {
            System.out.println("EMAIL - " + event.getMessage());
        }
    }
}
