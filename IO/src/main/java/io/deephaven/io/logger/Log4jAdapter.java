/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import io.deephaven.io.log.LogLevel;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;

import java.util.*;

// --------------------------------------------------------------------
/**
 * Simple adapter that sends Log4j style log output to a Logger.
 */
public class Log4jAdapter {

    private static final Set<String> APPENDER_NAMES_TO_PRESERVE = new HashSet<>(Arrays.asList("MAILER"));

    // ----------------------------------------------------------------
    /**
     * Sends all Log4j style log output to the given Logger. Specifically, adds the Logger as an appender of the root
     * logger and removes appenders that aren't in the preservation list. Any non-root loggers and their appenders are
     * left unmolested. The goal is to redirect all default logging when it is not feasible to replace the Log4j-style
     * logging.
     */
    public static void sendLog4jToLogger(final Logger logger) {
        // noinspection unchecked
        final Enumeration<Appender> allAppenders = org.apache.log4j.Logger.getRootLogger().getAllAppenders();
        final List<Appender> appendersToRemove = new ArrayList<>();
        while (allAppenders.hasMoreElements()) {
            final Appender appender = allAppenders.nextElement();
            if (!APPENDER_NAMES_TO_PRESERVE.contains(appender.getName())) {
                appendersToRemove.add(appender);
            }
        }
        for (final Appender appender : appendersToRemove) {
            logger.debug().append(Log4jAdapter.class.getSimpleName()).append(": Removing Log4j root logger appender: ")
                    .append(appender.getName()).endl();
            org.apache.log4j.Logger.getRootLogger().removeAppender(appender);
        }
        org.apache.log4j.Logger.getRootLogger().addAppender(new AppenderSkeleton() {
            {
                setName("LOG4J_TO_FISHLIB_LOGGER");
            }

            @Override
            public void close() {}

            @Override
            public boolean requiresLayout() {
                return false;
            }

            @Override
            protected void append(org.apache.log4j.spi.LoggingEvent event) {
                logger.getEntry(getLogLevel(event.getLevel()), event.getTimeStamp() * 1000,
                        null == event.getThrowableInformation() ? null : event.getThrowableInformation().getThrowable())
                        .append(event.getRenderedMessage()).endl();
            }
        });
    }

    /** See also {@link Log4jLoggerImpl#getLog4jLevel)}. */
    public static LogLevel getLogLevel(org.apache.log4j.Level level) {
        if (level == null) {
            throw new IllegalArgumentException("Null level");
        }

        // common case first
        if (level.toInt() == org.apache.log4j.Level.INFO.toInt()) {
            return LogLevel.INFO;
        } else if (level.toInt() == org.apache.log4j.Level.DEBUG.toInt()) {
            return LogLevel.DEBUG;
        } else if (level.toInt() == org.apache.log4j.Level.TRACE.toInt()) {
            return LogLevel.TRACE;
        } else if (level.toInt() == org.apache.log4j.Level.WARN.toInt()) {
            return LogLevel.WARN;
        } else if (level.toInt() == org.apache.log4j.Level.ERROR.toInt()) {
            return LogLevel.ERROR;
        } else if (level.toInt() == org.apache.log4j.Level.FATAL.toInt()) {
            return LogLevel.FATAL;
        } else if (level.toInt() == CustomLog4jLevel.EMAIL.toInt()) {
            return LogLevel.EMAIL;
        } else if (level instanceof MailLevel) {
            return new LogLevel.MailLevel(((MailLevel) level).getSubject());
        } else {
            throw new IllegalArgumentException(level.toString());
        }
    }
}
