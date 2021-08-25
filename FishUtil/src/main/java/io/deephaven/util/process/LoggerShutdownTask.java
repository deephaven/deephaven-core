/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.process;

import io.deephaven.io.log.LogSink;
import io.deephaven.util.loggers.Log4JTimedBufferedWriter;
import org.apache.log4j.LogManager;

public class LoggerShutdownTask extends OnetimeShutdownTask {

    @Override
    protected void shutdown() {
        LogSink.Shutdown.shutdown();
        Log4JTimedBufferedWriter.shutdown();
        LogManager.shutdown();
    }

    // I think I can replace all of the following (adapted from ServiceFactorImpl.finishLogging() and related methods)
    // with
    // LogManager.shutdown(). Keeping this around for posterity until I've got sufficient confidence in the new way:
    // ---------------------------------------------------------------------------------------------------------------------
    //
    // private static void tryToFlushLo44jAppenders() {
    // for (final Appender log4jAppender : gatherLog4jAppenders()) {
    // if (log4jAppender instanceof WriterAppender){
    // ((WriterAppender)log4jAppender).setImmediateFlush(true);
    // log4jAppender.setLayout(new PatternLayout(""));
    // ((WriterAppender)log4jAppender).append(new LoggingEvent("LoggerShutdownTask", <some logger>, Level.INFO, null,
    // null));
    // }
    // }
    // }
    //
    // @SuppressWarnings("unchecked")
    // private static Collection<Appender> gatherLog4jAppenders() {
    // final HashSet<Appender> appenders = new HashSet<>();
    // for (final Enumeration<org.apache.log4j.Logger> currentLoggers = LogManager.getCurrentLoggers();
    // currentLoggers.hasMoreElements();) {
    // for (final Enumeration<Appender> currentAppenders = currentLoggers.nextElement().getAllAppenders();
    // currentAppenders.hasMoreElements();) {
    // addLog4jAppenders(appenders, currentAppenders.nextElement());
    // }
    // }
    // for (final Enumeration<Appender> rootLoggerAppenders = LogManager.getRootLogger().getAllAppenders();
    // rootLoggerAppenders.hasMoreElements();) {
    // addLog4jAppenders(appenders, rootLoggerAppenders.nextElement());
    // }
    // return appenders;
    // }
    //
    // @SuppressWarnings("unchecked")
    // private static void addLog4jAppenders(final Collection<Appender> appenders, final Appender appender){
    // appenders.add(appender);
    // if (appender instanceof AppenderAttachable){
    // for (final Enumeration<Appender> attachedAppenders = ((AppenderAttachable)appender).getAllAppenders();
    // attachedAppenders.hasMoreElements();) {
    // addLog4jAppenders(appenders, attachedAppenders.nextElement());
    // }
    // }
    // }
    //
    // ---------------------------------------------------------------------------------------------------------------------
}
