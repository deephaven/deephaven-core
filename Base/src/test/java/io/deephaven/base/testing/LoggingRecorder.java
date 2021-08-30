/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.testing;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import junit.framework.ComparisonFailure;
import junit.framework.TestCase;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

// --------------------------------------------------------------------
/**
 * Temporarily intercepts logging for a given {@link Logger} and saves all logged messages to memory
 * so that a unit test can verify the correct messages were logged.
 * <P>
 * Usage:
 * <LI>To start capturing (in {@link TestCase#setUp}):
 * 
 * <Pre>
 * m_loggingRecorder = new LoggingRecorder(Logger.getLogger(Target.class));
 * </Pre>
 * 
 * <LI>To stop capturing (in {@link TestCase#tearDown}):
 * 
 * <Pre>
 * m_loggingRecorder.detach();<BR>m_loggingRecorder=null;
 * </Pre>
 * 
 * <LI>To check results:
 * 
 * <Pre>
 * m_loggingRecorder.assertNoMessagesLogged();
 * </Pre>
 * 
 * &nbsp; &nbsp; &nbsp; or
 * 
 * <Pre>
 * m_loggingRecorder.assertMessagesLogged(<BR>    new CheckedMessage("Hello, World!", Level.INFO)<BR>);
 * </Pre>
 */
public class LoggingRecorder {

    private static final CheckedMessage[] NO_CHECKED_MESSAGES = new CheckedMessage[0];

    private final List<LoggingEvent> m_loggingEvents = new LinkedList<LoggingEvent>();

    private final Logger m_logger;
    private final boolean m_bOriginalAdditivity;

    private final Appender m_appender = new Appender() {
        public void addFilter(Filter filter) {
            throw Assert.statementNeverExecuted();
        }

        public Filter getFilter() {
            throw Assert.statementNeverExecuted();
        }

        public void clearFilters() {
            throw Assert.statementNeverExecuted();
        }

        public void close() {
            throw Assert.statementNeverExecuted();
        }

        public void doAppend(LoggingEvent loggingEvent) {
            loggingEvent.getRenderedMessage();
            m_loggingEvents.add(loggingEvent);
        }

        public String getName() {
            return "LoggingRecorder";
        }

        public void setErrorHandler(ErrorHandler errorHandler) {
            throw Assert.statementNeverExecuted();
        }

        public ErrorHandler getErrorHandler() {
            throw Assert.statementNeverExecuted();
        }

        public void setLayout(Layout layout) {
            throw Assert.statementNeverExecuted();
        }

        public Layout getLayout() {
            throw Assert.statementNeverExecuted();
        }

        public void setName(String string) {
            throw Assert.statementNeverExecuted();
        }

        public boolean requiresLayout() {
            throw Assert.statementNeverExecuted();
        }
    };

    // ------------------------------------------------------------
    public LoggingRecorder(Logger logger) {
        m_logger = logger;
        m_bOriginalAdditivity = m_logger.getAdditivity();
        m_logger.setAdditivity(false);
        m_logger.addAppender(m_appender);
    }

    // ------------------------------------------------------------
    public void detach() {
        m_logger.removeAppender(m_appender);
        m_logger.setAdditivity(m_bOriginalAdditivity);
    }

    // ------------------------------------------------------------
    public List<LoggingEvent> getReportAndReset() {
        LoggingEvent[] loggingEvents =
            m_loggingEvents.toArray(new LoggingEvent[m_loggingEvents.size()]);
        m_loggingEvents.clear();
        return Arrays.asList(loggingEvents);
    }

    // ------------------------------------------------------------
    public void assertNoMessagesLogged() {
        assertMessagesLogged(NO_CHECKED_MESSAGES);
    }

    // ------------------------------------------------------------
    public void assertOneWarningLogged(String sMessageFragment) {
        Require.nonempty(sMessageFragment, "sMessageFragment");
        assertOneMessageLogged(sMessageFragment, Level.WARN);
    }

    // ------------------------------------------------------------
    public void assertOneMessageLogged(String sMessageFragment, Level level) {
        Require.nonempty(sMessageFragment, "sMessageFragment");
        assertMessagesLogged(new CheckedMessage(sMessageFragment, level));
    }

    // ------------------------------------------------------------
    public void assertMessagesLogged(CheckedMessage... checkedMessages) {
        List<LoggingEvent> messages = getReportAndReset();
        if (checkedMessages.length != messages.size()) {
            dumpLogMessages(messages);
        }
        junit.framework.Assert.assertEquals(checkedMessages.length, messages.size());
        for (int nIndex = 0; nIndex < checkedMessages.length; nIndex++) {
            CheckedMessage checkedMessage = checkedMessages[nIndex];
            LoggingEvent message = messages.get(nIndex);
            checkedMessage.checkMessage(message.getRenderedMessage());

            if (null != checkedMessage.getDetailFragment()) {
                SimpleTestSupport.assertStringContains(
                    message.getThrowableInformation().getThrowable().toString(),
                    checkedMessage.getDetailFragment());
            }
            junit.framework.Assert.assertEquals(checkedMessage.getLevel(), message.getLevel());
        }
    }

    // ------------------------------------------------------------
    public void assertMessagesLoggedInAnyOrder(CheckedMessage... checkedMessages) {
        List<LoggingEvent> messages = getReportAndReset();
        if (checkedMessages.length != messages.size()) {
            dumpLogMessages(messages);
        }
        junit.framework.Assert.assertEquals(checkedMessages.length, messages.size());
        for (int nIndex = 0; nIndex < checkedMessages.length; nIndex++) {
            CheckedMessage checkedMessage = checkedMessages[nIndex];

            boolean found = false;
            for (LoggingEvent message : messages) {
                try {
                    checkedMessage.checkMessage(message.getRenderedMessage());

                    if (null != checkedMessage.getDetailFragment()) {
                        SimpleTestSupport.assertStringContains(
                            message.getThrowableInformation().getThrowable().toString(),
                            checkedMessage.getDetailFragment());
                    }
                    junit.framework.Assert.assertEquals(checkedMessage.getLevel(),
                        message.getLevel());
                    found = true;
                    break;
                } catch (ComparisonFailure e) {
                    // continue
                }
            }
            if (!found) {
                junit.framework.Assert
                    .fail("Could not find \"" + checkedMessage.getMessageFragment() + "\"");
            }
        }
    }

    public void discardLoggedMessagePotentiallyManyOfButOnly(CheckedMessage checkedMessage) {
        List<LoggingEvent> messages = getReportAndReset();
        for (LoggingEvent message : messages) {
            checkedMessage.checkMessage(message.getRenderedMessage());
            if (null != checkedMessage.getDetailFragment()) {
                SimpleTestSupport.assertStringContains(
                    message.getThrowableInformation().getThrowable().toString(),
                    checkedMessage.getDetailFragment());
            }
            junit.framework.Assert.assertEquals(checkedMessage.getLevel(), message.getLevel());
        }
    }

    // ------------------------------------------------------------
    private static void dumpLogMessages(List<LoggingEvent> messages) {
        for (LoggingEvent message : messages) {
            System.err.println("Possibly unexpected log message: [" + message.getLevel() + "] "
                + message.getRenderedMessage()
                + (null != message.getThrowableInformation()
                    ? " (" + message.getThrowableInformation().getThrowable().toString() + ")"
                    : ""));
        }
    }
}
