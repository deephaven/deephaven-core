/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.loggers;

import io.deephaven.configuration.Configuration;
import io.deephaven.util.SMTPMailer;
import io.deephaven.io.logger.MailLevel;
import org.apache.log4j.*;
import org.apache.log4j.spi.*;

import java.net.*;
import java.util.*;

public class SimpleMailAppender extends AppenderSkeleton {

    private static final Logger log = Logger.getLogger(SimpleMailAppender.class);

    private String to_ = null;
    private SMTPMailer mailer_ = new SMTPMailer();

    private boolean hadFatalEvent = false;

    public static Level MAIL(String subject) {
        return new MailLevel(subject);
    }

    protected void append(LoggingEvent loggingEvent) {
        try {
            Level level = loggingEvent.getLevel();
            if (level.equals(Level.FATAL)) {
                if (hadFatalEvent) {
                    return;
                }

                hadFatalEvent = true;
            }

            if (to_ == null) {
                return;
            }

            String hostname = InetAddress.getLocalHost().getHostName();

            final StringBuilder subjectB = new StringBuilder();
            if (Configuration.getInstance().hasProperty("system.type")) {
                subjectB.append("[").append(Configuration.getInstance().getProperty("system.type")).append("] ");
            }
            subjectB.append(level).append(" ").append(loggingEvent.getMessage().toString().replaceFirst(".*FATAL", ""));

            final StringBuilder message = new StringBuilder();

            message.append("/*----------------------------------------*/\n");
            message.append("Host: ").append(hostname).append("\n");
            message.append("Config: ").append(Configuration.getConfFileNameFromProperties()).append("\n");
            message.append("Date: ").append(new Date()).append("\n");
            message.append("User: ").append(System.getProperty("user.name")).append("\n");
            message.append("Process: ").append(System.getProperty("process.name")).append("\n");
            message.append("/*----------------------------------------*/\n");

            message.append("\n");

            message.append(loggingEvent.getMessage().toString());
            message.append("\n\n");

            String s[] = loggingEvent.getThrowableStrRep();
            if (s != null) {
                for (String value : s) {
                    message.append(value);
                    message.append("\n");
                }
            }

            String from = System.getProperty("user.name") + "@" + hostname;
            String subject = subjectB.toString();
            if (subject.length() > 128)
                subject = subject.substring(0, 128);

            if (level instanceof MailLevel) {
                String subjectOverride = ((MailLevel) level).getSubject();
                if (subjectOverride != null)
                    subject = subjectOverride;
            }

            mailer_.sendEmail(from, to_.split(","), subject, message.toString());
        } catch (Exception e) {
            log.error("Couldn't send email", e);
        }
    }

    public void close() {

    }

    public boolean requiresLayout() {
        return true;
    }

    public void setTo(String to) {
        to_ = to;
    }
}
