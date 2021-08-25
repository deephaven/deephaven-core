/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log;

public class LogLevel {

    private final int priority;
    private final String name;

    public LogLevel(int priority, String name) {
        this.priority = priority;
        this.name = name;
    }

    public int getPriority() {
        return priority;
    }

    public String getName() {
        return name;
    }

    public String toString() {
        return name;
    }

    // Using FATAL level is deprecated; we intend to get rid of it.
    public static final LogLevel FATAL = new LogLevel(50000, "FATAL");
    public static final LogLevel EMAIL = new LogLevel(45000, "EMAIL");
    public static final LogLevel STDERR = new LogLevel(40001, "STDERR");
    public static final LogLevel ERROR = new LogLevel(40000, "ERROR");
    public static final LogLevel WARN = new LogLevel(30000, "WARN");
    public static final LogLevel STDOUT = new LogLevel(20001, "STDOUT");
    public static final LogLevel INFO = new LogLevel(20000, "INFO");
    public static final LogLevel DEBUG = new LogLevel(10000, "DEBUG");
    public static final LogLevel TRACE = new LogLevel(5000, "TRACE");

    public static LogLevel valueOf(String s) {
        if (s.equals("FATAL"))
            return FATAL;
        if (s.equals("EMAIL"))
            return EMAIL;
        if (s.equals("STDERR"))
            return STDERR;
        if (s.equals("ERROR"))
            return ERROR;
        if (s.equals("WARN"))
            return WARN;
        if (s.equals("STDOUT"))
            return STDOUT;
        if (s.equals("INFO"))
            return INFO;
        if (s.equals("DEBUG"))
            return DEBUG;
        if (s.equals("TRACE"))
            return TRACE;
        throw new IllegalArgumentException(s);
    }

    public static class MailLevel extends LogLevel {

        private final String subject;

        public MailLevel() {
            super(50001, "MAILER");
            this.subject = null;
        }

        public MailLevel(String subject) {
            super(50001, "MAILER");
            this.subject = subject;
        }

        public String getSubject() {
            return subject;
        }
    }
}
