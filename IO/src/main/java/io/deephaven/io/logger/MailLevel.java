/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import org.apache.log4j.Level;

public class MailLevel extends Level {
    private String subject = null;

    public MailLevel() {
        this(null);
    }

    public MailLevel(String subject) {
        super(50001, "MAILER", 0);
        this.subject = subject;
    }

    public String getSubject() {
        return subject;
    }
}
