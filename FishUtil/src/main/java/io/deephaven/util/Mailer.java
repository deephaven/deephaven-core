/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Mailer {
    void sendEmail(String sender, String[] recipients, String subject, String msg) throws IOException;

    void sendEmail(String sender, String recipient, String subject, String msg) throws IOException;

    void sendHTMLEmail(String sender, String recipient, String subject, String msg) throws IOException;

    void sendEmail(String sender, String recipient, String subject, String msg,
            List<Map.Entry<String, String>> extraHeaderEntries) throws IOException;
}
