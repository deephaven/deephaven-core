/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import org.apache.log4j.Level;

/**
 * Allows us to have an explicit priority named e-mail - this level is just logged with the priority string "EMAIL", and
 * the actual email is created by an external tool which scans the logfiles for EMAIL lines.
 */
public class CustomLog4jLevel extends Level {
    public static final int EMAIL_INT = 45000;
    final static public Level EMAIL = new CustomLog4jLevel(EMAIL_INT, "EMAIL", 1);

    protected CustomLog4jLevel(int i, String s, int j) {
        super(i, s, j);
    }
}
