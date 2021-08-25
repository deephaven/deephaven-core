/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.testing;

import java.util.regex.Pattern;

import junit.framework.ComparisonFailure;
import org.apache.log4j.Level;

// --------------------------------------------------------------------
/**
 * The details of a message that should have been logged, when the message must be matched against a regular expression.
 * For use with {@link LoggingRecorder}.
 */
public class CheckedMessageRegexp extends CheckedMessage {

    private final Pattern m_pattern;

    public CheckedMessageRegexp(Pattern pattern, Level level) {
        super(pattern.toString(), level);
        m_pattern = pattern;
    }

    @Override
    public void checkMessage(String sRenderedMessage) {
        if (!m_pattern.matcher(sRenderedMessage).matches()) {
            throw new ComparisonFailure("Could not match pattern.", m_pattern.toString(), sRenderedMessage);
        }
    }
}
