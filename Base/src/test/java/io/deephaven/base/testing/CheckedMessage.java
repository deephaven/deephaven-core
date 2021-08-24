/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.testing;

import io.deephaven.base.verify.Require;
import org.apache.log4j.Level;

// --------------------------------------------------------------------
/**
 * The details of a message that should have been logged. For use with {@link LoggingRecorder}.
 */
public class CheckedMessage {
    private final String m_sMessageFragment;
    private final String m_sDetailFragment;
    private final Level m_level;

    public CheckedMessage(String sMessageFragment, Level level) {
        Require.nonempty(sMessageFragment, "sMessageFragment");
        m_sMessageFragment = sMessageFragment;
        m_sDetailFragment = null;
        m_level = level;
    }

    public CheckedMessage(String sMessageFragment, String sDetailFragment, Level level) {
        Require.nonempty(sMessageFragment, "sMessageFragment");
        Require.nonempty(sDetailFragment, "sDetailFragment");
        m_sMessageFragment = sMessageFragment;
        m_sDetailFragment = sDetailFragment;
        m_level = level;
    }

    public String getMessageFragment() {
        return m_sMessageFragment;
    }

    public String getDetailFragment() {
        return m_sDetailFragment;
    }

    public Level getLevel() {
        return m_level;
    }

    public void checkMessage(String sRenderedMessage) {
        SimpleTestSupport.assertStringContains(sRenderedMessage, m_sMessageFragment);
    }
}
