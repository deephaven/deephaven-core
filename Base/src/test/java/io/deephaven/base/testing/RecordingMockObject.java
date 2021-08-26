/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.testing;

// --------------------------------------------------------------------
/**
 * Base for mock objects that can keep a record of activity.
 */
public class RecordingMockObject {

    volatile StringBuffer m_stringBuffer = new StringBuffer();

    // ----------------------------------------------------------------
    public synchronized String getActivityRecordAndReset() {
        String sActivityRecord = m_stringBuffer.toString();
        m_stringBuffer = new StringBuffer();
        return sActivityRecord;
    }

    // ----------------------------------------------------------------
    public synchronized String getActivityRecordDoNotReset() {
        return m_stringBuffer.toString();
    }

    // ----------------------------------------------------------------
    public synchronized void recordActivity(String sMessage) {
        m_stringBuffer.append(sMessage);
    }
}
