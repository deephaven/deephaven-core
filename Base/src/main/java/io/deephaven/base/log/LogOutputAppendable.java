//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.log;

/**
 * Allows objects to be smart about appending themselves to LogOutput instances... (instead of always calling
 * LogOutput.append(Object.toString()))
 */
public interface LogOutputAppendable {
    LogOutput append(LogOutput logOutput);
}
