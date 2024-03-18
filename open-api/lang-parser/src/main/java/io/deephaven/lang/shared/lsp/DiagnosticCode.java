//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.lang.shared.lsp;

/**
 * A collection of int constants used for diagnostic messages.
 *
 *
 */
public interface DiagnosticCode {

    int MALFORMED = 1_000;
    int MALFORMED_TYPE_ARGUMENT = 1_001;
    int MALFORMED_EXPRESSION = 1_002;
    int MISSING_CLOSE_QUOTE = 1_003;
    int MISSING_CLOSE_PAREN = 1_003;
    int MISSING_CLOSE_QUOTE_AND_PAREN = 1_004;
    int MISSING_EQUALS = 1_005;
    int MISSING_DOT = 1_006;
}
