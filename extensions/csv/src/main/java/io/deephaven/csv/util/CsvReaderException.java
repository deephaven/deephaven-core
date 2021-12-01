package io.deephaven.csv.util;

/**
 * The standard Exception class for various CSV errors.
 */
public class CsvReaderException extends Exception {
    public CsvReaderException(String message) {
        super(message);
    }

    public CsvReaderException(String message, Throwable cause) {
        super(message, cause);
    }
}
