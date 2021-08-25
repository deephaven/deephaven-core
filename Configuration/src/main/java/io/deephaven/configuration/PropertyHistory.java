package io.deephaven.configuration;

/**
 * A simple datastore to record one step in the history of a property. A property may have its value overwritten by
 * subsequent declarations or in subsequent files, but it is convenient to be able to identify what the active value of
 * a property is, where that value was created, and what other values the property may have been assigned by other files
 * before getting to this final value.
 */
public class PropertyHistory {
    /**
     * The name of the file where the property value was set, or a note that the value was not set by a configuration
     * file.
     */
    final public String fileName;
    /**
     * The number of the line within the file where the property value was set. If the property value was not set via a
     * configuration file (such as a system property or a programmatic change of value), then this should be 0.
     */
    final public int lineNumber;
    /**
     * The value that the property was set to by the specified line number in the specified file.
     */
    final public String value;

    /**
     * The full string of the active context.
     */
    final public String context;

    /**
     * Create a PropertyHistory element.
     * 
     * @param fileName The name of the file where this property value was set
     * @param lineNumber The number of the line within the file where this property value was set.
     * @param value The value this property was set to on the specified line in the specified file.
     */
    public PropertyHistory(String fileName, int lineNumber, String value, String context) {
        this.fileName = fileName;
        this.lineNumber = lineNumber;
        this.value = value;
        this.context = context;
    }
}
