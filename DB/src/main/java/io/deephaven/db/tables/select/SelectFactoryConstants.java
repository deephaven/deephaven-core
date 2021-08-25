package io.deephaven.db.tables.select;

/**
 * A set of constants used to build consistent patterns to process query parameters.
 */
@SuppressWarnings("WeakerAccess")
public final class SelectFactoryConstants {
    /** The beginning of a complete expression. Matches the beginning and initial whitespace */
    public static final String START_PTRN = "\\A\\s*";

    /** The end of a complete expression. Matches any trailing spaces and the end of the input */
    public static final String END_PTRN = "\\s*\\Z";

    /**
     * Matches a variable starting with a letter, _ or $ followed by any number of letters, numbers,
     * _ or $
     */
    public static final String ID_PTRN = "[a-zA-Z_$][a-zA-Z0-9_$]*";

    /** An integer, including the initial minus sign */
    public static final String INT_PTRN = "-?\\d+";

    /** A Floating point number, optionally including the initial sign */
    public static final String FLT_PTRN = "[+-]?\\d*\\.\\d+[f]?" + "|" + "[+-]?\\d+\\.\\d*[f]?";

    /** A string, surrounded by either " or ` characters */
    public static final String STR_PTRN = "(\"[^\"]*\")|(`[^`]*`)";

    /** A DateTime, surrounded by ' characters */
    public static final String DATETIME_PTRN = "('[^']*')";

    /** A case insensitive boolean literal */
    public static final String BOOL_PTRN = "[tT][rR][uU][eE]" + "|" + "[fF][aA][lL][sS][eE]";

    /**
     * Any {@link #INT_PTRN int}, {@link #FLT_PTRN float}, {@link #BOOL_PTRN boolean}, or
     * {@link #STR_PTRN string} as defined above
     */
    public static final String LITERAL_PTRN = "(?:" + INT_PTRN + ")|(?:" + FLT_PTRN + ")|(?:"
        + BOOL_PTRN + ")|(?:" + STR_PTRN + ")|(?:" + DATETIME_PTRN + ")";

    /** Case insensitive 'icase' expression */
    public static final String ICASE = "[iI][cC][aA][sS][eE]";

    /** Case insensitive 'not' expression */
    public static final String NOT = "[nN][oO][tT]";

    /** Case insensitive 'in' expression */
    public static final String IN = "[iI][nN]";

    /** Case insensitive 'includes' expression */
    public static final String INCLUDES = "[iI][nN][cC][lL][uU][dD][eE][sS]";

    /** Case insensitive 'any' expression */
    public static final String ANY = "[aA][nN][yY]";

    /** Case insensitive 'all' expression */
    public static final String ALL = "[aA][lL][lL]";

    /** Any non line terminating expression */
    public static final String ANYTHING = ".*\\S+";
}
