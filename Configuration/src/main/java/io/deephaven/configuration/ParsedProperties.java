/*
 * Copyright (c) 2018 Deephaven and Patent Pending
 */

package io.deephaven.configuration;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.deephaven.io.logger.Logger;
import io.deephaven.internal.log.LoggerFactory;
import org.apache.commons.lang3.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.configuration.Configuration.QUIET_PROPERTY;

/**
 * Class for reading in a customized properties file, applying only the locally-relevant properties and keeping track of
 * which properties may not be further modified. Maintains the ordering of the properties from the input file.
 */
@SuppressWarnings("SpellCheckingInspection")
public class ParsedProperties extends Properties {
    // region Constants
    private static final String TOKEN_COMMENT_HASH = "#";
    private static final String TOKEN_COMMENT_BANG = "!";
    // Note the space; someone could have a property named 'finalize.this'
    private static final String TOKEN_FINALIZE = "finalize ";
    // Note the space; someone could have a property named 'final.countdown'
    private static final String TOKEN_FINAL = "final ";
    private static final String TOKEN_INCLUDE = "includefiles";
    private static final String TOKEN_SCOPE = Matcher.quoteReplacement("[");
    private static final String TOKEN_SCOPE_END = Matcher.quoteReplacement("]");
    private static final String TOKEN_SCOPE_OPEN = Matcher.quoteReplacement("{");
    private static final String TOKEN_SCOPE_CLOSE = Matcher.quoteReplacement("}");

    private static final Pattern propPattern = Pattern.compile("[:=]");
    private static final Pattern commaPattern = Pattern.compile(",");
    private static final Pattern equalPattern = Pattern.compile("[=]");

    private static final Logger log = LoggerFactory.getLogger(ParsedProperties.class);

    // endregion Constants

    // region Class variables
    private String thisFile;

    private final LinkedList<ConfigurationScope> scope = new LinkedList<>();
    private final Map<String, Object> props;

    // We want to report the line number in the case of errors within a line, and may want to report on it later other
    // ways.
    private int lineNum = 0;

    // We also need to keep track of which properties have been finalized and may no longer be modified,
    // regardless of scope.
    final private Set<String> finalProperties;

    // We also want to keep track of which properties were set by which line in the current file
    final private Map<String, List<PropertyHistory>> lineNumbers;

    // The Context tells us which items in the input file can be ignored for whatever is calling this configuration.
    final private ConfigurationContext context;

    final private PropertyInputStreamLoader propertyInputStreamLoader;

    private boolean expectingScopeOpen = false;
    private boolean haveParsedFirstLine = false;

    private final boolean ignoreScopes;

    // endregion Class variables

    // region Properties

    /**
     * A mapping from each property name to the file and location where the property came from. This is public so that
     * we can use this in a property inspector to get the full story about what properties exist and where those
     * properties were defined.
     *
     * @return The current map
     */
    @SuppressWarnings({"unused"})
    // This is used from an external utility for showing useful information about prop files.
    public Map<String, List<PropertyHistory>> getLineNumbers() {
        return Collections.unmodifiableMap(lineNumbers);
    }

    // endregion Properties

    // region Constructors

    /**
     * A constructor that starts with no existing scoped or final properties.
     */
    @SuppressWarnings("WeakerAccess")
    // This is used from an external utility for showing useful information about prop files.
    public ParsedProperties() {
        this(false);
    }

    /**
     * A constructor that starts with no existing scoped or final properties.
     *
     * @param ignoreScopes True if this parser should ignore scope restrictions, false otherwise. Used by the
     *        PropertyInspector when checking whether required or disallowed properties are present.
     */
    public ParsedProperties(final boolean ignoreScopes) {
        context = new ConfigurationContext();
        finalProperties = new HashSet<>();
        lineNumbers = new HashMap<>();
        props = new LinkedHashMap<>();
        this.ignoreScopes = ignoreScopes;
        this.propertyInputStreamLoader = PropertyInputStreamLoaderFactory.newInstance();
    }

    @Override
    public synchronized void putAll(Map<?, ?> t) {
        t.forEach((k, v) -> setProperty(k.toString(), v.toString()));
    }

    /**
     * A constructor that passes through the current state of any scoped and final properties, used when processing
     * includefiles.
     *
     * @param callingProperties An existing ParsedProperties object with existing data that should be further filled
     *        out.
     */
    @SuppressWarnings("CopyConstructorMissesField")
    private ParsedProperties(final ParsedProperties callingProperties) {
        this.context = callingProperties.getContext();
        this.finalProperties = callingProperties.getFinalProperties();
        this.lineNumbers = callingProperties.lineNumbers; // We actually want the original item, not a copy.
        this.props = callingProperties.props;
        this.ignoreScopes = callingProperties.ignoreScopes;
        this.propertyInputStreamLoader = callingProperties.propertyInputStreamLoader;
        // explicitly do NOT copy over scope, since we should be back to root when the import happens.
        // explicitly do not copy over filenames; those will get handled during the 'load' and 'merge' steps as needed.
        // explicitly do not copy over 'expectingScopeOpen' - that should not propagate across includefiles.
    }
    // endregion Constructors

    // region Parsing

    /**
     * Parse a single line.
     *
     * @param nextLine The text to be parsed.
     */
    private void parseLine(String nextLine) throws IOException {

        final boolean inScope = isContextValid();

        if (nextLine == null || nextLine.startsWith(TOKEN_COMMENT_HASH) || nextLine.startsWith(TOKEN_COMMENT_BANG)
                || nextLine.length() == 0) {
            // If the line starts with # or with !, it is a comment and should be ignored.
            // Blank lines can also be ignored.
            // noinspection UnnecessaryReturnStatement
            return;
        } else {
            if (expectingScopeOpen && !nextLine.startsWith(TOKEN_SCOPE_OPEN)) {
                throw new ConfigurationException(
                        TOKEN_SCOPE_OPEN + " must immediately follow a scope declaration, found : " + nextLine);
            } else if (!expectingScopeOpen && nextLine.startsWith(TOKEN_SCOPE_OPEN)) {
                throw new ConfigurationException(
                        TOKEN_SCOPE_OPEN + " may not be used as the first character of a property name: " + nextLine);
            } else if (startsWithIgnoreCase(nextLine, TOKEN_FINALIZE) && inScope) {
                finalizeProperty(nextLine);
            } else if (startsWithIgnoreCase(nextLine, TOKEN_FINAL) && inScope) {
                String restOfLine = ltrim(nextLine.replaceFirst(TOKEN_FINAL, ""));
                storePropertyDeclaration(restOfLine, true);
            } else if (startsWithIgnoreCase(nextLine, TOKEN_SCOPE)) {
                defineScopeBlock(nextLine);
            } else if (startsWithIgnoreCase(nextLine, TOKEN_SCOPE_OPEN)) {
                openScopeBlock(nextLine);
            } else if (startsWithIgnoreCase(nextLine, TOKEN_SCOPE_CLOSE)) {
                closeScopeBlock(nextLine);
            } else if (inScope) {
                storePropertyDeclaration(nextLine, false);
            }
            haveParsedFirstLine = true;
        }
    }

    /**
     * Wrapper on a call to String.regionMatches to make it more readable.
     *
     * @param baseString The string to search within
     * @param findString The string to find within the base string.
     * @return True if the target string starts the base string, false otherwise.
     */
    private static boolean startsWithIgnoreCase(String baseString, String findString) {
        return baseString.regionMatches(true, 0, findString, 0, findString.length());
    }

    /**
     * Record the value of a property declaration in the form X=Y or X:Y (ignoring whitespace around the = or :). Since
     * the property value may include terminal whitespace or any other character, it is NOT permitted to place further
     * instructions immediately on the same line.
     *
     * @param nextLine The line to be parsed
     * @param markFinal If this line was preceded by a 'final' directive, also mark this property as final.
     */
    private void storePropertyDeclaration(String nextLine, boolean markFinal) throws IOException {
        // Always starts with a token, then may have either : or =, followed by a value for the token.
        String[] parts = propPattern.split(nextLine, 2);
        final String token = parts[0].trim();
        String value;
        if (parts.length > 1) {
            value = ltrim(parts[1]);
        } else {
            value = "";
        }

        // the 'includefiles' line get special handling; in that case, we want to explicitly just load up the specified
        // files.
        // Since 'includefiles' must be the first non-comment line (if present), just immediately load those files.
        if (token.equals(TOKEN_INCLUDE)) {
            if (haveParsedFirstLine) {
                throw new ConfigurationException(TOKEN_INCLUDE
                        + " found in location other than first non-comment line in file " + thisFile + ".");
            }
            ParsedProperties includeProps;
            for (String file : commaPattern.split(value)) {
                // Since we're passing around the same collection objects, they'll automatically populate everything in
                // one batch.
                includeProps = new ParsedProperties(this);
                includeProps.load(file.trim());
            }
        } else if (token.equalsIgnoreCase(TOKEN_FINAL) || token.equalsIgnoreCase(TOKEN_FINALIZE)) {
            // If someone tries to use 'final final = value' or 'final finalize = value', then smack them on the wrist.
            throw new ConfigurationException(token + " is a reserved keyword and may not be used as a property name.");
        } else {
            if (!isFinal(token)) {
                List<PropertyHistory> tokenHistory = lineNumbers.computeIfAbsent(token, prop -> new ArrayList<>());
                props.put(token, value);
                // Store the line number that this declaration was made on AFTER storing the actual declaration,
                // so if there's a conflict with the 'final' descriptor, the previous line number will be persisted.
                tokenHistory.add(0, new PropertyHistory(thisFile, lineNum, value, stringScope()));
                if (markFinal) {
                    finalizeProperty(token);
                }
            } else {
                handleFinalConflict(token);
            }
        }
    }

    /**
     * Exit out of the current scope, returning to the next scope level up. It is permitted to place further
     * instructions immediately after this on the same line.
     *
     * @param nextLine The string to be parsed
     */
    private void closeScopeBlock(String nextLine) throws IOException {
        // If the line starts with }, and the scope is not 0, then end the current scope and move up one level.
        if (scope.size() > 0) {
            scope.removeLast(); // Whatever the last-added scope was, that is now removed.
        } else {
            throw new ConfigurationException(
                    TOKEN_SCOPE_CLOSE + " found at line " + lineNum + " with no matching " + TOKEN_SCOPE_OPEN);
        }
        String restOfLine = ltrim(nextLine.replaceFirst(TOKEN_SCOPE_CLOSE, ""));
        parseLine(restOfLine);
    }

    /**
     * Start operating inside a scope block that has already been defined. It is permitted to place further instructions
     * immediately after this on the same line.
     *
     * @param nextLine The line to be parsed.
     */
    private void openScopeBlock(String nextLine) throws IOException {
        // If the line starts with {, then move deeper into scope (if we are waiting to do so)
        if (expectingScopeOpen) {
            expectingScopeOpen = false;
        } else {
            throw new ConfigurationException(
                    "Found " + TOKEN_SCOPE_OPEN + " at line " + lineNum + " when none was expected.");
        }
        if (nextLine.length() > 1) {
            parseLine(ltrim(nextLine.substring(TOKEN_SCOPE_OPEN.length())));
        }
    }

    /**
     * Define a scope block. It is permitted to place the open indicator immediately after this on the same line.
     *
     * @param nextLine The line to be parsed.
     */
    private void defineScopeBlock(String nextLine) throws IOException {
        // If the line starts with [, then it is a scope declaration, and may have a terminal open-brace, which may have
        // more commands in it.

        // It is possible to define multiple scope items on the same level, or 'or' blocks.
        final int endBlock = nextLine.indexOf(TOKEN_SCOPE_END);
        if (endBlock < 0) {
            throw new ConfigurationException(
                    "Invalid scope declaration: unterminated scope block at line " + lineNum + ": " + nextLine);
        }
        // Skip the first character, since we know it's the opening [.
        final String scopeSection = nextLine.substring(1, endBlock);
        final String[] scopeItems = commaPattern.split(scopeSection);
        if (scopeItems.length == 0) {
            throw new ConfigurationException(
                    "Invalid scope declaration: scope with no scope items at line " + lineNum + ": " + nextLine);
        }
        final ArrayList<ConfigurationScope> newScopes = new ArrayList<>();
        for (String aScope : scopeItems) {
            String[] parts = equalPattern.split(aScope, 2);
            if (parts.length < 2) {
                throw new ConfigurationException(
                        "Invalid scope declaration: no '=' found at line " + lineNum + ":" + nextLine);
            }
            final String contextToken = parts[0].trim();
            final String contextValue = ltrim(parts[1]);

            newScopes.add(new ConfigurationScope(contextToken, contextValue)); // Whatever scope we've just entered, add
                                                                               // it to the end of the current list of
                                                                               // scope conditions.
        }

        scope.add(new ConfigurationScope(newScopes));

        // We've handled the scope as needed; now handle anything else on this line, if any.
        // The next thing we expect is an { to open this scope.
        String restOfLine = ltrim(nextLine.substring(endBlock + 1));
        expectingScopeOpen = true;
        parseLine(restOfLine);
    }

    /**
     * Finalize a given property. If the property does not exist, it will be created with a value of empty-string. It is
     * not permitted to place additional instructions on this line; anything after 'finalize' will be treated as the
     * name of the token.
     *
     * @param nextLine The string to be processed, in the form "finalize [token]".
     */
    private void finalizeProperty(String nextLine) {
        // If we're ignoring scopes, we have to ignore finalization as well; properties could be finalized in different
        // scopes.
        if (ignoreScopes)
            return;
        // if the line starts with 'finalize ', then the remainder of the line is a token to be marked as final.
        // Note that if the token has not been declared yet, it will be marked as final, and may not then be created.
        // It is non-harmful to finalize something that is already final, since at that point it can't be modified
        // anyway.
        final String[] tokens = commaPattern.split(nextLine.replaceFirst(TOKEN_FINALIZE, ""));
        for (String aToken : tokens) {
            final String token = aToken.trim();
            if (!this.containsKey(token)) {
                List<PropertyHistory> tokenHistory = lineNumbers.computeIfAbsent(token, prop -> new ArrayList<>());
                tokenHistory.add(0, new PropertyHistory(thisFile, lineNum, "(Property finalized with no value defined)",
                        stringScope()));
            }
            makePropertyFinal(token);
        }
    }

    /**
     * Indicate whether a specified property has already been marked as final. Note that wildcard values may be included
     * in 'finalize' uses, so a property will be considered 'final' if it matches a finalized value with a wildcard.
     * Example: If 'a.*.b' has been finalized, then 'a.foo.b' will be considered final after that.
     *
     * @param token The name of the property to check.
     * @return True if the property has been marked as final, false otherwise.
     */
    private boolean isFinal(String token) {
        return finalProperties.stream().anyMatch(str -> {
            final String pattern = createWildcardExpansionPattern(str);
            Pattern pat = Pattern.compile(pattern);
            // System.out.println("checking " + token + " against " + pattern);
            return pat.matcher(token).matches();
        });
    }

    /**
     * Make a property final, so that it can no longer have a new value entered. It is non-harmful to mark a property
     * final multiple times, since the check for 'final' status is binary and irreversible. There could exist cases
     * where a property might be conditionally marked final in one block, then universally finalized later, so this is
     * explicitly permitted.
     *
     * @param token The name of the property to mark as final.
     */
    private void makePropertyFinal(String token) {
        // If we're ignoring scopes, we have to ignore finalization as well; a property could be included in two
        // different scopes and be 'final' in both.
        if (ignoreScopes)
            return;
        finalProperties.add(token);
    }

    /**
     * Indiciate whether, given the current scope, the context is valid for the current line. Null values are
     * automatically considered invalid.
     *
     * @return True if the current scope matches the context, false otherwise.
     */
    private boolean isContextValid() {
        // Context is always valid when ignoring scope.
        if (ignoreScopes)
            return true;
        for (ConfigurationScope aScope : scope) {
            if (!aScope.scopeMatches(context)) {
                return false;
            }
        }
        return true;
    }

    // endregion Parsing

    // region Properties class overrides

    // A variety of Hashtable properties need to be overridden to redirect them to the LinkedHashMap we're actually
    // using.

    @Override
    public void clear() {
        props.clear();
    }

    @Override
    public boolean contains(Object value) {
        return props.containsValue(value.toString());
    }

    @Override
    public boolean containsKey(Object key) {
        return props.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return props.containsValue(value);
    }

    @Override
    public Enumeration<java.lang.Object> elements() {
        throw new RuntimeException("'Elements' method is not available for Deephaven properties.");
    }

    @SuppressWarnings("unchecked")
    @NotNull
    @Override
    public Set entrySet() {
        return props.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        if (o.getClass() != props.getClass())
            return false;
        return props.equals(o);
    }

    @Override
    public Object get(@NotNull Object key) {
        return props.get(key);
    }

    @Override
    public String getProperty(String key) {
        Object propVal = get(key);
        if (propVal == null)
            return null;
        return (propVal instanceof String) ? (String) propVal : null;
    }

    @Override
    public boolean isEmpty() {
        return props.isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Enumeration keys() {
        return Collections.enumeration(props.keySet());
    }

    @SuppressWarnings("unchecked")
    @Override
    @NotNull
    public Set keySet() {
        return props.keySet();
    }

    @Override
    public void list(PrintStream out) {
        this.list(new PrintWriter(out, true));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void list(PrintWriter out) {
        out.println("-- listing properties --");
        for (Map.Entry e : (Set<Map.Entry>) this.entrySet()) {
            String key = (String) e.getKey();
            String val = (String) e.getValue();
            if (val.length() > 40) {
                val = val.substring(0, 37) + "...";
            }
            out.println(key + "=" + val);
        }
    }

    @Override
    public int size() {
        return props.size();
    }

    @SuppressWarnings("unchecked")
    @NotNull
    @Override
    public Collection values() {
        return props.values();
    }

    /**
     * Load the properties from the specified InputStream, ignoring any directives that do not match the current
     * context. Automatically closes the stream when the last line has been processed.
     *
     * @param stream The open stream providing a view into the data to be parsed.
     * @throws IOException If the stream cannot be read at some point.
     */
    @Override
    public synchronized void load(InputStream stream) throws IOException {
        // Since logical lines are allowed to be represented as multiple actual lines due to trailing \,
        // we need to assemble the logical lines for parsing.
        Reader reader = new BufferedReader(new InputStreamReader(stream));
        final ParsedPropertiesLineReader lr = new ParsedPropertiesLineReader(reader);

        // When we include a file, that file needs to be processed before anything else in this one -
        // the fact that the other file is listed means it is assumed to pre-exist this one, and should
        // be handled first.
        String nextLine;

        while (lr.isOpen()) {
            String rawLine = lr.readLine();
            if (rawLine == null) {
                break;
            }
            nextLine = convertUnicodeEncoding(rawLine);
            lineNum += lr.getNumLinesLastRead(); // Since a logical line can break across multiple actual lines, need to
                                                 // track the actual line numbers.
            if (nextLine == null) {
                break;
            }
            parseLine(nextLine);
        }
        if (scope.size() != 0) {
            throw new ConfigurationException("Failed to close scope in file " + thisFile);
        }
        // Once the load is finished, we also want to make sure that if someone tries to set a value from code or
        // otherwise,
        // they don't get told that it happened inside this file.
        this.thisFile = "(Modified outside of configuration file)";
        this.lineNum = -1;
    }

    /**
     * Determine whether a property is final or not, and only allow the update if it is not already final.
     *
     * @param key The name of the property to set a value for.
     * @param value The value of the property being set.
     * @return The previous value of the property being set, or null if it had no previous value.
     */
    @Override
    public synchronized Object put(Object key, Object value) {
        return setProperty(key.toString(), value.toString());
    }

    /**
     * Remove a non-final property from the collection. Attempting to remove a final property will cause a
     * ConfigurationException to be thrown.
     *
     * @param key The name of the property to be removed.
     * @return The value of the property, if it existed.
     */
    @Override
    public synchronized Object remove(Object key) {
        if (isFinal(key.toString())) {
            handleFinalConflict(key.toString());
        }
        return props.remove(key);
    }

    /**
     * Determine whether a property is final or not, and only allow the update if it is not already final. This should
     * not be called from within the load operation for this class.
     *
     * @param key The name of the property to set a value for.
     * @param value The value of the property being set.
     * @return The previous value of the property being set, or null if it had no previous value.
     */
    @Override
    public synchronized Object setProperty(String key, String value) {
        if (!isFinal(key)) {
            // If something calls 'put' on this property other than during a load, then we don't have a location for it.
            List<PropertyHistory> tokenHistory = lineNumbers.computeIfAbsent(key, prop -> new ArrayList<>());
            // Once we're fully converted to Java 9 or higher, we could use the StackWalker API to get this more
            // efficiently. Since we're currently moving away from Java 8, which would want to use SharedSecrets
            // instead,
            // and since our code internally does not appear to have any cases where properties get set in a loop
            // where performance would be notably impacted, we can go ahead and use the full stack for now.
            // We also want to skip frame 0, since that's THIS method, which we know is in use anyway.
            StackTraceElement[] stack = new Throwable().getStackTrace();
            StringBuilder stackOutput = new StringBuilder("<not from configuration file>: ");
            if (stack != null && stack.length > 1) {
                int traceLen = Integer.min(4, stack.length);
                for (int fc = 1; fc < traceLen; fc++) {
                    stackOutput.append(stack[fc].toString()).append(System.lineSeparator());
                }
            }
            tokenHistory.add(0, new PropertyHistory(stackOutput.toString(), 0, value, stringScope()));
            return props.put(key, value);
        } else {
            handleFinalConflict(key);
            return props.get(key);
        }
    }

    // endregion Class overrides

    // region Utilities

    /**
     * Convert the current Scope to a String representation.
     *
     * @return A String representing the current scope.
     */
    private String stringScope() {
        return "[" + scope.stream().map(ConfigurationScope::toString).collect(Collectors.joining("]-[")) + "]";
    }

    /**
     * Load the properties from the specified file, ignoring any directives that do not match the current context.
     * Directories may be specified, including relative paths, but the '~' operator is not supported.
     *
     * <p>
     * Loads the fileName via {@link PropertyInputStreamLoader}.
     *
     * @param fileName The resource or file to load.
     * @throws IOException if the property stream cannot be processed
     * @throws ConfigurationException if the property stream cannot be opened
     *
     */
    public synchronized void load(String fileName) throws IOException, ConfigurationException {
        if (System.getProperty(QUIET_PROPERTY) == null) {
            log.info("Loading " + fileName);
        }
        thisFile = fileName;

        try (InputStream resourceAsStream = propertyInputStreamLoader
                .openConfiguration(fileName)) {
            this.load(resourceAsStream);
        }
    }

    /**
     * In a property name, "*" should be treated as a wildcard, so if we find one, it should be treated as a regex for
     * ".*" - but everything else has to be an exact match. Enclose the entire thing in the regex literal markers "\Q"
     * and "\E" to mark the entire name as being a string literal, then replace any instances of "*" with ".*" and make
     * everything around the "*" into the end of the prior literal and the start of a new literal. So "A*B" ->
     * "\QA\E.*\QB\E" == "Match the pattern of the exact string 'A', then any text, then the exact string 'B'."
     *
     * @param token The string to be wildcard-expanded
     * @return The possibly-wildcarded string turned into a regex, with all non-asterisk characters treated as string
     *         literals, not a pattern.
     */
    private String createWildcardExpansionPattern(String token) {
        return ("\\Q" + token + "\\E").replaceAll("\\*", "\\\\E.*\\\\Q");
    }


    /**
     * Handle situations where something tries to modify a property that has been marked as 'final.' If an included file
     * marks something as final, this file may not edit that value, regardless of whether this file calls it final or
     * not. If this file includes a property twice, and the first one is marked final, then that's also an issue.
     *
     * @param token The name of the property that was being set.
     */
    private void handleFinalConflict(final String token) throws ConfigurationException {
        // Since includefiles has to be the first effective line, if present at all,
        // we don't need to handle backreferences - everything from the included files will be present
        // by the time the current file is processed.
        List<PropertyHistory> tokenHistory = lineNumbers.get(token);
        String finalPattern = null;
        if (tokenHistory == null) {
            // This token was finalized via a pattern-match, and this exact value doesn't exist. Find the matching
            // pattern.
            finalPattern = finalProperties.stream().filter(str -> {
                final String pattern = createWildcardExpansionPattern(str);
                Pattern pat = Pattern.compile(pattern);
                // System.out.println("checking " + token + " against " + pattern);
                return pat.matcher(token).matches();
            }).findFirst().orElse("");
            tokenHistory = lineNumbers.get(finalPattern);
            // It shouldn't be possible to have something final but not be able to find it, but if it did happen, just
            // return a less-informative error.
            if (tokenHistory == null) {
                throw new ConfigurationException(
                        ("Property '" + token + "' previously marked as final was then modified in file '" + thisFile
                                + "' at line " + lineNum));
            }
        }
        StringBuilder msgBuilder = new StringBuilder("Property '" + token +
                "' marked as final in file '" + tokenHistory.get(0).fileName +
                "' with value at line " + tokenHistory.get(0).lineNumber);
        if (finalPattern != null) {
            msgBuilder.append(" with pattern '").append(finalPattern).append("' and");
        }
        if (lineNum >= 0) {
            msgBuilder.append(" was then modified in file '").append(thisFile).append("' at line ").append(lineNum);
        } else {
            msgBuilder.append(" was then modified outside of a configuration file");
        }
        throw new ConfigurationException(msgBuilder.toString());
    }

    /**
     * A list of all the properties that have been marked as final and thus may not be further updated.
     *
     * @return The set of all the properties that have thus far been marked as final.
     */
    private Set<String> getFinalProperties() {
        return finalProperties;
    }

    /**
     * Get the context for the current process, along with all the values that have been retrieved thus far so they
     * don't need to be looked up again.
     *
     * @return The existing context
     */
    private ConfigurationContext getContext() {
        return context;
    }

    /**
     * Handle escaped unicode characters, just in case anyone made any uXXXX characters.
     *
     * @param inString The input string that may contain encoded characters
     * @return A String with any encoded characters rendered into regular characters.
     */
    private static String convertUnicodeEncoding(String inString) {
        return StringEscapeUtils.unescapeJava(inString);
    }

    /**
     * Quick utility for trimming whitespace from the left side of a string only.
     *
     * @param trimMe The string to be LTrimmed.
     * @return The string without any whitespace on the left.
     */
    private static String ltrim(String trimMe) {
        if (trimMe == null) {
            return null;
        }
        int pos = 0;
        while (pos < trimMe.length() && Character.isWhitespace(trimMe.charAt(pos))) {
            pos++;
        }
        return trimMe.substring(pos);
    }

    // endregion Utilities

    /**
     * Take a properties file and return it line by line - taking into account the 'this continues on next line'
     * indicators.
     */
    private class ParsedPropertiesLineReader {
        private final BufferedReader breader;
        private boolean open = true;
        private String nextLine;
        private int numLinesLastRead = 0;

        /**
         * Create the line reader and initially populate its internal objects.
         *
         * @param reader The Reader object to be processed.
         */
        ParsedPropertiesLineReader(Reader reader) {
            breader = new BufferedReader(reader);
        }

        /**
         * Return the next logical line from the file, where a line ending in \ means to continue on the next line.
         * Automatically closes the stream when the last line has been read.
         *
         * @return The next logical line.
         * @throws IOException If the file cannot be read.
         */
        String readLine() throws IOException {
            StringBuilder retLine = new StringBuilder();
            numLinesLastRead = 0;
            // First time we enter this, need to populate nextLine.
            if (nextLine == null) {
                nextLine = ltrim(breader.readLine());
            }
            do {
                // remove trailing backslash, if that was a single value
                if (retLine.toString().endsWith("\\") && !retLine.toString().endsWith("\\\\")) {
                    retLine.deleteCharAt(retLine.length() - 1);
                }
                // StringBuilder will explicitly write 'null' if a null is appended, so give that special handling.
                if (nextLine != null) {
                    retLine.append(nextLine);
                }
                nextLine = ltrim(breader.readLine());
                numLinesLastRead++;
            } while (retLine.toString().endsWith("\\") && !retLine.toString().endsWith("\\\\") && nextLine != null);
            // If we've read the last line, close the reader.
            if (nextLine == null) {
                open = false;
                breader.close();
            }

            return retLine.toString();
        }

        int getNumLinesLastRead() {
            return numLinesLastRead;
        }

        /**
         * Indicate whether the reader has been closed.
         *
         * @return True if the reader can still be read from, false otherwise.
         */
        boolean isOpen() {
            return open;
        }
    }

    /**
     * Return the configuration contexts. This is the list of system properties that may have been used to parse the
     * configuration file. This collection will be immutable.
     *
     * @return the configuration contexts.
     */
    Collection<String> getContextKeyValues() {
        return context.getContextKeyValues();
    }
}
