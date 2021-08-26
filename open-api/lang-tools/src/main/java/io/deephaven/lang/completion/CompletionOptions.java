package io.deephaven.lang.completion;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Various context-sensitive options to use when generating completions.
 *
 * This includes information like "result will have quotes around it", or "result should have a comma after it" or
 * "result should have space before it".
 */
public class CompletionOptions {

    private boolean requireQuotes;
    private String quoteType;
    private boolean requireCommaSuffix;
    private boolean requireSpacePrefix;
    private String[] nextTokens;
    private String[] prevTokens;
    private Set<String> stopTokens;

    public boolean isRequireQuotes() {
        return requireQuotes;
    }

    public CompletionOptions setRequireQuotes(boolean requireQuotes) {
        this.requireQuotes = requireQuotes;
        return this;
    }

    public boolean isRequireCommaSuffix() {
        return requireCommaSuffix;
    }

    public CompletionOptions setRequireCommaSuffix(boolean requireCommaSuffix) {
        this.requireCommaSuffix = requireCommaSuffix;
        return this;
    }

    public boolean isRequireSpacePrefix() {
        return requireSpacePrefix;
    }

    public CompletionOptions setRequireSpacePrefix(boolean requireSpacePrefix) {
        this.requireSpacePrefix = requireSpacePrefix;
        return this;
    }

    public String getQuoteType() {
        return quoteType;
    }

    public CompletionOptions setQuoteType(String quoteType) {
        this.quoteType = quoteType;
        if (quoteType != null && !quoteType.isEmpty()) {
            requireQuotes = true;
        }
        return this;
    }

    public String[] getNextTokens() {
        return nextTokens;
    }

    public CompletionOptions setNextTokens(String... nextTokens) {
        this.nextTokens = nextTokens;
        return this;
    }

    public String[] getPrevTokens() {
        return prevTokens;
    }

    public CompletionOptions setPrevTokens(String... prevTokens) {
        this.prevTokens = prevTokens;
        return this;
    }

    public Set<String> getStopTokens() {
        if (stopTokens == null) {
            stopTokens = new HashSet<>();
        }
        return stopTokens;
    }

    public CompletionOptions setStopTokens(String... stopTokens) {
        getStopTokens().addAll(Arrays.asList(stopTokens));
        return this;
    }
}
