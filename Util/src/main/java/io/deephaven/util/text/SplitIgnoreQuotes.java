package io.deephaven.util.text;

import io.deephaven.datastructures.util.CollectionUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits a String on a character ignoring that character inside quotes and back ticks. For example splitting on a
 * comma:
 *
 * 'a,b', "c,d", 'e', "f", g splits to ['a,b'] ["c,d"] ['e'] ["f"] [g]
 */
public class SplitIgnoreQuotes {
    private static final char QUOTE = '\"';
    private static final char BACK_TICK = '`';

    private SplitMode mode = SplitMode.NORMAL;
    private StringBuilder builder;
    private final List<String> values = new ArrayList<>();

    /**
     * Splits a String using the splitter character but ignoring it inside quotes and back ticks.
     *
     * @param string the String to split
     * @param splitter the character to split on
     * @return a String array of split values
     */
    public String[] split(String string, char splitter) {
        mode = SplitMode.NORMAL;
        builder = new StringBuilder();
        values.clear();

        for (int i = 0; i < string.length(); i++) {
            final char c = string.charAt(i);
            if (c == QUOTE) {
                processQuote(c);
            } else if (c == BACK_TICK) {
                processBackTick(c);
            } else if (c == splitter) {
                processSplitter(c);
            } else {
                processChar(c);
            }
        }

        if (mode == SplitMode.IN_QUOTES) {
            throw new RuntimeException("Unmatched quote in expression " + string);
        } else if (mode == SplitMode.IN_BACK_TICKS) {
            throw new RuntimeException("Unmatched back tick in expression " + string);
        }

        // Check if there is one last value left
        addCurrentValue();

        return values.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    }

    private void processQuote(char c) {
        switch (mode) {
            case NORMAL:
                mode = SplitMode.IN_QUOTES;
                break;
            case IN_QUOTES:
                mode = SplitMode.NORMAL;
                break;
        }
        processChar(c);
    }

    private void processBackTick(char c) {
        switch (mode) {
            case NORMAL:
                mode = SplitMode.IN_BACK_TICKS;
                break;
            case IN_BACK_TICKS:
                mode = SplitMode.NORMAL;
                break;
        }
        processChar(c);
    }

    private void processSplitter(char c) {
        if (mode == SplitMode.IN_QUOTES || mode == SplitMode.IN_BACK_TICKS) {
            processChar(c);
        } else {
            // This is where the actual split occurs
            addCurrentValue();
            builder = new StringBuilder();
        }
    }

    private void addCurrentValue() {
        final String value = builder.toString().trim();
        if (!value.isEmpty()) {
            values.add(value);
        }
    }

    private void processChar(char c) {
        builder.append(c);
    }

    private enum SplitMode {
        NORMAL, IN_QUOTES, IN_BACK_TICKS
    }
}
